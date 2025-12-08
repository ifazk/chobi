//  Chobi and Chithi: Managment tools for ZFS snapshot, send, and recv
//  Copyright (C) 2025  Ifaz Kabir

//  This program is free software: you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation, either version 3 of the License, or
//  (at your option) any later version.

//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.

//  You should have received a copy of the GNU General Public License
//  along with this program.  If not, see <https://www.gnu.org/licenses/>.

use chobi::AutoKill;
use chobi::chithi::sys::{get_date, hostname};
use chobi::chithi::{Args, Cmd, CmdTarget, Fs, Role, get_is_roots, sys};
use clap::Parser;
use log::{debug, error, info, trace, warn};
use regex_lite::Regex;
use std::{
    collections::HashMap,
    io::{self, BufRead, BufReader, Write},
    process::{Stdio, exit},
};

const DOES_NOT_EXIST: &str = "dataset does not exist";

struct CmdConfig<'args, 'target> {
    source_zfs: Cmd<'args, &'target [&'args str]>,
    target_ps: Cmd<'args, &'target [&'args str]>,
    target_zfs: Cmd<'args, &'target [&'args str]>,
    args: &'args Args,
    zfs_recv: Regex,
}

impl<'args, 'target> CmdConfig<'args, 'target> {
    pub fn new(
        source_cmd_target: &'args CmdTarget<'args>,
        source_is_root: bool,
        target_cmd_target: &'args CmdTarget<'args>,
        target_is_root: bool,
        args: &'args Args,
    ) -> io::Result<Self> {
        let source_zfs = Cmd::new(source_cmd_target, !source_is_root, "zfs", &[][..]);
        let target_ps = Cmd::new(target_cmd_target, false, "ps", &["-Ao", "args="][..]);
        let target_zfs = Cmd::new(target_cmd_target, !target_is_root, "zfs", &[][..]);
        if !args.no_command_checks {
            source_zfs.check_exists()?;
            target_ps.check_exists()?;
            target_zfs.check_exists()?;
        }
        // precompile zfs_recv regex
        let zfs_recv = {
            // In syncoid they use this regex:
            // "zfs *(receive|recv)[^\/]*\Q$fs\E\Z"
            // zfs | space star | receive or recv | (not /) star | quoted(fs.fs) | \Z
            // The (not /) star crudely covers flags like -s -F, etc.
            // \Z is like $, but can match before a newline as well
            // In our version we do a suffix check and then use the following pattern
            Regex::new(r"zfs *(receive|recv).*\s$").expect("regex pattern should be correct")
        };

        Ok(Self {
            source_zfs,
            target_ps,
            target_zfs,
            args,
            zfs_recv,
        })
    }

    pub fn check_ssh_if_needed(
        source_cmd_target: &CmdTarget,
        target_cmd_target: &CmdTarget,
        local_cmd_target: &CmdTarget,
        no_command_checks: bool,
    ) -> io::Result<()> {
        if (source_cmd_target.is_remote() || target_cmd_target.is_remote()) && !no_command_checks {
            let ssh_exists = Cmd::new(local_cmd_target, false, "ssh", &[][..])
                .to_check()
                .output()?
                .status
                .success();
            if !ssh_exists {
                error!("there are remote targets, but ssh does not exist in local system");
                exit(1);
            }
        }
        Ok(())
    }

    fn is_zfs_busy(&self, fs: &Fs) -> io::Result<bool> {
        debug!(
            "checking if {fs} is already in zfs receive using {} ...",
            self.target_ps
        );

        let mut ps_cmd = self.target_ps.to_cmd();
        ps_cmd.stdout(Stdio::piped());
        let mut ps_process = ps_cmd.spawn()?;

        let ps_stdout = ps_process.stdout.take().expect("handle present");
        let _ps_process = AutoKill::new(ps_process);
        let ps_stdout = BufReader::new(ps_stdout);

        for line in ps_stdout.lines() {
            let line = line?;
            if let Some(line_prefix) = line.strip_suffix(fs.fs.as_ref())
                && self.zfs_recv.is_match(line_prefix)
            {
                debug!("process {line} matches target {fs}");
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn target_exists(&self, fs: &Fs) -> io::Result<bool> {
        // We don't use get_zfs_value(fs, "name") here to avoid printing the error value
        let mut target_zfs = self.target_zfs.to_mut();
        target_zfs.args(["get", "-H", "name"]);
        target_zfs.arg(fs.fs.as_ref());
        debug!("checking if target filesystem {fs} exists using {target_zfs}...");
        let output = target_zfs.capture()?;
        if !output.status.success() {
            if output
                .stderr
                .windows(DOES_NOT_EXIST.len())
                .any(|x| x == DOES_NOT_EXIST.as_bytes())
            {
                return Ok(false);
            }
            error!("failed to check if target filesystem {fs} exists");
            exit(1);
        }
        // TODO why is this correct? "pool/foobar" begins with "pool/foo", but
        // are different file systems. is this a bug in syncoid?
        Ok(output.stdout[..].starts_with(fs.fs.as_bytes()))
    }

    fn get_resume_token(&self, fs: &Fs) -> io::Result<Option<String>> {
        // returning early without checking ErrorKind::NotFound is okay here
        // since there is a call to target_exists before
        let token = self.get_zfs_value(fs, "receive_resume_token")?;
        if !["-", ""].contains(&token.as_str()) {
            return Ok(Some(token));
        }
        debug!("no recv token found");
        Ok(None)
    }

    fn get_child_datasets<'a>(&self, fs: &Fs<'a>) -> io::Result<Vec<Fs<'a>>> {
        let mut source_zfs = self.source_zfs.to_mut();
        const LIST_CHILD_DATASET: [&str; 6] = [
            "list",
            "-o",
            "name,origin",
            "-t",
            "filesystem,volume",
            "-Hr",
        ];
        source_zfs.args(LIST_CHILD_DATASET);
        source_zfs.arg(fs.fs.as_ref());
        debug!("getting list of child datasets for {fs} using {source_zfs}...");
        let output = source_zfs.capture_stdout()?;
        if !output.status.success() {
            error!("failed to get child datasets for {fs}");
            exit(1);
        }
        let mut children = Vec::new();
        for line in output.stdout.lines() {
            let line = line?;
            let Some((name, origin)) = line.split_once("\t") else {
                return Err(io::Error::other(format!(
                    "expected tab separated name and origin, got {line}"
                )));
            };
            let child = fs.new_child(name.to_string(), origin.to_string());
            children.push(child);
        }
        Ok(children)
    }

    fn pick_zfs(&self, role: Role) -> &Cmd<'args, &'target [&'args str]> {
        match role {
            Role::Source => &self.source_zfs,
            Role::Target => &self.target_zfs,
        }
    }

    /// Returns ErrorKind::NotFound if dataset does not exist
    fn get_zfs_value(&self, fs: &Fs, property: &str) -> io::Result<String> {
        let mut zfs = self.pick_zfs(fs.role).to_mut();
        zfs.args(["get", "-H", property, &fs.fs]);
        debug!("getting current value of {property} on {fs} using {zfs}...");
        let output = zfs.capture()?;
        // Output err since we're not inheriting
        io::stderr().write_all(&output.stderr)?;

        // handle does not exist
        let err_bytes = &output.stderr[..];
        if err_bytes
            .windows(DOES_NOT_EXIST.len())
            .any(|x| x == DOES_NOT_EXIST.as_bytes())
        {
            return Err(io::Error::new(io::ErrorKind::NotFound, DOES_NOT_EXIST));
        };

        if !output.status.success() {
            // other error
            error!("failed to get property {property} for {fs}");
            exit(1);
        };
        let stdout = output.stdout;
        let stdout = str::from_utf8(&stdout)
            .map_err(|e| {
                io::Error::other(format!(
                    "could not parse output of zfs -H {property} {}: {e}",
                    fs.fs
                ))
            })?
            .trim();
        let value = stdout.split('\t').nth(2).ok_or_else(|| {
            io::Error::other(format!(
                "expected zfs -H {property} {} to return at least three fields",
                fs.fs
            ))
        })?;
        Ok(value.to_string())
    }

    fn get_snaps(&self, fs: &Fs) -> io::Result<HashMap<String, (String, String)>> {
        let mut zfs = self.pick_zfs(fs.role).to_mut();
        zfs.args([
            "get",
            "-Hpd",
            "1",
            "-t",
            "snapshot",
            "guid,creation",
            &fs.fs,
        ]);
        debug!("getting list of snapshots on {fs} using {zfs}",);
        let mut zfs = zfs.to_cmd();
        zfs.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        let mut zfs_process = zfs.spawn()?;

        // the output will have guids and creation on separate lines
        let zfs_stdout = zfs_process
            .stdout
            .take()
            .expect("stdout for process is piped");
        let _zfs_process = AutoKill::new(zfs_process);
        let zfs_stdout = BufReader::new(zfs_stdout);
        let zfs_lines = zfs_stdout.lines();

        let mut pre_snapshots = HashMap::new();
        let mut creation_counter = 0usize;

        for line in zfs_lines {
            let line = line?;
            let mut tsv = line.split('\t');
            let fs_at_snapshot = tsv.next().ok_or_else(|| {
                io::Error::other("expected zfs get to return at least three fields")
            })?;
            let Some(snapshot) = fs_at_snapshot
                .strip_prefix(fs.fs.as_ref())
                .and_then(|at_snapshot| at_snapshot.strip_prefix('@'))
                .map(|snapshot| snapshot.to_string())
            else {
                // skip anything that is not the specified fs
                warn!(
                    "getting snapshots for {fs} got filesystem snapshot {fs_at_snapshot} which is not of the form {}@SNAPSHOT",
                    fs.fs
                );
                continue;
            };
            let property = tsv.next().ok_or_else(|| {
                io::Error::other("expected zfs get to return at least three fields")
            })?;
            let value = tsv.next().ok_or_else(|| {
                io::Error::other("expected zfs get to return at least three fields")
            })?;
            if property == "guid" {
                let mapped_value = pre_snapshots.entry(snapshot).or_insert((None, None));
                mapped_value.0 = Some(value.to_string());
            } else if property == "creation" {
                // creation times are accurate to a second, but snapshots in the
                // same second are highly likely. The list command output is
                // ordered, so we add a running number to the creation timestamp
                // and make sure they are ordered correctly.
                // Note syncoid goes out of the way to number these from 0, but
                // we just do a running counter since we only care about the order
                // TODO: check if this messes with anything or do we really need
                // things orderered from 0
                let mapped_value = pre_snapshots.entry(snapshot).or_insert((None, None));
                mapped_value.1 = Some(format!("{value}{creation_counter:03}"));
                creation_counter += 1;
            } else {
                // skip anything that is not guid/creation
                warn!("getting snapshots for {fs} got property which is not one of guid,creation");
                continue;
            };
        }

        let mut snapshots = HashMap::new();
        for (snapshot, pair) in pre_snapshots {
            let Some(guid_creation) = pair.0.zip(pair.1) else {
                return Err(io::Error::other(format!(
                    "didn't get both guid and creation for {snapshot}"
                )));
            };
            snapshots.insert(snapshot, guid_creation);
        }

        Ok(snapshots)
    }

    fn new_sync_snap(&self, fs: &Fs) -> io::Result<String> {
        let hostname = hostname()?;
        let date = get_date();
        let snap_name = format!(
            "chithi_{}{hostname}_{date}",
            self.args.identifier.as_deref().unwrap_or_default()
        );
        // TODO skip creating snap if snap_name will be excluded
        let fs_snapshot = format!("{}@{snap_name}", fs.fs);
        if !self.args.dry_run {
            let mut zfs = self.pick_zfs(fs.role).to_mut();
            zfs.args(["snapshot", fs_snapshot.as_str()]);
            debug!("creating sync snapshot using zfs snapshot {fs_snapshot}...");
            let output = zfs.capture_stdout()?;

            if !output.status.success() {
                error!("failed to create snapshot {fs_snapshot}");
                exit(2);
            }
        } else {
            debug!("dry-run not running zfs snapshot {fs_snapshot}...");
        };
        Ok(snap_name)
    }

    // skip_sync_snapshot is set to true for these senarios
    // 1. fallback clone creation
    // 2. !bookmark && force-delete && delete successful (redo sync and skip snapshot creation beacuse it was already done)
    // 3. sync incremental fails with destination already exists && force delete (redo sync and skip snapshot creating because it was already done)
    /// Syncs a single dataset
    fn sync_dataset(&self, source: &Fs, target: &Fs, skip_sync_snapshot: bool) -> io::Result<()> {
        debug!("syncing source {} to target {}", source, target);
        let sync = self.get_zfs_value(source, "syncoid:sync");
        let sync = match sync {
            Ok(sync) => sync,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                warn!("Skipping dataset (dataset no longer exists): {source}");
                return Ok(());
            }
            Err(e) => {
                // syncoid sets exit code here, we're going to let caller decide
                return Err(e);
            }
        };

        if sync == "false" {
            info!("Skipping dataset (syncoid:sync=false): {source}...");
            return Ok(());
        } else if !["true", "-", ""].contains(&sync.as_str()) {
            // empty is handled the same as "-", hostnames "true" and "false" are
            // unsupported, and hostnames cannot start with "-" anyway (citation needed)
            let host_id = sys::hostname()?;
            if !sync.split(',').any(|x| x == host_id) {
                info!("Skipping dataset (syncoid:sync does not contain {host_id}): {source}...");
                return Ok(());
            }
        }

        // Check that zfs is not in recv
        if self.is_zfs_busy(target)? {
            warn!("Cannot sync now: {target} is already target of a zfs recv process");
            exit(1);
        }

        // Check if target exists
        let target_exists = self.target_exists(target)?;

        let recv_token = if target_exists && !self.args.no_resume {
            let recv_token = self.get_resume_token(target)?;
            if let Some(recv_token) = recv_token.as_ref() {
                debug!("got recv resume token: {recv_token}")
            };
            recv_token
        } else {
            None
        };

        'snapshot_checking: {
            // skip snapshot checking/creation in case of resumed recv
            // TODO figure out how to avoid the recursive call, and just handle
            // the resume first
            if recv_token.is_some() {
                break 'snapshot_checking;
            }
            let source_snaps = self.get_snaps(source)?;
            let target_snaps = if target_exists {
                Some(self.get_snaps(target)?)
            } else {
                None
            };

            if self.args.dump_snaps {
                // TODO: println might be more appropriate
                for (snapshot, (guid, creation)) in source_snaps.iter() {
                    info!(
                        "got snapshot {snapshot} with guid:{guid} creation:{creation} for {source}"
                    )
                }
                if let Some(target_snaps) = target_snaps.as_ref() {
                    for (snapshot, (guid, creation)) in target_snaps {
                        info!(
                            "got snapshot {snapshot} with guid:{guid} creation:{creation} for {target}"
                        )
                    }
                };
            }

            if !self.args.no_sync_snap && !skip_sync_snapshot {
                let new_sync_snap = self.new_sync_snap(source)?;
                // TODO remove debug
                debug!("created new sync snap {new_sync_snap}")
            }
        }

        // TODO
        debug!("syncing datasets is not implemented yet");

        Ok(())
    }
}

fn main() -> io::Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("warn"))
        .format_timestamp(None)
        .format_target(false)
        .init();

    // Build fs
    let source = Fs::new(args.source_host.as_deref(), &args.source, Role::Source);
    let target = Fs::new(args.target_host.as_deref(), &args.target, Role::Target);
    let (source_is_root, target_is_root) =
        get_is_roots(source.host, target.host, args.no_privilege_elevation);

    trace!("built fs");

    // TODO get ssh master

    // Build command targets
    let source_cmd_target = CmdTarget::new(source.host, &args.ssh_options);
    let target_cmd_target = CmdTarget::new(target.host, &args.ssh_options);
    let local_cmd_target = CmdTarget::new_local();

    trace!("built cmd targets");

    // Build command configs
    CmdConfig::check_ssh_if_needed(
        &source_cmd_target,
        &target_cmd_target,
        &local_cmd_target,
        args.no_command_checks,
    )?;
    let cmds = CmdConfig::new(
        &source_cmd_target,
        source_is_root,
        &target_cmd_target,
        target_is_root,
        &args,
    )?;

    trace!("built cmd configs");

    // Check if recursive
    if !args.recursive {
        cmds.sync_dataset(&source, &target, false)?
    } else {
        // Get child datasets
        let datasets = cmds.get_child_datasets(&source)?;
        if datasets.is_empty() {
            error!("no datasets found");
            exit(1);
        }
        for fs in datasets {
            // assume no clone handling
            let child_target = target.child_from_source(&source, &fs)?;
            cmds.sync_dataset(&fs, &child_target, false)?;
        }
    }

    Ok(())
}
