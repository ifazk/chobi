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

use chobi::chithi::{Args, Cmd, CmdTarget, Fs, Role, get_is_roots, sys};
use clap::Parser;
use log::{debug, error, info, trace, warn};
use regex::Regex;
use std::{
    io::{self, BufRead, BufReader, Write},
    process::{Stdio, exit},
};

struct CmdConfig<'args> {
    source_zfs: Cmd<'args>,
    target_ps: Cmd<'args>,
    target_zfs: Cmd<'args>,
}

impl<'args> CmdConfig<'args> {
    pub fn new(
        source_cmd_target: &'args CmdTarget<'args>,
        source_is_root: bool,
        target_cmd_target: &'args CmdTarget<'args>,
        target_is_root: bool,
        no_command_checks: bool,
    ) -> io::Result<Self> {
        let source_zfs = Cmd::new(source_cmd_target, !source_is_root, "zfs", &[]);
        let target_ps = Cmd::new(target_cmd_target, false, "ps", &["-Ao", "args="]);
        let target_zfs = Cmd::new(target_cmd_target, !target_is_root, "zfs", &[]);
        if !no_command_checks {
            source_zfs.check_exists()?;
            target_ps.check_exists()?;
            target_zfs.check_exists()?;
        }
        Ok(Self {
            source_zfs,
            target_ps,
            target_zfs,
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
        let ps_process = ps_cmd.spawn()?;

        let ps_stdout = ps_process.stdout.expect("handle present");
        let ps_stdout = BufReader::new(ps_stdout);

        // TODO do we really need rexeg for this? What's the [^\/]* really doing?
        let re = {
            let fs_re = regex::escape(fs.fs.as_ref());
            // TODO is the \n? needed if we're using .lines(), leaving it there since it's harmless
            let pattern = format!(r"zfs *(receive|recv)[^\/]*{}\n?$", fs_re);
            Regex::new(&pattern).expect("regex pattern should be correct")
        };

        for line in ps_stdout.lines() {
            let line = line?;
            if re.is_match(&line) {
                debug!("process {line} matches target {fs}");
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn target_exists(&self, fs: &Fs) -> io::Result<bool> {
        // TODO can we use get_zfs_value(fs, "name") instead?
        let mut target_zfs = self.target_zfs.to_cmd();
        target_zfs.args(["get", "-H", "name"]);
        target_zfs.arg(fs.fs.as_ref());
        debug!("checking if target filesystem {fs} exists ...");
        target_zfs
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        let output = target_zfs.output()?;
        if !output.status.success() {
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
        let mut source_zfs = self.source_zfs.to_cmd();
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
        debug!(
            "getting list of child datasets for {fs} using {} {} {}...",
            self.source_zfs,
            LIST_CHILD_DATASET.join(" "),
            fs.fs
        );
        source_zfs
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        let output = source_zfs.output()?;
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

    fn pick_zfs(&self, role: Role) -> &Cmd<'args> {
        match role {
            Role::Source => &self.source_zfs,
            Role::Target => &self.target_zfs,
        }
    }

    /// Returns ErrorKind::NotFound if dataset does not exist
    fn get_zfs_value(&self, fs: &Fs, property: &str) -> io::Result<String> {
        debug!("getting current value of {property} on {fs}");
        let mut zfs = self.pick_zfs(fs.role).to_cmd();
        zfs.args(["get", "-H", property, &fs.fs])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let output = zfs.output()?;
        // Output err since we're not inheriting
        io::stderr().write_all(&output.stderr)?;

        // handle does not exist
        const DOES_NOT_EXIST: &str = "dataset does not exist";
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

    /// Syncs a single dataset
    fn sync_dataset(&self, source: &Fs, target: &Fs) -> io::Result<()> {
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
        if self.is_zfs_busy(&target)? {
            warn!("Cannot sync now: {target} is already target of a zfs recv process");
            exit(1);
        }

        // Check if target exists
        let target_exists = self.target_exists(target)?;

        // TODO support --no-resume
        let _recv_token = if target_exists {
            let recv_token = self.get_resume_token(&target)?;
            if let Some(recv_token) = recv_token.as_ref() {
                debug!("got recv resume token: {recv_token}")
            };
            recv_token
        } else {
            None
        };

        // TODO
        debug!("syncing datasets is not implemented yet");

        Ok(())
    }
}

fn main() -> io::Result<()> {
    let args = Args::parse();

    env_logger::init();

    // Build fs
    let source = Fs::new(args.source_host.as_deref(), &args.source, Role::Source);
    let target = Fs::new(args.target_host.as_deref(), &args.target, Role::Target);
    let (source_is_root, target_is_root) =
        get_is_roots(source.host, target.host, args.no_privilege_elevation);

    trace!("built fs");

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
        args.no_command_checks,
    )?;

    trace!("built cmd configs");

    // Check if recursive
    if !args.recursive {
        cmds.sync_dataset(&source, &target)?
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
            cmds.sync_dataset(&fs, &child_target)?;
        }
    }

    Ok(())
}
