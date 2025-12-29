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

use chobi::AutoTerminate;
use chobi::chithi::sync_pipelines::OptionalCommands;
use chobi::chithi::sys::{get_syncoid_date, hostname};
use chobi::chithi::util::ReadableBytes;
use chobi::chithi::zfs::{Creation, IntermediateSource, Snapshot, SnapshotInfo};
use chobi::chithi::{Args, Cmd, CmdTarget, Fs, Role, Sequence, get_is_roots};
use clap::Parser;
use log::{debug, error, info, trace, warn};
use regex_lite::Regex;
use std::{
    cell::LazyCell,
    collections::{HashMap, HashSet},
    io::{self, BufRead, BufReader},
    process::Stdio,
    thread::sleep,
    time::Duration,
};

const DOES_NOT_EXIST: &str = "dataset does not exist";
const RESUME_ERROR_1: &str = "used in the initial send no longer exists";
thread_local! {
static RESUME_ERROR_2: LazyCell<Regex> = LazyCell::new(|| {
    Regex::new(r"incremental source [0-9xa-f]+ no longer exists")
        .expect("regex pattern should be correct")
});
}

struct CmdConfig<'args, 'target> {
    source_is_root: bool,
    target_is_root: bool,
    source_zfs: Cmd<'args, &'target [&'args str]>,
    target_ps: Cmd<'args, &'target [&'args str]>,
    target_zfs: Cmd<'args, &'target [&'args str]>,
    optional_cmds: OptionalCommands<'args>,
    _optional_features: HashSet<&'static str>,
    args: &'args Args,
    zfs_recv: Regex,
}

impl<'args, 'target> CmdConfig<'args, 'target> {
    pub fn new(
        source_cmd_target: &'args CmdTarget<'args>,
        source_is_root: bool,
        target_cmd_target: &'args CmdTarget<'args>,
        target_is_root: bool,
        local_cmd_target: &'args CmdTarget<'args>,
        args: &'args Args,
    ) -> io::Result<Self> {
        let source_zfs = Cmd::new(source_cmd_target, !source_is_root, "zfs", &[][..]);
        let target_ps = Cmd::new(target_cmd_target, false, "ps", &["-Ao", "args="][..]);
        let target_zfs = Cmd::new(target_cmd_target, !target_is_root, "zfs", &[][..]);
        if !args.no_command_checks {
            source_zfs.check_exists()?;
            target_ps.check_exists()?;
            target_zfs.check_exists()?;
            // sh is a posix standard, so we don't need to check
        }
        let optional_cmds =
            OptionalCommands::new(source_cmd_target, target_cmd_target, local_cmd_target, args)?;
        // TODO
        let optional_features = HashSet::new();
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
            source_is_root,
            target_is_root,
            source_zfs,
            target_ps,
            target_zfs,
            _optional_features: optional_features,
            optional_cmds,
            args,
            zfs_recv,
        })
    }

    pub fn check_ssh_if_needed(
        source_cmd_target: &mut CmdTarget,
        target_cmd_target: &mut CmdTarget,
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
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "ssh not found in local system",
                ));
            }
        }
        if source_cmd_target.is_remote() || target_cmd_target.is_remote() {
            if source_cmd_target.host() == target_cmd_target.host() {
                let source_control = source_cmd_target.make_control()?;
                target_cmd_target.set_control(source_control);
            } else {
                source_cmd_target.make_control()?;
                target_cmd_target.make_control()?;
            }
        }
        Ok(())
    }

    fn destroy_ssh_masters(
        source_cmd_target: &mut CmdTarget,
        target_cmd_target: &mut CmdTarget,
    ) -> io::Result<()> {
        if source_cmd_target.is_remote() || target_cmd_target.is_remote() {
            if source_cmd_target.host() == target_cmd_target.host() {
                target_cmd_target.set_control(None);
                source_cmd_target.destroy_control()?;
            } else {
                source_cmd_target.destroy_control()?;
                target_cmd_target.destroy_control()?;
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
        let _ps_process = AutoTerminate::new(ps_process);
        let ps_stdout = BufReader::new(ps_stdout);

        // if in recv lines look like
        // zfs receive <FLAGS|OPTIONS> <poolname>/<dataset>
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

    fn is_zfs_busy_for(&self, fss: &[Fs]) -> io::Result<bool> {
        debug!(
            "checking if already in zfs receive using {} ...",
            self.target_ps
        );

        let mut ps_cmd = self.target_ps.to_cmd();
        ps_cmd.stdout(Stdio::piped());
        let mut ps_process = ps_cmd.spawn()?;

        let ps_stdout = ps_process.stdout.take().expect("handle present");
        let _ps_process = AutoTerminate::new(ps_process);
        let ps_stdout = BufReader::new(ps_stdout);

        // if in recv lines look like
        // zfs receive <FLAGS|OPTIONS> <poolname>/<dataset>
        for line in ps_stdout.lines() {
            let line = line?;
            for fs in fss {
                if let Some(line_prefix) = line.strip_suffix(fs.fs.as_ref())
                    && self.zfs_recv.is_match(line_prefix)
                {
                    debug!("process {line} matches target {fs}");
                    return Ok(true);
                }
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
        let output = if self.args.debug {
            target_zfs.capture()?
        } else {
            target_zfs.to_cmd().output()?
        };
        if !output.status.success() {
            if output
                .stderr
                .windows(DOES_NOT_EXIST.len())
                .any(|x| x == DOES_NOT_EXIST.as_bytes())
            {
                return Ok(false);
            }
            error!("failed to check if target filesystem {fs} exists");
            return Err(io::Error::other("failed to check if target exists"));
        }
        // zfs get -H name only returns a single output, and we check if this
        // output matches the fs name. Syncoid does this using a prefix check,
        // and so do we.
        Ok(output.stdout[..].starts_with(fs.fs.as_bytes()))
    }

    fn get_resume_token(&self, fs: &Fs) -> io::Result<Option<String>> {
        // returning early without checking ErrorKind::NotFound is okay here
        // since there is a call to target_exists before
        let token = self.get_zfs_value(fs, &["receive_resume_token"])?;
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
            return Err(io::Error::other("failed to get child datasets"));
        }
        let mut children = Vec::new();
        let mut parent_processed = false;
        'outer: for line in output.stdout.lines() {
            let line = line?;
            let Some((name, origin)) = line.split_once("\t") else {
                return Err(io::Error::other(format!(
                    "expected tab separated name and origin, got {line}"
                )));
            };
            if !parent_processed {
                parent_processed = true;
                if self.args.skip_parent {
                    debug!("skipping parent dataset {name}");
                    continue;
                }
            }
            if !self.args.exclude_datasets.is_empty() {
                for r in &self.args.exclude_datasets {
                    if r.is_match(name) {
                        debug!("excluding dataset {name} because of --exclude-datasets={r}");
                        continue 'outer;
                    }
                }
            }
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
    fn get_zfs_value(&self, fs: &Fs, property: &[&str]) -> io::Result<String> {
        let property_nice = property.join(" ");
        let mut zfs = self.pick_zfs(fs.role).to_mut();
        zfs.args(["get", "-H"]);
        zfs.args(property);
        zfs.arg(&fs.fs);
        debug!("getting current value of {property_nice} on {fs} using {zfs}...");
        let output = if self.args.debug {
            zfs.capture()?
        } else {
            zfs.to_cmd().output()?
        };

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
            error!("failed to get property {property_nice} for {fs}");
            return Err(io::Error::other("failed to get zfs property"));
        };
        let stdout = output.stdout;
        let stdout = str::from_utf8(&stdout)
            .map_err(|e| {
                io::Error::other(format!(
                    "could not parse output of zfs -H {property_nice} {}: {e}",
                    fs.fs
                ))
            })?
            .trim();
        let value = stdout.split('\t').nth(2).ok_or_else(|| {
            io::Error::other(format!(
                "expected zfs -H {property_nice} {} to return at least three fields",
                fs.fs
            ))
        })?;
        Ok(value.to_string())
    }

    /// Will fail for dry-run sync-snaps, so special case that instead of using this
    fn get_send_size(
        &self,
        send_from: (Option<&str>, &str),
        send_to: Option<&str>,
    ) -> io::Result<u64> {
        let is_recv_token = send_from.0.is_some_and(|flag| flag == "-t");
        let send_options = if is_recv_token {
            self.args.send_options.filter_allowed(&['V', 'e'])
        } else {
            self.args
                .send_options
                .filter_allowed(&['L', 'V', 'R', 'X', 'b', 'c', 'e', 'h', 'p', 's', 'w'])
        };

        let mut from_to = Vec::new();
        if let Some(flag) = send_from.0 {
            from_to.push(flag);
        }
        from_to.push(send_from.1);
        if let Some(other) = send_to {
            from_to.push(other);
        }

        let mut source_zfs = self.source_zfs.to_mut();
        source_zfs.arg("send");
        source_zfs.args(send_options);
        source_zfs.arg("-nvP");
        source_zfs.args(&from_to);
        debug!("getting estimated transfer size from source using {source_zfs}...");
        let output = source_zfs.capture_stdout()?;
        if !output.status.success() {
            error!("failed to get estimated size for {}", from_to.join(" "));
            return Err(io::Error::other("failed to get estimated send size"));
        };
        let output = String::from_utf8_lossy(&output.stdout);
        // The last line of the multiline output is the size, but we need to
        // remove the human readable portions before parsing
        let send_size = output.trim().lines().last();
        let send_size = send_size
            .and_then(|send_size| {
                send_size
                    .rsplit_terminator(|c: char| !c.is_ascii_digit())
                    .next()
            })
            .map(str::trim)
            .and_then(|s| s.parse::<u64>().ok())
            .map(|send_size| if send_size < 4096 { 4096 } else { send_size }) // to avoid confusion with zero size pv, give minimum 4K size;
            .unwrap_or_default();
        debug!("got estimated transfer size of {send_size}");
        Ok(send_size)
    }

    fn run_sync_cmd(
        &self,
        _source: &Fs,
        send_from: (Option<&str>, &str),
        send_to: Option<&str>,
        target: &Fs,
        pv_size: u64,
    ) -> io::Result<()> {
        let pv_size_str = pv_size.to_string();
        let _disp_pv_size = ReadableBytes::from(pv_size);
        let send_options = if send_from.0 == Some("-t") {
            self.args
                .send_options
                .filter_allowed(&['P', 'V', 'e', 'v'][..])
        } else if send_from.1.contains('#') {
            self.args
                .send_options
                .filter_allowed(&['L', 'V', 'c', 'e', 'w'][..])
        } else {
            self.args.send_options.filter_allowed(
                &[
                    'L', 'P', 'V', 'R', 'X', 'b', 'c', 'e', 'h', 'p', 's', 'v', 'w',
                ][..],
            )
        };

        let mut recv_options = self
            .args
            .recv_options
            .filter_allowed(&['h', 'o', 'x', 'u', 'v']);

        // save state on interrupted stream
        if !self.args.no_resume {
            recv_options.push("-s");
        }
        // if rollbacks aren't allowed, disable forced recv
        if !self.args.no_rollback {
            recv_options.push("-F");
        }

        // TODO preserve properties (user properties are pretty much the only thing that needs escaping since they are arbitrary strings)
        // TODO preserve recordsize

        let send_cmd = {
            let mut args = Vec::from(["send"]);
            args.extend(send_options);
            if let Some(flag) = send_from.0 {
                args.push(flag);
            }
            args.push(send_from.1);
            if let Some(send_to) = send_to {
                args.push(send_to);
            }
            Cmd::new(&CmdTarget::Local, !self.source_is_root, "zfs", args)
        };
        let recv_cmd = {
            let mut args = Vec::from(["receive"]);
            args.extend(recv_options);
            args.push(&target.fs);
            Cmd::new(&CmdTarget::Local, !self.target_is_root, "zfs", args)
        };
        let pipelines = self
            .optional_cmds
            .build_sync_pipelines(send_cmd, recv_cmd, &pv_size_str);
        if self.is_zfs_busy(target)? {
            warn!("Cannot sync now: {target} is already target of a zfs recv process");
            return Err(io::Error::new(
                io::ErrorKind::ResourceBusy,
                "target is already in zfs recv",
            ));
        }
        if self.args.dry_run {
            if let (Some(local_pipeline), Some(other_pipeline)) = (&pipelines.1, &pipelines.2) {
                debug!(
                    "dry-run not running pipelines '{}' | '{}' | '{}'...",
                    pipelines.0, local_pipeline, other_pipeline
                );
            } else if let (None, Some(other_pipeline)) = (&pipelines.1, &pipelines.2) {
                debug!(
                    "dry-run not running pipelines '{}' | '{}'...",
                    pipelines.0, other_pipeline
                );
            } else {
                debug!("dry-run not running pipelines {}...", pipelines.0);
            }
            return Ok(());
        }
        self.optional_cmds.run_sync_pipelines(pipelines)
    }

    fn sync_resume(&self, source: &Fs, target: &Fs, recv_token: &str) -> io::Result<()> {
        let send_from = (Some("-t"), recv_token);
        let pv_size = self.get_send_size(send_from, None)?;
        info!(
            "Resuming interrupted zfs send/recv from {source} to {target} (~ {})",
            ReadableBytes::from(pv_size)
        );
        self.run_sync_cmd(source, send_from, None, target, pv_size)
    }

    fn reset_recv_state(&self, target: &Fs) -> io::Result<()> {
        let mut target_zfs = self.target_zfs.to_mut();
        target_zfs.args(["receive", "-A", &target.fs]);
        debug!("reset partial recv state of {target} using {target_zfs}...");
        match target_zfs.to_cmd().stderr(Stdio::inherit()).status() {
            Ok(exit) if exit.success() => Ok(()),
            Ok(fail) => {
                error!("resetting partial recv state failed with: {fail}");
                Err(io::Error::other("resetting recv state was unsuccessful"))
            }
            Err(e) => {
                error!("resetting partial recv state failed with: {e}");
                Err(io::Error::other(
                    "resetting recv state failed with an error",
                ))
            }
        }
    }

    /// similar to sync_full, but creates a clone
    fn sync_clone(&self, source: &Fs, target: &Fs, snapshot: &str) -> io::Result<()> {
        // openzfs docs: If the destination is a clone, the source may be the
        // origin snapshot, which must be fully specified (for example,
        // pool/fs@origin, not just @origin).
        let send_from = source.origin.as_deref().ok_or(io::Error::other(
            "clone sync failed because source didn't have an origin",
        ))?;
        let send_from = (Some("-i"), send_from);
        let send_to = format!("{}@{snapshot}", source.fs);
        let send_to = Some(send_to.as_str());
        let pv_size = self.get_send_size(send_from, send_to)?;
        if self.args.no_stream {
            info!(
                "--no-stream selected; sending newest full snapshot {} to new clone target filesystem {target} (~ {})",
                send_from.1,
                ReadableBytes::from(pv_size)
            );
        } else {
            info!(
                "Sending oldest full snapshot {} to new clone target filesystem {target} (~ {})",
                send_from.1,
                ReadableBytes::from(pv_size)
            );
        }
        match self.run_sync_cmd(source, send_from, send_to, target, pv_size) {
            Ok(()) => Ok(()),
            Err(e) => {
                // TODO this feels incorrect if the failure is because of a connection interruption
                info!("clone creation failed, trying ordinary replication as fallback: {e}");
                self.sync_full(source, target, snapshot)
            }
        }
    }

    fn sync_full(&self, source: &Fs, target: &Fs, snapshot: &str) -> io::Result<()> {
        let send_from = format!("{}@{snapshot}", source.fs);
        let pv_size = self.get_send_size((None, &send_from), None)?;
        if self.args.no_stream {
            info!(
                "--no-stream selected; sending newest full snapshot {send_from} to new target filesystem {target} (~ {})",
                ReadableBytes::from(pv_size)
            );
        } else {
            info!(
                "Sending oldest full snapshot {send_from} to new target filesystem {target} (~ {})",
                ReadableBytes::from(pv_size)
            );
        }
        self.run_sync_cmd(source, (None, &send_from), None, target, pv_size)
    }

    fn sync_intermidiate(
        &self,
        source: &Fs,
        target: &Fs,
        from_intermediate: IntermediateSource,
        to_snapshot: &str,
    ) -> io::Result<()> {
        let from_source = from_intermediate.source();
        let send_from = (Some("-i"), from_source.as_str());
        let send_to = format!("{}@{to_snapshot}", source.fs);
        let send_to = Some(send_to.as_str());
        let pv_size = self.get_send_size(send_from, send_to)?;
        info!(
            "Sending incremental intermediate snapshot {} .. {}@{to_snapshot} to new target filesystem {target} (~ {})",
            from_source,
            source.fs,
            ReadableBytes::from(pv_size)
        );
        self.run_sync_cmd(source, send_from, send_to, target, pv_size)
    }

    fn sync_incremental(
        &self,
        source: &Fs,
        target: &Fs,
        from_snapshot: &str,
        to_snapshot: &str,
    ) -> io::Result<()> {
        let send_from = (Some("-I"), from_snapshot);
        let send_to = format!("{}@{to_snapshot}", source.fs);
        let send_to = Some(send_to.as_str());
        let pv_size = self.get_send_size(send_from, send_to)?;
        info!(
            "Sending full incremental snapshot {from_snapshot} .. {}@{to_snapshot} to new target filesystem {target} (~ {})",
            source.fs,
            ReadableBytes::from(pv_size)
        );
        self.run_sync_cmd(source, send_from, send_to, target, pv_size)
    }

    // This is called in the stream case
    fn sync_incremental_or_fallback(
        &self,
        source: &Fs,
        target: &Fs,
        (intermediate_source, snapshots): (IntermediateSource, &[Snapshot<String>]),
    ) -> io::Result<()> {
        if !self.args.include_snaps.is_empty() || !self.args.exclude_snaps.is_empty() {
            info!(
                "--no-stream is omitted but snaps are filtered. Simulating -I with filtered snaps"
            );
            // should only be called with snapshots non-empty, but fail gracefully anyway
            if let Some(snapshot) = snapshots.first() {
                self.sync_intermidiate(source, target, intermediate_source, &snapshot.name)?;
            };
            for snapshots in snapshots.windows(2) {
                let from_snapshot = IntermediateSource::Snapshot((&snapshots[0]).into());
                let to_snapshot = &snapshots.last().expect("windows length 2").name;
                self.sync_intermidiate(source, target, from_snapshot, to_snapshot)?
            }
            Ok(())
        } else {
            match intermediate_source {
                IntermediateSource::Snapshot(snapshot) => {
                    let from_snapshot = snapshot.name;
                    let to_snapshot = &snapshots.last().expect("non-empty checked").name;
                    self.sync_incremental(source, target, from_snapshot, to_snapshot)
                }
                from_bookmark @ IntermediateSource::Bookmark(_) => {
                    let next_snapshot = snapshots.first().expect("non-empty checked");
                    self.sync_intermidiate(source, target, from_bookmark, &next_snapshot.name)?;
                    if snapshots.len() > 1 {
                        let last_snapshot = snapshots.last().expect("non-empty checked");
                        self.sync_incremental(
                            source,
                            target,
                            &next_snapshot.name,
                            &last_snapshot.name,
                        )?
                    };
                    Ok(())
                }
            }
        }
    }

    fn get_snaps(&self, fs: &Fs) -> io::Result<Vec<Snapshot<String>>> {
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
        let zfs_process = AutoTerminate::new(zfs_process);
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
                .map(|snapshot: &str| snapshot.to_string())
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
                let Some(creation) = Creation::new(value, creation_counter) else {
                    warn!("could not parse creation value {value} for {fs}");
                    continue;
                };
                mapped_value.1 = Some(creation);
                creation_counter += 1;
            } else {
                // skip anything that is not guid/creation
                warn!("getting snapshots for {fs} got property which is not one of guid,creation");
                continue;
            };
        }

        // We're done with the process, so drop it
        std::mem::drop(zfs_process);

        let mut snapshots = Vec::new();
        for (snapshot, pair) in pre_snapshots {
            let Some((guid, creation)) = pair.0.zip(pair.1) else {
                // This should not happen if zfs on the source behaves correctly
                return Err(io::Error::other(format!(
                    "didn't get both guid and creation for {snapshot}"
                )));
            };
            let snapshot = Snapshot::new(snapshot, guid, creation);
            // We do this check here so that we don't need to keep checking
            if self.snap_is_included(&snapshot.name) {
                snapshots.push(snapshot);
            }
        }

        snapshots.sort_by(
            |Snapshot {
                 name: name_x,
                 guid: _,
                 creation: creation_x,
             },
             Snapshot {
                 name: name_y,
                 guid: _,
                 creation: creation_y,
             }| {
                if creation_x.eq(creation_y) {
                    name_x.cmp(name_y)
                } else {
                    creation_x.cmp(creation_y)
                }
            },
        );

        Ok(snapshots)
    }

    // This is annoying but we have to duplicate most of the get_snaps code for
    // bookmarks. Syncoid does this, and so will we. The reason for this is the
    // error handling is different between the two cases. Some of the code can
    // be abstracted to share, but at this point I don't see the point.
    /// Throws empty vector when bookmarks feature is not available
    fn get_bookmarks(&self, fs: &Fs) -> io::Result<Vec<Snapshot<String>>> {
        let mut zfs = self.pick_zfs(fs.role).to_mut();
        zfs.args([
            "get",
            "-Hpd",
            "1",
            "-t",
            "bookmark",
            "guid,creation",
            &fs.fs,
        ]);
        debug!("getting list of bookmarks on {fs} using {zfs}",);
        let mut zfs = zfs.to_cmd();
        zfs.stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let zfs_output = zfs.output()?;

        // Error checking
        // Syncoid checks only the first line, unsure if that matters
        let zfs_stderr = String::from_utf8_lossy(&zfs_output.stderr);
        let zfs_err_first = zfs_stderr.lines().next().unwrap_or_default();
        if zfs_err_first.contains("invalid type")
            || zfs_err_first.contains("operation not applicable to datasets of this type")
        {
            return Ok(Vec::new());
        };

        // the output will have guids and creation on separate lines
        let zfs_stdout = String::from_utf8_lossy(&zfs_output.stdout);
        let zfs_lines = zfs_stdout.lines();

        let mut pre_snapshots = HashMap::new();
        let mut creation_counter = 0usize;

        for line in zfs_lines {
            let mut tsv = line.split('\t');
            let fs_pound_bookmark = tsv.next().ok_or_else(|| {
                io::Error::other("expected zfs get to return at least three fields")
            })?;
            let Some(bookmark) = fs_pound_bookmark
                .strip_prefix(fs.fs.as_ref())
                .and_then(|pound_bookmark| pound_bookmark.strip_prefix('#'))
                .map(|bookmark: &str| bookmark.to_string())
            else {
                // skip anything that is not the specified fs
                warn!(
                    "getting bookmarks for {fs} got filesystem bookmark {fs_pound_bookmark} which is not of the form {}#BOOKMARK",
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
                let mapped_value = pre_snapshots.entry(bookmark).or_insert((None, None));
                mapped_value.0 = Some(value.to_string());
            } else if property == "creation" {
                // creation times are accurate to a second, but snapshots in the
                // same second are highly likely. The list command output is
                // ordered, so we add a running number to the creation timestamp
                // and make sure they are ordered correctly.
                let mapped_value = pre_snapshots.entry(bookmark).or_insert((None, None));
                let Some(creation) = Creation::new(value, creation_counter) else {
                    warn!("could not parse creation value {value} for {fs}");
                    continue;
                };
                mapped_value.1 = Some(creation);
                creation_counter += 1;
            } else {
                // skip anything that is not guid/creation
                warn!("getting bookmarks for {fs} got property which is not one of guid,creation");
                continue;
            };
        }

        let mut snapshots = Vec::new();
        for (snapshot, pair) in pre_snapshots {
            let Some((guid, creation)) = pair.0.zip(pair.1) else {
                // This should not happen if zfs on the source behaves correctly
                return Err(io::Error::other(format!(
                    "didn't get both guid and creation for {snapshot}"
                )));
            };
            let snapshot = Snapshot::new(snapshot, guid, creation);
            // We do this check here so that we don't need to keep checking
            if self.snap_is_included(&snapshot.name) {
                snapshots.push(snapshot);
            }
        }

        snapshots.sort_by(
            |Snapshot {
                 name: name_x,
                 guid: _,
                 creation: creation_x,
             },
             Snapshot {
                 name: name_y,
                 guid: _,
                 creation: creation_y,
             }| {
                if creation_x.eq(creation_y) {
                    name_x.cmp(name_y)
                } else {
                    creation_x.cmp(creation_y)
                }
            },
        );

        Ok(snapshots)
    }

    fn dump_snaps_maybe<T: std::fmt::Display>(&self, source_or_target: &Fs, snaps: &[Snapshot<T>]) {
        if self.args.dump_snaps {
            for Snapshot {
                name,
                guid,
                creation,
            } in snaps
            {
                info!(
                    "got snapshot {name} with guid:{guid} creation:{creation} for {source_or_target}"
                )
            }
        }
    }

    fn snap_is_included(&self, snap_name: &str) -> bool {
        if !self.args.exclude_snaps.is_empty() {
            for exclude in self.args.exclude_snaps.iter() {
                if exclude.is_match(snap_name) {
                    debug!("excluded {snap_name} because of exclude pattern {exclude}");
                    return false;
                }
            }
        }
        // Empty means include everything
        if self.args.include_snaps.is_empty() {
            return true;
        }
        // Non empty means selectively include
        for include in self.args.include_snaps.iter() {
            if include.is_match(snap_name) {
                debug!("included {snap_name} because of exclude pattern {include}");
                return true;
            }
        }
        false
    }

    fn new_sync_snap(&self, fs: &Fs) -> io::Result<Option<String>> {
        let hostname = hostname()?;
        let date = get_syncoid_date();
        let snap_name = format!(
            "chithi_{}{hostname}_{date}",
            self.args.identifier.as_deref().unwrap_or_default()
        );
        if !self.snap_is_included(&snap_name) {
            return Ok(None);
        }
        let fs_snapshot = format!("{}@{snap_name}", fs.fs);
        if !self.args.dry_run {
            let mut zfs = self.pick_zfs(fs.role).to_mut();
            zfs.args(["snapshot", fs_snapshot.as_str()]);
            debug!("creating sync snapshot using {zfs}...");
            let output = zfs.capture_stdout()?;

            if !output.status.success() {
                error!("failed to create snapshot {fs_snapshot}");
                return Err(io::Error::other("failed to create snapshot"));
            }
        } else {
            debug!("dry-run not running zfs snapshot {fs_snapshot}...");
        };
        Ok(Some(snap_name))
    }

    fn oldest_sync_snap<'a>(
        &self,
        source_snaps: &'a [Snapshot<String>],
    ) -> Option<Snapshot<&'a str>> {
        let len = source_snaps.len();
        if len > 0 {
            source_snaps.first().map(|snapshot| snapshot.into())
        } else {
            None
        }
    }

    fn newest_sync_snap<'a>(&self, source_snaps: &'a [Snapshot<String>]) -> Option<&'a String> {
        let len = source_snaps.len();
        if len > 0 {
            source_snaps.get(len - 1).map(|snapshot| &snapshot.name)
        } else {
            None
        }
    }

    fn get_matching_snapshot<'a>(
        &self,
        sorted_source_snaps: &'a [Snapshot<String>],
        target_snaps: &HashMap<&str, SnapshotInfo<&str>>,
    ) -> Option<(IntermediateSource<'a>, &'a [Snapshot<String>])> {
        for (idx, source_snap) in sorted_source_snaps.iter().enumerate().rev() {
            if let Some(target_snap) = target_snaps.get(source_snap.name.as_str())
                && source_snap.guid == target_snap.guid
            {
                let match_t = (&sorted_source_snaps[idx]).into();
                let next = idx + 1;
                let rest = &sorted_source_snaps[next..];
                return Some((IntermediateSource::Snapshot(match_t), rest));
            }
        }
        None
    }

    fn get_matching_bookmark<'b, 'c>(
        &self,
        sorted_target_snaps: &[Snapshot<String>],
        sorted_source_bookmarks: &'b [Snapshot<String>],
        sorted_source_snaps: &'c [Snapshot<String>],
    ) -> Option<(IntermediateSource<'b>, &'c [Snapshot<String>])> {
        let bookmark_guids = sorted_source_bookmarks
            .iter()
            .map(|bookmark| bookmark.guid.as_str())
            .collect::<HashSet<_>>();
        let target_snap = sorted_target_snaps
            .iter()
            .rfind(|target_snap| bookmark_guids.contains(target_snap.guid.as_str()))?;
        let bookmark = sorted_source_bookmarks
            .iter()
            .rfind(|source_bookmark| source_bookmark.guid == target_snap.guid)
            .expect("guid was taken from bookmarks_guid");
        // technically can replace this linear search with a binary
        let source_snaps_later = sorted_source_snaps
            .iter()
            .enumerate()
            .find_map(|(idx, snap)| {
                (snap.creation.creation > bookmark.creation.creation).then_some(idx)
            })
            .map_or(&sorted_source_snaps[0..0], |idx| {
                &sorted_source_snaps[idx..]
            });
        Some((
            IntermediateSource::Bookmark(bookmark.into()),
            source_snaps_later,
        ))
    }

    fn prune_old_sync_snaps(
        &self,
        fs: &Fs,
        snapshots: &[Snapshot<String>],
        new_snapshot: &str,
        hostname: &str,
    ) -> io::Result<()> {
        let format_prefixes = self
            .args
            .prune_formats
            .iter()
            .map(|format| {
                format!(
                    "{format}_{}{hostname}",
                    self.args.identifier.as_deref().unwrap_or_default()
                )
            })
            .collect::<Vec<_>>();
        let mut snaps = Vec::new();
        for snap in snapshots {
            // Don't delete newly created snapshots
            if snap.name == new_snapshot {
                continue;
            }
            if format_prefixes
                .iter()
                .any(|format_prefix| snap.name.starts_with(format_prefix))
            {
                snaps.push(format!("{}@{}", fs.fs, snap.name));
            }
        }
        if snaps.is_empty() {
            return Ok(());
        }
        let zfs = self.pick_zfs(fs.role);
        let target = zfs.target();
        let zfs = zfs.to_mut().to_local();
        const MAX_PRUNE: usize = 10usize;
        for chunk in snaps.chunks(MAX_PRUNE) {
            let cmds = chunk
                .iter()
                .map(|snap| {
                    let mut zfs = zfs.to_mut();
                    zfs.args(["destroy", snap]);
                    zfs
                })
                .collect::<Vec<_>>();
            if let Some(sequence) = Sequence::from(target, cmds) {
                if chunk.len() == 1 {
                    // nicer debug message in the usual case
                    debug!(
                        "pruning 1 snapshot from {}: {}",
                        target.pretty_str(),
                        chunk[0]
                    );
                } else {
                    debug!(
                        "pruning {} snapshots from {}",
                        chunk.len(),
                        target.pretty_str()
                    );
                }
                let mut seq = sequence.to_cmd();
                let status = seq
                    .stdin(Stdio::null())
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .status()?;
                if !status.success() {
                    warn!("'{}' failed with: {status}", sequence);
                }
            };
        }
        Ok(())
    }

    // skip_sync_snapshot is set to true for these senarios
    // 1. fallback clone creation
    // 2. !bookmark && force-delete && delete successful (redo sync and skip snapshot creation beacuse it was already done)
    // 3. sync incremental fails with destination already exists && force delete (redo sync and skip snapshot creating because it was already done)
    /// Syncs a single dataset
    fn sync_dataset(&self, source: &Fs, target: &Fs, skip_sync_snapshot: bool) -> io::Result<()> {
        debug!("syncing source {} to target {}", source, target);
        let sync = self.get_zfs_value(source, &["syncoid:sync"]);
        let sync = match sync {
            Ok(sync) => sync,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // syncoid also does a replication count check here, throwing a
                // hard error if there haven't been any replications
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
            let host_id = hostname()?;
            if !sync.split(',').any(|x| x == host_id) {
                info!("Skipping dataset (syncoid:sync does not contain {host_id}): {source}...");
                return Ok(());
            }
        }

        // Check that zfs is not in recv
        if self.is_zfs_busy(target)? {
            warn!("Cannot sync now: {target} is already target of a zfs recv process");
            return Err(io::Error::new(
                io::ErrorKind::ResourceBusy,
                "target is already in zfs recv",
            ));
        }

        // Check if target exists
        let target_exists = self.target_exists(target)?;

        #[allow(unused_variables)]
        let recv_token = if target_exists && !self.args.no_resume {
            let recv_token = self.get_resume_token(target)?;
            if let Some(recv_token) = recv_token.as_ref() {
                debug!("got recv resume token: {recv_token}")
            };
            recv_token
        } else {
            None
        };

        // we handle any resumes first
        let mut resumed = false;
        if let Some(recv_token) = recv_token {
            let resume_res = self.sync_resume(source, target, &recv_token);
            if let Err(resume_err) = &resume_res
                && resume_err.kind() == io::ErrorKind::Other
                && let err_str = resume_err.to_string()
                && (err_str.contains(RESUME_ERROR_1)
                    || RESUME_ERROR_2.with(|regex| regex.is_match(&err_str)))
            {
                // reset and continue normal resume
                warn!(
                    "resetting partially receive state because the snapshot source no longer exists"
                );
                self.reset_recv_state(target)?;
            } else {
                resume_res?;
                resumed = true;
            }
        }

        let mut source_snaps = self.get_snaps(source)?;

        self.dump_snaps_maybe(source, &source_snaps);

        let mut created_new_sync_snap = None;
        let newest_sync_snapshot = if !self.args.no_sync_snap && !skip_sync_snapshot {
            let new_sync_snap = self.new_sync_snap(source)?;
            if let Some(new_snap_name) = new_sync_snap {
                // before returning, update source snaps
                created_new_sync_snap = Some(new_snap_name.clone());
                source_snaps.push(Snapshot::fake_newest(new_snap_name.clone()));
                new_snap_name
            } else {
                let Some(newest_snapshot) = self.newest_sync_snap(&source_snaps).cloned() else {
                    const NO_SNAP: &str = "New sync snapshot was not created or was filered out, and there were no snapshots on source";
                    error!("{}", NO_SNAP);
                    return Err(io::Error::other(NO_SNAP));
                };
                newest_snapshot
            }
        } else {
            let Some(newest_snapshot) = self.newest_sync_snap(&source_snaps).cloned() else {
                error!("No snapshots exist on source {source} and you asked for --no-sync-snap");
                return Err(io::Error::other(
                    "No snapshots exist on source and you asked for --no-sync-snap",
                ));
            };
            newest_snapshot
        };

        let Some(oldest_snapshot) = self.oldest_sync_snap(&source_snaps) else {
            // This should be dead code since getting newest_sync_snapshot
            // should have errored, but keeping it here defensively
            const NO_SNAP: &str =
                "Could not fetch oldest snapsshot for {source} or it was filered out";
            debug!("{}", NO_SNAP);
            return Err(io::Error::other(NO_SNAP));
        };

        let mut target_created = false;

        // Finally do syncs
        // If target does not exist, create it with inital full sync, and get target snaps
        let mut target_snaps_list = if !target_exists {
            if self.args.no_stream {
                debug!(
                    "target {target} does not exist, and --no-stream selected. Syncing newest snapshot for {source}"
                );
                // for --no-stream were done here
                // TODO (but not cleanup)
                if source.origin.is_some() && target.origin.is_some() {
                    return self.sync_clone(source, target, &newest_sync_snapshot);
                } else {
                    return self.sync_full(source, target, &newest_sync_snapshot);
                }
            }
            // Do initial sync from oldest snapshot, then do -I or -i to the newest
            if source.origin.is_some() && target.origin.is_some() {
                self.sync_clone(source, target, oldest_snapshot.name)?
            } else {
                self.sync_full(source, target, oldest_snapshot.name)?
            }
            target_created = true;
            vec![(&oldest_snapshot).into()]
        } else {
            self.get_snaps(target)?
        };
        let mut target_snaps_map = Snapshot::list_to_map(&target_snaps_list);
        let source_bookmarks_maybe;

        let matching_snapshot_and_later = {
            self.dump_snaps_maybe(target, &target_snaps_list);
            let matching_snap = self.get_matching_snapshot(&source_snaps, &target_snaps_map);
            let bookmark = if matching_snap.is_none() {
                let source_bookmarks = self.get_bookmarks(source)?;
                self.dump_snaps_maybe(source, &source_bookmarks);
                source_bookmarks_maybe = Some(source_bookmarks);
                self.get_matching_bookmark(
                    &target_snaps_list,
                    source_bookmarks_maybe.as_ref().expect("set to some above"),
                    &source_snaps,
                )
            } else {
                None
            };
            if let Some(matching_snap) = matching_snap.or(bookmark) {
                matching_snap
            } else {
                // Size check target fs to see if it was accidentally created before sync
                let target_size = if self.args.dry_run {
                    // target was created if it did not exist
                    (64 * 1024 * 1024).to_string()
                } else {
                    self.get_zfs_value(target, &["-p", "used"])?
                };
                let target_size = target_size.parse::<u64>().map_err(|e| {
                    io::Error::other(format!("parsing target size failed with {e}"))
                })?;
                if target_size < (64 * 1024 * 1024) {
                    error!(
                        "NOTE: Target dataset {target} is < 64MB used - did you mistakenly run `zfs create {}` on the target?",
                        target.fs
                    );
                    error!(
                        "NOTE: ZFS initial replication must be to a NON EXISTANT DATASET, which will then be CREATED by the initial replication process."
                    );
                    if self.args.force_delete {
                        error!(
                            "NOTE: Not deleting target even though --force-delete was passed. Please delete {target} manually."
                        );
                    }
                    return Err(io::Error::other(
                        "no matching snapshots and target dataset is too small",
                    ));
                };
                if self.args.force_delete && !target.fs.contains('/') {
                    // force delete is not possible for root file systems
                    error!(
                        "NOTE: Target {target} is a root dataset. Force delete is not possible for root datasets. Please delete {target} manually."
                    );
                    return Err(io::Error::other("not deleting root dataset"));
                } else if self.args.force_delete {
                    // destroy target fs and do initial sync from oldest snapshot, then do -I or -i to the newest
                    let mut target_zfs = self.target_zfs.to_mut();
                    target_zfs.args(["destroy", "-r", &target.fs]);
                    let output = target_zfs.to_cmd().output()?;
                    if !output.status.success() {
                        return Err(io::Error::other(format!(
                            "destroying target fs failed with\n{}\n{}",
                            String::from_utf8_lossy(&output.stdout),
                            String::from_utf8_lossy(&output.stderr)
                        )));
                    };
                    if self.args.no_stream {
                        // for --no-stream were done here
                        // TODO (but not cleanup)
                        return self.sync_full(source, target, &newest_sync_snapshot);
                    } else {
                        self.sync_full(source, target, oldest_snapshot.name)?;
                        target_snaps_list = vec![(&oldest_snapshot).into()];
                        target_snaps_map = Snapshot::list_to_map(&target_snaps_list);
                        let Some(matching_snapshots) =
                            self.get_matching_snapshot(&source_snaps, &target_snaps_map)
                        else {
                            error!(
                                "internal error, target snapshots list created from oldest snapshot, but getting matching list failed"
                            );
                            return Err(io::Error::other(
                                "building matching snapshots from oldest failed",
                            ));
                        };
                        matching_snapshots
                    }
                } else {
                    error!(
                        "NOTE: Target dataset {target} exists but has no snapshots matching with {source}"
                    );
                    error!(
                        "NOTE: Cowardly refusing to destroy existing target. You may pass the --force-delete flag to override this."
                    );
                    return Err(io::Error::other("no matching snapshots"));
                }
            }
        };

        if matching_snapshot_and_later.1.is_empty() {
            // message is not that meaningful if target was just created with the latest snapshot or we resumed to the latest stapshot
            if !target_created && !resumed {
                info!(
                    "no snapshots newer than {} in {source}. Target {target} up to date, nothing to do, not syncing.",
                    matching_snapshot_and_later.0.source()
                );
            }
            return Ok(());
        }

        // If we got this far, target exists now and has matching snapshot
        if self.args.no_stream {
            // for --no-stream we do a single -i stream to newest and finish
            self.sync_intermidiate(
                source,
                target,
                matching_snapshot_and_later.0,
                &matching_snapshot_and_later
                    .1
                    .last()
                    .expect("non-empty checked")
                    .name,
            )?
        } else {
            self.sync_incremental_or_fallback(source, target, matching_snapshot_and_later)?
        };

        if !self.args.keep_sync_snap
            && let Some(new_sync_snap) = created_new_sync_snap
        {
            let hostname = hostname()?;
            self.prune_old_sync_snaps(source, &source_snaps, &new_sync_snap, &hostname)?;
            self.prune_old_sync_snaps(target, &target_snaps_list, &new_sync_snap, &hostname)?;
        }

        Ok(())
    }
}

fn main() -> io::Result<()> {
    let args = Args::parse();

    // TODO: validate send and recv options and conflicts

    let default_log = if args.quiet {
        "error"
    } else if args.debug {
        "debug"
    } else {
        "info"
    };

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or(default_log))
        .format_timestamp(None)
        .format_target(false)
        .init();

    if let Some(max_delay) = args.max_delay_seconds {
        let max_delay = max_delay.get();
        let delay_seconds = rand::random_range(0..max_delay);
        info!("Delaying transfer for {delay_seconds} seconds");
        sleep(Duration::from_secs(delay_seconds as u64));
    }

    // Build fs
    let source = Fs::new(args.source_host.as_deref(), &args.source, Role::Source);
    let target = Fs::new(args.target_host.as_deref(), &args.target, Role::Target);
    let (source_is_root, target_is_root) =
        get_is_roots(source.host, target.host, args.no_privilege_elevation);

    debug!("source_is_root:{source_is_root}, target_is_root:{target_is_root}");

    trace!("built fs");

    // Build command targets
    let mut source_cmd_target = CmdTarget::new(
        source.host,
        args.ssh_cipher.as_deref(),
        args.ssh_config.as_deref(),
        args.ssh_identity.as_deref(),
        args.ssh_port.as_deref(),
        &args.ssh_options,
    );
    let mut target_cmd_target = CmdTarget::new(
        target.host,
        args.ssh_cipher.as_deref(),
        args.ssh_config.as_deref(),
        args.ssh_identity.as_deref(),
        args.ssh_port.as_deref(),
        &args.ssh_options,
    );
    let local_cmd_target = CmdTarget::new_local();

    trace!("built cmd targets");

    // Build command configs
    CmdConfig::check_ssh_if_needed(
        &mut source_cmd_target,
        &mut target_cmd_target,
        &local_cmd_target,
        args.no_command_checks,
    )?;
    let cmds = CmdConfig::new(
        &source_cmd_target,
        source_is_root,
        &target_cmd_target,
        target_is_root,
        &local_cmd_target,
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
            error!("no source datasets found");
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "no source datasets found",
            ));
        }
        let mut targets = Vec::new();
        // build targets
        for fs in &datasets {
            let child_target = target.child_from_source(&source, fs, args.clone_handling())?;
            targets.push(child_target)
        }
        // Do an early check for any busy children
        // I don't think syncoid does this and will start syncing parents and
        // fail when syncing children. We want to be a bit more resiliant to
        // cron or systemd starting serval instances of chithi on a timer.
        if args.recv_check_start() && cmds.is_zfs_busy_for(&targets)? {
            return Err(io::Error::new(
                io::ErrorKind::ResourceBusy,
                "one of the child datasets are in recv",
            ));
        }
        // Check if the parent exists before starting trasfer
        if args.skip_parent && !cmds.target_exists(&target)? {
            error!(
                "--skip-parent is set, but the target parent dataset does not exist. You may need to create {} manually",
                target
            );
            return Err(io::Error::new(
                io::ErrorKind::ResourceBusy,
                "--skip-parent is set, but the target parent dataset does not exist",
            ));
        }
        let mut deferred = Vec::new();
        for (fs, child_target) in datasets.iter().zip(targets.iter()) {
            if args.clone_handling()
                && child_target.origin.is_some()
                && targets
                    .iter()
                    .any(|target| Some(target.fs.as_ref()) == child_target.origin_dataset())
            {
                deferred.push((fs, child_target));
            } else {
                cmds.sync_dataset(fs, child_target, false)?;
            }
        }
        for (fs, child_target) in deferred {
            cmds.sync_dataset(fs, child_target, false)?;
        }
    }

    CmdConfig::destroy_ssh_masters(&mut source_cmd_target, &mut target_cmd_target)?;

    Ok(())
}
