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

use chobi::chithi::{Args, Cmd, CmdTarget, Fs, get_is_roots};
use clap::Parser;
use log::{debug, error};
use regex::Regex;
use std::{
    io::{self, BufRead, BufReader},
    process::{Stdio, exit},
};

struct CmdConfig<'args> {
    target_ps: Cmd<'args>,
    target_zfs: Cmd<'args>,
}

impl<'args> CmdConfig<'args> {
    pub fn new(
        target_cmd_target: &'args CmdTarget<'args>,
        target_is_root: bool,
        no_command_checks: bool,
    ) -> io::Result<Self> {
        let target_ps = Cmd::new(target_cmd_target, false, "ps", &["-Ao", "args="]);
        let target_zfs = Cmd::new(target_cmd_target, !target_is_root, "zfs", &[]);
        if !no_command_checks {
            target_ps.check_exists()?;
            target_zfs.check_exists()?;
        }
        Ok(Self {
            target_ps,
            target_zfs,
        })
    }

    fn is_zfs_busy(&self, fs: &Fs) -> io::Result<bool> {
        debug!(
            "checking to see if {fs} is already in zfs receive using {} ...",
            self.target_ps
        );

        let mut ps_cmd = self.target_ps.to_cmd();
        ps_cmd.stdout(Stdio::piped());
        let ps_process = ps_cmd.spawn()?;

        let ps_stdout = ps_process.stdout.expect("handle present");
        let ps_stdout = BufReader::new(ps_stdout);

        // TODO do we really need rexeg for this? What's the [^\/]* really doing?
        let re = {
            let fs_re = regex::escape(fs.fs);
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
        // TODO check fs escaping needs
        let mut target_zfs = self.target_zfs.to_cmd();
        target_zfs.args(["get", "-H", "name"]);
        target_zfs.arg(fs.fs);
        debug!("checking to see if target filesystem {fs} exists ...");
        target_zfs
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null());
        let output = target_zfs.output()?;
        // TODO why is this connect? "pool/foobar" begins with "pool/foo", but
        // are different file systems. is this a bug in syncoid?
        Ok(output.stdout[..].starts_with(fs.fs.as_bytes()) && output.status.success())
    }
}

fn main() -> io::Result<()> {
    let args = Args::parse();

    env_logger::init();

    let source = Fs::new(args.source_host.as_deref(), &args.source);
    let target = Fs::new(args.target_host.as_deref(), &args.target);

    // Build commands
    let (source_is_root, target_is_root) =
        get_is_roots(source.host, target.host, args.no_privilege_elevation);
    let source_cmd_target = CmdTarget::new(source.host, &args.ssh_options);
    let target_cmd_target = CmdTarget::new(target.host, &args.ssh_options);
    let local_cmd_target = CmdTarget::new_local();

    if (source_cmd_target.is_remote() || target_cmd_target.is_remote()) && !args.no_command_checks {
        let ssh_exists = Cmd::new(&local_cmd_target, false, "ssh", &[][..])
            .to_check()
            .output()?
            .status
            .success();
        if !ssh_exists {
            error!("there are remote targets, but ssh does not exist in local system");
            exit(1);
        }
    }

    let cmds = CmdConfig::new(&target_cmd_target, target_is_root, args.no_command_checks)?;

    // Check if zfs is busy
    if cmds.is_zfs_busy(&target)? {
        error!("target {target} is currently in zfs recv");
        exit(1);
    }

    // Check if target exists
    if !cmds.target_exists(&target)? {
        error!("target {target} does not exist");
        exit(1);
    }

    Ok(())
}
