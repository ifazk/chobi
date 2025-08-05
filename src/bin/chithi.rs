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

use clap::Parser;
use log::debug;
use regex::Regex;
use std::{
    io::{self, BufRead, BufReader},
    process::{Command, Stdio},
};
use chobi::chithi::Args;

struct CmdConfig {
    ssh_cmd: String,
    ps_cmd: String,
}

impl CmdConfig {
    fn is_zfs_busy(&self, rhost: String, fs: String, _is_root: bool) -> io::Result<bool> {
        let mut ps_cmd: Command;
        let mut on_rhost = String::new();
        if !rhost.is_empty() {
            ps_cmd = Command::new(&self.ssh_cmd);
            ps_cmd.arg(&rhost);
            ps_cmd.arg(&self.ps_cmd);
            on_rhost.push_str(" on ");
            on_rhost.push_str(&rhost);
        } else {
            ps_cmd = Command::new(&self.ps_cmd);
        };
        ps_cmd.args(["-Ao", "args="]);

        // TODO do the debug! without this string
        let ps_cmd_str = {
            let mut str = ps_cmd.get_program().to_string_lossy().to_string();
            for arg in ps_cmd.get_args() {
                str.push(' ');
                str.push_str(&arg.to_string_lossy());
            }
            str
        };
        debug!(
            "checking to see if {fs}{on_rhost} is already in zfs receive using {} ...",
            ps_cmd_str
        );

        ps_cmd.stdout(Stdio::piped());
        let ps_process = ps_cmd.spawn()?;

        let ps_stdout = ps_process.stdout.expect("handle present");
        let ps_stdout = BufReader::new(ps_stdout);

        let re = {
            let fs_re = regex::escape(&fs);
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
}

impl Default for CmdConfig {
    fn default() -> Self {
        Self {
            ssh_cmd: "ssh".to_string(),
            ps_cmd: "ps".to_string(),
        }
    }
}

fn main() {
    let args = Args::parse();

    env_logger::init();
}
