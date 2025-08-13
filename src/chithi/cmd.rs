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

use log::error;
use std::{
    fmt::Display, io, process::{exit, Command}
};

type SshOption = String;

pub struct Ssh<'args> {
    host: &'args str,
    options: &'args Vec<SshOption>,
}

impl<'args> Ssh<'args> {
    pub fn new(host: &'args str, options: &'args Vec<SshOption>) -> Self {
        Self { host, options }
    }
    pub fn to_cmd(&self) -> Command {
        let mut cmd = Command::new("ssh");
        for option in self.options {
            cmd.args(["-o", option]);
        }
        cmd.arg(self.host);
        cmd
    }
}

pub enum CmdTarget<'args> {
    Local,
    Remote { ssh: Ssh<'args> },
}

impl<'args> CmdTarget<'args> {
    pub fn new_local() -> Self {
        Self::Local
    }
    pub fn new(host: Option<&'args str>, ssh_options: &'args Vec<SshOption>) -> Self {
        host.map_or(Self::Local, |host| {
            let ssh = Ssh::new(host, ssh_options);
            Self::Remote { ssh }
        })
    }
    pub fn is_remote(&self) -> bool {
        match self {
            CmdTarget::Local => false,
            CmdTarget::Remote { .. } => true,
        }
    }
    fn make_cmd(&self, base: &'static str) -> Command {
        match self {
            CmdTarget::Local => Command::new(base),
            CmdTarget::Remote { ssh } => {
                let mut cmd = ssh.to_cmd();
                cmd.arg(base);
                cmd
            }
        }
    }
    pub fn on_str(&self) -> &str {
        match self {
            CmdTarget::Local => "",
            CmdTarget::Remote { .. } => " on ",
        }
    }
    pub fn host(&self) -> &str {
        match self {
            CmdTarget::Local => "",
            CmdTarget::Remote { ssh } => ssh.host,
        }
    }
}

impl<'args> Display for CmdTarget<'args> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CmdTarget::Local => {},
            CmdTarget::Remote { ssh } => {
                write!(f, "ssh ")?;
                for option in ssh.options {
                    write!(f, "-o {} ", option)?;
                };
            },
        };
        Ok(())
    }
}

pub struct Cmd<'args> {
    target: &'args CmdTarget<'args>,
    base: &'static str,
    args: &'args [&'args str],
}

impl<'args> Cmd<'args> {
    pub fn new(target: &'args CmdTarget<'args>, cmd: &'static str, args: &'args [&'args str]) -> Self {
        Self {
            target,
            base: cmd,
            args,
        }
    }

    pub fn to_cmd(&self) -> Command {
        let mut cmd = self.target.make_cmd(self.base);
        for arg in self.args {
            cmd.arg(arg);
        }
        cmd
    }

    pub fn to_check(&self) -> Command {
        // Like syncoid, use POSIX compatible command to check for program existence
        // TODO figure out if there's a RUST native way of doing this
        let mut cmd = self.target.make_cmd("command");
        cmd.arg("-v");
        cmd.arg(self.base);
        cmd
    }

    pub fn check_exists(&self) -> io::Result<()> {
        let exists = self.to_check().output()?.status.success();
        if !exists {
            error!(" does not exist in local system",);
            exit(1);
        }
        Ok(())
    }
}

impl<'args> Display for Cmd<'args> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.target, self.base)?;
        for arg in self.args {
            write!(f, " {}", arg)?;
        };
        Ok(())
    }
}