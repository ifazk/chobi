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

use log::{debug, error};
use std::{
    borrow::Cow,
    fmt::Display,
    io,
    process::{Command, Output, Stdio, exit},
};

use crate::chithi::sys;

type SshOption = String;

#[derive(PartialEq, Eq)]
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

#[derive(PartialEq, Eq)]
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
    fn make_check(&self, base: &'static str) -> Command {
        // Like syncoid, use POSIX compatible command to check for program existence
        // TODO figure out if there's a RUST native way of doing this
        match self {
            CmdTarget::Local => {
                debug!("checking local command {base}");
                let mut cmd = Command::new("sh");
                cmd.arg("-c");
                cmd.arg(format!("command -v {base}"));
                cmd
            }
            CmdTarget::Remote { ssh } => {
                debug!("checking remote command {base} in {}", ssh.host);
                let mut cmd = ssh.to_cmd();
                cmd.args(["command", "-v", base]);
                cmd
            }
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
            CmdTarget::Local => {}
            CmdTarget::Remote { ssh } => {
                write!(f, "ssh ")?;
                for option in ssh.options {
                    write!(f, "-o {} ", option)?;
                }
                write!(f, "{} ", ssh.host)?;
            }
        };
        Ok(())
    }
}

pub struct Cmd<'args, T> {
    target: &'args CmdTarget<'args>,
    sudo: bool,
    base: &'static str,
    args: T,
}

fn escape_str<'a>(s: &'a str) -> Cow<'a, str> {
    if s.contains([
        '#', '\'', '"', ' ', '\t', '\n', '\r', '|', '&', ';', '<', '>', '(', ')', '$', '*', '?',
        '[', ']', '^', '!', '~', '%', '{', '}', //'=', ',', '-',
    ]) {
        let mut result = String::new();
        result.push('\''); // start quote
        for ch in s.chars() {
            if ch == '\'' {
                result.push('\''); // end quote
                result.push_str("\\'"); // single quote that's escaped
                result.push('\''); // restart quote
            } else {
                result.push(ch);
            }
        }
        result.push('\''); // end quote
        Cow::Owned(result)
    } else {
        Cow::Borrowed(s)
    }
}

impl<'args, 'cmd, T: AsRef<[&'cmd str]>> Cmd<'args, T> {
    pub fn new(target: &'args CmdTarget<'args>, sudo: bool, cmd: &'static str, args: T) -> Self {
        Self {
            target,
            sudo,
            base: cmd,
            args,
        }
    }

    pub fn to_cmd(&self) -> Command {
        let mut cmd = if self.sudo {
            let mut cmd = self.target.make_cmd("sudo");
            cmd.arg(self.base);
            cmd
        } else {
            self.target.make_cmd(self.base)
        };
        for arg in self.args.as_ref() {
            cmd.arg(arg);
        }
        cmd
    }

    pub fn to_mut(&self) -> Cmd<'args, Vec<&'cmd str>> {
        self.into()
    }

    pub fn to_check(&self) -> Command {
        self.target.make_check(self.base)
    }

    pub fn check_exists(&self) -> io::Result<()> {
        let exists = self.to_check().output()?.status.success();
        if !exists {
            let local = if self.target.is_remote() {
                ""
            } else {
                "local system"
            };
            error!("{} does not exist in {}{local}", self.base, self.target);
            exit(1);
        }
        Ok(())
    }

    /// Run command printing and catputuring output
    pub fn capture(&self) -> io::Result<Output> {
        let mut command = self.to_cmd();
        sys::capture(&mut command)
    }

    /// Run command inheriting stderr and capturing std output
    pub fn capture_stdout(&self) -> io::Result<Output> {
        let mut command = self.to_cmd();
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        command.output()
    }
}

impl<'args, 'cmd> Cmd<'args, Vec<&'cmd str>> {
    pub fn arg(&mut self, value: &'cmd str) {
        self.args.push(value);
    }
    pub fn args<T: AsRef<[&'cmd str]>>(&mut self, values: T) {
        for &value in values.as_ref() {
            self.args.push(value);
        }
    }
}

impl<'args, 'cmd, T: AsRef<[&'cmd str]>> Display for Cmd<'args, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sudo = if self.sudo { "sudo " } else { "" };
        write!(f, "{}{}{}", self.target, sudo, self.base)?;
        if self.target.is_remote() {
            for &arg in self.args.as_ref() {
                write!(f, " {}", escape_str(arg))?;
            }
            return Ok(());
        }
        for &arg in self.args.as_ref() {
            write!(f, " {}", arg)?;
        }
        Ok(())
    }
}

impl<'args, 'cmd, T: AsRef<[&'cmd str]>> From<&Cmd<'args, T>> for Cmd<'args, Vec<&'cmd str>> {
    fn from(
        Cmd {
            target,
            sudo,
            base,
            args,
        }: &Cmd<'args, T>,
    ) -> Self {
        Self {
            target,
            sudo: *sudo,
            base,
            args: args.as_ref().to_vec(),
        }
    }
}

/// Builds a pipeline of commands that will be passed as a script via ssh or sh -c
pub struct Pipeline<'args, T> {
    target: &'args CmdTarget<'args>,
    cmds: Vec<Cmd<'args, T>>,
}

impl<'args, T> Pipeline<'args, T> {
    pub fn new(target: &'args CmdTarget<'args>, cmd: Cmd<'args, T>) -> Self {
        Self {
            target,
            cmds: vec![cmd],
        }
    }
    pub fn add_cmd(&mut self, cmd: Cmd<'args, T>) {
        self.cmds.push(cmd);
    }
}

impl<'args, 'cmd, T: AsRef<[&'cmd str]>> Pipeline<'args, T> {
    pub fn to_cmd(&self) -> Command {
        match self.target {
            CmdTarget::Local => {
                let mut cmd = Command::new("sh");
                cmd.args(["-c", "--"]);
                if let Some(inner) = self.cmds.first() {
                    use std::fmt::Write;
                    let mut arg = String::new();
                    write!(arg, "{}", Self::escape_cmd(inner)).expect("formatting should not fail");
                    for inner in &self.cmds[1..] {
                        write!(arg, "| {}", Self::escape_cmd(inner))
                            .expect("formatting should not fail");
                    }
                    cmd.arg(arg);
                };
                cmd
            }
            CmdTarget::Remote { ssh } => {
                let mut cmd = Command::new("ssh");
                for option in ssh.options {
                    cmd.args(["-o", option]);
                }
                cmd.arg(ssh.host);
                // We don't have control over what shell is interpreting these
                // bytes. Zsh really doesn't like foo#bar, so let's escape those
                // and anthing that contains special shell characters.
                if let Some(first) = self.cmds.first() {
                    cmd.arg(Self::escape_cmd(first));
                    for other in &self.cmds[1..] {
                        cmd.arg("|");
                        cmd.arg(Self::escape_cmd(other));
                    }
                };
                cmd
            }
        }
    }

    fn escape_cmd(cmd: &Cmd<'args, T>) -> String {
        let mut result = String::new();
        if cmd.target.is_remote() {
            let ssh = format!("{}", cmd.target);
            result.push_str(&ssh);
        }
        if cmd.sudo {
            result.push_str("sudo ");
        }
        result.push_str(cmd.base);
        if cmd.target.is_remote() {
            // potentially needs double escape
            result.push(' ');
            for &arg in cmd.args.as_ref() {
                result.push(' ');
                let arg = escape_str(arg);
                result.push_str(&escape_str(&arg));
            }
            return result;
        }
        for &arg in cmd.args.as_ref() {
            result.push(' ');
            result.push_str(&escape_str(arg));
        }
        result
    }
}

impl<'args, 'cmd, T: AsRef<[&'cmd str]>> Display for Pipeline<'args, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.target {
            CmdTarget::Local => {
                write!(f, "sh -c -- ")?;
                if let Some(cmd) = self.cmds.first() {
                    write!(f, "{}", cmd)?;
                    for cmd in &self.cmds[1..] {
                        write!(f, "| {}", cmd)?;
                    }
                }
            }
            CmdTarget::Remote { ssh } => {
                write!(f, "ssh ")?;
                for option in ssh.options {
                    write!(f, "-o {} ", option)?;
                }
                write!(f, "{} ", ssh.host)?;
                if let Some(cmd) = self.cmds.first() {
                    write!(f, "{}", Self::escape_cmd(cmd))?;
                    for cmd in &self.cmds[1..] {
                        write!(f, "| {}", Self::escape_cmd(cmd))?;
                    }
                }
            }
        }
        Ok(())
    }
}
