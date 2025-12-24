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

use std::process::exit;
use std::thread::sleep;
use std::time::Duration;

pub mod chithi;

pub fn wip() {
    println!("This binary is not implemented yet");
    exit(1);
}

/// Automatically reaps the child's pid when it goes out of scope
/// It is better to call terminate and wait in happy paths, letting AutoKill
/// terminate a program on it's own hangs for at least 10ms to allow enough time
/// for the program to terminate before .
pub struct AutoKill {
    inner: std::process::Child,
    terminated: bool,
}

impl AutoKill {
    pub fn new(child: std::process::Child) -> Self {
        Self {
            inner: child,
            terminated: false,
        }
    }
    /// Terminate the program, if it hasn't been done already.
    /// Should not be called if there's reason to believe that the program has
    /// terminated already (e.g. it closed it's output file descriptor), and
    /// wait() should be called directly instead.
    pub fn terminate(&mut self) {
        if self.terminated {
            return;
        }
        self.terminated = true;
        if !self.is_reaped() {
            let pid = self.pid();
            let _ = unsafe { libc::kill(pid, libc::SIGTERM) };
        }
    }
    pub fn wait(&mut self) -> std::io::Result<std::process::ExitStatus> {
        self.terminated = true;
        self.inner.wait()
    }
    fn is_reaped(&mut self) -> bool {
        self.inner.try_wait().as_ref().is_ok_and(Option::is_some)
    }
    fn pid(&self) -> libc::pid_t {
        self.inner.id() as libc::pid_t
    }
}

impl Drop for AutoKill {
    fn drop(&mut self) {
        if self.is_reaped() {
            return;
        }
        // try terminate
        self.terminate();
        // give the process some time to terminate, anything else gets too fancy with separate code for FreeBSD and Linux
        sleep(Duration::from_millis(10));
        if self.is_reaped() {
            return;
        }
        // issue kill and hope for the best and wait
        let _ = self.inner.kill();
        let _ = self.inner.wait();
    }
}
