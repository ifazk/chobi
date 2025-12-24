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

pub mod chithi;

pub fn wip() {
    println!("This binary is not implemented yet");
    exit(1);
}

pub struct AutoKill {
    inner: std::process::Child,
}

impl AutoKill {
    pub fn new(child: std::process::Child) -> Self {
        Self { inner: child }
    }
}

impl Drop for AutoKill {
    fn drop(&mut self) {
        if self.inner.try_wait().as_ref().is_ok_and(Option::is_some) {
            return;
        }
        // issue kill and hope for the best and wait
        let _ = self.inner.kill();
        let _ = self.inner.wait();
    }
}
