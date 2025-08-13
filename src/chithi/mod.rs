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

// Note: All modules are expected to be private, with types re-exported if they
// need to be public

mod args;
mod fs;
mod cmd;

pub use args::Args;
pub use fs::{Fs, get_is_roots};
pub use cmd::CmdTarget;
pub use cmd::Cmd;
