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

use libc::getuid;
use std::fmt::Display;

/// Check whether we should assume operations are as root
pub fn get_is_roots(source: Option<&str>, target: Option<&str>, bypass_root_check: bool) -> (bool, bool) {
    fn get_is_root(host: Option<&str>, bypass_root_check: bool) -> Option<bool> {
        host.and_then(|user| user.split_once('@'))
            .map(|(user, _)| bypass_root_check || user == "root")
    }
    let source_is_root = get_is_root(source, bypass_root_check);
    let target_is_root = get_is_root(target, bypass_root_check);
    match (source_is_root, target_is_root) {
        (Some(s), Some(t)) => (s, t),
        (s, t) => {
            let local_is_root = unsafe { getuid() == 0 };
            (s.unwrap_or(local_is_root), t.unwrap_or(local_is_root))
        }
    }
}

pub struct Fs<'args> {
    pub host: Option<&'args str>,
    pub fs: &'args str,
}

fn split_host_at_colon(host: &str) -> Option<(&str, &str)> {
    let mut iter = host.char_indices();
    while let Some((pos, c)) = iter.next() {
        if c == '/' {
            return None;
        }
        if c == ':' {
            return Some((&host[0..pos], iter.as_str()));
        }
    }
    None
}

impl<'args> Fs<'args> {
    pub fn new(host_opt: Option<&'args str>, fs: &'args str) -> Self {
        // There are three cases
        // 1. There's a separately provided hostname (which can also contain a
        // username), in which case we .
        // This provided hostname can be the empty string, see below.
        // 2. There's no seprately provided hostname, and there's a : in fs
        // before any '/' -> host:filesystem, user@host:filesystem, or
        // user@host:pool/filesystem
        // 3. If there's no seprately provided hostname, and there's no : in fs,
        // then fs is treated as a local filesystem
        //
        // Syncoid tries to figure out if : is part of a local pool name or if
        // it is used to separate the hostname from the filesystem, but we
        // don't. If there is a : in the poolname, then hostname must be set
        // separately.
        let (host, fs) = match host_opt {
            Some(host) => (if host.is_empty() { None } else { Some(host) }, fs),
            None => {
                if let Some((host, fs)) = split_host_at_colon(fs) {
                    (Some(host), fs)
                } else {
                    (None, fs)
                }
            }
        };
        Self { host, fs }
    }
}

impl<'args> Display for Fs<'args> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.fs)?;
        if let Some(host) = self.host {
            write!(f, "on {}", host)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_user_hosts() {
        let Fs { host, fs } = Fs::new(None, "user@host:pool");
        assert_eq!(host, Some("user@host"));
        assert_eq!(fs, "pool");
        let Fs { host, fs } = Fs::new(None, "user@host:pool/filesystem");
        assert_eq!(host, Some("user@host"));
        assert_eq!(fs, "pool/filesystem");
    }

    #[test]
    fn simple_hosts_without_users() {
        let Fs { host, fs } = Fs::new(None, "host:pool");
        assert_eq!(host, Some("host"));
        assert_eq!(fs, "pool");
        let Fs { host, fs } = Fs::new(None, "host:pool/filesystem");
        assert_eq!(host, Some("host"));
        assert_eq!(fs, "pool/filesystem");
        let Fs { host, fs } = Fs::new(None, "host:pool/filesystem:alsofs");
        assert_eq!(host, Some("host"));
        assert_eq!(fs, "pool/filesystem:alsofs");
    }

    #[test]
    fn simple_user_hosts_pool_fs_colon() {
        let Fs { host, fs } = Fs::new(None, "user@host:pool:alsopool");
        assert_eq!(host, Some("user@host"));
        assert_eq!(fs, "pool:alsopool");
        let Fs { host, fs } = Fs::new(None, "user@host:pool:alsopool/filesystem:alsofs");
        assert_eq!(host, Some("user@host"));
        assert_eq!(fs, "pool:alsopool/filesystem:alsofs");
    }

    #[test]
    fn empty_user_hosts() {
        let Fs { host, fs } = Fs::new(Some(""), "pool");
        assert_eq!(host, None);
        assert_eq!(fs, "pool");
        let Fs { host, fs } = Fs::new(Some(""), "pool/filesystem");
        assert_eq!(host, None);
        assert_eq!(fs, "pool/filesystem");
    }

    #[test]
    fn empty_user_hosts_pool_fs_colon() {
        let Fs { host, fs } = Fs::new(Some(""), "poolnothost:alsopool");
        assert_eq!(host, None);
        assert_eq!(fs, "poolnothost:alsopool");
        let Fs { host, fs } = Fs::new(Some(""), "poolnothost:alsopool/filesystem:alsofs");
        assert_eq!(host, None);
        assert_eq!(fs, "poolnothost:alsopool/filesystem:alsofs");
    }

    #[test]
    fn nonempty_user_hosts_pool_fs_colon() {
        let Fs { host, fs } = Fs::new(Some("user@host"), "poolnothost:alsopool");
        assert_eq!(host, Some("user@host"));
        assert_eq!(fs, "poolnothost:alsopool");
        let Fs { host, fs } = Fs::new(Some("user@host"), "poolnothost:alsopool/filesystem:alsofs");
        assert_eq!(host, Some("user@host"));
        assert_eq!(fs, "poolnothost:alsopool/filesystem:alsofs");
        let Fs { host, fs } = Fs::new(
            Some("user:wierduser@host:wierdhost"),
            "poolnothost:alsopool/filesystem:alsofs",
        );
        assert_eq!(host, Some("user:wierduser@host:wierdhost"));
        assert_eq!(fs, "poolnothost:alsopool/filesystem:alsofs");
    }
}
