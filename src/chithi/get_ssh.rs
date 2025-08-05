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

pub struct GetSsh {
    pub host: Option<String>,
    pub fs: String,
    pub is_root: bool,
}

impl GetSsh {
    pub fn new(host_opt: Option<String>, fs: String) -> Self {
        // There are three cases
        // 1. There's a separately provided hostname (which can also containe a
        // username), in which case we just figure out if the fs is root or not.
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
                if let Some(idx) = fs.find(&[':', '/']) {
                    let bytes = fs.as_bytes();
                    let (head, tail) = bytes.split_at(idx);
                    if bytes[idx] == b':' {
                        (
                            Some(String::from_utf8_lossy(head).to_string()),
                            String::from_utf8_lossy(&tail[1..]).to_string(),
                        )
                    } else {
                        (None, fs)
                    }
                } else {
                    (None, fs)
                }
            }
        };
        let is_root = !fs.contains('/');
        Self { host, fs, is_root }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_user_hosts() {
        let GetSsh { host, fs, is_root } = GetSsh::new(None, String::from("user@host:pool"));
        assert_eq!(host, Some("user@host".to_string()));
        assert_eq!(fs, "pool");
        assert_eq!(is_root, true);
        let GetSsh { host, fs, is_root } =
            GetSsh::new(None, String::from("user@host:pool/filesystem"));
        assert_eq!(host, Some("user@host".to_string()));
        assert_eq!(fs, "pool/filesystem");
        assert_eq!(is_root, false);
    }

    #[test]
    fn simple_hosts_without_users() {
        let GetSsh { host, fs, is_root } = GetSsh::new(None, String::from("host:pool"));
        assert_eq!(host, Some("host".to_string()));
        assert_eq!(fs, "pool");
        assert_eq!(is_root, true);
        let GetSsh { host, fs, is_root } = GetSsh::new(None, String::from("host:pool/filesystem"));
        assert_eq!(host, Some("host".to_string()));
        assert_eq!(fs, "pool/filesystem");
        assert_eq!(is_root, false);
        let GetSsh { host, fs, is_root } = GetSsh::new(None, String::from("host:pool/filesystem:alsofs"));
        assert_eq!(host, Some("host".to_string()));
        assert_eq!(fs, "pool/filesystem:alsofs");
        assert_eq!(is_root, false);
    }

    #[test]
    fn simple_user_hosts_pool_fs_colon() {
        let GetSsh { host, fs, is_root } =
            GetSsh::new(None, String::from("user@host:pool:alsopool"));
        assert_eq!(host, Some("user@host".to_string()));
        assert_eq!(fs, "pool:alsopool");
        assert_eq!(is_root, true);
        let GetSsh { host, fs, is_root } = GetSsh::new(
            None,
            String::from("user@host:pool:alsopool/filesystem:alsofs"),
        );
        assert_eq!(host, Some("user@host".to_string()));
        assert_eq!(fs, "pool:alsopool/filesystem:alsofs");
        assert_eq!(is_root, false);
    }

    #[test]
    fn empty_user_hosts() {
        let GetSsh { host, fs, is_root } = GetSsh::new(Some("".to_string()), String::from("pool"));
        assert_eq!(host, None);
        assert_eq!(fs, "pool");
        assert_eq!(is_root, true);
        let GetSsh { host, fs, is_root } =
            GetSsh::new(Some("".to_string()), String::from("pool/filesystem"));
        assert_eq!(host, None);
        assert_eq!(fs, "pool/filesystem");
        assert_eq!(is_root, false);
    }

    #[test]
    fn empty_user_hosts_pool_fs_colon() {
        let GetSsh { host, fs, is_root } =
            GetSsh::new(Some("".to_string()), String::from("poolnothost:alsopool"));
        assert_eq!(host, None);
        assert_eq!(fs, "poolnothost:alsopool");
        assert_eq!(is_root, true);
        let GetSsh { host, fs, is_root } = GetSsh::new(
            Some("".to_string()),
            String::from("poolnothost:alsopool/filesystem:alsofs"),
        );
        assert_eq!(host, None);
        assert_eq!(fs, "poolnothost:alsopool/filesystem:alsofs");
        assert_eq!(is_root, false);
    }

    #[test]
    fn nonempty_user_hosts_pool_fs_colon() {
        let GetSsh { host, fs, is_root } =
            GetSsh::new(Some("user@host".to_string()), String::from("poolnothost:alsopool"));
        assert_eq!(host, Some("user@host".to_string()));
        assert_eq!(fs, "poolnothost:alsopool");
        assert_eq!(is_root, true);
        let GetSsh { host, fs, is_root } = GetSsh::new(
            Some("user@host".to_string()),
            String::from("poolnothost:alsopool/filesystem:alsofs"),
        );
        assert_eq!(host, Some("user@host".to_string()));
        assert_eq!(fs, "poolnothost:alsopool/filesystem:alsofs");
        assert_eq!(is_root, false);
        let GetSsh { host, fs, is_root } = GetSsh::new(
            Some("user:wierduser@host:wierdhost".to_string()),
            String::from("poolnothost:alsopool/filesystem:alsofs"),
        );
        assert_eq!(host, Some("user:wierduser@host:wierdhost".to_string()));
        assert_eq!(fs, "poolnothost:alsopool/filesystem:alsofs");
        assert_eq!(is_root, false);
    }
}
