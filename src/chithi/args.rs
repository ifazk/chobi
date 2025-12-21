use crate::chithi::compress::Compress;
use crate::chithi::send_recv_opts::{OptionsLine, Opts};
use bw::Bytes;
use clap::Parser;
use regex_lite::Regex;

mod bw;

/// ZFS snapshot replication tool
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Compresses data during transfer. Currently accepted options are gzip,
    /// pigz-fast, pigz-slow, zstd-fast, zstdmt-fast, zstd-slow, zstdmt-slow,
    /// lz4, xz, lzo & none
    #[arg(long, default_value = "lzo", value_name = "FORMAT", value_parser = Compress::try_from_str)]
    pub compress: Compress,

    /// Extra identifier which is included in the snapshot name. Can be used for
    /// replicating to multiple targets.
    #[arg(long, value_name = "EXTRA", value_parser = validate_identifier)]
    pub identifier: Option<String>,

    /// Also transfers child datasets
    #[arg(short, long)]
    pub recursive: bool,

    /// Skips syncing of the parent dataset. Does nothing without '--recursive' option.
    #[arg(long, requires = "recursive")]
    pub skip_parent: bool,

    /// Bandwidth limit in bytes/kbytes/etc per second on the source transfer
    #[arg(long, value_parser = Bytes::try_from_str)]
    pub source_bwlimit: Option<Bytes>,

    /// Bandwidth limit in bytes/kbytes/etc per second on the target transfer
    #[arg(long, value_parser = Bytes::try_from_str)]
    pub target_bwlimit: Option<Bytes>,

    /// Specify the mbuffer size, please refer to mbuffer(1) manual page.
    #[arg(long, default_value = "16M", value_name = "VALUE")]
    pub mbuffer_size: String,

    /// Configure how pv displays the progress bar
    #[arg(long, default_value = "-p -t -e -r -b", value_name = "OPTIONS")]
    pub pv_options: String,

    /// Replicates using newest snapshot instead of intermediates
    #[arg(long)]
    pub no_stream: bool,

    /// Does not create new snapshot, only transfers existing
    #[arg(long)]
    pub no_sync_snap: bool,

    /// Does not rollback snapshots on target (it probably requires a readonly target)
    #[arg(long)]
    pub no_rollback: bool,

    /// Exclude specific datasets that match the given regular expression. Can be specified multiple times.
    #[arg(long, value_name = "REGEX")]
    pub exclude_datasets: Vec<Regex>,

    /// Exclude specific snapshots that match the given regular expression. Can be specified multiple times. If a snapshot matches both exclude-snaps and include-snaps patterns, then it will be excluded.
    #[arg(long, value_name = "REGEX")]
    pub exclude_snaps: Vec<Regex>,

    /// Only include snapshots that match the given regular expression. Can be specified multiple times. If a snapshot matches both exclude-snaps and include-snaps patterns, then it will be excluded.
    #[arg(long, value_name = "REGEX")]
    pub include_snaps: Vec<Regex>,

    /// Use advanced options for zfs send (the arguments are filtered as needed), e.g. chithi --send-options="Lc e" sets zfs send -L -c -e ...
    #[arg(long, value_name = "OPTIONS", value_parser = Opts::try_from_str, default_value_t)]
    pub send_options: Opts<Vec<OptionsLine<String>>>,

    /// Use advanced options for zfs receive (the arguments are filtered as needed), e.g. chithi --recv-options="ux recordsize o compression=lz4" sets zfs receive -u -x recordsize -o compression=lz4 ...
    #[arg(long, value_name = "OPTIONS", value_parser = Opts::try_from_str, default_value_t)]
    pub recv_options: Opts<Vec<OptionsLine<String>>>,

    /// Passes OPTION to ssh for remote usage. Can be specified multiple times
    #[arg(short = 'o', long = "sshoption", value_name = "OPTION")]
    pub ssh_options: Vec<String>,

    /// Prints out a lot of additional information during a chithi run. Logs overridden by --quiet and RUST_LOG environment variable
    #[arg(long)]
    pub debug: bool,

    /// Supresses non-error output and progress bars. Logs overridden by RUST_LOG environment variable
    #[arg(long)]
    pub quiet: bool,

    /// Dumps a list of snapshots during the run
    #[arg(long)]
    pub dump_snaps: bool,

    /// Passes OPTION to ssh for remote usage. Can be specified multiple times
    #[arg(long)]
    pub no_command_checks: bool,

    /// Do a dry run, without modifying datasets and pools
    #[arg(long)]
    pub dry_run: bool,

    /// Don't use the ZFS resume feature if available
    #[arg(long)]
    pub no_resume: bool,

    /// Don't try to recreate clones on target. Clone handling is done by
    /// deferring child datasets that are clones to a second pass of syncing, so
    /// this flag is not meaningful without the --recursive flag.
    #[arg(long, requires = "recursive")]
    pub no_clone_handling: bool,

    /// Bypass the root check, for use with ZFS permission delegation
    #[arg(long)]
    pub no_privilege_elevation: bool,

    /// Manually specifying source host (and user)
    #[arg(long)]
    pub source_host: Option<String>,

    /// Manually specifying target host (and user)
    #[arg(long)]
    pub target_host: Option<String>,

    /// Remove target datasets recursively if there are no matching snapshots/bookmarks (also overwrites conflicting named snapshots)
    #[arg(long)]
    pub force_delete: bool,

    /// Prevents the recursive recv check at the start of the sync
    #[arg(long, requires = "recursive")]
    pub no_recv_check_start: bool,

    pub source: String,

    pub target: String,
}

impl Args {
    pub fn clone_handling(&self) -> bool {
        !self.no_clone_handling
    }
    pub fn recv_check_start(&self) -> bool {
        !self.no_recv_check_start
    }
}

fn validate_identifier(value: &str) -> Result<String, &'static str> {
    fn invalid_char(c: char) -> bool {
        !(c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == ':' || c == '.')
    }
    if value.contains(invalid_char) {
        Err("extra indentifier contains invalid chars!")
    } else {
        Ok(value.to_string())
    }
}
