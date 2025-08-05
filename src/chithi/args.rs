use clap::Parser;

mod bw;
use bw::Bytes;

/// ZFS snapshot replication tool
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Compresses data during transfer. Currently accepted options are gzip,
    /// pigz-fast, pigz-slow, zstd-fast, zstdmt-fast, zstd-slow, zstdmt-slow,
    /// lz4, xz, lzo & none
    #[arg(long, default_value = "lzo", value_name="FORMAT")]
    compress: String,

    /// Extra identifier which is included in the snapshot name. Can be used for
    /// replicating to multiple targets.
    #[arg(long, value_name="EXTRA")]
    identifier: Option<String>,

    /// Also transfers child datasets
    #[arg(short, long)]
    recursive: bool,

    /// Skips syncing of the parent dataset. Does nothing without '--recursive' option.
    #[arg(long, requires = "recursive")]
    skip_parent: bool,

    /// Bandwidth limit in bytes/kbytes/etc per second on the source transfer
    #[arg(long, value_parser = Bytes::try_from_str)]
    source_bwlimit: Option<Bytes>,

    /// Bandwidth limit in bytes/kbytes/etc per second on the target transfer
    #[arg(long, value_parser = Bytes::try_from_str)]
    target_bwlimit: Option<Bytes>,

    /// Specify the mbuffer size, please refer to mbuffer(1) manual page.
    #[arg(long, default_value = "16M", value_name="VALUE")]
    mbuffer_size: String,

    /// Configure how pv displays the progress bar
    #[arg(long, default_value = "-p -t -e -r -b", value_name="OPTIONS")]
    pv_options: String,

    /// Does not create new snapshot, only transfers existing
    #[arg(long)]
    no_sync_snap: bool,


    /// Use advanced options for zfs send (the arguments are filtered as needed), e.g. syncoid --sendoptions="Lc e" sets zfs send -L -c -e ...
    #[arg(long, value_name="OPTIONS")]
    sendoptions: Option<String>,

    /// Use advanced options for zfs receive (the arguments are filtered as needed), e.g. syncoid --recvoptions="ux recordsize o compression=lz4" sets zfs receive -u -x recordsize -o compression=lz4 ...
    #[arg(long, value_name="OPTIONS")]
    recvoptions: Option<String>,

    /// Passes OPTION to ssh for remote usage. Can be specified multiple times
    #[arg(short = 'o',long, value_name="OPTION")]
    sshoption: Vec<String>,

    /// Manually specifying source host (and user)
    #[arg(long)]
    source_host: Option<String>,

    /// Manually specifying target host (and user)
    #[arg(long)]
    target_host: Option<String>,

    source: String,

    target: String,
}