use std::fmt::Display;

#[derive(Debug, Clone, Default)]
pub enum Compress {
    Gzip,
    PigzFast,
    PigzSlow,
    ZstdFast,
    ZstdmtFast,
    ZstdSlow,
    ZstdmtSlow,
    Xz,
    #[default]
    Lzo,
    Lz4,
    None,
}

pub struct CompressCommand {
    pub base: &'static str,
    pub args: &'static [&'static str],
    pub decompress: &'static str,
    pub decompress_args: &'static [&'static str],
}

impl Compress {
    pub fn to_str(&self) -> &'static str {
        match self {
            Compress::Gzip => "gzip",
            Compress::PigzFast => "pigz-fast",
            Compress::PigzSlow => "pigz-slow",
            Compress::ZstdFast => "zstd-fast",
            Compress::ZstdmtFast => "zstdmt-fast",
            Compress::ZstdSlow => "zstd-slow",
            Compress::ZstdmtSlow => "zstdmt-slow",
            Compress::Xz => "xz",
            Compress::Lzo => "lzo",
            Compress::Lz4 => "lz4",
            Compress::None => "none",
        }
    }
    pub fn try_from_str(value: &str) -> Result<Self, &'static str> {
        match value {
            "gzip" => Ok(Compress::Gzip),
            "pigz-fast" => Ok(Compress::PigzFast),
            "pigz-slow" => Ok(Compress::PigzSlow),
            "zstd-fast" => Ok(Compress::ZstdFast),
            "zstdmt-fast" => Ok(Compress::ZstdmtFast),
            "zstd-slow" => Ok(Compress::ZstdSlow),
            "zstdmt-slow" => Ok(Compress::ZstdmtSlow),
            "xz" => Ok(Compress::Xz),
            "lzo" => Ok(Compress::Lzo),
            "lz4" => Ok(Compress::Lz4),
            "none" => Ok(Compress::None),
            _ => Err("unsupported compress format"),
        }
    }
    pub fn to_cmd(&self) -> Option<CompressCommand> {
        match self {
            Compress::Gzip => Some(CompressCommand {
                base: "gzip",
                args: &["-3"][..],
                decompress: "zcat",
                decompress_args: &[][..],
            }),
            Compress::PigzFast => Some(CompressCommand {
                base: "pigz",
                args: &["-3"][..],
                decompress: "pigz",
                decompress_args: &["-dc"][..],
            }),
            Compress::PigzSlow => Some(CompressCommand {
                base: "pigz",
                args: &["-9"][..],
                decompress: "pigz",
                decompress_args: &["-dc"][..],
            }),
            Compress::ZstdFast => Some(CompressCommand {
                base: "zstd",
                args: &["-3"][..],
                decompress: "zstd",
                decompress_args: &["-dc"][..],
            }),
            Compress::ZstdmtFast => Some(CompressCommand {
                base: "zstdmt",
                args: &["-3"][..],
                decompress: "zstdmt",
                decompress_args: &["-dc"][..],
            }),
            Compress::ZstdSlow => Some(CompressCommand {
                base: "zstd",
                args: &["-19"][..],
                decompress: "zstd",
                decompress_args: &["-dc"][..],
            }),
            Compress::ZstdmtSlow => Some(CompressCommand {
                base: "zstdmt",
                args: &["-19"][..],
                decompress: "zstdmt",
                decompress_args: &["-dc"][..],
            }),
            Compress::Xz => Some(CompressCommand {
                base: "xz",
                args: &[][..],
                decompress: "xz",
                decompress_args: &["-d"][..],
            }),
            Compress::Lzo => Some(CompressCommand {
                base: "lzop",
                args: &[][..],
                decompress: "lzop",
                decompress_args: &["-dfc"][..],
            }),
            Compress::Lz4 => Some(CompressCommand {
                base: "lz4",
                args: &[][..],
                decompress: "lz4",
                decompress_args: &["-dc"][..],
            }),
            Compress::None => None,
        }
    }
}

impl Display for Compress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_str())
    }
}
