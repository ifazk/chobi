use std::fmt::Display;

pub struct ReadableBytes(u64);

impl From<u64> for ReadableBytes {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl Display for ReadableBytes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const KB: u64 = 1024;
        const MB: u64 = 1024 * KB;
        const GB: u64 = 1024 * MB;

        if self.0 == 0 {
            write!(f, "UNKNOWN")?;
        } else if self.0 >= GB {
            let gb = self.0 as f64 / GB as f64;
            write!(f, "{gb:.1} GiB")?;
        } else if self.0 >= MB {
            let mb = self.0 as f64 / MB as f64;
            write!(f, "{mb:.1} MiB")?;
        } else {
            let kb = self.0 / KB;
            write!(f, "{} KiB", kb)?;
        }
        Ok(())
    }
}
