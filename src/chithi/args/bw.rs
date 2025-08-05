#[derive(Debug, Clone, Copy)]
pub enum BytesSuffix {
    K,
    M,
    G,
    T,
}

impl TryFrom<char> for BytesSuffix {
    type Error = &'static str;

    fn try_from(value: char) -> Result<Self, Self::Error> {
        match value {
            'k' | 'K' => Ok(BytesSuffix::K),
            'm' | 'M' => Ok(BytesSuffix::M),
            'g' | 'G' => Ok(BytesSuffix::G),
            't' | 'T' => Ok(BytesSuffix::T),
            _ => Err("could not parse size suffix"),
        }
    }
}

const SUFFIXES: [char; 8] = ['k', 'K', 'm', 'M', 'g', 'G', 't', 'T'];

#[derive(Debug, Clone, Copy)]
pub struct Bytes {
    pub size: u64,
    pub suffix: Option<BytesSuffix>,
}

impl Bytes {
    pub fn try_from_str(value: &str) -> Result<Self, &'static str> {
        let mut num = String::new();
        let mut suffix = None;
        let mut done = false;
        for c in value.chars() {
            if done {
                return Err("expected size value to end after suffix but found more characters");
            };

            if c.is_ascii_digit() {
                num.push(c);
            } else if SUFFIXES.contains(&c) {
                suffix = Some(
                    c.try_into()
                        .expect("suffixes array should mirror the try_from"),
                );
                done = true;
            } else {
                return Err("unrecognized character when parsing size");
            }
        }
        let limit = num
            .parse::<u64>()
            .expect("should be able to parse ascii digits into size limit");
        if limit == 0 {
            return Err("size limit cannot be zero");
        };
        Ok(Self { size: limit, suffix })
    }
}
