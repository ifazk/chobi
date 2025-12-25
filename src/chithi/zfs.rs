use std::collections::HashMap;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Creation {
    pub creation: u64,
    idx: usize,
}

impl Creation {
    pub fn new(creation: &str, idx: usize) -> Option<Self> {
        let creation = creation.parse::<u64>().ok()?;
        Some(Self { creation, idx })
    }
    pub fn fake_new(creation: u64, idx: usize) -> Self {
        Self { creation, idx }
    }
}

impl std::fmt::Display for Creation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:010}{:03}", self.creation, self.idx)
    }
}

/// A type that represents a snapshot or bookmark.
/// We abstract over the type, but in reality T is either String or &str
pub struct Snapshot<T> {
    pub name: T,
    pub guid: T,
    pub creation: Creation,
}

impl<T> Snapshot<T> {
    pub fn new(name: T, guid: T, creation: Creation) -> Self {
        Self {
            name,
            guid,
            creation,
        }
    }
}

impl Snapshot<String> {
    pub fn fake_newest(name: String) -> Self {
        const FAKE_NEW_SYNC_GUID: &str = "9999999999999999999";
        const FAKE_NEW_SYNC_CREATION: u64 = 9999999999;
        Self {
            name,
            guid: FAKE_NEW_SYNC_GUID.to_string(),
            creation: Creation::fake_new(FAKE_NEW_SYNC_CREATION, 0),
        }
    }
    pub fn list_to_map(list: &[Self]) -> HashMap<&str, SnapshotInfo<&str>> {
        list.iter()
            .map(|snapshot| (snapshot.name.as_str(), snapshot.into()))
            .collect::<HashMap<_, _>>()
    }
}

impl<'a> From<&'a Snapshot<String>> for Snapshot<&'a str> {
    fn from(value: &'a Snapshot<String>) -> Self {
        Self {
            name: &value.name,
            guid: &value.guid,
            creation: value.creation,
        }
    }
}

impl<'a, 'b> From<&'a Snapshot<&'b str>> for Snapshot<String> {
    fn from(value: &'a Snapshot<&'b str>) -> Self {
        Self {
            name: value.name.to_string(),
            guid: value.guid.to_string(),
            creation: value.creation,
        }
    }
}

pub enum IntermediateSource<'a> {
    Snapshot(Snapshot<&'a str>),
    Bookmark(Snapshot<&'a str>),
}

impl<'a> IntermediateSource<'a> {
    pub fn source(&self) -> String {
        match self {
            IntermediateSource::Snapshot(snapshot) => format!("@{}", snapshot.name),
            IntermediateSource::Bookmark(snapshot) => format!("#{}", snapshot.name),
        }
    }
    pub fn kind(&self) -> &'static str {
        match self {
            IntermediateSource::Snapshot(_) => "snapshot",
            IntermediateSource::Bookmark(_) => "bookmark",
        }
    }
}

/// SnapshotInfo is mostly useful for hashmaps. We abstract over the type, but
/// in reality T is either String or &str
pub struct SnapshotInfo<T> {
    pub guid: T,
    pub creation: Creation,
}

impl<T: Clone> From<Snapshot<T>> for SnapshotInfo<T> {
    fn from(value: Snapshot<T>) -> Self {
        Self {
            guid: value.guid.clone(),
            creation: value.creation,
        }
    }
}

impl<T: Clone> From<&Snapshot<T>> for SnapshotInfo<T> {
    fn from(value: &Snapshot<T>) -> Self {
        Self {
            guid: value.guid.clone(),
            creation: value.creation,
        }
    }
}

impl<'a> From<&'a Snapshot<String>> for SnapshotInfo<&'a str> {
    fn from(value: &'a Snapshot<String>) -> Self {
        Self {
            guid: value.guid.as_str(),
            creation: value.creation,
        }
    }
}
