use fnv::FnvHashSet;

const MIN: u8 = 0;

#[repr(u8)]
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum AclOperation {
    Unknown = MIN,
    Any = 1,
    All = 2,
    Read = 3,
    Write = 4,
    Create = 5,
    Delete = 6,
    Alter = 7,
    Describe = 8,
    ClusterAction = 9,
    DescribeConfigs = 10,
    AlterConfigs = 11,
    IdempotentWrite = 12,
    CreateTokens = 13,
    DescribeTokens = MAX,
}

const MAX: u8 = 14;

impl From<u8> for AclOperation {
    fn from(value: u8) -> Self {
        match value {
            // SAFETY: AclOperation is valid in the range provided
            MIN..=MAX => unsafe { std::mem::transmute::<u8, AclOperation>(value) },
            _ => AclOperation::Unknown,
        }
    }
}

pub fn acl_from_bitfield(bits: i32) -> Option<FnvHashSet<AclOperation>> {
    if bits == i32::MIN {
        return None;
    }

    let mut set = FnvHashSet::default();

    for i in MIN..=MAX {
        if (bits >> 1) & 1 != 0 {
            set.insert((i as u8).into());
        }
    }

    Some(set)
}
