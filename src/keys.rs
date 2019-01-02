use byteorder::{BigEndian, ByteOrder, WriteBytesExt};
use crate::proto::*;

const SEPARATOR: u8 = 0xAA;
const PREFIX_START: u8 = SEPARATOR;
const PREFIX_END: u8 = SEPARATOR+1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum KeyPart {
    Raw(Vec<u8>),
    Byte(u8),
    Constant(KeySpace),
    String(String),
    Int(u64),
}

impl From<u8> for KeyPart {
    fn from(value: u8) -> Self {
        KeyPart::Byte(value)
    }
}

impl From<&[u8]> for KeyPart {
    fn from(value: &[u8]) -> Self {
        KeyPart::Raw(value.to_vec())
    }
}

impl From<Vec<u8>> for KeyPart {
    fn from(value: Vec<u8>) -> Self {
        KeyPart::Raw(value)
    }
}

impl From<KeySpace> for KeyPart {
    fn from(value: KeySpace) -> Self {
        KeyPart::Constant(value)
    }
}

impl From<String> for KeyPart {
    fn from(value: String) -> Self {
        KeyPart::String(value)
    }
}

impl From<&str> for KeyPart {
    fn from(value: &str) -> Self {
        KeyPart::String(value.to_string())
    }
}

impl From<u64> for KeyPart {
    fn from(value: u64) -> Self {
        KeyPart::Int(value)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MetaKey(Vec<KeyPart>);

impl MetaKey {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add<K: Into<KeyPart>>(mut self, part: K) -> Self {
        if !self.0.is_empty() {
            self.0.push(KeyPart::Byte(SEPARATOR));
        }
        self.0.push(part.into());
        self
    }

    pub fn strip_prefix(&self, bytes: &[u8]) -> Option<Vec<u8>> {
        let root = self.into_key();
        for (i, byte) in root.iter().enumerate() {
            if bytes[i] != *byte {
                return None;
            }
        }
        if bytes[root.len()] != SEPARATOR {
            return None;
        }
        Some(bytes[root.len()+1..].to_vec())
    }
}

pub trait FromKey {
    fn from_key(key: &[u8]) -> Self;
}

impl FromKey for Vec<u8> {
    fn from_key(key: &[u8]) -> Self {
        key.to_vec()
    }
}

impl FromKey for String {
    fn from_key(key: &[u8]) -> Self {
        String::from_utf8_lossy(key).to_string()
    }
}

impl FromKey for u64 {
    fn from_key(key: &[u8]) -> Self {
        byteorder::BigEndian::read_u64(key)
    }
}

pub trait IntoKey {
    fn add_to_key(&self, key: &mut Vec<u8>);

    fn into_key(&self) -> Vec<u8> {
        let mut v = vec![];
        self.add_to_key(&mut v);
        v
    }

    fn into_prefix(&self) -> (Vec<u8>, Vec<u8>) {
        let k = self.into_key();
        let mut left = k.clone();
        left.push(PREFIX_START);
        let mut right = k.clone();
        right.push(PREFIX_END);
        (left, right)
    }
}

impl IntoKey for KeyPart {
    fn add_to_key(&self, key: &mut Vec<u8>) {
        match self {
            KeyPart::Constant(keyspace) => keyspace.add_to_key(key),
            KeyPart::String(s) => s.add_to_key(key),
            KeyPart::Int(n) => key.write_u64::<BigEndian>(*n).unwrap(),
            KeyPart::Raw(bytes) => bytes.add_to_key(key),
            KeyPart::Byte(byte) => key.push(*byte),
        }
    }
}

impl IntoKey for KeySpace {
    fn add_to_key(&self, key: &mut Vec<u8>) {
        key.extend(&self.to_prefix())
    }
}

// impl<T> IntoKey for T where T: AsRef<[u8]> {
//     fn add_to_key(&self, key: &mut Vec<u8>) {
//         key.extend(self.as_ref())
//     }
// }

impl IntoKey for String {
    fn add_to_key(&self, key: &mut Vec<u8>) {
        key.extend(self.as_bytes())
    }
}

impl IntoKey for Vec<u8> {
    fn add_to_key(&self, key: &mut Vec<u8>) {
        key.extend(self)
    }
}

impl IntoKey for &[u8] {
    fn add_to_key(&self, key: &mut Vec<u8>) {
        key.extend(*self)
    }
}

impl IntoKey for MetaKey {
    fn add_to_key(&self, key: &mut Vec<u8>) {
        self.0.iter().for_each(|part| part.add_to_key(key))
    }
}

impl<'a> IntoKey for &'a MetaKey {
    fn add_to_key(&self, key: &mut Vec<u8>) {
        (*self).add_to_key(key)
    }
}

macro_rules! tuple_impls {
    ($(
        $Tuple:ident {
            $(($idx:tt) -> $T:ident)+
        }
    )+) => {
        $(
            impl<$($T:IntoKey),+> IntoKey for ($($T,)+) {
                fn add_to_key(&self, key: &mut Vec<u8>) {
                    $(self.$idx.add_to_key(key);)+
                }
            }
        )+
    }
}

tuple_impls! {
    Tuple1 {
        (0) -> A
    }
    Tuple2 {
        (0) -> A
        (1) -> B
    }
    Tuple3 {
        (0) -> A
        (1) -> B
        (2) -> C
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum KeySpace {
    Index = 0x1,
    Peer = 0x2,
    Shard = 0x3,
    Id = 0x4,
    ShardState = 0x50,
}

impl KeySpace {
    fn to_prefix(&self) -> [u8; 4] {
        let mut v = [0; 4];
        byteorder::BigEndian::write_u32(&mut v, self.clone() as u32);
        v
    }

    pub fn as_key(&self) -> MetaKey {
        MetaKey::new().add(self.clone())
    }
}

pub fn build_index_key(index_name: &str) -> MetaKey {
    MetaKey::new().add(KeySpace::Index).add(index_name)
}

pub fn build_shard_prefix(index_name: &str) -> MetaKey {
    build_index_key(index_name).add(KeySpace::Shard)
}

pub fn build_shard_key(index_name: &str, shard_id: u64) -> MetaKey {
    build_shard_prefix(index_name).add(shard_id)
}

pub fn build_peer_key(peer: &Peer) -> MetaKey {
    MetaKey::new().add(KeySpace::Peer).add(peer.id)
}

pub fn id_key(part: impl Into<KeyPart>) -> MetaKey {
    MetaKey::new().add(KeySpace::Id).add(part)
}
