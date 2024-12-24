use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};

use super::error::EncodeError;

pub struct AutoIncrementId(AtomicU64);

// 使用CAS自旋锁实现自增ID
impl AutoIncrementId {
    pub fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    pub fn next(&self) -> u64 {
        let mut id = self.0.load(Ordering::Relaxed);
        loop {
            let new_id = id + 1;
            match self
                .0
                .compare_exchange(id, new_id, Ordering::AcqRel, Ordering::Relaxed)
            {
                Ok(_) => return new_id,
                Err(x) => id = x,
            }
        }
    }
}

struct Key<'a> {
    father_id: u64,
    current_id: u64,
    field_key: &'a str,
}

impl<'a> Key<'a> {
    pub fn encode(&self) -> Bytes {
        let mut key = BytesMut::with_capacity(8 + 8 + self.field_key.len());
        key.extend_from_slice(&self.father_id.to_be_bytes());
        key.extend_from_slice(&self.current_id.to_be_bytes());
        key.extend_from_slice(self.field_key.as_bytes());
        key.freeze()
    }

    pub fn decode(key: &'a Bytes) -> Result<Self, EncodeError> {
        if key.len() <= 16 {
            return Err(EncodeError::InvalidLength);
        }
        let father_id = u64::from_be_bytes([
            key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
        ]);
        let current_id = u64::from_be_bytes([
            key[8], key[9], key[10], key[11], key[12], key[13], key[14], key[15],
        ]);
        let field_key = std::str::from_utf8(&key[16..])?;
        Ok(Self {
            father_id,
            current_id,
            field_key,
        })
    }
}

/// 用于存储JSON数据的节点，使用u8作为类型标识，使用u64作为数据索引
/// 0 - Null， 1 - Bool， 2 - Number， 3 - String， 4 - Array， 5 - Object
enum NodeValue {
    Null,
    Bool(bool),
    Number(f64),
    String(Bytes),
    Array(u64),
    Object(u64),
}

impl NodeValue {
    pub fn encode(&self) -> Bytes {
        match self {
            NodeValue::Null => Bytes::from_static(&[0]),
            NodeValue::Bool(b) => Bytes::from_iter([1, *b as u8]),
            NodeValue::Number(n) => {
                let mut bytes = BytesMut::with_capacity(9);
                bytes.extend_from_slice(&[2]);
                bytes.extend_from_slice(&n.to_be_bytes());
                bytes.freeze()
            }
            NodeValue::String(s) => {
                let mut bytes = BytesMut::with_capacity(1 + s.len());
                bytes.extend_from_slice(&[3]);
                bytes.extend_from_slice(s);
                bytes.freeze()
            }
            NodeValue::Array(id) => {
                let mut bytes = BytesMut::with_capacity(9);
                bytes.extend_from_slice(&[4]);
                bytes.extend_from_slice(&id.to_be_bytes());
                bytes.freeze()
            }
            NodeValue::Object(id) => {
                let mut bytes = BytesMut::with_capacity(9);
                bytes.extend_from_slice(&[5]);
                bytes.extend_from_slice(&id.to_be_bytes());
                bytes.freeze()
            }
        }
    }

    pub fn decode(data: &Bytes) -> Result<Self, EncodeError> {
        if data.is_empty() {
            return Err(EncodeError::InvalidLength);
        }
        match data[0] {
            0 => Ok(NodeValue::Null),
            1 => Ok(NodeValue::Bool(data[1] != 0)),
            2 => {
                if data.len() < 9 {
                    return Err(EncodeError::InvalidLength);
                }
                let n = f64::from_be_bytes([
                    data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
                ]);
                Ok(NodeValue::Number(n))
            }
            3 => Ok(NodeValue::String(data.slice(1..))),
            4 => {
                if data.len() < 9 {
                    return Err(EncodeError::InvalidLength);
                }
                let id = u64::from_be_bytes([
                    data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
                ]);
                Ok(NodeValue::Array(id))
            }
            5 => {
                if data.len() < 9 {
                    return Err(EncodeError::InvalidLength);
                }
                let id = u64::from_be_bytes([
                    data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
                ]);
                Ok(NodeValue::Object(id))
            }
            _ => Err(EncodeError::InvalidLength),
        }
    }
}
