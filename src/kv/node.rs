use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Bytes, BytesMut};

use super::error::EncodeError;
use anyhow::Result;

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

struct VariableSizedId {
    value: Vec<u8>,
}

impl VariableSizedId {
    pub fn bytes_len(&self) -> usize {
        self.value.len()
    }
    pub fn new(value: u64) -> Self {
        let mut size = 1;
        let mut v = value;
        while v >= 0x80 {
            size += 1;
            v >>= 7;
        }
        let mut bytes = Vec::with_capacity(size);
        let mut v = value;
        for _ in 0..size {
            let mut byte = (v & 0x7F) as u8;
            v >>= 7;
            if v != 0 {
                byte |= 0x80;
            }
            bytes.push(byte);
        }
        Self { value: bytes }
    }

    pub fn encode(&self) -> Bytes {
        let mut id = BytesMut::with_capacity(1 + self.value.len());
        id.extend_from_slice(&[self.value.len() as u8]);
        id.extend_from_slice(&self.value);
        id.freeze()
    }

    pub fn decode(id: &[u8]) -> Result<Self, EncodeError> {
        if id.is_empty() {
            return Err(EncodeError::InvalidLength);
        }
        let size = id[0];
        if id.len() < size as usize + 1 {
            return Err(EncodeError::InvalidLength);
        }
        Ok(Self {
            value: id[1..=size as usize].to_vec(),
        })
    }
}

struct Key<'a> {
    ids: Vec<VariableSizedId>,
    field_key: &'a str,
}

const SPLITOR: u8 = 0x00;

impl<'a> Key<'a> {
    pub fn encode(&self) -> Bytes {
        let mut size = 0;
        for id in &self.ids {
            size += id.bytes_len();
        }
        let mut key = BytesMut::with_capacity(size + 1 + self.field_key.len());
        for id in &self.ids {
            key.extend_from_slice(&id.encode());
        }
        key.extend_from_slice(&[SPLITOR]);
        key.extend_from_slice(self.field_key.as_bytes());
        key.freeze()
    }

    pub fn decode(key: &'a Bytes) -> Result<Self, EncodeError> {
        let mut ids = Vec::new();
        let mut remind = &key[0..key.len() - 1];
        while !remind.is_empty() && remind[0] != SPLITOR {
            let id_len = remind[0] as usize;

            let id = VariableSizedId::decode(&remind[0..=id_len])?;
            ids.push(id);
            remind = &remind[id_len + 1..];
        }
        if remind.is_empty() {
            return Err(EncodeError::InvalidLength);
        }
        let field_key = std::str::from_utf8(&remind[1..])?;
        Ok(Self { ids, field_key })
    }
}

/// 用于存储JSON数据的节点，使用u8作为类型标识，使用u64作为数据索引
/// 0 - Null， 1 - Bool， 2 - Number， 3 - String， 4 - Array， 5 - Object
pub enum NodeValue {
    Null,
    Bool(bool),
    Number(f64),
    String(Bytes),
    Array(Bytes),
    Object(Bytes),
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
                let mut bytes = BytesMut::with_capacity(1 + id.len());
                bytes.extend_from_slice(&[4]);
                bytes.extend_from_slice(id);
                bytes.freeze()
            }
            NodeValue::Object(id) => {
                let mut bytes = BytesMut::with_capacity(1 + id.len());
                bytes.extend_from_slice(&[5]);
                bytes.extend_from_slice(id);
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
                if data.len() <= 1 {
                    return Err(EncodeError::InvalidLength);
                }
                let id = data.slice(1..);
                Ok(NodeValue::Array(id))
            }
            5 => {
                if data.len() <= 9 {
                    return Err(EncodeError::InvalidLength);
                }
                let id = data.slice(1..);
                Ok(NodeValue::Object(id))
            }
            _ => Err(EncodeError::InvalidType),
        }
    }
}
