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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VariableSizedId {
    value: Vec<u8>,
}

impl VariableSizedId {
    /// 创建一个 `VariableSizedId`
    /// 这里采用变长编码的方式，将 `u64` 按 7 bits 一组进行编码。
    pub fn new(value: u64) -> Self {
        let mut size = 1;
        let mut v = value;
        // 计算需要多少个字节
        while v >= 0x80 {
            size += 1;
            v >>= 7;
        }

        let mut bytes = Vec::with_capacity(size);
        let mut v = value;
        for _ in 0..size {
            let mut byte = (v & 0x7F) as u8; // 取低 7 位
            v >>= 7;
            // 如果还有后续字节，则在最高位打标记
            if v != 0 {
                byte |= 0x80;
            }
            bytes.push(byte);
        }
        Self { value: bytes }
    }

    /// 返回内部字节长度
    pub fn bytes_len(&self) -> usize {
        self.value.len()
    }

    /// 将内部存储的变长 ID 转为 `u64`
    pub fn to_u64(&self) -> Result<u64, EncodeError> {
        let mut result = 0u64;
        let mut shift = 0u32;

        for (i, &b) in self.value.iter().enumerate() {
            // 最多只允许 10 个字节：因为 2^64 最大需要 10 字节来表示（变长编码）
            if i >= 10 {
                return Err(EncodeError::Overflow);
            }

            let val = (b & 0x7F) as u64;
            result |= val << shift;

            // 如果最高位为 0，表示结束
            if (b & 0x80) == 0 {
                return Ok(result);
            }

            shift += 7;
        }

        // 如果循环结束还没 return，说明还需要更多字节（或数据有误）
        Err(EncodeError::InvalidLength)
    }

    /// 将 `VariableSizedId` 编码为二进制格式：先写入 1 字节长度，再写入实际数据
    pub fn encode(&self) -> Bytes {
        let mut id = BytesMut::with_capacity(1 + self.value.len());
        id.extend_from_slice(&[self.value.len() as u8]);
        id.extend_from_slice(&self.value);
        id.freeze()
    }

    /// 从二进制切片中解析出 `VariableSizedId`
    pub fn decode(id: &[u8]) -> Result<Self, EncodeError> {
        if id.is_empty() {
            return Err(EncodeError::InvalidLength);
        }
        let size = id[0] as usize;
        if id.len() < size + 1 {
            return Err(EncodeError::InvalidLength);
        }
        Ok(Self {
            value: id[1..=size].to_vec(),
        })
    }

    /// 与 `u64` 做加法，返回新的 `VariableSizedId`，若溢出则返回错误
    pub fn checked_add(&self, rhs: u64) -> Result<Self, EncodeError> {
        let lhs_val = self.to_u64()?;
        // 检查加法是否溢出
        let (sum, overflow) = lhs_val.overflowing_add(rhs);
        if overflow {
            return Err(EncodeError::Overflow);
        }
        // 重新编码
        Ok(Self::new(sum))
    }

    /// 与 `u64` 做加法，返回新的 `VariableSizedId`
    pub fn unchecked_add(&self, rhs: u64) -> Self {
        let lhs_val = self.to_u64().unwrap();
        let sum = lhs_val + rhs;
        Self::new(sum)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_small_value() {
        // 0x7F
        let vid = VariableSizedId::new(127);
        // 0x7F 一字节最值, 不需要高位标记
        assert_eq!(vid.value, vec![0x7F]);
        assert_eq!(vid.bytes_len(), 1);
    }

    #[test]
    fn test_new_large_value() {
        // 0x80
        let vid = VariableSizedId::new(128);
        // 128(0x80) 变长编码 => [0x80, 0x01]
        // 第一个字节: 0x80 表示低 7 位全是 0，加上最高位 1
        // 第二个字节: 0x01 表示剩余部分
        assert_eq!(vid.value, vec![0x80, 0x01]);
        assert_eq!(vid.bytes_len(), 2);
    }

    #[test]
    fn test_encode_decode() {
        let vid = VariableSizedId::new(300);
        let encoded = vid.encode();
        // encoded = [长度, vid.value...]
        // 300 => 0xAC 0x02 (172,2) 变长编码
        // vid.value = [0xAC, 0x02], 长度 2 => encoded = [2, 0xAC, 0x02]
        assert_eq!(encoded.len(), 3);
        assert_eq!(encoded[0], 2);
        assert_eq!(&encoded[1..], &[0xAC, 0x02]);

        let decoded = VariableSizedId::decode(&encoded).unwrap();
        assert_eq!(decoded.value, vid.value);
    }

    #[test]
    fn test_decode_invalid_length() {
        // 长度声明 2，但实际只有一个字节
        let encoded = [2, 0xAC];
        let result = VariableSizedId::decode(&encoded);
        assert_eq!(result, Err(EncodeError::InvalidLength));

        // 空数组
        let result = VariableSizedId::decode(&[]);
        assert_eq!(result, Err(EncodeError::InvalidLength));
    }

    #[test]
    fn test_to_u64() {
        let vid = VariableSizedId::new(0x1234_5678);
        let val = vid.to_u64().unwrap();
        assert_eq!(val, 0x1234_5678);

        // 针对 127 (单字节最大)
        let vid127 = VariableSizedId::new(127);
        assert_eq!(vid127.to_u64().unwrap(), 127);

        // 针对 128 (触发多字节)
        let vid128 = VariableSizedId::new(128);
        assert_eq!(vid128.to_u64().unwrap(), 128);
    }

    #[test]
    fn test_to_u64_overflow() {
        // 手动构造一个超过 10 字节的 vec，用于模拟错误
        let vid = VariableSizedId {
            value: vec![0xFF; 11], // 11 字节
        };
        let result = vid.to_u64();
        assert_eq!(result, Err(EncodeError::Overflow));
    }

    #[test]
    fn test_checked_add_no_overflow() {
        let vid = VariableSizedId::new(100);
        let vid_added = vid.checked_add(27).unwrap();
        assert_eq!(vid_added.to_u64().unwrap(), 127);
    }

    #[test]
    fn test_checked_add_overflow() {
        let vid = VariableSizedId::new(u64::MAX);
        // 再加 1 肯定溢出
        let result = vid.checked_add(1);
        assert_eq!(result, Err(EncodeError::Overflow));
    }

    #[test]
    fn test_checked_add_large_values() {
        let vid = VariableSizedId::new(u64::MAX - 10);
        let vid_added = vid.checked_add(10).unwrap();
        assert_eq!(vid_added.to_u64().unwrap(), u64::MAX);

        // 再多加一点就溢出了
        let result = vid_added.checked_add(1);
        assert_eq!(result, Err(EncodeError::Overflow));
    }
}
