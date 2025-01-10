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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    pub fn checked_plus(&self, rhs: u64) -> Result<Self, EncodeError> {
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
    pub fn unchecked_plus(&self, rhs: u64) -> Self {
        let lhs_val = self.to_u64().unwrap();
        let sum = lhs_val + rhs;
        Self::new(sum)
    }

    pub fn unchecked_minus(&self, rhs: u64) -> Self {
        let lhs_val = self.to_u64().unwrap();
        let sum = lhs_val - rhs;
        Self::new(sum)
    }
}

/// 从字节流中读取一个完整的 Varint，返回 (VariableSizedId, 消费了多少字节)
pub fn read_variable_sized_id(data: &[u8]) -> Result<(VariableSizedId, usize), EncodeError> {
    let mut bytes = Vec::new();
    let mut consumed = 0;

    for &b in data {
        bytes.push(b);
        consumed += 1;
        // 如果最高位=0，则这是最后一个字节
        if (b & 0x80) == 0 {
            return Ok((VariableSizedId { value: bytes }, consumed));
        }
        // 防止过长溢出
        if consumed > 10 {
            return Err(EncodeError::Overflow);
        }
        if consumed >= data.len() {
            return Err(EncodeError::InvalidLength);
        }
    }

    Err(EncodeError::InvalidLength)
}

const SPLITOR: u8 = 0x00;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyIndex {
    Id(VariableSizedId),
    Field(Bytes),
    Root,
}

impl KeyIndex {
    pub fn is_root(&self) -> bool {
        match &self {
            Self::Root => true,
            _ => false,
        }
    }

    pub fn is_id(&self) -> bool {
        match &self {
            Self::Id(_) => true,
            _ => false,
        }
    }

    pub fn is_field(&self) -> bool {
        match &self {
            Self::Field(_) => true,
            _ => false,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        match self {
            KeyIndex::Id(id) => {
                let mut bytes = vec![0x02];
                bytes.extend_from_slice(&id.value);
                bytes
            }
            KeyIndex::Field(field) => {
                let mut bytes = vec![0x01];
                bytes.extend_from_slice(field.as_ref());
                bytes
            }
            KeyIndex::Root => vec![0x03],
        }
    }

    pub fn decode(data: &[u8]) -> Result<Self, EncodeError> {
        if data.is_empty() {
            return Err(EncodeError::InvalidLength);
        }
        match data[0] {
            0x01 => {
                let field =
                    std::str::from_utf8(&data[1..]).map_err(|e| EncodeError::InvalidUtf8(e))?;
                Ok(KeyIndex::Field(Bytes::copy_from_slice(field.as_bytes())))
            }
            0x02 => {
                let id = VariableSizedId::decode(&data[1..])?;
                Ok(KeyIndex::Id(id))
            }
            0x03 => Ok(KeyIndex::Root),
            _ => Err(EncodeError::InvalidType),
        }
    }
}

/// 这里的 Key 包含：多个 ID 和一个 Field Key, ids = <super node id> + <current id>
#[derive(Debug, Clone)]
pub struct Key {
    pub ids: Vec<VariableSizedId>,
    pub field_key: KeyIndex,
}

impl Key {
    /// 编码：将所有 ID 依次写入（每个都是自描述的 varint），然后写分隔符，再写 field_key
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // 写 n 个 self.ids
        for id in &self.ids {
            buf.extend_from_slice(&id.unchecked_plus(1).value);
        }

        // 写分隔符
        buf.extend_from_slice([SPLITOR].as_ref());

        // 写 field_key
        buf.extend_from_slice(&self.field_key.encode());

        buf
    }

    /// 解码：反复读 varint，直到遇到分隔符；剩余部分为 field_key
    pub fn decode(bytes: &[u8]) -> Result<Self, EncodeError> {
        let mut ids = Vec::new();
        let mut offset = 0;

        // 循环读 varint
        while offset < bytes.len() {
            // 如果碰到分隔符，就停止
            if bytes[offset] == SPLITOR {
                offset += 1;
                break;
            }

            // 否则解析一个完整的 varint
            let (vid, consumed) = read_variable_sized_id(&bytes[offset..])?;
            offset += consumed;
            ids.push(vid.unchecked_minus(1));
        }

        // 现在 offset 指向 field_key 或超出边界
        if offset > bytes.len() {
            return Err(EncodeError::InvalidLength);
        }

        let field_key_bytes = &bytes[offset..];
        let field_key_str = KeyIndex::decode(field_key_bytes)?;

        Ok(Self {
            ids,
            field_key: field_key_str,
        })
    }

    pub fn super_id_prefix(&self) -> Vec<u8> {
        // 只取前 n-1 个 id + SPLITOR
        // 1. 计算长度
        let len = self
            .ids
            .iter()
            .take(self.ids.len() - 1)
            .map(|id| id.bytes_len())
            .sum::<usize>();
        let mut buf = Vec::with_capacity(len + 1);
        // 2. 写入前 n-1 个 id
        for id in self.ids.iter().take(self.ids.len() - 1) {
            buf.extend_from_slice(&id.value);
        }
        // 3. 写入分隔符
        buf.push(SPLITOR);
        buf
    }

    pub fn sub_key(&self, id: VariableSizedId, index: KeyIndex) -> Self {
        // 1. 生成新的 ids
        let mut ids = self.ids.clone();
        ids.push(id);
        // 2. 生成新的 field_key
        let field_key = index;
        Self { ids, field_key }
    }
}
/// 0 - Null， 1 - Bool， 2 - Number，3 NumberI, 4, NumberU 5 - String， 6 - Array， 7 - Object
pub enum NodeValue {
    Null,
    Bool(bool),
    Number(f64),
    NumberI(i64),
    NumberU(u64),
    String(Bytes),
    Array,
    Object,
}

impl NodeValue {
    pub fn is_object(&self) -> bool {
        matches!(self, NodeValue::Object)
    }
    pub fn is_array(&self) -> bool {
        matches!(self, NodeValue::Array)
    }
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
            NodeValue::NumberI(i) => {
                let mut bytes = BytesMut::with_capacity(9);
                bytes.extend_from_slice(&[3]);
                bytes.extend_from_slice(&i.to_be_bytes());
                bytes.freeze()
            }
            NodeValue::NumberU(u) => {
                let mut bytes = BytesMut::with_capacity(9);
                bytes.extend_from_slice(&[4]);
                bytes.extend_from_slice(&u.to_be_bytes());
                bytes.freeze()
            }
            NodeValue::String(s) => {
                let mut bytes = BytesMut::with_capacity(1 + s.len());
                bytes.extend_from_slice(&[3]);
                bytes.extend_from_slice(s);
                bytes.freeze()
            }
            NodeValue::Array => {
                let mut bytes = BytesMut::with_capacity(1);
                bytes.extend_from_slice(&[4]);
                bytes.freeze()
            }
            NodeValue::Object => {
                let mut bytes = BytesMut::with_capacity(1);
                bytes.extend_from_slice(&[5]);
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
            3 => {
                if data.len() < 9 {
                    return Err(EncodeError::InvalidLength);
                }
                let n = i64::from_be_bytes([
                    data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
                ]);
                Ok(NodeValue::NumberI(n))
            }
            4 => {
                if data.len() < 9 {
                    return Err(EncodeError::InvalidLength);
                }
                let n = u64::from_be_bytes([
                    data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8],
                ]);
                Ok(NodeValue::NumberU(n))
            }
            5 => Ok(NodeValue::String(data.slice(1..))),
            6 => Ok(NodeValue::Array),
            7 => Ok(NodeValue::Object),
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
        let vid_added = vid.checked_plus(27).unwrap();
        assert_eq!(vid_added.to_u64().unwrap(), 127);
    }

    #[test]
    fn test_checked_add_overflow() {
        let vid = VariableSizedId::new(u64::MAX);
        // 再加 1 肯定溢出
        let result = vid.checked_plus(1);
        assert_eq!(result, Err(EncodeError::Overflow));
    }

    #[test]
    fn test_checked_add_large_values() {
        let vid = VariableSizedId::new(u64::MAX - 10);
        let vid_added = vid.checked_plus(10).unwrap();
        assert_eq!(vid_added.to_u64().unwrap(), u64::MAX);

        // 再多加一点就溢出了
        let result = vid_added.checked_plus(1);
        assert_eq!(result, Err(EncodeError::Overflow));
    }

    #[test]
    fn test_encode_decode_no_ids() {
        let k = Key {
            ids: vec![],
            field_key: KeyIndex::Field(Bytes::copy_from_slice(b"hello")),
        };
        let encoded = k.encode();
        let decoded = Key::decode(&encoded).unwrap();
        assert_eq!(decoded.ids.len(), 0);
        assert_eq!(
            decoded.field_key,
            KeyIndex::Field(Bytes::copy_from_slice(b"hello"))
        );
    }

    #[test]
    fn test_encode_decode_multiple_ids() {
        let id1 = VariableSizedId::new(127);
        let id2 = VariableSizedId::new(300);
        let id3 = VariableSizedId::new(9999);

        let k = Key {
            ids: vec![id1.clone(), id2.clone(), id3.clone()],
            field_key: KeyIndex::Field(Bytes::copy_from_slice(b"rust")),
        };

        let encoded = k.encode();
        let decoded = Key::decode(&encoded).unwrap();
        assert_eq!(decoded.ids, vec![id1, id2, id3]);
        assert_eq!(
            decoded.field_key,
            KeyIndex::Field(Bytes::copy_from_slice(b"rust"))
        );
    }

    #[test]
    fn test_decode_key_invalid_length() {
        // 构造不完整：只写了一个 varint 的第一个字节(带最高位=1)，没写完
        let mut buf = BytesMut::new();
        // 0x81 => 0x01 + 最高位=1，表示还有后续字节，但我们不写后续了
        buf.extend_from_slice(&[0x81]);
        let buf = buf.freeze();

        let decoded = Key::decode(&buf);
        match decoded {
            Err(EncodeError::InvalidLength) => {}
            _ => assert!(false),
        }
    }
}
