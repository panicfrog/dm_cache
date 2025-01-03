use std::collections::HashSet;

use crate::kv::{read_variable_sized_id, EncodeError, VariableSizedId};
use anyhow::Result;

#[derive(Debug, Clone)]
pub struct Metadata {
    pub version: u64,
    pub last_id: u64,
    pub last_timestamp: u64,
    pub roots: HashSet<Vec<u8>>,
}

impl Metadata {
    pub fn new() -> Self {
        Self {
            version: 0,
            last_id: 0,
            last_timestamp: 0,
            roots: HashSet::new(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        // 1. 先计算roots的长度
        let roots_len = self.roots.iter().fold(0, |acc, id| acc + id.len() + 1);
        // 2. 创建一个Vec<u8>，预分配足够的空间
        let mut buf = Vec::with_capacity(8 * 3 + roots_len);
        // 3. 写入version
        buf.extend_from_slice(&self.version.to_be_bytes());
        // 4. 写入last_id
        buf.extend_from_slice(&self.last_id.to_be_bytes());
        // 5. 写入last_timestamp
        buf.extend_from_slice(&self.last_timestamp.to_be_bytes());
        // 6. 遍历写入roots
        for id in &self.roots {
            // 先写入长度，然后写入数据
            // 这里限制每个root的key占用的字节数不能超过256
            let len = id.len() as u8;
            buf.extend_from_slice(&[len]);
            buf.extend_from_slice(id);
        }
        buf
    }

    pub fn decode(buf: &[u8]) -> Result<Self, EncodeError> {
        if (buf.len() < 24) || (buf.len() % 8 != 0) {
            return Err(EncodeError::InvalidLength);
        }
        // 1. 读取version
        let version = u64::from_be_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        // 2. 读取last_id
        let last_id = u64::from_be_bytes([
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ]);
        // 3. 读取last_timestamp
        let last_timestamp = u64::from_be_bytes([
            buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23],
        ]);
        // 4. 读取roots
        let mut roots = HashSet::new();
        let mut offset = 24;
        while offset < buf.len() {
            let len = buf[offset];
            if offset + len as usize >= buf.len() {
                return Err(EncodeError::Overflow);
            }
            let id = buf[(offset + 1)..(offset + 1 + len as usize)].to_vec();
            roots.insert(id);
            offset += 1 + len as usize
        }
        Ok(Self {
            version,
            last_id,
            last_timestamp,
            roots,
        })
    }
}
