use anyhow::Result;
use bytes::Bytes;
use std::path::Path;

use super::{node::NodeValue, Key, StoreError};

pub struct Store {
    pub(crate) tree: sled::Db,
}

pub fn init_store<P: AsRef<Path>>(file: &P) -> Result<Store> {
    let tree = sled::open(file)?;

    Ok(Store { tree })
}

impl Store {
    pub fn new<P: AsRef<Path>>(file: &P) -> Result<Self, sled::Error> {
        let tree = sled::open(file)?;

        Ok(Store { tree })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<NodeValue>> {
        let value = self.tree.get(key)?;
        Ok(value
            .map(|v| v.to_vec())
            .map(|v| Bytes::copy_from_slice(&v))
            .map(|b| NodeValue::decode(&b))
            .transpose()?)
    }

    pub fn set(&self, key: &[u8], value: &NodeValue) -> Result<()> {
        let value = value.encode();
        self.tree.insert(key, value.as_ref())?;
        Ok(())
    }

    pub fn get_super_node(&self, current: &Key) -> Result<Option<(Key, NodeValue)>, StoreError> {
        let current_key_raw = current.super_id_prefix();
        let mut iter = self.tree.range(current_key_raw..);

        let super_kv = iter.next().transpose()?;
        let (k, v) = if let Some(kv) = super_kv {
            kv
        } else {
            return Ok(None);
        };
        let key = Key::decode(&k).map_err(|e| StoreError::EncodeError(e))?;
        let node_value = NodeValue::decode(&Bytes::copy_from_slice(&v))
            .map_err(|e| StoreError::EncodeError(e))?;

        Ok(Some((key, node_value)))
    }

    pub(crate) fn get_raw(&self, key: &[u8]) -> Result<Option<Bytes>, sled::Error> {
        let value = self.tree.get(key)?;
        Ok(value.map(|v| Bytes::copy_from_slice(&v)))
    }

    pub(crate) fn set_raw(&self, key: &[u8], value: &[u8]) -> Result<(), sled::Error> {
        self.tree.insert(key, value)?;
        Ok(())
    }
}
