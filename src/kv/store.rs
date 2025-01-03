use anyhow::Result;
use bytes::Bytes;
use std::path::Path;

use super::node::NodeValue;

pub struct Store {
    tree: sled::Db,
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

    pub(crate) fn get_raw(&self, key: &[u8]) -> Result<Option<Bytes>, sled::Error> {
        let value = self.tree.get(key)?;
        Ok(value.map(|v| Bytes::copy_from_slice(&v)))
    }

    pub(crate) fn set_raw(&self, key: &[u8], value: &[u8]) -> Result<(), sled::Error> {
        self.tree.insert(key, value)?;
        Ok(())
    }
}
