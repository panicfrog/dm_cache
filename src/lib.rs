mod db;
mod json;
mod kv;

use anyhow::Result;
use bytes::Bytes;
use json::ItemValue;
use kv::{Key, NodeValue, VariableSizedId};
use parking_lot::RwLock;
use simd_json::StaticNode;
use std::sync::OnceLock;
use thiserror::Error;

#[derive(Error, Debug, Clone)]
pub enum DBError {
    #[error("Database path not set")]
    PathNotSet,
    #[error("Database path already set")]
    PathAlreadySet,
    #[error("KV error: {0}")]
    KVError(#[from] kv::EncodeError),
    #[error("Database init error: {0}")]
    DatabaseInitError(#[from] sled::Error),
    #[error("Store error {0}")]
    DatabaseStoreError(#[from] kv::StoreError),
    #[error("Database json error")]
    DatabaseJsonError,
    #[error("Database transaction error {0}")]
    DatabaseUnabortableTransaction(#[from] sled::transaction::UnabortableTransactionError),
    #[error("Database trac")]
    DatabaseTransationError(#[from] sled::transaction::TransactionError),
    #[error("Duplicate root key")]
    DuplicateRootKey,
    #[error("No super node")]
    NoSuperNode,
    #[error("Invalid type of super node")]
    InvalidSuperNodeType,
}

pub struct Database {
    store: kv::Store,
    metadata: db::Metadata,
}

// 全局变量
static INIT_PATH: OnceLock<String> = OnceLock::new();
static DATABASE: OnceLock<Result<RwLock<Database>, DBError>> = OnceLock::new();
const METADAT_KEY: &'static [u8] = b"__METADATA__";

// 设置数据库路径
pub fn set_database_path(path: &str) -> Result<(), DBError> {
    INIT_PATH
        .set(path.to_string())
        .map_err(|_| DBError::PathAlreadySet)
}

// 获取数据库实例
pub fn get_database() -> Result<&'static RwLock<Database>, DBError> {
    let db_result = DATABASE.get_or_init(|| {
        let path = INIT_PATH.get().ok_or(DBError::PathNotSet)?;
        let store = kv::Store::new(path).map_err(|e| DBError::DatabaseInitError(e))?;
        let (metadata, loaded) = match store.get_raw(METADAT_KEY) {
            Ok(Some(v)) => (db::Metadata::decode(&v)?, true),
            Ok(None) => (db::Metadata::new(), false),
            Err(e) => return Err(DBError::DatabaseInitError(e)),
        };
        if !loaded {
            store.set_raw(METADAT_KEY, &metadata.encode())?;
        }
        Ok(RwLock::new(Database { store, metadata }))
    });

    match db_result {
        Ok(db) => Ok(db),
        Err(err) => match err {
            DBError::DatabaseInitError(_) => Err(err.clone()),
            _ => Err(err.clone()),
        },
    }
}

pub fn insert_json(key: &[u8], value: &mut [u8]) -> Result<(), DBError> {
    let k = Key::decode(key)?;
    let db = get_database()?.write();
    let mut metadata = db.metadata.clone();
    if k.field_key.is_root() {
        if metadata.roots.contains(value) {
            return Err(DBError::DuplicateRootKey)?;
        }
        metadata.roots.insert(key.to_vec());
    } else {
        // 如果不是root，查找它的父节点，如果不存在报错
        if k.ids.len() < 2 {
            return Err(DBError::NoSuperNode)?;
        }
        let (_, super_value) = if let Some(kv) = db.store.get_super_node(&k)? {
            kv
        } else {
            return Err(DBError::NoSuperNode)?;
        };
        // 如果父节点是object，那么子节点只能是field
        if k.field_key.is_field() && !super_value.is_object() {
            return Err(DBError::InvalidSuperNodeType)?;
        }
        // 如果父节点是array，那么子节点只能是id
        if k.field_key.is_id() && !super_value.is_array() {
            return Err(DBError::InvalidSuperNodeType)?;
        }
    }
    let root_value = simd_json::to_borrowed_value(value).map_err(|_| DBError::DatabaseJsonError)?;
    let json_iter = json::JsonDfsIter::new(&root_value, k, |item, node_key| {
        let sub_key = match item {
            json::IterItem2::KV(k, _) => {
                metadata.last_id += 1;
                let sub_key = node_key.sub_key(
                    VariableSizedId::new(metadata.last_id),
                    kv::KeyIndex::Field(Bytes::copy_from_slice(k.as_bytes())),
                );
                sub_key
            }
            json::IterItem2::IV(idx, _) => {
                metadata.last_id += 1;
                let sub_key = node_key.sub_key(
                    VariableSizedId::new(metadata.last_id),
                    kv::KeyIndex::Id(VariableSizedId::new(idx.clone() as u64)),
                );
                sub_key
            }
            json::IterItem2::Array
            | json::IterItem2::Object
            | json::IterItem2::String(_)
            | json::IterItem2::Static(_) => {
                if let Some(last_id) = node_key.ids.last() {
                    if let Ok(last_id) = last_id.to_u64() {
                        if metadata.last_id < last_id {
                            metadata.last_id = last_id;
                        }
                    }
                }
                node_key.clone()
            }
        };
        sub_key
    });
    let mut betch = sled::Batch::default();
    for (item, key) in json_iter {
        let encoded_key = key.encode();
        let key_raw = encoded_key.as_slice();
        let value = match item {
            json::IterItem2::IV(_, v) | json::IterItem2::KV(_, v) => v,
            json::IterItem2::Array => json::ItemValue::Array,
            json::IterItem2::Object => json::ItemValue::Object,
            json::IterItem2::Static(s) => json::ItemValue::Static(s),
            json::IterItem2::String(s) => json::ItemValue::String(s),
        };
        let node_value = match value {
            ItemValue::Array => NodeValue::Array,
            ItemValue::Object => NodeValue::Object,
            ItemValue::String(s) => NodeValue::String(Bytes::copy_from_slice(s.as_bytes())),
            ItemValue::Static(StaticNode::Bool(b)) => NodeValue::Bool(*b),
            ItemValue::Static(StaticNode::F64(f)) => NodeValue::Number(*f),
            ItemValue::Static(StaticNode::I64(i)) => NodeValue::NumberI(*i),
            ItemValue::Static(StaticNode::U64(u)) => NodeValue::NumberU(*u),
            ItemValue::Static(StaticNode::Null) => NodeValue::Null,
        };
        let node_value_raw: &[u8] = &node_value.encode();
        betch.insert(key_raw, node_value_raw);
    }
    // insert metadata
    let metadata_raw = metadata.encode();
    betch.insert(METADAT_KEY, metadata_raw);
    db.store.tree.apply_batch(betch)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_json() {
        let path = "test_db";
        set_database_path(path).unwrap();
        let mut value = r#"{"a": 1, "b": 2, "c": [1, 2, 3], "d": {"e": 1, "f": 2}}"#
            .as_bytes()
            .to_vec();
        let root_key = Key {
            ids: vec![VariableSizedId::new(0)],
            field_key: kv::KeyIndex::Root,
        };
        let root_key_raw = root_key.encode();
        insert_json(&root_key_raw, &mut value).unwrap();
        let db = get_database().unwrap().read();
        db.store.tree.iter().for_each(|r| {
            let (k, v) = r.unwrap();
            println!("{:?} {:?}", k, v);
        });
    }
}
