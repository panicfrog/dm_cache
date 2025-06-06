mod db;
mod json;
mod kv;

use anyhow::Result;
use bytes::Bytes;
use db::Metadata;
use json::ItemValue;
use kv::{Key, KeyIndex, NodeValue, VariableSizedId};
use parking_lot::RwLock;
use simd_json::StaticNode;
use std::sync::OnceLock;
use thiserror::Error;

// 重新导出 JSONPath 解析相关的类型和函数
pub use db::{parse, JsonPathSegment, JsonPathParseError};

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
const METADAT_KEY: &'static [u8] = b"~~METADATA~~";

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

fn make_sub_key(node_key: &Key, metadata: &mut Metadata, kind: KeyIndex) -> Key {
    metadata.last_id += 1;
    node_key.sub_key(VariableSizedId::new(metadata.last_id), kind)
}

pub fn insert_json(key: &[u8], value: &mut [u8]) -> Result<(), DBError> {
    let k = Key::decode(key)?;
    let mut db = get_database()?.write();
    let mut metadata = db.metadata.clone();
    if k.field_key.is_root() {
        if metadata.roots.contains(key) {
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
            json::IterItem::KV(k, _) => make_sub_key(
                node_key,
                &mut metadata,
                kv::KeyIndex::Field(Bytes::copy_from_slice(k.as_bytes())),
            ),
            json::IterItem::IV(idx, _) => make_sub_key(
                node_key,
                &mut metadata,
                kv::KeyIndex::Id(VariableSizedId::new(*idx as u64)),
            ),
            json::IterItem::Array
            | json::IterItem::Object
            | json::IterItem::String(_)
            | json::IterItem::Static(_) => {
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
            json::IterItem::IV(_, v) | json::IterItem::KV(_, v) => v,
            json::IterItem::Array => json::ItemValue::Array,
            json::IterItem::Object => json::ItemValue::Object,
            json::IterItem::Static(s) => json::ItemValue::Static(s),
            json::IterItem::String(s) => json::ItemValue::String(s),
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
    db.metadata = metadata;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_insert_json() {
        let path = "test_db";
        
        // 清理之前的测试数据库
        if std::path::Path::new(path).exists() {
            std::fs::remove_dir_all(path).expect("Failed to remove test database");
        }
        
        let _ = set_database_path(path); // 忽略可能的错误，因为可能已经设置过
        let mut value = r#"{"a": 1, "b": 2, "c": [1, 2, 3], "d": {"e": 1, "f": 2}}"#
            .as_bytes()
            .to_vec();
        let root_key: Key = Key {
            ids: vec![VariableSizedId::new(0)],
            field_key: kv::KeyIndex::Root,
        };
        let root_key_raw = root_key.encode();
        insert_json(&root_key_raw, &mut value).unwrap();

        let db = get_database().unwrap().read();
        db.store.tree.flush().unwrap();
        db.store.tree.iter().for_each(|r| {
            let (k, v) = r.unwrap();
            println!("{:?} {:?}", k, v);
        });
    }

    #[test]
    fn test_get_database_metadata() {
        let path = "test_db_metadata";
        
        // 清理之前的测试数据库
        if std::path::Path::new(path).exists() {
            std::fs::remove_dir_all(path).expect("Failed to remove test database");
        }
        
        let _ = set_database_path(path); // 忽略可能的错误，因为可能已经设置过
        let db = get_database().unwrap();
        let db = db.read();
        db.store.tree.iter().for_each(|r| {
            let (k, v) = r.unwrap();
            println!("{:?} {:?}", k, v);
        });
    }

    #[test]
    fn test_duplicate_root_key() {
        let path = "test_duplicate_db";
        
        // 清理之前的测试数据库
        if std::path::Path::new(path).exists() {
            std::fs::remove_dir_all(path).expect("Failed to remove test database");
        }
        
        let _ = set_database_path(path); // 忽略可能的错误，因为可能已经设置过
        
        let mut value1 = r#"{"x": 1, "y": 2}"#.as_bytes().to_vec();
        let mut value2 = r#"{"z": 3, "w": 4}"#.as_bytes().to_vec();
        
        // 使用相同的root key来测试重复插入
        let root_key: Key = Key {
            ids: vec![VariableSizedId::new(100)],
            field_key: kv::KeyIndex::Root,
        };
        let root_key_raw = root_key.encode();
        
        // 第一次插入应该成功
        let result1 = insert_json(&root_key_raw, &mut value1);
        assert!(result1.is_ok(), "First insertion should succeed");
        
        // 检查插入后metadata.roots长度应该是1
        {
            let db = get_database().unwrap().read();
            assert_eq!(db.metadata.roots.len(), 1, "After first insertion, metadata.roots should contain exactly 1 root key");
            assert!(db.metadata.roots.contains(&root_key_raw), "metadata.roots should contain the inserted root key");
        }
        
        // 第二次插入相同的root key应该失败
        let result2 = insert_json(&root_key_raw, &mut value2);
        assert!(result2.is_err(), "Second insertion should fail");
        
        if let Err(err) = result2 {
            match err {
                DBError::DuplicateRootKey => {
                    println!("Correctly detected duplicate root key");
                }
                _ => panic!("Expected DuplicateRootKey error, but got: {:?}", err),
            }
        }
    }
}
