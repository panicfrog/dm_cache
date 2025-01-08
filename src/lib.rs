mod db;
mod json;
mod kv;

use anyhow::Result;
use bytes::Bytes;
use json::ItemValue;
use kv::{NodeValue, VariableSizedId};
use parking_lot::RwLock;
use simd_json::{Node, StaticNode};
use std::{ops::Deref, sync::OnceLock};
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

pub fn insert_json(key: &[u8], value: &mut [u8]) -> Result<()> {
    let k = kv::Key::decode(key)?;
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
    let root_value = simd_json::to_borrowed_value(value)?;
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
                println!("IV: {:?}", idx);
                sub_key
            }
            json::IterItem2::Array => node_key.clone(),
            json::IterItem2::Object => node_key.clone(),
            json::IterItem2::String(_) => node_key.clone(),
            json::IterItem2::Static(_) => node_key.clone(),
        };
        sub_key
    });

    for (item, key) in json_iter {
        // TODO: 插入数据
    }

    // TODO: 插入 JSON 数据到kv数据库
    // json::parse_and_iter(value, k, |item, node_key| {
    //     let raw_node_key = node_key.encode();
    //     let (sub_key, raw_node_value) = match item {
    //         json::IterItem::KV(k, item_value) => {
    //             metadata.last_id += 1;
    //             let sub_key = node_key.sub_key(
    //                 VariableSizedId::new(metadata.last_id),
    //                 kv::KeyIndex::Field(Bytes::copy_from_slice(k.as_bytes())),
    //             );
    //             // TODO: store item_value
    //             println!("KV: {:?}", k);
    //             let node_value = match item_value {
    //                 ItemValue::Array => NodeValue::Array,
    //                 ItemValue::Object => NodeValue::Array,
    //                 ItemValue::String(s) => NodeValue::String(Bytes::copy_from_slice(s.as_bytes())),
    //                 ItemValue::Static(StaticNode::Bool(b)) => NodeValue::Bool(*b),
    //                 ItemValue::Static(StaticNode::F64(f)) => NodeValue::Number(*f),
    //                 ItemValue::Static(StaticNode::I64(i)) => NodeValue::NumberI(*i),
    //                 ItemValue::Static(StaticNode::U64(u)) => NodeValue::NumberU(*u),
    //                 ItemValue::Static(StaticNode::Null) => NodeValue::Null,
    //             };
    //             let raw_node_value = node_value.encode();
    //             (sub_key, raw_node_value)
    //         }
    //         json::IterItem::IV(idx, item_value) => {
    //             metadata.last_id += 1;
    //             let sub_key = node_key.sub_key(
    //                 VariableSizedId::new(metadata.last_id),
    //                 kv::KeyIndex::Id(VariableSizedId::new(idx.clone() as u64)),
    //             );
    //             // TODO: store item_value
    //             let node_value = match item_value {
    //                 ItemValue::Array => NodeValue::Array,
    //                 ItemValue::Object => NodeValue::Array,
    //                 ItemValue::String(s) => NodeValue::String(Bytes::copy_from_slice(s.as_bytes())),
    //                 ItemValue::Static(StaticNode::Bool(b)) => NodeValue::Bool(*b),
    //                 ItemValue::Static(StaticNode::F64(f)) => NodeValue::Number(*f),
    //                 ItemValue::Static(StaticNode::I64(i)) => NodeValue::NumberI(*i),
    //                 ItemValue::Static(StaticNode::U64(u)) => NodeValue::NumberU(*u),
    //                 ItemValue::Static(StaticNode::Null) => NodeValue::Null,
    //             };
    //             let raw_node_value = node_value.encode();
    //             println!("IV: {:?}", idx);
    //             (sub_key, raw_node_value)
    //         }
    //         json::IterItem::Array(arr_key) => {
    //             // TODO: store Array as value
    //             let node_value = NodeValue::Array;
    //             let raw_node_value = node_value.encode();
    //             ((*arr_key).clone(), raw_node_value)
    //         }
    //         json::IterItem::Object(obj) => {
    //             // TODO: store Object as value
    //             let node_value = NodeValue::Object;
    //             let raw_node_value = node_value.encode();
    //             ((*obj).clone(), raw_node_value)
    //         }
    //         json::IterItem::String(s) => {
    //             // TODO: store String as value
    //             let node_value = NodeValue::String(Bytes::copy_from_slice(s.as_bytes()));
    //             let raw_node_value = node_value.encode();
    //             (node_key.clone(), raw_node_value)
    //         }
    //         json::IterItem::Static(s) => {
    //             // TODO: store Static as value
    //             let node_value = match s {
    //                 StaticNode::Bool(b) => NodeValue::Bool(*b),
    //                 StaticNode::F64(f) => NodeValue::Number(*f),
    //                 StaticNode::I64(i) => NodeValue::NumberI(*i),
    //                 StaticNode::U64(u) => NodeValue::NumberU(*u),
    //                 StaticNode::Null => NodeValue::Null,
    //             };
    //             let raw_node_value = node_value.encode();
    //             (node_key.clone(), raw_node_value)
    //         }
    //     };
    //     sub_key
    // })?;
    Ok(())
}
