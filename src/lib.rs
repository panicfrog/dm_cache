mod db;
mod json;
mod kv;

use anyhow::Result;
use parking_lot::RwLock;
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
    #[error("Duplicate root key")]
    DuplicateRootKey,
    #[error("No super node")]
    NoSuperNode,
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
    }
    // TODO: 插入 JSON 数据到kv数据库
    json::parse_and_iter(value, key, |item, state| {
        print!("{:?}", &item);
        state
    })?;
    Ok(())
}
