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
    #[error("Database init error: {0}")]
    DatabaseInitError(#[from] sled::Error),
}

// 全局变量
static INIT_PATH: OnceLock<String> = OnceLock::new();
static DATABASE: OnceLock<Result<RwLock<kv::Store>, DBError>> = OnceLock::new();

// 设置数据库路径
pub fn set_database_path(path: &str) -> Result<(), DBError> {
    INIT_PATH
        .set(path.to_string())
        .map_err(|_| DBError::PathAlreadySet)
}

// 获取数据库实例
pub fn get_database() -> Result<&'static RwLock<kv::Store>, DBError> {
    let db_result = DATABASE.get_or_init(|| {
        let path = INIT_PATH.get().ok_or(DBError::PathNotSet)?;
        let store = kv::Store::new(path).map_err(|e| DBError::DatabaseInitError(e))?;
        Ok(RwLock::new(store))
    });

    match db_result {
        Ok(db) => Ok(db),
        Err(err) => match err {
            DBError::DatabaseInitError(_) => Err(err.clone()),
            _ => Err(err.clone()),
        },
    }
}
