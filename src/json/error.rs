use simd_json;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum JsonError {
    #[error("parse error")]
    ParseError(#[from] simd_json::Error),
}
