use thiserror::Error;

#[derive(Error, Clone, Debug, PartialEq, Eq)]
pub enum EncodeError {
    #[error("invalid UTF-8 string")]
    InvalidUtf8(#[from] std::str::Utf8Error),
    #[error("invalid length")]
    InvalidLength,
    #[error("invalid type")]
    InvalidType,
    #[error("invalid value")]
    Overflow,
}

#[derive(Error, Clone, Debug, PartialEq, Eq)]
pub enum StoreError {
    #[error("sled error: {0}")]
    SledError(#[from] sled::Error),
    #[error("encode error: {0}")]
    EncodeError(#[from] EncodeError),
}
