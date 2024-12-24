use thiserror::Error;

#[derive(Error, Debug)]
pub enum EncodeError {
    #[error("invalid UTF-8 string")]
    InvalidUtf8(#[from] std::str::Utf8Error),
    #[error("invalid length")]
    InvalidLength,
}
