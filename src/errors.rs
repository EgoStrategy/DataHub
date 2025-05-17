use thiserror::Error;
use std::num::ParseIntError;

#[derive(Error, Debug)]
pub enum DataHubError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("HTTP request error: {0}")]
    RequestError(#[from] reqwest::Error),

    #[error("JSON parsing error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Arrow error: {0}")]
    ArrowError(String),

    #[error("Date parsing error: {0}")]
    DateError(#[from] chrono::ParseError),

    #[error("Excel parsing error: {0}")]
    ExcelError(#[from] calamine::Error),

    #[error("Exchange error: {0}")]
    ExchangeError(String),

    #[error("Data error: {0}")]
    DataError(String),

    #[error("Parse int error: {0}")]
    ParseIntError(#[from] ParseIntError),

    #[error("Unknown error: {0}")]
    Unknown(String),
}

pub type Result<T> = std::result::Result<T, DataHubError>;

// 用于从字符串创建错误
impl From<String> for DataHubError {
    fn from(s: String) -> Self {
        DataHubError::Unknown(s)
    }
}

// 用于从&str创建错误
impl From<&str> for DataHubError {
    fn from(s: &str) -> Self {
        DataHubError::Unknown(s.to_string())
    }
}
