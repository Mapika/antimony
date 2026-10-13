#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid object id: {0}")]
    InvalidId(String),
    #[error("malformed object: {0}")]
    Malformed(String),
    #[error("invalid path: {0:?}")]
    InvalidPath(String),
}

pub type Result<T> = std::result::Result<T, Error>;
