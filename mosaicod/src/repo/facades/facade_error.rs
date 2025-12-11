use crate::{repo, rw, store, types};

#[derive(thiserror::Error, Debug)]
pub enum FacadeError {
    #[error("unable to find data: {0}")]
    NotFound(String),
    #[error("missing metadata field `{0}`")]
    MissingMetadataField(String),
    #[error("error reading {src}: {msg}")]
    ReadError { src: String, msg: String },
    #[error("error writing to {dst} data: {msg}")]
    WriteError { dst: String, msg: String },
    #[error("store error :: {0}")]
    StoreError(#[from] store::Error),
    #[error("data serialization error :: {0}")]
    DataSerializationError(#[from] rw::Error),
    #[error("metadata error :: {0}")]
    MetadataError(#[from] types::MetadataError),
    #[error("repository error :: {0}")]
    RepositoryError(#[from] repo::Error),
    #[error("sequence locked, unable to perform modifications")]
    SequenceLocked,
    #[error("topic locked, unable to perform modifications")]
    TopicLocked,
    #[error("topic unlocked, unable to perform the requested operation over an unlocked topic")]
    TopicUnlocked,
    #[error("unimplemented")]
    Unimplemented,
    #[error("unauthorized")]
    Unauthorized,
}
