use crate::store;

/// Errors that can occur during the construction or deserialization of a Query.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Wraps errors occurring during the JSON parsing/deserialization phase.
    #[error("query deserialization error :: {0}")]
    DeserializationError(String),

    #[error("operation error :: field `{field}` has {err}")]
    OpError { field: String, err: super::OpError },

    #[error("bad field `{field}`")]
    BadField { field: String },

    #[error("datafusion backend error :: {0}")]
    DataFusion(#[from] datafusion::error::DataFusionError),

    #[error("bad path :: {0}")]
    BadPath(#[from] url::ParseError),

    #[error("store error :: {0}")]
    StoreError(#[from] store::Error),
}

impl Error {
    pub fn unsupported_op(field_name: String) -> Self {
        Self::OpError {
            field: field_name,
            err: super::OpError::UnsupportedOperation,
        }
    }
}
