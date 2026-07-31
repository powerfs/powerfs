//! Error types for the Master TLV client.

use thiserror::Error;

pub type MasterNetResult<T> = Result<T, MasterNetError>;

#[derive(Debug, Error)]
pub enum MasterNetError {
    #[error("not connected to master")]
    NotConnected,

    #[error("no master endpoints configured")]
    NoEndpoints,

    #[error("connection failed: {0}")]
    ConnectionFailed(String),

    #[error("request timeout")]
    Timeout,

    #[error("master returned error status {status}: {detail}")]
    ServerError { status: u16, detail: String },

    #[error("leader redirect failed: {0}")]
    RedirectFailed(String),

    #[error("redirect response has empty leader address")]
    EmptyRedirect,

    #[error("TLV decode error: {0}")]
    DecodeError(String),

    #[error("all endpoints exhausted after {attempts} attempts")]
    AllEndpointsExhausted { attempts: usize },
}
