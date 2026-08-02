//! Transport trait — transport layer abstraction for TCP/RDMA/QUIC.
//! Currently only TCP is implemented; RDMA/QUIC are reserved for future use.

use crate::errors::NetError;

/// Transport layer connection trait.
pub trait Transport: Send + Sync {
    /// Send a request and wait for the response (synchronous semantics).
    fn send_request(
        &self,
        msg_type: u16,
        body: &[u8],
        metadata: &[(&str, &str)],
    ) -> Result<Vec<u8>, NetError>;
}

/// Transport layer connection pool trait.
pub trait TransportPool: Send + Sync {
    type Transport: Transport;

    /// Get or create a connection to the given address.
    fn get_or_create(&self, addr: &str) -> Result<Self::Transport, NetError>;

    /// Close the connection to the given address.
    fn close(&self, addr: &str);

    /// Health check for the given address.
    fn is_healthy(&self, addr: &str) -> bool;
}

/// Batch send interface (reserved for RDMA optimization).
pub trait BatchTransport: Transport {
    /// Send a batch of requests and return the list of responses.
    fn send_batch(
        &self,
        requests: &[(u16, &[u8])],
    ) -> Result<Vec<Result<Vec<u8>, NetError>>, NetError>;
}
