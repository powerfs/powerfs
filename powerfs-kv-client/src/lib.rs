pub mod client;
pub mod s3_test_client;

#[cfg(any(feature = "spdk", feature = "spdk-stub"))]
pub mod spdk_test_client;

pub use client::{KvCacheClient, KvCacheClientError};
pub use s3_test_client::{S3TestClient, S3TestClientError};

#[cfg(any(feature = "spdk", feature = "spdk-stub"))]
pub use spdk_test_client::{SpdkTestClient, TestClientError};
