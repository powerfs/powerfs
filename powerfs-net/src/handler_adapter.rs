//! Handler Adapter - Bridges PowerFsNetHandler with ServerConnectionManager
//!
//! Provides `ManagedNetHandler` which implements `PowerFsNetHandler` while
//! delegating to `ServerConnectionManager` for session management and
//! middleware processing.
//!
//! Also provides `LegacyHandler` to wrap existing `PowerFsNetHandler`
//! implementations as `ServerRequestHandler`, enabling incremental migration.
//!
//! # Integration with PowerFsNetServer
//!
//! When using `PowerFsNetServer::bind_with_manager`, session registration
//! and unregistration are handled automatically by the server.
//! `ManagedNetHandler`'s `on_disconnect` is a no-op in this case since
//! the server already handles cleanup.
//!
//! # Example
//!
//! ```rust,ignore
//! // Auto-managed (recommended)
//! let handler = Arc::new(MyRequestHandler::new(...));
//! let manager = Arc::new(ServerConnectionManager::new());
//! let net_handler = Arc::new(ManagedNetHandler::from_arc(manager.clone(), handler));
//! PowerFsNetServer::bind_with_manager("0.0.0.0", 8080, net_handler, manager).await?;
//! ```

use std::sync::Arc;

use crate::errors::NetResult;
use crate::protocol::{ClientType, NetMessage};
use crate::server_connection::{ServerConnectionManager, ServerRequestHandler};

use super::request_context::RequestContext;
use super::server::PowerFsNetHandler;

/// ManagedNetHandler - implements PowerFsNetHandler with session management + middleware
pub struct ManagedNetHandler {
    manager: Arc<ServerConnectionManager>,
    handler: Arc<dyn ServerRequestHandler>,
}

impl ManagedNetHandler {
    pub fn new(manager: ServerConnectionManager, handler: Arc<dyn ServerRequestHandler>) -> Self {
        Self {
            manager: Arc::new(manager),
            handler,
        }
    }

    pub fn from_arc(
        manager: Arc<ServerConnectionManager>,
        handler: Arc<dyn ServerRequestHandler>,
    ) -> Self {
        Self { manager, handler }
    }

    pub fn manager(&self) -> &Arc<ServerConnectionManager> {
        &self.manager
    }
}

#[async_trait::async_trait]
impl PowerFsNetHandler for ManagedNetHandler {
    async fn handle_request(&self, client_id: u64, msg: &NetMessage) -> NetResult<NetMessage> {
        self.manager
            .process_with_pipeline(client_id, msg, self.handler.clone())
            .await
    }

    async fn on_connect(&self, client_id: u64, client_type: ClientType) {
        log::info!(
            "NET_HANDLER: client connected, id={}, type={:?}",
            client_id,
            client_type
        );
    }

    async fn on_disconnect(&self, client_id: u64) {
        self.manager.unregister_session(client_id).await;
    }
}

/// LegacyHandler - adapts PowerFsNetHandler to ServerRequestHandler
///
/// Enables existing handlers (MasterNetHandler, VolumeNetHandler, etc.)
/// to be used with ServerConnectionManager's middleware pipeline.
pub struct LegacyHandler {
    inner: Arc<dyn PowerFsNetHandler>,
}

impl LegacyHandler {
    pub fn new(inner: Arc<dyn PowerFsNetHandler>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl ServerRequestHandler for LegacyHandler {
    async fn handle(&self, ctx: &mut RequestContext, msg: &NetMessage) -> NetResult<NetMessage> {
        let client_id = ctx.client.client_id;
        self.inner.handle_request(client_id, msg).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::middleware::{LoggingMiddleware, MetricsMiddleware};
    use crate::protocol::{FrameFlags, FrameHeader, MsgType};
    use crate::request_context::RequestContext;
    use std::net::SocketAddr;

    fn make_test_msg() -> NetMessage {
        NetMessage::new(FrameHeader::new(
            MsgType::Lookup.as_u16(),
            FrameFlags::new(FrameFlags::REQUEST),
            1,
            0,
        ))
    }

    struct TestBusinessHandler;

    #[async_trait::async_trait]
    impl ServerRequestHandler for TestBusinessHandler {
        async fn handle(
            &self,
            _ctx: &mut RequestContext,
            msg: &NetMessage,
        ) -> NetResult<NetMessage> {
            let mut resp = NetMessage::new(FrameHeader::new(
                msg.header.msg_type,
                FrameFlags::new(FrameFlags::RESPONSE),
                msg.header.seq,
                0,
            ));
            resp.header.status = crate::STATUS_OK;
            Ok(resp)
        }
    }

    #[tokio::test]
    async fn test_managed_handler_request() {
        let manager = ServerConnectionManager::new();
        let handler = Arc::new(TestBusinessHandler);
        let managed = ManagedNetHandler::new(manager, handler);

        let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let client_id: u64 = 1;
        managed
            .manager
            .register_session(client_id, ClientType::Fuse, addr)
            .await;

        let msg = make_test_msg();
        let result = managed.handle_request(client_id, &msg).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().header.status, crate::STATUS_OK);
    }

    #[tokio::test]
    async fn test_managed_handler_disconnect() {
        let manager = ServerConnectionManager::new();
        let handler = Arc::new(TestBusinessHandler);
        let managed = ManagedNetHandler::new(manager, handler);

        let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let client_id: u64 = 2;
        managed
            .manager
            .register_session(client_id, ClientType::Fuse, addr)
            .await;
        assert_eq!(managed.manager.active_count().await, 1);

        managed.on_disconnect(client_id).await;
        assert_eq!(managed.manager.active_count().await, 0);
    }

    #[tokio::test]
    async fn test_legacy_handler_adapter() {
        struct LegacyTestHandler;

        #[async_trait::async_trait]
        impl PowerFsNetHandler for LegacyTestHandler {
            async fn handle_request(
                &self,
                _client_id: u64,
                msg: &NetMessage,
            ) -> NetResult<NetMessage> {
                let mut resp = NetMessage::new(FrameHeader::new(
                    msg.header.msg_type,
                    FrameFlags::new(FrameFlags::RESPONSE),
                    msg.header.seq,
                    0,
                ));
                resp.header.status = crate::STATUS_OK;
                Ok(resp)
            }
        }

        let legacy = Arc::new(LegacyTestHandler);
        let adapter = LegacyHandler::new(legacy);

        let manager = ServerConnectionManager::new();
        let managed = ManagedNetHandler::new(manager, Arc::new(adapter));

        let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let client_id: u64 = 3;
        managed
            .manager
            .register_session(client_id, ClientType::Fuse, addr)
            .await;

        let msg = make_test_msg();
        let result = managed.handle_request(client_id, &msg).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_managed_handler_with_custom_pipeline() {
        use crate::middleware::RequestPipeline;

        let pipeline = RequestPipeline::new()
            .add_middleware(LoggingMiddleware::new())
            .add_middleware(MetricsMiddleware::new());

        let manager = ServerConnectionManager::new().with_pipeline(pipeline);
        let handler = Arc::new(TestBusinessHandler);
        let managed = ManagedNetHandler::new(manager, handler);

        let addr: SocketAddr = "127.0.0.1:12345".parse().unwrap();
        let client_id: u64 = 4;
        managed
            .manager
            .register_session(client_id, ClientType::Fuse, addr)
            .await;

        let msg = make_test_msg();
        let result = managed.handle_request(client_id, &msg).await;
        assert!(result.is_ok());
    }
}
