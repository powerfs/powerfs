//! Middleware pipeline for server-side request processing
//!
//! Provides a composable middleware chain that wraps request handling
//! with cross-cutting concerns: rate limiting, logging, tracing, etc.

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;

use crate::errors::NetResult;
use crate::protocol::{FrameFlags, NetMessage};

use super::request_context::RequestContext;

/// Trait for server-side middleware
///
/// Middleware wraps the next handler, adding behavior before and/or after
/// the actual request processing. Multiple middlewares can be composed
/// into a pipeline.
///
/// # Example
/// ```ignore
/// async fn handle(&self, ctx, msg, next) -> NetResult<NetMessage> {
///     // Before processing
///     let start = Instant::now();
///     
///     // Call next middleware or final handler
///     let result = next.run(ctx, msg).await;
///     
///     // After processing
///     log::info!("Request took {:?}", start.elapsed());
///     result
/// }
/// ```
#[async_trait]
pub trait Middleware: Send + Sync {
    async fn handle(
        &self,
        ctx: &mut RequestContext,
        msg: &NetMessage,
        next: &dyn NextHandler,
    ) -> NetResult<NetMessage>;

    fn name(&self) -> &str {
        "middleware"
    }
}

/// NextHandler - represents the next step in the middleware chain
///
/// Implemented by the pipeline internally. Middleware calls `next.run()`
/// to proceed to the next middleware or the final handler.
#[async_trait]
pub trait NextHandler: Send + Sync {
    async fn run(&self, ctx: &mut RequestContext, msg: &NetMessage) -> NetResult<NetMessage>;
}

/// Pipeline that composes multiple middlewares around a final handler
pub struct RequestPipeline {
    middlewares: Vec<Arc<dyn Middleware>>,
}

impl RequestPipeline {
    pub fn new() -> Self {
        Self {
            middlewares: Vec::new(),
        }
    }

    pub fn add_middleware(mut self, mw: impl Middleware + 'static) -> Self {
        self.middlewares.push(Arc::new(mw));
        self
    }

    pub fn add_arc(mut self, mw: Arc<dyn Middleware>) -> Self {
        self.middlewares.push(mw);
        self
    }

    /// Execute the pipeline with a final handler
    pub async fn execute(
        &self,
        ctx: &mut RequestContext,
        msg: &NetMessage,
        handler: Arc<dyn NextHandler>,
    ) -> NetResult<NetMessage> {
        // Build the chain: middleware[last] -> ... -> middleware[0] -> handler
        let mut current = handler;

        for mw in self.middlewares.iter().rev() {
            let mw = mw.clone();
            current = Arc::new(PipelineNode {
                middleware: mw,
                next: current,
            });
        }

        current.run(ctx, msg).await
    }

    pub fn is_empty(&self) -> bool {
        self.middlewares.is_empty()
    }

    pub fn len(&self) -> usize {
        self.middlewares.len()
    }

    pub fn middlewares(&self) -> &[Arc<dyn Middleware>] {
        &self.middlewares
    }
}

impl Default for RequestPipeline {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for conveniently constructing a RequestPipeline with common configurations
///
/// # Example
///
/// ```rust,ignore
/// // Default pipeline (logging + metrics + tracing)
/// let pipeline = PipelineBuilder::default_build();
///
/// // With rate limiting
/// let pipeline = PipelineBuilder::with_rate_limit(100);
///
/// // Custom configuration
/// let pipeline = PipelineBuilder::new()
///     .with_logging()
///     .with_metrics()
///     .with_tracing()
///     .with_rate_limit(50)
///     .build();
/// ```
pub struct PipelineBuilder {
    enable_logging: bool,
    enable_metrics: bool,
    enable_tracing: bool,
    rate_limit: Option<usize>,
    custom_middlewares: Vec<Arc<dyn Middleware>>,
}

impl PipelineBuilder {
    pub fn new() -> Self {
        Self {
            enable_logging: false,
            enable_metrics: false,
            enable_tracing: false,
            rate_limit: None,
            custom_middlewares: Vec::new(),
        }
    }

    /// Default pipeline: logging + metrics
    pub fn default_build() -> RequestPipeline {
        Self::new().with_logging().with_metrics().build()
    }

    /// Pipeline with all built-in middlewares enabled
    pub fn full_tracing() -> RequestPipeline {
        Self::new()
            .with_logging()
            .with_metrics()
            .with_tracing()
            .build()
    }

    /// Pipeline with rate limiting (logging + metrics + rate_limit)
    pub fn rate_limited(max_concurrent: usize) -> RequestPipeline {
        Self::new()
            .with_logging()
            .with_metrics()
            .with_concurrent_rate_limit(max_concurrent)
            .build()
    }

    pub fn with_logging(mut self) -> Self {
        self.enable_logging = true;
        self
    }

    pub fn with_metrics(mut self) -> Self {
        self.enable_metrics = true;
        self
    }

    pub fn with_tracing(mut self) -> Self {
        self.enable_tracing = true;
        self
    }

    pub fn with_concurrent_rate_limit(mut self, max_concurrent: usize) -> Self {
        self.rate_limit = Some(max_concurrent);
        self
    }

    pub fn with_custom(mut self, mw: impl Middleware + 'static) -> Self {
        self.custom_middlewares.push(Arc::new(mw));
        self
    }

    pub fn with_custom_arc(mut self, mw: Arc<dyn Middleware>) -> Self {
        self.custom_middlewares.push(mw);
        self
    }

    pub fn build(self) -> RequestPipeline {
        let mut pipeline = RequestPipeline::new();

        if self.enable_logging {
            pipeline = pipeline.add_middleware(LoggingMiddleware::new());
        }
        if self.enable_tracing {
            pipeline = pipeline.add_middleware(TracingMiddleware::new());
        }
        if let Some(limit) = self.rate_limit {
            pipeline = pipeline.add_middleware(RateLimitMiddleware::new(limit));
        }
        if self.enable_metrics {
            pipeline = pipeline.add_middleware(MetricsMiddleware::new());
        }

        for mw in self.custom_middlewares {
            pipeline = pipeline.add_arc(mw);
        }

        pipeline
    }
}

impl Default for PipelineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Internal node in the middleware chain
struct PipelineNode {
    middleware: Arc<dyn Middleware>,
    next: Arc<dyn NextHandler>,
}

#[async_trait]
impl NextHandler for PipelineNode {
    async fn run(&self, ctx: &mut RequestContext, msg: &NetMessage) -> NetResult<NetMessage> {
        self.middleware.handle(ctx, msg, self.next.as_ref()).await
    }
}

/// Simple handler wrapper for a closure-based final handler
pub struct FnHandler<F>
where
    F: Fn(&mut RequestContext, &NetMessage) -> NetResult<NetMessage> + Send + Sync,
{
    f: F,
}

impl<F> FnHandler<F>
where
    F: Fn(&mut RequestContext, &NetMessage) -> NetResult<NetMessage> + Send + Sync,
{
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

#[async_trait]
impl<F> NextHandler for FnHandler<F>
where
    F: Fn(&mut RequestContext, &NetMessage) -> NetResult<NetMessage> + Send + Sync + 'static,
{
    async fn run(&self, ctx: &mut RequestContext, msg: &NetMessage) -> NetResult<NetMessage> {
        (self.f)(ctx, msg)
    }
}

// ============================================================================
// LoggingMiddleware
// ============================================================================

/// Middleware that logs every request with trace_id and latency
pub struct LoggingMiddleware;

impl LoggingMiddleware {
    pub fn new() -> Self {
        Self
    }
}

impl Default for LoggingMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Middleware for LoggingMiddleware {
    async fn handle(
        &self,
        ctx: &mut RequestContext,
        msg: &NetMessage,
        next: &dyn NextHandler,
    ) -> NetResult<NetMessage> {
        let trace_id = ctx.trace_id().to_string();
        let client_id = ctx.client.client_id;
        let msg_type_name = ctx.msg_type_name().to_string();
        let start = Instant::now();

        log::debug!(
            "[TRACE {}] → REQUEST: client={}, type={:?}, seq={}, body_len={}",
            trace_id,
            client_id,
            msg.msg_type(),
            msg.header.seq,
            msg.body.len()
        );

        let result = next.run(ctx, msg).await;

        let elapsed = start.elapsed().as_micros() as u64;
        let status = match &result {
            Ok(resp) if resp.is_ok() => "OK".to_string(),
            Ok(resp) => {
                ctx.set_elapsed();
                format!("ERR({})", resp.header.status)
            }
            Err(e) => {
                ctx.set_elapsed();
                format!("FAIL({})", e)
            }
        };

        log::debug!(
            "[TRACE {}] ← RESPONSE: client={}, type={:?}, status={}, latency={}us",
            trace_id,
            client_id,
            msg_type_name,
            status,
            elapsed
        );

        result
    }

    fn name(&self) -> &str {
        "logging"
    }
}

// ============================================================================
// RateLimitMiddleware
// ============================================================================

/// Middleware that limits concurrent requests per client
pub struct RateLimitMiddleware {
    max_concurrent: usize,
    active: tokio::sync::Mutex<std::collections::HashMap<u64, usize>>,
}

impl RateLimitMiddleware {
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            max_concurrent,
            active: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }
}

#[async_trait]
impl Middleware for RateLimitMiddleware {
    async fn handle(
        &self,
        ctx: &mut RequestContext,
        msg: &NetMessage,
        next: &dyn NextHandler,
    ) -> NetResult<NetMessage> {
        let client_id = ctx.client.client_id;

        {
            let mut active = self.active.lock().await;
            let count = active.entry(client_id).or_insert(0);
            if *count >= self.max_concurrent {
                log::warn!(
                    "[TRACE {}] Rate limit exceeded: client={}, concurrent={}, max={}",
                    ctx.trace_id(),
                    client_id,
                    *count,
                    self.max_concurrent
                );
                let resp = NetMessage::new(
                    crate::protocol::FrameHeader::new(
                        msg.header.msg_type,
                        FrameFlags::new(FrameFlags::RESPONSE),
                        msg.header.seq,
                        0,
                    )
                    .with_status(crate::STATUS_ERR_SERVER_ERROR),
                );
                return Ok(resp);
            }
            *count += 1;
        }

        let result = next.run(ctx, msg).await;

        {
            let mut active = self.active.lock().await;
            if let Some(count) = active.get_mut(&client_id) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    active.remove(&client_id);
                }
            }
        }

        result
    }

    fn name(&self) -> &str {
        "rate_limit"
    }
}

// ============================================================================
// MetricsMiddleware
// ============================================================================

/// Middleware that updates per-client request metrics
pub struct MetricsMiddleware {
    requests: tokio::sync::Mutex<std::collections::HashMap<u64, RequestMetrics>>,
}

#[derive(Debug, Default, Clone)]
pub struct RequestMetrics {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub total_latency_us: u64,
}

impl MetricsMiddleware {
    pub fn new() -> Self {
        Self {
            requests: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    pub async fn get_metrics(&self, client_id: u64) -> Option<RequestMetrics> {
        let m = self.requests.lock().await;
        m.get(&client_id).cloned()
    }

    pub async fn get_all_metrics(&self) -> std::collections::HashMap<u64, RequestMetrics> {
        let m = self.requests.lock().await;
        m.clone()
    }
}

impl Default for MetricsMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Middleware for MetricsMiddleware {
    async fn handle(
        &self,
        ctx: &mut RequestContext,
        msg: &NetMessage,
        next: &dyn NextHandler,
    ) -> NetResult<NetMessage> {
        let client_id = ctx.client.client_id;
        let start = Instant::now();

        let result = next.run(ctx, msg).await;

        let elapsed_us = start.elapsed().as_micros() as u64;

        let mut metrics = self.requests.lock().await;
        let entry = metrics.entry(client_id).or_default();
        entry.total_requests += 1;
        entry.total_latency_us += elapsed_us;

        match &result {
            Ok(resp) if resp.is_ok() => entry.successful_requests += 1,
            _ => entry.failed_requests += 1,
        }

        result
    }

    fn name(&self) -> &str {
        "metrics"
    }
}

// ============================================================================
// TracingMiddleware
// ============================================================================

/// Middleware that adds distributed tracing span context to RequestContext
///
/// Each request gets a unique `trace_id` + `span_id`, and the timing
/// information is recorded in the context for downstream consumers.
/// Combined with an external tracing system (Jaeger, Zipkin, etc.),
/// this enables full distributed tracing across services.
pub struct TracingMiddleware;

impl TracingMiddleware {
    pub fn new() -> Self {
        Self
    }
}

impl Default for TracingMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Middleware for TracingMiddleware {
    async fn handle(
        &self,
        ctx: &mut RequestContext,
        msg: &NetMessage,
        next: &dyn NextHandler,
    ) -> NetResult<NetMessage> {
        let span_id = ctx.trace_id().to_string();
        let start = Instant::now();

        log::trace!(
            "[TRACE_START] trace={}, client={}, type={:?}, seq={}",
            span_id,
            ctx.client.client_id,
            msg.msg_type(),
            msg.header.seq
        );

        let result = next.run(ctx, msg).await;

        let elapsed = start.elapsed().as_micros() as u64;

        match &result {
            Ok(resp) if resp.is_ok() => {
                log::trace!(
                    "[TRACE_END] trace={}, status=OK, latency={}us",
                    span_id,
                    elapsed
                );
            }
            Ok(resp) => {
                log::trace!(
                    "[TRACE_END] trace={}, status=ERR({}), latency={}us",
                    span_id,
                    resp.header.status,
                    elapsed
                );
            }
            Err(e) => {
                log::trace!(
                    "[TRACE_END] trace={}, status=FAIL, latency={}us, error={}",
                    span_id,
                    elapsed,
                    e
                );
            }
        }

        result
    }

    fn name(&self) -> &str {
        "tracing"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{FrameFlags, FrameHeader, MsgType};

    fn make_test_msg() -> NetMessage {
        NetMessage::new(FrameHeader::new(
            MsgType::Lookup.as_u16(),
            FrameFlags::new(FrameFlags::REQUEST),
            1,
            0,
        ))
    }

    struct TestHandler;

    #[async_trait::async_trait]
    impl NextHandler for TestHandler {
        async fn run(&self, _ctx: &mut RequestContext, msg: &NetMessage) -> NetResult<NetMessage> {
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
    async fn test_tracing_middleware() {
        let mw = TracingMiddleware::new();
        let msg = make_test_msg();
        let info = crate::request_context::ClientInfo {
            client_id: 1,
            client_type: crate::protocol::ClientType::Fuse,
            address: "127.0.0.1:12345".parse().unwrap(),
        };
        let mut ctx = RequestContext::new(&info, &msg);
        let handler = TestHandler;

        let result = mw.handle(&mut ctx, &msg, &handler).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().header.status, crate::STATUS_OK);
    }

    #[tokio::test]
    async fn test_rate_limit_allows_requests() {
        let mw = RateLimitMiddleware::new(10);
        let msg = make_test_msg();
        let info = crate::request_context::ClientInfo {
            client_id: 1,
            client_type: crate::protocol::ClientType::Fuse,
            address: "127.0.0.1:12345".parse().unwrap(),
        };
        let handler = TestHandler;

        for _ in 0..5 {
            let mut ctx = RequestContext::new(&info, &msg);
            let result = mw.handle(&mut ctx, &msg, &handler).await;
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_rate_limit_blocks_excess() {
        let mw = RateLimitMiddleware::new(2);
        let msg = make_test_msg();
        let info = crate::request_context::ClientInfo {
            client_id: 1,
            client_type: crate::protocol::ClientType::Fuse,
            address: "127.0.0.1:12345".parse().unwrap(),
        };

        // Fill up the limit
        {
            let mut active = mw.active.lock().await;
            active.insert(1, 2);
        }

        let mut ctx = RequestContext::new(&info, &msg);
        let handler = TestHandler;
        let result = mw.handle(&mut ctx, &msg, &handler).await;
        assert!(result.is_ok());
        let resp = result.unwrap();
        assert_ne!(resp.header.status, crate::STATUS_OK);
    }
}
