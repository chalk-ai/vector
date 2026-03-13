use vector_lib::event::Event;

pub mod http;
pub mod tcp;

pub use http::HttpListener;
pub use tcp::TcpListener;

/// A listener captures data from a real Vector sink during a pipeline test.
#[async_trait::async_trait]
pub trait TestListener: Send + Sync {
    /// Bind to the configured port and start accepting connections.
    async fn start(&mut self) -> Result<(), String>;
    /// Stop accepting and return all captured events.
    async fn collect(&mut self) -> Vec<Event>;
}
