use std::net::SocketAddr;

pub mod http;
pub mod socket;

pub use http::HttpGenerator;
pub use socket::SocketGenerator;

/// A generator sends test events into a real Vector source.
#[async_trait::async_trait]
pub trait TestGenerator: Send + Sync {
    /// The address of the source this generator connects to.
    fn target_address(&self) -> SocketAddr;
    /// Send all configured events to the source.
    async fn send(&self) -> Result<(), String>;
}
