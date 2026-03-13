use std::net::SocketAddr;

use hyper::{Body, Method, Request, Uri};
use vector_lib::event::Event;

use super::TestGenerator;

/// Sends events via HTTP POST to an HTTP source.
pub struct HttpGenerator {
    pub uri: Uri,
    pub events: Vec<Event>,
    pub method: Method,
}

#[async_trait::async_trait]
impl TestGenerator for HttpGenerator {
    fn target_address(&self) -> SocketAddr {
        let host = self.uri.host().unwrap_or("127.0.0.1");
        let port = self.uri.port_u16().unwrap_or(80);
        format!("{host}:{port}").parse().unwrap()
    }

    async fn send(&self) -> Result<(), String> {
        let client = hyper::Client::new();
        for event in &self.events {
            let body = serde_json::to_vec(event.as_log())
                .map_err(|e| format!("failed to serialize event: {e}"))?;
            let req = Request::builder()
                .method(self.method.clone())
                .uri(self.uri.clone())
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .map_err(|e| e.to_string())?;
            client
                .request(req)
                .await
                .map_err(|e| format!("HTTP request failed: {e}"))?;
        }
        Ok(())
    }
}
