use std::net::SocketAddr;

use vector_lib::event::Event;

use super::TestGenerator;
use crate::test_util::send_lines;

/// Sends events over TCP to a socket source by serializing each event as a JSON line.
pub struct SocketGenerator {
    pub address: SocketAddr,
    pub events: Vec<Event>,
}

#[async_trait::async_trait]
impl TestGenerator for SocketGenerator {
    fn target_address(&self) -> SocketAddr {
        self.address
    }

    async fn send(&self) -> Result<(), String> {
        let lines = self
            .events
            .iter()
            .map(|e| {
                serde_json::to_string(e.as_log())
                    .map_err(|err| format!("failed to serialize event: {err}"))
            })
            .collect::<Result<Vec<_>, _>>()?;

        send_lines(self.address, lines)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}
