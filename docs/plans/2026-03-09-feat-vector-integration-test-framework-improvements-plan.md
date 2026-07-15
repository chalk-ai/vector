---
title: "feat: Vector Pipeline Integration Test Framework - Implementation Improvements"
type: feat
status: active
date: 2026-03-09
origin: rfcs/2026-03-04-vector-integration-test-framework.md
---

# Vector Pipeline Integration Test Framework - Implementation Improvements

## Overview

The RFC (`rfcs/2026-03-04-vector-integration-test-framework.md`) proposes a solid design for
pipeline integration tests. This plan surfaces concrete implementation improvements by leveraging
existing Vector test utilities instead of building new infrastructure from scratch, and promotes
automatic port allocation from a "future improvement" to a Phase 1 requirement.

## Key Findings

Vector already has battle-tested test utilities that cover most of what the RFC proposes building
from scratch. The RFC's implementation section should be revised to reuse these rather than
reinvent them.

### Existing utilities that map directly to RFC proposals

| RFC Proposal | Existing Utility | Location |
|---|---|---|
| `HttpListener` (hyper server) | `build_test_server_generic()` | `src/sinks/util/test.rs:69` |
| `TcpListener` | `CountReceiver::receive_lines()` | `src/test_util/mod.rs:641` |
| `SocketGenerator::send()` | `send_lines()` | `src/test_util/mod.rs:137` |
| HTTP server spawn helper | `spawn_blackhole_http_server()` | `src/test_util/http.rs:15` |
| Gzip decompression | `get_received_gzip()` | `src/sinks/util/test.rs:116` |
| Zlib decompression | `get_received_zlib()` | `src/sinks/util/test.rs:123` |
| Port allocation | `next_addr()` / `PortGuard` | `src/test_util/addr.rs:66` |
| TCP readiness | `wait_for_tcp()` | `src/test_util/mod.rs:528` |
| Graceful server shutdown | `Trigger` / `Tripwire` | `src/test_util/mod.rs` |

---

## Proposed Changes to RFC Implementation Section

### Change 1: Automatic port allocation via config templates (promote from "future" to Phase 1)

**Problem in RFC:** Examples use hardcoded ports (9000, 9001, 9002). The RFC acknowledges this
causes parallel test conflicts but defers it as a future improvement.

**Proposed fix:** Auto-allocate ports at framework startup using `next_addr()` and inject them into
the pipeline config via template substitution before building the topology.

**Config syntax:**

```yaml
sources:
  socket:
    address: "{{test.gen.addr}}"      # auto-allocated, matched to gen generator

sinks:
  http_out:
    inputs: ["parse"]
    type: http
    encoding:
      codec: json
    uri: "http://{{test.listener.out}}/"  # auto-allocated, matched to out listener

tests:
  - name: "transforms and sends two events"
    generators:
      gen:
        type: socket
        events:
          - source: '{ "message": "hello world", "level": "info" }'

    listeners:
      out:
        type: http
        decoding:
          codec: json
```

**How it works:**
1. Framework scans config for `{{test.gen.<name>}}` and `{{test.listener.<name>}}` placeholders
2. For each placeholder, calls `next_addr()` to allocate a port with a `PortGuard`
3. Performs string substitution in raw config TOML/YAML before parsing the topology
4. Keeps `PortGuard` values alive for the duration of the test

**Why `next_addr()` instead of hardcoded:** The existing `PortGuard` mechanism is thread-safe,
prevents races between concurrent tests, and is already used throughout Vector's test suite.
See `src/test_util/addr.rs:66-95` for the implementation.

**Drawback addressed:** Parallel test execution becomes safe by default. No test file coordination
needed.

---

### Change 2: `HttpListener` wraps `build_test_server_generic()` instead of raw hyper

**Problem in RFC:** The proposed `HttpListener` implementation (`rfcs/...#http-listener-implementation`)
manually builds a hyper server, handles decompression inline, and uses `Arc<Mutex<Vec<Bytes>>>` for
capture. This duplicates logic already in `src/sinks/util/test.rs`.

**Proposed implementation:**

```rust
// src/test_util/pipeline_test/listeners/http.rs

pub struct HttpListener {
    addr: SocketAddr,
    status_code: StatusCode,
    decompression: Option<Decompression>,
    decoding: DecodingConfig,
    // populated after start()
    rx: Option<mpsc::Receiver<(Parts, Bytes)>>,
    trigger: Option<Trigger>,
}

#[async_trait]
impl TestListener for HttpListener {
    async fn start(&mut self) -> Result<(), String> {
        let status = self.status_code;
        let (rx, trigger, server) = build_test_server_generic(self.addr, move || {
            Response::builder()
                .status(status)
                .body(Body::empty())
                .unwrap()
        });
        tokio::spawn(server);
        self.rx = Some(rx);
        self.trigger = Some(trigger);
        wait_for_tcp(self.addr).await;
        Ok(())
    }

    async fn collect(&mut self) -> Vec<Event> {
        // Drop trigger to signal shutdown, drain channel
        drop(self.trigger.take());
        let rx = self.rx.take().unwrap();

        let bodies: Vec<(Parts, Bytes)> = rx.collect().await;

        match self.decompression {
            Some(Decompression::Gzip) => {
                // Reuse get_received_gzip() pattern directly
                bodies.into_iter()
                    .flat_map(|(_, body)| decompress_gzip_body(&body))
                    .flat_map(|raw| decode_body(raw, &self.decoding))
                    .collect()
            }
            // ... other codecs
            None => bodies.into_iter()
                .flat_map(|(_, body)| decode_body(body.to_vec(), &self.decoding))
                .collect()
        }
    }
}
```

**Benefit:** The full machinery of `build_test_server_generic()` — hyper server setup, async body
collection, Trigger/Tripwire shutdown — is reused without duplication. The `HttpListener` only adds
decompression and decoding logic on top of captured bytes.

**Reference:** `src/sinks/util/test.rs:69-114` for `build_test_server_generic()` signature and
behaviour.

---

### Change 3: `TcpListener` wraps `CountReceiver::receive_lines()`

**Problem in RFC:** `TcpListener` is listed as a planned component (`src/test_util/pipeline_test/listeners/tcp.rs`) but has no implementation sketch. `CountReceiver::receive_lines()` already does exactly this.

**Proposed implementation:**

```rust
// src/test_util/pipeline_test/listeners/tcp.rs

pub struct TcpListener {
    addr: SocketAddr,
    receiver: Option<CountReceiver<String>>,
}

#[async_trait]
impl TestListener for TcpListener {
    async fn start(&mut self) -> Result<(), String> {
        // CountReceiver::receive_lines binds the port immediately
        self.receiver = Some(CountReceiver::receive_lines(self.addr));
        wait_for_tcp(self.addr).await;
        Ok(())
    }

    async fn collect(&mut self) -> Vec<Event> {
        let lines = self.receiver.take().unwrap().await;
        lines.into_iter()
            .map(|line| Event::Log(LogEvent::from_str_legacy(line)))
            .collect()
    }
}
```

**Benefit:** Zero new TCP listener code. `CountReceiver::receive_lines()` already handles
`TcpListenerStream`, `FramedRead` with `LinesCodec`, atomic counting, and graceful shutdown.

**Reference:** `src/test_util/mod.rs:641-697`.

---

### Change 4: `SocketGenerator` wraps `send_lines()`

**Problem in RFC:** The `SocketGenerator::send()` reimplements TCP connection + `LinesCodec` +
line-by-line write. `send_lines()` already does this.

**Proposed implementation:**

```rust
// src/test_util/pipeline_test/generators/socket.rs

pub struct SocketGenerator {
    address: SocketAddr,
    events: Vec<Event>,
}

#[async_trait]
impl TestGenerator for SocketGenerator {
    fn target_address(&self) -> SocketAddr {
        self.address
    }

    async fn send(&self) -> Result<(), String> {
        let lines = self.events.iter()
            .map(|e| serde_json::to_string(e.as_log()).unwrap())
            .collect::<Vec<_>>();

        send_lines(self.address, lines)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}
```

**Benefit:** `send_lines()` handles connection, codec framing, and shutdown. No new TCP client code.

**Reference:** `src/test_util/mod.rs:137-162`.

---

### Change 5: Replace `sleep(2s)` drain with topology shutdown signal

**Problem in RFC:** The lifecycle uses `tokio::time::sleep(Duration::from_secs(2))` before
collecting. This is fragile — too short under load, wastes time when fast.

**Better approach:** Call `topology.stop().await` and wait for it to complete. `RunningTopology::stop()`
flushes all buffered events through sinks before returning. There is no need for an arbitrary sleep:

```rust
// 4. Run generators
for generator in &self.generators {
    generator.send().await;
}

// 5. Stop topology — this flushes all sinks before returning
topology.stop().await;

// 6. Collect from listeners (sinks have flushed by now)
let mut collected = HashMap::new();
for (name, listener) in &mut self.listeners {
    collected.insert(name.clone(), listener.collect().await);
}
```

If the sink has a non-zero `batch.timeout_secs`, the topology shutdown will wait for batches to
flush. This is the correct synchronization point.

**Fallback:** For sinks with very long batch timeouts in test configs, the test config should set
`batch.timeout_secs: 1` (or similar small value). This is already standard practice in Vector's
existing sink tests.

---

### Change 6: `HttpGenerator` wraps `hyper::Client` (already a dep)

The RFC lists `HttpGenerator` as a planned generator for testing HTTP sources. Since `hyper` is
already a workspace dependency (`0.14.32`), the implementation is straightforward:

```rust
// src/test_util/pipeline_test/generators/http.rs

pub struct HttpGenerator {
    uri: Uri,
    events: Vec<Event>,
    method: Method,
}

#[async_trait]
impl TestGenerator for HttpGenerator {
    fn target_address(&self) -> SocketAddr {
        // parse from uri
    }

    async fn send(&self) -> Result<(), String> {
        let client = hyper::Client::new();
        for event in &self.events {
            let body = serde_json::to_vec(event.as_log()).unwrap();
            let req = Request::builder()
                .method(self.method.clone())
                .uri(self.uri.clone())
                .header("Content-Type", "application/json")
                .body(Body::from(body))
                .unwrap();
            client.request(req).await.map_err(|e| e.to_string())?;
        }
        Ok(())
    }
}
```

No new HTTP client dependency needed.

---

## Updated Plan of Attack

Revised from the RFC, with the above changes incorporated:

### Phase 1: Foundation (deferred items promoted)

- [ ] Implement config template substitution for `{{test.gen.<name>}}` and
  `{{test.listener.<name>}}` using `next_addr()` — replaces hardcoded ports
- [ ] Extract `build_input_event()` into `src/test_util/event_builder.rs` (as RFC proposes)
- [ ] Define `TestGenerator` and `TestListener` traits (as RFC proposes)

### Phase 2: Core Implementations (using existing utilities)

- [ ] `SocketGenerator` wrapping `send_lines()`
- [ ] `HttpListener` wrapping `build_test_server_generic()` with decompression + decoding
- [ ] `TcpListener` wrapping `CountReceiver::receive_lines()`
- [ ] `HttpGenerator` using `hyper::Client`

### Phase 3: Framework Wiring

- [ ] Implement `tests[].generators` and `tests[].listeners` config parsing
- [ ] Implement `build_pipeline_tests()` alongside `build_unit_tests()`
- [ ] Implement VRL array assertion runner (pass `.` as array of events)
- [ ] Extend `build_unit_tests_main()` to detect and dispatch pipeline tests
- [ ] Fix drain: replace `sleep(2s)` with `topology.stop().await`

### Phase 4: Test Coverage

- [ ] Write first pipeline test: socket source → remap → HTTP sink → HTTP listener
- [ ] Write pipeline test for `route` transform with two HTTP sinks
- [ ] Write pipeline test for gzip compression + ndjson encoding
- [ ] Add pipeline test examples to `tests/behavior/pipelines/`

### Phase 5: Docs

- [ ] Add documentation in `docs/DEVELOPING.md`
- [ ] Update RFC with implementation decisions

---

## Updated File Organization

```
src/test_util/
├── pipeline_test/
│   ├── mod.rs              # build_pipeline_tests(), PipelineTest, template substitution
│   ├── generators/
│   │   ├── mod.rs           # TestGenerator trait
│   │   ├── socket.rs        # SocketGenerator — wraps send_lines()
│   │   └── http.rs          # HttpGenerator — wraps hyper::Client
│   ├── listeners/
│   │   ├── mod.rs           # TestListener trait
│   │   ├── http.rs          # HttpListener — wraps build_test_server_generic()
│   │   └── tcp.rs           # TcpListener — wraps CountReceiver::receive_lines()
│   └── assertions.rs        # VRL array assertion runner
├── event_builder.rs         # Shared: build Event from VRL/raw/log/metric defs
├── mod.rs                   # (add pub mod pipeline_test)
└── ...
```

---

## Acceptance Criteria

- [ ] Tests with `tests[].generators` or `tests[].listeners` run via `vector test`
- [ ] No hardcoded ports in test configs — all addresses use `{{test.*}}` templates
- [ ] `HttpListener` uses `build_test_server_generic()` — no duplicate hyper server code
- [ ] `TcpListener` uses `CountReceiver::receive_lines()` — no new TCP server code
- [ ] `SocketGenerator` uses `send_lines()` — no new TCP client code
- [ ] Drain uses `topology.stop().await` — no `sleep(2s)`
- [ ] All four example tests from the RFC pass (Examples 1–4)
- [ ] Tests can run in parallel without port conflicts

---

## Sources & References

### Origin

- **RFC:** `rfcs/2026-03-04-vector-integration-test-framework.md`

### Existing utilities to reuse

- `build_test_server_generic()`: `src/sinks/util/test.rs:69`
- `build_test_server()`: `src/sinks/util/test.rs:43`
- `get_received_gzip()`: `src/sinks/util/test.rs:116`
- `CountReceiver::receive_lines()`: `src/test_util/mod.rs:641`
- `send_lines()`: `src/test_util/mod.rs:137`
- `spawn_blackhole_http_server()`: `src/test_util/http.rs:15`
- `wait_for_tcp()`: `src/test_util/mod.rs:528`
- `next_addr()` / `PortGuard`: `src/test_util/addr.rs:66`
- `Trigger` / `Tripwire` pattern: used throughout `src/sinks/util/test.rs`

### Prior art in codebase

- End-to-end test pattern: `src/topology/test/end_to_end.rs`
- HTTP sink integration tests: `src/sinks/http/tests.rs`
- Socket source tests: `src/sources/socket/mod.rs` (see `wait_for_tcp_and_release`)
