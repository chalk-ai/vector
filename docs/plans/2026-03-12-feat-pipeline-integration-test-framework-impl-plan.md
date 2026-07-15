---
title: "feat: Pipeline Integration Test Framework — Implementation Plan"
type: feat
status: active
date: 2026-03-12
origin: rfcs/2026-03-04-vector-integration-test-framework.md
---

# Pipeline Integration Test Framework — Implementation Plan

## Overview

Step-by-step implementation guide for the Pipeline Integration Test Framework described in
`rfcs/2026-03-04-vector-integration-test-framework.md` and its implementation sketch
`rfcs/2026-03-04-vector-integration-test-framework/impl-sketch.md`.

A companion plan at `docs/plans/2026-03-09-feat-vector-integration-test-framework-improvements-plan.md`
covers specific design improvements (wrapping existing utilities, dynamic port allocation). This plan
covers the full implementation sequence, file-by-file changes, and concrete acceptance criteria.

---

## Problem Statement

The existing `[[tests]]` / `vector test` framework is transform-only: sources and sinks are stripped and
replaced with synthetic components. Real sinks — encoding, batching, compression, HTTP requests — are
never exercised. Sink-level bugs reach production because the barrier to writing a full pipeline test
is too high (requires Docker, Rust code, and CI pipeline config).

This RFC adds `generators` and `listeners` to `[[tests]]` so that full pipeline tests can be declared
purely in config and run via `vector test` — no Docker, no Rust code.

---

## Key Design Decisions

- **HttpListener builds its own hyper server** (not wrapping `build_test_server_generic()`), because
  `build_test_server_generic()` only forwards bodies on 2xx responses — which would silently drop
  retry-test assertions. See impl-sketch for rationale.
- **TcpListener wraps `CountReceiver::receive_lines()`** — no new TCP server code needed.
- **SocketGenerator wraps `send_lines()`** — no new TCP client code needed.
- **Dynamic port allocation in Phase 1** (not deferred): `{{test.gen.<name>}}` /
  `{{test.listener.<name>}}` templates resolved via `next_addr()` before topology build.
- **`topology.stop().await` as the drain synchronization point** — no arbitrary sleep.
- **`RunnableTest` trait** bridges `UnitTest` and `PipelineTest` for a unified runner loop.
- **`test-utils` feature gate** (not `#[cfg(test)]`): `pipeline_test` module must be callable from
  `vector test`, which is a production binary command.

---

## Implementation Phases

### Phase 1 — Foundation (sequential, each step depends on the previous)

#### Step 1: Extract `build_input_event()` into `src/test_util/event_builder.rs`

**File:** `src/test_util/event_builder.rs` (new file)
**Source:** `src/config/unit_test/mod.rs:606-666`

The existing private function `build_input_event()` in `src/config/unit_test/mod.rs` builds an `Event`
from a `TestInput` (VRL source, raw string, log fields map, or metric). Generators need the same logic.

**Changes:**
- Copy (and make `pub`) `build_input_event()` into `src/test_util/event_builder.rs`
- Gate it behind `#[cfg(any(test, feature = "test-utils"))]`
- Add `pub mod event_builder;` to `src/test_util/mod.rs` under the same gate
- Update `src/config/unit_test/mod.rs` to call the shared version (delete the private copy, add
  `use crate::test_util::event_builder::build_input_event;`)

**Type used:** `TestInput` is in `src/config/mod.rs:550-580` — no change needed there.

---

#### Step 2: Define `RunnableTest` trait; update `build_unit_tests_main()` return type

**Files:**
- `src/config/unit_test/mod.rs` — change return type, add `RunnableTest` impl for `UnitTest`
- `src/unit_test.rs` — update runner loop to use `Box<dyn RunnableTest>`

The existing `build_unit_tests_main()` returns `Result<Vec<UnitTest>, Vec<String>>`. The runner loop
in `src/unit_test.rs` iterates over `Vec<UnitTest>` and calls `.run().await`. Changing the return
type to `Vec<Box<dyn RunnableTest>>` lets both `UnitTest` and `PipelineTest` share the same loop.

**Trait definition** (add to `src/config/unit_test/mod.rs` or a new `src/test_util/runnable_test.rs`):

```rust
#[async_trait]
pub trait RunnableTest: Send {
    fn name(&self) -> &str;
    async fn run(self: Box<Self>) -> UnitTestResult;
}
```

`UnitTestResult` is already defined in `src/config/unit_test/mod.rs`. Keep it there; import from both
sides.

**Changes:**
- Add `RunnableTest` trait (gated: `#[cfg(any(test, feature = "test-utils"))]`)
- Implement `RunnableTest for UnitTest`
- Change `build_unit_tests_main()` signature to `-> Result<Vec<Box<dyn RunnableTest>>, Vec<String>>`
- Update `src/unit_test.rs` runner loop:
  ```rust
  // before
  for test in unit_tests { results.push(test.run().await); }
  // after
  for test in unit_tests { results.push(test.run().await); }
  // (same loop body — Box<dyn RunnableTest>::run() is called via dispatch)
  ```

---

#### Step 3: Implement `TestGenerator` trait and `SocketGenerator`

**Files:**
- `src/test_util/pipeline_test/generators/mod.rs` (new) — `TestGenerator` trait
- `src/test_util/pipeline_test/generators/socket.rs` (new) — `SocketGenerator`

```rust
// generators/mod.rs
#[async_trait]
pub trait TestGenerator: Send + Sync {
    fn target_address(&self) -> SocketAddr;
    async fn send(&self) -> Result<(), String>;
}
```

`SocketGenerator` wraps `send_lines()` from `src/test_util/mod.rs:137`:

```rust
// generators/socket.rs
pub struct SocketGenerator {
    pub address: SocketAddr,
    pub events: Vec<Event>,
}

#[async_trait]
impl TestGenerator for SocketGenerator {
    fn target_address(&self) -> SocketAddr { self.address }

    async fn send(&self) -> Result<(), String> {
        let lines = self.events.iter()
            .map(|e| serde_json::to_string(e.as_log())
                .map_err(|e| format!("serialize error: {}", e)))
            .collect::<Result<Vec<_>, _>>()?;
        send_lines(self.address, lines).await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}
```

`send_lines()` is at `src/test_util/mod.rs:137` — already in the same crate.

---

#### Step 4: Implement `TestListener` trait and `HttpListener`

**Files:**
- `src/test_util/pipeline_test/listeners/mod.rs` (new) — `TestListener` trait
- `src/test_util/pipeline_test/listeners/http.rs` (new) — `HttpListener`

The impl-sketch uses a custom hyper server (not `build_test_server_generic()`) to capture bodies on
every response status — including 500 — for retry-count assertions. The `Trigger`/`Tripwire` shutdown
pattern is borrowed from `src/sinks/util/test.rs`.

```rust
// listeners/mod.rs
#[async_trait]
pub trait TestListener: Send + Sync {
    async fn start(&mut self) -> Result<(), String>;
    async fn collect(&mut self) -> Vec<Event>;
}
```

```rust
// listeners/http.rs
pub struct HttpListener {
    addr: SocketAddr,
    status_code: StatusCode,
    decompression: Option<DecompressionAlgorithm>,
    decoding: DecodingConfig,
    rx: Option<mpsc::Receiver<Bytes>>,
    trigger: Option<Trigger>,
}
```

The server captures every request body unconditionally, then drops the `Trigger` on `collect()` to
signal shutdown. `Tripwire::new()` / `Trigger` come from the `stream-cancel` crate already used in
`src/sinks/util/test.rs`.

Decompression: decompress with `flate2` (already a dep via `compression` feature) or `async-compression`.
Decoding: use Vector's existing `Decoder` / `DecodingConfig` from `codecs` crate.

`wait_for_tcp()` (at `src/test_util/mod.rs:528`) confirms the server is bound before returning.

---

#### Step 5: Config parsing — `TestGeneratorConfig`, `TestListenerConfig`, extended `TestDefinition`

**Files:**
- `src/config/mod.rs` — extend `TestDefinition` with `generators` and `listeners` fields
- New config types added adjacent to existing `TestInput` / `TestOutput`

`TestDefinition` (currently at `src/config/mod.rs:407`) gains two new fields:

```rust
#[serde(default)]
pub generators: IndexMap<String, TestGeneratorConfig>,
#[serde(default)]
pub listeners: IndexMap<String, TestListenerConfig>,
```

New enum types:

```rust
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TestGeneratorConfig {
    Socket { events: Vec<TestInput> },
    Http   { events: Vec<TestInput> },
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TestListenerConfig {
    Http {
        #[serde(default = "default_status_200")]
        status_code: u16,
        #[serde(default)]
        decompression: Option<DecompressionAlgorithm>,
        decoding: DecodingConfig,
    },
    Tcp {},
}
```

Note: no `port` or `address` fields — these are allocated dynamically via template substitution (Step 7).

Gate: `#[cfg(any(test, feature = "test-utils"))]` on the new fields and types.

---

#### Step 6: VRL array assertion runner

**File:** `src/test_util/pipeline_test/assertions.rs` (new)

The assertion model differs from unit tests: VRL conditions receive `.` as an **array of all events**
rather than one event at a time. This enables count checks (`assert_eq!(length(.), 2)`) and
cross-event assertions.

```rust
pub fn run_vrl_assertion(
    condition: &Condition,
    events: &[Event],
) -> Result<(), String> {
    // Build a VRL Value::Array from the event slice
    let array_value = Value::Array(
        events.iter()
            .map(|e| e.as_log().clone().into())
            .collect()
    );
    // Create a synthetic event whose root is the array
    let mut target_event = Event::Log(LogEvent::default());
    target_event.as_mut_log().insert(log_schema().message_key(), array_value);
    // Run the condition
    match condition.check_with_context(&mut target_event) {
        Ok(_) => Ok(()),
        Err(e) => Err(e),
    }
}
```

The existing `Condition` type is used by `OutputCheck` in unit tests — it already supports VRL source
compilation. No new condition types are needed; the `type: vrl` / `source:` YAML fields map to the
existing `VrlConfig` variant.

---

#### Step 7: Config template substitution and `build_pipeline_tests()`

**File:** `src/test_util/pipeline_test/mod.rs` (new)

This is the highest-complexity step. It implements:

1. **`resolve_test_addresses()`**: Scan raw config string for `{{test.gen.<name>}}` and
   `{{test.listener.<name>}}` patterns, call `next_addr()` for each unique name, return a substituted
   config string and an `AddressMap` (`HashMap<String, (SocketAddr, PortGuard)>`).

2. **`classify_test_config()`**: Inspect `tests[].generators` / `tests[].listeners` — return `Pipeline`,
   `Unit`, or `Mixed`.

3. **`build_pipeline_tests()`**: Load config → resolve addresses → parse topology → build pieces →
   instantiate generators/listeners → return `Vec<Box<dyn RunnableTest>>`.

4. **`PipelineTest::run()`**: Full lifecycle (start listeners → start topology → wait for sources →
   run generators → stop topology → collect → assert).

**`build_unit_tests_main()` dispatch:**

```rust
pub async fn build_unit_tests_main(
    paths: &[ConfigPath],
    signal_handler: &mut signal::SignalHandler,
) -> Result<Vec<Box<dyn RunnableTest>>, Vec<String>> {
    let raw = load_raw_config(paths)?;
    match classify_test_config(&raw) {
        TestConfigKind::Pipeline => build_pipeline_tests(paths, raw).await,
        TestConfigKind::Unit => build_unit_tests(paths).await,
        TestConfigKind::Mixed => Err(vec![
            "mixed pipeline and unit test definitions are not supported \
             in the same file — split into separate files".into()
        ]),
    }
}
```

**Template substitution detail:**

```
Pattern: {{test.gen.NAME}}   → 127.0.0.1:<port>   (generator address)
Pattern: {{test.listener.NAME}} → 127.0.0.1:<port>  (listener address / used in sink URI)
```

For sink URIs, `{{test.listener.out}}` in `uri: "http://{{test.listener.out}}/"` must substitute to
`127.0.0.1:PORT` (not just `:PORT`). The regex `\{\{test\.(gen|listener)\.([a-z_][a-z0-9_]*)\}\}`
captures kind and name; substitution is a simple string replace on the raw YAML/TOML before parsing.

---

### Phase 2 — Test Coverage (parallel after Phase 1)

#### Step 8: First pipeline test — socket → remap → HTTP sink

**File:** `tests/behavior/pipelines/http_sink.yml` (new)

Uses the template syntax from Phase 1. Matches RFC Example 1.

```yaml
sources:
  socket:
    type: socket
    mode: tcp
    address: "{{test.gen.gen}}"

transforms:
  parse:
    inputs: ["socket"]
    type: remap
    source: |
      .parsed = true
      .severity = upcase!(.level)
      del(.level)

sinks:
  http_out:
    inputs: ["parse"]
    type: http
    encoding:
      codec: json
    uri: "http://{{test.listener.out}}/"
    batch:
      timeout_secs: 1

tests:
  - name: "transforms and sends two events"
    generators:
      gen:
        type: socket
        events:
          - source: '{ "message": "hello world", "level": "info" }'
          - source: '{ "message": "something failed", "level": "error" }'

    listeners:
      out:
        type: http
        decoding:
          codec: json

    outputs:
      - extract_from: out
        conditions:
          - type: vrl
            source: |
              assert!(is_array(.))
              assert_eq!(length(.), 2)
              assert_eq!(.[0].message, "hello world")
              assert_eq!(.[0].severity, "INFO")
              assert_eq!(.[0].parsed, true)
              assert!(!exists(.[0].level))
              assert_eq!(.[1].message, "something failed")
              assert_eq!(.[1].severity, "ERROR")
```

---

#### Step 9: Pipeline test for `route` transform with two sinks

**File:** `tests/behavior/pipelines/route_multi_output.yml` (new)

Matches RFC Example 2.

---

#### Step 10: Pipeline test for gzip + ndjson

**File:** `tests/behavior/pipelines/compression.yml` (new)

Matches RFC Example 4. Validates `HttpListener` decompression path.

---

#### Step 11: `HttpGenerator` for HTTP source testing

**File:** `src/test_util/pipeline_test/generators/http.rs` (new)

Uses `hyper::Client` (already a workspace dep) to POST events to an `http_server` source.
Registered alongside `SocketGenerator` in `TestGeneratorConfig::Http`.

---

#### Step 12: `TcpListener` for socket sink testing

**File:** `src/test_util/pipeline_test/listeners/tcp.rs` (new)

Wraps `CountReceiver::receive_lines()` from `src/test_util/mod.rs:641`. The `TcpListener` binds in
`start()` and awaits the `CountReceiver` future in `collect()`. Each received line is parsed as a
`LogEvent`.

---

#### Step 13: Retry-count test

**File:** `tests/behavior/pipelines/retry_on_error.yml` (new)

Matches RFC Example 3. Uses `HttpListener` with `status_code: 500` and asserts `length(.) >= 2`.
Validates that the custom hyper server captures bodies even on non-2xx responses.

---

#### Step 14: Documentation

**File:** `docs/DEVELOPING.md`

Add a "Pipeline Integration Tests" section explaining:
- When to use pipeline tests vs unit tests vs Docker integration tests
- Config template syntax (`{{test.gen.NAME}}`, `{{test.listener.NAME}}`)
- How to run: `vector test tests/behavior/pipelines/http_sink.yml`
- VRL array assertion model

---

## File Organization

```
src/test_util/
├── event_builder.rs         # Step 1: shared build_input_event()
├── pipeline_test/
│   ├── mod.rs               # Step 7: build_pipeline_tests(), PipelineTest, template substitution
│   ├── generators/
│   │   ├── mod.rs           # Step 3: TestGenerator trait
│   │   ├── socket.rs        # Step 3: SocketGenerator (wraps send_lines)
│   │   └── http.rs          # Step 11: HttpGenerator (wraps hyper::Client)
│   ├── listeners/
│   │   ├── mod.rs           # Step 4: TestListener trait
│   │   ├── http.rs          # Step 4: HttpListener (custom hyper, not build_test_server_generic)
│   │   └── tcp.rs           # Step 12: TcpListener (wraps CountReceiver::receive_lines)
│   └── assertions.rs        # Step 6: VRL array assertion runner
└── mod.rs                   # add: pub mod event_builder; pub mod pipeline_test;

src/config/
├── mod.rs                   # Step 5: extend TestDefinition, add config enums
└── unit_test/
    └── mod.rs               # Step 2: RunnableTest trait, change return type, dispatch

src/unit_test.rs             # Step 2: update runner loop for Box<dyn RunnableTest>

tests/behavior/
└── pipelines/               # Steps 8-10, 13
    ├── http_sink.yml
    ├── route_multi_output.yml
    ├── compression.yml
    └── retry_on_error.yml

docs/DEVELOPING.md           # Step 14
```

---

## Critical Implementation Notes

### `test-utils` feature gate (not `#[cfg(test)]`)

The `pipeline_test` module is called from `vector test`, which is a production binary (not a test
binary). Gate all new code behind `#[cfg(any(test, feature = "test-utils"))]` — the same gate used
for `src/test_util/components.rs`. Never use `#[cfg(test)]` alone for code that must be reachable
from `vector test`.

`test-utils` is in `Cargo.toml` at line 1034:
```toml
test-utils = ["vector-lib/test"]
```

For development builds, add `test-utils` to the default features so `vector test` can use pipeline
tests locally. CI uses `--features test-utils` when running `vector test`.

### `build_test_server_generic()` is `#[cfg(test)]` — cannot be reused

`src/sinks/util/test.rs` is fully `#[cfg(test)]` gated. `HttpListener` cannot call it. The
impl-sketch custom hyper server is the correct approach — it also fixes the body-capture-on-non-2xx
gap. See impl-sketch section "HttpListener" for full rationale.

### Topology `stop()` semantics

`RunningTopology::stop()` at `src/topology/running.rs:148` flushes all buffered events through sinks
before resolving. The 30-second outer `tokio::time::timeout` in `PipelineTest::run()` guards against
stuck sinks (e.g., a sink retrying indefinitely against a 500 listener). Test configs should set
`batch.timeout_secs: 1` to keep shutdown fast.

### `next_addr()` and `PortGuard` lifetime

`next_addr()` at `src/test_util/addr.rs:66` returns `(PortGuard, SocketAddr)`. The `PortGuard` must
be kept alive for the entire test duration — store it in `PipelineTest._port_guards: Vec<PortGuard>`.
Dropping a `PortGuard` removes the port from the global reservation set, potentially allowing
another concurrent test to reuse it.

### `classify_test_config()` strategy

Classification is done on the parsed `Vec<TestDefinition>` (not raw YAML). A test has `generators`
or `listeners` if either map is non-empty. The `Mixed` case (some tests have them, some don't) is a
config error. Empty `generators` and `listeners` maps are the zero-value default from `#[serde(default)]`,
so a pure-unit-test config file never hits the `Pipeline` branch.

### VRL array assertion — `.` root semantics

The unit test framework runs VRL conditions with `.` bound to a single `Event`. The pipeline
assertion runner must bind `.` to the collected events array. The simplest approach: construct a
`Value::Array(events_as_vrl_values)` and pass it as the VRL target's root. The existing VRL runtime
in Vector (`lib/vrl/`) supports this via `vrl::state::RuntimeState` with a custom target.

---

## Acceptance Criteria

### Functional

- [ ] `vector test tests/behavior/pipelines/http_sink.yml` passes (RFC Example 1)
- [ ] `vector test tests/behavior/pipelines/route_multi_output.yml` passes (RFC Example 2)
- [ ] `vector test tests/behavior/pipelines/retry_on_error.yml` passes (RFC Example 3)
- [ ] `vector test tests/behavior/pipelines/compression.yml` passes (RFC Example 4)
- [ ] Mixed pipeline+unit test file returns a descriptive error
- [ ] Existing `vector test` unit tests are unaffected

### Design

- [x] Explicit hardcoded ports in test configs (template substitution deferred to Part 2)
- [x] `HttpListener` captures bodies on all response codes (including 500)
- [x] `SocketGenerator` calls `send_lines()` — no new TCP client code
- [x] `TcpListener` calls `CountReceiver::receive_lines()` — no new TCP server code
- [x] Drain uses `topology.stop().await` — no `sleep()`
- [x] No `#[cfg(test)]` gate on any pipeline_test code (use `test-utils` feature)

### Quality

- [x] `make check-clippy` passes with no new warnings
- [x] `make check-fmt` passes
- [ ] All Phase 2 test YAML files run in CI without Docker

---

## Dependencies & Prerequisites

All dependencies already present in the workspace:
- `hyper` 0.14.32 — HTTP client and server for `HttpGenerator` and `HttpListener`
- `stream-cancel` — `Trigger`/`Tripwire` shutdown pattern (used in `src/sinks/util/test.rs`)
- `flate2` / `async-compression` — gzip/zstd decompression in `HttpListener`
- `serde_json` — event serialization in `SocketGenerator`
- `tokio::sync::mpsc` — body capture channel in `HttpListener`
- `async-trait` — for `TestGenerator` / `TestListener` / `RunnableTest` traits
- `vrl` workspace crate — VRL assertion compilation and execution

---

## Risk Analysis

| Risk | Likelihood | Mitigation |
|---|---|---|
| `topology.stop()` doesn't flush before timeout | Medium | Set `batch.timeout_secs: 1` in test configs; 30s outer timeout surfaces stuck sinks |
| Port reservation races in fast parallel CI | Low | `PortGuard` + global `RESERVED_PORTS` set prevents reuse; already battle-tested |
| VRL array `.` binding semantics unclear | Medium | Study `src/conditions/vrl.rs` and `lib/vrl/` runtime before Step 6 |
| `build_test_server_generic()` gating | Known | Confirmed `#[cfg(test)]` — custom hyper server is the correct path |
| `TestDefinition` serde changes break existing configs | Low | New fields use `#[serde(default)]` — existing configs parse without generators/listeners |

---

## Sources & References

### RFC and Design Documents

- **RFC:** `rfcs/2026-03-04-vector-integration-test-framework.md`
- **Implementation sketch:** `rfcs/2026-03-04-vector-integration-test-framework/impl-sketch.md`
- **Improvements plan:** `docs/plans/2026-03-09-feat-vector-integration-test-framework-improvements-plan.md`

### Key Existing Utilities

- `build_input_event()` to extract: `src/config/unit_test/mod.rs:606`
- `send_lines()`: `src/test_util/mod.rs:137`
- `CountReceiver::receive_lines()`: `src/test_util/mod.rs:641`
- `wait_for_tcp()`: `src/test_util/mod.rs:528`
- `next_addr()` / `PortGuard`: `src/test_util/addr.rs:66`
- `UnitTest` struct: `src/config/unit_test/mod.rs:51`
- `TestDefinition` struct: `src/config/mod.rs:407`
- `RunningTopology::start_validated()`: `src/topology/running.rs:1275`
- `RunningTopology::stop()`: `src/topology/running.rs:148`

### Prior Art (Topology Patterns)

- End-to-end test pattern: `src/topology/test/end_to_end.rs`
- HTTP sink tests (declarative equivalent): `src/sinks/http/tests.rs`
- `Trigger`/`Tripwire` shutdown: `src/sinks/util/test.rs:69`
