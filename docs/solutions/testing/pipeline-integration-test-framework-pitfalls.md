---
title: "Pipeline Integration Test Framework: TCP, VRL, Timing, and Hyper Pitfalls"
problem_type: [build_error, runtime_error, logic_error, integration_issue]
component: "test_util/pipeline_test, topology, socket source, hyper"
symptoms:
  - "error[E0433]: could not find `pipeline_test` in `test_util` — item is gated behind test-utils feature"
  - "Deadlock: topology.sources_finished().await hangs indefinitely for TCP socket sources"
  - "Empty events: VRL object literal in build_event_from_fields creates {} without mutating target"
  - "Remap error: function call error for upcase — expected string, got null (field missing)"
  - "Events lost or flaky tests: timing race between generator close and topology.stop()"
  - "thread panicked at send_lines: Connection refused (os error 61)"
  - "thread panicked at Server::bind: Address already in use (os error 48)"
tags:
  - tcp-sources
  - topology-lifecycle
  - vrl-semantics
  - async-timing-race
  - hyper
  - codec
  - feature-gates
  - test-framework
severity: high
date: "2026-03-14"
status: solved
---

# Pipeline Integration Test Framework: TCP, VRL, Timing, and Hyper Pitfalls

Collected pitfalls encountered while implementing `src/test_util/pipeline_test/` — a
framework for running full Vector topology tests (real sources + sinks) from YAML config
files via `vector test`. Each section is an independent bug with root cause, fix, and
prevention note.

---

## 1. Feature Gate: `pipeline_test` invisible to production code

### Symptom
```
error[E0433]: failed to resolve: could not find `pipeline_test` in `test_util`
note: found an item that was configured out
  --> src/test_util/mod.rs:64:9
  |
  | #[cfg(any(test, feature = "test-utils"))]
  |      ------------------------------ the item is gated here
```

### Root Cause
`pipeline_test` was declared with `#[cfg(any(test, feature = "test-utils"))]`. But
`build_unit_tests_main()` in `src/config/unit_test/mod.rs` — which is production code
called by the `vector test` CLI command — has no feature gate, so it unconditionally
calls `crate::test_util::pipeline_test::build_pipeline_tests()`. When `test-utils` is
not in the feature set, the module is compiled out and the call site fails.

### Fix
Remove the gate. `pipeline_test` needs to be unconditionally available, same as
`event_builder` and the `unit_test` module itself:

```rust
// src/test_util/mod.rs — before
#[cfg(any(test, feature = "test-utils"))]
pub mod pipeline_test;

// after
pub mod pipeline_test;
```

### Prevention
Any module called from `src/unit_test.rs` or `src/config/unit_test/mod.rs` cannot be
behind a feature gate, because `vector test` is a production binary command. Use
`#[cfg(any(test, feature = "test-utils"))]` only for code that is _never_ reachable
from a non-test binary entrypoint.

---

## 2. `sources_finished().await` deadlocks for persistent sources

### Symptom
`vector test` hangs indefinitely after events are sent.

### Root Cause
`topology.sources_finished()` resolves only when all sources have terminated. TCP
socket sources (`type: socket, mode: tcp`) listen indefinitely — they never terminate
on their own, only when explicitly signalled by `topology.stop()`. Calling
`sources_finished().await` _before_ `stop()` creates a deadlock: the future waits for
sources to finish, but sources only finish after `stop()` is called, which is never
reached.

### Fix
Remove `sources_finished().await` entirely. `topology.stop()` sends the shutdown signal
to all sources, drains transforms, and flushes sinks before resolving:

```rust
// before
topology.sources_finished().await;
let _ = tokio::time::timeout(Duration::from_secs(30), topology.stop()).await;

// after — stop() handles everything
let _ = tokio::time::timeout(Duration::from_secs(30), topology.stop()).await;
```

### Prevention
`sources_finished()` is appropriate only for self-terminating sources (e.g.,
`demo_logs` with a `count` limit, `stdin` reaching EOF). For any source that runs a
server loop (TCP, HTTP, gRPC), always drive shutdown via `stop()`.

---

## 3. VRL object literal doesn't mutate the target

### Symptom
Events sent by generators are empty `{}`. Downstream remap transforms fail:
```
Mapping failed with event. error="function call error for "upcase" at (27:42):
expected string, got null"
```

### Root Cause
`build_event_from_fields` with `type: vrl` evaluates the source expression and then
uses `target.value.clone()` to build the `LogEvent`. But a VRL object literal like
`{ "message": "hello", "level": "info" }` evaluates to the object as its _return
value_ without mutating the target `.`. The target remains `{}` throughout, so
`target.value` is always empty.

This contrasts with field-assignment style (`.message = "hello"\n.level = "info"`)
which _does_ mutate the target's `.` root.

### Fix
Use the program's return value when it is a `Value::Object`; fall back to
`target.value` only for programs that use assignment style:

```rust
// src/test_util/event_builder.rs — before
result.program.resolve(&mut ctx).map(|_| {
    Event::Log(LogEvent::from_parts(
        target.value.clone(),   // always {} when source is an object literal
        ...
    ))
})

// after
result.program.resolve(&mut ctx).map(|return_value| {
    let event_value = match return_value {
        vrl::value::Value::Object(_) => return_value,  // object literal style
        _ => target.value.clone(),                      // assignment style
    };
    Event::Log(LogEvent::from_parts(event_value, ...))
})
```

### Prevention
When writing VRL in a `source:` field for test event generation, both styles now work:

```yaml
# Object literal style — return value is the event
source: '{ "message": "hello", "level": "info" }'

# Assignment style — target mutations are the event
source: |
  .message = "hello"
  .level = "info"
```

Do not assume VRL's final expression is implicitly assigned to `.`; that only happens
in remap transforms, not in the raw `compile` + `resolve` API.

---

## 4. Socket source uses `bytes` codec by default

### Symptom
Fields sent by the generator exist only as a raw JSON string in `.message`:
```
{ message: '{"level":"info","message":"hello world"}' }
```
Remap transforms that access `.level` or `.message` directly get `null`.

### Root Cause
Vector's `socket` source defaults to `codec: bytes` (raw text). Each newline-delimited
frame is stored verbatim as the `.message` field. The generator serializes events as
JSON, but without a JSON codec on the source side, the JSON is never parsed.

### Fix
Add `decoding.codec: json` to each socket source in pipeline test configs:

```yaml
sources:
  socket:
    type: socket
    mode: tcp
    address: "127.0.0.1:19100"
    decoding:
      codec: json   # ← required; default is "bytes"
```

### Prevention
Always explicitly declare `decoding.codec` in test configs. Never rely on source
defaults when the generator sends structured data. The mismatch is silent — the event
is created successfully, but with the wrong shape.

---

## 5. Timing race: `stop()` signals source before it reads buffered socket data

### Symptom
Tests pass on some runs and fail on others (flaky). Assertions like
`assert_eq!(length(.), 2)` occasionally see fewer events than expected.

### Root Cause
After generators send data and close the TCP connection, data sits in the OS socket
buffer. When `topology.stop()` is called immediately after, the source's tokio
`select!` loop has two branches ready simultaneously: incoming data AND the shutdown
signal. Tokio picks randomly — sometimes it reads the data first, sometimes it handles
shutdown first (discarding the unread buffer).

### Fix
Add a short sleep between generator completion and `topology.stop()` to give the
source task time to be scheduled and drain the socket before the shutdown signal
arrives:

```rust
// After all generators have sent their data:
tokio::time::sleep(Duration::from_millis(250)).await;

let _ = tokio::time::timeout(Duration::from_secs(30), topology.stop()).await;
```

250 ms is sufficient for local loopback TCP. This is test code — a sleep is the right
tool here.

### Prevention
This race is inherent in any test that (a) sends data, (b) immediately stops the
topology. A sleep is the pragmatic fix. The alternative — polling the listener for
expected event count — would require knowing expected counts in the lifecycle code,
complicating the interface unnecessarily.

---

## 6. `Connection refused`: generator connects before source binds

### Symptom
```
thread 'main' panicked at src/test_util/mod.rs:152:50:
called `Result::unwrap()` on an `Err` value:
Os { code: 61, kind: ConnectionRefused, message: "Connection refused" }
```

### Root Cause
`RunningTopology::start_validated()` returns before sources have finished binding
their ports (source tasks start asynchronously). If `generator.send()` is called
immediately after, `TcpStream::connect()` attempts to connect to a port that isn't
listening yet.

### Fix
After topology start and before generator send, wait for each generator's target
address to become reachable using `wait_for_tcp()`:

```rust
// After topology.start_validated():
for generator in &self.generators {
    if tokio::time::timeout(
        Duration::from_secs(5),
        crate::test_util::wait_for_tcp(generator.target_address()),
    )
    .await
    .is_err()
    {
        let _ = tokio::time::timeout(Duration::from_secs(5), topology.stop()).await;
        return UnitTestResult {
            errors: vec![format!(
                "source at {} did not start within 5s",
                generator.target_address()
            )],
        };
    }
}
// Now safe to send
for generator in &self.generators {
    generator.send().await?;
}
```

### Prevention
Never assume a topology source is ready immediately after `start_validated()` returns.
Always gate generator connections on a `wait_for_tcp()` readiness probe.

---

## 7. `hyper::Server::bind()` panics instead of returning an error

### Symptom
```
thread 'main' panicked at hyper/src/server/server.rs:81:13:
error binding to 127.0.0.1:19101: Address already in use (os error 48)
```

### Root Cause
`hyper::Server::bind()` calls `unwrap_or_else(|e| panic!(...))` internally — it does
not return a `Result`. A duplicate port (from a previous test run still in TIME_WAIT,
or concurrent test execution) panics the entire process rather than failing the
individual test cleanly.

### Fix
Use `Server::try_bind()` which returns `Result<Builder, hyper::Error>`:

```rust
// listeners/http.rs — before
let server = Server::bind(&addr)
    .serve(service)
    ...

// after
let server = Server::try_bind(&addr)
    .map_err(|e| format!("HttpListener failed to bind {addr}: {e}"))?
    .serve(service)
    ...
```

The `?` propagates the error back through `start() -> Result<(), String>`, which
`PipelineTest::run()` converts to a `UnitTestResult { errors: [...] }`.

### Prevention
Never use `Server::bind()` in test infrastructure. The panic is unrecoverable and
crashes the test runner rather than emitting a clean test failure. Always use
`try_bind()` and propagate errors.

---

## Quick Reference Checklist

When adding a new pipeline test YAML file:

- [ ] All socket sources have `decoding: codec: json` (no default assumed)
- [ ] Codec name is `json` (not `ndjson` — invalid; `json` handles both array and NDJSON)
- [ ] `retry_initial_backoff_secs` is ≥ 1 (0 is invalid, config parse error)
- [ ] Ports are unique across all test files (no two files share a port)
- [ ] `batch.timeout_secs: 1` on HTTP sinks (fast flush on stop)

When adding a new `TestListener` implementation:

- [ ] Use `try_bind()` / `try_bind_addr()` variants, not panic-on-fail `bind()`
- [ ] Return `Err(String)` from `start()` on bind failure
- [ ] Call `wait_for_tcp(addr)` in `start()` before returning `Ok(())`

When modifying `PipelineTest::run()`:

- [ ] `wait_for_tcp` for each generator before `generator.send()`
- [ ] Sleep ≥ 250 ms after generators complete, before `topology.stop()`
- [ ] `topology.stop()` wrapped in `tokio::time::timeout(Duration::from_secs(30), ...)`
- [ ] No `sources_finished().await` before `stop()`
