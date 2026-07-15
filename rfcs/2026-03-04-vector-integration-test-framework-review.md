# RFC Review: Pipeline Integration Test Framework

**Target:** `rfcs/2026-03-04-vector-integration-test-framework.md`
**Date:** 2026-03-10

The RFC is well-motivated with strong prior art grounding. Three issues need to be resolved before implementation begins; the rest are clarifications.

---

## Critical (must fix before implementation)

### 1. `topology.stop()` claim is insufficiently justified — and has a deadlock risk

The RFC states "No grace period needed; `topology.stop()` is the correct synchronization point." This is partially correct but incomplete:

- When `build_pipeline_tests` constructs a `Config`, `graceful_shutdown_duration` will be `None` by default, which means `stop()` uses `Box::pin(future::pending())` as the timeout arm — it waits forever. If a sink is stuck retrying against a 500 listener (Example 3), `stop().await` will never return.
- The RFC must add a per-test timeout around `run()` (e.g., `tokio::time::timeout(Duration::from_secs(30), ...)`) so a misbehaving sink doesn't deadlock the test suite.
- The drawback "Sink flush timing" appears in both the Drawbacks section and Outstanding Questions with different framings. Pick one.

### 2. `build_test_server_generic()` only captures bodies on 2xx responses

`build_test_server_generic()` at `src/sinks/util/test.rs:94` only sends to the channel when the response status is success. Example 3 configures a 500 listener to test retry behavior. The `HttpListener` wrapping this function will receive no captured events despite the sink retrying multiple times, making `assert!(length(.) >= 2)` silently fail with a misleading assertion error rather than an infrastructure error. This is a concrete behavioral bug in the design — `HttpListener` cannot wrap `build_test_server_generic()` directly for error-status testing.

### 3. Mixed-mode dispatch is unspecified

`has_pipeline_test_components()` routes the *entire file* to either `build_pipeline_tests` or `build_unit_tests`. A config with some tests using generators and some using the existing `[[tests]]` format is silently broken. The RFC must specify: are mixed configs forbidden (and if so, what error), or will both types coexist? If coexistence, `build_unit_tests_main` needs to return both result sets.

---

## High (significant gaps)

### 4. `RunnableTest` trait doesn't exist; signature change unspecified

The RFC pseudo-code shows `build_unit_tests_main` returning `Vec<Box<dyn RunnableTest>>`, but no such trait exists. The current signature returns `Vec<UnitTest>`. The RFC must specify the trait definition and how `UnitTest` and `PipelineTest` implement it, since this affects a public API in `src/config/mod.rs` and its call site in `src/unit_test.rs`.

### 5. Config schema for generators/listeners is entirely absent

The RFC describes the YAML shape but provides no Rust config structs or serde definitions for `TestGeneratorConfig` or `TestListenerConfig`. The discriminated union (e.g., `type: socket` vs `type: http`) and how they integrate into the existing `TestDefinition` struct need at least a sketch. This is the most implementation-critical missing piece.

### 6. Error handling panics instead of failing tests

Multiple `.unwrap()` calls produce panics instead of test failures:

- `RunningTopology::start_validated(...).await.unwrap()` — `start_validated` returns `Option`, so config errors become panics
- `self.rx.take().unwrap()` in `collect()` — panics if `start()` was never called or failed (and `start()` results are currently discarded in the lifecycle loop)
- `serde_json::to_string(e.as_log()).unwrap()` — panics on metric/trace events (the RFC says generators support the same types as existing `[[tests]]` inputs, including metrics)

All three should surface as `UnitTestResult { errors }` rather than panics.

### 7. `wait_for_tcp` panics instead of failing the test

`wait_for_tcp()` panics after 5 seconds if the source never binds (e.g., port already in use). This surfaces as a test panic rather than a test failure, making CI output unreadable. Wrap it in a timeout that returns a `UnitTestResult` error.

---

## Moderate

### 8. Ordering and batching semantics of collected events are unstated

Example 1 asserts `.[0].message == "hello world"` — but is ordering guaranteed? What happens when the HTTP sink batches both events into a single HTTP request body? Does the listener produce two events from one request body, and in what order? This affects whether index-based assertions are reliable and needs to be stated explicitly.

### 9. `PortGuard` has no home in `PipelineTest`

Part 2 allocates ports via `next_addr()` which returns `(PortGuard, SocketAddr)`. The `PortGuard` must be kept alive for the test duration or the port is released and could be reallocated. `PipelineTest` has no `_port_guards: Vec<PortGuard>` field. Add it explicitly.

### 10. Generator `source:` field name is ambiguous

The examples use:

```yaml
events:
  - source: '{ "message": "hello world" }'
```

But `build_input_event()` in `src/config/unit_test/mod.rs:607` uses `type: "vrl"` with a `value` field, not `source`. If generators reuse `build_input_event()` directly, the YAML schema must match. If `source:` is a new shorthand for VRL input, that's a design decision that needs to be explicit.

### 11. Feature gate for `src/test_util/pipeline_test/` is unspecified

`src/test_util/` is gated with `#[cfg(test)]` in many places, but `build_pipeline_tests()` must be callable from `vector test` (a production binary command). The RFC must specify which feature gate (`test-utils`? a new gate?) this module lives behind.

---

## Minor

### 12. Example 3 retry test is inherently flaky

`retry_max_duration_secs: 1` with `assert!(length(.) >= 2)` depends on the retry rate fitting within 1 second. On slow CI machines this may not produce 2 attempts. Either raise the timeout or mark this as illustrative-only.

### 13. "Port conflicts mitigated" overstates Part 1 safety

The drawbacks say conflicts are "mitigated initially by using unique ports per test file." This only holds for strictly sequential single-process runs. Parallel CI jobs on the same host will still conflict. Replace with: "not safe for parallel execution in Part 1; Part 2 is the fix."

### 14. Comparison table: "Kept (relevant subset)" for transforms is imprecise

Change to "Kept (declared path only)" to match what the existing unit test framework actually does.

### 15. Plan of Attack lacks dependency ordering

Steps 1–6 are prerequisites for Steps 7–13 but read as an unordered checklist. Add a note indicating the dependency order.

---

## What the plan doc gets right

`docs/plans/2026-03-09-feat-vector-integration-test-framework-improvements-plan.md` correctly identifies the most important implementation fix (promoting auto-port allocation to Phase 1) and maps all four proposed implementations to existing Vector utilities. The RFC itself should adopt the template syntax as the *only* supported syntax in its Proposal examples — the current hardcoded port examples in Examples 1–4 will lead implementers to build the fragile version first.
