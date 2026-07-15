---
title: "feat: Harden pipeline integration test framework developer ergonomics"
type: feat
status: active
date: 2026-03-14
---

# feat: Harden pipeline integration test framework developer ergonomics

## Overview

The pipeline integration test framework (`src/test_util/pipeline_test/`) was recently
implemented and stabilized after uncovering 7 significant pitfalls (see
`docs/solutions/testing/pipeline-integration-test-framework-pitfalls.md`). All bugs
are fixed, but the framework still requires contributors to know several non-obvious
rules: explicit codec declaration, unique port assignment, specific lifecycle ordering,
and VRL style restrictions.

This plan addresses the three highest-leverage improvements: dynamic port allocation,
early config validation, and contributor documentation.

## Problem Statement / Motivation

Contributors adding new pipeline test YAML files currently must:

- Manually pick unique ports across all test files (no guard against conflicts)
- Know that `decoding.codec: json` is required even though the generator always emits JSON
- Know the exact lifecycle ordering (`wait_for_tcp` → `send` → `sleep 250ms` → `stop`)
- Distinguish VRL object-literal vs. assignment style when writing event sources

When these rules are violated, failures are either silent (wrong event shape, no error)
or catastrophic (panic from `Server::bind`, topology hang). None of these failure modes
point contributors toward the root cause.

## Proposed Solution

### 1. Dynamic port allocation (eliminates port conflict class)

Replace hardcoded ports in test YAML with a `port: auto` sentinel value. The framework
binds each socket/HTTP listener to port 0 (OS assigns a free port), then passes the
resolved address back to generators before they attempt to connect.

This eliminates pitfalls 6 (connection refused) and 7 (address in use) for any test
using `port: auto`, and removes the need to maintain a cross-file port registry.

**Config API change:**
```yaml
# Before (hardcoded, must be globally unique)
sources:
  socket:
    type: socket
    mode: tcp
    address: "127.0.0.1:19100"

# After (OS assigns a free port)
sources:
  socket:
    type: socket
    mode: tcp
    address: "127.0.0.1:0"
```

The framework already receives generator configs that reference source addresses. With
`port: 0` support, `start_validated()` must return or expose the actual bound address
so `PipelineTest` can rewrite generator `target_address()` before calling `send()`.

> **Note:** Topology currently does not expose bound addresses after start. This may
> require surfacing bound port info from `RunningTopology` or using a side-channel
> (e.g., shared `AtomicU16` populated by the source's bind callback). This is the
> highest-effort item and may be scoped to a follow-up PR if the API change is large.

### 2. Early config validation (eliminates silent mismatch class)

Add a `validate_pipeline_test_config()` function called from `build_one_pipeline_test()`
before topology start. Validate:

- Every `TestGeneratorConfig::Socket` that sends to a `socket` source **must** have
  `decoding.codec: json` on that source (or equivalently, the source is typed `socket`
  and its decode config is not `bytes`).
- `retry_initial_backoff_secs` ≥ 1 on any HTTP sink with retries enabled.
- No two test files in the same run share the same port (port collision registry built
  at `build_pipeline_tests()` time across all loaded `TestDefinition`s).

Validation errors surface as `UnitTestResult { errors: [...] }` with actionable
messages, e.g.:

```
pipeline test "http_sink_test": socket source at 127.0.0.1:19100 uses "bytes" codec
but its generator sends JSON. Add `decoding: { codec: json }` to the source config.
```

### 3. Contributor documentation

Add `tests/behavior/pipelines/README.md` — a task-oriented guide for contributors
adding new pipeline tests. The document covers:

- When to use pipeline tests vs. unit tests
- Minimal working example (annotated YAML)
- The five required checklist items from the solutions document
- VRL event source styles (object literal vs. assignment)
- How to run a single test locally (`cargo vdev test --test-name <name>`)
- Cross-referencing `docs/solutions/testing/pipeline-integration-test-framework-pitfalls.md`
  for deeper explanations

## Technical Considerations

### Dynamic port allocation trade-offs

`port: 0` binding is clean on the listener side — `TcpListener::bind("0")` returns a
`TcpListener` whose actual port is readable via `local_addr()`. The complication is
that YAML config objects are deserialized before topology start, so the generator's
`address` field is set from config. Two options:

**Option A — Post-start address rewrite:** After `start_validated()`, query bound
addresses from the running topology and patch generator configs in memory before
`send()`. Requires topology to expose a `bound_address(component_id)` API.

**Option B — Late binding via shared state:** Sources write their bound address to a
`tokio::sync::watch` channel; generators subscribe and wait for the value. More complex
but avoids topology API changes.

Option A is simpler and matches how integration test harnesses work in similar projects.
Scope the topology API addition to return a `HashMap<ComponentId, SocketAddr>` for
bound-port sources.

### Validation placement

Config validation should run in `build_one_pipeline_test()` after deserialization
but before `RunningTopology::start_validated()`. Returning early with validation errors
avoids topology teardown overhead and gives faster feedback.

Cross-file port collision detection requires collecting all test definitions first, so
it belongs in `build_pipeline_tests()` before the per-test loop.

### Documentation scope

The README should be short (< 150 lines) and task-oriented. Link to the solutions
document for root-cause detail; do not duplicate it.

## System-Wide Impact

- **Interaction graph**: Config validation touches the config deserialization path
  (`src/config/unit_test/mod.rs` → `build_pipeline_tests()`). No production topology
  code is affected.
- **Error propagation**: Validation errors return `UnitTestResult { errors }` — same
  path as existing test failures. The CLI runner already formats and displays these.
- **State lifecycle risks**: Dynamic port allocation only affects test code. No
  production state is involved.
- **API surface parity**: `build_unit_tests()` (classic unit tests) has its own
  validation path; changes here are isolated to the pipeline test path.
- **Integration test scenarios**: The existing four YAML test files in
  `tests/behavior/pipelines/` serve as regression tests for the framework itself.

## Acceptance Criteria

- [ ] A new `TestGeneratorConfig::Socket` paired with a socket source lacking
      `decoding.codec: json` produces a clear validation error, not a silent wrong-shape event
- [ ] Two pipeline tests sharing a port produce a clear error at load time, not a panic
      at runtime
- [ ] `retry_initial_backoff_secs: 0` produces a clear validation error
- [ ] `tests/behavior/pipelines/README.md` exists and covers the five checklist items
      from `docs/solutions/testing/pipeline-integration-test-framework-pitfalls.md`
- [ ] All four existing YAML tests continue to pass after changes
- [ ] (Stretch) A pipeline test YAML can use `address: "127.0.0.1:0"` and the framework
      wires the resolved port to generators automatically

## Dependencies & Risks

- **Dynamic port allocation** depends on `RunningTopology` exposing bound addresses.
  If that API surface is large or invasive, defer this item to a follow-up PR and
  document the manual port registry approach in the README instead.
- **Config validation** for codec mismatch requires correlating generator target
  addresses with source addresses across the loaded config. This cross-referencing is
  straightforward but must handle the case where source and generator addresses differ
  (e.g., `0.0.0.0` vs `127.0.0.1`).

## Implementation Files

- `src/test_util/pipeline_test/mod.rs` — add `validate_pipeline_test_config()`, call
  before topology start; add port collision check in `build_pipeline_tests()`
- `src/config/mod.rs` or a new `src/test_util/pipeline_test/validation.rs` — validation
  logic
- `src/topology/running.rs` — (stretch) expose `bound_addresses()` method
- `tests/behavior/pipelines/README.md` — new contributor guide
- Update existing YAML test files to use `address: "127.0.0.1:0"` if dynamic allocation
  is implemented

## Sources & References

### Internal References

- Pitfalls document: `docs/solutions/testing/pipeline-integration-test-framework-pitfalls.md`
- Framework entry point: `src/test_util/pipeline_test/mod.rs`
- Config types: `src/config/mod.rs:407–552`
- CLI runner: `src/unit_test.rs:154–184`
- Topology lifecycle: `src/topology/running.rs`
- Existing test YAMLs: `tests/behavior/pipelines/`

### Related Work

- Recent stabilization commits: `a342f85`, `fb727ad`, `29dd6f6`, `ef06db5`, `bdbbfd3`
