# RFC DRAFT - Split Component Lifecycle into Four Distinct Phases

Component config traits currently conflate structural validation, environment validation, pure
construction, and task spawning into a single `build()` method. This is most acute for
`TransformConfig`, but the same shape of problem exists for `SinkConfig`. This RFC proposes a
shared `ComponentConfig` trait with four explicit lifecycle phases (`validate_structure`,
`validate_environment`, `build`, and `start`), implemented by `TransformConfig` and `SinkConfig`,
to make `vector validate` reliable, prevent resource leaks on topology reload rollback, and
simplify unit testing.

## Context

- Immediate motivation: [#25161](https://github.com/vectordotdev/vector/pull/25161) fixed
  `vector validate --no-environment` silently skipping VRL/condition errors, but needed ~540 lines
  (`validate_env()` plus `TransformContext::key` guard clauses) to work around the lack of a clean
  trait contract.
- Investigating the fix surfaced that `SinkConfig` has the same entanglement in a milder form.
  `build()` already returns `(VectorSink, Healthcheck)`, an unstarted sink plus a deferred
  environment check, and the topology builder already treats `Healthcheck` as a distinct phase
  (`run_healthchecks` gates topology commit in `src/topology/running.rs`, before `spawn_diff`
  actually starts driving events). Formalizing this as a named phase on a shared trait, rather than
  an implicit convention, closes the same `vector validate` gap for sinks that this RFC closes for
  transforms.

## Scope

### In scope

- A new `ComponentConfig` trait with associated types `Context` and `Built`, defining
  `validate_structure`, `validate_environment`, and `build` as the three config-time phases.
- `TransformConfig: ComponentConfig<Context = TransformContext, Built = Transform>`: introduce
  `validate_structure`, `validate_environment`, and `start`; redefine `build` as pure, synchronous
  construction.
- `SinkConfig: ComponentConfig<Context = SinkContext, Built = VectorSink>`: hoist `Healthcheck`
  construction out of `build()` and into `validate_environment`; redefine `build` as pure,
  synchronous construction of an unstarted `VectorSink`.
- Update `TopologyPiecesBuilder` and `vector validate` to call each phase at the right point for
  both transforms and sinks.
- Migrate all existing transforms and sinks.

### Out of scope

- `SourceConfig` is deferred. `Source` is defined as `BoxFuture<'static, Result<(), ()>>`
  (`lib/vector-core/src/source.rs`), so `build()`'s return value *is* the run loop rather than an
  inert handle. Applying `ComponentConfig` to sources is doable but a larger effort than this RFC's
  scope; see Future Improvements.
- Changes to user-visible configuration format or component behavior.

## Motivation

- `vector validate` has no clean way to "check VRL without starting threads." The current workaround
  (stub enrichment tables, `validate_env()`, `context.key` guards) must be replicated per-transform.
- `build()` spawns background tokio tasks before a topology reload is committed. If the reload is
  rolled back, those tasks leak.
- Testing transform logic requires spinning up background machinery because construction and startup
  are inseparable.
- The `build()` signature gives no signal about whether an implementation is safe to call
  speculatively (during validation) or whether it has observable side effects.
- For sinks, `--no-environment` is all-or-nothing: `vector validate` either skips `build()` entirely
  for every sink (no config validation beyond deserialization) or calls the real `build()`, which
  today also constructs a live `Healthcheck` future tied to real credentials/endpoints
  (e.g. `src/sinks/http/config.rs`, `src/sinks/kafka/config.rs`). There is no way to validate a
  sink's config-level construction (auth parsing, encoder setup) without also being able to reach the
  real endpoint.
- [#25840](https://github.com/vectordotdev/vector/issues/25840) is a concrete, currently-open
  instance of the same gap on the `validate_structure` side. The routing-field template
  confinement check is purely lexical (no I/O), but it lives in `SinkConfig::build()`
  (e.g. `src/sinks/aws_s3/config.rs`), so `--no-environment` skips it and a confinement-violating
  config is only caught at real boot.

## Proposal

### User Experience

No user-visible change. `vector validate` and `vector validate --no-environment` behave the same
externally; the difference is that `validate` now exercises the same VRL compilation and sink
construction paths as normal startup, rather than separate, potentially divergent ones.

### Implementation

A shared `ComponentConfig` trait defines the three config-time phases. `start` lives on the built
value itself (`Transform`, `VectorSink`) rather than on the config trait, since starting is a
runtime concern, not a configuration concern:

```rust
#[async_trait]
pub trait ComponentConfig: Send + Sync {
    /// The context passed to `validate_environment` and `build`: `TransformContext` or
    /// `SinkContext`, fixed by the implementing trait.
    type Context;

    /// The value `build()` produces: `Transform` or `VectorSink`. Unstarted; safe to
    /// discard on topology rollback.
    type Built: Send;

    /// Phase 1: pure structural checks (reserved output names, duplicate route keys,
    /// invalid sample rates, malformed URIs). No context, no I/O. Called during config
    /// compilation on both `vector validate` and normal startup.
    fn validate_structure(&self) -> Result<(), Vec<String>> { Ok(()) }

    /// Phase 2: environment-dependent checks. For transforms: compile VRL, build
    /// conditions, resolve enrichment table references against stub (validate) or real
    /// (startup) resources. For sinks: the existing `Healthcheck` future, hoisted out of
    /// `build()`.
    async fn validate_environment(&self, cx: &Self::Context) -> Result<(), Vec<String>>;

    /// Phase 3: pure, synchronous construction. Receives context; produces `Self::Built`.
    /// No task spawning, no I/O. Safe to discard on topology rollback.
    fn build(&self, cx: &Self::Context) -> crate::Result<Self::Built>;
}

pub trait TransformConfig: ComponentConfig<Context = TransformContext, Built = Transform> + ... {}

pub trait SinkConfig: ComponentConfig<Context = SinkContext, Built = VectorSink> + ... {}

impl Transform {
    /// Phase 4: startup. Spawns background tasks, opens connections, registers metrics.
    /// Called only after the topology diff is committed.
    async fn start(self, cx: &TransformContext) -> RunningTransform { ... }
}

impl VectorSink {
    /// Phase 4: startup. Today this is `VectorSink::run(self, input_stream)`, called from
    /// `spawn_diff` after `run_healthchecks` succeeds. No signature change needed here: the
    /// existing `run()` already plays the role of `start()`.
    async fn run(self, input_rx: BufferReceiver<Event>) -> Result<(), ()> { ... }
}
```

`dyn SourceConfig`/`dyn SinkConfig`/`dyn TransformConfig` remain object-safe: each subtrait pins
`ComponentConfig`'s associated types concretely, so `Box<dyn SinkConfig>` (`BoxedSink`) is
unaffected and `typetag::serde` registration is unchanged.

**Call sites:**

| Call site | Phases invoked (transforms) | Phases invoked (sinks) |
| --- | --- | --- |
| `vector validate --no-environment` | `validate_structure` | `validate_structure` |
| `vector validate` | `validate_structure` + `validate_environment` (with stubs) | `validate_structure` + `validate_environment` (real `Healthcheck`, but never awaited against traffic-affecting side effects beyond the probe itself) |
| Normal startup / reload (pre-commit) | `validate_structure` + `validate_environment` (real resources) + `build` | `validate_structure` + `validate_environment` (`run_healthchecks`) + `build` |
| Normal startup / reload (post-commit) | `start` | `run` (existing `VectorSink::run`) |

**Migration:**

1. Add `ComponentConfig` with `validate_structure`, `validate_environment`, and sync `build` as
   required methods, with a blanket adapter that delegates all three to each trait's existing async
   `build()` for un-migrated components.
2. Migrate transforms one at a time, starting with `remap` (VRL) and `filter` / `route` (conditions).
3. Migrate sinks one at a time: hoist `Healthcheck` construction out of `build()` into
   `validate_environment`, starting with `http` and `kafka` as representative cases, since their
   `build()` impls already construct `Healthcheck` as a clearly separable step
   (`src/sinks/http/config.rs`, `src/sinks/kafka/config.rs`).
4. Update `TopologyPiecesBuilder` to invoke phases at the appropriate points for both transforms and
   sinks. For sinks this mostly formalizes the existing `build`, `run_healthchecks`, `spawn_diff`
   ordering in `src/topology/running.rs` rather than restructuring it.
5. Update `vector validate` to call `validate_environment` with stub enrichment tables (transforms)
   and real `Healthcheck` construction without probing traffic-affecting side effects beyond the
   health probe itself (sinks); remove the `validate_env()` workaround method.
6. Remove the blanket adapter once all transforms and sinks are migrated.

## Alternatives

- **Keep the current approach and add more per-transform workarounds.** Already proven insufficient:
  the fix PR added hundreds of lines of guard logic with no improvement to the trait contract.
- **Return a compiled artifact from `validate_environment` so `build()` doesn't recompile.**
  `build()` runs once per topology build or reload, not per event, so the duplicate VRL/condition
  compilation is a one-off, compile-time cost of a few milliseconds at most. Not worth the added
  type-system complexity (GAT vs. `Box<dyn Any>`) this would require.

## Plan Of Attack

1. Spike `ComponentConfig` behind a blanket adapter for one transform and one sink to prove the
   pattern compiles and holds under `TopologyPiecesBuilder` and `vector validate`.
2. Open a tracking issue listing every transform and sink still on the blanket adapter; migrate them
   incrementally, checking each off as it moves over.
3. Remove the blanket adapter and the `validate_env()` workaround once the tracking issue is clear.

## Future Improvements

- Apply the same `ComponentConfig` contract to `SourceConfig`. This requires introducing a new
  unstarted-source type to serve as `ComponentConfig::Built` (today `Source` is
  `BoxFuture<'static, Result<(), ()>>`, the run loop itself, not an inert handle), and auditing each
  source implementation individually: some (e.g. `socket`) already defer all work into the returned
  future and would migrate cheaply; others (e.g. `file`) perform environment-dependent work eagerly
  inside `build()` today and would need that work relocated to `validate_environment`.
