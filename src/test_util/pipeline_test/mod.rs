pub mod assertions;
pub mod generators;
pub mod listeners;

use std::{collections::HashMap, time::Duration};

use hyper::Method;

use crate::{
    config::{
        self, ConfigBuilder, ConfigDiff, ConfigPath, TestDefinition, TestGeneratorConfig,
        TestListenerConfig, TestOutput,
        loading::ConfigBuilderLoader,
        unit_test::{RunnableTest, UnitTestResult},
    },
    event::Event,
    topology::{RunningTopology, builder::TopologyPiecesBuilder},
};

use super::event_builder::build_event_from_fields;

use self::{
    generators::{HttpGenerator, SocketGenerator, TestGenerator},
    listeners::{
        HttpListener, TcpListener, TestListener,
        http::{BodyDecoding, Decompression},
    },
};

/// A complete pipeline integration test.
///
/// Runs a real Vector topology against test generators (which send events into sources)
/// and captures output via test listeners (which receive events from sinks).
pub struct PipelineTest {
    pub name: String,
    /// Config builder (without test definitions) used to build the topology per run.
    config_builder: ConfigBuilder,
    generators: Vec<Box<dyn TestGenerator>>,
    listeners: HashMap<String, Box<dyn TestListener>>,
    outputs: Vec<TestOutput<String>>,
}

#[async_trait::async_trait]
impl RunnableTest for PipelineTest {
    fn name(&self) -> &str {
        &self.name
    }

    async fn run(self: Box<Self>) -> UnitTestResult {
        (*self).run().await
    }
}

impl PipelineTest {
    pub async fn run(mut self) -> UnitTestResult {
        // Build the topology config and pieces fresh for each run.
        let config = match self.config_builder.build() {
            Ok(c) => c,
            Err(errs) => return UnitTestResult { errors: errs },
        };
        let diff = ConfigDiff::initial(&config);
        let pieces = match TopologyPiecesBuilder::new(&config, &diff).build().await {
            Ok(p) => p,
            Err(errs) => return UnitTestResult { errors: errs },
        };

        // 1. Start all listeners so they are ready to accept connections.
        for (name, listener) in &mut self.listeners {
            if let Err(e) = listener.start().await {
                return UnitTestResult {
                    errors: vec![format!("failed to start listener '{name}': {e}")],
                };
            }
        }

        // 2. Start the topology.
        let Some((topology, _)) = RunningTopology::start_validated(config, diff, pieces).await
        else {
            return UnitTestResult {
                errors: vec!["failed to start topology".to_string()],
            };
        };

        // 3. Send test events via all generators.
        for generator in &self.generators {
            if let Err(e) = generator.send().await {
                // Best-effort cleanup before returning the error.
                let _ = tokio::time::timeout(Duration::from_secs(5), topology.stop()).await;
                return UnitTestResult {
                    errors: vec![format!("generator error: {e}")],
                };
            }
        }

        // 4. Wait for sources to finish draining, then shut down the topology.
        //    A 30-second timeout prevents a stuck sink from deadlocking the test suite.
        topology.sources_finished().await;
        let _ = tokio::time::timeout(Duration::from_secs(30), topology.stop()).await;

        // 5. Collect events from all listeners.
        let mut listener_events: HashMap<String, Vec<Event>> = HashMap::new();
        for (name, listener) in &mut self.listeners {
            listener_events.insert(name.clone(), listener.collect().await);
        }

        // 6. Run VRL assertions against captured events.
        let mut errors = Vec::new();
        for output in &self.outputs {
            let conditions = match &output.conditions {
                Some(c) => c,
                None => continue,
            };
            for name in output.extract_from.clone().to_vec() {
                let events = listener_events.get(&name).map(Vec::as_slice).unwrap_or(&[]);
                for condition in conditions {
                    if let Err(e) = assertions::run_vrl_assertion(condition, events) {
                        errors.push(format!("output '{name}': {e}"));
                    }
                }
            }
        }

        UnitTestResult { errors }
    }
}

/// Build a single `PipelineTest` from a test definition and the topology config builder.
fn build_one_pipeline_test(
    test_def: TestDefinition<String>,
    config_builder: ConfigBuilder,
) -> Result<PipelineTest, Vec<String>> {
    // Build generators.
    let mut generators: Vec<Box<dyn TestGenerator>> = Vec::new();
    for (name, gen_config) in &test_def.generators {
        let events = match gen_config {
            TestGeneratorConfig::Socket { events, .. }
            | TestGeneratorConfig::Http { events, .. } => events
                .iter()
                .map(|e| {
                    build_event_from_fields(
                        &e.type_str,
                        e.value.as_deref(),
                        e.source.as_deref(),
                        e.log_fields.as_ref(),
                        e.metric.as_ref(),
                    )
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| vec![format!("generator '{name}': {e}")])?,
        };
        let boxed_gen: Box<dyn TestGenerator> = match gen_config {
            TestGeneratorConfig::Socket { address, .. } => Box::new(SocketGenerator {
                address: *address,
                events,
            }),
            TestGeneratorConfig::Http { address, .. } => {
                let uri = format!("http://{address}/")
                    .parse()
                    .map_err(|e| vec![format!("generator '{name}': invalid URI: {e}")])?;
                Box::new(HttpGenerator {
                    uri,
                    events,
                    method: Method::POST,
                })
            }
        };
        generators.push(boxed_gen);
    }

    // Build listeners.
    let mut listeners: HashMap<String, Box<dyn TestListener>> = HashMap::new();
    for (name, listener_config) in &test_def.listeners {
        let listener: Box<dyn TestListener> = match listener_config {
            TestListenerConfig::Http {
                address,
                status_code,
                decompression,
                decoding,
            } => {
                let decomp = match decompression {
                    Some(config::ListenerDecompression::Gzip) => Decompression::Gzip,
                    Some(config::ListenerDecompression::Zstd) => {
                        return Err(vec![format!(
                            "listener '{name}': zstd decompression not yet implemented"
                        )]);
                    }
                    None => Decompression::None,
                };
                let body_decoding = match &decoding.codec {
                    config::ListenerCodec::Json => BodyDecoding::Json,
                };
                Box::new(HttpListener::new(
                    *address,
                    *status_code,
                    decomp,
                    body_decoding,
                ))
            }
            TestListenerConfig::Tcp { address } => Box::new(TcpListener::new(*address)),
        };
        listeners.insert(name.clone(), listener);
    }

    Ok(PipelineTest {
        name: test_def.name,
        config_builder,
        generators,
        listeners,
        outputs: test_def.outputs,
    })
}

/// Build all pipeline tests from the given config paths.
///
/// Config files must use explicit addresses in generator/listener configs — template
/// substitution is not supported in Part 1.
pub async fn build_pipeline_tests(
    paths: &[ConfigPath],
) -> Result<Vec<Box<dyn RunnableTest>>, Vec<String>> {
    let mut config_builder = ConfigBuilderLoader::default()
        .interpolate_env(true)
        .load_from_paths(paths)?;

    // Extract test definitions before building the config, to avoid resolve_outputs running on
    // pipeline test outputs (which refer to listener names, not graph component names).
    let test_definitions = std::mem::take(&mut config_builder.tests);

    let mut tests: Vec<Box<dyn RunnableTest>> = Vec::new();
    let mut build_errors: Vec<String> = Vec::new();

    for test_def in test_definitions {
        let test_name = test_def.name.clone();
        match build_one_pipeline_test(test_def, config_builder.clone()) {
            Ok(test) => tests.push(Box::new(test)),
            Err(errs) => {
                let mut msg = errs.join("\n  ");
                msg.insert_str(0, &format!("failed to build test '{test_name}':\n  "));
                build_errors.push(msg);
            }
        }
    }

    if build_errors.is_empty() {
        Ok(tests)
    } else {
        Err(build_errors)
    }
}
