pub mod assertions;
pub mod generators;
pub mod listeners;

use std::{collections::HashMap, net::SocketAddr, time::Duration};

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

        // 3. Wait for each source to be ready, then send test events via all generators.
        //    Sources bind their ports asynchronously after topology start; connecting before
        //    the port is open causes a panic in send_lines. A 5-second timeout surfaces
        //    misconfigured addresses quickly rather than hanging indefinitely.
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
        for generator in &self.generators {
            if let Err(e) = generator.send().await {
                // Best-effort cleanup before returning the error.
                let _ = tokio::time::timeout(Duration::from_secs(5), topology.stop()).await;
                return UnitTestResult {
                    errors: vec![format!("generator error: {e}")],
                };
            }
        }

        // 4. Wait briefly, then stop the topology.
        //
        //    After generators close their connections the data is in the OS socket buffer, but
        //    the source task may not have been scheduled yet. When topology.stop() sends a
        //    shutdown signal the source's select! loop can pick shutdown before reading pending
        //    data, losing events. A short sleep gives the source time to drain the socket
        //    before we send the stop signal.
        tokio::time::sleep(Duration::from_millis(250)).await;

        //    stop() signals sources, drains transforms, and flushes sinks before returning.
        //    A 30-second outer timeout prevents a stuck sink from deadlocking the test suite.
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

/// Validate that every `Socket` generator targets a `socket` (TCP) source that uses
/// `decoding.codec: json`. Generators always send newline-delimited JSON; without
/// `codec: json` on the source the JSON is stored verbatim in `.message` and downstream
/// VRL transforms that access structured fields will receive `null`.
///
/// Matches sources to generators by port number to tolerate `0.0.0.0` vs `127.0.0.1`
/// differences between source and generator addresses.
fn validate_socket_codecs(
    test_name: &str,
    test_def: &TestDefinition<String>,
    config_builder: &crate::config::ConfigBuilder,
) -> Vec<String> {
    let mut errors = Vec::new();

    for (gen_name, gen_config) in &test_def.generators {
        let gen_addr: &SocketAddr = match gen_config {
            TestGeneratorConfig::Socket { address, .. } => address,
            TestGeneratorConfig::Http { .. } => continue,
        };

        // Search for a socket TCP source whose bound port matches the generator's target port.
        for (_component_key, source_outer) in &config_builder.sources {
            let Ok(source_json) = serde_json::to_value(&source_outer.inner) else {
                continue;
            };

            // Only inspect socket sources in TCP mode.
            if source_json.get("type").and_then(|v| v.as_str()) != Some("socket") {
                continue;
            }
            if source_json.get("mode").and_then(|v| v.as_str()) != Some("tcp") {
                continue;
            }

            // Match by port (tolerates address family mismatches like 0.0.0.0 vs 127.0.0.1).
            let source_addr_str = source_json
                .get("address")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let source_port: u16 = source_addr_str
                .rsplit(':')
                .next()
                .and_then(|p| p.parse().ok())
                .unwrap_or(0);
            if source_port != gen_addr.port() {
                continue;
            }

            // Found the source — check its codec.
            let codec = source_json
                .pointer("/decoding/codec")
                .and_then(|v| v.as_str())
                .unwrap_or("bytes");
            if codec != "json" {
                errors.push(format!(
                    "test '{test_name}' generator '{gen_name}': socket source at \
                     {source_addr_str} uses codec '{codec}', but generators always send \
                     newline-delimited JSON. Add `decoding: {{codec: json}}` to the source.",
                ));
            }
        }
    }

    errors
}

/// Build a single `PipelineTest` from a test definition and the topology config builder.
fn build_one_pipeline_test(
    test_def: TestDefinition<String>,
    config_builder: ConfigBuilder,
) -> Result<PipelineTest, Vec<String>> {
    // Validate config before building anything expensive.
    let codec_errors = validate_socket_codecs(&test_def.name, &test_def, &config_builder);
    if !codec_errors.is_empty() {
        return Err(codec_errors);
    }

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

    // Check for port collisions across all test definitions before building any topology.
    // Tests within the same run may overlap in TIME_WAIT if a previous run was interrupted,
    // and some test runners may execute tests concurrently in the future.
    {
        let mut port_owners: HashMap<u16, String> = HashMap::new();
        let mut port_errors: Vec<String> = Vec::new();

        for test_def in &test_definitions {
            let mut check_port = |port: u16, owner: String| {
                if let Some(prior) = port_owners.get(&port) {
                    port_errors.push(format!(
                        "port {port} is used by both {prior} and {owner}"
                    ));
                } else {
                    port_owners.insert(port, owner);
                }
            };

            for (gen_name, gen_config) in &test_def.generators {
                let addr = match gen_config {
                    TestGeneratorConfig::Socket { address, .. }
                    | TestGeneratorConfig::Http { address, .. } => address,
                };
                check_port(
                    addr.port(),
                    format!("test '{}' generator '{gen_name}'", test_def.name),
                );
            }

            for (listener_name, listener_config) in &test_def.listeners {
                let addr = match listener_config {
                    TestListenerConfig::Http { address, .. }
                    | TestListenerConfig::Tcp { address } => address,
                };
                check_port(
                    addr.port(),
                    format!("test '{}' listener '{listener_name}'", test_def.name),
                );
            }
        }

        if !port_errors.is_empty() {
            return Err(port_errors);
        }
    }

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
