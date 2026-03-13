use std::collections::BTreeMap;

use vector_lib::TimeZone;
use vrl::compiler::runtime::Runtime;
use vrl::{
    compiler::{CompileConfig, TargetValue, TypeState},
    value::{Secrets, Value},
};

use crate::{
    conditions::{AnyCondition, ConditionConfig},
    event::Event,
    format_vrl_diagnostics,
};

/// Run a VRL assertion condition against an array of captured events.
///
/// Unlike unit test conditions (which run once per event), pipeline test conditions receive
/// `.` as a `Value::Array` containing all events captured by a listener. This enables count
/// checks (`assert_eq!(length(.), 2)`), ordering checks (`.[0].message`), and cross-event
/// assertions.
///
/// Returns `Ok(())` if the condition passes, or an error message if it fails or errors.
pub fn run_vrl_assertion(condition: &AnyCondition, events: &[Event]) -> Result<(), String> {
    // Extract the VRL source from the condition config.
    let source = extract_vrl_source(condition)?;

    // Build the events array: each event becomes a VRL object value.
    let array_value = Value::Array(
        events
            .iter()
            .map(|e| Value::Object(event_to_object(e)))
            .collect(),
    );

    // Compile with a relaxed type checker — the root can be any type (array in this case).
    let functions = vector_vrl_functions::all();
    let state = TypeState::default();
    let config = CompileConfig::default();

    let result = vrl::compiler::compile_with_state(&source, &functions, &state, config)
        .map_err(|diag| format_vrl_diagnostics(&source, diag))?;

    // Run the program with `.` bound to the events array.
    let mut target = TargetValue {
        value: array_value,
        metadata: Value::Object(BTreeMap::new()),
        secrets: Secrets::default(),
    };
    let timezone = TimeZone::default();

    Runtime::default()
        .resolve(&mut target, &result.program, &timezone)
        .map_err(|err| format!("assertion failed: {err}"))
        .and_then(|value| match value {
            Value::Boolean(true) => Ok(()),
            Value::Boolean(false) => Err("assertion returned false".to_string()),
            _ => Err(format!("assertion returned non-boolean: {value}")),
        })
}

fn extract_vrl_source(condition: &AnyCondition) -> Result<String, String> {
    match condition {
        AnyCondition::String(s) => Ok(s.clone()),
        AnyCondition::Map(ConditionConfig::Vrl(vrl_config)) => Ok(vrl_config.source.clone()),
        _ => Err("pipeline test assertions only support VRL conditions \
             (type: vrl or a bare VRL expression string)"
            .to_string()),
    }
}

fn event_to_object(event: &Event) -> BTreeMap<vrl::prelude::KeyString, Value> {
    match event {
        Event::Log(log) => {
            // LogEvent's root is already a Value::Object — clone its fields.
            match log.clone().into_parts().0 {
                Value::Object(map) => map,
                other => {
                    let mut m = BTreeMap::new();
                    m.insert("message".into(), other);
                    m
                }
            }
        }
        Event::Metric(metric) => {
            let mut m = BTreeMap::new();
            m.insert("name".into(), Value::Bytes(metric.name().to_owned().into()));
            if let Some(ns) = metric.namespace() {
                m.insert("namespace".into(), Value::Bytes(ns.to_owned().into()));
            }
            m
        }
        Event::Trace(trace) => {
            // TraceEvent::into_parts() returns (ObjectMap, EventMetadata) directly.
            trace.clone().into_parts().0
        }
    }
}
