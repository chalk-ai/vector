use std::collections::BTreeMap;

use indexmap::IndexMap;
use vrl::{
    compiler::{Context, TargetValue, TimeZone, state::RuntimeState},
    diagnostic::Formatter,
    value,
};

use crate::event::{Event, EventMetadata, LogEvent, Metric, Value};

/// Builds an [`Event`] from the fields shared by `TestInput` and generator event definitions.
///
/// - `type_str`: `"raw"`, `"vrl"`, `"log"`, or `"metric"`.
/// - `value`: raw string value (used when `type_str == "raw"`).
/// - `source`: VRL source expression (used when `type_str == "vrl"`).
/// - `log_fields`: key-value log fields (used when `type_str == "log"`).
/// - `metric`: metric definition (used when `type_str == "metric"`).
pub fn build_event_from_fields(
    type_str: &str,
    value: Option<&str>,
    source: Option<&str>,
    log_fields: Option<&IndexMap<String, Value>>,
    metric: Option<&Metric>,
) -> Result<Event, String> {
    match type_str {
        "raw" => match value {
            Some(v) => Ok(Event::Log(LogEvent::from_str_legacy(v.to_owned()))),
            None => Err("input type 'raw' requires the field 'value'".to_string()),
        },
        "vrl" => {
            if let Some(src) = source {
                let result = vrl::compiler::compile(src, &vector_vrl_functions::all())
                    .map_err(|e| Formatter::new(src, e).to_string())?;

                let mut target = TargetValue {
                    value: value!({}),
                    metadata: value::Value::Object(BTreeMap::new()),
                    secrets: value::Secrets::default(),
                };

                let mut state = RuntimeState::default();
                let timezone = TimeZone::default();
                let mut ctx = Context::new(&mut target, &mut state, &timezone);

                result
                    .program
                    .resolve(&mut ctx)
                    .map(|return_value| {
                        // Use the program's return value when it is an Object — this handles
                        // object-literal sources like `{ "message": "hello" }` which evaluate
                        // to an object without mutating `.`.
                        // Fall back to `target.value` for programs that use field assignments
                        // (`.field = value`) which mutate `.` and return a non-Object value.
                        let event_value = match return_value {
                            vrl::value::Value::Object(_) => return_value,
                            _ => target.value.clone(),
                        };
                        Event::Log(LogEvent::from_parts(
                            event_value,
                            EventMetadata::default_with_value(target.metadata.clone()),
                        ))
                    })
                    .map_err(|e| e.to_string())
            } else {
                Err("input type 'vrl' requires the field 'source'".to_string())
            }
        }
        "log" => {
            if let Some(fields) = log_fields {
                let mut event = LogEvent::from_str_legacy("");
                for (path, val) in fields {
                    event
                        .parse_path_and_insert(path, val.clone())
                        .map_err(|e| e.to_string())?;
                }
                Ok(event.into())
            } else {
                Err("input type 'log' requires the field 'log_fields'".to_string())
            }
        }
        "metric" => {
            if let Some(m) = metric {
                Ok(Event::Metric(m.clone()))
            } else {
                Err("input type 'metric' requires the field 'metric'".to_string())
            }
        }
        _ => Err(format!(
            "unrecognized input type '{type_str}', expected one of: 'raw', 'vrl', 'log' or 'metric'"
        )),
    }
}
