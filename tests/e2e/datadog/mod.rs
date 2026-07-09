pub mod logs;
pub mod metrics;

use std::future::Future;
use std::time::Duration;

use reqwest::{Client, Method};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::Value;
use tracing::{trace, warn};

// Fakeintake may not be ready to accept connections yet, particularly right
// after the compose services start, so transient request failures are retried.
const MAX_FETCH_ATTEMPTS: usize = 10;
const FETCH_RETRY_INTERVAL: Duration = Duration::from_secs(1);

// Shared wait between polling attempts for tests that need to retry until
// expected data has arrived at fakeintake (data itself may take a moment to
// flow through the pipeline, on top of any transient connection issues
// already handled by `get_fakeintake_payloads`). How many attempts are needed
// varies per test, so `max_retries` is left to the caller.
const WAIT_INTERVAL: Duration = Duration::from_secs(1);

// Calls `fetch` up to `max_retries` times, sleeping `wait` between attempts,
// until `is_complete` reports the fetched value is ready to use.
pub(super) async fn poll_until<T, F, Fut, P>(
    max_retries: usize,
    wait: Duration,
    mut fetch: F,
    mut is_complete: P,
) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = T>,
    P: FnMut(&T) -> bool,
{
    assert!(max_retries > 0, "max_retries must be greater than zero");

    let mut value = fetch().await;
    let mut attempt = 1;
    while attempt < max_retries && !is_complete(&value) {
        tokio::time::sleep(wait).await;
        value = fetch().await;
        attempt += 1;
    }

    value
}

// Like `poll_until`, but for callers where a single `is_complete` pass isn't
// enough to know the data is done arriving: the dogstatsd emitter
// (tests/e2e/datadog-metrics/dogstatsd_client/client.py) sends each metric type
// across many loop iterations, so the first fetch to satisfy `is_complete` may
// still be a partial flush that a later fetch would extend. Once `is_complete`
// is satisfied, this keeps polling until two consecutive fetches are equal
// (i.e. the data has stopped changing) or `max_retries` is hit.
pub(super) async fn poll_until_stable<T, F, Fut, P>(
    max_retries: usize,
    wait: Duration,
    mut fetch: F,
    mut is_complete: P,
) -> T
where
    T: PartialEq,
    F: FnMut() -> Fut,
    Fut: Future<Output = T>,
    P: FnMut(&T) -> bool,
{
    assert!(max_retries > 0, "max_retries must be greater than zero");

    let mut value = fetch().await;
    let mut attempt = 1;
    while attempt < max_retries {
        if is_complete(&value) {
            tokio::time::sleep(wait).await;
            attempt += 1;
            let next = fetch().await;
            if next == value {
                return next;
            }
            value = next;
            continue;
        }

        tokio::time::sleep(wait).await;
        value = fetch().await;
        attempt += 1;
    }

    value
}

fn fake_intake_vector_address() -> String {
    std::env::var("FAKE_INTAKE_VECTOR_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:8082".to_string())
}

fn fake_intake_agent_address() -> String {
    std::env::var("FAKE_INTAKE_AGENT_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:8083".to_string())
}

#[derive(Deserialize, Debug)]
struct FakeIntakePayload<D> {
    // When string, base64 encoded
    data: D,
    #[serde(rename = "encoding")]
    _encoding: String,
    #[serde(rename = "timestamp")]
    _timestamp: String,
}

type FakeIntakePayloadJson = FakeIntakePayload<Value>;

type FakeIntakePayloadRaw = FakeIntakePayload<String>;

trait FakeIntakeResponseT {
    fn build_url(base: &str, endpoint: &str) -> String;
}

#[derive(Deserialize, Debug)]
struct FakeIntakeResponse<P> {
    payloads: Vec<P>,
}

type FakeIntakeResponseJson = FakeIntakeResponse<FakeIntakePayloadJson>;

impl FakeIntakeResponseT for FakeIntakeResponseJson {
    fn build_url(base: &str, endpoint: &str) -> String {
        format!("{base}/fakeintake/payloads?endpoint={endpoint}&format=json",)
    }
}

type FakeIntakeResponseRaw = FakeIntakeResponse<FakeIntakePayloadRaw>;

impl FakeIntakeResponseT for FakeIntakeResponseRaw {
    fn build_url(base: &str, endpoint: &str) -> String {
        format!("{base}/fakeintake/payloads?endpoint={endpoint}",)
    }
}

async fn get_fakeintake_payloads<R>(base: &str, endpoint: &str) -> R
where
    R: FakeIntakeResponseT + DeserializeOwned,
{
    let url = &R::build_url(base, endpoint);

    let mut last_error = String::new();
    for attempt in 1..=MAX_FETCH_ATTEMPTS {
        match Client::new().request(Method::GET, url).send().await {
            Ok(response) => {
                trace!(
                    "Fakeintake response headers for {endpoint}: {:?}",
                    response.headers()
                );

                match response.json::<R>().await {
                    Ok(parsed) => return parsed,
                    Err(e) => last_error = format!("Parsing fakeintake payloads failed: {e}"),
                }
            }
            Err(e) => last_error = format!("Sending GET request to {url} failed: {e}"),
        }

        if attempt < MAX_FETCH_ATTEMPTS {
            warn!("{last_error}, retrying...");
            tokio::time::sleep(FETCH_RETRY_INTERVAL).await;
        }
    }

    panic!("{last_error} after {MAX_FETCH_ATTEMPTS} attempts");
}
