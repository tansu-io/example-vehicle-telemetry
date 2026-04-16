// Copyright ⓒ 2024-2026 Peter Morgan <peter.james.morgan@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use governor::{
    Quota, RateLimiter,
    clock::{Clock, QuantaClock},
};
use human_units::{
    FormatDuration as _,
    iec::{Byte, Prefix},
};
use opentelemetry::{
    InstrumentationScope, KeyValue, global,
    metrics::{Counter, Meter},
};
use opentelemetry_sdk::{
    error::OTelSdkResult,
    metrics::{
        SdkMeterProvider, Temporality,
        data::{AggregatedMetrics, Histogram, Metric, MetricData, ResourceMetrics},
        exporter::PushMetricExporter,
    },
};
use opentelemetry_semantic_conventions::SCHEMA_URL;
use rand::{Rng, SeedableRng, rngs::SmallRng};
use serde::Serialize;
use std::{
    fmt::{self, Display},
    fs::File,
    io::BufReader,
    ops::AddAssign,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, LazyLock, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tansu_client::{Client, ConnectionManager};
use tansu_sans_io::{
    produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    record::{Record, deflated, inflated},
};
use tokio::{
    signal::unix::{SignalKind, signal},
    task::JoinSet,
    time::sleep,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument};
use url::Url;

pub(crate) static METER: LazyLock<Meter> = LazyLock::new(|| {
    global::meter_with_scope(
        InstrumentationScope::builder(env!("CARGO_PKG_NAME"))
            .with_version(env!("CARGO_PKG_VERSION"))
            .with_schema_url(SCHEMA_URL)
            .build(),
    )
});

static PRODUCE_RECORD_COUNT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    METER
        .u64_counter("produce_record_count")
        .with_description("Produced record count")
        .build()
});

static PRODUCE_API_DURATION: LazyLock<opentelemetry::metrics::Histogram<u64>> =
    LazyLock::new(|| {
        METER
            .u64_histogram("produce_duration")
            .with_unit("ms")
            .with_description("Produce API latencies in milliseconds")
            .build()
    });

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CancelKind {
    Interrupt,
    Terminate,
    Timeout,
}

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
struct Info {
    started_at: SystemTime,
    previous: Option<ObservationLatency>,
    current: ObservationLatency,
}

impl Info {
    fn new(started_at: SystemTime) -> Self {
        Self {
            started_at,
            current: Default::default(),
            previous: Default::default(),
        }
    }

    fn with_previous(self, previous: Option<ObservationLatency>) -> Self {
        Self { previous, ..self }
    }

    fn elapsed(&self) -> Duration {
        self.current
            .observation
            .taken_at
            .duration_since(
                self.previous
                    .map_or(self.started_at, |previous| previous.observation.taken_at),
            )
            .expect("duration")
    }

    fn bytes_sent(&self) -> u64 {
        self.current.observation.bytes_sent
            - self
                .previous
                .map(|previous| previous.observation.bytes_sent)
                .unwrap_or_default()
    }

    fn records_sent(&self) -> u64 {
        self.current.observation.record_count
            - self
                .previous
                .map(|previous| previous.observation.record_count)
                .unwrap_or_default()
    }

    fn records_sent_per_second(&self) -> f64 {
        self.records_sent() as f64 / self.elapsed().as_secs() as f64
    }

    fn bandwidth(&self) -> Byte {
        self.bytes_sent()
            .checked_div(self.elapsed().as_secs())
            .map(|throughput| Byte::with_iec_prefix(throughput, Prefix::None))
            .expect("throughput")
    }
}

impl Display for Info {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "elapsed: {}, {} records sent, {:.1} records/s, ({}/s), latency: {} min, {:.1}ms avg, {} max",
            self.elapsed().format_duration(),
            self.records_sent(),
            self.records_sent_per_second(),
            self.bandwidth().format_iec(),
            self.current
                .latency
                .min
                .map(|min| min.format_duration())
                .expect("minimum"),
            self.current.latency.mean.expect("mean"),
            self.current
                .latency
                .max
                .map(|max| max.format_duration())
                .expect("max")
        )
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, PartialOrd)]
struct Latency {
    min: Option<Duration>,
    max: Option<Duration>,
    mean: Option<f64>,
}

impl From<&Histogram<u64>> for Latency {
    fn from(histogram: &Histogram<u64>) -> Self {
        let min = histogram
            .data_points()
            .filter_map(|dp| dp.min())
            .min()
            .map(Duration::from_millis);

        let max = histogram
            .data_points()
            .filter_map(|dp| dp.max())
            .max()
            .map(Duration::from_millis);

        let sum = histogram.data_points().map(|dp| dp.sum()).sum::<u64>() as f64;
        let count = histogram.data_points().map(|dp| dp.count()).sum::<u64>() as f64;

        let mean = Some(sum / count);

        Self { min, max, mean }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, PartialOrd)]
struct ObservationLatency {
    observation: Observation,
    latency: Latency,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct Observation {
    taken_at: SystemTime,
    bytes_sent: u64,
    record_count: u64,
}

impl AddAssign for Observation {
    fn add_assign(&mut self, rhs: Self) {
        self.taken_at = self.taken_at.max(rhs.taken_at);
        self.bytes_sent += rhs.bytes_sent;
        self.record_count += rhs.record_count;
    }
}

impl Default for Observation {
    fn default() -> Self {
        Self {
            taken_at: SystemTime::now(),
            bytes_sent: Default::default(),
            record_count: Default::default(),
        }
    }
}

#[derive(Debug)]
struct MetricExporter {
    started_at: SystemTime,
    temporality: Temporality,
    previous: Mutex<Option<ObservationLatency>>,
    cancellation: CancellationToken,
}

impl MetricExporter {
    fn new(cancellation: CancellationToken) -> Self {
        let started_at = SystemTime::now();
        Self {
            started_at,
            temporality: Default::default(),
            previous: Default::default(),
            cancellation,
        }
    }

    #[instrument(skip_all, fields(scope = scope.name(), metric = metric.name()))]
    fn info(&self, scope: &InstrumentationScope, metric: &Metric, info: &mut Info) {
        match (scope.name(), metric.name(), metric.data()) {
            ("tansu-client", "tcp_bytes_sent", AggregatedMetrics::U64(MetricData::Sum(sum))) => {
                for (point, data) in sum.data_points().enumerate() {
                    debug!(point, value = ?data.value());
                }

                info.current.observation.bytes_sent =
                    sum.data_points().map(|sum| sum.value()).sum::<u64>();
            }

            (
                env!("CARGO_PKG_NAME"),
                "produce_record_count",
                AggregatedMetrics::U64(MetricData::Sum(sum)),
            ) => {
                for (point, data) in sum.data_points().enumerate() {
                    debug!(point, value = ?data.value());
                }

                info.current.observation.record_count =
                    sum.data_points().map(|sum| sum.value()).sum::<u64>();
            }

            (
                env!("CARGO_PKG_NAME"),
                "produce_duration",
                AggregatedMetrics::U64(MetricData::Histogram(histogram)),
            ) => {
                info.current.latency = Latency::from(histogram);
            }

            _ => (),
        }
    }
}

impl PushMetricExporter for MetricExporter {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let cancelled = self.cancellation.is_cancelled();

        if cancelled {
            if let Some(previous) = *self.previous.lock().expect("previous") {
                let mut info = Info::new(self.started_at);
                info.current = previous;

                println!("{}", info);
            }
        } else {
            let mut previous = self.previous.lock().expect("previous");

            let mut info = Info::new(self.started_at).with_previous(previous.take());

            for scope in metrics.scope_metrics() {
                debug!(scope = scope.scope().name());

                for metric in scope.metrics() {
                    debug!(scope = scope.scope().name(), metric = metric.name());

                    self.info(scope.scope(), metric, &mut info);
                }
            }

            println!("{info}");

            _ = previous.replace(info.current);
        }

        Ok(())
    }

    fn force_flush(&self) -> OTelSdkResult {
        Ok(())
    }

    #[instrument]
    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        Ok(())
    }

    fn temporality(&self) -> Temporality {
        self.temporality
    }
}

fn parse_duration(s: &str) -> std::result::Result<Duration, String> {
    humantime::parse_duration(s).map_err(|e| e.to_string())
}

#[derive(Parser)]
#[command(about = "Simulate vehicle telemetry by producing GPS updates to Kafka")]
struct Args {
    /// CSV file containing VRM plates (one per line)
    #[arg(short, long, default_value = "vrm.csv")]
    input: PathBuf,

    /// Kafka broker URL
    #[arg(short, long, default_value = "tcp://localhost:9092")]
    broker: Url,

    /// Kafka topic to produce to
    #[arg(short, long, default_value = "telemetry")]
    topic: String,

    /// Update interval per vehicle (e.g. 10s, 500ms)
    #[arg(long, default_value = "10s", value_parser = parse_duration)]
    interval: Duration,

    /// Total simulation duration (e.g. 60s, 5m)
    #[arg(short, long, default_value = "60s", value_parser = parse_duration)]
    duration: Duration,
}

#[derive(Serialize, Clone)]
struct Telemetry {
    latitude: f64,
    longitude: f64,
    altitude: f64,
}

impl Telemetry {
    /// Initialise at a random position within rough UK mainland bounds.
    fn random(rng: &mut impl Rng) -> Self {
        Self {
            latitude: rng.random_range(50.0_f64..58.0),
            longitude: rng.random_range(-5.0_f64..2.0),
            altitude: rng.random_range(0.0_f64..200.0),
        }
    }

    /// Apply a small random walk, clamped to stay within plausible bounds.
    fn step(&mut self, rng: &mut impl Rng) {
        self.latitude += rng.random_range(-0.0001_f64..=0.0001);
        self.longitude += rng.random_range(-0.0001_f64..=0.0001);
        self.altitude += rng.random_range(-1.0_f64..=1.0);
        self.latitude = self.latitude.clamp(49.0, 61.0);
        self.longitude = self.longitude.clamp(-8.0, 3.0);
        self.altitude = self.altitude.clamp(0.0, 1000.0);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let token = CancellationToken::new();

    let meter_provider = {
        let exporter = MetricExporter::new(token.clone());
        let meter_provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter)
            .build();
        global::set_meter_provider(meter_provider.clone());

        meter_provider
    };

    let mut interrupt_signal = signal(SignalKind::interrupt()).unwrap();
    debug!(?interrupt_signal);

    let mut terminate_signal = signal(SignalKind::terminate()).unwrap();
    debug!(?terminate_signal);

    let args = Args::parse();

    // Read every VRM from the CSV (no header row).
    let vrms: Vec<String> = {
        let file = File::open(&args.input).with_context(|| format!("opening {:?}", args.input))?;
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(BufReader::new(file));
        rdr.records()
            .map(|r| Ok(r?.into_iter().next().unwrap_or_default().to_owned()))
            .collect::<Result<Vec<String>>>()?
    };

    println!(
        "Loaded {} VRMs; simulating for {} at {} per vehicle",
        vrms.len(),
        humantime::format_duration(args.duration),
        humantime::format_duration(args.interval),
    );

    let pool = ConnectionManager::builder(args.broker.clone())
        .client_id(Some("vehicle-simulator".into()))
        .build()
        .await
        .with_context(|| format!("connecting to {}", args.broker))?;
    let client = Arc::new(Client::new(pool));

    // One token per vehicle per interval.
    let quota = Quota::with_period(args.interval).expect("interval must be non-zero");
    let limiter = Arc::new(RateLimiter::keyed(quota));

    let duration = Box::pin(sleep(args.duration)) as Pin<Box<dyn Future<Output = ()>>>;

    let interval = args.interval;
    let topic = Arc::new(args.topic.clone());
    let n = vrms.len();

    let mut set = JoinSet::new();

    let mut seed_rng = rand::rng();

    for (i, vrm) in vrms.into_iter().enumerate() {
        let client = client.clone();
        let limiter = limiter.clone();
        let topic = topic.clone();
        let token = token.clone();
        let mut state = Telemetry::random(&mut seed_rng);
        // Seed a Send-able per-task RNG from the main-thread RNG.
        let rng_seed = seed_rng.random::<u64>();

        // Spread the first burst evenly across one full interval so the broker
        // is not hit with all vehicles simultaneously.
        let initial_delay = if n > 1 {
            interval.mul_f64(i as f64 / n as f64)
        } else {
            Duration::ZERO
        };

        set.spawn(async move {
            let attributes = [KeyValue::new("vrm", vrm.clone())];

            let clock = QuantaClock::default();
            let mut rng = SmallRng::seed_from_u64(rng_seed);

            if !initial_delay.is_zero() {
                tokio::select! {
                    cancelled = token.cancelled() => {
                        debug!(?cancelled);
                        return
                    },
                    _ = tokio::time::sleep(initial_delay) => {}
                }
            }

            loop {
                // Wait until the governor allows this key.
                loop {
                    match limiter.check_key(&vrm) {
                        Ok(_) => break,
                        Err(not_until) => {
                            let wait = not_until.wait_time_from(clock.now());
                            tokio::select! {
                                cancelled = token.cancelled() => {
                                    debug!(?cancelled);
                                    return
                                },

                                _ = tokio::time::sleep(wait) => {}
                            }
                        }
                    }
                }

                state.step(&mut rng);

                let ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;

                let value = match serde_json::to_string(&state) {
                    Ok(j) => j,
                    Err(e) => {
                        eprintln!("JSON serialisation error for {vrm}: {e}");
                        continue;
                    }
                };

                let record = Record::builder()
                    .key(Some(Bytes::from(format!(r#""{vrm}""#))))
                    .value(Some(Bytes::from(value)));

                let batch = match inflated::Batch::builder()
                    .base_offset(0)
                    .partition_leader_epoch(-1)
                    .magic(2)
                    .attributes(0)
                    .last_offset_delta(0)
                    .base_timestamp(ts)
                    .max_timestamp(ts)
                    .producer_id(-1)
                    .producer_epoch(-1)
                    .base_sequence(-1)
                    .record(record)
                    .build()
                {
                    Ok(b) => b,
                    Err(e) => {
                        eprintln!("Batch build error for {vrm}: {e}");
                        continue;
                    }
                };

                let deflated_batch = match deflated::Batch::try_from(batch) {
                    Ok(b) => b,
                    Err(e) => {
                        eprintln!("Deflate error for {vrm}: {e}");
                        continue;
                    }
                };

                let frame = deflated::Frame {
                    batches: vec![deflated_batch],
                };

                let partition_data = PartitionProduceData::default()
                    .index(0)
                    .records(Some(frame));

                let topic_data = TopicProduceData::default()
                    .name(topic.as_ref().clone())
                    .partition_data(Some(vec![partition_data]));

                let req = ProduceRequest::default()
                    .acks(1)
                    .timeout_ms(5_000)
                    .topic_data(Some(vec![topic_data]));

                let produce_start = SystemTime::now();
                if let Err(e) = client.call(req).await.inspect(|_| {
                    PRODUCE_RECORD_COUNT.add(1u64, &attributes);
                    PRODUCE_API_DURATION.record(
                        produce_start
                            .elapsed()
                            .inspect(|duration| debug!(produce_duration_ms = duration.as_millis()))
                            .map_or(0, |duration| duration.as_millis() as u64),
                        &attributes,
                    );
                }) {
                    eprintln!("Produce error for {vrm}: {e}");
                }
            }
        });
    }

    let join_all = async {
        while !set.is_empty() {
            debug!(len = set.len());
            _ = set.join_next().await;
        }
    };

    let cancellation = tokio::select! {

        timeout = duration => {
            debug!(?timeout);
            token.cancel();
            Some(CancelKind::Timeout)
        }

        completed = join_all => {
            debug!(?completed);
            None
        }

        interrupt = interrupt_signal.recv() => {
            debug!(?interrupt);
            Some(CancelKind::Interrupt)
        }

        terminate = terminate_signal.recv() => {
            debug!(?terminate);
            Some(CancelKind::Terminate)
        }

    };

    debug!(?cancellation);

    meter_provider
        .shutdown()
        .inspect(|shutdown| debug!(?shutdown))?;

    if let Some(CancelKind::Timeout) = cancellation {
        sleep(Duration::from_secs(5)).await;
    }

    debug!(abort = set.len());
    set.abort_all();

    while !set.is_empty() {
        _ = set.join_next().await;
    }

    println!("Simulation complete");
    Ok(())
}
