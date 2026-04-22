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
    collections::BTreeMap,
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
    ErrorCode,
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
use tracing_subscriber::{
    EnvFilter, fmt::format::FmtSpan, layer::SubscriberExt, util::SubscriberInitExt,
};
use url::Url;

// OTel default explicit bucket boundaries (scale matches our histogram unit).
// 13 bounds → 14 buckets (last is overflow).
const DEFAULT_BOUNDS: &[f64] = &[
    0.0, 5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 2500.0, 5000.0, 7500.0,
    10000.0,
];

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
            .with_boundaries(DEFAULT_BOUNDS.into())
            .with_unit("ms")
            .with_description("Produce API latencies in microsecond")
            .build()
    });

#[derive(Copy, Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CancelKind {
    Interrupt,
    Terminate,
    Timeout,
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
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
                    .as_ref()
                    .map_or(self.started_at, |previous| previous.observation.taken_at),
            )
            .expect("duration")
    }

    fn bytes_sent(&self) -> u64 {
        self.current.observation.bytes_sent
            - self
                .previous
                .as_ref()
                .map(|previous| previous.observation.bytes_sent)
                .unwrap_or_default()
    }

    fn records_sent(&self) -> u64 {
        self.current.observation.record_count
            - self
                .previous
                .as_ref()
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
        if let Some(minimum) = self.current.latency.min
            && let Some(mean) = self.current.latency.mean
            && let Some(max) = self.current.latency.max
        {
            write!(
                f,
                "elapsed: {}, {} records sent, {:.1} records/s, ({}/s), min: {}, mean: {}, {}, max: {}",
                self.elapsed().format_duration(),
                self.records_sent(),
                self.records_sent_per_second(),
                self.bandwidth().format_iec(),
                minimum.format_duration(),
                mean.format_duration(),
                self.current
                    .latency
                    .percentiles
                    .iter()
                    .map(|(percentile, duration)| {
                        format!("{percentile}: {}", duration.format_duration())
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
                max.format_duration(),
            )
        } else {
            write!(
                f,
                "elapsed: {}, {} records sent, {:.1} records/s, ({}/s)",
                self.elapsed().format_duration(),
                self.records_sent(),
                self.records_sent_per_second(),
                self.bandwidth().format_iec(),
            )
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, PartialOrd)]
struct Latency {
    min: Option<Duration>,
    max: Option<Duration>,
    mean: Option<Duration>,

    percentiles: BTreeMap<Percentile, Duration>,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum Percentile {
    TwoNines,
    ThreeNines,
}

impl Display for Percentile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} %ile", self.as_ref() * 100.0)
    }
}

impl AsRef<f64> for Percentile {
    fn as_ref(&self) -> &f64 {
        match self {
            Percentile::TwoNines => &0.99,
            Percentile::ThreeNines => &0.999,
        }
    }
}

/// Core percentile calculation from explicit-bucket histogram data.
///
/// `bounds` has N entries; `counts` has N+1 entries (last bucket is the overflow).
/// Returns `None` when there are no observations.
fn percentile_from_buckets(bounds: &[f64], counts: &[u64], p: &f64) -> Option<Duration> {
    let total: u64 = counts.iter().sum();
    if total == 0 {
        return None;
    }

    let target = ((total as f64 * p).ceil() as u64).max(1);
    let mut cumulative = 0u64;

    for (i, &count) in counts.iter().enumerate() {
        cumulative += count;
        if cumulative >= target {
            let lower = if i == 0 { 0.0f64 } else { bounds[i - 1] };
            let upper = if i < bounds.len() {
                bounds[i]
            } else {
                bounds.last().copied().unwrap_or(lower) * 2.0
            };
            let rank_in_bucket = target - (cumulative - count);
            let frac = if count > 0 {
                rank_in_bucket as f64 / count as f64
            } else {
                0.5
            };
            return Some(Duration::from_micros(
                (lower + frac * (upper - lower)) as u64,
            ));
        }
    }

    None
}

fn histogram_percentile(histogram: &Histogram<u64>, p: &f64) -> Option<Duration> {
    let first = histogram.data_points().next()?;
    let bounds: Vec<f64> = first.bounds().collect();
    let n_buckets = bounds.len() + 1;

    let mut agg_counts = vec![0u64; n_buckets];
    for dp in histogram.data_points() {
        for (i, c) in dp.bucket_counts().enumerate() {
            if i < n_buckets {
                agg_counts[i] += c;
            }
        }
    }

    percentile_from_buckets(&bounds, &agg_counts, &p)
}

impl From<&Histogram<u64>> for Latency {
    fn from(histogram: &Histogram<u64>) -> Self {
        let min = histogram
            .data_points()
            .filter_map(|dp| dp.min())
            .min()
            .map(Duration::from_micros)
            .inspect(|min| debug!(min = %min.format_duration()));

        let max = histogram
            .data_points()
            .filter_map(|dp| dp.max())
            .max()
            .map(Duration::from_micros)
            .inspect(|max| debug!(max = %max.format_duration()));

        let sum = histogram.data_points().map(|dp| dp.sum()).sum::<u64>() as f64;
        let count = histogram.data_points().map(|dp| dp.count()).sum::<u64>() as f64;

        debug!(sum, count);

        let mean = Some(Duration::from_micros((sum / count) as u64))
            .inspect(|mean| debug!(mean = %mean.format_duration()));

        let percentiles = [Percentile::TwoNines, Percentile::ThreeNines]
            .into_iter()
            .filter_map(|percentile| {
                histogram_percentile(histogram, percentile.as_ref())
                    .map(|duration| (percentile, duration))
                    .inspect(|(percentile, duration)|debug!(%percentile, duration = %duration.format_duration()))
            })
            .collect();

        Self {
            min,
            max,
            mean,
            percentiles,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, PartialOrd)]
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

            (scope, metric, _) => debug!(scope, metric),
        }
    }
}

impl PushMetricExporter for MetricExporter {
    async fn export(&self, metrics: &ResourceMetrics) -> OTelSdkResult {
        let cancelled = self.cancellation.is_cancelled();

        if cancelled {
            if let Some(ref previous) = *self.previous.lock().expect("previous") {
                let mut info = Info::new(self.started_at);
                info.current = previous.clone();

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
#[command(about = "Simulate vehicle plates by producing GPS updates to Kafka")]
struct Args {
    /// CSV file containing vehicle plates (one per line)
    #[arg(short, long, default_value = "vrm.csv")]
    input: PathBuf,

    /// Kafka broker URL
    #[arg(
        short,
        long,
        env = "ADVERTISED_LISTENER_URL",
        default_value = "tcp://localhost:9092"
    )]
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
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(
            tracing_subscriber::fmt::layer()
                .with_level(true)
                .with_line_number(true)
                .with_thread_ids(false)
                .with_span_events(FmtSpan::FULL),
        )
        .init();

    let token = CancellationToken::new();
    debug!(?token);

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

    // Read every plate from the CSV (no header row).
    let plates: Vec<String> = {
        let file = File::open(&args.input).with_context(|| format!("opening {:?}", args.input))?;
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(BufReader::new(file));
        rdr.records()
            .map(|r| Ok(r?.into_iter().next().unwrap_or_default().to_owned()))
            .collect::<Result<Vec<String>>>()?
    };

    println!(
        "Loaded {} vehicles; simulating for {} at {} per vehicle; broker: {}",
        plates.len(),
        humantime::format_duration(args.duration),
        humantime::format_duration(args.interval),
        args.broker,
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
    let n = plates.len();

    let mut set = JoinSet::new();

    let mut seed_rng = rand::rng();

    for (i, plate) in plates.into_iter().enumerate() {
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
            let attributes = [KeyValue::new("plate", plate.clone())];

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
                    match limiter.check_key(&plate) {
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
                        eprintln!("JSON serialisation error for {plate}: {e}");
                        continue;
                    }
                };

                let record = Record::builder()
                    .key(Some(Bytes::from(format!(r#""{plate}""#))))
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
                        eprintln!("Batch build error for {plate}: {e}");
                        continue;
                    }
                };

                let deflated_batch = match deflated::Batch::try_from(batch) {
                    Ok(b) => b,
                    Err(e) => {
                        eprintln!("Deflate error for {plate}: {e}");
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

                match client.call(req).await {
                    Err(e) => {
                        eprintln!("Produce error for {plate}: {e}");
                    }

                    Ok(response) => {
                        for topic in response.responses.as_deref().unwrap_or_default() {
                            for partition in
                                topic.partition_responses.as_deref().unwrap_or_default()
                            {
                                assert_eq!(i16::from(ErrorCode::None), partition.error_code);
                            }
                        }

                        PRODUCE_RECORD_COUNT.add(1u64, &attributes);
                        PRODUCE_API_DURATION.record(
                            produce_start
                                .elapsed()
                                .inspect(|duration| {
                                    debug!(plate, produce_duration = duration.as_micros() as u64)
                                })
                                .map_or(0, |duration| duration.as_micros() as u64),
                            &attributes,
                        );
                    }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn counts_uniform(n: u64) -> Vec<u64> {
        // Spread n observations evenly across the first 10 buckets (0–1000 ms).
        let buckets = 10usize;
        let per = n / buckets as u64;
        let mut counts = vec![0u64; DEFAULT_BOUNDS.len() + 1];
        for i in 0..buckets {
            counts[i] = per;
        }
        counts
    }

    #[test]
    fn empty_histogram_returns_none() {
        let counts = vec![0u64; DEFAULT_BOUNDS.len() + 1];
        assert_eq!(
            percentile_from_buckets(DEFAULT_BOUNDS, &counts, &0.99),
            None
        );
        assert_eq!(
            percentile_from_buckets(DEFAULT_BOUNDS, &counts, &0.999),
            None
        );
    }

    #[test]
    fn single_observation_in_first_bucket() {
        // One observation in bucket [0, 5). p99 and p99.9 should both land there.
        let mut counts = vec![0u64; DEFAULT_BOUNDS.len() + 1];
        counts[0] = 1;
        let p99 = percentile_from_buckets(DEFAULT_BOUNDS, &counts, &0.99).unwrap();
        let p99_9 = percentile_from_buckets(DEFAULT_BOUNDS, &counts, &0.999).unwrap();
        assert!(p99 < Duration::from_micros(5), "p99={p99:?}");
        assert!(p99_9 < Duration::from_micros(5), "p99.9={p99_9:?}");
    }

    #[test]
    fn all_in_one_bucket_interpolates_within_bounds() {
        // 1000 observations in bucket [100, 250) — all percentiles must lie in [100, 250).
        let mut counts = vec![0u64; DEFAULT_BOUNDS.len() + 1];
        // bucket index 7 spans [100, 250)
        counts[7] = 1000;
        let p99 = percentile_from_buckets(DEFAULT_BOUNDS, &counts, &0.99).unwrap();
        let p99_9 = percentile_from_buckets(DEFAULT_BOUNDS, &counts, &0.999).unwrap();
        assert!(
            p99 >= Duration::from_micros(100) && p99 < Duration::from_micros(250),
            "p99={p99:?}"
        );
        assert!(
            p99_9 >= Duration::from_micros(100) && p99_9 < Duration::from_micros(250),
            "p99.9={p99_9:?}"
        );
        assert!(p99_9 >= p99, "p99.9 should be >= p99");
    }

    #[test]
    fn p99_interpolates_correctly_uniform_distribution() {
        // 1000 observations, 100 per bucket across buckets 0–9.
        // p99 = rank 990; buckets 0–8 hold 900, so rank 990 lands in bucket 9 [500, 750).
        // rank_in_bucket = 990 - 900 = 90, frac = 90/100 = 0.9
        // value = 500 + 0.9 * (750 - 500) = 500 + 225 = 725 ms
        let counts = counts_uniform(1000);
        let p99 = percentile_from_buckets(DEFAULT_BOUNDS, &counts, &0.99).unwrap();
        assert_eq!(p99, Duration::from_micros(725), "p99={p99:?}");
    }

    #[test]
    fn p99_9_interpolates_correctly_uniform_distribution() {
        // p99.9 = rank ceil(1000 * 0.999) = 999; same bucket 9 [500, 750).
        // rank_in_bucket = 999 - 900 = 99, frac = 99/100 = 0.99
        // value = 500 + 0.99 * 250 = 500 + 247.5 → truncated to 747 ms
        let counts = counts_uniform(1000);
        let p99_9 = percentile_from_buckets(DEFAULT_BOUNDS, &counts, &0.999).unwrap();
        assert_eq!(p99_9, Duration::from_micros(747), "p99.9={p99_9:?}");
    }

    #[test]
    fn p99_9_greater_than_or_equal_to_p99() {
        let counts = counts_uniform(10_000);
        let p99 = percentile_from_buckets(DEFAULT_BOUNDS, &counts, &0.99).unwrap();
        let p99_9 = percentile_from_buckets(DEFAULT_BOUNDS, &counts, &0.999).unwrap();
        assert!(p99_9 >= p99, "p99.9={p99_9:?} p99={p99:?}");
    }

    #[test]
    fn overflow_bucket_uses_doubled_upper_bound() {
        // All observations in the overflow bucket (> 5000 ms).
        // Upper bound is 5000 * 2 = 10000, lower is 5000.
        let mut counts = vec![0u64; DEFAULT_BOUNDS.len() + 1];
        *counts.last_mut().unwrap() = 100;
        let p99 = percentile_from_buckets(DEFAULT_BOUNDS, &counts, &0.99).unwrap();
        assert!(p99 >= Duration::from_micros(5000), "p99={p99:?}");
        assert!(p99 <= Duration::from_micros(10000), "p99={p99:?}");
    }

    #[test]
    fn p99_exact_boundary_one_percent_in_last_bucket() {
        // 99 observations in bucket 0 [0,5), 1 in bucket 1 [5,10).
        // p99 = rank 99 → still in bucket 0; p99.9 = rank 100 → in bucket 1.
        let mut counts = vec![0u64; DEFAULT_BOUNDS.len() + 1];
        counts[0] = 99;
        counts[1] = 1;
        let p99 = percentile_from_buckets(DEFAULT_BOUNDS, &counts, &0.99).unwrap();
        let p99_9 = percentile_from_buckets(DEFAULT_BOUNDS, &counts, &0.999).unwrap();
        assert!(
            p99 < Duration::from_micros(5),
            "p99={p99:?} expected in [0, 5)"
        );
        assert!(
            p99_9 >= Duration::from_micros(5) && p99_9 < Duration::from_micros(10),
            "p99.9={p99_9:?} expected in [5, 10)"
        );
    }
}
