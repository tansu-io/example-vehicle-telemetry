use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use governor::{
    Quota, RateLimiter,
    clock::{Clock, QuantaClock},
};
use rand::{Rng, SeedableRng, rngs::SmallRng};
use serde::Serialize;
use std::{
    fs::File,
    io::BufReader,
    path::PathBuf,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tansu_client::{Client, ConnectionManager};
use tansu_sans_io::{
    produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
    record::{Record, deflated, inflated},
};
use tokio::task::JoinSet;
use url::Url;

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

    eprintln!(
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

    let deadline = tokio::time::Instant::now() + args.duration;
    let interval = args.interval;
    let topic = Arc::new(args.topic.clone());
    let n = vrms.len();

    let mut set = JoinSet::new();
    let mut seed_rng = rand::rng();

    for (i, vrm) in vrms.into_iter().enumerate() {
        let client = client.clone();
        let limiter = limiter.clone();
        let topic = topic.clone();
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
            let clock = QuantaClock::default();
            let mut rng = SmallRng::seed_from_u64(rng_seed);

            if !initial_delay.is_zero() {
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => return,
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
                                _ = tokio::time::sleep_until(deadline) => return,
                                _ = tokio::time::sleep(wait) => {}
                            }
                        }
                    }
                }

                if tokio::time::Instant::now() >= deadline {
                    return;
                }

                state.step(&mut rng);

                let ts = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as i64;

                let json = match serde_json::to_string(&state) {
                    Ok(j) => j,
                    Err(e) => {
                        eprintln!("JSON serialisation error for {vrm}: {e}");
                        continue;
                    }
                };

                let record = Record::builder()
                    .key(Some(Bytes::from(vrm.clone())))
                    .value(Some(Bytes::from(json)));

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

                if let Err(e) = client.call(req).await {
                    eprintln!("Produce error for {vrm}: {e}");
                }
            }
        });
    }

    tokio::time::sleep_until(deadline).await;
    eprintln!("Aborting...");
    set.abort_all();
    while set.join_next().await.is_some() {}

    eprintln!("Simulation complete");
    Ok(())
}
