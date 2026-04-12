use anyhow::{Context, Result};
use clap::Parser;
use std::{fs::File, io::BufReader, path::PathBuf};
use tansu_client::{Client, ConnectionManager};
use tansu_sans_io::create_topics_request::{CreatableTopic, CreateTopicsRequest};
use url::Url;

#[derive(Parser)]
#[command(about = "Create a Kafka topic for each VRM in a CSV file")]
struct Args {
    /// CSV file to read plates from
    #[arg(short, long, default_value = "vrm.csv")]
    input: PathBuf,

    /// Kafka broker URL
    #[arg(short, long, default_value = "tcp://localhost:9092")]
    broker: Url,

    /// Number of partitions per topic
    #[arg(short, long, default_value_t = 1)]
    partitions: i32,

    /// Replication factor per topic
    #[arg(short, long, default_value_t = 1)]
    replication_factor: i16,

    /// Number of topics to create per request
    #[arg(long, default_value_t = 100)]
    batch_size: usize,

    /// Timeout in milliseconds for each CreateTopics request
    #[arg(long, default_value_t = 30_000)]
    timeout_ms: i32,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Read all VRM names from the CSV, skipping the header row.
    let plates = {
        let file = File::open(&args.input).with_context(|| format!("opening {:?}", args.input))?;
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(BufReader::new(file));
        rdr.records()
            .map(|r| Ok(r?.into_iter().next().unwrap_or_default().to_owned()))
            .collect::<Result<Vec<String>>>()?
    };

    eprintln!("Read {} plates from {:?}", plates.len(), args.input);

    let pool = ConnectionManager::builder(args.broker.clone())
        .client_id(Some("example-vrm-create-topics".into()))
        .build()
        .await
        .with_context(|| format!("connecting to {}", args.broker))?;

    let client = Client::new(pool);

    let mut created = 0usize;
    let mut errors = 0usize;

    for (batch_num, chunk) in plates.chunks(args.batch_size).enumerate() {
        let topics: Vec<CreatableTopic> = chunk
            .iter()
            .map(|name| {
                CreatableTopic::default()
                    .name(name.clone())
                    .num_partitions(args.partitions)
                    .replication_factor(args.replication_factor)
                    .assignments(Some([].into()))
                    .configs(Some([].into()))
            })
            .collect();

        let req = CreateTopicsRequest::default()
            .topics(Some(topics))
            .timeout_ms(args.timeout_ms)
            .validate_only(Some(false));

        let resp = client
            .call(req)
            .await
            .with_context(|| format!("batch {batch_num}"))?;

        if let Some(results) = resp.topics {
            for topic in results {
                if topic.error_code == 0 {
                    created += 1;
                } else {
                    eprintln!(
                        "error creating topic {:?}: code {}{}",
                        topic.name,
                        topic.error_code,
                        topic
                            .error_message
                            .map(|m| format!(" ({m})"))
                            .unwrap_or_default()
                    );
                    errors += 1;
                }
            }
        }
    }

    eprintln!("Done — created: {created}, errors: {errors}");
    Ok(())
}
