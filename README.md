# Vehicle Telemetry Example

A demo that simulates a fleet of UK vehicles sending GPS telemetry to a [Tansu](https://github.com/tansu-io/tansu) broker. Each vehicle is identified by its Vehicle Registration Mark (VRM) and periodically produces a JSON location update to a Kafka-compatible topic.

## Prerequisites

- [Rust](https://rustup.rs/) (edition 2024)
- [Docker](https://docs.docker.com/get-docker/) with Compose
- [just](https://github.com/casey/just)

## Quick start

```sh
cargo build
just up                  # start broker, Prometheus, and Grafana; create the telemetry topic
just plate-generator     # generate vrm.csv (100,000 unique UK plates)
just create-topics       # create one Kafka topic per VRM
just vehicle-simulator   # run for 10 minutes, one update per vehicle per minute
```

Open the Grafana dashboard at <http://localhost:3000> to watch broker metrics in real time.

## How it works

```
plate_generator ──► vrm.csv ──► create_topics   (one Kafka topic per vehicle)
                           └──► vehicle_simulator ──► "telemetry" topic
```

### Binaries

| Binary | Purpose |
|---|---|
| `plate_generator` | Generates unique UK-style VRMs (`AB12 CDE`) following DVLA rules and writes them to a CSV file |
| `create_topics` | Reads `vrm.csv` and creates a Kafka topic for each VRM on the broker |
| `vehicle_simulator` | Reads `vrm.csv` and runs one async task per vehicle, producing JSON `{latitude, longitude, altitude}` updates to the `telemetry` topic with the VRM as the record key |

The simulator starts vehicles staggered across the first interval to avoid a burst of simultaneous requests, then rate-limits each vehicle independently using a token-bucket limiter.

### Infrastructure

| Service | URL | Description |
|---|---|---|
| Tansu broker | `tcp://localhost:9092` | Kafka-compatible broker backed by SQLite |
| Prometheus | <http://localhost:9090> | Receives OTLP metrics from the broker |
| Grafana | <http://localhost:3000> | Pre-provisioned dashboard (anonymous admin) |

Broker data is persisted to `./data/tansu.db`. Run `just clean` to remove it along with build artefacts.

## Usage reference

### `plate_generator`

```
cargo run --bin plate_generator -- [OPTIONS]

Options:
  -c, --count <COUNT>    Number of unique plates to generate [default: 400000]
  -o, --output <OUTPUT>  Output CSV file path [default: vrm.csv]
```

### `create_topics`

```
cargo run --bin create_topics -- [OPTIONS]

Options:
  -i, --input <INPUT>                    CSV file to read plates from [default: vrm.csv]
  -b, --broker <BROKER>                  Kafka broker URL [default: tcp://localhost:9092]
  -p, --partitions <PARTITIONS>          Number of partitions per topic [default: 1]
  -r, --replication-factor <FACTOR>      Replication factor per topic [default: 1]
      --batch-size <BATCH_SIZE>          Topics per CreateTopics request [default: 100]
      --timeout-ms <TIMEOUT_MS>          Request timeout in milliseconds [default: 30000]
```

### `vehicle_simulator`

```
cargo run --bin vehicle_simulator -- [OPTIONS]

Options:
  -i, --input <INPUT>        CSV file containing VRM plates [default: vrm.csv]
  -b, --broker <BROKER>      Kafka broker URL [default: tcp://localhost:9092]
  -t, --topic <TOPIC>        Kafka topic to produce to [default: telemetry]
      --interval <INTERVAL>  Update interval per vehicle (e.g. 10s, 500ms) [default: 10s]
  -d, --duration <DURATION>  Total simulation duration (e.g. 60s, 5m) [default: 60s]
```

## Development

```sh
cargo build          # build all binaries
cargo test           # run all tests
just logs            # dump broker logs to broker.log
just down            # stop and remove all containers and volumes
just clean           # remove broker database and build artefacts
```
