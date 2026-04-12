# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is a vehicle telemetry demo that simulates UK vehicles sending GPS updates to a [Tansu](https://github.com/tansu-io/tansu) broker (a Kafka-compatible broker). It consists of three CLI binaries and a Docker Compose stack.

## Commands

### Build
```sh
cargo build              # debug build
cargo build --release    # release build
```

### Test
```sh
cargo test                          # run all tests
cargo test <test_name>              # run a single test by name
cargo test -- --nocapture           # show println! output
```

### Infrastructure
```sh
just up          # tear down, start Docker Compose (broker + Prometheus + Grafana), create telemetry topic
just down        # stop and remove all containers and volumes
just logs        # dump broker logs to broker.log
```

### Running the binaries
```sh
just plate-generator              # generate vrm.csv (100k UK plates, default)
just create-topics                # create a Kafka topic per VRM from vrm.csv
just vehicle-simulator            # run simulation (10m duration, 1m interval)

# Or directly:
cargo run --bin plate_generator -- --count 1000
cargo run --bin create_topics
cargo run --bin vehicle_simulator -- --duration 60s --interval 5s
```

### Monitoring UIs
```sh
just grafana-ui      # open http://localhost:3000
just prometheus-ui   # open http://localhost:9090
```

## Architecture

### Data flow
```
plate_generator → vrm.csv → create_topics (one Kafka topic per VRM)
                          → vehicle_simulator → Kafka "telemetry" topic
```

1. **`plate_generator`** (`src/bin/plate_generator.rs`) — generates unique UK-style VRMs (format `AB12 CDE`) following DVLA rules (no I/Q/Z, valid age identifiers) and writes them to `vrm.csv`.

2. **`create_topics`** (`src/bin/create_topics.rs`) — reads `vrm.csv` and creates one Kafka topic per VRM on the broker, batched in groups of 100. Uses `tansu-client` with the `CreateTopicsRequest` API.

3. **`vehicle_simulator`** (`src/bin/vehicle_simulator.rs`) — reads `vrm.csv`, spawns one async Tokio task per vehicle. Each task produces JSON telemetry (`{latitude, longitude, altitude}`) to the `telemetry` topic using the VRM as the Kafka record key. Rate-limited via `governor` (one produce per vehicle per interval). Initial sends are staggered across one interval to avoid a thundering-herd on the broker.

### Infrastructure (Docker Compose)
- **broker** — `ghcr.io/tansu-io/tansu`, Kafka-compatible, SQLite-backed storage at `./data/tansu.db`. Exposes `:9092`.
- **prometheus** — scrapes OTLP metrics exported by the broker. Exposes `:9090`.
- **grafana** — anonymous admin access, pre-provisioned with Prometheus datasource and dashboards. Exposes `:3000`.

### Key dependencies
- `tansu-client` / `tansu-sans-io` — Kafka protocol client and wire-format types for the Tansu broker.
- `governor` — token-bucket rate limiter, keyed by VRM, to control produce frequency.
- `clap` (derive) — CLI argument parsing for all three binaries.
- `humantime` — human-readable duration parsing (`10s`, `5m`, etc.) for simulator flags.
