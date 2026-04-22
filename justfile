set dotenv-load := true

up: clean-tansu-db docker-compose-up telemetry-topic-create

down: docker-compose-down

[private]
docker-compose-up *args: docker-compose-down
    docker compose --ansi never --progress plain up --no-color --quiet-pull --wait --detach {{ args }}

[private]
docker-compose-down *args:
    docker compose down --remove-orphans --volumes {{ args }}

docker-compose-ps:
    docker compose ps

[private]
docker-compose-logs *args:
    docker compose logs {{ args }}

clean: clean-tansu-db
    cargo clean

[private]
topic-create topic *args:
    docker compose exec broker /tansu topic create {{ topic }} {{ args }}

telemetry-topic-create: (topic-create "telemetry" "--config" "cleanup.policy=compact" "--config" "tansu.virtual=true")

plate-generator count="400000" profile="dev":
    target/{{ replace(profile, "dev", "debug") }}/plate_generator --count {{ count }}

create-topics profile="dev":
    target/{{ replace(profile, "dev", "debug") }}/create_topics

vehicle-simulator duration="10m" interval="1m" profile="dev": build
    target/{{ replace(profile, "dev", "debug") }}/vehicle_simulator --duration {{ duration }} --interval {{ interval }} | tee simulator.log

grafana-ui:
    open http://localhost:3000/

prometheus-ui:
    open http://localhost:9090/

build:
    cargo build

logs:
    docker compose logs --no-log-prefix --no-color broker > broker.log

clean-tansu-db:
    rm -f data/*.db*

prune:
    docker system prune --force

ps:
    docker compose ps

frames:
    cat broker.log|grep -E "body: \w+(Request|Response)|UnexpectedEof" --only-matching

telemetry-consumer:
    docker compose exec kafka39 /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server broker:9092 \
        --timeout-ms 15000 \
        --topic telemetry \
        --from-beginning \
        --property print.timestamp=true \
        --property print.key=true \
        --property print.offset=true \
        --property print.partition=true \
        --property print.value=true

consumer vrm:
    docker compose exec kafka39 /opt/kafka/bin/kafka-console-consumer.sh \
        --bootstrap-server broker:9092 \
        --timeout-ms 15000 \
        --topic 'telemetry/{{ vrm }}' \
        --from-beginning \
        --property print.timestamp=true \
        --property print.key=true \
        --property print.offset=true \
        --property print.partition=true \
        --property print.value=true
