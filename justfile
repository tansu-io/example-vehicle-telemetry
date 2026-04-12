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

telemetry-topic-create: (topic-create "telemetry" "--config" "cleanup.policy=compact")

plate-generator profile="dev" count="100000":
    target/{{ replace(profile, "dev", "debug") }}/plate_generator --count {{ count }}

create-topics profile="dev":
    target/{{ replace(profile, "dev", "debug") }}/create_topics

vehicle-simulator profile="dev" duration="10m" interval="1m":
    target/{{ replace(profile, "dev", "debug") }}/vehicle_simulator --duration {{ duration }} --interval {{ interval }}

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

docker-prune:
    docker system prune --force
