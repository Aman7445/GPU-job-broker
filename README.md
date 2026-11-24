# GPU Job Broker

The Job Broker accepts GPU jobs over HTTP, schedules them on simulated nodes, and reports usage and cost by team. It runs on FastAPI and Uvicorn with SQLite for jobs, nodes, the simulation clock, and billing history. You can run it on a regular CPU machine.

## Start Uvicorn directly

From the repository root, create a Python 3.12 virtual environment and install the dependencies.

```sh
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m uvicorn job_broker:app --host 127.0.0.1 --port 8081 --workers 1
```

Open [Swagger UI](http://127.0.0.1:8081/docs) to try the API. With the virtual environment active, `python job_broker.py` also starts the service on port 8081.

For manual clock testing, start it with the command below. It uses a separate database for the demo and enables `POST /sim/advance`.

```sh
BROKER_DB=demo.sqlite BROKER_CLOCK_SPEED=0 \
  python -m uvicorn job_broker:app --host 127.0.0.1 --port 8081 --workers 1
```

Use `runtime:app` in either Uvicorn command to add the same health and metrics endpoints that the container exposes. Each SQLite database supports one broker process. Stop the running instance before starting another one with the same database.

## HTTP API

These routes are defined in [`job_broker.py`](job_broker.py). Request bodies use JSON. Job and node timestamps are simulated Unix seconds.

| Method and path | Request | Result |
| --- | --- | --- |
| `POST /jobs` | Send `Idempotency-Key` and a JSON object with `team`, `gpus`, `estimated_minutes`, and optional `priority`. | Creates a job and returns `id` and `status`. Returns `201` for a new job, `200` for a matching retry, or `409` if the same team and key are reused with different submission details. |
| `GET /jobs/{job_id}` | Pass the numeric job ID in the path. | Returns the job with its current status, assigned or reserved node, and timestamps. Returns `404` for an unknown job. |
| `POST /jobs/{job_id}/cancel` | Pass the job ID. No request body is needed. | Cancels a queued or running job and releases any GPUs it held. Returns the job. |
| `POST /jobs/{job_id}/complete` | Pass the job ID and send `{"outcome":"succeeded"}` or `{"outcome":"failed"}`. | Records the outcome of a running job and returns the job. Returns `409` if the job is still queued. |
| `GET /queue` | Optional query parameters `after` and `limit`. | Lists queued jobs in ID order, including jobs with a reservation on a booting node. |
| `GET /nodes` | Optional query parameters `after`, `limit`, and `include_stopped=true`. | Lists booting and ready nodes, with running and reserved GPU counts. Set `include_stopped=true` to also see failed and terminated nodes. |
| `GET /costs` | Required query parameters `from` and `to` as ISO 8601 timestamps or whole Unix seconds. | Returns the window, the simulation time as `as_of`, the total as `total_usd`, and each team's `gpu_hours` and `cost_usd`. The start is inclusive and the end is exclusive. |
| `POST /sim/advance` | Available with `BROKER_CLOCK_SPEED=0`. Send a JSON body such as `{"seconds":600}` with an integer from 1 to 86,400. | Advances the simulated clock, processes scheduling and lifecycle events, and returns the new `sim_time`. |

- Submit `gpus` as an integer from the set `1`, `2`, `4`, or `8`. Provide a positive duration of up to 5,256,000 minutes. Priority is `low`, `normal`, or `high` and defaults to `normal`.
- Generate a fresh idempotency key for each intentional new job. Reuse the same key when retrying that submission. After trimming, team names allow 1–100 characters and keys allow 1–200 characters.
- A job starts as `queued`, enters `running` when its node is ready, and ends as `succeeded`, `failed`, or `cancelled`. A queued job can go straight to `cancelled`. A cancel or complete call on a terminal job returns the existing job.
- List responses contain `items` and `next_after`. The first page defaults to `after=0&limit=100`. The maximum limit is 500. When a response supplies a cursor, pass it as `after` for the next page.
- Cost windows require `to` to be later than `from`. Timestamps without a time zone use UTC. GPU-hours are requested GPUs times the hours actually run within the window. Cost includes boot time, idle time, and any adjustment for the 10-minute node billing minimum.

### Submit a job

This example uses direct Uvicorn on port 8081. For the container deployment, use port 8080.

```sh
curl -fsS http://127.0.0.1:8081/jobs \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: vision-demo-001' \
  -d '{"team":"vision","gpus":2,"estimated_minutes":15,"priority":"normal"}'
```

### Operational endpoints

The container serves [`runtime.py`](runtime.py), which adds these endpoints to the API.

| Method and path | Result |
| --- | --- |
| `GET /livez` | Confirms that the HTTP process is responding. |
| `GET /readyz` | Reads the broker state from SQLite and returns readiness and the current `sim_time`. |
| `GET /metrics` | Returns Prometheus-format simulation time, oldest queued job age, capacity errors, and job and node counts by state. |

## Run with Docker or Podman

Use Docker with Docker Compose, or Podman with a Compose provider. On macOS, use `podman machine start` if the machine is stopped. For a first-time setup, run `podman machine init` before starting it. Run `podman compose version` to confirm that a provider is available. The Podman deployment was tested with Docker Compose as the provider. [Podman Compose documentation](https://docs.podman.io/en/stable/markdown/podman-compose.1.html)

From the repository root, choose one of the following.

**Docker**

```sh
export BROKER_IMAGE=gpu-job-broker:local
./release.sh "$BROKER_IMAGE"
docker compose up -d --no-build --wait
```

For a quick Docker build and start, use `docker compose up --build -d --wait`.

**Podman**

```sh
export BROKER_IMAGE=localhost/gpu-job-broker:local
CONTAINER_ENGINE=podman ./release.sh "$BROKER_IMAGE"
podman compose up -d --no-build --wait
```

With either engine, check readiness and open [Swagger UI](http://127.0.0.1:8080/docs).

```sh
curl -fsS http://127.0.0.1:8080/readyz
```

The image runs as a non-root user. The default published port is available on host loopback. For shared access, put an authenticated HTTPS gateway in front of the service. Team names are supplied by the caller.

| Setting | Default | Effect |
| --- | --- | --- |
| `BROKER_IMAGE` | `gpu-job-broker:local` | Selects the local image or release reference for Compose. |
| `BROKER_HTTP_PORT` | `8080` | Selects the loopback port on the container host. |
| `BROKER_DB` | `submissions.sqlite` beside `job_broker.py` on the host and `/data/broker.sqlite` in the container | Sets the database path for the broker process. |
| `BROKER_CLOCK_SPEED` | `60` | Advances 60 simulated seconds per real second. Use `0` for manual time. |
| `BROKER_IDLE_SECONDS` | `300` | Sets the idle-node reaping timeout in simulated seconds. |

Compose mounts the named `broker-data` volume under `/data`. Keep the Compose project name stable so later starts and upgrades reuse the database and its journal. Run one replica per database.

## Build, publish, and deploy

The [release script](release.sh) builds the image, runs the application and replay tests in it, then checks readiness, metrics, persistence across container replacement, and backup and restore on a temporary volume. It can run locally or in CI on a runner with a container engine and `curl`. Build for the deployment host's CPU architecture.

**Publish with Docker**

```sh
docker login ghcr.io
./release.sh ghcr.io/your-org/gpu-job-broker:v0.1.0 --push
```

**Publish with Podman**

```sh
podman login ghcr.io
CONTAINER_ENGINE=podman ./release.sh ghcr.io/your-org/gpu-job-broker:v0.1.0 --push
```

Copy [`compose.yaml`](compose.yaml) to a stable directory on the deployment host. Authenticate there if the image is private. Use the published image reference including its digest. Save the `BROKER_IMAGE` assignment in the host's `.env` file so future Compose commands select the same release.

```sh
export BROKER_IMAGE='ghcr.io/your-org/gpu-job-broker@sha256:YOUR_RELEASE_DIGEST'
```

**Deploy with Docker**

```sh
docker pull "$BROKER_IMAGE"
docker compose up -d --no-build --wait
```

**Deploy with Podman**

```sh
podman pull "$BROKER_IMAGE"
podman compose up -d --no-build --wait
```

Run the readiness check after rollout. Compose waits for the health check and reuses the mounted volume when it replaces the container. [Docker Compose rollout](https://docs.docker.com/reference/cli/docker/compose/up/)

## Operate the service

Run Compose commands from the directory containing `compose.yaml`.

**Docker**

```sh
docker compose ps
docker compose logs --tail 100 broker
docker compose stop broker
docker compose up -d --no-build --wait
```

**Podman**

```sh
podman compose ps
podman compose logs --tail 100 broker
podman compose stop broker
podman compose up -d --no-build --wait
```

Use the stop and start commands for planned maintenance. The simulator resumes from its last committed second. Check the health and metrics endpoints with either engine.

```sh
curl -fsS http://127.0.0.1:8080/livez
curl -fsS http://127.0.0.1:8080/readyz
curl -fsS http://127.0.0.1:8080/metrics
```

- If the automatic clock stalls, check the logs for clock errors.
- If the queue grows, check capacity errors and available GPUs in `/nodes`. A large job needs all its GPUs on one node.
- Use `/nodes?include_stopped=true` to inspect boot failures and reaped nodes. Use `/costs` to check spending for a time window.

## Back up, restore, and roll back

Store snapshots outside the container on durable storage. SQLite takes a consistent snapshot while the broker continues serving requests. Take a snapshot before each upgrade. [Python SQLite backup API](https://docs.python.org/3.12/library/sqlite3.html#sqlite3.Connection.backup)

Set a backup destination on the host. Choose the backup command for your engine.

```sh
umask 077
backup_dir=/path/to/operator-backups
mkdir -p "$backup_dir"
snapshot="$backup_dir/broker-$(date -u +%Y%m%dT%H%M%SZ).sqlite"
```

**Docker backup**

```sh
docker compose exec -T broker python -m sqlite_ops backup > "$snapshot.tmp" &&
  mv "$snapshot.tmp" "$snapshot"
```

**Podman backup**

```sh
podman compose exec -T broker python -m sqlite_ops backup > "$snapshot.tmp" &&
  mv "$snapshot.tmp" "$snapshot"
```

To restore, set `snapshot` to the file you want to recover and stop the broker first. Restoring replaces the current state and resets the simulation clock to the snapshot. Keep a separate copy of newer state if you need it. The restore command checks the schema, integrity, and foreign keys before applying the snapshot.

**Docker restore**

```sh
docker compose stop broker &&
  docker compose run --rm -T --no-deps broker python -m sqlite_ops restore --replace < "$snapshot" &&
  docker compose up -d --no-build --wait
```

**Podman restore**

```sh
podman compose stop broker &&
  podman compose run --rm -T --no-deps broker python -m sqlite_ops restore --replace < "$snapshot" &&
  podman compose up -d --no-build --wait
```

Check `/readyz`, a known job, and a known cost window after recovery. To roll back to an earlier image with a compatible SQLite schema, set `BROKER_IMAGE` to its digest and repeat the deploy steps. Preserve the named volume during routine cleanup.

## Replay the Northwind trace

Replay uses temporary databases and runs both the fair and priority FIFO policies against the bundled CSV with the same provider seed and idle timeout. The JSON output contains per-team wait percentiles, costs, and the input audit. See the [policy comparison](policy_comparison.md) for the results and how to read them.

**Local Python with the virtual environment active**

```sh
python replay.py --seed 42 --idle-seconds 300
python -m unittest discover -s tests -v
```

**Docker**

```sh
docker run --rm "$BROKER_IMAGE" python replay.py --seed 42 --idle-seconds 300
```

**Podman**

```sh
podman run --rm "$BROKER_IMAGE" python replay.py --seed 42 --idle-seconds 300
```
