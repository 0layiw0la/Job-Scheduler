# Job Scheduler

A durable webhook scheduler. Submit a job over HTTP, and it fires a webhook at the exact right second, surviving process death and concurrent worker contention.

## Features

* **Stateless Workers:** No leader election, queue broker, or coordinator process. Postgres acts as the only state, making it easy to scale horizontally across multiple instances.
* **Database Concurrency:** Concurrency relies entirely on a single database transaction (`FOR UPDATE SKIP LOCKED`).
* **Guaranteed Delivery:** At-least-once delivery with idempotency keys for exactly-once effects.
* **Smart Retries:** Constant or exponential backoff with full jitter, honoring `Retry-After`.
* **Timezone & DST Safe:** Handles spring gaps and autumn overlaps accurately.
* **SSRF Protection:** Resolves host endpoints and rejects private/loopback addresses.

## Built With

* Python
* PostgreSQL
* FastAPI
* SQLAlchemy & Alembic
* Docker

## Getting Started

### Prerequisites

* Python 3.10+
* PostgreSQL
* Docker (for containerized deployment)

### Setup (Docker)

Run the complete stack and scale out instantly:

```bash
docker compose up --build
docker compose up --scale scheduler=10

```

### Setup (Local Development)

Install dependencies:

```bash
pip install -e ".[dev]"

```

Set your environment variables:

```bash
export DATABASE_URL=postgresql://scheduler:scheduler@localhost:5432/scheduler
export SCHEDULER_API_KEY=$(openssl rand -hex 16)

```

### Database Setup

Apply migrations to prepare the database schema:

```bash
alembic upgrade head

```

### Run the application

```bash
scheduler serve --with-worker  # Runs API + worker loops in one process
scheduler worker               # Runs loops only (for scaling out)

```

## Usage

**Schedule a Job**

```bash
curl -s -X POST localhost:8000/v1/jobs \
  -H "Authorization: Bearer $SCHEDULER_API_KEY" \
  -H "Idempotency-Key: keep-warm-1" \
  -d '{
        "name": "keep my api warm",
        "cron": "*/10 * * * *",
        "timezone": "Europe/London",
        "endpoint": "https://my-service.onrender.com/healthz",
        "method": "GET",
        "overlap_policy": "skip",
        "max_attempts": 3
      }'

```

**Preview Schedules (CLI)**
Check exact firing times locally without hitting the API:

```bash
scheduler next --cron "30 2 * * *" --timezone America/New_York --count 4

```

## Limitations & Roadmap

* **No UI:** The `/v1/cluster` and execution APIs exist, but there is no frontend dashboard yet.
* **Polling:** Currently relies on polling. Postgres `LISTEN`/`NOTIFY` is planned to trigger workers instantly and lower the latency tail.
* **Rate Limiting:** Currently implemented per-instance in memory; cluster-wide rate limits are planned.
* **Out of Scope (Permanently):** Job dependency graphs (use Airflow), running arbitrary user code (this is strictly a webhook cannon), and multi-tenancy. Sub-second precision is not supported.