# Coinbase EOD Pricing Pipeline

A data pipeline that streams real-time trade data from Coinbase,
buffers it in Redis, and writes a precise end-of-day price snapshot to Snowflake
at 5PM Eastern Time each trading day.

---

## High-Level Architecture

```
Coinbase WebSocket
      │
      │  real-time stream
      ▼
CoinbaseWSListener          (thread: Coinbase SDK WSClient)
      │
      │  price-feed with UTC timestamp
      ▼
Redis Buffer
  ├── tick:last:<PRODUCT_ID>               rolling last-seen tick (overwritten each message)
  └── tick:interval:<PRODUCT_ID>:<TS>      frozen snapshots every 5 minutes (48h TTL)
      │
      ├── every 5 min: IntervalSnapshotter freezes tick:last → tick:interval
      │
      └── at 5PM ET:   EodWriter picks best tick → Snowflake MERGE
                        (prefers live tick, falls back to latest interval snapshot)
      ▼
Snowflake
  └── eod_price_feeds            one row per asset per trade date
```

---

## How to Run

```bash
# 0. Fill in snowflake configuration and credentials, make sure .env looks right
cat .env

# 1. Build and start
docker compose up --build

# 2. Verify Redis is receiving ticks
docker exec -it coinbase_eod_redis redis-cli hgetall tick:last:BTC-USD

# 3. Tail logs
docker compose logs -f pipeline
```

---

## Snowflake Table

```sql
CREATE TABLE eod_price_feeds (
    trade_date        DATE,
    product_id        VARCHAR(20),
    price             FLOAT,
    tick_ts_utc       TIMESTAMP_TZ,   -- timestamp of the actual last tick
    is_stale          BOOLEAN,        -- true if last tick > 10 min (configurable in .env) before EOD time
    source            VARCHAR(50),    -- 'coinbase_websocket'
    updated_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trade_date, product_id)   -- logical constraint
);
```

Writes use MERGE ON (trade_date, product_id) — re-running the EOD write for the same
day updates the existing row rather than inserting a duplicate.

---

## Key Decisions

### Why WebSocket?
Rather than polling the Coinbase REST API at a scheduled time, the pipeline maintains
a persistent WebSocket connection that receives every trade in real time. This gives
millisecond-precision data at 5PM rather than being bound to the granularity of a
candle interval.

### Why Redis as a buffer?
Redis sits between the WebSocket stream and Snowflake as a durable intermediary.
It decouples ingestion from writing — if the Snowflake write fails, or if the process
restarts, the tick data is not lost. Redis is configured with both RDB snapshots and
AOF (Append Only File) persistence so the buffer survives process crashes.

A pure in-memory dict would be simpler, but a single crash at 4:58PM would mean no
data for the day with no recovery path.

On the other hand, using Kafka would provide full tick history, replay, and multiple consumers, 
but adds significant operational overhead. I choose Redis in this use case:
durable enough to survive crashes, simple enough to run in a single Docker container,
and cheap enough to not require a separate cluster.

### Why interval snapshots every 5 minutes?
The live tick buffer (tick:last) is always the preferred source for the EOD write.
But if the Snapshot Scheduler fails at closing time, the live tick would already be overwritten by a newer price,
and there would be no way to get the last seen price before closing. Interval snapshots provide a recovery point 
and a minimal replayable source for the EOD writer.

---

## Project Structure

```
coinbase-sr/
├── docker-compose.yml            Redis + pipeline services
├── Dockerfile                    Python 3.9 container
├── requirements.txt
├── .env                          Credentials and config (not committed)
├── config/
│   └── redis.conf                RDB + AOF persistence settings
└── src/
    ├── main.py                   Entrypoint — wires all components, handles signals
    ├── redis_buffer.py           Redis read/write wrapper
    ├── coinbase_ws_listener.py   Coinbase WebSocket → Redis
    └── snapshot_scheduler.py     Interval snapshotter + 5PM EOD writer → Snowflake
```

---

## Data Flow in Detail

### 1. Ingestion (coinbase_ws_listener.py)
The CoinbaseWSListener subscribes to the Coinbase Advanced Trade WebSocket ticker
channel. On every incoming message it:
- Parses the timestamp, normalizing variable-precision fractional seconds to 6 digits
- Converts the timestamp to a timezone-aware UTC datetime
- Constructs and validates a Tick dataclass
- Writes to tick:last:<PRODUCT_ID> in Redis via RedisTickBuffer.update_tick()

The Coinbase SDK (WSClient) manages its own internal asyncio loop inside a daemon
thread. The main process stays alive via threading.Event.wait() and shuts down
cleanly on SIGTERM or SIGINT.

### 2. Interval Snapshotting (snapshot_scheduler.py — Loop 1)
Every 5 minutes (configured in .env), aligned to clock boundaries (:00, :05, :10...), the
IntervalSnapshotter reads tick:last:<PRODUCT_ID> and writes a frozen copy to
tick:interval:<PRODUCT_ID>:<YYYY-MM-DDTHH:MM:SS.ffffff+00:00> with a 48-hour TTL.

These keys serve as the recovery mechanism. They are never updated after creation —
each is an immutable point-in-time record.

### 3. EOD Write (snapshot_scheduler.py — Loop 2)
At exactly 5PM ET, the EodWriter selects the best available data for each asset
using the following priority:

  1. tick:last:<PRODUCT_ID>
        Is the live tick's timestamp <= 5PM ET cutoff?
        YES: use it (most precise — last trade before close)
        NO:  fall back (last trade was after 5PM, e.g. extended hours)

  2. tick:interval:<PRODUCT_ID>:<today>*
        Scan today's interval snapshots, take the latest one before cutoff
        found: use it
        not found: log error, skip product

Each selected tick is wrapped in an EodSnapshot dataclass and written to Snowflake
via an idempotent MERGE statement — safe to re-run without creating duplicates.

---

## Timestamp Handling

All timestamps are stored in Redis and passed to Snowflake in a single canonical format:

  YYYY-MM-DDTHH:MM:SS.ffffff+00:00

- Always UTC
- Always timezone-aware (explicit +00:00 offset)
- Always microsecond precision (6 fractional digits)
- Lexicographically sortable — enables efficient key comparison without parsing

Coinbase sends timestamps with variable fractional precision (5, 9 digits etc.).
These are normalized to 6 digits before parsing using a regex applied in both
ws_listener.py (on ingest) and models.py (on Redis read).

The EOD definition configured in .env file specifies the timezone, which is only used 
in the snapshot scheduler to determine when to run the EOD process. 
All other timestamps are standardized to UTC to avoid confusion.

---

## Concurrency Model

The pipeline runs three concurrent execution contexts:

```
Main thread
  ├── signal handler (SIGTERM/SIGINT) → sets threading.Event → triggers shutdown
  └── blocks on threading.Event.wait()

WSClient thread  (managed by Coinbase SDK)
  └── asyncio loop: receives ticks → calls on_message callback → writes to Redis

Scheduler thread  (daemon)
  └── asyncio loop:
        ├── IntervalSnapshotter  (every 5 min)
        └── EodWriter            (once at 5PM ET)
```

The threading.Event is the single shared shutdown signal. Setting it causes:
- wait_until_stopped() in the main thread to unblock and exit
- _interruptible_sleep() loops in the scheduler to exit cleanly
- The scheduler thread to finish its current work and terminate

---

## Configuration

All configuration is via environment variables in config/.env:

| Variable                  | Description                                                            |
|---------------------------|------------------------------------------------------------------------|
| PRODUCT_IDS               | Comma-separated pairs for price feeds to ingest (e.g. BTC-USD,ETH-USD) |
| CHANNELS                  | Comma-separated pairs for Coinbase channels to subscribe to            |
| COINBASE_API_JSON_FP      | Filepath to the Coinbase API key (in json)                             |
| SNAPSHOT_INTERVAL_MINUTES | Frequency(M) in Interval Snapshotting (default : 5)                    |
| SNAPSHOT_TTL_HOURS        | How long (H) to keep each Snapshot (default : 48)                      |
| EOD_HOUR                  | EOD snapshot hour in ET (default : 17)                                 |
| EOD_MINUTE                | EOD snapshot minute in ET (default : 0)                                |
| EOD_TZ                    | EOD Timezone (default : America/New_York)                              |
| REDIS_HOST                | Redis host name (default : redis)                                      |
| REDIS_PORT                | Redis port (default : 6379)                                            |
| REDIS_PORT                | Redis DB (default : 0)                                                 |
| SNOWFLAKE_ACCOUNT         | Account identifier                                                     |
| SNOWFLAKE_USER            | Snowflake username                                                     |
| SNOWFLAKE_PASSWORD        | Snowflake password                                                     |
| SNOWFLAKE_DATABASE        | Target database                                                        |
| SNOWFLAKE_SCHEMA          | Target schema                                                          |
| SNOWFLAKE_WAREHOUSE       | Warehouse to use for writes                                            |

Adding more price feeds requires only updating PRODUCT_ID in .env — no code changes needed.
---

## Querying EOD Data

```sql
-- Latest EOD prices
SELECT trade_date, product_id, price, tick_ts_utc
FROM eod_price_feeds
ORDER BY trade_date DESC, asset;

-- Price history for a single product_id
SELECT trade_date, price
FROM eod_price_feeds
WHERE product_id = 'BTC-USD'
ORDER BY trade_date DESC;
```

## Extra considerations (Not Implemented)

### 1. Pipeline separation
In actual production scenario, each price feed should run in seperate processes / containers (different k8s pod, for example).
This implementation heavily utilizes asyncio - especially via the Coinbase SDK, which wraps the subscription 
and message under different asyncio event loop. To de-risk the ETL processes from each other, we can duplicate 
the configuration for each price feeds and create seperate containers. 

### 2. Validation (via models.py)
All data crossing a boundary (WebSocket → Redis, Redis → Snowflake) should be
through typed dataclasses:

- Tick: validates asset (non-empty string), price (positive float), volume
(non-negative float), and both timestamps (timezone-aware datetimes). Normalizes
all timestamps to UTC on construction.
- EodSnapshot: constructed from a validated Tick, serializes to typed Snowflake
parameters via to_snowflake_params().

### 3. Kafka as buffer 
As mentioned, streaming the live data through Kafka (via a producer), and separately consuming 
from the topic, partitioned by the product_id, would provide even stronger durability and fault tolerance.
The logs effectively serve as data backup for replayability. It would help decouples the risks of ingestion
and processing failure, however, it introduces maintenance overhead, as the Kafka producer failure would cause 
the pipeline to stop. 
