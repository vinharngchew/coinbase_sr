"""
Entrypoint. Wires together:
  - Coinbase WSClient listener
  - Updating Redis buffer
  - EOD update scheduler
"""

import asyncio
import logging
import os
import signal
import sys
import threading

from dotenv import load_dotenv

from src.coinbase_ws_listener import CoinbaseWSListener
from src.redis_buffer import RedisTickBuffer
from src.snapshot_scheduler import SnapshotScheduler

# ------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/app/logs/pipeline.log"),
    ])
logger = logging.getLogger(__name__)

load_dotenv()
# ------------------------------------------------------------------
# Config from env
# ------------------------------------------------------------------

def load_config() -> dict:
    required = [
        "PRODUCT_IDS",
        "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA", "SNOWFLAKE_WAREHOUSE",
    ]

    missing = [k for k in required if not os.getenv(k)]
    if missing:
        logger.error(f"Missing required env vars: {missing}")
        sys.exit(1)

    return {
        "product_ids": os.environ["PRODUCT_IDS"].split(","),
        "channels": os.getenv("CHANNELS", "ticker,heartbeats").split(","),
        "coinbase_api_json_fp": os.getenv("COINBASE_API_JSON_FP"),
        
        "snapshot": {
            "snapshot_interval_seconds": int(os.getenv("SNAPSHOT_INTERVAL_MINUTES", 5)) * 60, 
            "snapshot_ttl_seconds": int(os.getenv("SNAPSHOT_TTL_HOURS",48)) * 60 * 60,
            "eod_hour":   int(os.getenv("EOD_HOUR", 17)),
            "eod_minute": int(os.getenv("EOD_MINUTE", 0)),
            "eod_tz": os.getenv("EOD_TZ", "America/New_York"),
            "eod_price_stale_threshold_seconds":  int(os.getenv("EOD_PRICE_STALE_THRESHOLD_MINUTES", 10)) * 60,
        },

        
        "redis": {
            "host": os.getenv("REDIS_HOST", "localhost"),
            "port": int(os.getenv("REDIS_PORT", 6379)),
            "db": int(os.getenv("REDIS_DB", 0))
        },

        "snowflake": {
            "account": os.environ["SNOWFLAKE_ACCOUNT"],
            "user": os.environ["SNOWFLAKE_USER"],
            "password": os.environ["SNOWFLAKE_PASSWORD"],
            "database": os.environ["SNOWFLAKE_DATABASE"],
            "schema": os.environ["SNOWFLAKE_SCHEMA"],
            "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
        },
    }

def main():
    cfg = load_config()
    logger.info("Starting pipeline for ProductIDs %s", cfg["product_ids"])
    
    # Shared stop event across both the listener and the scheduler
    stop_event = threading.Event()

    # Redis buffer
    redis_buffer = RedisTickBuffer(
        host=cfg["redis"]["host"],
        port=cfg["redis"]["port"],
        db=cfg["redis"]["db"],
        snapshot_ttl_seconds=cfg["snapshot"]["snapshot_ttl_seconds"]
    )

    # WebSocket listener
    listener = CoinbaseWSListener(
        product_ids=cfg["product_ids"],
        channels=cfg["channels"],
        redis_buffer=redis_buffer,
        api_key_location=cfg["coinbase_api_json_fp"]
    )

    # SIGTERM / SIGINT handler — called on Docker stop, k8s eviction, etc.
    def _handle_signal(signum, frame):
        logger.info(f"Signal {signum} received — shutting down gracefully…")
        listener.stop()      # unsubscribe, close WSClient, set stop_event
        stop_event.set()     # also unblocks the scheduler thread
    
    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    # Run the async snapshot scheduler in a background thread so it doesn't
    # block the main thread (which needs to stay alive for signal handling)
    def _run_scheduler():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        scheduler = SnapshotScheduler(
            product_ids=cfg["product_ids"],
            redis_buffer=redis_buffer,
            snowflake_cfg=cfg["snowflake"],
            stop_event=stop_event,
            snapshot_cfg=cfg["snapshot"],
        )
        loop.run_until_complete(scheduler.run())
        loop.close()

    scheduler_thread = threading.Thread(target=_run_scheduler, daemon=True, name="scheduler")
    scheduler_thread.start()

    listener.start()

    logger.info("Pipeline running. Waiting for shutdown signal…")
    listener.wait_until_stopped()

    # Wait for the scheduler to finish its current work cleanly
    scheduler_thread.join(timeout=10)
    logger.info("Pipeline shut down cleanly.")

if __name__ == "__main__":
    main()

