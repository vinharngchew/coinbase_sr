"""
Two concurrent loops running in the same asyncio event loop:

  1. IntervalSnapshotter  — fires every interval, freezes last-seen tick
                            to a timestamped Redis key throughout the day.

  2. EodWriter            — fires once at EOD, queries Redis for the
                            best interval snapshot at or before 17:00:00,
                            and writes it to Snowflake via idempotent MERGE.

Separating these concerns means a failure in the EOD write never affects
the intraday snapshotting, and a process restart mid-day still has a full
history of interval snapshots to draw from.
"""

import asyncio
import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
import sys
from typing import List
from zoneinfo import ZoneInfo, available_timezones

import snowflake.connector

from .redis_buffer import RedisTickBuffer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/app/logs/pipeline.log"),
    ])
logger = logging.getLogger(__name__)

@dataclass 
class SnapShotConfig:
    """
    Configuration for snapshotting
    """
    snapshot_interval_seconds: int
    snapshot_ttl_seconds: int
    eod_hour: int
    eod_minute: int
    eod_tz: ZoneInfo
    eod_price_stale_threshold_seconds: int

    @classmethod
    def from_dict(cls, data: dict):
        required = ["snapshot_interval_seconds",
                    "snapshot_ttl_seconds",
                    "eod_hour",
                    "eod_minute",
                    "eod_tz",
                    "eod_price_stale_threshold_seconds"]

        assert all (key in data for key in required), f"Snapshot config must have all the keys set for {required}" 
        
        try:
            data["eod_tz"] = ZoneInfo(data["eod_tz"])
        except KeyError as e:
            timezone_value_error = f"{e}: eoz_tz must be one of the following {available_timezones()}"
            logger.error(timezone_value_error)
            raise e

        return cls(**data)

class SnapshotScheduler:
    def __init__(
        self,
        product_ids: List[str],
        redis_buffer: RedisTickBuffer,
        snowflake_cfg: dict,
        stop_event: threading.Event,
        snapshot_cfg: dict

    ):
        self.product_ids = product_ids
        self.buffer = redis_buffer
        self.snowflake_cfg = snowflake_cfg
        self.snapshot_cfg = SnapShotConfig.from_dict(snapshot_cfg)
        self.stop_event = stop_event

    # ------------------------------------------------------------------
    # Public entry point — runs both loops concurrently
    # ------------------------------------------------------------------

    async def run(self):
        await asyncio.gather(
            self._run_interval_snapshotter(),
            self._run_eod_writer(),
        )

    # ------------------------------------------------------------------
    # Loop 1: fires every interval
    # ------------------------------------------------------------------

    async def _run_interval_snapshotter(self):
        """Freeze a snapshot of the last-seen tick every interval"""
        while not self.stop_event.is_set():
            wait_secs = self._seconds_until_next_interval()
            logger.info(f"Next interval snapshot in {wait_secs:.1f}s")

            await self._interruptible_sleep(wait_secs)
            if self.stop_event.is_set():
                break

            now_utc = datetime.now(tz=ZoneInfo("UTC"))
            for product_id in self.product_ids:
                self.buffer.freeze_snapshot(product_id, ts_utc=now_utc)

        logger.info("Interval snapshotter stopped.")

    # ------------------------------------------------------------------
    # Loop 2: fires once at EOD
    # ------------------------------------------------------------------

    async def _run_eod_writer(self):
        """At EOD, find the best interval snapshot and write to Snowflake."""
        while not self.stop_event.is_set():
            wait_secs = self._seconds_until_eod()
            logger.info(f"Next EOD write in {wait_secs:.1f}s ({wait_secs/3600:.2f}h)")

            await self._interruptible_sleep(wait_secs)
            if self.stop_event.is_set():
                break

            await self._write_eod()

            # Sleep 61s before looping to avoid double-firing
            await asyncio.sleep(61)

        logger.info("EOD writer stopped.")

    # ------------------------------------------------------------------
    # EOD write logic
    # ------------------------------------------------------------------

    async def _write_eod(self):
        now_eod_tz = datetime.now(tz=self.snapshot_cfg.eod_tz)
        
        trade_date = now_eod_tz.date()
        
        cutoff     = now_eod_tz.replace(
            hour=self.snapshot_cfg.eod_hour, minute=self.snapshot_cfg.eod_minute,
            second=0, microsecond=0
        )

        logger.info(f"Running EOD write for {trade_date}, cutoff={cutoff.isoformat()}")

        rows = []
        for product_id in self.product_ids:
            snapshot = self.buffer.get_best_eod_snapshot(product_id, cutoff=cutoff.astimezone(ZoneInfo("UTC")))
            
            if snapshot is None:
                logger.error(f"[{product_id}] No tick price found — skipping EOD write.")
                continue

            # Staleness check — how far is the best snapshot from EOD?
            tick_eod_tz = datetime.fromisoformat(snapshot["ts_utc"]).astimezone(self.snapshot_cfg.eod_tz)
            delta_seconds = (cutoff - tick_eod_tz).total_seconds() 
            is_stale = delta_seconds > self.snapshot_cfg.eod_price_stale_threshold_seconds

            if is_stale:
                logger.warning(
                    f"[{product_id}] Best snapshot is {delta_seconds} seconds before cutoff — marking stale."
                )

            rows.append({
                "trade_date": trade_date.isoformat(),
                "product_id":  product_id,
                "price": float(snapshot["price"]),
                "tick_ts_utc":  snapshot["ts_utc"],
                "is_stale":  is_stale,
                "source": "coinbase_websocket",
            })

        if rows:
            await asyncio.get_event_loop().run_in_executor(
                None, self._write_to_snowflake, rows
            )

    # ------------------------------------------------------------------
    # Snowflake write
    # ------------------------------------------------------------------

    def _write_to_snowflake(self, rows: List[dict]):
        logger.info(f"Writing {len(rows)} rows to Snowflake…")
        try:
            conn = snowflake.connector.connect(**self.snowflake_cfg)
            cur  = conn.cursor()

            cur.execute("""
                CREATE TABLE IF NOT EXISTS eod_price_feeds (
                    trade_date        DATE,
                    product_id        VARCHAR(20),
                    price             FLOAT,
                    tick_ts_utc       TIMESTAMP_TZ,
                    is_stale          BOOLEAN,
                    source            VARCHAR(50),
                    updated_at        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (trade_date, product_id)
                )
            """)

            merge_sql = """
                MERGE INTO eod_price_feeds AS target
                USING (
                    SELECT
                        %(trade_date)s::DATE             AS trade_date,
                        %(product_id)s                   AS product_id,
                        %(price)s::FLOAT                 AS price,
                        %(tick_ts_utc)s::TIMESTAMP_TZ    AS tick_ts_utc,
                        %(is_stale)s::BOOLEAN            AS is_stale,
                        %(source)s                       AS source
                ) AS src
                ON target.trade_date = src.trade_date AND target.product_id = src.product_id
                WHEN MATCHED THEN UPDATE SET
                    price          = src.price,
                    tick_ts_utc    = src.tick_ts_utc,
                    is_stale       = src.is_stale,
                    source         = src.source,
                    updated_at     = CURRENT_TIMESTAMP
                WHEN NOT MATCHED THEN INSERT (
                    trade_date, product_id, price,
                    tick_ts_utc, is_stale, source
                ) VALUES (
                    src.trade_date, src.product_id, src.price,
                    src.tick_ts_utc, src.is_stale, src.source
                )
            """

            for row in rows:
                cur.execute(merge_sql, row)
                logger.info(f"Merged: {row['product_id']} @ {row['price']} for {row['trade_date']}")

            conn.commit()
            logger.info("Snowflake write complete.")

        except Exception as e:
            logger.error(f"Snowflake write failed: {e}")
            raise
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _seconds_until_next_interval(self) -> float:
        """Seconds until the next 5-minute boundary (e.g. :00, :05, :10…)"""
        now = datetime.now(tz=self.snapshot_cfg.eod_tz)
        elapsed = now.minute * 60 + now.second
        elapsed_in_interval = elapsed % self.snapshot_cfg.snapshot_interval_seconds
        return self.snapshot_cfg.snapshot_interval_seconds - elapsed_in_interval

    def _seconds_until_eod(self) -> float:
        """Seconds until next EOD."""
        now_eod_tz = datetime.now(tz=self.snapshot_cfg.eod_tz)
        target = now_eod_tz.replace(
            hour=self.snapshot_cfg.eod_hour, minute=self.snapshot_cfg.eod_minute,
            second=0, microsecond=0
        )
        if now_eod_tz >= target:
            target += timedelta(days=1)
        return (target - now_eod_tz).total_seconds()

    async def _interruptible_sleep(self, total_seconds: float):
        """Sleep in 1s chunks so stop_event is checked regularly."""
        for _ in range(int(total_seconds)):
            if self.stop_event.is_set():
                return
            await asyncio.sleep(1)