"""
Wrapper around Redis for storing and retrieving last-seen tick data.
Key schema:
    tick:last:<PRODUCT_ID>       → Hash  { price, ts_utc}
    tick:snapshot:<DATE>:<PRODUCT_ID> → Hash (frozen at EOD closing, retained for replayability)
"""

import logging
from datetime import datetime
from typing import Optional, List
import sys

import redis

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/app/logs/pipeline.log"),
    ])
logger = logging.getLogger(__name__)

# Canonical format for all timestamps written to Redis
TS_FORMAT = "%Y-%m-%dT%H:%M:%S.%f+00:00"

class RedisTickBuffer:
    def __init__(self, 
                 host: str = "localhost", 
                 port: int = 6379, 
                 db: int = 0,
                 snapshot_ttl_seconds: int = 48*60*60):
        
        self.redis_client = redis.Redis(
            host=host,
            port=port,
            db=db,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
            retry_on_timeout=True,
        )

        self.snapshot_ttl_seconds = snapshot_ttl_seconds
        self._ping()

    def _ping(self):
        try:
            self.redis_client.ping()
            logger.info("Redis connection established.")
        except redis.ConnectionError as e:
            logger.error("Cannot connect to Redis: %s", e)
            raise

    def update_tick(self, 
                    product_id: str, 
                    price: float, 
                    ts_utc: datetime):
        
        """
        Overwrite the last-seen tick for a product_id. Called on every WebSocket message.
        """
        key = f"tick:last:{product_id}"
        self.redis_client.hset(key, mapping={
            "price":   str(price),
            "ts_utc":  ts_utc.strftime(TS_FORMAT)
        })
        logger.debug(f"Updated tick → {key}: price={price}, ts_utc={ts_utc}")

    def freeze_snapshot(self, product_id: str, ts_utc: datetime) -> Optional[dict]:
        """
        Copy the current last-seen tick into a timestamped interval snapshot key.
        Called every interval. Returns the snapshot dict or None if buffer is empty.

        Key format: tick:interval:<PRODUCT_ID>:<YYYY-MM-DDTHH:MM:SS>
        Lexicographic sort on the timestamp suffix is used at EOD to find
        the latest snapshot at or before EOD.
        """
        src_key  = f"tick:last:{product_id}"

        # Truncate to seconds so keys are clean and sortable
        ts_utc_str = ts_utc.strftime(TS_FORMAT)
        snap_key = f"tick:interval:{product_id}:{ts_utc_str}"

        tick = self.redis_client.hgetall(src_key)
        if not tick:
            logger.warning(f"[{product_id}] No tick in buffer at interval {ts_utc_str} — skipping.")
            return None

        self.redis_client.hset(snap_key, mapping=tick)
        self.redis_client.expire(snap_key, self.snapshot_ttl_seconds)

        logger.info(f"Interval snapshot frozen → {snap_key}")
        return dict(tick)
    
    def get_last_tick(self, product_id: str) -> Optional[dict]:
        """
        Read last seen price from redis buffer
        """
        key = f"tick:last:{product_id}"
        data = self.redis_client.hgetall(key)
        return data if data else None
    
    def get_best_eod_snapshot(self, product_id: str, cutoff: datetime) -> Optional[dict]:
        """
        Return the most precise tick at or before the cutoff time.

        Strategy:
          1. Check tick:last:<product_id> first — this is the freshest possible data.
             Use it if its timestamp is at or before the cutoff.
          2. Fall back to interval snapshots if the live tick is missing or
             its timestamp is after the cutoff.
        
        Returns the snapshot dict, or None if no snapshots exist before cutoff.
        """
        cutoff_str = cutoff.strftime(TS_FORMAT)
        date_prefix  = cutoff.strftime("%Y-%m-%d")

        # Step 1 — try the live tick first
        live_tick = self.get_last_tick(product_id)
        if live_tick is not None:
            live_ts_str = datetime.fromisoformat(live_tick["ts_utc"]).strftime(TS_FORMAT)
            if live_ts_str <= cutoff_str:
                logger.info(f"[{product_id}] Using live tick for EOD: ts={live_ts_str}")
                return live_tick
            else:
                logger.info(
                    f"[{product_id}] Live tick is after cutoff ({live_ts_str} > {cutoff_str}) "
                    f"— falling back to interval snapshots."
                )

        logger.info(f"[{product_id}] Using last interval snapshot for EOD...")

        # Step 2 — fall back to interval snapshots
        pattern = f"tick:interval:{product_id}:{date_prefix}*"

        # Collect all interval keys for this product_id
        all_keys: List[str] = self.redis_client.keys(pattern)
        if not all_keys:
            logger.warning(f"[{product_id}] No interval snapshots found.")
            return None

        # Keys are lexicographically sortable by their timestamp suffix
        eligible = [k for k in all_keys if k.split(":")[-1] <= cutoff_str]
        if not eligible:
            logger.warning(f"[{product_id}] No interval snapshots at or before {cutoff_str}.")
            return None

        best_key = max(eligible)
        data = self.redis_client.hgetall(best_key)

        logger.info(f"[{product_id}] Best EOD snapshot: {best_key}")
        return dict(data) if data else None
    
    def ping(self) -> bool:
        """
        Health check
        """
        try:
            return self.redis_client.ping()
        except redis.RedisError:
            return False
