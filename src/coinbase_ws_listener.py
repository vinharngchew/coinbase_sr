"""
Connects to the Coinbase Advanced Trade WebSocket API and streams real-time ticker data into the Redis buffer.
Using the coinbase SDK, which already provides boilerplate handling on asyncio execution, authentication, auto-reconnect with exponential backoff, graceful shutdown
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import json
import logging
import re
import sys
import threading
from typing import List

from coinbase.websocket import (WSClient,
                                WSClientConnectionClosedException,
                                WSClientException
                                )

from src.redis_buffer import RedisTickBuffer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("/app/logs/pipeline.log"),
    ])
logger = logging.getLogger(__name__)

class CoinbaseWSListener:

    def __init__(self, 
                 product_ids: List[str], 
                 channels: List[str], 
                 redis_buffer: RedisTickBuffer,
                 api_key_location: str = None):
        """
        Instantiate class with standardized config
        """
        self.product_ids = product_ids
        self.channels = channels
        self.redis_buffer = redis_buffer
        self.api_key_location = api_key_location

        # threading.Event — set this to trigger a clean shutdown
        self._stop_event = threading.Event()

        self.ws_client = WSClient(key_file=self.api_key_location, 
                                  on_message=self.on_message, 
                                  on_open=self.on_open,
                                  on_close=self.on_close,
                                  verbose=True)

    @property
    def next_closing_datetime(self):
        """
        Define the next closing datetime
        """
        eod_tz = self.eod_definition.get("timezone")
        eod_tz = ZoneInfo(eod_tz)
        eod_hour = self.eod_definition.get("closing_hour")

        current_datetime = datetime.now(tz=eod_tz)
        closing_datetime = datetime(current_datetime.year, current_datetime.month, current_datetime.day, hour=eod_hour, tzinfo=eod_tz)
                 
        # Past closing hour of the day, the next closing time is tomorrow
        if current_datetime > closing_datetime:
            closing_datetime = closing_datetime + timedelta(days=1)
        
        closing_datetime = closing_datetime.replace(tzinfo=eod_tz)
        return closing_datetime
    
    def start(self):
        """
        Open the connection and subscribe to the channels
        """
        try:
            self.ws_client.open()
            self.ws_client.subscribe(product_ids=self.product_ids, channels=self.channels)
        except WSClientConnectionClosedException as e:
            logger.error("Connection closed : %s. Retry attempts exhausted.", e)
        except WSClientException as e:
            print("Error encountered : %s", e)
    
    def stop(self):
        """
        Signal shutdown and close the WebSocket cleanly.
        Called by the SIGTERM/SIGINT handler in main.py.
        """
        logger.info("Stopping WebSocket listener…")
        self._stop_event.set()
        try:
            self.ws_client.unsubscribe(product_ids=self.product_ids, channels=self.channels)
            self.ws_client.close()
        except Exception as e:
            logger.warning(f"Error during WSClient close: {e}")
    
    def wait_until_stopped(self):
        """
        Block the calling thread until stop() is called.
        Use this in main.py after start() to keep the process alive.
        """
        self._stop_event.wait()

    def on_message(self, msg):
        """
        Callback function to handle websocket messages
        """

        if self._stop_event.is_set():
            return
        
        try:
            parsed_message = json.loads(msg)
        except json.JSONDecodeError:
            return

        if parsed_message.get("type") and parsed_message.get("type") == "error":
            logger.error("Error : %s", parsed_message.get("message"))
        
        else: 
            message_channel = parsed_message.get("channel")
            logger.debug("Receive message %s from %s channel", parsed_message, message_channel)
            
            # Map message from channel to specific callback functions
            channel_specific_callbacks = {
                "heartbeats": self._parse_heartbeats,
                "ticker": self._parse_ticker,
                "subscriptions": self._parse_subscriptions,
            }

            try:
                channel_specific_callbacks[message_channel](parsed_message)
            except KeyError:
                logger.warning("Message with channel type : %s has no defined callback function", message_channel)    

    def _parse_heartbeats(self, parsed_message):
        """
        Process a heartbeat channel event
        """
        logger.info("Received %s", parsed_message)
    
    def _parse_ticker(self, parsed_message):
        """
        Process a ticket channel event
        """
        
        ts_utc_str = parsed_message.get("timestamp")
        
        # Normalize fractional seconds to 6 digits — Coinbase sends variable
        # precision (e.g. 5 or 9 digits) which fromisoformat() rejects
        ts_utc_str = re.sub(
            r'(\.\d+)',
            lambda m: m.group(1).ljust(7, '0')[:7],
            ts_utc_str.replace("Z", "+00:00")
        )
        ts_utc = datetime.fromisoformat(ts_utc_str)

        for event in parsed_message.get("events", []):
            for ticker in event.get("tickers", []):
                product_id = ticker.get("product_id")
                price = ticker.get("price")
                
                if not all([product_id, price, ts_utc_str]):
                    return

                self.redis_buffer.update_tick(
                    product_id=product_id,
                    price=float(price),
                    ts_utc=ts_utc
                )

    def _parse_subscriptions(self, parsed_message):
        """
        Confirming subscription
        """
        logger.info("Subscriptions confirmed: %s", parsed_message)

    def on_close(self):
        """
        Callback function when websocket connection is closed
        """
        logger.info("Connection closed.")
    
    def on_open(self):
        """
        Callback function when the connection is successfully opened.
        """
        logger.info("Connection opened.")