# file: multi_ws_recorder.py
import asyncio
import json
import time
from collections import deque
import random
import signal
import logging
import aiofiles
import websockets  # pip install websockets
import os
import requests
import shutil
import threading
import telebot
from datetime import datetime

# Configure logging to write to both console and log file with UTC timezone
import logging.handlers
from datetime import datetime, timezone

class UTCFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        if datefmt:
            s = dt.strftime(datefmt)
        else:
            s = dt.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s,%03d" % (s, record.msecs)
        return s

# Set up logging with UTC timezone
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("collector.log"),
        logging.StreamHandler()
    ]
)

# Apply UTC formatter to all handlers
utc_formatter = UTCFormatter("%(asctime)s %(levelname)s %(message)s")
for handler in logging.root.handlers:
    handler.setFormatter(utc_formatter)

# ---------- Config ----------
BINANCE_FUTURES_WS_BASE = "wss://fstream.binance.com"
INBOUND_LIMIT_PER_CONN = 10  # msgs/sec (enforced by Binance)
CONN_ATTEMPT_LIMIT = 300     # attempts per 5 minutes per IP
CONN_ATTEMPT_WINDOW = 300    # seconds (5 minutes)
STARTUP_STAGGER = 2.0        # seconds between starting connections
PING_INTERVAL = 60           # seconds - websockets handles ping/pong
TOP_ORDERBOOK_LEVELS = 20   # Number of top bid/ask levels to save
INCREMENT = 100

# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN = "7698606892:AAF3tdAefxtYV0pNg-rKbWzhEP39HUZX4AA"  # Replace with your bot token
TELEGRAM_CHAT_ID = "757310263"      # Replace with your chat ID
DISK_CHECK_INTERVAL = 300  # Check disk space every 5 minutes
DISK_WARNING_THRESHOLD = 10  # Warning when disk usage is above 90% (10% free)
# ----------------------------


class DiskSpaceMonitor:
    """Monitor disk space and send notifications via Telegram."""
    
    def __init__(self, bot_token, chat_id, machine_number, check_interval=300, warning_threshold=10):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.check_interval = check_interval
        self.warning_threshold = warning_threshold  # Percentage of free space to trigger warning
        self.running = False
        self.thread = None
        self.bot = None
        self.machine_number = machine_number

        if bot_token and bot_token != "YOUR_BOT_TOKEN_HERE":
            try:
                self.bot = telebot.TeleBot(bot_token)
                logging.info("Telegram bot initialized successfully")
            except Exception as e:
                logging.error(f"Failed to initialize Telegram bot: {e}")
                self.bot = None
        else:
            logging.warning("Telegram bot token not configured")
    
    def _format_bytes(self, bytes_value):
        """Convert bytes to human readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.1f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.1f} PB"
    
    def _get_disk_usage(self, path="/"):
        """Get disk usage information for the specified path."""
        try:
            total, used, free = shutil.disk_usage(path)
            free_percentage = (free / total) * 100
            used_percentage = (used / total) * 100
            
            return {
                'total': total,
                'used': used,
                'free': free,
                'free_percentage': free_percentage,
                'used_percentage': used_percentage,
                'path': path
            }
        except Exception as e:
            logging.error(f"Error getting disk usage: {e}")
            return None
    
    def _send_telegram_message(self, message):
        """Send message to Telegram."""
        if not self.bot or self.chat_id == "YOUR_CHAT_ID_HERE":
            logging.warning("Telegram not configured, message not sent")
            return False
        
        try:
            self.bot.send_message(self.chat_id, message, parse_mode='HTML')
            return True
        except Exception as e:
            logging.error(f"Failed to send Telegram message: {e}")
            return False
    
    def _check_disk_space(self):
        """Check disk space and send notification if needed."""
        disk_info = self._get_disk_usage()
        if not disk_info:
            return
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")
        
        # Create status message
        status_emoji = "🟢" if disk_info['free_percentage'] > self.warning_threshold else "🔴"
        
        message = f"""
{status_emoji} <b>Disk Space Monitor (Machine {self.machine_number})</b>
📅 <b>Time:</b> {timestamp}
💾 <b>Path:</b> {disk_info['path']}

📊 <b>Disk Usage:</b>
• Total: {self._format_bytes(disk_info['total'])}
• Used: {self._format_bytes(disk_info['used'])} ({disk_info['used_percentage']:.1f}%)
• Free: {self._format_bytes(disk_info['free'])} ({disk_info['free_percentage']:.1f}%)

⚠️ <b>Warning Threshold:</b> {self.warning_threshold}% free space
        """.strip()
        
        # Send notification if below threshold or every hour for status updates
        should_send = (
            disk_info['free_percentage'] <= self.warning_threshold or
            int(time.time()) % 3600 < self.check_interval  # Send hourly status
        )
        
        if should_send:
            success = self._send_telegram_message(message)
            if success:
                logging.info(f"Disk space notification sent: {disk_info['free_percentage']:.1f}% free")
            else:
                logging.warning("Failed to send disk space notification")
        else:
            logging.debug(f"Disk space check: {disk_info['free_percentage']:.1f}% free (no notification needed)")
    
    def _monitor_loop(self):
        """Main monitoring loop that runs in background thread."""
        logging.info(f"Disk space monitor started (checking every {self.check_interval} seconds)")
        
        while self.running:
            try:
                self._check_disk_space()
            except Exception as e:
                logging.error(f"Error in disk space monitor: {e}")
            
            # Sleep for the check interval
            for _ in range(self.check_interval):
                if not self.running:
                    break
                time.sleep(1)
        
        logging.info("Disk space monitor stopped")
    
    def start(self):
        """Start the disk space monitoring in a background thread."""
        if self.running:
            logging.warning("Disk space monitor is already running")
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.thread.start()
        logging.info("Disk space monitor thread started")
    
    def stop(self):
        """Stop the disk space monitoring."""
        if not self.running:
            return
        
        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)
        logging.info("Disk space monitor stopped")


class ConnAttemptTracker:
    """Track connection attempts in a sliding window to avoid exceeding 300 per 5 minutes."""
    def __init__(self):
        self.attempts = deque()

    def record_attempt(self):
        now = time.time()
        self.attempts.append(now)
        self._evict(now)

    def _evict(self, now):
        while self.attempts and (now - self.attempts[0] > CONN_ATTEMPT_WINDOW):
            self.attempts.popleft()

    def count(self):
        now = time.time()
        self._evict(now)
        return len(self.attempts)

    def can_attempt(self):
        return self.count() < CONN_ATTEMPT_LIMIT


class GlobalRateLimiter:
    """Global rate limiter that can pause all workers when rate limits are hit."""
    def __init__(self):
        self.is_banned = False
        self.ban_until = 0
        self.retry_after = 0
        self.lock = asyncio.Lock()
    
    async def check_rate_limit(self, response):
        """Check response for rate limit errors and update global state."""
        async with self.lock:
            if response.status_code == 429:
                # Rate limit hit - get retry-after header
                self.retry_after = int(response.headers.get('Retry-After', 60))
                self.ban_until = time.time() + self.retry_after
                self.is_banned = True
                logging.warning(f"Rate limit hit! Banned for {self.retry_after} seconds")
                return False
            elif response.status_code == 418:
                # IP banned - longer ban
                self.ban_until = time.time() + 300  # 5 minutes
                self.is_banned = True
                logging.error(f"IP banned! Banned for 5 minutes")
                return False
            else:
                # Success - reset ban if it was set
                if self.is_banned:
                    self.is_banned = False
                    self.ban_until = 0
                    logging.info("Rate limit ban lifted")
                return True
    
    async def wait_if_banned(self):
        """Wait if currently banned."""
        async with self.lock:
            if self.is_banned and time.time() < self.ban_until:
                wait_time = self.ban_until - time.time()
                logging.info(f"Waiting {wait_time:.1f}s due to rate limit ban")
                await asyncio.sleep(wait_time)
                self.is_banned = False
                self.ban_until = 0


class ConnectionWorker:
    """
    One websocket connection worker. Handles one stream (or combined streams if you change build_url).
    Implements Binance Futures official orderbook management process.
    """
    def __init__(self, stream_name, attempt_tracker: ConnAttemptTracker, rate_limiter: GlobalRateLimiter, out_dir="data", downgrade_on_spike=True):
        self.stream = stream_name.lower()
        self.attempt_tracker = attempt_tracker
        self.rate_limiter = rate_limiter
        self.backoff_base = 1.0
        self.backoff_attempts = 0
        self.running = True
        self.inbound_timestamps = deque(maxlen=200)
        self.id = self.stream
        self.out_dir = out_dir
        self.downgrade_on_spike = downgrade_on_spike
        self.current_stream = self.stream
        self._file = None
        # throttling writes to disk
        self.last_write_time = 0
        self.write_interval = 1.0  # 1 second

        # Orderbook state
        self.snapshot = None
        self.last_update_id = None
        self.snapshot_ready = False
        self.local_orderbook = {"bids": {}, "asks": {}}
        self.last_processed_u = None

    def _ws_url(self):
        return f"{BINANCE_FUTURES_WS_BASE}/ws/{self.current_stream}"

    async def _open_output(self):
        filename = f"{self.out_dir}/all_streams_data.txt"
        self._file = await aiofiles.open(filename, mode="a", encoding="utf-8")
        await self._get_depth_snapshot()

    async def _get_depth_snapshot(self):
        """Fetch snapshot from REST and reset local state"""
        try:
            # Check if we're globally banned
            await self.rate_limiter.wait_if_banned()
            
            symbol = self.stream.split('@')[0].upper()
            url = "https://fapi.binance.com/fapi/v1/depth"
            params = {"symbol": symbol, "limit": 1000}

            response = requests.get(url, params=params, timeout=5)
            
            # Check for rate limit errors
            if not await self.rate_limiter.check_rate_limit(response):
                # We're banned, wait and retry
                await self.rate_limiter.wait_if_banned()
                return await self._get_depth_snapshot()  # Retry after ban
            
            if response.status_code == 200:
                self.snapshot = response.json()
                self.last_update_id = self.snapshot.get("lastUpdateId")

                # reset state
                self.last_processed_u = None
                self.snapshot_ready = True

                self._initialize_local_orderbook()

                # Create clean snapshot format with top N levels
                snapshot_bids = self.snapshot.get("bids", [])[:TOP_ORDERBOOK_LEVELS]
                snapshot_asks = self.snapshot.get("asks", [])[:TOP_ORDERBOOK_LEVELS]

                timestamp = self.snapshot.get("T")
            
                orderbook_data = {
                    "symbol": symbol,
                    "timestamp": timestamp,
                    "update_id": None,
                    "prev_update_id": None,
                    "bids": [[str(p), str(q)] for p, q in snapshot_bids],
                    "asks": [[str(p), str(q)] for p, q in snapshot_asks]
                }

                await self._file.write(json.dumps(orderbook_data) + "\n")

                logging.info(f"{self.id}: Snapshot loaded, lastUpdateId={self.last_update_id}")
            else:
                logging.warning(f"{self.id}: Failed to get snapshot: {response.status_code}")
        except Exception as e:
            logging.warning(f"{self.id}: Error getting snapshot: {e}")

    def _initialize_local_orderbook(self):
        self.local_orderbook = {"bids": {}, "asks": {}}

        for price, qty in self.snapshot.get("bids", []):
            q = float(qty)
            if q > 0:
                self.local_orderbook["bids"][float(price)] = q

        for price, qty in self.snapshot.get("asks", []):
            q = float(qty)
            if q > 0:
                self.local_orderbook["asks"][float(price)] = q

        logging.info(f"{self.id}: Local orderbook initialized with {len(self.local_orderbook['bids'])} bids, {len(self.local_orderbook['asks'])} asks")

    async def _process_depth_update(self, message: str):
        try:
            data = json.loads(message)
            if data.get("e") != "depthUpdate":
                return

            U, u, pu = data.get("U"), data.get("u"), data.get("pu")

            if not self.snapshot_ready:
                return

            # discard old
            if u < self.last_update_id:
                return

            # first event after snapshot
            if self.last_processed_u is None:
                if not (U <= self.last_update_id and u >= self.last_update_id):
                    logging.warning(f"{self.id}: First event misaligned U={U}, u={u}, lastUpdateId={self.last_update_id}")
                    await self._get_depth_snapshot()
                    return
            else:
                # check sequence
                if pu != self.last_processed_u:
                    logging.warning(f"{self.id}: Sequence mismatch pu={pu} != lastProcessedU={self.last_processed_u}")
                    await self._get_depth_snapshot()
                    return

            self.last_processed_u = u
            await self._apply_orderbook_update(data)

        except Exception as e:
            logging.error(f"{self.id}: Error processing depth update: {e}")

    async def _apply_orderbook_update(self, data):
        # Update local orderbook
        for price, qty in data.get("b", []):
            p, q = float(price), float(qty)
            if q == 0:
                self.local_orderbook["bids"].pop(p, None)
            else:
                self.local_orderbook["bids"][p] = q

        for price, qty in data.get("a", []):
            p, q = float(price), float(qty)
            if q == 0:
                self.local_orderbook["asks"].pop(p, None)
            else:
                self.local_orderbook["asks"][p] = q

        # Sort and limit to top 100 levels
        sorted_bids = sorted(self.local_orderbook["bids"].items(), reverse=True)
        sorted_asks = sorted(self.local_orderbook["asks"].items())
        
        # Limit to top N levels for each side
        limited_bids = sorted_bids[:TOP_ORDERBOOK_LEVELS]
        limited_asks = sorted_asks[:TOP_ORDERBOOK_LEVELS]

        orderbook_data = {
            "symbol": data.get("s"),
            "timestamp": int(time.time() * 1000),
            "update_id": data.get("u"),
            "prev_update_id": data.get("pu"),
            "bids": [[str(p), str(q)] for p, q in limited_bids],
            "asks": [[str(p), str(q)] for p, q in limited_asks]
        }

        await self._write_msg(json.dumps(orderbook_data))

    async def _write_msg(self, raw: str):
        if self._file:
            now = time.time()
            if now - self.last_write_time >= self.write_interval:
                await self._file.write(raw + "\n")
                self.last_write_time = now

    async def stop(self):
        self.running = False

    async def run_once(self):
        if not self.attempt_tracker.can_attempt():
            logging.warning(f"{self.id}: connection-attempt limit reached; delaying")
            await asyncio.sleep(5 + random.random() * 5)
            return False

        self.attempt_tracker.record_attempt()
        url = self._ws_url()
        logging.info(f"{self.id}: connecting to {url} (attempts={self.attempt_tracker.count()})")

        try:
            async with websockets.connect(url, ping_interval=PING_INTERVAL, ping_timeout=10) as ws:
                await self._open_output()
                self.backoff_attempts = 0
                logging.info(f"{self.id}: connected")

                async for message in ws:
                    self.inbound_timestamps.append(time.time())
                    await self._process_depth_update(message)

                    if not self.running:
                        logging.info(f"{self.id}: stop requested; closing")
                        await ws.close()
                        break

                if self._file:
                    await self._file.flush()
                    await self._file.close()
                    self._file = None

                return True

        except websockets.exceptions.ConnectionClosedError as e:
            logging.warning(f"{self.id}: connection closed error: {e}")
        except Exception as e:
            logging.exception(f"{self.id}: exception during websocket session: {e}")

        if self._file:
            try:
                await self._file.flush()
                await self._file.close()
            except Exception:
                pass
            self._file = None

        return False

    async def run(self):
        while self.running:
            ok = await self.run_once()
            if ok:
                self.backoff_attempts = max(0, self.backoff_attempts - 1)
                await asyncio.sleep(0.5 + random.random() * 0.5)
            else:
                self.backoff_attempts += 1
                wait = min(60, self.backoff_base * (2 ** (self.backoff_attempts - 1)))
                wait *= (0.5 + random.random() * 0.5)
                logging.info(f"{self.id}: backoff {wait:.1f}s")
                await asyncio.sleep(wait)

        logging.info(f"{self.id}: worker stopped")


class MultiConnector:
    def __init__(self, streams, machine_number, out_dir="data", downgrade_on_spike=True, startup_stagger=STARTUP_STAGGER):
        self.streams = [s.lower() for s in streams]
        self.attempt_tracker = ConnAttemptTracker()
        self.rate_limiter = GlobalRateLimiter()
        self.workers = []
        self.tasks = []
        self.out_dir = out_dir
        self.downgrade_on_spike = downgrade_on_spike
        self.startup_stagger = startup_stagger
        self.machine_number = machine_number

        # Initialize disk space monitor
        self.disk_monitor = DiskSpaceMonitor(
            bot_token=TELEGRAM_BOT_TOKEN,
            chat_id=TELEGRAM_CHAT_ID,
            machine_number=self.machine_number,
            check_interval=DISK_CHECK_INTERVAL,
            warning_threshold=DISK_WARNING_THRESHOLD
        )

    async def start(self):
        # Start disk space monitor
        self.disk_monitor.start()
        
        for stream in self.streams:
            w = ConnectionWorker(stream, self.attempt_tracker, self.rate_limiter, out_dir=self.out_dir, downgrade_on_spike=self.downgrade_on_spike)
            self.workers.append(w)

        for w in self.workers:
            t = asyncio.create_task(w.run())
            self.tasks.append(t)
            await asyncio.sleep(self.startup_stagger)

        logging.info(f"Started {len(self.workers)} workers")

    async def stop(self):
        logging.info("Stopping all workers...")
        for w in self.workers:
            await w.stop()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        
        # Stop disk space monitor
        self.disk_monitor.stop()
        
        logging.info("All workers stopped.")


# -----------------------
# Example usage
# -----------------------
async def main():
    with open("offsets.json", "r") as f:
        offsets = json.load(f)
    
    machine_number = 1
    lower_bound = (machine_number - 1) * INCREMENT
    upper_bound = machine_number * INCREMENT

    # for symbol in sorted(offsets.keys()):
    test_streams = [f"{sym}usdt@depth@100ms".lower() for sym in sorted(offsets.keys())]
    test_streams = test_streams[lower_bound:upper_bound]  # single stream for testing

    output_dir = "data"
    os.makedirs(output_dir, exist_ok=True)

    manager = MultiConnector(test_streams, machine_number, out_dir=output_dir)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal(sig, frame):
        logging.info("Received stop signal")
        stop_event.set()

    signal.signal(signal.SIGINT, _signal)
    signal.signal(signal.SIGTERM, _signal)

    await manager.start()
    await stop_event.wait()
    await manager.stop()

if __name__ == "__main__":
    asyncio.run(main())
