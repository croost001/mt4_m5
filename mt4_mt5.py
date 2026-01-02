import time
import csv
import os
import json
import argparse
import MetaTrader5 as mt5
from datetime import datetime
import requests
import threading
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
)
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")  # set this env var
TELEGRAM_CHAT_ID = None  # Will be set when bot receives first message


# Enable logging
# Custom formatter with high precision timestamps (nanoseconds)
class HighPrecisionFormatter(logging.Formatter):
    """Custom formatter that includes fractional seconds (nanoseconds) in timestamps."""

    def formatTime(self, record, datefmt=None):
        """Format the record's time with nanosecond precision."""
        # record.created is seconds since epoch as a float
        # Convert to datetime and extract fractional seconds
        dt = datetime.fromtimestamp(record.created)
        # Format base datetime
        formatted = dt.strftime("%Y-%m-%d %H:%M:%S")
        # Extract fractional seconds (nanoseconds)
        # record.created has microsecond precision, but we'll show nanoseconds
        fractional_seconds = record.created - int(record.created)
        nanoseconds = int(fractional_seconds * 1_000_000_000)
        # Add nanoseconds (9 digits) after the seconds
        return f"{formatted}.{nanoseconds:09d}"


# Log format for both console and file
log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Configure console logging
console_handler = logging.StreamHandler()
console_handler.setFormatter(HighPrecisionFormatter(log_format))
logging.basicConfig(
    level=logging.INFO,
    handlers=[console_handler],  # Console output
)

# Configure file logging with daily rotation
# Log file will be in the same directory as the script
script_dir = Path(__file__).parent
log_file_path = script_dir / "mt4_to_mt5_copier.log"

# Timed rotating file handler: rotate daily at midnight, keep 30 days of logs
file_handler = TimedRotatingFileHandler(
    log_file_path,
    when="midnight",  # Rotate at midnight
    interval=1,  # Every day
    backupCount=30,  # Keep 30 days of logs
    encoding="utf-8",
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(HighPrecisionFormatter(log_format))
file_handler.suffix = (
    "%Y-%m-%d"  # Append date to rotated files: mt4_to_mt5_copier.log.2025-12-29
)

# Add file handler to root logger
logging.getLogger().addHandler(file_handler)

# Reduce verbosity for httpx (HTTP library)
logging.getLogger("httpx").setLevel(logging.WARNING)

# Create logger for this module
logger = logging.getLogger(__name__)

# Telegram bot application instance
telegram_app = None
telegram_running = True  # Start as True, will be set to False when /stop is called

MT4_ABSENCE_GRACE_SEC = 5  # seconds to wait before closing a trade missing from MT4
MISSING_SINCE = {}  # src_ticket -> datetime when first seen missing
# Track secondary limit orders by (symbol, price, order_type) to detect existing orders even if comment is wrong
SECONDARY_ORDERS_BY_KEY = {}  # (symbol, price, order_type) -> mt5_order_ticket
# Track recently opened positions to prevent duplicates (src_ticket -> datetime when last opened)
RECENTLY_OPENED = {}  # src_ticket -> datetime when position was opened
RECENTLY_OPENED_COOLDOWN_SEC = (
    10  # Don't reopen same source ticket within this many seconds
)


# --- CONFIG ---
# Helper function to parse environment variables with type conversion
def get_env(key, default=None, var_type=str):
    """Get environment variable with type conversion."""
    value = os.getenv(key, default)
    if value is None:
        return None
    if var_type == bool:
        return str(value).lower() in ("true", "1", "yes", "on")
    elif var_type == int:
        return int(value)
    elif var_type == float:
        return float(value)
    elif var_type == dict:
        if value == "" or value == "{}":
            return {}
        return json.loads(value)
    elif var_type == list:
        if value == "" or value == "[]":
            return []
        return json.loads(value)
    return var_type(value)


# File paths and MT5 connection settings
MT4_FILE_PATH = get_env(
    "MT4_FILE_PATH",
    r"C:\Users\Administrator\AppData\Roaming\MetaQuotes\Terminal\Common\Files\mt4_trades.csv",
)
MT5_LOGIN = get_env("MT5_LOGIN", 511055954, int)
MT5_PASSWORD = get_env("MT5_PASSWORD", "")
MT5_SERVER = get_env("MT5_SERVER", "FTMO-Server")
POLL_INTERVAL_SEC = get_env("POLL_INTERVAL_SEC", 1, int)
USE_FILE_WATCHER = get_env("USE_FILE_WATCHER", True, bool)

# Internal state variables (not in .env)
LAST_MT4_TRADES = {}
LAST_MT4_FILE_MTIME = None  # Track file modification time to avoid unnecessary reads
SYMBOL_CACHE = {}  # Cache symbol info to avoid repeated MT5 API calls
_last_error_print_time = 0  # Track when we last printed an error to avoid spam

# File watcher event for signaling file changes
file_changed_event = threading.Event()
file_observer = None

# Symbol mapping and tolerance settings
# Optional: map MT4 symbols to MT5 symbols if they differ (e.g. "GER40" -> "DE40.cash")
# Format: JSON object, e.g. {"GER40": "DE40.cash"}
SYMBOL_MAP = get_env("SYMBOL_MAP", "{}", dict)

# Price tolerance in points - prevents new entries if current price is too close to SL/TP
# Format: JSON object, e.g. {"GER40": 5.0}
PRICE_TOLERANCE = get_env("PRICE_TOLERANCE", "{}", dict)

# Entry price tolerance as absolute price difference
# Format: JSON object, e.g. {"GER40.cash": 5.0, "EURUSD": 0.0005}
ENTRY_PRICE_TOLERANCE = get_env(
    "ENTRY_PRICE_TOLERANCE",
    '{"GER40.cash": 5.0, "EURUSD": 0.0005, "XAUUSD": 3.0, "USJPY": 0.05, "BTCUSD": 200.0, "JP225.cash": 20.0}',
    dict,
)

# Multiplier for entry price tolerance on the "better price" side
# Format: JSON object, e.g. {"GER40.cash": 2.0}
ENTRY_PRICE_TOLERANCE_BETTER_PRICE_MULTIPLIER = get_env(
    "ENTRY_PRICE_TOLERANCE_BETTER_PRICE_MULTIPLIER", "{}", dict
)
ENTRY_PRICE_TOLERANCE_BETTER_PRICE_MULTIPLIER_DEFAULT = get_env(
    "ENTRY_PRICE_TOLERANCE_BETTER_PRICE_MULTIPLIER_DEFAULT", 2.0, float
)

# Limit order settings
USE_LIMIT_ORDERS_ON_TOLERANCE_FAIL = get_env(
    "USE_LIMIT_ORDERS_ON_TOLERANCE_FAIL", False, bool
)
USE_SECONDARY_LIMIT_ORDER = get_env("USE_SECONDARY_LIMIT_ORDER", False, bool)
SECONDARY_LIMIT_ORDER_LOT_PERCENTAGE = get_env(
    "SECONDARY_LIMIT_ORDER_LOT_PERCENTAGE", 0.5, float
)
SECONDARY_LIMIT_ORDER_DISTANCE_PERCENTAGE = get_env(
    "SECONDARY_LIMIT_ORDER_DISTANCE_PERCENTAGE", 0.5, float
)

# Lot multiplier settings
GLOBAL_LOT_MULTIPLIER = get_env("GLOBAL_LOT_MULTIPLIER", 1.0, float)
# Format: JSON object, e.g. {"GER40": 2.0}
LOT_MULTIPLIER = get_env("LOT_MULTIPLIER", "{}", dict)
# Format: JSON object with string keys, e.g. {"123456": 1.5, "789012": 0.5}
# Note: JSON keys must be strings, we'll convert to int when using
MAGIC_LOT_MULTIPLIER_JSON = get_env("MAGIC_LOT_MULTIPLIER", "{}", dict)
MAGIC_LOT_MULTIPLIER = (
    {int(k): v for k, v in MAGIC_LOT_MULTIPLIER_JSON.items()}
    if MAGIC_LOT_MULTIPLIER_JSON
    else {}
)

# Magic number filter - only copy trades with these magic numbers
# Format: JSON array, e.g. [123456, 789012]
MAGIC_FILTER = get_env("MAGIC_FILTER", "[]", list)

# Telegram notification settings
TELEGRAM_ENABLED = get_env("TELEGRAM_ENABLED", True, bool)
# --- Helper functions ---


def load_config_file(path):
    """Load JSON config file and return as dict."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def apply_config(config):
    """Override module-level config from a dict.

    This lets you run multiple copies of the script with different configs.
    Only known keys are applied; others are ignored.
    """
    global MT4_FILE_PATH, MT5_LOGIN, MT5_PASSWORD, MT5_SERVER, POLL_INTERVAL_SEC
    global SYMBOL_MAP, PRICE_TOLERANCE, ENTRY_PRICE_TOLERANCE, ENTRY_PRICE_TOLERANCE_BETTER_PRICE_MULTIPLIER, ENTRY_PRICE_TOLERANCE_BETTER_PRICE_MULTIPLIER_DEFAULT
    global GLOBAL_LOT_MULTIPLIER, LOT_MULTIPLIER, MAGIC_LOT_MULTIPLIER, MAGIC_FILTER
    global TELEGRAM_ENABLED, USE_FILE_WATCHER

    if "MT4_FILE_PATH" in config:
        MT4_FILE_PATH = config["MT4_FILE_PATH"]
    if "MT5_LOGIN" in config:
        MT5_LOGIN = int(config["MT5_LOGIN"])
    if "MT5_PASSWORD" in config:
        MT5_PASSWORD = config["MT5_PASSWORD"]
    if "MT5_SERVER" in config:
        MT5_SERVER = config["MT5_SERVER"]
    if "POLL_INTERVAL_SEC" in config:
        POLL_INTERVAL_SEC = int(config["POLL_INTERVAL_SEC"])

    if "SYMBOL_MAP" in config:
        SYMBOL_MAP = dict(config["SYMBOL_MAP"])
    if "PRICE_TOLERANCE" in config:
        PRICE_TOLERANCE = dict(config["PRICE_TOLERANCE"])
    if "ENTRY_PRICE_TOLERANCE" in config:
        ENTRY_PRICE_TOLERANCE = dict(config["ENTRY_PRICE_TOLERANCE"])
    if "ENTRY_PRICE_TOLERANCE_BETTER_PRICE_MULTIPLIER" in config:
        ENTRY_PRICE_TOLERANCE_BETTER_PRICE_MULTIPLIER = dict(
            config["ENTRY_PRICE_TOLERANCE_BETTER_PRICE_MULTIPLIER"]
        )
    if "ENTRY_PRICE_TOLERANCE_BETTER_PRICE_MULTIPLIER_DEFAULT" in config:
        ENTRY_PRICE_TOLERANCE_BETTER_PRICE_MULTIPLIER_DEFAULT = float(
            config["ENTRY_PRICE_TOLERANCE_BETTER_PRICE_MULTIPLIER_DEFAULT"]
        )
    if "USE_LIMIT_ORDERS_ON_TOLERANCE_FAIL" in config:
        USE_LIMIT_ORDERS_ON_TOLERANCE_FAIL = bool(
            config["USE_LIMIT_ORDERS_ON_TOLERANCE_FAIL"]
        )
    if "USE_SECONDARY_LIMIT_ORDER" in config:
        USE_SECONDARY_LIMIT_ORDER = bool(config["USE_SECONDARY_LIMIT_ORDER"])
    if "SECONDARY_LIMIT_ORDER_LOT_PERCENTAGE" in config:
        SECONDARY_LIMIT_ORDER_LOT_PERCENTAGE = float(
            config["SECONDARY_LIMIT_ORDER_LOT_PERCENTAGE"]
        )
    if "SECONDARY_LIMIT_ORDER_DISTANCE_PERCENTAGE" in config:
        SECONDARY_LIMIT_ORDER_DISTANCE_PERCENTAGE = float(
            config["SECONDARY_LIMIT_ORDER_DISTANCE_PERCENTAGE"]
        )
    if "GLOBAL_LOT_MULTIPLIER" in config:
        GLOBAL_LOT_MULTIPLIER = float(config["GLOBAL_LOT_MULTIPLIER"])
    if "LOT_MULTIPLIER" in config:
        LOT_MULTIPLIER = dict(config["LOT_MULTIPLIER"])
    if "MAGIC_LOT_MULTIPLIER" in config:
        # Convert string keys to int keys for magic number multipliers
        magic_mult = config["MAGIC_LOT_MULTIPLIER"]
        if isinstance(magic_mult, dict):
            MAGIC_LOT_MULTIPLIER = {int(k): v for k, v in magic_mult.items()}
        else:
            MAGIC_LOT_MULTIPLIER = {}
    if "MAGIC_FILTER" in config:
        # Expect list of ints
        MAGIC_FILTER[:] = [int(m) for m in config["MAGIC_FILTER"]]
    if "TELEGRAM_ENABLED" in config:
        TELEGRAM_ENABLED = bool(config["TELEGRAM_ENABLED"])
    if "USE_FILE_WATCHER" in config:
        USE_FILE_WATCHER = bool(config["USE_FILE_WATCHER"])

    # Print effective configuration after applying overrides
    logger.info("Effective configuration:")
    logger.info(f"  MT4_FILE_PATH         = {MT4_FILE_PATH}")
    logger.info(f"  MT5_LOGIN             = {MT5_LOGIN}")
    logger.info(f"  MT5_SERVER            = {MT5_SERVER}")
    logger.info(f"  POLL_INTERVAL_SEC     = {POLL_INTERVAL_SEC}")
    logger.info(f"  GLOBAL_LOT_MULTIPLIER = {GLOBAL_LOT_MULTIPLIER}")
    if SYMBOL_MAP:
        logger.info(f"  SYMBOL_MAP            = {SYMBOL_MAP}")
    if PRICE_TOLERANCE:
        logger.info(f"  PRICE_TOLERANCE       = {PRICE_TOLERANCE}")
    if ENTRY_PRICE_TOLERANCE:
        logger.info(f"  ENTRY_PRICE_TOLERANCE = {ENTRY_PRICE_TOLERANCE}")
    if ENTRY_PRICE_TOLERANCE_BETTER_PRICE_MULTIPLIER:
        logger.info(
            f"  ENTRY_PRICE_TOLERANCE_BETTER_PRICE_MULTIPLIER = {ENTRY_PRICE_TOLERANCE_BETTER_PRICE_MULTIPLIER}"
        )
    logger.info(
        f"  ENTRY_PRICE_TOLERANCE_BETTER_PRICE_MULTIPLIER_DEFAULT = {ENTRY_PRICE_TOLERANCE_BETTER_PRICE_MULTIPLIER_DEFAULT}"
    )
    if USE_LIMIT_ORDERS_ON_TOLERANCE_FAIL:
        logger.info(
            f"  USE_LIMIT_ORDERS_ON_TOLERANCE_FAIL = {USE_LIMIT_ORDERS_ON_TOLERANCE_FAIL}"
        )
    if USE_SECONDARY_LIMIT_ORDER:
        logger.info(f"  USE_SECONDARY_LIMIT_ORDER = {USE_SECONDARY_LIMIT_ORDER}")
        logger.info(
            f"  SECONDARY_LIMIT_ORDER_LOT_PERCENTAGE = {SECONDARY_LIMIT_ORDER_LOT_PERCENTAGE}"
        )
        logger.info(
            f"  SECONDARY_LIMIT_ORDER_DISTANCE_PERCENTAGE = {SECONDARY_LIMIT_ORDER_DISTANCE_PERCENTAGE}"
        )
    if LOT_MULTIPLIER:
        logger.info(f"  LOT_MULTIPLIER        = {LOT_MULTIPLIER}")
    if MAGIC_LOT_MULTIPLIER:
        logger.info(f"  MAGIC_LOT_MULTIPLIER  = {MAGIC_LOT_MULTIPLIER}")
    if MAGIC_FILTER:
        logger.info(f"  MAGIC_FILTER          = {MAGIC_FILTER}")
    if TELEGRAM_ENABLED:
        logger.info(f"  TELEGRAM_ENABLED     = {TELEGRAM_ENABLED}")


def get_csv_info():
    """Get CSV file contents and last modification time.

    Returns a tuple of (csv_content, mtime_str, mtime_datetime) or (None, None, None) if file doesn't exist.
    """
    if not os.path.exists(MT4_FILE_PATH):
        return None, None, None

    try:
        # Get modification time
        mtime = os.path.getmtime(MT4_FILE_PATH)
        mtime_datetime = datetime.fromtimestamp(mtime)
        mtime_str = mtime_datetime.strftime("%Y-%m-%d %H:%M:%S")

        # Read CSV contents
        try:
            with open(MT4_FILE_PATH, "r", newline="", encoding="utf-8") as f:
                csv_content = f.read()
            return csv_content, mtime_str, mtime_datetime
        except (PermissionError, FileNotFoundError, OSError) as e:
            # File might be locked, return what we can
            return f"[File locked/unreadable: {str(e)}]", mtime_str, mtime_datetime
    except OSError:
        return None, None, None


def send_telegram_message(text, include_csv=True):
    """Send a Telegram message if enabled and chat ID is set.

    Args:
        text: The main message text
        include_csv: If True, append CSV contents and modification time to the message
    """
    global TELEGRAM_CHAT_ID, BOT_TOKEN, TELEGRAM_ENABLED

    if not TELEGRAM_ENABLED or not BOT_TOKEN:
        logger.debug("Telegram not enabled or BOT_TOKEN not set")
        return False

    if not TELEGRAM_CHAT_ID:
        # Chat ID not set yet - this is likely the issue!
        logger.debug(
            f"TELEGRAM_CHAT_ID not set. Send a message to the bot first (e.g., /start or /status) to set the chat ID."
        )
        return False

    # Append CSV info if requested
    if include_csv:
        csv_content, mtime_str, _ = get_csv_info()
        if csv_content is not None:
            # Format CSV info
            csv_info = f"\n\n--- CSV File Info ---\n"
            csv_info += f"Last Update: {mtime_str}\n"
            csv_info += f"File: {os.path.basename(MT4_FILE_PATH)}\n"
            csv_info += f"\n--- CSV Contents ---\n"

            # Telegram has a 4096 character limit per message
            # Reserve space for the main message and CSV header
            max_message_length = 4096
            reserved_space = len(text) + len(csv_info) + 100  # 100 chars buffer
            available_space = max_message_length - reserved_space

            if available_space > 0 and len(csv_content) > available_space:
                # Truncate CSV content and add note
                csv_content = (
                    csv_content[:available_space]
                    + f"\n... [TRUNCATED - CSV too long, showing first {available_space} chars]"
                )

            csv_info += csv_content
            text = text + csv_info
        else:
            text = (
                text
                + f"\n\n--- CSV File Info ---\nFile not found or unreadable: {MT4_FILE_PATH}"
            )

    # Final check: Telegram message limit is 4096 characters
    if len(text) > 4096:
        # Truncate the entire message, keeping the beginning
        truncation_note = "\n\n... [MESSAGE TRUNCATED - Too long for Telegram]"
        max_length = 4096 - len(truncation_note)
        text = text[:max_length] + truncation_note

    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        params = {"chat_id": TELEGRAM_CHAT_ID, "text": text}
        response = requests.post(url, data=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data.get("ok"):
                return True
            else:
                error_desc = data.get("description", "Unknown error")
                logger.error(f"Telegram API error: {error_desc}")
                logger.error(f"Response: {data}")
                return False
        else:
            logger.error(f"HTTP error {response.status_code}: {response.text}")
            return False
    except Exception as e:
        logger.error(f"Error sending Telegram message: {e}")
        import traceback

        traceback.print_exc()
        return False


async def handle_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /status command."""
    global TELEGRAM_CHAT_ID

    # Store chat ID from first message
    if TELEGRAM_CHAT_ID is None:
        TELEGRAM_CHAT_ID = update.effective_chat.id
        logger.info(f"Stored Telegram chat ID: {TELEGRAM_CHAT_ID}")

    try:
        mt5_positions = mt5.positions_get()
        position_count = len(mt5_positions) if mt5_positions else 0

        mt5_orders = mt5.orders_get()
        order_count = len(mt5_orders) if mt5_orders else 0

        mt4_trades = read_mt4_trades()
        mt4_count = len(mt4_trades)

        status_msg = (
            f"MT5 Copier Status\n"
            f"MT4 Trades: {mt4_count}\n"
            f"MT5 Positions: {position_count}\n"
            f"MT5 Pending Orders: {order_count}\n"
            f"Server: {MT5_SERVER}\n"
            f"Account: {MT5_LOGIN}"
        )
        # Use send_telegram_message to include CSV info
        send_telegram_message(status_msg, include_csv=True)
        logger.info(f"Status requested via Telegram")
    except Exception as e:
        error_msg = f"Failed to get status: {str(e)}"
        send_telegram_message(error_msg, include_csv=True)


async def handle_stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /stop command."""
    global TELEGRAM_CHAT_ID, telegram_running

    # Store chat ID from first message
    if TELEGRAM_CHAT_ID is None:
        TELEGRAM_CHAT_ID = update.effective_chat.id
        logger.info(f"Stored Telegram chat ID: {TELEGRAM_CHAT_ID}")

    await update.message.reply_text("Stopping copier as requested...")
    logger.info("Stop command received via Telegram")
    telegram_running = False


async def handle_help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /help command."""
    global TELEGRAM_CHAT_ID

    # Store chat ID from first message
    if TELEGRAM_CHAT_ID is None:
        TELEGRAM_CHAT_ID = update.effective_chat.id
        logger.info(f"Stored Telegram chat ID: {TELEGRAM_CHAT_ID}")

    help_msg = (
        "Available Commands:\n"
        "/status - Get copier status\n"
        "/stop - Stop the copier\n"
        "/help - Show this help"
    )
    await update.message.reply_text(help_msg)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle incoming messages (store chat ID)."""
    global TELEGRAM_CHAT_ID

    # Store chat ID from first message
    if TELEGRAM_CHAT_ID is None:
        TELEGRAM_CHAT_ID = update.effective_chat.id
        logger.info(f"Stored Telegram chat ID: {TELEGRAM_CHAT_ID}")

    # Echo unrecognized commands
    text = update.message.text or ""
    if text and not text.startswith("/"):
        await update.message.reply_text("Use /help to see available commands")


def init_telegram_bot():
    """Initialize and start Telegram bot in background thread."""
    global telegram_app, telegram_running, BOT_TOKEN, TELEGRAM_ENABLED

    if not TELEGRAM_ENABLED or not BOT_TOKEN:
        logger.info("Telegram bot not enabled or BOT_TOKEN not set")
        return

    try:
        telegram_app = Application.builder().token(BOT_TOKEN).build()

        # Register command handlers
        telegram_app.add_handler(CommandHandler("status", handle_status_command))
        telegram_app.add_handler(CommandHandler("stat", handle_status_command))
        telegram_app.add_handler(CommandHandler("info", handle_status_command))
        telegram_app.add_handler(CommandHandler("stop", handle_stop_command))
        telegram_app.add_handler(CommandHandler("help", handle_help_command))
        telegram_app.add_handler(CommandHandler("start", handle_help_command))

        # Handle any other messages
        telegram_app.add_handler(
            MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)
        )

        # Start bot in background thread
        telegram_running = True

        def run_bot():
            telegram_app.run_polling(allowed_updates=Update.ALL_TYPES)

        bot_thread = threading.Thread(target=run_bot, daemon=True)
        bot_thread.start()
        logger.info("Telegram bot initialized and running")
    except Exception as e:
        logger.warning(f"Failed to initialize Telegram bot: {e}")
        telegram_app = None


def init_mt5():
    if not mt5.initialize():
        raise RuntimeError(f"MT5 initialize() failed, error code: {mt5.last_error()}")
    # If terminal login already has the right account, you might skip this
    if not mt5.login(MT5_LOGIN, password=MT5_PASSWORD, server=MT5_SERVER):
        raise RuntimeError(f"MT5 login() failed, error code: {mt5.last_error()}")
    logger.info("MT5 initialized and logged in")


class MT4FileHandler(FileSystemEventHandler):
    """File system event handler for MT4 trades CSV file."""

    def __init__(self, file_path):
        self.file_path = Path(file_path)
        self.watched_file = self.file_path.name
        self.watched_dir = self.file_path.parent

    def on_modified(self, event):
        """Called when a file or directory is modified."""
        if not event.is_directory:
            # Check if it's the file we're watching
            if Path(event.src_path).name == self.watched_file:
                # Signal that the file has changed
                file_changed_event.set()
                logger.info(f"File change detected: {event.src_path}")

    def on_created(self, event):
        """Called when a file or directory is created."""
        if not event.is_directory:
            if Path(event.src_path).name == self.watched_file:
                file_changed_event.set()
                logger.info(f"File created: {event.src_path}")


def setup_file_watcher(file_path):
    """Set up file system watcher for MT4 trades CSV file."""
    global file_observer, USE_FILE_WATCHER

    if not USE_FILE_WATCHER:
        return None

    try:
        file_path_obj = Path(file_path)
        if not file_path_obj.exists():
            logger.info(
                f"Warning: File {file_path} does not exist yet. Watcher will monitor directory."
            )

        # Watch the directory containing the file
        watch_dir = file_path_obj.parent
        if not watch_dir.exists():
            logger.info(
                f"Warning: Directory {watch_dir} does not exist. File watching disabled."
            )
            USE_FILE_WATCHER = False
            return None

        event_handler = MT4FileHandler(file_path)
        observer = Observer()
        observer.schedule(event_handler, str(watch_dir), recursive=False)
        observer.start()
        logger.info(
            f"File watcher started for {file_path} (watching directory: {watch_dir})"
        )
        return observer
    except Exception as e:
        logger.warning(f"Failed to set up file watcher: {e}. Falling back to polling.")
        USE_FILE_WATCHER = False
        return None


def _read_csv_trades():
    """Helper function to read and parse CSV trades file.

    Returns a tuple of (trades_dict, row_count) where:
    - trades_dict: dict keyed by ticket with trade data
    - row_count: number of data rows read (excluding header)

    Raises (PermissionError, FileNotFoundError, OSError) if file can't be read.
    """
    trades = {}
    row_count = 0

    with open(MT4_FILE_PATH, "r", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            row_count += 1
            try:
                ticket = int(row["ticket"])
                symbol = row["symbol"]
                order_type = int(row["type"])  # 0=BUY,1=SELL
                lots = float(row["lots"])
                open_price = float(row["open_price"])
                sl = float(row["sl"])
                tp = float(row["tp"])
                # Try to read magic number if present, otherwise use default
                magic = int(row.get("magic", 987654))
            except (ValueError, KeyError):
                continue

            symbol_mt5 = SYMBOL_MAP.get(symbol, symbol)

            trades[ticket] = {
                "ticket": ticket,
                "symbol_mt4": symbol,
                "symbol": symbol_mt5,
                "type": order_type,
                "lots": lots,
                "open_price": open_price,
                "sl": sl,
                "tp": tp,
                "magic": magic,
            }

    return trades, row_count


def read_mt4_trades():
    """Read trades from the CSV exported by the MT4 EA.

    Returns a dict keyed by MT4 ticket.
    Uses LAST_MT4_TRADES as a fallback if the file can't be read.
    Only reads file if it has been modified since last read.
    Retries if file is empty or locked (MT4 might be writing).
    """
    global LAST_MT4_TRADES, LAST_MT4_FILE_MTIME, _last_error_print_time

    # Check if file exists and has been modified
    if not os.path.exists(MT4_FILE_PATH):
        # File temporarily missing while EA is moving it -> keep previous snapshot
        return LAST_MT4_TRADES

    try:
        current_mtime = os.path.getmtime(MT4_FILE_PATH)
        # Skip reading if file hasn't changed
        if LAST_MT4_FILE_MTIME is not None and current_mtime == LAST_MT4_FILE_MTIME:
            return LAST_MT4_TRADES
    except OSError:
        # Can't get mtime, proceed with read attempt
        pass

    # Retry logic: try reading up to 5 times with exponential backoff
    # MT4 locks the file during writes, so we need to wait and retry
    max_retries = 5
    base_retry_delay = 0.02  # Start with 20ms delay

    for attempt in range(max_retries):
        trades = {}
        try:
            # Check file size first - if file is too small (just header or empty), skip
            try:
                file_size = os.path.getsize(MT4_FILE_PATH)
                if (
                    file_size < 50
                ):  # Header is about 80 bytes, so if less than 50, likely empty/incomplete
                    if attempt < max_retries - 1:
                        # Exponential backoff: 20ms, 40ms, 80ms, 160ms
                        retry_delay = base_retry_delay * (2**attempt)
                        time.sleep(retry_delay)
                        continue
                    # Last attempt, return last known trades
                    return LAST_MT4_TRADES
            except OSError:
                pass

            # Try to read the file - this will raise PermissionError if locked by MT4
            trades, row_count = _read_csv_trades()

            # If we read the file but got no trades (only header), do one re-read to verify
            # If both reads match (both empty), accept as legitimate. Otherwise, accept second read.
            if row_count == 0:
                # First read was empty, do one re-read after 50ms
                time.sleep(0.05)  # 50ms delay
                try:
                    trades_second_read, row_count_second = _read_csv_trades()
                except (PermissionError, FileNotFoundError, OSError):
                    # Second read failed, accept first read (empty)
                    trades_second_read = {}
                    row_count_second = 0

                # Compare both reads - if they match (both empty), accept as legitimate
                if row_count == 0 and row_count_second == 0:
                    # Both reads are empty - file is legitimately empty
                    LAST_MT4_TRADES = {}
                    try:
                        LAST_MT4_FILE_MTIME = os.path.getmtime(MT4_FILE_PATH)
                    except OSError:
                        pass
                    logger.debug(
                        f"CSV file is empty (no trades), verified by two reads, returning empty dict"
                    )
                    return {}
                else:
                    # Reads don't match - file changed between reads, accept second read (more recent)
                    trades = trades_second_read
                    # Continue to update snapshot below

            # If we got here, the read was successful -> update snapshot and mtime
            LAST_MT4_TRADES = trades
            try:
                LAST_MT4_FILE_MTIME = os.path.getmtime(MT4_FILE_PATH)
            except OSError:
                pass
            return trades

        except (PermissionError, FileNotFoundError, OSError) as e:
            # File temporarily locked or being replaced -> retry or keep previous snapshot
            if attempt < max_retries - 1:
                # Exponential backoff: 100ms, 200ms, 400ms, 800ms
                retry_delay = base_retry_delay * (2**attempt)
                if isinstance(e, PermissionError):
                    logger.debug(
                        f"File locked (MT4 writing), retrying in {retry_delay*1000:.0f}ms (attempt {attempt + 1}/{max_retries})..."
                    )
                time.sleep(retry_delay)
                continue

            # Last attempt failed, keep previous snapshot
            # Only print error occasionally to avoid spam (every 10 seconds)
            current_time = time.time()
            if current_time - _last_error_print_time > 10:
                error_type = type(e).__name__
                if isinstance(e, PermissionError):
                    logger.warning(
                        f"File locked after {max_retries} attempts (MT4 writing), using last snapshot..."
                    )
                else:
                    logger.info(
                        f"Could not read mt4_trades.csv ({error_type}), using last snapshot..."
                    )
                _last_error_print_time = current_time
            return LAST_MT4_TRADES

    # All retries exhausted, return last known trades
    return LAST_MT4_TRADES


def get_mt5_positions():
    """Return open positions in MT5 as a dict keyed by a custom 'source_ticket' (magic or comment-based).

    Positions can have comments like 'SRC:xxxxxx' (market orders), 'O1:xxxxxx' (filled limit orders),
    or 'O2:xxxxxx' (filled secondary limit orders).
    """
    positions = {}
    mt5_positions = mt5.positions_get()
    if mt5_positions is None:
        return positions

    for pos in mt5_positions:
        # pos.comment might be something like "SRC:123456", "O1:123456", or "O2:123456"
        src_ticket = None
        if (
            pos.comment.startswith("SRC:")
            or pos.comment.startswith("O1:")
            or pos.comment.startswith("O2:")
        ):
            try:
                src_ticket = int(pos.comment.split()[0].split(":")[1])
            except (IndexError, ValueError):
                src_ticket = None

        if src_ticket is not None:
            # If we already have a position for this source ticket, prefer the one with "SRC:" comment
            # (the main position) over filled limit/secondary orders
            if src_ticket in positions:
                if pos.comment.startswith("SRC:") and not positions[
                    src_ticket
                ].comment.startswith("SRC:"):
                    positions[src_ticket] = pos
            else:
                positions[src_ticket] = pos

    return positions


def get_mt5_pending_orders():
    """Return pending orders (limit orders) in MT5 as a dict keyed by source ticket.

    Pending orders are identified by comment starting with 'SRC:', 'O1:' (limit), or 'O2:' (secondary).
    """
    pending_orders = {}
    orders = mt5.orders_get()
    if orders is None:
        return pending_orders

    for order in orders:
        # order.comment might be something like "SRC:123456", "O1:123456" (limit), or "O2:123456" (secondary)
        src_ticket = None
        if order.comment.startswith("SRC:"):
            try:
                src_ticket = int(order.comment.split()[0].split(":")[1])
            except (IndexError, ValueError):
                src_ticket = None
        elif order.comment.startswith("O1:"):
            try:
                src_ticket = int(order.comment.split()[0].split(":")[1])
            except (IndexError, ValueError):
                src_ticket = None
        elif order.comment.startswith("O2:"):
            try:
                src_ticket = int(order.comment.split()[0].split(":")[1])
            except (IndexError, ValueError):
                src_ticket = None

        if src_ticket is not None:
            # Store all orders for a source ticket (may have multiple: limit and secondary)
            if src_ticket not in pending_orders:
                pending_orders[src_ticket] = []
            if isinstance(pending_orders[src_ticket], list):
                pending_orders[src_ticket].append(order)
            else:
                # Convert single order to list
                pending_orders[src_ticket] = [pending_orders[src_ticket], order]

    return pending_orders


def ensure_symbol_ready(symbol):
    """Make sure symbol is available for trading in MT5.
    Returns symbol_info for reuse to avoid redundant API calls.
    """
    global SYMBOL_CACHE

    # Check cache first
    if symbol in SYMBOL_CACHE:
        info = SYMBOL_CACHE[symbol]
        if info is not None and info.visible:
            return info

    # Symbol not in cache or not ready, fetch it
    if not mt5.symbol_select(symbol, True):
        raise RuntimeError(f"Failed to select symbol {symbol}")
    info = mt5.symbol_info(symbol)
    if info is None or not info.visible:
        raise RuntimeError(f"Symbol {symbol} not visible in MarketWatch")

    # Cache the symbol info
    SYMBOL_CACHE[symbol] = info
    return info


def check_price_tolerance(symbol, order_type, sl, tp):
    """Check if current price is too close to SL or TP based on PRICE_TOLERANCE config.

    Returns (is_valid, reason) where is_valid is True if trade can proceed.
    """
    # Check if symbol has tolerance configured
    tolerance_points = PRICE_TOLERANCE.get(symbol)
    if tolerance_points is None:
        # No tolerance configured for this symbol, allow trade
        return True, None

    # Get symbol info for point value (static, can use cache)
    symbol_info = ensure_symbol_ready(symbol)
    if symbol_info is None:
        return (
            True,
            None,
        )  # If we can't get symbol info, proceed (will fail later anyway)

    point = symbol_info.point

    # Get fresh tick data to get current bid/ask prices (don't use cached symbol info prices)
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        # Fallback to symbol info if tick data unavailable
        if order_type == 0:  # BUY
            current_price = symbol_info.ask
        else:  # SELL
            current_price = symbol_info.bid
    else:
        # Use fresh tick data for current prices
        if order_type == 0:  # BUY
            current_price = tick.ask
        else:  # SELL
            current_price = tick.bid
    tolerance_price = tolerance_points * point

    # Check distance to SL
    if sl > 0:
        sl_distance = abs(current_price - sl)
        if sl_distance <= tolerance_price:
            return (
                False,
                f"Price {current_price} is within {tolerance_points} points of SL {sl} (distance: {sl_distance/point:.2f} points)",
            )

    # Check distance to TP
    if tp > 0:
        tp_distance = abs(current_price - tp)
        if tp_distance <= tolerance_price:
            return (
                False,
                f"Price {current_price} is within {tolerance_points} points of TP {tp} (distance: {tp_distance/point:.2f} points)",
            )

    return True, None


def check_entry_price_tolerance(symbol, order_type, original_price):
    """Check if current price is within tolerance of the original MT4 entry price.

    Tolerance is specified as absolute price difference (not points).

    For BUY orders: only enter if original_price - (tolerance * multiplier) <= current_price <= original_price + tolerance
    - Upper bound: current_price <= original_price + tolerance (prevents entering at worse prices)
    - Lower bound: current_price >= original_price - (tolerance * multiplier) (allows better prices with more tolerance)

    For SELL orders: only enter if original_price - tolerance <= current_price <= original_price + (tolerance * multiplier)
    - Lower bound: current_price >= original_price - tolerance (prevents entering at worse prices)
    - Upper bound: current_price <= original_price + (tolerance * multiplier) (allows better prices with more tolerance)

    Returns (is_valid, reason) where is_valid is True if trade can proceed.
    """
    # Check if symbol has entry price tolerance configured
    tolerance_price = ENTRY_PRICE_TOLERANCE.get(symbol)
    if tolerance_price is None:
        # No tolerance configured for this symbol, allow trade
        return True, None

    # Get multiplier for better price tolerance (symbol-specific or default)
    multiplier = ENTRY_PRICE_TOLERANCE_BETTER_PRICE_MULTIPLIER.get(
        symbol, ENTRY_PRICE_TOLERANCE_BETTER_PRICE_MULTIPLIER_DEFAULT
    )

    # Get fresh tick data to get current bid/ask prices (don't use cached symbol info)
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        # Fallback to symbol info if tick data unavailable
        symbol_info = ensure_symbol_ready(symbol)
        if symbol_info is None:
            return (
                True,
                None,
            )  # If we can't get symbol info, proceed (will fail later anyway)
        if order_type == 0:  # BUY
            current_price = symbol_info.ask
        else:  # SELL
            current_price = symbol_info.bid
    else:
        # Use fresh tick data for current prices
        if order_type == 0:  # BUY
            current_price = tick.ask
        else:  # SELL
            current_price = tick.bid

    # Check tolerance based on order type (using absolute price difference)
    if order_type == 0:  # BUY
        # Upper bound: current_price <= original_price + tolerance (prevents worse prices)
        max_allowed_price = original_price + tolerance_price
        if current_price > max_allowed_price:
            price_diff = current_price - max_allowed_price
            return (
                False,
                f"BUY: Current price {current_price} is above original price {original_price} + tolerance {tolerance_price} (max allowed: {max_allowed_price}, difference: {price_diff:.5f})",
            )

        # Lower bound: current_price >= original_price - (tolerance * multiplier) (allows better prices)
        min_allowed_price = original_price - (tolerance_price * multiplier)
        if current_price < min_allowed_price:
            price_diff = min_allowed_price - current_price
            return (
                False,
                f"BUY: Current price {current_price} is below original price {original_price} - (tolerance {tolerance_price} * multiplier {multiplier}) = {min_allowed_price:.5f} (min allowed: {min_allowed_price:.5f}, difference: {price_diff:.5f})",
            )
    else:  # SELL
        # Lower bound: current_price >= original_price - tolerance (prevents worse prices)
        min_allowed_price = original_price - tolerance_price
        if current_price < min_allowed_price:
            price_diff = min_allowed_price - current_price
            return (
                False,
                f"SELL: Current price {current_price} is below original price {original_price} - tolerance {tolerance_price} (min allowed: {min_allowed_price}, difference: {price_diff:.5f})",
            )

        # Upper bound: current_price <= original_price + (tolerance * multiplier) (allows better prices)
        max_allowed_price = original_price + (tolerance_price * multiplier)
        if current_price > max_allowed_price:
            price_diff = current_price - max_allowed_price
            return (
                False,
                f"SELL: Current price {current_price} is above original price {original_price} + (tolerance {tolerance_price} * multiplier {multiplier}) = {max_allowed_price:.5f} (max allowed: {max_allowed_price:.5f}, difference: {price_diff:.5f})",
            )

    return True, None


def send_limit_order(
    symbol, order_type, lots, sl, tp, src_ticket, entry_price, magic=987654
):
    """Send a limit order in MT5 at the original MT4 entry price."""
    symbol_info = ensure_symbol_ready(symbol)
    if symbol_info is None:
        logger.info(f"Symbol info unavailable for {symbol}")
        return False

    if order_type == 0:  # BUY
        mt5_type = mt5.ORDER_TYPE_BUY_LIMIT
    else:  # SELL
        mt5_type = mt5.ORDER_TYPE_SELL_LIMIT

    request = {
        "action": mt5.TRADE_ACTION_PENDING,
        "symbol": symbol,
        "volume": lots,
        "type": mt5_type,
        "price": entry_price,
        "sl": sl if sl > 0 else 0.0,
        "tp": tp if tp > 0 else 0.0,
        "deviation": 20,
        "magic": magic,
        "comment": f"O1:{src_ticket}",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }

    result = mt5.order_send(request)
    if result is None:
        logger.info(
            f"order_send() failed for limit order (ticket {src_ticket}), last_error={mt5.last_error()}"
        )
        return False

    if result.retcode != mt5.TRADE_RETCODE_DONE:
        logger.error(
            f"Limit order failed, retcode={result.retcode}, comment={result.comment}"
        )
        return False
    else:
        order_type_str = "BUY LIMIT" if order_type == 0 else "SELL LIMIT"
        sl_str = f", SL={sl}" if sl > 0 else ""
        tp_str = f", TP={tp}" if tp > 0 else ""
        message = f"MT5 Limit Order Placed\nSymbol: {symbol}\nType: {order_type_str}\nVolume: {lots}\nPrice: {entry_price:.5f}{sl_str}{tp_str}\nMT4 Ticket: {src_ticket}"
        send_telegram_message(message)
        logger.info(
            f"Placed limit order for MT4 ticket {src_ticket}: {symbol} {mt5_type} at {entry_price}, volume={lots}"
        )
        return True


def cancel_order(order):
    """Cancel a pending order."""
    request = {
        "action": mt5.TRADE_ACTION_REMOVE,
        "order": order.ticket,
    }

    result = mt5.order_send(request)
    if result is None:
        logger.error(
            f"Failed to cancel order {order.ticket}, last_error={mt5.last_error()}"
        )
        return False

    if result.retcode != mt5.TRADE_RETCODE_DONE:
        logger.info(
            f"Cancel order failed, retcode={result.retcode}, comment={result.comment}"
        )
        return False
    else:
        logger.info(
            f"Cancelled limit order {order.ticket} (MT4 ticket from comment: {order.comment})"
        )
        return True


def calculate_secondary_limit_price(entry_price, sl, order_type):
    """Calculate the price for a secondary limit order between entry and SL.

    For BUY: entry_price > sl, so price should be between entry and sl (below entry)
    For SELL: entry_price < sl, so price should be between entry and sl (above entry)

    Returns the calculated price, or None if SL is invalid (0 or negative).
    """
    if sl <= 0:
        return None  # No SL, cannot calculate

    if order_type == 0:  # BUY
        # Entry is above SL, so we want a price below entry
        distance = entry_price - sl
        price = entry_price - (distance * SECONDARY_LIMIT_ORDER_DISTANCE_PERCENTAGE)
    else:  # SELL
        # Entry is below SL, so we want a price above entry
        distance = sl - entry_price
        price = entry_price + (distance * SECONDARY_LIMIT_ORDER_DISTANCE_PERCENTAGE)

    return price


def send_secondary_limit_order(
    symbol,
    order_type,
    original_lots,
    sl,
    tp,
    src_ticket,
    entry_price,
    magic=987654,
    mt5_order_tickets_set=None,
):
    """Send a secondary limit order between entry price and SL."""
    # Calculate the limit order price
    limit_price = calculate_secondary_limit_price(entry_price, sl, order_type)
    if limit_price is None:
        logger.info(
            f"Cannot place secondary limit order for {symbol} (MT4 ticket {src_ticket}): No valid SL"
        )
        return False

    # Calculate lot size (percentage of original lots)
    secondary_lots = original_lots * SECONDARY_LIMIT_ORDER_LOT_PERCENTAGE

    symbol_info = ensure_symbol_ready(symbol)
    if symbol_info is None:
        logger.info(f"Symbol info unavailable for {symbol}")
        return False

    # Round lot size to symbol's lot step and clamp to min/max
    lot_step = symbol_info.volume_step
    min_lot = symbol_info.volume_min
    max_lot = symbol_info.volume_max

    # Round to lot step
    secondary_lots = round(secondary_lots / lot_step) * lot_step
    # Clamp to min/max
    secondary_lots = max(min_lot, min(secondary_lots, max_lot))

    if order_type == 0:  # BUY
        mt5_type = mt5.ORDER_TYPE_BUY_LIMIT
    else:  # SELL
        mt5_type = mt5.ORDER_TYPE_SELL_LIMIT

    request = {
        "action": mt5.TRADE_ACTION_PENDING,
        "symbol": symbol,
        "volume": secondary_lots,
        "type": mt5_type,
        "price": limit_price,
        "sl": sl if sl > 0 else 0.0,
        "tp": tp if tp > 0 else 0.0,
        "deviation": 20,
        "magic": magic,
        "comment": f"O2:{src_ticket}",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }

    result = mt5.order_send(request)
    if result is None:
        logger.error(
            f"order_send() failed for secondary limit order (ticket {src_ticket}), last_error={mt5.last_error()}"
        )
        return False

    if result.retcode != mt5.TRADE_RETCODE_DONE:
        logger.info(
            f"Secondary limit order failed, retcode={result.retcode}, comment={result.comment}"
        )
        return False
    else:
        distance_pct = SECONDARY_LIMIT_ORDER_DISTANCE_PERCENTAGE * 100
        lot_pct = SECONDARY_LIMIT_ORDER_LOT_PERCENTAGE * 100
        order_ticket = result.order if hasattr(result, "order") else None
        if order_ticket:
            if mt5_order_tickets_set is not None:
                # Store the MT5 order ticket so we don't cancel it in this cycle
                mt5_order_tickets_set.add(order_ticket)
            # Store order by key for future detection (even if comment is wrong)
            order_key = (symbol, limit_price, mt5_type)
            SECONDARY_ORDERS_BY_KEY[order_key] = order_ticket
        order_type_str = "BUY LIMIT" if order_type == 0 else "SELL LIMIT"
        sl_str = f", SL={sl}" if sl > 0 else ""
        tp_str = f", TP={tp}" if tp > 0 else ""
        message = f"MT5 Secondary Limit Order Placed\nSymbol: {symbol}\nType: {order_type_str}\nVolume: {secondary_lots} ({lot_pct}% of {original_lots})\nPrice: {limit_price:.5f} ({distance_pct}% between entry and SL){sl_str}{tp_str}\nMT4 Ticket: {src_ticket}"
        send_telegram_message(message)
        logger.info(
            f"Placed secondary limit order for MT4 ticket {src_ticket}: {symbol} {mt5_type} "
            f"at {limit_price} ({distance_pct}% between entry {entry_price} and SL {sl}), "
            f"volume={secondary_lots} ({lot_pct}% of {original_lots} lots), MT5 ticket={order_ticket}"
        )
        return True


def send_order(symbol, order_type, lots, sl, tp, src_ticket, magic=987654):
    """Send a market order in MT5 mirroring the MT4 trade."""
    symbol_info = ensure_symbol_ready(symbol)
    if symbol_info is None:
        logger.info(f"Symbol info unavailable for {symbol}")
        return

    if order_type == 0:  # BUY
        price = symbol_info.ask
        mt5_type = mt5.ORDER_TYPE_BUY
    else:  # SELL
        price = symbol_info.bid
        mt5_type = mt5.ORDER_TYPE_SELL

    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": lots,
        "type": mt5_type,
        "price": price,
        "sl": sl if sl > 0 else 0.0,
        "tp": tp if tp > 0 else 0.0,
        "deviation": 20,
        "magic": magic,  # Use magic from MT4 trade, or default
        "comment": f"SRC:{src_ticket}",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }

    result = mt5.order_send(request)
    if result is None:
        logger.info(
            f"order_send() failed for ticket {src_ticket}, last_error={mt5.last_error()}"
        )
        return

    if result.retcode != mt5.TRADE_RETCODE_DONE:
        logger.info(
            f"order_send() failed, retcode={result.retcode}, comment={result.comment}"
        )
    else:
        order_type_str = "BUY" if order_type == 0 else "SELL"
        sl_str = f", SL={sl}" if sl > 0 else ""
        tp_str = f", TP={tp}" if tp > 0 else ""
        message = f"MT5 Trade Opened\nSymbol: {symbol}\nType: {order_type_str}\nVolume: {lots}\nPrice: {price:.5f}{sl_str}{tp_str}\nMT4 Ticket: {src_ticket}"
        send_telegram_message(message)
        logger.info(
            f"Opened MT5 {symbol} position for MT4 ticket {src_ticket}, volume={lots}"
        )


def modify_position_sltp(position, new_sl, new_tp):
    """Modify stop loss and take profit for an existing MT5 position."""
    # Normalize SL/TP values (0 means no SL/TP)
    sl = new_sl if new_sl > 0 else 0.0
    tp = new_tp if new_tp > 0 else 0.0

    request = {
        "action": mt5.TRADE_ACTION_SLTP,
        "symbol": position.symbol,
        "position": position.ticket,
        "sl": sl,
        "tp": tp,
    }

    result = mt5.order_send(request)
    if result is None:
        error_msg = f"order_send() failed (modify SL/TP), last_error={mt5.last_error()}"
        logger.error(error_msg)
        # Send notification about failure
        sl_str = f"{sl:.5f}" if sl > 0 else "None"
        tp_str = f"{tp:.5f}" if tp > 0 else "None"
        message = f"MT5 SL/TP Update Failed\nSymbol: {position.symbol}\nPosition: {position.ticket}\nAttempted SL: {sl_str}\nAttempted TP: {tp_str}\nError: {mt5.last_error()}"
        send_telegram_message(message)
        return False

    if result.retcode != mt5.TRADE_RETCODE_DONE:
        error_msg = (
            f"Modify SL/TP failed, retcode={result.retcode}, comment={result.comment}"
        )
        logger.error(error_msg)
        # Send notification about failure
        sl_str = f"{sl:.5f}" if sl > 0 else "None"
        tp_str = f"{tp:.5f}" if tp > 0 else "None"
        message = f"MT5 SL/TP Update Failed\nSymbol: {position.symbol}\nPosition: {position.ticket}\nAttempted SL: {sl_str}\nAttempted TP: {tp_str}\nRetcode: {result.retcode}\nComment: {result.comment}"
        send_telegram_message(message)
        return False
    else:
        sl_str = f"{sl:.5f}" if sl > 0 else "None"
        tp_str = f"{tp:.5f}" if tp > 0 else "None"
        message = f"MT5 SL/TP Updated\nSymbol: {position.symbol}\nPosition: {position.ticket}\nSL: {sl_str}\nTP: {tp_str}\nVolume: {position.volume}"
        result = send_telegram_message(message)
        logger.info(
            f"Updated SL/TP for MT5 position {position.ticket}: SL={sl}, TP={tp}"
        )
        if not result:
            logger.info(
                f"Warning: Failed to send Telegram notification for SL/TP update (position {position.ticket})"
            )
        return True


def close_position(position):
    """Close an MT5 position by ticket."""
    symbol = position.symbol
    symbol_info = ensure_symbol_ready(symbol)

    if position.type == mt5.POSITION_TYPE_BUY:
        mt5_type = mt5.ORDER_TYPE_SELL
        price = symbol_info.bid
    else:
        mt5_type = mt5.ORDER_TYPE_BUY
        price = symbol_info.ask

    # Check if position was closed due to SL/TP hit
    close_reason = "Manual Close"
    if position.sl > 0:
        # Check if close price is close to SL (within 5 points)
        symbol_info_full = ensure_symbol_ready(symbol)
        if symbol_info_full:
            point = symbol_info_full.point
            sl_distance = abs(price - position.sl)
            if sl_distance <= (5 * point):
                close_reason = "Stop Loss Hit"
    if position.tp > 0:
        # Check if close price is close to TP (within 5 points)
        symbol_info_full = ensure_symbol_ready(symbol)
        if symbol_info_full:
            point = symbol_info_full.point
            tp_distance = abs(price - position.tp)
            if tp_distance <= (5 * point):
                close_reason = "Take Profit Hit"

    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": position.volume,
        "type": mt5_type,
        "position": position.ticket,
        "price": price,
        "deviation": 20,
        "magic": position.magic,
        "comment": f"CLOSE_SRC:{position.comment}",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }

    result = mt5.order_send(request)
    if result is None:
        logger.error(f"order_send() failed (close), last_error={mt5.last_error()}")
        return

    if result.retcode != mt5.TRADE_RETCODE_DONE:
        logger.error(
            f"Close failed, retcode={result.retcode}, comment={result.comment}"
        )
    else:
        # Extract MT4 ticket from comment if available
        mt4_ticket = "Unknown"
        if (
            position.comment.startswith("SRC:")
            or position.comment.startswith("O1:")
            or position.comment.startswith("O2:")
        ):
            try:
                mt4_ticket = position.comment.split()[0].split(":")[1]
            except (IndexError, ValueError):
                pass

        position_type_str = "BUY" if position.type == mt5.POSITION_TYPE_BUY else "SELL"
        profit_loss = position.profit
        profit_str = (
            f"Profit: {profit_loss:.2f}"
            if profit_loss >= 0
            else f"Loss: {profit_loss:.2f}"
        )

        message = f"MT5 Position Closed ({close_reason})\nSymbol: {symbol}\nType: {position_type_str}\nVolume: {position.volume}\nClose Price: {price:.5f}\n{profit_str}\nMT4 Ticket: {mt4_ticket}"
        send_telegram_message(message)
        logger.info(f"Closed MT5 position {position.ticket}")


def sync_trades():
    """Main sync: ensure MT5 mirrors MT4 open trades with grace period for missing trades."""
    global MISSING_SINCE, RECENTLY_OPENED

    mt4_trades = read_mt4_trades()
    mt5_positions_by_src = get_mt5_positions()
    mt5_pending_orders_by_src = get_mt5_pending_orders()
    now = datetime.now()

    # Clean up old entries from RECENTLY_OPENED (older than cooldown period)
    tickets_to_remove = []
    for src_ticket, open_time in RECENTLY_OPENED.items():
        time_since_open = (now - open_time).total_seconds()
        if time_since_open > RECENTLY_OPENED_COOLDOWN_SEC:
            tickets_to_remove.append(src_ticket)
    for ticket in tickets_to_remove:
        del RECENTLY_OPENED[ticket]

    # Track orders we've placed in this sync cycle to avoid duplicates
    orders_placed_this_cycle = set()  # Source tickets we've placed orders for
    mt5_order_tickets_placed_this_cycle = set()  # Actual MT5 order tickets we've placed

    # 1. Open any trades that exist in MT4 but not in MT5, or update SL/TP for existing ones
    for src_ticket, trade in mt4_trades.items():
        # Filter by magic number if MAGIC_FILTER is configured
        trade_magic = trade.get("magic", 987654)
        if MAGIC_FILTER and trade_magic not in MAGIC_FILTER:
            continue  # Skip trades that don't match magic filter

        if src_ticket not in mt5_positions_by_src:
            # Check if this source ticket was recently opened (prevent duplicates)
            if src_ticket in RECENTLY_OPENED:
                time_since_open = (now - RECENTLY_OPENED[src_ticket]).total_seconds()
                if time_since_open < RECENTLY_OPENED_COOLDOWN_SEC:
                    logger.info(
                        f"Skipping MT5 trade for MT4 ticket {src_ticket} ({trade['symbol']}): "
                        f"Recently opened {time_since_open:.1f}s ago (cooldown: {RECENTLY_OPENED_COOLDOWN_SEC}s)"
                    )
                    continue

            # Check price tolerance (SL/TP proximity) before opening new trade
            is_valid, reason = check_price_tolerance(
                trade["symbol"], trade["type"], trade["sl"], trade["tp"]
            )
            if not is_valid:
                logger.info(
                    f"Skipping MT5 trade for MT4 ticket {src_ticket} ({trade['symbol']}): {reason}"
                )
                continue

            # Check entry price tolerance (current price vs original MT4 entry price)
            is_valid, reason = check_entry_price_tolerance(
                trade["symbol"], trade["type"], trade["open_price"]
            )
            if not is_valid:
                # If limit orders are enabled, place a limit order instead of skipping
                if USE_LIMIT_ORDERS_ON_TOLERANCE_FAIL:
                    # Check if we already have a pending order for this source ticket
                    if src_ticket not in mt5_pending_orders_by_src:
                        # Apply lot multipliers (global, symbol, and/or magic number)
                        original_lots = trade["lots"]
                        global_multiplier = GLOBAL_LOT_MULTIPLIER
                        symbol_multiplier = LOT_MULTIPLIER.get(trade["symbol"], 1.0)
                        magic_multiplier = MAGIC_LOT_MULTIPLIER.get(trade_magic, 1.0)
                        total_multiplier = (
                            global_multiplier * symbol_multiplier * magic_multiplier
                        )
                        adjusted_lots = original_lots * total_multiplier

                        logger.info(
                            f"Entry tolerance not met for MT4 ticket {src_ticket} ({trade['symbol']}): {reason}"
                        )
                        logger.info(
                            f"Placing limit order at original entry price {trade['open_price']}"
                        )
                        send_limit_order(
                            symbol=trade["symbol"],
                            order_type=trade["type"],
                            lots=adjusted_lots,
                            sl=trade["sl"],
                            tp=trade["tp"],
                            src_ticket=src_ticket,
                            entry_price=trade["open_price"],
                            magic=trade_magic,
                        )

                        # Place secondary limit order if enabled
                        if USE_SECONDARY_LIMIT_ORDER and trade["sl"] > 0:
                            # Skip if we already placed an order for this ticket in this cycle
                            if src_ticket not in orders_placed_this_cycle:
                                order_placed = send_secondary_limit_order(
                                    symbol=trade["symbol"],
                                    order_type=trade["type"],
                                    original_lots=adjusted_lots,
                                    sl=trade["sl"],
                                    tp=trade["tp"],
                                    src_ticket=src_ticket,
                                    entry_price=trade["open_price"],
                                    magic=trade_magic,
                                    mt5_order_tickets_set=mt5_order_tickets_placed_this_cycle,
                                )
                                if order_placed:
                                    orders_placed_this_cycle.add(src_ticket)
                    # else: limit order already exists, do nothing
                else:
                    logger.info(
                        f"Skipping MT5 trade for MT4 ticket {src_ticket} ({trade['symbol']}): {reason}"
                    )
                continue

            # If we have a pending limit order for this trade but tolerance is now met, cancel it
            # (we'll place a market order instead)
            if src_ticket in mt5_pending_orders_by_src:
                logger.info(
                    f"Cancelling limit order {mt5_pending_orders_by_src[src_ticket].ticket} "
                    f"(entry tolerance now met for MT4 ticket {src_ticket}, placing market order instead)"
                )
                cancel_order(mt5_pending_orders_by_src[src_ticket])

            # Apply lot multipliers (global, symbol, and/or magic number)
            original_lots = trade["lots"]
            global_multiplier = GLOBAL_LOT_MULTIPLIER
            symbol_multiplier = LOT_MULTIPLIER.get(trade["symbol"], 1.0)
            magic_multiplier = MAGIC_LOT_MULTIPLIER.get(trade_magic, 1.0)
            total_multiplier = global_multiplier * symbol_multiplier * magic_multiplier
            adjusted_lots = original_lots * total_multiplier

            if total_multiplier != 1.0:
                multiplier_info = []
                if global_multiplier != 1.0:
                    multiplier_info.append(f"global={global_multiplier}x")
                if symbol_multiplier != 1.0:
                    multiplier_info.append(f"symbol={symbol_multiplier}x")
                if magic_multiplier != 1.0:
                    multiplier_info.append(f"magic={magic_multiplier}x")
                logger.info(
                    f"Applying lot multiplier {total_multiplier}x "
                    f"({' * '.join(multiplier_info)}) for {trade['symbol']} (magic {trade_magic}): "
                    f"{original_lots} -> {adjusted_lots}"
                )

            logger.info(f"Opening missing MT5 trade for MT4 ticket {src_ticket}")
            send_order(
                symbol=trade["symbol"],
                order_type=trade["type"],
                lots=adjusted_lots,
                sl=trade["sl"],
                tp=trade["tp"],
                src_ticket=src_ticket,
                magic=trade_magic,
            )
            # Track that we just opened this position to prevent duplicates
            RECENTLY_OPENED[src_ticket] = now

            # Place secondary limit order if enabled
            if USE_SECONDARY_LIMIT_ORDER and trade["sl"] > 0:
                # Skip if we already placed an order for this ticket in this cycle
                if src_ticket not in orders_placed_this_cycle:
                    order_placed = send_secondary_limit_order(
                        symbol=trade["symbol"],
                        order_type=trade["type"],
                        original_lots=adjusted_lots,
                        sl=trade["sl"],
                        tp=trade["tp"],
                        src_ticket=src_ticket,
                        entry_price=trade["open_price"],
                        magic=trade_magic,
                        mt5_order_tickets_set=mt5_order_tickets_placed_this_cycle,
                    )
                    if order_placed:
                        orders_placed_this_cycle.add(src_ticket)
        else:
            # Position exists, check if SL/TP need updating
            position = mt5_positions_by_src[src_ticket]
            mt4_sl = trade["sl"] if trade["sl"] > 0 else 0.0
            mt4_tp = trade["tp"] if trade["tp"] > 0 else 0.0
            mt5_sl = position.sl if position.sl > 0 else 0.0
            mt5_tp = position.tp if position.tp > 0 else 0.0

            # Check if SL or TP has changed (with small tolerance for floating point comparison)
            sl_changed = abs(mt4_sl - mt5_sl) > 0.00001
            tp_changed = abs(mt4_tp - mt5_tp) > 0.00001

            if sl_changed or tp_changed:
                logger.info(
                    f"Updating SL/TP for MT5 position {position.ticket} "
                    f"(MT4 ticket {src_ticket}): SL {mt5_sl}->{mt4_sl}, TP {mt5_tp}->{mt4_tp}"
                )
                result = modify_position_sltp(position, mt4_sl, mt4_tp)
                if not result:
                    logger.info(
                        f"Warning: SL/TP modification returned False for position {position.ticket}"
                    )

            # Check if secondary limit order exists for this position, place it if missing
            if USE_SECONDARY_LIMIT_ORDER and trade["sl"] > 0:
                # Skip if we already placed an order for this ticket in this cycle
                if src_ticket in orders_placed_this_cycle:
                    continue

                has_secondary_order = False

                # Calculate expected order price and type
                expected_price = calculate_secondary_limit_price(
                    trade["open_price"], trade["sl"], trade["type"]
                )
                if expected_price is None:
                    continue  # Can't calculate price (no SL)

                expected_order_type = (
                    mt5.ORDER_TYPE_BUY_LIMIT
                    if trade["type"] == 0
                    else mt5.ORDER_TYPE_SELL_LIMIT
                )
                order_key = (trade["symbol"], expected_price, expected_order_type)

                # Check if we know about this order from previous cycles
                if order_key in SECONDARY_ORDERS_BY_KEY:
                    known_ticket = SECONDARY_ORDERS_BY_KEY[order_key]
                    # Verify the order still exists
                    current_orders = mt5.orders_get()
                    if current_orders is not None:
                        for order in current_orders:
                            if order.ticket == known_ticket:
                                has_secondary_order = True
                                logger.info(
                                    f"Found existing secondary limit order {order.ticket} for MT4 ticket {src_ticket} (by price/type match)"
                                )
                                break

                # Also check directly with MT5 for existing secondary orders by comment
                if not has_secondary_order:
                    current_orders = mt5.orders_get()
                    if current_orders is not None:
                        for order in current_orders:
                            # Check by comment first
                            if order.comment.startswith("O2:"):
                                try:
                                    order_src_ticket = int(
                                        order.comment.split()[0].split(":")[1]
                                    )
                                    if order_src_ticket == src_ticket:
                                        has_secondary_order = True
                                        # Store the order key for future reference
                                        order_key_match = (
                                            order.symbol,
                                            order.price_open,
                                            order.type,
                                        )
                                        SECONDARY_ORDERS_BY_KEY[order_key_match] = (
                                            order.ticket
                                        )
                                        logger.info(
                                            f"Found existing secondary limit order {order.ticket} for MT4 ticket {src_ticket}"
                                        )
                                        break
                                except (IndexError, ValueError):
                                    pass

                            # Also check by symbol, price, and order type (in case comment is wrong)
                            if (
                                order.symbol == trade["symbol"]
                                and abs(order.price_open - expected_price) < 0.0001
                                and order.type == expected_order_type
                            ):
                                has_secondary_order = True
                                SECONDARY_ORDERS_BY_KEY[order_key] = order.ticket
                                logger.info(
                                    f"Found existing secondary limit order {order.ticket} for MT4 ticket {src_ticket} (by symbol/price/type match, comment: {order.comment})"
                                )
                                break

                # Also check if secondary order has already filled (became a position)
                if not has_secondary_order:
                    # Check all MT5 positions directly (not just those in mt5_positions_by_src,
                    # since get_mt5_positions() prefers SRC: over O2: comments)
                    all_mt5_positions = mt5.positions_get()
                    if all_mt5_positions is not None:
                        for pos in all_mt5_positions:
                            # Check if this is a filled secondary order for the same source ticket
                            if pos.comment.startswith("O2:"):
                                try:
                                    pos_src_ticket = int(
                                        pos.comment.split()[0].split(":")[1]
                                    )
                                    if (
                                        pos_src_ticket == src_ticket
                                        and pos.symbol == trade["symbol"]
                                    ):
                                        # Position exists from filled secondary order, don't place another
                                        has_secondary_order = True
                                        logger.info(
                                            f"Secondary limit order already filled (position {pos.ticket} exists for MT4 ticket {src_ticket})"
                                        )
                                        break
                                except (IndexError, ValueError):
                                    pass

                if not has_secondary_order:
                    # Calculate adjusted lots for secondary order
                    original_lots = trade["lots"]
                    global_multiplier = GLOBAL_LOT_MULTIPLIER
                    symbol_multiplier = LOT_MULTIPLIER.get(trade["symbol"], 1.0)
                    magic_multiplier = MAGIC_LOT_MULTIPLIER.get(trade_magic, 1.0)
                    total_multiplier = (
                        global_multiplier * symbol_multiplier * magic_multiplier
                    )
                    adjusted_lots = original_lots * total_multiplier

                    logger.info(
                        f"Placing missing secondary limit order for existing position "
                        f"(MT4 ticket {src_ticket})"
                    )
                    order_placed = send_secondary_limit_order(
                        symbol=trade["symbol"],
                        order_type=trade["type"],
                        original_lots=adjusted_lots,
                        sl=trade["sl"],
                        tp=trade["tp"],
                        src_ticket=src_ticket,
                        entry_price=trade["open_price"],
                        magic=trade_magic,
                        mt5_order_tickets_set=mt5_order_tickets_placed_this_cycle,
                    )
                    # Track that we placed an order for this ticket in this cycle
                    if order_placed:
                        orders_placed_this_cycle.add(src_ticket)

        # Remove from missing tracking if it exists now
        if src_ticket in MISSING_SINCE:
            del MISSING_SINCE[src_ticket]

    # 2. Cancel pending limit orders for trades that no longer exist in MT4
    # Refresh orders list to get current state (don't rely on cached list)
    current_all_orders = mt5.orders_get()
    if current_all_orders is not None:
        for order in current_all_orders:
            # Skip orders we just placed in this cycle (by MT5 order ticket)
            if order.ticket in mt5_order_tickets_placed_this_cycle:
                logger.info(
                    f"Skipping cancellation of order {order.ticket} "
                    f"(just placed in this cycle, comment: {order.comment})"
                )
                continue

            # Parse order comment to get source ticket
            order_src_ticket = None
            if (
                order.comment.startswith("SRC:")
                or order.comment.startswith("O1:")
                or order.comment.startswith("O2:")
            ):
                try:
                    order_src_ticket = int(order.comment.split()[0].split(":")[1])
                except (IndexError, ValueError):
                    # Comment parsing failed - check if this is a known secondary order by price/type
                    order_key = (order.symbol, order.price_open, order.type)
                    if (
                        order_key in SECONDARY_ORDERS_BY_KEY
                        and SECONDARY_ORDERS_BY_KEY[order_key] == order.ticket
                    ):
                        # This is a secondary order we know about, but comment is wrong
                        # Don't cancel it unless we're sure the source trade is gone
                        # (We'll check this by verifying the order still matches an active trade)
                        continue
                    else:
                        continue

            # Cancel order if source ticket no longer exists in MT4
            # BUT skip orders we just placed in this cycle (by source ticket)
            if order_src_ticket is not None and order_src_ticket not in mt4_trades:
                if order_src_ticket in orders_placed_this_cycle:
                    logger.info(
                        f"Skipping cancellation of order {order.ticket} "
                        f"(MT4 ticket {order_src_ticket} - just placed in this cycle, comment: {order.comment})"
                    )
                    continue

                # Before cancelling, check if this is a secondary order (O2:) that matches an active trade
                # (in case the comment is wrong but the order is actually for an active trade)
                order_key = (order.symbol, order.price_open, order.type)
                order_matches_active_trade = False
                if order.comment.startswith("O2:"):
                    # This is a secondary order - check if it matches any active trade
                    for mt4_src_ticket, mt4_trade in mt4_trades.items():
                        if mt4_trade["sl"] > 0 and mt4_trade["symbol"] == order.symbol:
                            expected_price = calculate_secondary_limit_price(
                                mt4_trade["open_price"],
                                mt4_trade["sl"],
                                mt4_trade["type"],
                            )
                            if (
                                expected_price
                                and abs(order.price_open - expected_price) < 0.0001
                            ):
                                expected_order_type = (
                                    mt5.ORDER_TYPE_BUY_LIMIT
                                    if mt4_trade["type"] == 0
                                    else mt5.ORDER_TYPE_SELL_LIMIT
                                )
                                if order.type == expected_order_type:
                                    order_matches_active_trade = True
                                    logger.info(
                                        f"Skipping cancellation of order {order.ticket} "
                                        f"(O2: order matches active MT4 trade {mt4_src_ticket} by price/type, comment says ticket {order_src_ticket})"
                                    )
                                    # Update tracking to map to correct source ticket
                                    SECONDARY_ORDERS_BY_KEY[order_key] = order.ticket
                                    break

                if not order_matches_active_trade:
                    logger.info(
                        f"Cancelling order {order.ticket} (MT4 ticket {order_src_ticket} no longer exists, comment: {order.comment})"
                    )
                    cancel_order(order)
                    # Remove from tracking if it was a secondary order
                    if order_key in SECONDARY_ORDERS_BY_KEY:
                        del SECONDARY_ORDERS_BY_KEY[order_key]

    # 3. Cancel pending LIMIT orders if position now exists (order was filled)
    # Note: Don't cancel SECONDARY orders - they should remain active alongside positions
    for src_ticket, orders in mt5_pending_orders_by_src.items():
        if src_ticket in mt5_positions_by_src:
            # Handle both single order and list of orders
            order_list = orders if isinstance(orders, list) else [orders]
            for order in order_list:
                # Only cancel O1: orders (entry limit orders), not O2: orders
                if order.comment.startswith("O1:"):
                    logger.info(
                        f"Cancelling limit order {order.ticket} (MT4 ticket {src_ticket} position now exists)"
                    )
                    cancel_order(order)
                # O2: orders should remain active even when position exists

    # 4. Close trades in MT5 that no longer exist in MT4 (with grace period)
    for src_ticket, position in mt5_positions_by_src.items():
        if src_ticket not in mt4_trades:
            # Track when we first noticed it missing
            if src_ticket not in MISSING_SINCE:
                MISSING_SINCE[src_ticket] = now
                logger.info(
                    f"MT4 ticket {src_ticket} missing, starting grace period..."
                )
            else:
                # Check if grace period has elapsed
                time_missing = (now - MISSING_SINCE[src_ticket]).total_seconds()
                if time_missing >= MT4_ABSENCE_GRACE_SEC:
                    logger.info(
                        f"Closing MT5 position {position.ticket} (no longer in MT4, grace period expired)"
                    )
                    close_position(position)
                    del MISSING_SINCE[src_ticket]


def main():
    global file_observer

    parser = argparse.ArgumentParser(description="MT4 -> MT5 trade copier")
    parser.add_argument(
        "--config",
        type=str,
        help="Path to JSON config file (overrides built-in defaults)",
    )
    args = parser.parse_args()

    # Load external config if provided
    if args.config:
        try:
            cfg = load_config_file(args.config)
            apply_config(cfg)
            logger.info(f"Loaded config from {args.config}")
        except Exception as e:
            logger.error(f"Failed to load config file {args.config}: {e}")
            return

    # Initialize Telegram bot if enabled
    if TELEGRAM_ENABLED:
        init_telegram_bot()

    # Set up file watcher if enabled
    if USE_FILE_WATCHER:
        file_observer = setup_file_watcher(MT4_FILE_PATH)
        if file_observer is None:
            logger.info("File watcher not available, using polling mode")

    try:
        init_mt5()
        logger.info("Starting copier loop...")
        if USE_FILE_WATCHER and file_observer:
            logger.info("Using file system watching (event-driven)")
        else:
            logger.info(f"Using polling mode (checking every {POLL_INTERVAL_SEC}s)")

        # Send startup notification
        if TELEGRAM_ENABLED:
            startup_message = f"MT4 to MT5 Copier Started\nMT5 Server: {MT5_SERVER}\nMT5 Login: {MT5_LOGIN}\nPoll Interval: {POLL_INTERVAL_SEC}s"
            send_telegram_message(startup_message)

        try:
            while True:
                if TELEGRAM_ENABLED and not telegram_running:
                    # Stop command received via Telegram
                    logger.info("Stop command received via Telegram, shutting down...")
                    break

                # Sync trades
                sync_trades()

                # Wait for file change event or timeout
                if USE_FILE_WATCHER and file_observer:
                    # Wait for file change event, with timeout as fallback
                    # This allows the loop to continue even if events are missed
                    file_changed_event.wait(timeout=POLL_INTERVAL_SEC)
                    file_changed_event.clear()  # Reset the event
                else:
                    # Fallback to polling
                    time.sleep(POLL_INTERVAL_SEC)
        except KeyboardInterrupt:
            logger.info("Stopping copier.")
            # Send shutdown notification
            if TELEGRAM_ENABLED:
                shutdown_message = (
                    "MT4 to MT5 Copier stopped by user (KeyboardInterrupt)"
                )
                send_telegram_message(shutdown_message)
        except Exception as e:
            # Send crash notification
            error_msg = (
                f"MT4 to MT5 Copier crashed with error:\n{type(e).__name__}: {str(e)}"
            )
            logger.error(f"Error: {error_msg}")
            if TELEGRAM_ENABLED:
                send_telegram_message(error_msg)
            raise  # Re-raise to preserve stack trace
    finally:
        # Stop file watcher if running
        if file_observer:
            file_observer.stop()
            file_observer.join()
            logger.info("File watcher stopped")
        mt5.shutdown()


if __name__ == "__main__":
    main()
