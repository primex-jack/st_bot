import os
import time
import subprocess
import requests
import logging
import json
from datetime import datetime
import psutil
import sqlite3
import argparse

# Load configuration from config.json
def load_config():
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        logger.error("Config file 'config.json' not found.")
        return {}

config = load_config()
TELEGRAM_TOKEN = config.get('telegram_token', '')
CHAT_ID = config.get('telegram_chat_id', '')
TIMEFRAME = config.get('timeframe', '1m')
BOT_NAME = config.get('bot_name', 'UnnamedBot')  # Default to 'UnnamedBot' if not specified

# Configure logging with bot name
logging.basicConfig(
    level=logging.INFO,
    format=f'[{BOT_NAME}] %(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Script Version
SCRIPT_VERSION = "2.7.8"  # Updated for database trade monitoring

# File paths
BOT_SCRIPT = 'binance_bot_ST_STRICT.py'
PID_FILE = 'bot.pid'
BOT_LOG = 'bot.log'
BOT_ERRORS_LOG = 'bot_errors.log'
LAST_PROCESSED_ID_FILE = 'last_processed_id.txt'

# Initialize last_start_time globally
last_start_time = None

def send_telegram_message(message):
    if not TELEGRAM_TOKEN or not CHAT_ID:
        logger.warning("Telegram credentials not provided. Cannot send message.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {'chat_id': CHAT_ID, 'text': f"[{BOT_NAME}] {message}"}
    try:
        response = requests.post(url, data=payload)
        response.raise_for_status()
        logger.info(f"Telegram message sent: {message}")
    except Exception as e:
        logger.error(f"Failed to send Telegram message: {e}")

def is_process_running(pid):
    try:
        if psutil.pid_exists(pid):
            process = psutil.Process(pid)
            if BOT_SCRIPT in ' '.join(process.cmdline()):
                return True
        return False
    except psutil.NoSuchProcess:
        return False
    except Exception as e:
        logger.error(f"Error checking process {pid}: {e}")
        try:
            os.kill(pid, 0)
            return True
        except OSError:
            return False

def kill_process(pid):
    try:
        if psutil.pid_exists(pid):
            process = psutil.Process(pid)
            process.terminate()
            time.sleep(1)
            if process.is_running():
                process.kill()
            logger.info(f"Successfully killed process with PID {pid}")
    except Exception as e:
        logger.error(f"Failed to kill process {pid}: {e}")

def parse_timeframe(timeframe):
    if timeframe.endswith('m'):
        minutes = int(timeframe[:-1])
        return minutes * 60
    else:
        logger.warning(f"Unsupported timeframe: {timeframe}. Assuming 60 seconds.")
        return 60

def get_last_log_timestamp(log_file, keyword):
    try:
        with open(log_file, 'r') as f:
            lines = f.readlines()
        for line in reversed(lines):
            if keyword in line:
                timestamp_str = line.split(' - ')[0].replace(f'[{BOT_NAME}] ', '')
                return datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S,%f')
    except Exception as e:
        logger.error(f"Error reading log file {log_file}: {e}")
    return None

def get_new_trades(last_processed_id):
    """Fetch new trades from the database with id > last_processed_id."""
    try:
        conn = sqlite3.connect('trade_history.db', timeout=10)
        conn.row_factory = sqlite3.Row  # Return rows as dictionaries
        c = conn.cursor()
        c.execute("""
            SELECT id, timestamp, side, entry_price, exit_price, stop_loss, profit_loss
            FROM trades
            WHERE id > ?
            ORDER BY id ASC
        """, (last_processed_id,))
        rows = c.fetchall()
        conn.close()
        return rows
    except sqlite3.Error as e:
        logger.error(f"Database error: {e}")
        return []

def start_bot(old_pid=None, force_first_trade=False):
    if old_pid:
        kill_process(old_pid)
    with open(BOT_ERRORS_LOG, 'a') as error_log:
        command = ['python', BOT_SCRIPT]
        if force_first_trade:
            command.append('--force-first-trade')
        process = subprocess.Popen(command, stderr=error_log)
    pid = process.pid
    with open(PID_FILE, 'w') as f:
        f.write(str(pid))
    global last_start_time
    last_start_time = datetime.now()
    logger.info(f"Started bot with PID {pid} at {last_start_time}")
    send_telegram_message(f"Bot started with PID {pid} at {last_start_time}")

def main(force_first_trade):
    global last_start_time
    logger.info("Watchdog started. Monitoring bot process...")
    send_telegram_message("Watchdog started. Monitoring bot process...")

    # Initialize last_processed_id
    if os.path.exists(LAST_PROCESSED_ID_FILE):
        with open(LAST_PROCESSED_ID_FILE, 'r') as f:
            try:
                last_processed_id = int(f.read().strip())
            except ValueError:
                last_processed_id = 0
    else:
        try:
            conn = sqlite3.connect('trade_history.db', timeout=10)
            c = conn.cursor()
            c.execute("SELECT MAX(id) FROM trades")
            max_id = c.fetchone()[0]
            conn.close()
            last_processed_id = max_id if max_id is not None else 0
        except sqlite3.Error as e:
            logger.error(f"Error querying database for max id: {e}")
            last_processed_id = 0
    logger.info(f"Initialized last_processed_id to {last_processed_id}")

    candle_interval = parse_timeframe(TIMEFRAME)
    buffer = 180  # 3 minutes buffer
    startup_grace_period = 60  # 60 seconds grace period

    while True:
        if os.path.exists(PID_FILE):
            with open(PID_FILE, 'r') as f:
                pid_str = f.read().strip()
            try:
                pid = int(pid_str)
                if not is_process_running(pid):
                    logger.warning(f"Bot process (PID {pid}) not found. Restarting...")
                    send_telegram_message(f"Bot process (PID {pid}) not found. Restarting...")
                    start_bot(pid, force_first_trade)
                else:
                    logger.info(f"Bot process (PID {pid}) is running.")
                    if last_start_time is None:
                        last_start_time = datetime.now()
                        logger.info(f"Initialized last_start_time to {last_start_time} for running process PID {pid}")
                    if (datetime.now() - last_start_time).total_seconds() > startup_grace_period:
                        last_candle_timestamp = get_last_log_timestamp(BOT_LOG, 'Bar Closed')
                        if last_candle_timestamp:
                            time_since_last_candle = (datetime.now() - last_candle_timestamp).total_seconds()
                            if time_since_last_candle > candle_interval + buffer:
                                logger.warning(f"Bot not processing candles for {time_since_last_candle:.0f} seconds (>{candle_interval + buffer} seconds). Restarting...")
                                send_telegram_message(f"Bot not processing candles for {time_since_last_candle:.0f} seconds. Restarting...")
                                start_bot(pid, force_first_trade)
                        else:
                            logger.warning("No 'Bar Closed' messages found in log after grace period. Restarting...")
                            send_telegram_message("No 'Bar Closed' messages found in log after grace period. Restarting...")
                            start_bot(pid, force_first_trade)

                    # Process new trades from database
                    try:
                        trades = get_new_trades(last_processed_id)
                        for trade in trades:
                            if trade['exit_price'] is None:
                                message = (
                                    f"New Trade Opened:\n"
                                    f"Side: {trade['side']}\n"
                                    f"Entry Price: {trade['entry_price']:.2f}\n"
                                    f"Stop Loss: {trade['stop_loss']:.2f}\n"
                                    f"Time: {trade['timestamp']}"
                                )
                            else:
                                message = (
                                    f"Trade Closed:\n"
                                    f"Side: {trade['side']}\n"
                                    f"Exit Price: {trade['exit_price']:.2f}\n"
                                    f"Profit/Loss: {trade['profit_loss']:.2f} USDT\n"
                                    f"Time: {trade['timestamp']}"
                                )
                            send_telegram_message(message)
                        if trades:
                            last_processed_id = trades[-1]['id']
                            with open(LAST_PROCESSED_ID_FILE, 'w') as f:
                                f.write(str(last_processed_id))
                    except Exception as e:
                        logger.error(f"Error processing trades from database: {e}")

            except ValueError:
                logger.error("Invalid PID in bot.pid. Starting bot...")
                send_telegram_message("Invalid PID in bot.pid. Starting bot...")
                start_bot(force_first_trade=force_first_trade)
        else:
            logger.info("bot.pid file not found. Starting bot...")
            send_telegram_message("bot.pid file not found. Starting bot...")
            start_bot(force_first_trade=force_first_trade)

        time.sleep(30)  # Check every 30 seconds

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Watchdog for Binance Futures Trading Bot")
    parser.add_argument('--fft', action='store_true', help="Force first trade when starting the bot")
    args = parser.parse_args()
    main(force_first_trade=args.fft)