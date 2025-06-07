import logging
import json
import websocket
import pandas as pd
import numpy as np
from binance.um_futures import UMFutures
import sqlite3
import time
import hmac
import hashlib
import base64
from datetime import datetime, timezone, timedelta
from colorama import init, Fore, Style
import os
from tenacity import retry, wait_exponential, stop_after_attempt
import redis
import threading
import random
import uuid
from okx.Trade import TradeAPI
from okx.Account import AccountAPI
from okx.PublicData import PublicAPI

# Initialize colorama for colored terminal output
init()

# Script Version
SCRIPT_VERSION = "2.0.13" #123

# Set up logging with dual handlers
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.getLogger('').handlers = []
file_handler = logging.FileHandler('bot_v2.log')
file_handler.setLevel(logging.DEBUG)
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)
logger.propagate = False

# Global DataFrame to store kline data
kline_data = pd.DataFrame(
    columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'],
    dtype='float64'
)
kline_data['timestamp'] = pd.to_datetime(kline_data['timestamp'])

# Trading state
current_position = None
trade_history = []
pending_orders = []
previous_st_line = None
previous_trend = None
latest_st_line = None
latest_trend = None
historical_data_fetched = False  # New flag
first_closed_candle = True  # New flag


# Load configuration from config.json
def load_config():
    """Load and parse configuration from config.json."""
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
        return {
            'binance_api_key': config.get('binance_api_key', ''),
            'binance_api_secret': config.get('binance_api_secret', ''),
            'okx_api_key': config.get('okx_api_key', ''),
            'okx_api_secret': config.get('okx_api_secret', ''),
            'okx_passphrase': config.get('okx_passphrase', ''),
            'atr_period': config.get('atr_period', 65),
            'atr_ratio': config.get('atr_ratio', 10.0),
            'position_size': config.get('position_size', 10.0),
            'trading_pair': config.get('trading_pair', 'BTCUSDT'),
            'timeframe': config.get('timeframe', '5m'),
            'stop_loss_offset': config.get('stop_loss_offset', 500.0),
            'telegram_token': config.get('telegram_token', ''),
            'telegram_chat_id': config.get('telegram_chat_id', ''),
            'bot_instance_id': config.get('bot_instance_id', 'bot1'),
            'orders_per_trade': config.get('orders_per_trade', 10),
            'orders_range': config.get('orders_range', '3-0.5'),
            'tp_levels': config.get('tp_levels', 3),
            'tp_percentages': config.get('tp_percentages', '1,2,3'),
            'st_line_shift_threshold': config.get('st_line_shift_threshold', 0.5)
        }
    except FileNotFoundError:
        logger.error(f"{Fore.RED}Config file 'config.json' not found. Using defaults.{Style.RESET_ALL}")
        return {
            'binance_api_key': '',
            'binance_api_secret': '',
            'okx_api_key': '',
            'okx_api_secret': '',
            'okx_passphrase': '',
            'atr_period': 65,
            'atr_ratio': 10.0,
            'position_size': 10.0,
            'trading_pair': 'BTCUSDT',
            'timeframe': '5m',
            'stop_loss_offset': 500.0,
            'telegram_token': '',
            'telegram_chat_id': '',
            'bot_instance_id': 'bot1',
            'orders_per_trade': 10,
            'orders_range': '3-0.5',
            'tp_levels': 3,
            'tp_percentages': '1,2,3',
            'st_line_shift_threshold': 0.5
        }


# Write PID to file and log startup
with open('bot_v2.pid', 'w') as f:
    pid = os.getpid()
    f.write(str(pid))
logger.info(f"{Fore.CYAN}Bot V2 started with PID {pid} at {datetime.now(timezone.utc)}{Style.RESET_ALL}")

# Load config and define constants
config = load_config()
BINANCE_API_KEY = config['binance_api_key']
BINANCE_API_SECRET = config['binance_api_secret']
OKX_API_KEY = config['okx_api_key']
OKX_API_SECRET = config['okx_api_secret']
OKX_PASSPHRASE = config['okx_passphrase']
ATR_PERIOD = config['atr_period']
ATR_RATIO = config['atr_ratio']
POSITION_SIZE = config['position_size']
TRADING_PAIR = config['trading_pair']
OKX_TRADING_PAIR = TRADING_PAIR.replace("USDT", "-USDT-SWAP")
TIMEFRAME = config['timeframe']
STOP_LOSS_OFFSET = config['stop_loss_offset']
ORDERS_PER_TRADE = config['orders_per_trade']
ORDERS_RANGE = [float(x) for x in config['orders_range'].split('-')]
TP_LEVELS = config['tp_levels']
TP_PERCENTAGES = [float(x) for x in config['tp_percentages'].split(',')]
ST_LINE_SHIFT_THRESHOLD = config['st_line_shift_threshold'] / 100
BOT_INSTANCE_ID = config['bot_instance_id']

# Initialize Redis client
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Initialize Binance client for kline fetching
binance_client = UMFutures(key=BINANCE_API_KEY, secret=BINANCE_API_SECRET, base_url="https://fapi.binance.com")

# Initialize OKX clients for trading
okx_trade_api = TradeAPI(
    api_key=OKX_API_KEY,
    api_secret_key=OKX_API_SECRET,
    passphrase=OKX_PASSPHRASE,
    flag="0",
    debug=True
)
okx_account_api = AccountAPI(
    api_key=OKX_API_KEY,
    api_secret_key=OKX_API_SECRET,
    passphrase=OKX_PASSPHRASE,
    flag="0",
    debug=True
)
okx_public_api = PublicAPI(
    api_key=OKX_API_KEY,
    api_secret_key=OKX_API_SECRET,
    passphrase=OKX_PASSPHRASE,
    flag="0",
    debug=False
)

# Global DataFrame to store kline data
kline_data = pd.DataFrame(
    columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'],
    dtype='float64'
)
kline_data['timestamp'] = pd.to_datetime(kline_data['timestamp'])

# Trading state
current_position = None
trade_history = []
pending_orders = []
previous_st_line = None
previous_trend = None
latest_st_line = None
latest_trend = None

# File paths
SYMBOL_CONFIG_FILE = 'symbol_configs.json'


# Database initialization
def init_db():
    """Initialize SQLite database with extended schema."""
    global current_position, pending_orders
    conn = sqlite3.connect('trade_history_v2.db', timeout=10)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    trading_pair TEXT,
                    timeframe TEXT,
                    side TEXT,
                    entry_price REAL,
                    size REAL,
                    exit_price REAL,
                    stop_loss REAL,
                    profit_loss REAL,
                    trend REAL,
                    order_id TEXT,
                    stop_loss_order_id TEXT,
                    position_id TEXT
                 )''')
    c.execute('''CREATE TABLE IF NOT EXISTS orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id INTEGER,
                    order_id TEXT,
                    side TEXT,
                    price REAL,
                    size REAL,
                    status TEXT,
                    timestamp TEXT,
                    position_id TEXT,
                    run_id INTEGER
                 )''')
    c.execute('''CREATE TABLE IF NOT EXISTS take_profits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id INTEGER,
                    tp_level INTEGER,
                    order_id TEXT,
                    price REAL,
                    size REAL,
                    status TEXT,
                    timestamp TEXT,
                    position_id TEXT,
                    run_id INTEGER
                 )''')
    c.execute('''CREATE TABLE IF NOT EXISTS errors (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    error_message TEXT,
                    context TEXT
                 )''')
    c.execute('''CREATE TABLE IF NOT EXISTS bot_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    start_time TEXT,
                    end_time TEXT
                 )''')
    start_time = str(datetime.now(timezone.utc))
    c.execute("INSERT INTO bot_runs (start_time) VALUES (?)", (start_time,))
    run_id = c.lastrowid
    conn.commit()

    # Clear stale orders
    c.execute("DELETE FROM orders WHERE status = 'pending'")
    c.execute("DELETE FROM take_profits")
    c.execute("UPDATE trades SET exit_price = 0 WHERE exit_price IS NULL")
    conn.commit()

    # State recovery
    symbol_config = load_symbol_config(OKX_TRADING_PAIR, okx_public_api)
    okx_position = sync_position_with_okx(okx_account_api, okx_trade_api, OKX_TRADING_PAIR)
    if okx_position:
        current_position = okx_position
        current_position['position_id'] = str(uuid.uuid4())
        logger.info(f"Synced position from OKX: Side: {current_position['side']}, Entry Price: {current_position['entry_price']:.2f}, Size: {current_position['size']} XRP")
        c.execute('''INSERT INTO trades (timestamp, trading_pair, timeframe, side, entry_price, size, stop_loss, stop_loss_order_id, order_id, position_id)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (current_position['open_time'], TRADING_PAIR, TIMEFRAME, current_position['side'],
                   current_position['entry_price'], current_position['size'], current_position.get('stop_loss'),
                   current_position.get('stop_loss_order_id'), current_position.get('order_id'), current_position['position_id']))
        # Set stop-loss if none exists
        if not current_position.get('stop_loss') and latest_st_line is not None and latest_trend is not None:
            stop_loss = latest_st_line - STOP_LOSS_OFFSET if latest_trend == 1 else latest_st_line + STOP_LOSS_OFFSET
            stop_loss_order_id = update_stop_loss(
                okx_trade_api, OKX_TRADING_PAIR, current_position['side'],
                adjust_price(stop_loss, symbol_config), None,
                current_position['entry_price'], current_position['size'], okx_public_api, is_new_position=True)
            if stop_loss_order_id:
                logger.info(f"Placed stop-loss order ID: {stop_loss_order_id} at {stop_loss}")
                current_position['stop_loss'] = stop_loss
                current_position['stop_loss_order_id'] = stop_loss_order_id
                c.execute("UPDATE trades SET stop_loss = ?, stop_loss_order_id = ? WHERE position_id = ?",
                          (stop_loss, stop_loss_order_id, current_position['position_id']))
        elif not current_position.get('stop_loss'):
            # Fallback: Use entry price minus/plus a percentage if no ST line
            stop_loss = current_position['entry_price'] * (1 - 0.05) if current_position['side'] == 'LONG' else current_position['entry_price'] * (1 + 0.05)
            stop_loss_order_id = update_stop_loss(
                okx_trade_api, OKX_TRADING_PAIR, current_position['side'],
                adjust_price(stop_loss, symbol_config), None,
                current_position['entry_price'], current_position['size'], okx_public_api, is_new_position=True)
            if stop_loss_order_id:
                logger.info(f"Placed fallback stop-loss order ID: {stop_loss_order_id} at {stop_loss}")
                current_position['stop_loss'] = stop_loss
                current_position['stop_loss_order_id'] = stop_loss_order_id
                c.execute("UPDATE trades SET stop_loss = ?, stop_loss_order_id = ? WHERE position_id = ?",
                          (stop_loss, stop_loss_order_id, current_position['position_id']))
        conn.commit()
    else:
        current_position = None
        logger.info("No active position found on OKX")

    # Sync pending orders
    orders = okx_trade_api.get_order_list(instType="SWAP", instId=OKX_TRADING_PAIR, ordType="limit")
    if orders['code'] == "0":
        pending_orders.clear()
        for order in orders['data']:
            if order['state'] in ['live', 'partially_filled']:
                position_id = current_position['position_id'] if current_position and order['side'] == ('buy' if current_position['side'] == 'LONG' else 'sell') else None
                c.execute('''INSERT INTO orders (trade_id, order_id, side, price, size, status, timestamp, position_id, run_id)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                          (1, order['ordId'], order['side'], float(order['px']), float(order['sz']) * symbol_config['contractSize'],
                           'pending', str(datetime.now(timezone.utc)), position_id, run_id))
                pending_orders.append({
                    'order_id': order['ordId'],
                    'side': order['side'],
                    'price': float(order['px']),
                    'size': float(order['sz']) * symbol_config['contractSize'],
                    'position_id': position_id
                })
        conn.commit()
        if pending_orders:
            logger.info(f"Recovered {len(pending_orders)} pending orders from OKX")

    # Sync filled orders only for open position
    if current_position:
        start_timestamp = int(datetime.fromisoformat(start_time.replace('+00:00', 'Z')).timestamp() * 1000)
        orders = okx_trade_api.get_orders_history(instType="SWAP", instId=OKX_TRADING_PAIR, ordType="limit", limit=100)
        if orders['code'] == "0":
            for order in orders['data']:
                try:
                    order_utime = int(order['uTime']) if order['uTime'] else 0
                    logger.debug(f"Checking order {order['ordId']}: uTime={order_utime}, start_timestamp={start_timestamp}")
                    if order['state'] == 'filled' and order_utime >= start_timestamp:
                        c.execute("SELECT * FROM orders WHERE order_id = ?", (order['ordId'],))
                        if not c.fetchone():
                            position_id = current_position['position_id'] if order['side'] == ('buy' if current_position['side'] == 'LONG' else 'sell') else None
                            if position_id:
                                c.execute('''INSERT INTO orders (trade_id, order_id, side, price, size, status, timestamp, position_id, run_id)
                                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                                          (1, order['ordId'], order['side'], float(order['avgPx']), float(order['accFillSz']) * symbol_config['contractSize'],
                                           'filled', str(datetime.now(timezone.utc)), position_id, run_id))
                                fill_data = {
                                    'order_id': order['ordId'],
                                    'symbol': order['instId'],
                                    'side': order['side'],
                                    'price': float(order['avgPx']),
                                    'size': float(order['accFillSz']) * symbol_config['contractSize'],
                                    'position_id': position_id
                                }
                                redis_client.rpush(f"bot_{BOT_INSTANCE_ID}_fill_queue", json.dumps(fill_data))
                                logger.info(f"Synced filled order from history: {fill_data}")
                except (ValueError, KeyError) as e:
                    logger.error(f"Failed to process order {order.get('ordId', 'unknown')}: {str(e)}")
                    log_error(str(e), "init_db_order_sync")
            conn.commit()
    else:
        logger.info("No open position, skipping historical order sync")
    return conn


# Log error to database
from tenacity import retry, wait_fixed, stop_after_attempt

@retry(wait=wait_fixed(0.5), stop=stop_after_attempt(10))
def log_error(error_message, context):
    """Log errors to the database in a thread-safe manner."""
    try:
        conn = sqlite3.connect('trade_history_v2.db', timeout=20)
        c = conn.cursor()
        c.execute('''INSERT INTO errors (timestamp, error_message, context)
                     VALUES (?, ?, ?)''',
                  (str(datetime.now(timezone.utc)), error_message, context))
        conn.commit()
        conn.close()
    except sqlite3.OperationalError as e:
        logger.error(f"Database error during log_error: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Failed to log error: {str(e)}")


# Load or fetch symbol configuration
def load_symbol_config(symbol, public_api):
    """Load symbol configuration from JSON or OKX API."""
    try:
        with open('symbol_configs.json', 'r') as f:
            content = f.read().strip()
            if not content:
                logger.error("symbol_configs.json is empty")
                raise ValueError("symbol_configs.json is empty")
            symbol_configs = json.loads(content)
            if symbol not in symbol_configs:
                logger.warning(f"Symbol {symbol} not found in symbol_configs.json, fetching from OKX API")
                return fetch_symbol_config_from_api(symbol, public_api)
            return symbol_configs[symbol]
    except FileNotFoundError:
        logger.warning("symbol_configs.json not found, fetching from OKX API")
        return fetch_symbol_config_from_api(symbol, public_api)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in symbol_configs.json: {str(e)}")
        raise ValueError(f"Invalid JSON in symbol_configs.json: {str(e)}")
    except Exception as e:
        logger.error(f"Error loading symbol config: {str(e)}")
        raise

def fetch_symbol_config_from_api(symbol, public_api):
    """Fetch symbol configuration from OKX API."""
    try:
        result = public_api.get_instruments(instType="SWAP", instId=symbol)
        if result['code'] != "0" or not result['data']:
            raise ValueError(f"Failed to fetch instrument data for {symbol}: {result['msg']}")
        instrument = result['data'][0]
        config = {
            'contractSize': float(instrument['ctVal']),
            'lotSize': float(instrument.get('lotSz', 1.0)),
            'quantityPrecision': int(instrument.get('szPrec', 0)),
            'minQty': float(instrument.get('minSz', 1.0)),
            'minNotional': float(instrument.get('minNotional', 5.0))
        }
        logger.info(f"Fetched config for {symbol}: {config}")
        return config
    except Exception as e:
        logger.error(f"Failed to fetch symbol config from API for {symbol}: {str(e)}")
        raise


# Adjust quantity to match OKX precision
def adjust_quantity(quantity, symbol_config, price):
    """Adjust quantity to match OKX precision and minimum size, return asset units."""
    contract_size = symbol_config['contractSize']  # e.g., 0.01 for XRP
    lot_size = symbol_config.get('lotSize', 1.0)
    precision = symbol_config['quantityPrecision']
    min_qty = symbol_config['minQty']
    min_notional = symbol_config['minNotional']
    min_qty_notional = max(min_qty, min_notional / price / contract_size)

    asset_size = quantity  # Input in asset units, e.g., 1000 XRP
    logger.debug(f"Initial asset size: {asset_size:.4f} units")

    contracts = asset_size / contract_size  # e.g., 1000 / 0.01 = 100,000 contracts
    logger.debug(f"Initial contracts: {contracts:.4f}")

    lots = contracts / lot_size
    rounded_lots = round(max(lots, min_qty_notional / lot_size))
    adjusted_contracts = rounded_lots * lot_size
    adjusted = round(adjusted_contracts, precision)

    adjusted = round(adjusted / lot_size) * lot_size
    if adjusted < min_qty:
        adjusted = min_qty
        logger.debug(f"Adjusted quantity increased to meet minQty: {adjusted} contracts")

    final_asset_size = adjusted * contract_size
    logger.debug(f"Adjusted quantity: {adjusted} contracts = {final_asset_size:.4f} asset units")
    return final_asset_size


# Adjust price to match OKX precision
def adjust_price(price, symbol_config):
    """Adjust price to match OKX precision."""
    precision = symbol_config['pricePrecision']
    adjusted = round(price, precision)
    logger.debug(f"Adjusted price: {adjusted}")
    return adjusted


# Sync position with OKX
@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5))
def sync_position_with_okx(account_api, trade_api, symbol):
    """Sync current position with OKX."""
    try:
        positions = account_api.get_positions(instType="SWAP", instId=symbol)
        if positions['code'] != "0":
            raise Exception(f"Failed to fetch positions: {positions['msg']}")
        position_data = positions['data']
        position = next((pos for pos in position_data if pos['instId'] == symbol and float(pos['pos']) != 0), None)
        if not position:
            logger.info(f"No open position found for {symbol}")
            return None

        side = 'LONG' if float(position['pos']) > 0 else 'SHORT'
        entry_price = float(position['avgPx'])
        symbol_config = load_symbol_config(symbol, okx_public_api)
        contract_size = symbol_config.get('contractSize', 0.01)
        size = abs(float(position['pos'])) * contract_size

        orders = trade_api.order_algos_list(instType="SWAP", instId=symbol, ordType="conditional")
        if orders['code'] != "0":
            raise Exception(f"Failed to fetch algo orders: {orders['msg']}")
        open_orders = orders['data']
        stop_loss_order = next(
            (order for order in open_orders
             if order.get('state') in ['live', 'effective'] and
             'slTriggerPx' in order and order['slTriggerPx'] and float(order['slTriggerPx']) != 0.0 and
             order.get('side') == ('sell' if side == 'LONG' else 'buy') and
             order.get('reduceOnly') == 'true' and order.get('closeFraction') == '1'),
            None
        )
        stop_loss = float(stop_loss_order['slTriggerPx']) if stop_loss_order else None
        stop_loss_order_id = stop_loss_order['algoId'] if stop_loss_order else None

        synced_position = {
            'side': side,
            'entry_price': entry_price,
            'size': size,
            'stop_loss': stop_loss,
            'stop_loss_order_id': stop_loss_order_id,
            'open_time': position.get('openTime', str(datetime.now(timezone.utc)))
        }
        logger.info(f"Synced position: {synced_position}")
        return synced_position
    except Exception as e:
        logger.error(f"Failed to sync position: {str(e)}")
        raise


# Cancel all stop-loss orders
@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5))
def cancel_all_stop_loss_orders(trade_api, symbol):
    """Cancel all stop-loss orders for a symbol."""
    try:
        orders = trade_api.order_algos_list(instType="SWAP", instId=symbol, ordType="conditional")
        if orders['code'] != "0":
            raise Exception(f"Failed to fetch algo orders: {orders['msg']}")
        open_orders = orders['data']
        stop_loss_orders = [order for order in open_orders if order['state'] == 'effective']
        if not stop_loss_orders:
            logger.debug(f"No stop-loss algo orders to cancel for {symbol}")
            return True

        for order in stop_loss_orders:
            cancel_result = trade_api.cancel_algo_order(instId=symbol, algoId=order['algoId'])
            if cancel_result['code'] != "0":
                raise Exception(f"Failed to cancel algo order: {cancel_result['msg']}")
            logger.debug(f"Canceled stop-loss algo order ID: {order['algoId']}")

        orders = trade_api.order_algos_list(instType="SWAP", instId=symbol, ordType="conditional")
        if orders['code'] != "0":
            raise Exception(f"Failed to fetch algo orders after cancellation: {orders['msg']}")
        remaining_orders = [order for order in orders['data'] if order['state'] == 'effective']
        if remaining_orders:
            logger.error(f"Failed to cancel all stop-loss orders. Remaining orders: {remaining_orders}")
            return False

        logger.debug(f"Successfully canceled all stop-loss orders for {symbol}")
        return True
    except Exception as e:
        logger.error(f"Failed to cancel stop-loss orders: {str(e)}")
        raise


# Update stop-loss order
@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5))
def update_stop_loss(trade_api, symbol, side, new_stop_price, current_stop_order_id, current_price, position_size,
                     public_api, is_new_position=False):
    """Update or place a new stop-loss order."""
    try:
        symbol_config = load_symbol_config(symbol, public_api)
        new_stop_price = adjust_price(new_stop_price, symbol_config)

        if side == 'LONG':
            if new_stop_price >= current_price:
                new_stop_price = current_price - 0.01
        elif side == 'SHORT':
            if new_stop_price <= current_price:
                new_stop_price = current_price + 0.01

        order_side = 'sell' if side == 'LONG' else 'buy'

        if current_stop_order_id:
            logger.debug(f"Modifying stop-loss order ID: {current_stop_order_id}")
            amend_params = {
                "instId": symbol,
                "algoId": current_stop_order_id,
                "newSlTriggerPx": str(new_stop_price)
            }
            amend_result = trade_api.amend_algo_order(**amend_params)
            if amend_result['code'] != "0":
                if "Position does not exist" in amend_result.get('msg', '') or amend_result.get('code') == "51169":
                    logger.warning(f"No position exists to amend stop-loss for {symbol}")
                    return None
                logger.error(f"Failed to amend stop-loss order: {amend_result}")
                if not cancel_all_stop_loss_orders(trade_api, symbol):
                    raise Exception("Failed to cancel stop-loss orders")
            else:
                logger.info(f"Modified stop-loss order ID: {current_stop_order_id} to {new_stop_price}")
                return current_stop_order_id

        if not is_new_position:
            position = sync_position_with_okx(okx_account_api, trade_api, symbol)
            if not position:
                logger.warning(f"No position exists to set stop-loss for {symbol}")
                return None

        params = {
            "instId": symbol,
            "tdMode": "isolated",
            "side": order_side,
            "ordType": "conditional",
            "slTriggerPx": str(new_stop_price),
            "slOrdPx": "-1",
            "slTriggerPxType": "mark",
            "reduceOnly": "true",
            "closeFraction": "1"
        }
        stop_order = trade_api.place_algo_order(**params)
        if stop_order['code'] != "0":
            logger.error(f"Failed to place stop-loss order: {stop_order.get('msg', '')}")
            raise Exception(f"Failed to place stop-loss order")
        algo_id = stop_order['data'][0]['algoId']
        logger.info(f"Placed stop-loss order ID: {algo_id} at {new_stop_price}")
        return algo_id
    except Exception as e:
        logger.error(f"Failed to update stop-loss: {str(e)}")
        raise


# Place multiple limit orders
def place_limit_orders(trend, st_line, conn, symbol_config):
    """Place multiple limit orders based on trend and ST_LINE."""
    global pending_orders
    min_range, max_range = ORDERS_RANGE[1] / 100, ORDERS_RANGE[0] / 100
    base_size = POSITION_SIZE  # Asset units, e.g., 1000 XRP

    try:
        db_conn = sqlite3.connect('trade_history_v2.db', timeout=20)
        c = db_conn.cursor()
        c.execute("SELECT id FROM bot_runs ORDER BY id DESC LIMIT 1")
        run_id = c.fetchone()[0]

        try:
            orders = okx_trade_api.get_order_list(instType="SWAP", instId=OKX_TRADING_PAIR, ordType="limit")
            if orders['code'] == "0":
                for order in orders['data']:
                    if order['state'] in ['live', 'partially_filled']:
                        okx_trade_api.cancel_order(instId=OKX_TRADING_PAIR, ordId=order['ordId'])
                        logger.debug(f"Canceled existing order ID: {order['ordId']}")
        except Exception as e:
            logger.error(f"Failed to cancel existing orders: {str(e)}")
            log_error(str(e), "place_limit_orders")

        c.execute("UPDATE orders SET status = 'cancelled' WHERE status = 'pending'")
        pending_orders = []

        if trend == 1:
            base_price = st_line * (1 + min_range)
            price_range = st_line * (max_range - min_range)
            side = 'buy'
        else:
            base_price = st_line * (1 - max_range)
            price_range = st_line * (max_range - min_range)
            side = 'sell'

        orders = []
        price = base_price
        size = base_size * (1 + random.uniform(-0.1, 0.1))  # Asset units, e.g., 900–1100 XRP
        price = adjust_price(price, symbol_config)
        asset_size = adjust_quantity(size, symbol_config, price)  # Returns asset units
        contract_size = asset_size / symbol_config['contractSize']  # Convert to contracts
        logger.debug(f"Preparing order: {side} at {price} with size {asset_size:.4f} asset units ({contract_size:.2f} contracts)")
        orders.append({
            'instId': OKX_TRADING_PAIR,
            'tdMode': 'isolated',
            'side': side,
            'ordType': 'limit',
            'px': str(price),
            'sz': str(contract_size)
        })
        pending_orders.append({'price': price, 'size': asset_size, 'side': side, 'position_id': None})

        batch_result = okx_trade_api.place_multiple_orders(orders)
        placed_count = 0
        if batch_result['code'] != "0":
            logger.error(f"Failed to place limit orders: {batch_result['msg']}")
            log_error(f"Failed to place limit orders: {batch_result['msg']}", "place_limit_orders")
            for order_data in batch_result['data']:
                if order_data['sCode'] != "0":
                    logger.warning(f"Order failed: {order_data['sMsg']} (Price: {order_data.get('px')}, Size: {order_data.get('sz')})")
                elif order_data['sCode'] == "0":
                    order_id = order_data['ordId']
                    c.execute('''INSERT INTO orders (trade_id, order_id, side, price, size, status, timestamp, position_id, run_id)
                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                              (1, order_id, side, float(order_data['px']), float(order_data['sz']) * symbol_config['contractSize'],
                               'pending', str(datetime.now(timezone.utc)), None, run_id))
                    placed_count += 1
            if placed_count == 0:
                pending_orders = []
                c.execute("DELETE FROM orders WHERE status = 'pending'")
            db_conn.commit()
            logger.info(f"Placed {placed_count} limit orders for trend {trend}")
            return

        c = db_conn.cursor()
        for order_data, order in zip(batch_result['data'], orders):
            if order_data['sCode'] == "0":
                order_id = order_data['ordId']
                c.execute('''INSERT INTO orders (trade_id, order_id, side, price, size, status, timestamp, position_id, run_id)
                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                          (1, order_id, order['side'], float(order_data['px']), float(order_data['sz']) * symbol_config['contractSize'],
                           'pending', str(datetime.now(timezone.utc)), None, run_id))
                placed_count += 1
            else:
                logger.warning(f"Order failed: {order_data['sMsg']} (Price: {order_data.get('px')}, Size: {order_data.get('sz')})")
                pending_orders = [o for o in pending_orders if o['price'] != float(order_data.get('px', 0))]
        db_conn.commit()
        logger.info(f"Placed {placed_count} limit orders for trend {trend}")
    except Exception as e:
        logger.error(f"Error in place_limit_orders: {str(e)}")
        log_error(str(e), "place_limit_orders")
    finally:
        if 'db_conn' in locals():
            db_conn.close()


# Place take-profit orders
def place_tp_orders(fill_data, conn, symbol_config):
    """Place take-profit orders for a filled order."""
    try:
        db_conn = sqlite3.connect('trade_history_v2.db', timeout=10)
        c = db_conn.cursor()
        c.execute("SELECT id FROM bot_runs ORDER BY id DESC LIMIT 1")
        run_id = c.fetchone()[0]

        fill_size = fill_data['size']  # Asset units, e.g., 104 XRP
        fill_price = fill_data['price']  # e.g., 2.1669 USDT/XRP
        side = 'sell' if fill_data['side'] == 'buy' else 'buy'  # Reduce-only opposite side
        position_id = fill_data.get('position_id')

        # Handle TP_PERCENTAGES as string or list
        if isinstance(TP_PERCENTAGES, str):
            tp_percentages = [float(p) for p in TP_PERCENTAGES.split(',')]
        else:
            tp_percentages = [float(p) for p in TP_PERCENTAGES]  # Already a list
        tp_size = fill_size / len(tp_percentages)  # e.g., 104 / 5 ≈ 20.8 XRP
        min_qty = symbol_config['minQty'] * symbol_config['contractSize']  # Min asset units
        min_notional = symbol_config['minNotional']  # Min USDT value

        orders = []
        for i, percentage in enumerate(tp_percentages):
            tp_price = fill_price * (1 + percentage / 100) if fill_data['side'] == 'buy' else fill_price * (1 - percentage / 100)
            tp_price = adjust_price(tp_price, symbol_config)
            tp_size_adjusted = adjust_quantity(tp_size, symbol_config, tp_price)  # Ensure compliance
            if tp_size_adjusted * tp_price < min_notional or tp_size_adjusted < min_qty:
                logger.warning(f"TP order size {tp_size_adjusted:.4f} at {tp_price} below minQty or minNotional, skipping")
                continue
            contract_size = tp_size_adjusted / symbol_config['contractSize']  # Convert to contracts
            logger.debug(f"Preparing TP order: {side} at {tp_price} with size {tp_size_adjusted:.4f} asset units ({contract_size:.2f} contracts)")
            orders.append({
                'instId': OKX_TRADING_PAIR,
                'tdMode': 'isolated',
                'side': side,
                'ordType': 'limit',
                'px': str(tp_price),
                'sz': str(contract_size),
                'reduceOnly': 'true'
            })

        if not orders:
            logger.warning("No valid TP orders to place")
            return

        batch_result = okx_trade_api.place_multiple_orders(orders)
        placed_count = 0
        if batch_result['code'] != "0":
            logger.error(f"Failed to place TP orders: {batch_result['msg']}")
            log_error(f"Failed to place TP orders: {batch_result['msg']}", "place_tp_orders")
            return

        c = db_conn.cursor()
        for order_data, order in zip(batch_result['data'], orders):
            if order_data['sCode'] != "0":
                logger.warning(f"TP order failed: {order_data['sMsg']} (Price: {order_data.get('px')}, Size: {order_data.get('sz')})")
                continue
            order_id = order_data['ordId']
            c.execute('''INSERT INTO take_profits (trade_id, tp_level, order_id, price, size, status, timestamp, position_id, run_id)
                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                      (1, placed_count + 1, order_id, float(order_data['px']), float(order_data['sz']) * symbol_config['contractSize'],
                       'pending', str(datetime.now(timezone.utc)), position_id, run_id))
            placed_count += 1
        db_conn.commit()
        if placed_count > 0:
            logger.info(f"Placed {placed_count} TP orders for filled order")
        else:
            logger.warning("No TP orders placed due to failures")
    except Exception as e:
        logger.error(f"Error in place_tp_orders: {str(e)}")
        log_error(str(e), "place_tp_orders")
    finally:
        if 'db_conn' in locals():
            db_conn.close()


# Refill position after TP hit
def refill_position(tp_order, st_line, trend, conn, symbol_config):
    """Place a single limit order to refill position after TP hit."""
    global pending_orders
    min_range, max_range = ORDERS_RANGE[1] / 100, ORDERS_RANGE[0] / 100
    if trend == 1:
        base_price = st_line * (1 + min_range)
        side = 'buy'
    else:
        base_price = st_line * (1 - max_range)
        side = 'sell'

    price = adjust_price(base_price, symbol_config)
    size = adjust_quantity(tp_order['size'], symbol_config, price)
    order = {
        'instId': OKX_TRADING_PAIR,
        'tdMode': 'isolated',
        'side': side,
        'ordType': 'limit',
        'px': str(price),
        'sz': str(size)
    }
    result = okx_trade_api.place_order(**order)
    if result['code'] != "0":
        logger.error(f"Failed to place refill order: {result['msg']}")
        log_error(conn, result['msg'], "refill_position")
        return

    order_id = result['data'][0]['ordId']
    c = conn.cursor()
    c.execute('''INSERT INTO orders (trade_id, order_id, side, price, size, status, timestamp)
                 VALUES (?, ?, ?, ?, ?, ?, ?)''',
              (1, order_id, side, price, float(size), 'pending', str(datetime.now(timezone.utc))))
    pending_orders.append({'order_id': order_id, 'side': side, 'price': price, 'size': float(size)})
    conn.commit()
    logger.info(f"Refilled position with order at {price} for {size}")


# WebSocket handler for order fills
def order_websocket_handler(conn):
    """Handle WebSocket for order fill events and push to Redis."""
    logger.info(f"Starting order WebSocket handler for {BOT_INSTANCE_ID}")
    symbol_config = load_symbol_config(OKX_TRADING_PAIR, okx_public_api)

    def on_open(ws):
        logger.info(f"{Fore.BLUE}Order WebSocket opened{Style.RESET_ALL}")
        timestamp = str(int(time.time()))
        sign = base64.b64encode(hmac.new(
            OKX_API_SECRET.encode('utf-8'),
            f"{timestamp}GET/users/self/verify".encode('utf-8'),
            hashlib.sha256
        ).digest()).decode('utf-8')
        login_msg = {
            "op": "login",
            "args": [{
                "apiKey": OKX_API_KEY,
                "passphrase": OKX_PASSPHRASE,
                "timestamp": timestamp,
                "sign": sign
            }]
        }
        ws.send(json.dumps(login_msg))
        time.sleep(1)
        subscription = {
            "op": "subscribe",
            "args": [{"channel": "orders", "instType": "SWAP", "instId": OKX_TRADING_PAIR}]
        }
        ws.send(json.dumps(subscription))
        logger.debug("Sent subscription request for orders channel")
        def ping():
            while True:
                try:
                    ws.send(json.dumps({"op": "ping"}))
                    logger.debug("Sent ping to OKX WebSocket")
                    time.sleep(15)
                except:
                    break
        threading.Thread(target=ping, daemon=True).start()

    def on_message(ws, message):
        try:
            logger.debug(f"Order WebSocket received: {message}")
            data = json.loads(message)
            if 'event' in data and data['event'] == 'login':
                logger.info(f"WebSocket login response: {data}")
            elif 'data' in data:
                for order in data['data']:
                    logger.debug(f"Processing order: {order}")
                    if order['state'] == 'filled':
                        conn_poll = sqlite3.connect('trade_history_v2.db', timeout=10)
                        c = conn_poll.cursor()
                        c.execute("SELECT position_id FROM orders WHERE order_id = ?", (order['ordId'],))
                        result = c.fetchone()
                        position_id = result[0] if result else None
                        fill_data = {
                            'order_id': order['ordId'],
                            'symbol': order['instId'],
                            'side': order['side'],
                            'price': float(order['avgPx']),
                            'size': float(order['accFillSz']) * symbol_config['contractSize'],
                            'position_id': position_id
                        }
                        redis_client.rpush(f"bot_{BOT_INSTANCE_ID}_fill_queue", json.dumps(fill_data))
                        logger.info(f"Pushed fill event to Redis: {fill_data}")
                        conn_poll.close()
                    elif order['state'] in ['canceled', 'rejected'] and order.get('reduceOnly', 'false') == 'true':
                        logger.warning(f"TP order {order['ordId']} {order['state']}: {order.get('sMsg', 'No message')}")
                        conn_poll = sqlite3.connect('trade_history_v2.db', timeout=10)
                        c = conn_poll.cursor()
                        c.execute("UPDATE take_profits SET status = ? WHERE order_id = ?", (order['state'], order['ordId']))
                        conn_poll.commit()
                        conn_poll.close()
                    elif order['state'] == 'filled' and order['ordType'] == 'limit' and order.get('reduceOnly', 'false') == 'true':
                        conn_poll = sqlite3.connect('trade_history_v2.db', timeout=10)
                        c = conn_poll.cursor()
                        c.execute("UPDATE take_profits SET status = 'filled' WHERE order_id = ?", (order['ordId'],))
                        conn_poll.commit()
                        conn_poll.close()
                        refill_position(
                            {'order_id': order['ordId'], 'size': float(order['accFillSz']) * symbol_config['contractSize']},
                            latest_st_line, latest_trend, conn, symbol_config
                        )
        except Exception as e:
            logger.error(f"Order WebSocket error: {str(e)}")
            log_error(str(e), "order_websocket")

    def on_error(ws, error):
        logger.error(f"Order WebSocket error: {str(error)}")
        log_error(str(error), "order_websocket")

    def on_close(ws, close_status_code, close_msg):
        logger.info(f"Order WebSocket closed: {close_status_code} - {close_msg}")
        reconnect(ws)

    def poll_orders():
        while True:
            try:
                conn_poll = sqlite3.connect('trade_history_v2.db', timeout=10)
                c = conn_poll.cursor()
                orders = okx_trade_api.get_orders_history(instType="SWAP", instId=OKX_TRADING_PAIR, ordType="limit", limit=100)
                if orders['code'] == "0":
                    for order in orders['data']:
                        if order['state'] == 'filled':
                            c.execute("SELECT position_id FROM orders WHERE order_id = ? AND status != 'filled'", (order['ordId'],))
                            result = c.fetchone()
                            if result:
                                position_id = result[0]
                                fill_data = {
                                    'order_id': order['ordId'],
                                    'symbol': order['instId'],
                                    'side': order['side'],
                                    'price': float(order['avgPx']),
                                    'size': float(order['accFillSz']) * symbol_config['contractSize'],
                                    'position_id': position_id
                                }
                                redis_client.rpush(f"bot_{BOT_INSTANCE_ID}_fill_queue", json.dumps(fill_data))
                                logger.info(f"Polled fill event: {fill_data}")
                conn_poll.commit()
                conn_poll.close()
                time.sleep(30)
            except Exception as e:
                logger.error(f"Order polling error: {str(e)}")
                log_error(str(e), "order_polling")
                time.sleep(30)

    threading.Thread(target=poll_orders, daemon=True).start()
    ws = websocket.WebSocketApp(
        "wss://ws.okx.com:8443/ws/v5/private",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()


# Process fill events from Redis
@retry(wait=wait_fixed(0.5), stop=stop_after_attempt(10))
def process_fill_events(symbol_config):
    """Process order fill events from Redis queue."""
    global current_position
    while True:
        try:
            event = redis_client.blpop(f"bot_{BOT_INSTANCE_ID}_fill_queue", timeout=0)
            fill_data = json.loads(event[1])
            conn = sqlite3.connect('trade_history_v2.db', timeout=20)
            c = conn.cursor()
            c.execute("SELECT * FROM orders WHERE order_id = ?", (fill_data['order_id'],))
            order = c.fetchone()
            if order and (fill_data.get('position_id') == (current_position.get('position_id') if current_position else None) or fill_data.get('position_id') is None):
                c.execute("UPDATE orders SET status = 'filled' WHERE order_id = ?", (fill_data['order_id'],))
                if not current_position or current_position['side'] != ('LONG' if fill_data['side'] == 'buy' else 'SHORT'):
                    position_id = fill_data.get('position_id') or str(uuid.uuid4())
                    current_position = {
                        'side': 'LONG' if fill_data['side'] == 'buy' else 'SHORT',
                        'entry_price': fill_data['price'],
                        'size': fill_data['size'],
                        'open_time': str(datetime.now(timezone.utc)),
                        'order_id': fill_data['order_id'],
                        'position_id': position_id
                    }
                    logger.info(f"Opened new position: {current_position['side']} at {current_position['entry_price']:.4f}, Size: {current_position['size']} XRP, Position ID: {position_id}")
                    c.execute('''INSERT INTO trades (timestamp, trading_pair, timeframe, side, entry_price, size, order_id, position_id)
                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                              (str(datetime.now(timezone.utc)), TRADING_PAIR, TIMEFRAME, current_position['side'],
                               current_position['entry_price'], current_position['size'], current_position['order_id'], position_id))
                    c.execute("UPDATE orders SET position_id = ? WHERE status = 'pending' AND side = ? AND position_id IS NULL",
                              (position_id, fill_data['side']))
                else:
                    old_size = current_position['size']
                    current_position['size'] += fill_data['size']
                    current_position['entry_price'] = (
                        (current_position['entry_price'] * old_size + fill_data['price'] * fill_data['size']) /
                        current_position['size']
                    )
                    logger.info(f"Updated position: {current_position['side']} at {current_position['entry_price']:.4f}, Size: {current_position['size']} XRP")
                    c.execute("UPDATE trades SET size = ?, entry_price = ? WHERE position_id = ? AND exit_price IS NULL",
                              (current_position['size'], current_position['entry_price'], current_position['position_id']))
                place_tp_orders(fill_data, conn, symbol_config)
                stop_loss = latest_st_line - STOP_LOSS_OFFSET if latest_trend == 1 else latest_st_line + STOP_LOSS_OFFSET
                if stop_loss:
                    stop_loss_order_id = update_stop_loss(
                        okx_trade_api, OKX_TRADING_PAIR, current_position['side'],
                        adjust_price(stop_loss, symbol_config), current_position.get('stop_loss_order_id'),
                        fill_data['price'], current_position['size'], okx_public_api)
                    if stop_loss_order_id:
                        logger.info(f"Placed stop-loss order ID: {stop_loss_order_id} at {stop_loss}")
                        current_position['stop_loss'] = stop_loss
                        current_position['stop_loss_order_id'] = stop_loss_order_id
                        c.execute("UPDATE trades SET stop_loss = ?, stop_loss_order_id = ? WHERE position_id = ?",
                                  (stop_loss, stop_loss_order_id, current_position['position_id']))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Error processing fill event: {str(e)}")
            log_error(str(e), "process_fill_events")


# Fetch historical kline data
@retry(wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_historical_data(symbol=TRADING_PAIR, interval=TIMEFRAME, limit=1000, end_time=None, num_batches=2):
    """Fetch historical kline data with retries."""
    global kline_data, historical_data_fetched
    try:
        logger.debug(f"Fetching historical data for {symbol}, interval={interval}, limit={limit}")
        mainnet_client = UMFutures(key=BINANCE_API_KEY, secret=BINANCE_API_SECRET, base_url="https://fapi.binance.com")
        all_data = pd.DataFrame()
        for batch in range(num_batches):
            params = {'symbol': symbol, 'interval': interval, 'limit': limit}
            if end_time:
                params['endTime'] = end_time
            logger.debug(f"Requesting klines: {params}")
            klines = mainnet_client.klines(**params, timeout=10)
            if not klines:
                logger.warning(f"{Fore.YELLOW}No historical data returned from API for batch {batch}.{Style.RESET_ALL}")
                break
            batch_data = pd.DataFrame({
                'timestamp': [int(k[0]) for k in klines],
                'open': [float(k[1]) for k in klines],
                'high': [float(k[2]) for k in klines],
                'low': [float(k[3]) for k in klines],
                'close': [float(k[4]) for k in klines],
                'volume': [float(k[5]) for k in klines]
            }, dtype='float64')
            batch_data['timestamp'] = pd.to_datetime(batch_data['timestamp'], unit='ms')
            all_data = pd.concat([batch_data, all_data], ignore_index=True)
            logger.debug(f"Fetched {len(batch_data)} candles in batch {batch}")
            if len(klines) < limit:
                break
            end_time = int(batch_data['timestamp'].iloc[0].timestamp() * 1000) - 1
        all_data.sort_values('timestamp', inplace=True)
        all_data.drop_duplicates(subset='timestamp', keep='last', inplace=True)
        kline_data = pd.concat([all_data, kline_data], ignore_index=True)
        kline_data = kline_data.tail(2000)
        historical_data_fetched = True
        logger.info(f"{Fore.CYAN}Fetched {len(all_data)} historical candles, total size: {len(kline_data)}{Style.RESET_ALL}")
    except Exception as e:
        logger.error(f"{Fore.RED}Error fetching historical data: {str(e)}{Style.RESET_ALL}")
        log_error(conn, str(e), "fetch_historical_data")
        raise

# Calculate ATR
def calculate_atr(df, period):
    """Calculate Average True Range."""
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift(1))
    low_close = np.abs(df['low'] - df['close'].shift(1))
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = pd.Series(np.nan, index=df.index)
    atr.iloc[period - 1] = true_range.iloc[:period].mean()
    for i in range(period, len(df)):
        atr.iloc[i] = (atr.iloc[i - 1] * (period - 1) + true_range.iloc[i]) / period
    return atr


# Calculate EMA
def calculate_ema(series, period):
    """Calculate Exponential Moving Average."""
    return series.ewm(span=period, adjust=False).mean()


# Calculate Supertrend
def calculate_supertrend(df, atr_period, atr_ratio):
    """Calculate Supertrend indicator."""
    atr = calculate_atr(df, atr_period)
    atr_smma = calculate_ema(atr, atr_period)
    delta_stop = atr_smma * atr_ratio
    up = df['close'] - delta_stop
    dn = df['close'] + delta_stop
    trend_up = pd.Series(0.0, index=df.index)
    trend_down = pd.Series(0.0, index=df.index)
    trend = pd.Series(0.0, index=df.index)

    if len(df) > 0:
        trend.iloc[0] = 1
        trend_up.iloc[0] = up.iloc[0]
        trend_down.iloc[0] = dn.iloc[0]
        epsilon = 1e-5
        for i in range(1, len(df)):
            if df['close'].iloc[i - 1] > trend_up.iloc[i - 1]:
                trend_up.iloc[i] = max(up.iloc[i], trend_up.iloc[i - 1])
            else:
                trend_up.iloc[i] = up.iloc[i]
            if df['close'].iloc[i - 1] < trend_down.iloc[i - 1]:
                trend_down.iloc[i] = min(dn.iloc[i], trend_down.iloc[i - 1])
            else:
                trend_down.iloc[i] = dn.iloc[i]
            line_st = pd.Series(np.where(trend.iloc[i - 1] == 1, trend_up, trend_down), index=df.index)
            if df['close'].iloc[i] > line_st.iloc[i] + epsilon:
                trend.iloc[i] = 1
            elif df['close'].iloc[i] < line_st.iloc[i] - epsilon:
                trend.iloc[i] = -1
            else:
                trend.iloc[i] = trend.iloc[i - 1]
        line_st = pd.Series(np.where(trend == 1, trend_up, trend_down), index=df.index)
    return line_st, trend


# WebSocket callback functions for candles
def on_open(ws):
    """Handle WebSocket opening."""
    logger.info(f"{Fore.CYAN}Candle WebSocket opened{Style.RESET_ALL}")
    subscription = {
        "method": "SUBSCRIBE",
        "params": [f"{TRADING_PAIR.lower()}@kline_{TIMEFRAME}"],
        "id": 1
    }
    ws.send(json.dumps(subscription))


def on_message(ws, message):
    """Handle WebSocket messages for candles."""
    global kline_data, previous_st_line, previous_trend, current_position, conn, latest_st_line, latest_trend, historical_data_fetched, first_closed_candle
    try:
        data = json.loads(message)
        if 'data' not in data or 'k' not in data['data']:
            logger.debug(f"Non-kline WebSocket message: {message}")
            return
        kline = data['data']['k']
        new_row = pd.DataFrame({
            'timestamp': [pd.to_datetime(int(kline['t']), unit='ms')],
            'open': [float(kline['o'])],
            'high': [float(kline['h'])],
            'low': [float(kline['l'])],
            'close': [float(kline['c'])],
            'volume': [float(kline['v'])]
        })

        if not kline_data.empty and kline_data['timestamp'].iloc[-1] == new_row['timestamp'].iloc[0]:
            kline_data.iloc[-1] = new_row.iloc[0]
        else:
            kline_data = pd.concat([kline_data, new_row], ignore_index=True)
        kline_data = kline_data.tail(2000)

        if kline['x']:
            if not historical_data_fetched:
                logger.info("Fetching historical data for first closed candle")
                fetch_historical_data(limit=1000, end_time=int(kline['t']) - 1, num_batches=2)
                if len(kline_data) < ATR_PERIOD:
                    logger.warning(f"{Fore.YELLOW}Insufficient data after fetch: {len(kline_data)} candles, need {ATR_PERIOD}{Style.RESET_ALL}")
                    return

            if len(kline_data) < ATR_PERIOD:
                logger.warning(f"{Fore.YELLOW}Insufficient data: {len(kline_data)} candles, need {ATR_PERIOD}{Style.RESET_ALL}")
                return

            df = kline_data.copy()
            df.set_index('timestamp', inplace=True)

            line_st, trend = calculate_supertrend(df, ATR_PERIOD, ATR_RATIO)
            latest_st_line = line_st.iloc[-1]
            latest_trend = trend.iloc[-1]
            latest_close = df['close'].iloc[-1]

            logger.info(f"Close: {latest_close:.10f}, ST_LINE: {latest_st_line:.10f}, Trend: {latest_trend}")

            if previous_trend is not None and latest_trend != previous_trend:
                logger.info(f"Trend flipped from {previous_trend} to {latest_trend}")

            symbol_config = load_symbol_config(OKX_TRADING_PAIR, okx_public_api)
            stop_loss = latest_st_line - STOP_LOSS_OFFSET if latest_trend == 1 else latest_st_line + STOP_LOSS_OFFSET

            st_line_shift = abs((latest_st_line - previous_st_line) / previous_st_line) if previous_st_line else 0
            logger.debug(f"Order placement check: first_candle={first_closed_candle}, trend_changed={previous_trend != latest_trend}, st_line_shift={st_line_shift:.4%}, has_position={bool(current_position)}, has_orders={bool(pending_orders)}")
            if (first_closed_candle and not current_position and not pending_orders) or (previous_trend != latest_trend or st_line_shift > ST_LINE_SHIFT_THRESHOLD):
                logger.info(f"Placing limit orders: Trend={latest_trend}, ST_LINE={latest_st_line:.2f}")
                place_limit_orders(latest_trend, latest_st_line, conn, symbol_config)

            if current_position:
                okx_position = sync_position_with_okx(okx_account_api, okx_trade_api, OKX_TRADING_PAIR)
                if okx_position:
                    current_position['stop_loss'] = okx_position.get('stop_loss')
                    current_position['stop_loss_order_id'] = okx_position.get('stop_loss_order_id')
                stop_loss_order_id = update_stop_loss(
                    okx_trade_api, OKX_TRADING_PAIR, current_position['side'],
                    adjust_price(stop_loss, symbol_config), current_position.get('stop_loss_order_id'),
                    latest_close, current_position['size'], okx_public_api)
                if stop_loss_order_id:
                    current_position['stop_loss'] = stop_loss
                    current_position['stop_loss_order_id'] = stop_loss_order_id
                    c = conn.cursor()
                    c.execute("UPDATE trades SET stop_loss = ?, stop_loss_order_id = ? WHERE position_id = ?",
                              (stop_loss, stop_loss_order_id, current_position['position_id']))
                    conn.commit()
                if (current_position['side'] == 'LONG' and latest_close <= current_position['stop_loss']) or \
                   (current_position['side'] == 'SHORT' and latest_close >= current_position['stop_loss']):
                    cancel_all_stop_loss_orders(okx_trade_api, OKX_TRADING_PAIR)
                    close_result = okx_trade_api.close_positions(
                        instId=OKX_TRADING_PAIR,
                        mgnMode="isolated",
                        posSide="net"
                    )
                    if close_result['code'] != "0":
                        close_quantity = adjust_quantity(current_position['size'], symbol_config, latest_close)
                        close_side = 'sell' if current_position['side'] == 'LONG' else 'buy'
                        close_result = okx_trade_api.place_order(
                            instId=OKX_TRADING_PAIR,
                            tdMode="isolated",
                            side=close_side,
                            ordType="market",
                            sz=str(close_quantity / symbol_config['contractSize']),
                            reduceOnly="true"
                        )
                        if close_result['code'] != "0":
                            logger.error(f"Failed to close position: {close_result['msg']}")
                            log_error(f"Failed to close position: {close_result['msg']}", "stop_loss_trigger")
                            raise Exception(f"Failed to close position")
                    ord_id = close_result['data'][0].get('ordId', 'unknown') if close_result['code'] == "0" and 'data' in close_result else 'unknown'
                    trade = {
                        'timestamp': str(datetime.now(timezone.utc)),
                        'trading_pair': TRADING_PAIR,
                        'timeframe': TIMEFRAME,
                        'side': current_position['side'],
                        'entry_price': current_position['entry_price'],
                        'size': current_position['size'],
                        'exit_price': latest_close,
                        'stop_loss': current_position.get('stop_loss'),
                        'profit_loss': (latest_close - current_position['entry_price']) * current_position['size'] if current_position['side'] == 'LONG' else (current_position['entry_price'] - latest_close) * current_position['size'],
                        'trend': latest_trend,
                        'order_id': ord_id,
                        'stop_loss_order_id': current_position.get('stop_loss_order_id'),
                        'position_id': current_position['position_id']
                    }
                    trade_history.append(trade)
                    c = conn.cursor()
                    c.execute('''INSERT INTO trades (timestamp, trading_pair, timeframe, side, entry_price, size, exit_price, stop_loss, profit_loss, trend, order_id, stop_loss_order_id, position_id)
                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                              (trade['timestamp'], trade['trading_pair'], trade['timeframe'], trade['side'],
                               trade['entry_price'], trade['size'], trade['exit_price'], trade['stop_loss'],
                               trade['profit_loss'], trade['trend'], trade['order_id'], trade['stop_loss_order_id'], trade['position_id']))
                    conn.commit()
                    logger.info(f"Stop-loss triggered: Closed {trade['side']} at {latest_close:.2f}, P/L: {trade['profit_loss']:.2f} USDT")
                    current_position = None

            first_closed_candle = False
            previous_st_line = latest_st_line
            previous_trend = latest_trend

    except Exception as e:
        logger.error(f"{Fore.RED}Candle WebSocket error: {str(e)} (Message: {message}){Style.RESET_ALL}")
        log_error(f"Candle WebSocket error: {str(e)}", "on_message")


def on_error(ws, error):
    """Handle WebSocket errors."""
    logger.error(f"{Fore.RED}Candle WebSocket error: {str(error)}{Style.RESET_ALL}")
    log_error(conn, str(error), "on_error")


def on_close(ws, close_status_code, close_msg):
    """Handle WebSocket closing."""
    logger.info(f"{Fore.CYAN}Candle WebSocket closed: {close_status_code} - {close_msg}{Style.RESET_ALL}")
    reconnect(ws)


def reconnect(ws):
    """Reconnect WebSocket on failure."""
    delay = 5
    while True:
        try:
            ws.close()
            logger.info(f"{Fore.YELLOW}Reconnecting in {delay} seconds...{Style.RESET_ALL}")
            time.sleep(delay)
            ws.run_forever()
            logger.info(f"{Fore.GREEN}WebSocket reconnected successfully{Style.RESET_ALL}")
            break
        except Exception as e:
            logger.error(f"{Fore.RED}Reconnection failed: {str(e)}{Style.RESET_ALL}")
            delay = min(delay * 2, 60)


def main():
    """Main function to start the trading bot."""
    global conn, latest_st_line, latest_trend
    conn = init_db()

    # Start order WebSocket handler
    order_thread = threading.Thread(target=order_websocket_handler, args=(conn,))
    order_thread.start()

    # Start fill event processor
    symbol_config = load_symbol_config(OKX_TRADING_PAIR, okx_public_api)
    fill_thread = threading.Thread(target=process_fill_events, args=(symbol_config,))
    fill_thread.start()

    # Start Binance WebSocket for candles
    websocket_url = "wss://fstream.binance.com/stream"
    ws = websocket.WebSocketApp(
        websocket_url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    logger.warning(f"{Fore.YELLOW}Starting LIVE trading on OKX Futures V2! {SCRIPT_VERSION}{Style.RESET_ALL}")
    try:
        ws.run_forever()
    finally:
        c = conn.cursor()
        c.execute("UPDATE bot_runs SET end_time = ? WHERE end_time IS NULL", (str(datetime.now(timezone.utc)),))
        conn.commit()
        conn.close()


if __name__ == "__main__":
    main()