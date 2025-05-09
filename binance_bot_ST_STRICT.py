import logging
import json
import websocket
import pandas as pd
import time
import numpy as np
from binance.um_futures import UMFutures
import sqlite3
from datetime import datetime, timezone
from colorama import init, Fore, Style
import argparse
import os
from tenacity import retry, wait_exponential, stop_after_attempt
import requests
import hmac
import hashlib

# Initialize colorama for colored terminal output
init()

# Script Version
SCRIPT_VERSION = "2.8.4"  # Updated to 2.8.4 for immediate stop-loss setting after position open

# Set up logging with dual handlers
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logging.getLogger('').handlers = []
file_handler = logging.FileHandler('bot.log')
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

# Load configuration from config.json
def load_config():
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
        return {
            'api_key': config.get('binance_api_key', 'your_api_key'),
            'api_secret': config.get('binance_api_secret', 'your_api_secret'),
            'atr_period': config.get('atr_period', 7),
            'atr_ratio': config.get('atr_ratio', 9.0),
            'position_size': config.get('position_size', 0.1),
            'trading_pair': config.get('trading_pair', 'ETHUSDT'),
            'timeframe': config.get('timeframe', '1m'),
            'stop_loss_offset': config.get('stop_loss_offset', 100.0),
            'telegram_token': config.get('telegram_token', ''),
            'telegram_chat_id': config.get('telegram_chat_id', ''),
            'bot_instance': config.get('bot_instance', 'bot1')
        }
    except FileNotFoundError:
        logger.error(f"{Fore.RED}Config file 'config.json' not found. Using defaults.{Style.RESET_ALL}")
        return {
            'api_key': 'your_api_key',
            'api_secret': 'your_api_secret',
            'atr_period': 7,
            'atr_ratio': 9.0,
            'position_size': 0.1,
            'trading_pair': 'ETHUSDT',
            'timeframe': '1m',
            'stop_loss_offset': 100.0,
            'telegram_token': '',
            'telegram_chat_id': '',
            'bot_instance': 'bot1'
        }

# Write PID to file and log startup
with open('bot.pid', 'w') as f:
    pid = os.getpid()
    f.write(str(pid))
logger.info(f"{Fore.CYAN}Bot started with PID {pid} at {datetime.now(timezone.utc)}{Style.RESET_ALL}")

# Load config and define constants
config = load_config()
API_KEY = config['api_key']
API_SECRET = config['api_secret']
ATR_PERIOD = config['atr_period']
ATR_RATIO = config['atr_ratio']
POSITION_SIZE = config['position_size']
TRADING_PAIR = config['trading_pair']
TIMEFRAME = config['timeframe']
STOP_LOSS_OFFSET = config['stop_loss_offset']

# Initialize Futures client
client = UMFutures(key=API_KEY, secret=API_SECRET, base_url="https://fapi.binance.com")

# Global DataFrame to store kline data with explicit dtypes
kline_data = pd.DataFrame(
    columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'],
    dtype='float64'
)
kline_data['timestamp'] = pd.to_datetime(kline_data['timestamp'])

# Flags and variables for warmup
first_closed_candle_received = False
first_closed_timestamp = None
closed_candle_count = 0
historical_data_fetched = False
previous_trend = None
force_first_trade = False

# Trading state
current_position = None
trade_history = []

# File paths
SYMBOL_CONFIG_FILE = 'symbol_configs.json'

# Load or fetch symbol configuration
def load_symbol_config(symbol, client):
    symbol_configs = {}
    if os.path.exists(SYMBOL_CONFIG_FILE):
        with open(SYMBOL_CONFIG_FILE, 'r') as f:
            symbol_configs = json.load(f)

    if symbol not in symbol_configs:
        try:
            exchange_info = client.exchange_info()
            for s in exchange_info['symbols']:
                if s['symbol'] == symbol:
                    filters = {f['filterType']: f for f in s['filters']}
                    symbol_configs[symbol] = {
                        'quantityPrecision': s['quantityPrecision'],
                        'pricePrecision': s['pricePrecision'],
                        'minQty': float(filters['LOT_SIZE']['minQty']),
                        'minNotional': float(filters.get('NOTIONAL', {'minNotional': '5.0'})['minNotional'])
                    }
                    break
            with open(SYMBOL_CONFIG_FILE, 'w') as f:
                json.dump(symbol_configs, f, indent=4)
        except Exception as e:
            logger.error(
                f"{Fore.RED}Failed to fetch symbol info for {symbol}: {str(e)}. Using defaults.{Style.RESET_ALL}")
            symbol_configs[symbol] = {'quantityPrecision': 3, 'pricePrecision': 2, 'minQty': 0.001, 'minNotional': 5.0}
    return symbol_configs[symbol]

# Adjust quantity to match Binance precision and minimum size
def adjust_quantity(quantity, symbol_config, price):
    precision = symbol_config['quantityPrecision']
    min_qty = symbol_config['minQty']
    min_notional = symbol_config['minNotional']
    min_qty_notional = max(min_qty, min_notional / price)
    adjusted = max(round(quantity, precision), min_qty_notional)
    return adjusted

# Adjust price to match Binance precision
def adjust_price(price, symbol_config):
    precision = symbol_config['pricePrecision']
    return round(price, precision)

# Function to generate Binance API signature
def generate_signature(query_string, api_secret):
    return hmac.new(api_secret.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

# Function to fetch all open orders via /fapi/v1/openOrders
@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5))
def fetch_open_orders(symbol, api_key, api_secret):
    base_url = "https://fapi.binance.com"
    endpoint = "/fapi/v1/openOrders"
    timestamp = int(time.time() * 1000)
    params = f"symbol={symbol}&timestamp={timestamp}&recvWindow=10000"  # Added recvWindow for robustness
    signature = generate_signature(params, api_secret)
    url = f"{base_url}{endpoint}?{params}&signature={signature}"
    headers = {"X-MBX-APIKEY": api_key}
    response = requests.get(url, headers=headers, timeout=10)
    response.raise_for_status()
    return response.json()

# Sync position with Binance with retry logic
@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5))
def sync_position_with_binance(client, symbol):
    try:
        logger.debug("Step 1: Fetching position risk from Binance")
        positions = client.get_position_risk(symbol=symbol, recvWindow=10000)  # Added recvWindow
        logger.debug(f"Step 2: Position risk response: {positions}")
        position = next((pos for pos in positions if pos['symbol'] == symbol and float(pos['positionAmt']) != 0), None)
        logger.debug(f"Step 3: Selected position: {position}")
        if not position:
            logger.info(f"No open position found for {symbol}")
            return None

        logger.debug("Step 4: Extracting position details")
        side = 'LONG' if float(position['positionAmt']) > 0 else 'SHORT'
        entry_price = float(position['entryPrice'])
        size = abs(float(position['positionAmt']))

        logger.debug("Step 5: Fetching all open orders from Binance via /fapi/v1/openOrders")
        orders = fetch_open_orders(symbol, API_KEY, API_SECRET)
        logger.debug(f"Step 6: Open orders response: {orders}")

        stop_loss_order = next(
            (order for order in orders
             if order['type'] == 'STOP_MARKET' and
             order.get('reduceOnly', False) and
             order.get('closePosition', False) and
             ((side == 'LONG' and order['side'] == 'SELL') or (side == 'SHORT' and order['side'] == 'BUY'))),
            None
        )
        logger.debug(f"Step 7: Stop-loss order: {stop_loss_order}")
        stop_loss = float(stop_loss_order['stopPrice']) if stop_loss_order else None
        stop_loss_order_id = stop_loss_order['orderId'] if stop_loss_order else None

        synced_position = {
            'side': side,
            'entry_price': entry_price,
            'size': size,
            'stop_loss': stop_loss,
            'stop_loss_order_id': stop_loss_order_id,
            'open_time': str(datetime.now(timezone.utc))
        }

        logger.info(f"Synced position: {synced_position}")
        return synced_position
    except Exception as e:
        logger.error(f"Failed to sync position: {str(e)}", exc_info=True)
        raise

# Cancel all open stop-loss orders with retry logic
@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5))
def cancel_all_stop_loss_orders(client, symbol):
    try:
        orders = fetch_open_orders(symbol, API_KEY, API_SECRET)
        stop_loss_orders = [order for order in orders if order['type'] == 'STOP_MARKET']
        if not stop_loss_orders:
            logger.debug(f"No stop-loss orders to cancel for {symbol}")
            return True
        for order in stop_loss_orders:
            client.cancel_order(symbol=symbol, orderId=order['orderId'])
            logger.debug(f"Canceled stop-loss order ID: {order['orderId']}")
        return True
    except Exception as e:
        logger.error(f"Failed to cancel stop-loss orders: {str(e)}")
        raise

# Update stop-loss order on Binance with retry logic
@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5))
def update_stop_loss(client, symbol, side, new_stop_price, current_stop_order_id, current_price, is_new_position=False):
    try:
        symbol_config = load_symbol_config(symbol, client)
        new_stop_price = adjust_price(new_stop_price, symbol_config)

        if side == 'LONG' and new_stop_price >= current_price:
            new_stop_price = current_price - 0.01
        elif side == 'SHORT' and new_stop_price <= current_price:
            new_stop_price = current_price + 0.01

        if current_stop_order_id:
            try:
                cancel_result = client.cancel_order(symbol=symbol, orderId=current_stop_order_id)
                logger.debug(f"Cancel stop-loss order response: {cancel_result}")
            except Exception as e:
                if "Unknown order sent" in str(e):
                    logger.warning(f"Stop-loss order ID {current_stop_order_id} not found on Binance. Proceeding to set new stop-loss.")
                else:
                    logger.error(f"Failed to cancel stop-loss order: {str(e)}")
                    if not cancel_all_stop_loss_orders(client, symbol):
                        raise Exception("Failed to cancel existing stop-loss orders")

        # If this is a new position, we assume the position exists since it was just opened
        if not is_new_position:
            # For existing positions, verify the position still exists
            position = sync_position_with_binance(client, symbol)
            if not position:
                logger.warning(f"No position exists to set stop-loss for {symbol}.")
                return None

        stop_order = client.new_order(
            symbol=symbol,
            side='SELL' if side == 'LONG' else 'BUY',
            type='STOP_MARKET',
            stopPrice=new_stop_price,
            closePosition=True,
            timeInForce='GTE_GTC',
            workingType='MARK_PRICE',
            positionSide='BOTH'
        )
        logger.info(f"Placed new stop-loss order ID: {stop_order['orderId']} at {new_stop_price}")
        return stop_order['orderId']
    except Exception as e:
        if "Position does not exist" in str(e):
            logger.warning(f"No position exists to set stop-loss for {symbol}.")
            return None
        logger.error(f"Failed to update stop-loss: {str(e)}")
        raise

# Initialize SQLite database
def init_db():
    global current_position
    conn = sqlite3.connect('trade_history.db')
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
                    trend INTEGER,
                    order_id INTEGER,
                    stop_loss_order_id INTEGER
                 )''')
    conn.commit()

    # Fetch the current position from Binance
    binance_position = sync_position_with_binance(client, TRADING_PAIR)
    logger.debug(f"Binance position on init: {binance_position}")

    if binance_position:
        # Look for an active trade with the same order_id
        c.execute("SELECT * FROM trades WHERE trading_pair = ? AND order_id = ? AND exit_price IS NULL",
                 (TRADING_PAIR, binance_position.get('order_id')))
        existing_trade = c.fetchone()

        if existing_trade:
            # Update the existing trade with the current position details
            logger.info(f"Found existing active trade with order_id={binance_position['order_id']}. Updating details.")
            c.execute("""
                UPDATE trades
                SET timestamp = ?, side = ?, entry_price = ?, size = ?, stop_loss = ?, stop_loss_order_id = ?
                WHERE id = ?
            """, (
                binance_position['open_time'],
                binance_position['side'],
                binance_position['entry_price'],
                binance_position['size'],
                binance_position.get('stop_loss'),
                binance_position.get('stop_loss_order_id'),
                existing_trade[0]  # id
            ))
            conn.commit()
            current_position = binance_position
            logger.info(f"Updated active trade: Side: {current_position['side']}, Entry Price: {current_position['entry_price']:.2f}")
        else:
            # No matching active trade; clear any stale active trades and insert the new one
            c.execute("UPDATE trades SET exit_price = 0 WHERE trading_pair = ? AND exit_price IS NULL", (TRADING_PAIR,))
            if c.rowcount > 0:
                logger.info(f"Closed {c.rowcount} stale active trades in database for {TRADING_PAIR}")
            c.execute(
                "INSERT INTO trades (timestamp, trading_pair, timeframe, side, entry_price, size, stop_loss, stop_loss_order_id, order_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (binance_position['open_time'], TRADING_PAIR, TIMEFRAME, binance_position['side'],
                 binance_position['entry_price'], binance_position['size'], binance_position.get('stop_loss'),
                 binance_position.get('stop_loss_order_id'), binance_position.get('order_id'))
            )
            conn.commit()
            current_position = binance_position
            logger.info(f"Inserted new active trade: Side: {current_position['side']}, Entry Price: {current_position['entry_price']:.2f}")
    else:
        # No position on Binance; clear any active trades and set current_position to None
        c.execute("UPDATE trades SET exit_price = 0 WHERE trading_pair = ? AND exit_price IS NULL", (TRADING_PAIR,))
        if c.rowcount > 0:
            logger.info(f"Closed {c.rowcount} stale active trades in database for {TRADING_PAIR} (no position on Binance)")
        conn.commit()
        current_position = None
        logger.info("No active position found on Binance or in database")

    return conn

# Helper function to write current_position to the database
def write_position_to_db(conn, position):
    if not position:
        logger.debug("No position to write to database")
        return
    c = conn.cursor()
    # Check if a trade with this order_id already exists and is active
    c.execute("SELECT * FROM trades WHERE order_id = ? AND exit_price IS NULL", (position.get('order_id'),))
    existing_trade = c.fetchone()
    if existing_trade:
        logger.debug(f"Active trade with order_id={position['order_id']} already exists in database. Updating.")
        c.execute("""
            UPDATE trades
            SET timestamp = ?, side = ?, entry_price = ?, size = ?, stop_loss = ?, stop_loss_order_id = ?
            WHERE id = ?
        """, (
            position['open_time'],
            position['side'],
            position['entry_price'],
            position['size'],
            position.get('stop_loss'),
            position.get('stop_loss_order_id'),
            existing_trade[0]  # id
        ))
    else:
        # Check for stale active trades and close them
        c.execute("UPDATE trades SET exit_price = 0 WHERE trading_pair = ? AND exit_price IS NULL", (TRADING_PAIR,))
        if c.rowcount > 0:
            logger.info(f"Closed {c.rowcount} stale active trades before inserting new position")
        # Insert the new position
        c.execute(
            "INSERT INTO trades (timestamp, trading_pair, timeframe, side, entry_price, size, stop_loss, stop_loss_order_id, order_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (position['open_time'], TRADING_PAIR, TIMEFRAME, position['side'],
             position['entry_price'], position['size'], position.get('stop_loss'),
             position.get('stop_loss_order_id'), position.get('order_id'))
        )
    conn.commit()
    logger.debug(f"Wrote position to database: {position}")

# Log trade to database
def log_trade(conn, trade):
    c = conn.cursor()
    c.execute('''INSERT INTO trades (timestamp, trading_pair, timeframe, side, entry_price, size, exit_price, stop_loss, profit_loss, trend, order_id, stop_loss_order_id)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
              (trade['timestamp'], trade['trading_pair'], trade['timeframe'], trade['side'], trade['entry_price'],
               trade['size'], trade.get('exit_price'), trade.get('stop_loss'), trade.get('profit_loss'),
               trade.get('trend'), trade.get('order_id'), trade.get('stop_loss_order_id')))
    conn.commit()

# Display trade summary
def display_trade_summary(position, latest_close, line_st):
    if position:
        profit_loss = (latest_close - position['entry_price']) * position['size'] if position['side'] == 'LONG' else (
            position['entry_price'] - latest_close) * position['size']
        stop_loss_display = f"{position['stop_loss']:.2f}" if position.get('stop_loss') is not None else 'None'
        logger.info(
            f"{Fore.CYAN}Open Trade Summary - Side: {position['side']}, Open Time: {position['open_time']}, Entry Price: {position['entry_price']:.2f}, "
            f"Size: {position['size']:.4f}, Current Price: {latest_close:.4f}, Profit/Loss: {profit_loss:.2f} USDT, "
            f"ST_LINE: {line_st:.4f}, Stop Loss: {stop_loss_display}{Style.RESET_ALL}"
        )
    else:
        logger.info(f"{Fore.CYAN}No active position.{Style.RESET_ALL}")

# Fetch historical kline data with retry and timeout
@retry(wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_historical_data(symbol=TRADING_PAIR, interval=TIMEFRAME, limit=1000, end_time=None, num_batches=2):
    global kline_data, historical_data_fetched
    try:
        mainnet_client = UMFutures(key=API_KEY, secret=API_SECRET, base_url="https://fapi.binance.com")
        all_data = pd.DataFrame()
        for _ in range(num_batches):
            params = {'symbol': symbol, 'interval': interval, 'limit': limit}
            if end_time:
                params['endTime'] = end_time
            klines = mainnet_client.klines(**params, timeout=10)
            if not klines:
                logger.warning(f"{Fore.YELLOW}No historical data returned from API.{Style.RESET_ALL}")
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
            if len(klines) < limit:
                break  # No more data to fetch
            end_time = int(batch_data['timestamp'].iloc[0].timestamp() * 1000) - 1  # Corrected line
        all_data.sort_values('timestamp', inplace=True)
        all_data.drop_duplicates(subset='timestamp', keep='last', inplace=True)
        prev_rows = len(kline_data)
        kline_data = pd.concat([all_data, kline_data], ignore_index=True)
        kline_data = kline_data.tail(2000)  # Keep up to 2000 candles
        logger.info(
            f"{Fore.CYAN}Fetched {len(all_data)} historical candles. Total kline_data size: {len(kline_data)}{Style.RESET_ALL}")
        historical_data_fetched = True
    except Exception as e:
        logger.error(f"{Fore.RED}Error fetching historical data: {str(e)}{Style.RESET_ALL}")
        raise

# Indicator functions
def calculate_atr(df, period):
    high_low = df['high'] - df['low']
    high_close = np.abs(df['high'] - df['close'].shift(1))
    low_close = np.abs(df['low'] - df['close'].shift(1))
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = pd.Series(np.nan, index=df.index)
    atr.iloc[period - 1] = true_range.iloc[:period].mean()
    for i in range(period, len(df)):
        atr.iloc[i] = (atr.iloc[i - 1] * (period - 1) + true_range.iloc[i]) / period
    return atr

def calculate_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def calculate_supertrend(df, atr_period, atr_ratio):
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
        epsilon = 1e-5  # Adjust based on asset precision (e.g., 0.0001 for XRPUSDT)
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

# WebSocket callback functions
def on_open(ws):
    logger.info(f"{Fore.CYAN}WebSocket opened{Style.RESET_ALL}")
    subscription = {
        "method": "SUBSCRIBE",
        "params": [f"{TRADING_PAIR.lower()}@kline_{TIMEFRAME}"],
        "id": 1
    }
    ws.send(json.dumps(subscription))

def on_message(ws, message):
    global kline_data, first_closed_candle_received, first_closed_timestamp, closed_candle_count, historical_data_fetched, current_position, trade_history, conn, previous_trend
    try:
        data = json.loads(message)
        if 'data' not in data or 'k' not in data['data']:
            logger.debug("Message does not contain kline data, skipping")
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

        if not first_closed_candle_received and kline['x']:
            first_closed_candle_received = True
            first_closed_timestamp = kline['t']
            logger.info(
                f"{Fore.CYAN}First closed candle received at timestamp: {first_closed_timestamp}{Style.RESET_ALL}")
            fetch_historical_data(limit=1000, end_time=int(first_closed_timestamp) - 1, num_batches=2)

        if kline['x']:
            if len(kline_data) >= ATR_PERIOD:
                closed_candle_count += 1
                logger.info(f"{Fore.CYAN}Closed candle count: {closed_candle_count}{Style.RESET_ALL}")
                logger.info(f"{Fore.CYAN}Bar Closed - Close: {new_row['close'].iloc[0]:.2f}{Style.RESET_ALL}")
            else:
                logger.warning(
                    f"{Fore.YELLOW}Insufficient data: {len(kline_data)} candles, need at least {ATR_PERIOD}{Style.RESET_ALL}")
                return

            df = kline_data.copy()
            df.set_index('timestamp', inplace=True)

            # Define epsilon here for use in logging
            epsilon = 1e-5  # Match the value used in calculate_supertrend

            line_st, trend = calculate_supertrend(df, ATR_PERIOD, ATR_RATIO)
            latest_line_st = line_st.iloc[-1]
            latest_trend = trend.iloc[-1]
            latest_close = df['close'].iloc[-1]

            # Log close, ST_LINE, and trend
            logger.debug(f"Close: {latest_close:.10f}, ST_LINE: {latest_line_st:.10f}, Trend: {latest_trend}")

            # Log trend flips
            if previous_trend is not None and latest_trend != previous_trend:
                logger.info(f"Trend flipped from {previous_trend} to {latest_trend}")

            # Log ATR and ATR_SMMA periodically
            if closed_candle_count % 10 == 0:  # Every 10 candles
                atr = calculate_atr(df, ATR_PERIOD)
                atr_smma = calculate_ema(atr, ATR_PERIOD)
                logger.debug(f"ATR: {atr.iloc[-1]:.10f}, ATR_SMMA: {atr_smma.iloc[-1]:.10f}")

            # Log comparison details
            line_st_current = latest_line_st
            if latest_close > line_st_current + epsilon:
                logger.debug(
                    f"Trend set to 1: Close {latest_close:.10f} > ST_LINE {line_st_current:.10f} + epsilon")
            elif latest_close < line_st_current - epsilon:
                logger.debug(
                    f"Trend set to -1: Close {latest_close:.10f} < ST_LINE {line_st_current:.10f} - epsilon")
            else:
                logger.debug(
                    f"Trend unchanged: Close {latest_close:.10f} within epsilon of ST_LINE {line_st_current:.10f}")

            stop_loss = latest_line_st - STOP_LOSS_OFFSET if latest_trend == 1 else latest_line_st + STOP_LOSS_OFFSET
            symbol_config = load_symbol_config(TRADING_PAIR, client)
            adjusted_quantity = adjust_quantity(POSITION_SIZE, symbol_config, latest_close)
            adjusted_stop_price = adjust_price(stop_loss, symbol_config)

            # Sync position with Binance before making decisions
            binance_position = sync_position_with_binance(client, TRADING_PAIR)
            logger.debug(f"Position sync result: {binance_position}")
            if binance_position:
                if not current_position or current_position['side'] != binance_position['side'] or abs(
                        current_position['size'] - binance_position['size']) > 0.0001:
                    logger.warning(
                        f"{Fore.YELLOW}Position mismatch detected. Updating from Binance: {binance_position}{Style.RESET_ALL}")
                    current_position = binance_position
            elif binance_position is None and current_position:
                logger.warning("No position on Binance. Clearing local state.")
                current_position = None
            else:
                current_position = None

            # Update current_position with the synced stop-loss and order ID
            if binance_position and current_position:
                current_position['stop_loss'] = binance_position.get('stop_loss')
                current_position['stop_loss_order_id'] = binance_position.get('stop_loss_order_id')

            if current_position:
                position_trend = 1 if current_position['side'] == 'LONG' else -1
                logger.debug(f"Current position: side={current_position['side']}, position_trend={position_trend}, latest_trend={latest_trend}")
                if position_trend != latest_trend:
                    logger.info(f"Position trend mismatch detected: Position {current_position['side']} (trend {position_trend}), Indicator trend {latest_trend}. Flipping position.")
                    if current_position['side'] == 'LONG' and latest_trend == -1:
                        cancel_all_stop_loss_orders(client, TRADING_PAIR)
                        logger.debug("Attempting to close LONG position")
                        try:
                            close_order = client.new_order(
                                symbol=TRADING_PAIR,
                                side='SELL',
                                type='MARKET',
                                quantity=current_position['size']
                            )
                            logger.debug(f"Close position response: {close_order}")
                        except Exception as e:
                            if "Position does not exist" in str(e) or "Order does not exist" in str(e):
                                logger.warning(f"Position already closed on Binance. Clearing local state.")
                                current_position = None
                            else:
                                logger.error(f"Failed to close position: {str(e)}")
                                raise e

                        if current_position is not None:
                            trade = {
                                'timestamp': str(datetime.now(timezone.utc)),
                                'trading_pair': TRADING_PAIR,
                                'timeframe': TIMEFRAME,
                                'side': 'LONG',
                                'entry_price': current_position['entry_price'],
                                'size': current_position['size'],
                                'exit_price': latest_close,
                                'profit_loss': (latest_close - current_position['entry_price']) * current_position['size'],
                                'trend': latest_trend,
                                'order_id': close_order['orderId']
                            }
                            trade_history.append(trade)
                            log_trade(conn, trade)
                            binance_position = sync_position_with_binance(client, TRADING_PAIR)
                            if not binance_position:
                                logger.info("Position successfully closed. Opening new SHORT position.")
                                current_position = None
                            else:
                                logger.warning(f"Position still exists after closing attempt: {binance_position}")
                                current_position = binance_position
                                display_trade_summary(current_position, latest_close, latest_line_st)
                                return

                        if current_position is None:
                            logger.debug("Opening new SHORT position")
                            market_order = client.new_order(
                                symbol=TRADING_PAIR,
                                side='SELL',
                                type='MARKET',
                                quantity=adjusted_quantity
                            )
                            # Sync position immediately to confirm it was opened
                            binance_position = sync_position_with_binance(client, TRADING_PAIR)
                            if not binance_position:
                                logger.error("Failed to confirm new SHORT position after placing order.")
                                raise Exception("Failed to confirm new SHORT position after placing order.")
                            # Set stop-loss immediately after confirming the position
                            stop_loss_order_id = update_stop_loss(
                                client,
                                TRADING_PAIR,
                                'SHORT',
                                adjusted_stop_price,
                                None,
                                latest_close,
                                is_new_position=True
                            )
                            current_position = {
                                'side': 'SHORT',
                                'entry_price': binance_position['entry_price'],
                                'size': adjusted_quantity,
                                'stop_loss': adjusted_stop_price,
                                'trend': latest_trend,
                                'open_time': str(datetime.now(timezone.utc)),
                                'order_id': market_order['orderId'],
                                'stop_loss_order_id': stop_loss_order_id
                            }
                            logger.info(
                                f"{Fore.GREEN}Reversed to SHORT at {latest_close:.2f}, Stop Loss: {adjusted_stop_price:.2f}{Style.RESET_ALL}")
                            # Write the new position to the database immediately
                            write_position_to_db(conn, current_position)

                    elif current_position['side'] == 'SHORT' and latest_trend == 1:
                        cancel_all_stop_loss_orders(client, TRADING_PAIR)
                        logger.debug("Attempting to close SHORT position")
                        try:
                            close_order = client.new_order(
                                symbol=TRADING_PAIR,
                                side='BUY',
                                type='MARKET',
                                quantity=current_position['size']
                            )
                            logger.debug(f"Close position response: {close_order}")
                        except Exception as e:
                            if "Position does not exist" in str(e) or "Order does not exist" in str(e):
                                logger.warning(f"Position already closed on Binance. Clearing local state.")
                                current_position = None
                            else:
                                logger.error(f"Failed to close position: {str(e)}")
                                raise e

                        if current_position is not None:
                            trade = {
                                'timestamp': str(datetime.now(timezone.utc)),
                                'trading_pair': TRADING_PAIR,
                                'timeframe': TIMEFRAME,
                                'side': 'SHORT',
                                'entry_price': current_position['entry_price'],
                                'size': current_position['size'],
                                'exit_price': latest_close,
                                'profit_loss': (current_position['entry_price'] - latest_close) * current_position['size'],
                                'trend': latest_trend,
                                'order_id': close_order['orderId']
                            }
                            trade_history.append(trade)
                            log_trade(conn, trade)
                            binance_position = sync_position_with_binance(client, TRADING_PAIR)
                            if not binance_position:
                                logger.info("Position successfully closed. Opening new LONG position.")
                                current_position = None
                            else:
                                logger.warning(f"Position still exists after closing attempt: {binance_position}")
                                current_position = binance_position
                                display_trade_summary(current_position, latest_close, latest_line_st)
                                return

                        if current_position is None:
                            logger.debug("Opening new LONG position")
                            market_order = client.new_order(
                                symbol=TRADING_PAIR,
                                side='BUY',
                                type='MARKET',
                                quantity=adjusted_quantity
                            )
                            # Sync position immediately to confirm it was opened
                            binance_position = sync_position_with_binance(client, TRADING_PAIR)
                            if not binance_position:
                                logger.error("Failed to confirm new LONG position after placing order.")
                                raise Exception("Failed to confirm new LONG position after placing order.")
                            # Set stop-loss immediately after confirming the position
                            stop_loss_order_id = update_stop_loss(
                                client,
                                TRADING_PAIR,
                                'LONG',
                                adjusted_stop_price,
                                None,
                                latest_close,
                                is_new_position=True
                            )
                            current_position = {
                                'side': 'LONG',
                                'entry_price': binance_position['entry_price'],
                                'size': adjusted_quantity,
                                'stop_loss': adjusted_stop_price,
                                'trend': latest_trend,
                                'open_time': str(datetime.now(timezone.utc)),
                                'order_id': market_order['orderId'],
                                'stop_loss_order_id': stop_loss_order_id
                            }
                            logger.info(
                                f"{Fore.GREEN}Reversed to LONG at {latest_close:.2f}, Stop Loss: {adjusted_stop_price:.2f}{Style.RESET_ALL}")
                            # Write the new position to the database immediately
                            write_position_to_db(conn, current_position)

                elif current_position['side'] == ('LONG' if latest_trend == 1 else 'SHORT'):
                    logger.debug(f"Current SL: {current_position.get('stop_loss')}, Adjusted SL: {adjusted_stop_price}")
                    epsilon = 1e-5
                    current_sl = current_position.get('stop_loss')
                    if current_sl is None:
                        logger.debug("Stop-loss not detected in current position. Checking Binance directly.")
                        orders = fetch_open_orders(TRADING_PAIR, API_KEY, API_SECRET)
                        logger.debug(f"Open orders for stop-loss check: {orders}")
                        stop_loss_order = next(
                            (order for order in orders
                             if order['type'] == 'STOP_MARKET' and
                             order.get('reduceOnly', False) and
                             order.get('closePosition', False) and
                             ((current_position['side'] == 'LONG' and order['side'] == 'SELL') or
                              (current_position['side'] == 'SHORT' and order['side'] == 'BUY'))),
                            None
                        )
                        if stop_loss_order:
                            logger.info(f"Found existing stop-loss order on Binance: {stop_loss_order}")
                            current_sl = float(stop_loss_order['stopPrice'])
                            current_position['stop_loss'] = current_sl
                            current_position['stop_loss_order_id'] = stop_loss_order['orderId']
                    if current_sl is None or abs(current_sl - adjusted_stop_price) > epsilon:
                        new_stop_loss_order_id = update_stop_loss(client, TRADING_PAIR, current_position['side'],
                                                                  adjusted_stop_price,
                                                                  current_position.get('stop_loss_order_id'),
                                                                  latest_close)
                        if new_stop_loss_order_id:
                            current_position['stop_loss'] = adjusted_stop_price
                            current_position['stop_loss_order_id'] = new_stop_loss_order_id
                            logger.info(f"{Fore.YELLOW}Updated stop-loss to {adjusted_stop_price:.2f}{Style.RESET_ALL}")
                            # Update the stop-loss in the database
                            write_position_to_db(conn, current_position)
                    else:
                        logger.debug(f"Stop-loss unchanged: {current_sl:.2f} (within epsilon of {adjusted_stop_price:.2f})")

                if current_position and current_position.get('stop_loss') is not None:
                    logger.debug(f"Checking stop-loss trigger: side={current_position['side']}, latest_close={latest_close}, stop_loss={current_position['stop_loss']}")
                    if (current_position['side'] == 'LONG' and latest_close <= current_position['stop_loss']) or \
                       (current_position['side'] == 'SHORT' and latest_close >= current_position['stop_loss']):
                        logger.debug("Stop-loss triggered, closing position")
                        cancel_all_stop_loss_orders(client, TRADING_PAIR)
                        close_side = 'SELL' if current_position['side'] == 'LONG' else 'BUY'
                        previous_side = current_position['side']  # Store the side before closing
                        try:
                            close_order = client.new_order(
                                symbol=TRADING_PAIR,
                                side=close_side,
                                type='MARKET',
                                quantity=current_position['size']
                            )
                            logger.debug(f"Close position response: {close_order}")
                        except Exception as e:
                            if "Position does not exist" in str(e) or "Order does not exist" in str(e):
                                logger.warning(f"Position already closed on Binance (stop-loss likely triggered). Clearing local state.")
                            else:
                                logger.error(f"Failed to close position: {str(e)}")
                                raise e

                        trade = {
                            'timestamp': str(datetime.now(timezone.utc)),
                            'trading_pair': TRADING_PAIR,
                            'timeframe': TIMEFRAME,
                            'side': previous_side,
                            'entry_price': current_position['entry_price'],
                            'size': current_position['size'],
                            'exit_price': latest_close,
                            'stop_loss': current_position.get('stop_loss'),
                            'profit_loss': (latest_close - current_position['entry_price']) * current_position['size'] if previous_side == 'LONG' else (current_position['entry_price'] - latest_close) * current_position['size'],
                            'trend': latest_trend,
                            'order_id': close_order.get('orderId', current_position.get('order_id')),
                            'stop_loss_order_id': current_position.get('stop_loss_order_id')
                        }
                        trade_history.append(trade)
                        log_trade(conn, trade)
                        logger.info(
                            f"{Fore.RED}Stop-loss triggered: Closed {previous_side} at {latest_close:.2f}, P/L: {trade['profit_loss']:.2f} USDT{Style.RESET_ALL}")
                        binance_position = sync_position_with_binance(client, TRADING_PAIR)
                        if not binance_position:
                            logger.info("Position successfully closed. Awaiting manual intervention to resume trading.")
                            current_position = None
                        else:
                            logger.warning(f"Position still exists after stop-loss trigger: {binance_position}")
                            current_position = binance_position
                            display_trade_summary(current_position, latest_close, latest_line_st)
                            return

            if not current_position and ((force_first_trade and closed_candle_count == 1) or (
                    previous_trend is not None and previous_trend != latest_trend)):
                logger.debug(f"Opening new position: force_first_trade={force_first_trade}, closed_candle_count={closed_candle_count}, previous_trend={previous_trend}, latest_trend={latest_trend}")
                if latest_trend == 1:
                    logger.debug("Opening LONG position")
                    market_order = client.new_order(
                        symbol=TRADING_PAIR,
                        side='BUY',
                        type='MARKET',
                        quantity=adjusted_quantity
                    )
                    # Sync position immediately to confirm it was opened
                    binance_position = sync_position_with_binance(client, TRADING_PAIR)
                    if not binance_position:
                        logger.error("Failed to confirm new LONG position after placing order.")
                        raise Exception("Failed to confirm new LONG position after placing order.")
                    # Set stop-loss immediately after confirming the position
                    stop_loss_order_id = update_stop_loss(
                        client,
                        TRADING_PAIR,
                        'LONG',
                        adjusted_stop_price,
                        None,
                        latest_close,
                        is_new_position=True
                    )
                    current_position = {
                        'side': 'LONG',
                        'entry_price': binance_position['entry_price'],
                        'size': adjusted_quantity,
                        'stop_loss': adjusted_stop_price,
                        'trend': latest_trend,
                        'open_time': str(datetime.now(timezone.utc)),
                        'order_id': market_order['orderId'],
                        'stop_loss_order_id': stop_loss_order_id
                    }
                    logger.info(
                        f"{Fore.GREEN}Opened LONG at {latest_close:.2f}, Stop Loss: {adjusted_stop_price:.2f}{Style.RESET_ALL}")
                    # Write the new position to the database immediately
                    write_position_to_db(conn, current_position)

                elif latest_trend == -1:
                    logger.debug("Opening SHORT position")
                    market_order = client.new_order(
                        symbol=TRADING_PAIR,
                        side='SELL',
                        type='MARKET',
                        quantity=adjusted_quantity
                    )
                    # Sync position immediately to confirm it was opened
                    binance_position = sync_position_with_binance(client, TRADING_PAIR)
                    if not binance_position:
                        logger.error("Failed to confirm new SHORT position after placing order.")
                        raise Exception("Failed to confirm new SHORT position after placing order.")
                    # Set stop-loss immediately after confirming the position
                    stop_loss_order_id = update_stop_loss(
                        client,
                        TRADING_PAIR,
                        'SHORT',
                        adjusted_stop_price,
                        None,
                        latest_close,
                        is_new_position=True
                    )
                    current_position = {
                        'side': 'SHORT',
                        'entry_price': binance_position['entry_price'],
                        'size': adjusted_quantity,
                        'stop_loss': adjusted_stop_price,
                        'trend': latest_trend,
                        'open_time': str(datetime.now(timezone.utc)),
                        'order_id': market_order['orderId'],
                        'stop_loss_order_id': stop_loss_order_id
                    }
                    logger.info(
                        f"{Fore.GREEN}Opened SHORT at {latest_close:.2f}, Stop Loss: {adjusted_stop_price:.2f}{Style.RESET_ALL}")
                    # Write the new position to the database immediately
                    write_position_to_db(conn, current_position)

            logger.debug(f"Current Position: {current_position}, Latest Trend: {latest_trend}, Previous Trend: {previous_trend}")

            previous_trend = latest_trend
            display_trade_summary(current_position, latest_close, latest_line_st)

    except Exception as e:
        logger.error(f"{Fore.RED}Error in WebSocket message: {str(e)}{Style.RESET_ALL}")

def on_error(ws, error):
    logger.error(f"{Fore.RED}WebSocket error: {str(error)}{Style.RESET_ALL}")
    reconnect(ws)

def on_close(ws, close_status_code, close_msg):
    logger.info(f"{Fore.CYAN}WebSocket closed: {close_status_code} - {close_msg}{Style.RESET_ALL}")
    reconnect(ws)

def reconnect(ws):
    delay = 5
    while True:
        try:
            ws.close()  # Explicitly close the WebSocket before reconnecting
            logger.info(f"{Fore.YELLOW}Reconnecting in {delay} seconds...{Style.RESET_ALL}")
            time.sleep(delay)
            ws.run_forever()
            logger.info(f"{Fore.GREEN}WebSocket reconnected successfully{Style.RESET_ALL}")
            break
        except Exception as e:
            logger.error(f"{Fore.RED}Reconnection failed: {str(e)}{Style.RESET_ALL}")
            delay = min(delay * 2, 60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Binance Futures Trading Bot")
    parser.add_argument('--force-first-trade', action='store_true', help="Force trade on first closed candle")
    args = parser.parse_args()
    force_first_trade = args.force_first_trade

    conn = init_db()
    websocket_url = "wss://fstream.binance.com/stream"
    logger.warning(
        f"{Fore.YELLOW}WARNING: Now trading LIVE on Binance Futures mainnet! Force First Trade: {force_first_trade}{Style.RESET_ALL}")
    ws = websocket.WebSocketApp(websocket_url, on_open=on_open, on_message=on_message, on_error=on_error,
                                on_close=on_close)
    ws.run_forever()
    conn.close()
