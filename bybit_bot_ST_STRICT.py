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
from pybit.unified_trading import HTTP

# Initialize colorama for colored terminal output
init()

# Script Version
SCRIPT_VERSION = "2.8.7"  # Added debug logging for database writes

# Set up logging with dual handlers
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Enable DEBUG level for the logger
logging.getLogger('').handlers = []  # Clear any existing handlers

# File handler (logs DEBUG and above)
file_handler = logging.FileHandler('bot.log')
file_handler.setLevel(logging.DEBUG)  # Log DEBUG to file
file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

# Console handler (logs INFO and above)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)  # Log INFO and above to console
console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(console_formatter)
logger.addHandler(console_handler)

logger.propagate = False


# Load configuration from config.json
def load_config():
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
        logger.debug("Loaded config from config.json")
        return {
            'binance_api_key': config.get('binance_api_key', 'your_api_key'),
            'binance_api_secret': config.get('binance_api_secret', 'your_api_secret'),
            'bybit_api_key': config.get('bybit_api_key', 'your_bybit_api_key'),
            'bybit_api_secret': config.get('bybit_api_secret', 'your_bybit_api_secret'),
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
            'binance_api_key': 'your_api_key',
            'binance_api_secret': 'your_api_secret',
            'bybit_api_key': 'your_bybit_api_key',
            'bybit_api_secret': 'your_bybit_api_secret',
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
BINANCE_API_KEY = config['binance_api_key']
BINANCE_API_SECRET = config['binance_api_secret']
BYBIT_API_KEY = config['bybit_api_key']
BYBIT_API_SECRET = config['bybit_api_secret']
ATR_PERIOD = config['atr_period']
ATR_RATIO = config['atr_ratio']
POSITION_SIZE = config['position_size']
TRADING_PAIR = config['trading_pair']
BYBIT_TRADING_PAIR = TRADING_PAIR  # Bybit uses same format as Binance (e.g., ETHUSDT)
TIMEFRAME = config['timeframe']
STOP_LOSS_OFFSET = config['stop_loss_offset']
logger.debug(
    f"Config loaded: ATR_PERIOD={ATR_PERIOD}, ATR_RATIO={ATR_RATIO}, POSITION_SIZE={POSITION_SIZE}, TRADING_PAIR={TRADING_PAIR}, TIMEFRAME={TIMEFRAME}, STOP_LOSS_OFFSET={STOP_LOSS_OFFSET}")

# Initialize Binance client for kline fetching
binance_client = UMFutures(key=BINANCE_API_KEY, secret=BINANCE_API_SECRET, base_url="https://fapi.binance.com")
logger.debug("Initialized Binance client for kline fetching")

# Initialize Bybit client for trading
bybit_client = HTTP(
    api_key=BYBIT_API_KEY,
    api_secret=BYBIT_API_SECRET,
    testnet=False  # Set to True for testnet
)
logger.debug("Initialized Bybit client for trading")

# Global DataFrame to store kline data with explicit dtypes
kline_data = pd.DataFrame(
    columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'],
    dtype='float64'
)
kline_data['timestamp'] = pd.to_datetime(kline_data['timestamp'])
logger.debug("Initialized kline_data DataFrame")

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
SYMBOL_CONFIG_FILE = 'bybit_symbol_configs.json'

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

# Load or fetch symbol configuration (for Bybit)
def load_symbol_config(symbol):
    symbol_configs = {}
    if os.path.exists(SYMBOL_CONFIG_FILE):
        with open(SYMBOL_CONFIG_FILE, 'r') as f:
            symbol_configs = json.load(f)
        logger.debug(f"Loaded existing symbol configs from {SYMBOL_CONFIG_FILE}: {symbol_configs}")

    if symbol not in symbol_configs:
        try:
            logger.debug(f"Fetching instrument info for {symbol}")
            instruments = bybit_client.get_instruments_info(category="linear", symbol=symbol)
            logger.debug(f"Instrument info response: {instruments}")
            if instruments['retCode'] != 0:
                raise Exception(f"Failed to fetch instrument info: {instruments['retMsg']}")
            instrument = instruments['result']['list'][0]  # Take the first matching instrument
            logger.debug(f"Instrument details for {symbol}: {instrument}")
            tick_size_str = str(instrument['priceFilter']['tickSize'])
            price_precision = len(tick_size_str.split('.')[1]) if '.' in tick_size_str else 0
            symbol_configs[symbol] = {
                'lotSize': float(instrument['lotSizeFilter']['qtyStep']),
                'quantityPrecision': len(
                    str(float(instrument['lotSizeFilter']['qtyStep'])).rstrip('0').split('.')[1]) if '.' in str(
                    float(instrument['lotSizeFilter']['qtyStep'])) else 0,
                'pricePrecision': price_precision,
                'minQty': float(instrument['lotSizeFilter']['minOrderQty']),
                'minNotional': float(instrument['lotSizeFilter']['minOrderQty']) * float(
                    instrument['priceFilter']['tickSize'])  # Approximation
            }
            with open(SYMBOL_CONFIG_FILE, 'w') as f:
                json.dump(symbol_configs, f, indent=4)
            logger.debug(f"Saved new symbol config for {symbol}: {symbol_configs[symbol]}")
        except Exception as e:
            logger.error(
                f"{Fore.RED}Failed to fetch symbol info for {symbol}: {str(e)}. Using defaults.{Style.RESET_ALL}")
            symbol_configs[symbol] = {'lotSize': 0.001, 'quantityPrecision': 3, 'pricePrecision': 2, 'minQty': 0.001,
                                      'minNotional': 5.0}
    return symbol_configs[symbol]


# Adjust quantity to match Bybit precision and minimum size
def adjust_quantity(quantity, symbol_config, price):
    lot_size = symbol_config['lotSize']
    precision = symbol_config['quantityPrecision']
    min_qty = symbol_config['minQty']
    min_notional = symbol_config['minNotional']
    min_qty_notional = max(min_qty, min_notional / price)
    contracts = quantity  # Bybit uses direct quantity in base currency (e.g., ETH)
    lots = contracts / lot_size
    rounded_lots = max(round(lots), round(min_qty_notional / lot_size))
    adjusted = rounded_lots * lot_size
    adjusted = round(adjusted, precision)
    if adjusted < min_qty:
        adjusted = min_qty
        logger.debug(f"Adjusted quantity increased to meet minQty: {adjusted}")
    if adjusted % lot_size != 0:
        adjusted = round(adjusted / lot_size) * lot_size
        logger.debug(f"Adjusted quantity to be a multiple of lotSize: {adjusted}")
    logger.debug(
        f"Adjusting quantity: desired={quantity}, lotSize={lot_size}, precision={precision}, minQty={min_qty}, minNotional={min_notional}, price={price}, adjusted={adjusted}")
    return adjusted


# Adjust price to match Bybit precision
def adjust_price(price, symbol_config):
    precision = symbol_config['pricePrecision']
    adjusted = round(price, precision)
    logger.debug(f"Adjusting price: original={price}, precision={precision}, adjusted={adjusted}")
    return adjusted


# Sync position with Bybit
@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5))
def sync_position_with_bybit(client, symbol):
    try:
        logger.debug(f"Step 1: Fetching position from Bybit for {symbol}")
        positions = client.get_positions(category="linear", symbol=symbol)
        logger.debug(f"Position response: {positions}")
        if positions['retCode'] != 0:
            raise Exception(f"Failed to fetch positions: {positions['retMsg']}")
        position_list = positions['result']['list']
        position = next((pos for pos in position_list if float(pos['size']) > 0), None)
        logger.debug(f"Step 2: Selected position: {position}")
        if not position:
            logger.info(f"No open position found for {symbol}")
            return None

        logger.debug("Step 3: Extracting position details")
        side = 'LONG' if position['side'] == 'Buy' else 'SHORT'
        entry_price = float(position['avgPrice'])
        size = float(position['size'])

        # Fetch stop-loss orders
        logger.debug("Step 4: Fetching open orders from Bybit")
        orders = client.get_open_orders(category="linear", symbol=symbol)
        logger.debug(f"Open orders response: {orders}")
        if orders['retCode'] != 0:
            raise Exception(f"Failed to fetch orders: {orders['retMsg']}")
        stop_loss_order = next(
            (order for order in orders['result']['list']
             if order['stopOrderType'] == 'StopLoss' and
             order['side'] == ('Sell' if side == 'LONG' else 'Buy') and
             float(order['qty']) == float(position['size'])),
            None
        )
        logger.debug(f"Step 5: Stop-loss order: {stop_loss_order}")
        stop_loss = float(stop_loss_order['triggerPrice']) if stop_loss_order else None
        stop_loss_order_id = stop_loss_order['orderId'] if stop_loss_order else None

        synced_position = {
            'side': side,
            'entry_price': entry_price,
            'size': size,
            'stop_loss': stop_loss,
            'stop_loss_order_id': stop_loss_order_id,
            'open_time': position.get('createdTime', str(datetime.now(timezone.utc))),
            'order_id': position.get('orderId', None)  # Ensure order_id is included
        }

        logger.info(f"Synced position: {synced_position}")
        return synced_position
    except Exception as e:
        logger.error(f"Failed to sync position: {str(e)}", exc_info=True)
        raise


@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5))
def cancel_all_stop_loss_orders(client, symbol):
    try:
        logger.debug(f"Fetching open orders to cancel stop-loss for {symbol}")
        orders = client.get_open_orders(category="linear", symbol=symbol)
        logger.debug(f"Open orders response: {orders}")
        if orders['retCode'] != 0:
            raise Exception(f"Failed to fetch orders: {orders['retMsg']}")
        stop_loss_orders = [order for order in orders['result']['list'] if order['stopOrderType'] == 'StopLoss']
        logger.debug(f"Stop-loss orders found: {stop_loss_orders}")
        if not stop_loss_orders:
            logger.debug(f"No stop-loss orders to cancel for {symbol}")
            return True

        for order in stop_loss_orders:
            logger.debug(f"Cancelling stop-loss order ID: {order['orderId']}")
            cancel_result = client.cancel_order(category="linear", symbol=symbol, orderId=order['orderId'])
            logger.debug(f"Cancel order response: {cancel_result}")
            if cancel_result['retCode'] != 0:
                raise Exception(f"Failed to cancel order: {cancel_result['retMsg']}")
            logger.debug(f"Canceled stop-loss order ID: {order['orderId']}")

        # Verify all stop-loss orders are canceled with retries
        max_retries = 3
        retry_delay = 1  # seconds
        for attempt in range(max_retries):
            logger.debug(f"Verifying all stop-loss orders are canceled (attempt {attempt + 1}/{max_retries})")
            orders = client.get_open_orders(category="linear", symbol=symbol)
            logger.debug(f"Open orders after cancellation: {orders}")
            if orders['retCode'] != 0:
                raise Exception(f"Failed to fetch orders after cancellation: {orders['retMsg']}")
            remaining_orders = [order for order in orders['result']['list'] if order['stopOrderType'] == 'StopLoss']
            if not remaining_orders:
                logger.debug(f"Successfully canceled all stop-loss orders for {symbol}")
                return True
            logger.warning(f"Stop-loss orders still present after cancellation, retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)

        logger.error(
            f"Failed to cancel all stop-loss orders after {max_retries} attempts. Remaining orders: {remaining_orders}")
        return False

    except Exception as e:
        logger.error(f"Failed to cancel stop-loss orders: {str(e)}")
        raise


# Update stop-loss order on Bybit using set_trading_stop
@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5))
def update_stop_loss(client, symbol, side, new_stop_price, current_stop_order_id, current_price, position_size,
                     is_new_position=False):
    try:
        symbol_config = load_symbol_config(symbol)
        new_stop_price = adjust_price(new_stop_price, symbol_config)

        # Ensure stop-loss price is valid for the position side
        logger.debug(
            f"Validating stop-loss price: side={side}, new_stop_price={new_stop_price}, current_price={current_price}")
        if side == 'LONG':
            # For LONG: stop-loss must be below the current price
            if new_stop_price >= current_price:
                new_stop_price = current_price - 0.01
                logger.debug(f"Adjusted stop-loss for LONG to {new_stop_price} (below current price {current_price})")
        elif side == 'SHORT':
            # For SHORT: stop-loss must be above the current price
            if new_stop_price <= current_price:
                new_stop_price = current_price + 0.01
                logger.debug(f"Adjusted stop-loss for SHORT to {new_stop_price} (above current price {current_price})")

        # If there's an existing stop-loss order, reset it by setting stop-loss to 0 first
        if current_stop_order_id:
            logger.debug(f"Resetting existing stop-loss for {symbol}")
            try:
                reset_response = client.set_trading_stop(
                    category="linear",
                    symbol=symbol,
                    stopLoss="0",  # Reset stop-loss
                    takeProfit="0",  # Reset take-profit
                    tpTriggerBy="LastPrice",
                    slTriggerBy="LastPrice",
                    tpslMode="Full",
                    tpOrderType="Market",
                    slOrderType="Market",
                    positionIdx=0
                )
                logger.debug(f"Reset stop-loss response: {reset_response}")
                if reset_response['retCode'] != 0:
                    if reset_response['retCode'] == 34040:  # Not modified (position likely closed)
                        logger.warning(f"No position exists to reset stop-loss for {symbol} (ErrCode: 34040).")
                        return None
                    logger.error(f"Failed to reset stop-loss: {reset_response['retMsg']}")
                    # Attempt to cancel any existing stop-loss orders as a fallback
                    if not cancel_all_stop_loss_orders(client, symbol):
                        raise Exception("Failed to cancel existing stop-loss orders during reset")
            except Exception as e:
                if "not modified" in str(e):
                    logger.warning(f"No position exists to reset stop-loss for {symbol} (ErrCode: 34040).")
                    return None
                raise e

        # If this is a new position, we assume the position exists since it was just opened
        if not is_new_position:
            # For existing positions, verify the position still exists
            position = sync_position_with_bybit(client, symbol)
            if not position:
                logger.warning(f"No position exists to set stop-loss for {symbol} (ErrCode: 34040).")
                return None

        # Set the new stop-loss using set_trading_stop
        logger.debug(f"Setting stop-loss for {symbol}: stop_loss_price={new_stop_price}, position_idx=0")
        response = client.set_trading_stop(
            category="linear",
            symbol=symbol,
            stopLoss=str(new_stop_price),
            takeProfit="0",  # Disable take-profit
            tpTriggerBy="LastPrice",
            slTriggerBy="LastPrice",
            tpslMode="Full",
            tpOrderType="Market",
            slOrderType="Market",
            positionIdx=0
        )
        logger.debug(f"Set stop-loss response: {response}")
        if response['retCode'] != 0:
            if response['retCode'] == 34040:  # Not modified (position likely closed)
                logger.warning(f"No position exists to set stop-loss for {symbol} (ErrCode: 34040).")
                return None
            raise Exception(f"Failed to set stop-loss: {response['retMsg']}")

        logger.info(f"{Fore.YELLOW}Updated stop-loss to {new_stop_price:.2f}{Style.RESET_ALL}")
        return current_stop_order_id  # Return the existing order ID (or None if reset)

    except Exception as e:
        if "not modified" in str(e):
            logger.warning(f"No position exists to update stop-loss for {symbol} (ErrCode: 34040).")
            return None
        logger.error(f"Failed to update stop-loss: {str(e)}")
        raise


def init_db():
    global current_position
    try:
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
                        order_id TEXT,
                        stop_loss_order_id TEXT
                     )''')
        conn.commit()
        logger.debug("Initialized SQLite database: trade_history.db")

        bybit_position = sync_position_with_bybit(bybit_client, BYBIT_TRADING_PAIR)
        logger.debug(f"Position synced from Bybit during init: {bybit_position}")
        if bybit_position:
            current_position = bybit_position
            logger.info(
                f"Synced position from Bybit: Side: {current_position['side']}, Entry Price: {current_position['entry_price']:.2f}, Stop Loss: {current_position.get('stop_loss', 'None')}")

            # Check if a trade with the same order_id exists
            order_id = current_position.get('order_id')
            c.execute("SELECT * FROM trades WHERE trading_pair = ? AND order_id = ? AND exit_price IS NULL",
                      (TRADING_PAIR, order_id))
            existing_trade = c.fetchone()

            if existing_trade:
                # Update the existing trade
                logger.info(f"Found existing active trade with order_id={order_id}. Updating details.")
                c.execute("""
                    UPDATE trades
                    SET timestamp = ?, side = ?, entry_price = ?, size = ?, stop_loss = ?, stop_loss_order_id = ?
                    WHERE id = ?
                """, (
                    current_position['open_time'],
                    current_position['side'],
                    current_position['entry_price'],
                    current_position['size'],
                    current_position.get('stop_loss'),
                    current_position.get('stop_loss_order_id'),
                    existing_trade[0]  # id
                ))
            else:
                # Insert a new trade, close stale trades first
                c.execute("UPDATE trades SET exit_price = 0 WHERE trading_pair = ? AND exit_price IS NULL", (TRADING_PAIR,))
                if c.rowcount > 0:
                    logger.info(f"Closed {c.rowcount} stale active trades in database for {TRADING_PAIR}")
                c.execute(
                    "INSERT INTO trades (timestamp, trading_pair, timeframe, side, entry_price, size, stop_loss, stop_loss_order_id, order_id) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (current_position['open_time'], TRADING_PAIR, TIMEFRAME, current_position['side'],
                     current_position['entry_price'], current_position['size'], current_position.get('stop_loss'),
                     current_position.get('stop_loss_order_id'), current_position.get('order_id'))
                )
            conn.commit()
            logger.info(
                f"Inserted/Updated active trade in database: Side: {current_position['side']}, Entry Price: {current_position['entry_price']:.2f}")
        else:
            c.execute("UPDATE trades SET exit_price = 0 WHERE trading_pair = ? AND exit_price IS NULL", (TRADING_PAIR,))
            if c.rowcount > 0:
                logger.info(f"Closed {c.rowcount} stale active trades in database for {TRADING_PAIR} (no position on Bybit)")
            conn.commit()
            current_position = None
            logger.info("No active position found on Bybit or in database")

        return conn
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        raise


# Log trade to database
def log_trade(conn, trade):
    try:
        c = conn.cursor()
        logger.debug(f"Logging trade to database: {trade}")
        c.execute('''INSERT INTO trades (timestamp, trading_pair, timeframe, side, entry_price, size, exit_price, stop_loss, profit_loss, trend, order_id, stop_loss_order_id)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  (trade['timestamp'], trade['trading_pair'], trade['timeframe'], trade['side'], trade['entry_price'],
                   trade['size'], trade.get('exit_price'), trade.get('stop_loss'), trade.get('profit_loss'),
                   trade.get('trend'), trade.get('order_id'), trade.get('stop_loss_order_id')))
        conn.commit()
        logger.debug("Trade logged successfully")
    except sqlite3.Error as e:
        logger.error(f"Failed to log trade to database: {e}")
        raise


# Display trade summary
def display_trade_summary(position, latest_close, line_st):
    if position:
        profit_loss = (latest_close - position['entry_price']) * position['size'] if position['side'] == 'LONG' else (
                                                                                                                             position[
                                                                                                                                 'entry_price'] - latest_close) * \
                                                                                                                     position[
                                                                                                                         'size']
        stop_loss_display = f"{position['stop_loss']:.2f}" if position.get('stop_loss') is not None else 'None'
        logger.info(
            f"{Fore.CYAN}Open Trade Summary - Side: {position['side']}, Open Time: {position['open_time']}, Entry Price: {position['entry_price']:.2f}, "
            f"Size: {position['size']:.4f}, Current Price: {latest_close:.2f}, Profit/Loss: {profit_loss:.2f} USDT, "
            f"ST_LINE: {line_st:.2f}, Stop Loss: {stop_loss_display}{Style.RESET_ALL}"
        )
    else:
        logger.info(f"{Fore.CYAN}No active position.{Style.RESET_ALL}")


# Fetch historical kline data with retry and timeout
@retry(wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_historical_data(symbol=TRADING_PAIR, interval=TIMEFRAME, limit=1000, end_time=None, num_batches=2):
    global kline_data, historical_data_fetched
    try:
        logger.debug(
            f"Fetching historical klines: symbol={symbol}, interval={interval}, limit={limit}, end_time={end_time}, num_batches={num_batches}")
        mainnet_client = UMFutures(key=BINANCE_API_KEY, secret=BINANCE_API_SECRET, base_url="https://fapi.binance.com")
        all_data = pd.DataFrame()
        for batch in range(num_batches):
            params = {'symbol': symbol, 'interval': interval, 'limit': limit}
            if end_time:
                params['endTime'] = end_time
            logger.debug(f"Fetching batch {batch + 1}/{num_batches} with params: {params}")
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
            logger.debug(f"Fetched batch data: {len(batch_data)} rows")
            all_data = pd.concat([batch_data, all_data], ignore_index=True)
            if len(klines) < limit:
                logger.debug("Reached end of historical data")
                break
            end_time = int(batch_data['timestamp'].iloc[0].timestamp() * 1000) - 1
        all_data.sort_values('timestamp', inplace=True)
        all_data.drop_duplicates(subset='timestamp', keep='last', inplace=True)
        prev_rows = len(kline_data)
        kline_data = pd.concat([all_data, kline_data], ignore_index=True)
        kline_data = kline_data.tail(2000)
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
    logger.debug(f"Calculated ATR with period {period}: {atr.iloc[-1] if not atr.empty else 'N/A'}")
    return atr


def calculate_ema(series, period):
    ema = series.ewm(span=period, adjust=False).mean()
    logger.debug(f"Calculated EMA with period {period}: {ema.iloc[-1] if not ema.empty else 'N/A'}")
    return ema


def calculate_supertrend(df, atr_period, atr_ratio):
    logger.debug(f"Calculating Supertrend with atr_period={atr_period}, atr_ratio={atr_ratio}")
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
    logger.debug(
        f"Supertrend calculated: Latest line_st={line_st.iloc[-1] if not line_st.empty else 'N/A'}, trend={trend.iloc[-1] if not trend.empty else 'N/A'}")
    return line_st, trend


# WebSocket callback functions
def on_open(ws):
    logger.info(f"{Fore.CYAN}WebSocket opened{Style.RESET_ALL}")
    subscription = {
        "method": "SUBSCRIBE",
        "params": [f"{TRADING_PAIR.lower()}@kline_{TIMEFRAME}"],
        "id": 1
    }
    logger.debug(f"Sending WebSocket subscription: {subscription}")
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
            logger.debug(f"Prepared DataFrame for indicators: {df.tail(1).to_dict()}")

            epsilon = 1e-5

            line_st, trend = calculate_supertrend(df, ATR_PERIOD, ATR_RATIO)
            latest_line_st = line_st.iloc[-1]
            latest_trend = trend.iloc[-1]
            latest_close = df['close'].iloc[-1]

            logger.debug(f"Close: {latest_close:.10f}, ST_LINE: {latest_line_st:.10f}, Trend: {latest_trend}")

            if previous_trend is not None and latest_trend != previous_trend:
                logger.info(f"Trend flipped from {previous_trend} to {latest_trend}")

            if closed_candle_count % 10 == 0:
                atr = calculate_atr(df, ATR_PERIOD)
                atr_smma = calculate_ema(atr, ATR_PERIOD)
                logger.debug(f"ATR: {atr.iloc[-1]:.10f}, ATR_SMMA: {atr_smma.iloc[-1]:.10f}")

            if latest_close > latest_line_st + epsilon:
                logger.debug(
                    f"Trend set to 1: Close {latest_close:.10f} > ST_LINE {latest_line_st:.10f} + epsilon")
            elif latest_close < latest_line_st - epsilon:
                logger.debug(
                    f"Trend set to -1: Close {latest_close:.10f} < ST_LINE {latest_line_st:.10f} - epsilon")
            else:
                logger.debug(
                    f"Trend unchanged: {latest_close:.10f} within epsilon of {latest_line_st:.10f}")

            stop_loss = latest_line_st - STOP_LOSS_OFFSET if latest_trend == 1 else latest_line_st + STOP_LOSS_OFFSET
            logger.debug(
                f"Calculated stop_loss: latest_line_st={latest_line_st}, STOP_LOSS_OFFSET={STOP_LOSS_OFFSET}, stop_loss={stop_loss}")
            symbol_config = load_symbol_config(BYBIT_TRADING_PAIR)
            adjusted_quantity = adjust_quantity(POSITION_SIZE, symbol_config, latest_close)
            adjusted_stop_price = adjust_price(stop_loss, symbol_config)
            logger.debug(f"Adjusted stop_price: {adjusted_stop_price}, adjusted_quantity: {adjusted_quantity}")

            # Sync position with Bybit before making decisions
            bybit_position = sync_position_with_bybit(bybit_client, BYBIT_TRADING_PAIR)
            logger.debug(f"Position sync result: {bybit_position}")
            if bybit_position:
                if not current_position or current_position['side'] != bybit_position['side'] or abs(
                        current_position['size'] - bybit_position['size']) > 0.0001:
                    logger.warning(
                        f"{Fore.YELLOW}Position mismatch detected. Updating from Bybit: {bybit_position}{Style.RESET_ALL}")
                    current_position = bybit_position
            elif bybit_position is None and current_position:
                logger.warning("No position on Bybit. Clearing local state.")
                current_position = None
            else:
                current_position = None

            # Update current_position with the synced stop-loss and order ID
            if bybit_position and current_position:
                current_position['stop_loss'] = bybit_position.get('stop_loss')
                current_position['stop_loss_order_id'] = bybit_position.get('stop_loss_order_id')

            if current_position:
                position_trend = 1 if current_position['side'] == 'LONG' else -1
                logger.debug(
                    f"Current position: side={current_position['side']}, position_trend={position_trend}, latest_trend={latest_trend}")
                if position_trend != latest_trend:
                    logger.info(
                        f"Position trend mismatch detected: Position {current_position['side']} (trend {position_trend}), Indicator trend {latest_trend}. Flipping position.")
                    if current_position['side'] == 'LONG' and latest_trend == -1:
                        cancel_all_stop_loss_orders(bybit_client, BYBIT_TRADING_PAIR)
                        logger.debug("Attempting to close LONG position")
                        try:
                            close_order = bybit_client.place_order(
                                category="linear",
                                symbol=BYBIT_TRADING_PAIR,
                                side="Sell",
                                orderType="Market",
                                qty=str(adjusted_quantity),
                                reduceOnly=True
                            )
                            logger.debug(f"Close position response: {close_order}")
                            if close_order['retCode'] != 0:
                                if close_order['retCode'] == 110017:  # Current position is zero
                                    logger.warning(
                                        f"Position already closed on Bybit (ErrCode: 110017). Clearing local state.")
                                    current_position = None
                                else:
                                    logger.error(f"Failed to close position: {close_order['retMsg']}")
                                    raise Exception(f"Failed to close position: {close_order['retMsg']}")
                            else:
                                trade = {
                                    'timestamp': str(datetime.now(timezone.utc)),
                                    'trading_pair': TRADING_PAIR,
                                    'timeframe': TIMEFRAME,
                                    'side': 'LONG',
                                    'entry_price': current_position['entry_price'],
                                    'size': current_position['size'],
                                    'exit_price': latest_close,
                                    'profit_loss': (latest_close - current_position['entry_price']) * current_position[
                                        'size'],
                                    'trend': latest_trend,
                                    'order_id': close_order['result']['orderId'],
                                    'stop_loss_order_id': current_position.get('stop_loss_order_id')
                                }
                                trade_history.append(trade)
                                log_trade(conn, trade)
                                bybit_position = sync_position_with_bybit(bybit_client, BYBIT_TRADING_PAIR)
                                if not bybit_position:
                                    logger.info("Position successfully closed. Opening new SHORT position.")
                                    current_position = None
                                else:
                                    logger.warning(f"Position still exists after closing attempt: {bybit_position}")
                                    current_position = bybit_position
                                    display_trade_summary(current_position, latest_close, latest_line_st)
                                    return
                        except Exception as e:
                            if "current position is zero" in str(e):
                                logger.warning(
                                    f"Position already closed on Bybit (ErrCode: 110017). Clearing local state.")
                                current_position = None
                            else:
                                raise e

                        if current_position is None:
                            logger.debug("Opening new SHORT position")
                            market_order = bybit_client.place_order(
                                category="linear",
                                symbol=BYBIT_TRADING_PAIR,
                                side="Sell",
                                orderType="Market",
                                qty=str(adjusted_quantity)
                            )
                            logger.debug(f"Market order response: {market_order}")
                            if market_order['retCode'] != 0:
                                raise Exception(f"Failed to place market order: {market_order['retMsg']}")

                            # Sync position immediately to confirm it was opened
                            bybit_position = sync_position_with_bybit(bybit_client, BYBIT_TRADING_PAIR)
                            if not bybit_position:
                                logger.error("Failed to confirm new SHORT position after placing order.")
                                raise Exception("Failed to confirm new SHORT position after placing order.")

                            # Set stop-loss immediately after confirming the position
                            stop_loss_order_id = update_stop_loss(
                                bybit_client,
                                BYBIT_TRADING_PAIR,
                                'SHORT',
                                adjusted_stop_price,
                                None,
                                latest_close,
                                POSITION_SIZE,
                                is_new_position=True
                            )
                            current_position = {
                                'side': 'SHORT',
                                'entry_price': bybit_position['entry_price'],
                                'size': adjusted_quantity,
                                'stop_loss': adjusted_stop_price,
                                'trend': latest_trend,
                                'open_time': str(datetime.now(timezone.utc)),
                                'order_id': market_order['result']['orderId'],
                                'stop_loss_order_id': stop_loss_order_id
                            }
                            logger.info(
                                f"{Fore.GREEN}Reversed to SHORT at {latest_close:.2f}, Stop Loss: {adjusted_stop_price:.2f}{Style.RESET_ALL}")
                            # Write the new position to the database immediately
                            write_position_to_db(conn, current_position)

                    elif current_position['side'] == 'SHORT' and latest_trend == 1:
                        cancel_all_stop_loss_orders(bybit_client, BYBIT_TRADING_PAIR)
                        logger.debug("Attempting to close SHORT position")
                        try:
                            close_order = bybit_client.place_order(
                                category="linear",
                                symbol=BYBIT_TRADING_PAIR,
                                side="Buy",
                                orderType="Market",
                                qty=str(adjusted_quantity),
                                reduceOnly=True
                            )
                            logger.debug(f"Close position response: {close_order}")
                            if close_order['retCode'] != 0:
                                if close_order['retCode'] == 110017:  # Current position is zero
                                    logger.warning(
                                        f"Position already closed on Bybit (ErrCode: 110017). Clearing local state.")
                                    current_position = None
                                else:
                                    logger.error(f"Failed to close position: {close_order['retMsg']}")
                                    raise Exception(f"Failed to close position: {close_order['retMsg']}")
                            else:
                                trade = {
                                    'timestamp': str(datetime.now(timezone.utc)),
                                    'trading_pair': TRADING_PAIR,
                                    'timeframe': TIMEFRAME,
                                    'side': 'SHORT',
                                    'entry_price': current_position['entry_price'],
                                    'size': current_position['size'],
                                    'exit_price': latest_close,
                                    'profit_loss': (current_position['entry_price'] - latest_close) * current_position[
                                        'size'],
                                    'trend': latest_trend,
                                    'order_id': close_order['result']['orderId'],
                                    'stop_loss_order_id': current_position.get('stop_loss_order_id')
                                }
                                trade_history.append(trade)
                                log_trade(conn, trade)
                                bybit_position = sync_position_with_bybit(bybit_client, BYBIT_TRADING_PAIR)
                                if not bybit_position:
                                    logger.info("Position successfully closed. Opening new LONG position.")
                                    current_position = None
                                else:
                                    logger.warning(f"Position still exists after closing attempt: {bybit_position}")
                                    current_position = bybit_position
                                    display_trade_summary(current_position, latest_close, latest_line_st)
                                    return
                        except Exception as e:
                            if "current position is zero" in str(e):
                                logger.warning(
                                    f"Position already closed on Bybit (ErrCode: 110017). Clearing local state.")
                                current_position = None
                            else:
                                raise e

                        if current_position is None:
                            logger.debug("Opening new LONG position")
                            market_order = bybit_client.place_order(
                                category="linear",
                                symbol=BYBIT_TRADING_PAIR,
                                side="Buy",
                                orderType="Market",
                                qty=str(adjusted_quantity)
                            )
                            logger.debug(f"Market order response: {market_order}")
                            if market_order['retCode'] != 0:
                                raise Exception(f"Failed to place market order: {market_order['retMsg']}")

                            # Sync position immediately to confirm it was opened
                            bybit_position = sync_position_with_bybit(bybit_client, BYBIT_TRADING_PAIR)
                            if not bybit_position:
                                logger.error("Failed to confirm new LONG position after placing order.")
                                raise Exception("Failed to confirm new LONG position after placing order.")

                            # Set stop-loss immediately after confirming the position
                            stop_loss_order_id = update_stop_loss(
                                bybit_client,
                                BYBIT_TRADING_PAIR,
                                'LONG',
                                adjusted_stop_price,
                                None,
                                latest_close,
                                POSITION_SIZE,
                                is_new_position=True
                            )
                            current_position = {
                                'side': 'LONG',
                                'entry_price': bybit_position['entry_price'],
                                'size': adjusted_quantity,
                                'stop_loss': adjusted_stop_price,
                                'trend': latest_trend,
                                'open_time': str(datetime.now(timezone.utc)),
                                'order_id': market_order['result']['orderId'],
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
                        logger.debug("Stop-loss not detected in current position. Checking Bybit directly.")
                        orders = bybit_client.get_open_orders(category="linear", symbol=BYBIT_TRADING_PAIR)
                        logger.debug(f"Open orders for stop-loss check: {orders}")
                        if orders['retCode'] == 0:
                            stop_loss_order = next(
                                (order for order in orders['result']['list']
                                 if order['stopOrderType'] == 'StopLoss' and
                                 order['side'] == ('Sell' if current_position['side'] == 'LONG' else 'Buy')),
                                None
                            )
                            if stop_loss_order:
                                logger.info(f"Found existing stop-loss order on Bybit: {stop_loss_order}")
                                current_sl = float(stop_loss_order['triggerPrice'])
                                current_position['stop_loss'] = current_sl
                                current_position['stop_loss_order_id'] = stop_loss_order['orderId']
                    if current_sl is None or abs(current_sl - adjusted_stop_price) > epsilon:
                        logger.debug("Updating stop-loss due to mismatch or missing SL")
                        new_stop_loss_order_id = update_stop_loss(bybit_client, BYBIT_TRADING_PAIR,
                                                                  current_position['side'],
                                                                  adjusted_stop_price,
                                                                  current_position.get('stop_loss_order_id'),
                                                                  latest_close, current_position['size'])
                        if new_stop_loss_order_id:
                            current_position['stop_loss'] = adjusted_stop_price
                            current_position['stop_loss_order_id'] = new_stop_loss_order_id
                            logger.info(f"{Fore.YELLOW}Updated stop-loss to {adjusted_stop_price:.2f}{Style.RESET_ALL}")
                            # Update only the stop-loss in the database without closing the trade
                            c = conn.cursor()
                            c.execute("""
                                UPDATE trades
                                SET stop_loss = ?, stop_loss_order_id = ?
                                WHERE order_id = ? AND exit_price IS NULL
                            """, (
                                current_position['stop_loss'],
                                current_position['stop_loss_order_id'],
                                current_position['order_id']
                            ))
                            conn.commit()
                            logger.debug(f"Updated stop-loss in database for order_id={current_position['order_id']}")
                    else:
                        logger.debug(
                            f"Stop-loss unchanged: {current_sl:.2f} (within epsilon of {adjusted_stop_price:.2f})")

                if current_position and current_position.get('stop_loss') is not None:
                    logger.debug(
                        f"Checking stop-loss trigger: side={current_position['side']}, latest_close={latest_close}, stop_loss={current_position['stop_loss']}")
                    if (current_position['side'] == 'LONG' and latest_close <= current_position['stop_loss']) or \
                            (current_position['side'] == 'SHORT' and latest_close >= current_position['stop_loss']):
                        logger.debug("Stop-loss triggered, closing position")
                        cancel_all_stop_loss_orders(bybit_client, BYBIT_TRADING_PAIR)
                        close_side = 'Sell' if current_position['side'] == 'LONG' else 'Buy'
                        previous_side = current_position['side']  # Store the side before closing
                        try:
                            close_order = bybit_client.place_order(
                                category="linear",
                                symbol=BYBIT_TRADING_PAIR,
                                side=close_side,
                                orderType="Market",
                                qty=str(adjusted_quantity),
                                reduceOnly=True
                            )
                            logger.debug(f"Close position response: {close_order}")
                            if close_order['retCode'] != 0:
                                if close_order['retCode'] == 110017:  # Current position is zero
                                    logger.warning(
                                        f"Position already closed on Bybit (ErrCode: 110017). Clearing local state.")
                                    current_position = None
                                else:
                                    logger.error(f"Failed to close position: {close_order['retMsg']}")
                                    raise Exception(f"Failed to close position: {close_order['retMsg']}")
                            else:
                                trade = {
                                    'timestamp': str(datetime.now(timezone.utc)),
                                    'trading_pair': TRADING_PAIR,
                                    'timeframe': TIMEFRAME,
                                    'side': previous_side,
                                    'entry_price': current_position['entry_price'],
                                    'size': current_position['size'],
                                    'exit_price': latest_close,
                                    'stop_loss': current_position.get('stop_loss'),
                                    'profit_loss': (latest_close - current_position['entry_price']) * current_position[
                                        'size'] if previous_side == 'LONG' else (current_position[
                                                                                     'entry_price'] - latest_close) *
                                                                                current_position['size'],
                                    'trend': latest_trend,
                                    'order_id': close_order['result']['orderId'],
                                    'stop_loss_order_id': current_position.get('stop_loss_order_id')
                                }
                                trade_history.append(trade)
                                log_trade(conn, trade)
                                bybit_position = sync_position_with_bybit(bybit_client, BYBIT_TRADING_PAIR)
                                if not bybit_position:
                                    logger.info(
                                        "Position successfully closed. Awaiting manual intervention to resume trading.")
                                    current_position = None
                                else:
                                    logger.warning(f"Position still exists after stop-loss trigger: {bybit_position}")
                                    current_position = bybit_position
                                    display_trade_summary(current_position, latest_close, latest_line_st)
                                    return
                        except Exception as e:
                            if "current position is zero" in str(e):
                                logger.warning(
                                    f"Position already closed on Bybit (ErrCode: 110017). Clearing local state.")
                                current_position = None
                            else:
                                raise e

            if not current_position and ((force_first_trade and closed_candle_count == 1) or (
                    previous_trend is not None and previous_trend != latest_trend)):
                logger.debug(
                    f"Opening new position: force_first_trade={force_first_trade}, closed_candle_count={closed_candle_count}, previous_trend={previous_trend}, latest_trend={latest_trend}")
                if latest_trend ==


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
            ws.close()
            logger.info(f"{Fore.YELLOW}Reconnecting in {delay} seconds...{Style.RESET_ALL}")
            time.sleep(delay)
            ws.run_forever()
            logger.info(f"{Fore.GREEN}WebSocket reconnected successfully{Style.RESET_ALL}")
            break
        except Exception as e:
            logger.error(f"{Fore.RED}Reconnection failed: {str(e)}{Style.RESET_ALL}")
            delay = min(delay * 2, 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bybit Futures Trading Bot")
    parser.add_argument('--force-first-trade', action='store_true', help="Force trade on first closed candle")
    args = parser.parse_args()
    force_first_trade = args.force_first_trade
    logger.debug(f"Starting bot with force_first_trade={force_first_trade}")

    conn = init_db()
    websocket_url = "wss://fstream.binance.com/stream"
    logger.warning(
        f"{Fore.YELLOW}WARNING: Now trading LIVE on Bybit Futures mainnet! Force First Trade: {force_first_trade}{Style.RESET_ALL}")
    ws = websocket.WebSocketApp(websocket_url, on_open=on_open, on_message=on_message, on_error=on_error,
                                on_close=on_close)
    ws.run_forever()
    conn.close()