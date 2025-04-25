import logging
import json
import websocket
import pandas as pd
import time
import numpy as np
from binance.um_futures import UMFutures  # Keep Binance client for kline fetching
import sqlite3
from datetime import datetime, timezone
from colorama import init, Fore, Style
import argparse
import os
from tenacity import retry, wait_exponential, stop_after_attempt
import requests
import hmac
import hashlib
import okx.Trade as OKXTrade  # OKX API for trading
import okx.Account as OKXAccount  # OKX API for account/position info

# Initialize colorama for colored terminal output
init()

# Script Version
SCRIPT_VERSION = "2.8.2"

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
            'binance_api_key': config.get('binance_api_key', 'your_api_key'),
            'binance_api_secret': config.get('binance_api_secret', 'your_api_secret'),
            'okx_api_key': config.get('okx_api_key', 'your_okx_api_key'),
            'okx_api_secret': config.get('okx_api_secret', 'your_okx_api_secret'),
            'okx_passphrase': config.get('okx_passphrase', 'your_okx_passphrase'),
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
            'okx_api_key': 'your_okx_api_key',
            'okx_api_secret': 'your_okx_api_secret',
            'okx_passphrase': 'your_okx_passphrase',
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
OKX_API_KEY = config['okx_api_key']
OKX_API_SECRET = config['okx_api_secret']
OKX_PASSPHRASE = config['okx_passphrase']
ATR_PERIOD = config['atr_period']
ATR_RATIO = config['atr_ratio']
POSITION_SIZE = config['position_size']
TRADING_PAIR = config['trading_pair']
OKX_TRADING_PAIR = TRADING_PAIR.replace("USDT", "-USDT-SWAP")  # Convert to OKX format (e.g., XRP-USDT-SWAP)
TIMEFRAME = config['timeframe']
STOP_LOSS_OFFSET = config['stop_loss_offset']

# Initialize Binance client for kline fetching
binance_client = UMFutures(key=BINANCE_API_KEY, secret=BINANCE_API_SECRET, base_url="https://fapi.binance.com")

# Initialize OKX clients for trading
okx_trade_api = OKXTrade.TradeAPI(
    api_key=OKX_API_KEY,
    api_secret_key=OKX_API_SECRET,
    passphrase=OKX_PASSPHRASE,
    flag="0"  # "0" for live trading, "1" for demo trading
)
okx_account_api = OKXAccount.AccountAPI(
    api_key=OKX_API_KEY,
    api_secret_key=OKX_API_SECRET,
    passphrase=OKX_PASSPHRASE,
    flag="0"
)

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

# Load or fetch symbol configuration (for OKX)
def load_symbol_config(symbol, trade_api):
    symbol_configs = {}
    if os.path.exists(SYMBOL_CONFIG_FILE):
        with open(SYMBOL_CONFIG_FILE, 'r') as f:
            symbol_configs = json.load(f)

    if symbol not in symbol_configs:
        try:
            instruments = trade_api.get_instruments(instType="SWAP", instId=symbol)
            if instruments['code'] != "0":
                raise Exception(f"Failed to fetch instrument info: {instruments['msg']}")
            instrument = instruments['data'][0]
            symbol_configs[symbol] = {
                'quantityPrecision': int(instrument['lotSz']),  # OKX uses lotSz for quantity precision
                'pricePrecision': int(instrument['tickSz'].split('.')[1].count('0') + 1 if '.' in instrument['tickSz'] else 0),
                'minQty': float(instrument['minSz']),
                'minNotional': float(instrument.get('minSz', 5.0))  # OKX doesn't have minNotional, using minSz as proxy
            }
            with open(SYMBOL_CONFIG_FILE, 'w') as f:
                json.dump(symbol_configs, f, indent=4)
        except Exception as e:
            logger.error(
                f"{Fore.RED}Failed to fetch symbol info for {symbol}: {str(e)}. Using defaults.{Style.RESET_ALL}")
            symbol_configs[symbol] = {'quantityPrecision': 3, 'pricePrecision': 2, 'minQty': 0.001, 'minNotional': 5.0}
    return symbol_configs[symbol]

# Adjust quantity to match OKX precision and minimum size
def adjust_quantity(quantity, symbol_config, price):
    precision = symbol_config['quantityPrecision']
    min_qty = symbol_config['minQty']
    min_notional = symbol_config['minNotional']
    min_qty_notional = max(min_qty, min_notional / price)
    adjusted = max(round(quantity, precision), min_qty_notional)
    return adjusted

# Adjust price to match OKX precision
def adjust_price(price, symbol_config):
    precision = symbol_config['pricePrecision']
    return round(price, precision)

# Sync position with OKX with retry logic
@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5))
def sync_position_with_okx(account_api, trade_api, symbol):
    try:
        logger.debug("Step 1: Fetching position risk from OKX")
        positions = account_api.get_positions(instType="SWAP", instId=symbol)
        if positions['code'] != "0":
            raise Exception(f"Failed to fetch positions: {positions['msg']}")
        position_data = positions['data']
        position = next((pos for pos in position_data if pos['instId'] == symbol and float(pos['pos']) != 0), None)
        logger.debug(f"Step 2: Selected position: {position}")
        if not position:
            logger.info(f"No open position found for {symbol}")
            return None

        logger.debug("Step 3: Extracting position details")
        side = 'LONG' if float(position['pos']) > 0 else 'SHORT'
        entry_price = float(position['avgPx'])
        size = abs(float(position['pos']))

        logger.debug("Step 4: Fetching all open orders from OKX")
        orders = trade_api.get_order_list(instType="SWAP", instId=symbol)
        if orders['code'] != "0":
            raise Exception(f"Failed to fetch orders: {orders['msg']}")
        open_orders = orders['data']
        stop_loss_order = next(
            (order for order in open_orders
             if order['ordType'] == 'oco' and order['state'] == 'live' and
             ((side == 'LONG' and order['slTriggerPx'] and float(order['slTriggerPx']) < float(order['px'])) or
              (side == 'SHORT' and order['slTriggerPx'] and float(order['slTriggerPx']) > float(order['px'])))),
            None
        )
        logger.debug(f"Step 5: Stop-loss order: {stop_loss_order}")
        stop_loss = float(stop_loss_order['slTriggerPx']) if stop_loss_order else None
        stop_loss_order_id = stop_loss_order['ordId'] if stop_loss_order else None

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
def cancel_all_stop_loss_orders(trade_api, symbol):
    try:
        orders = trade_api.get_order_list(instType="SWAP", instId=symbol)
        if orders['code'] != "0":
            raise Exception(f"Failed to fetch orders: {orders['msg']}")
        open_orders = orders['data']
        stop_loss_orders = [order for order in open_orders if order['ordType'] == 'oco' and order['state'] == 'live']
        if not stop_loss_orders:
            logger.debug(f"No stop-loss orders to cancel for {symbol}")
            return True
        for order in stop_loss_orders:
            cancel_result = trade_api.cancel_order(instType="SWAP", instId=symbol, ordId=order['ordId'])
            if cancel_result['code'] != "0":
                raise Exception(f"Failed to cancel order: {cancel_result['msg']}")
            logger.debug(f"Canceled stop-loss order ID: {order['ordId']}")
        return True
    except Exception as e:
        logger.error(f"Failed to cancel stop-loss orders: {str(e)}")
        raise

# Update stop-loss order on OKX with retry logic
@retry(wait=wait_exponential(multiplier=1, min=4, max=10), stop=stop_after_attempt(5))
def update_stop_loss(trade_api, symbol, side, new_stop_price, current_stop_order_id, current_price):
    try:
        symbol_config = load_symbol_config(symbol, trade_api)
        new_stop_price = adjust_price(new_stop_price, symbol_config)

        if side == 'LONG' and new_stop_price >= current_price:
            new_stop_price = current_price - 0.01
        elif side == 'SHORT' and new_stop_price <= current_price:
            new_stop_price = current_price + 0.01

        if current_stop_order_id:
            if not cancel_all_stop_loss_orders(trade_api, symbol):
                raise Exception("Failed to cancel existing stop-loss orders")

        # Place a new stop-loss order (OCO order on OKX)
        order_side = 'sell' if side == 'LONG' else 'buy'
        stop_order = trade_api.place_order(
            instId=symbol,
            tdMode="cross",  # Cross margin mode
            side=order_side,
            posSide="net",
            ordType="oco",
            slTriggerPx=str(new_stop_price),
            slOrdPx="-1",  # Market order when stop-loss triggered
            sz="0",  # Size will be determined by closePosition
            ccy="USDT"
        )
        if order_side['code'] != "0":
            raise Exception(f"Failed to place stop-loss order: {order_side['msg']}")
        logger.info(f"Placed new stop-loss order ID: {order_side['data'][0]['ordId']} at {new_stop_price}")
        return order_side['data'][0]['ordId']
    except Exception as e:
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

    binance_position = sync_position_with_okx(okx_account_api, okx_trade_api, OKX_TRADING_PAIR)
    if binance_position:
        current_position = binance_position
        logger.info(
            f"Synced position from OKX: Side: {current_position['side']}, Entry Price: {current_position['entry_price']:.2f}, Stop Loss: {current_position.get('stop_loss', 'None')}")
        c.execute("UPDATE trades SET exit_price = 0 WHERE trading_pair = ? AND exit_price IS NULL", (TRADING_PAIR,))
        if c.rowcount > 0:
            logger.info(f"Closed {c.rowcount} stale active trades in database for {TRADING_PAIR}")
        conn.commit()

        c.execute(
            "INSERT INTO trades (timestamp, trading_pair, timeframe, side, entry_price, size, stop_loss, stop_loss_order_id, order_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (current_position['open_time'], TRADING_PAIR, TIMEFRAME, current_position['side'],
             current_position['entry_price'], current_position['size'], current_position.get('stop_loss'),
             current_position.get('stop_loss_order_id'), current_position.get('order_id'))
        )
        conn.commit()
        logger.info(f"Inserted new active trade into database: Side: {current_position['side']}, Entry Price: {current_position['entry_price']:.2f}")
    else:
        c.execute("UPDATE trades SET exit_price = 0 WHERE trading_pair = ? AND exit_price IS NULL", (TRADING_PAIR,))
        if c.rowcount > 0:
            logger.info(f"Closed {c.rowcount} stale active trades in database for {TRADING_PAIR} (no position on OKX)")
        conn.commit()
        current_position = None
        logger.info("No active position found on OKX or in database")

    return conn

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
            f"Size: {position['size']:.4f}, Current Price: {latest_close:.2f}, Profit/Loss: {profit_loss:.2f} USDT, "
            f"ST_LINE: {line_st:.2f}, Stop Loss: {stop_loss_display}{Style.RESET_ALL}"
        )
    else:
        logger.info(f"{Fore.CYAN}No active position.{Style.RESET_ALL}")

# Fetch historical kline data with retry and timeout (from Binance)
@retry(wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_historical_data(symbol=TRADING_PAIR, interval=TIMEFRAME, limit=1000, end_time=None, num_batches=2):
    global kline_data, historical_data_fetched
    try:
        mainnet_client = UMFutures(key=BINANCE_API_KEY, secret=BINANCE_API_SECRET, base_url="https://fapi.binance.com")
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
            end_time = int(batch_data['timestamp'].iloc[0].timestamp() * 1000) - 1
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

# Indicator functions (unchanged)
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

# WebSocket callback functions (unchanged for kline fetching from Binance)
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
        if 'data' in data and 'k' in data['data']:
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
            kline_data = kline_data.tail(2000)  # Keep up to 2000 candles

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
                symbol_config = load_symbol_config(OKX_TRADING_PAIR, okx_trade_api)
                adjusted_quantity = adjust_quantity(POSITION_SIZE, symbol_config, latest_close)
                adjusted_stop_price = adjust_price(stop_loss, symbol_config)

                if current_position:
                    if current_position['side'] == 'LONG' and latest_trend == -1:
                        cancel_all_stop_loss_orders(okx_trade_api, OKX_TRADING_PAIR)
                        close_order = okx_trade_api.close_positions(
                            instId=OKX_TRADING_PAIR,
                            mgnMode="cross",
                            posSide="net"
                        )
                        if close_order['code'] != "0":
                            raise Exception(f"Failed to close position: {close_order['msg']}")
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
                            'order_id': close_order['data'][0]['ordId']
                        }
                        trade_history.append(trade)
                        log_trade(conn, trade)
                        market_order = okx_trade_api.place_order(
                            instId=OKX_TRADING_PAIR,
                            tdMode="cross",
                            side="sell",
                            posSide="net",
                            ordType="market",
                            sz=str(adjusted_quantity)
                        )
                        if market_order['code'] != "0":
                            raise Exception(f"Failed to place market order: {market_order['msg']}")
                        stop_loss_order_id = update_stop_loss(okx_trade_api, OKX_TRADING_PAIR, 'SHORT', adjusted_stop_price, None, latest_close)
                        current_position = {
                            'side': 'SHORT',
                            'entry_price': latest_close,
                            'size': adjusted_quantity,
                            'stop_loss': adjusted_stop_price,
                            'trend': latest_trend,
                            'open_time': str(datetime.now(timezone.utc)),
                            'order_id': market_order['data'][0]['ordId'],
                            'stop_loss_order_id': stop_loss_order_id
                        }
                        logger.info(
                            f"{Fore.GREEN}Reversed to SHORT at {latest_close:.2f}, Stop Loss: {adjusted_stop_price:.2f}{Style.RESET_ALL}")

                    elif current_position['side'] == 'SHORT' and latest_trend == 1:
                        cancel_all_stop_loss_orders(okx_trade_api, OKX_TRADING_PAIR)
                        close_order = okx_trade_api.close_positions(
                            instId=OKX_TRADING_PAIR,
                            mgnMode="cross",
                            posSide="net"
                        )
                        if close_order['code'] != "0":
                            raise Exception(f"Failed to close position: {close_order['msg']}")
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
                            'order_id': close_order['data'][0]['ordId']
                        }
                        trade_history.append(trade)
                        log_trade(conn, trade)
                        market_order = okx_trade_api.place_order(
                            instId=OKX_TRADING_PAIR,
                            tdMode="cross",
                            side="buy",
                            posSide="net",
                            ordType="market",
                            sz=str(adjusted_quantity)
                        )
                        if market_order['code'] != "0":
                            raise Exception(f"Failed to place market order: {market_order['msg']}")
                        stop_loss_order_id = update_stop_loss(okx_trade_api, OKX_TRADING_PAIR, 'LONG', adjusted_stop_price, None, latest_close)
                        current_position = {
                            'side': 'LONG',
                            'entry_price': latest_close,
                            'size': adjusted_quantity,
                            'stop_loss': adjusted_stop_price,
                            'trend': latest_trend,
                            'open_time': str(datetime.now(timezone.utc)),
                            'order_id': market_order['data'][0]['ordId'],
                            'stop_loss_order_id': stop_loss_order_id
                        }
                        logger.info(
                            f"{Fore.GREEN}Reversed to LONG at {latest_close:.2f}, Stop Loss: {adjusted_stop_price:.2f}{Style.RESET_ALL}")

                    elif current_position['side'] == ('LONG' if latest_trend == 1 else 'SHORT'):
                        logger.debug(f"Current SL: {current_position.get('stop_loss')}, Adjusted SL: {adjusted_stop_price}")
                        if current_position.get('stop_loss') != adjusted_stop_price:
                            new_stop_loss_order_id = update_stop_loss(okx_trade_api, OKX_TRADING_PAIR, current_position['side'], adjusted_stop_price, current_position.get('stop_loss_order_id'), latest_close)
                            if new_stop_loss_order_id:
                                current_position['stop_loss'] = adjusted_stop_price
                                current_position['stop_loss_order_id'] = new_stop_loss_order_id
                                logger.info(f"{Fore.YELLOW}Updated stop-loss to {adjusted_stop_price:.2f}{Style.RESET_ALL}")

                    if current_position and current_position.get('stop_loss') is not None:
                        if (current_position['side'] == 'LONG' and latest_close <= current_position['stop_loss']) or \
                           (current_position['side'] == 'SHORT' and latest_close >= current_position['stop_loss']):
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
                                'order_id': current_position.get('order_id'),
                                'stop_loss_order_id': current_position.get('stop_loss_order_id')
                            }
                            trade_history.append(trade)
fulfilled, trade_history, conn, previous_trend
    try:
        data = json.loads(message)
        if 'data' in data and 'k' in data['data']:
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
            kline_data = kline_data.tail(2000)  # Keep up to 2000 candles

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
                symbol_config = load_symbol_config(OKX_TRADING_PAIR, okx_trade_api)
                adjusted_quantity = adjust_quantity(POSITION_SIZE, symbol_config, latest_close)
                adjusted_stop_price = adjust_price(stop_loss, symbol_config)

                if current_position:
                    if current_position['side'] == 'LONG' and latest_trend == -1:
                        cancel_all_stop_loss_orders(okx_trade_api, OKX_TRADING_PAIR)
                        close_order = okx_trade_api.close_positions(
                            instId=OKX_TRADING_PAIR,
                            mgnMode="cross",
                            posSide="net"
                        )
                        if close_order['code'] != "0":
                            raise Exception(f"Failed to close position: {close_order['msg']}")
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
                            'order_id': close_order['data'][0]['ordId']
                        }
                        trade_history.append(trade)
                        log_trade(conn, trade)
                        market_order = okx_trade_api.place_order(
                            instId=OKX_TRADING_PAIR,
                            tdMode="cross",
                            side="sell",
                            posSide="net",
                            ordType="market",
                            sz=str(adjusted_quantity)
                        )
                        if market_order['code'] != "0":
                            raise Exception(f"Failed to place market order: {market_order['msg']}")
                        stop_loss_order_id = update_stop_loss(okx_trade_api, OKX_TRADING_PAIR, 'SHORT', adjusted_stop_price, None, latest_close)
                        current_position = {
                            'side': 'SHORT',
                            'entry_price': latest_close,
                            'size': adjusted_quantity,
                            'stop_loss': adjusted_stop_price,
                            'trend': latest_trend,
                            'open_time': str(datetime.now(timezone.utc)),
                            'order_id': market_order['data'][0]['ordId'],
                            'stop_loss_order_id': stop_loss_order_id
                        }
                        logger.info(
                            f"{Fore.GREEN}Reversed to SHORT at {latest_close:.2f}, Stop Loss: {adjusted_stop_price:.2f}{Style.RESET_ALL}")

                    elif current_position['side'] == 'SHORT' and latest_trend == 1:
                        cancel_all_stop_loss_orders(okx_trade_api, OKX_TRADING_PAIR)
                        close_order = okx_trade_api.close_positions(
                            instId=OKX_TRADING_PAIR,
                            mgnMode="cross",
                            posSide="net"
                        )
                        if close_order['code'] != "0":
                            raise Exception(f"Failed to close position: {close_order['msg']}")
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
                            'order_id': close_order['data'][0]['ordId']
                        }
                        trade_history.append(trade)
                        log_trade(conn, trade)
                        market_order = okx_trade_api.place_order(
                            instId=OKX_TRADING_PAIR,
                            tdMode="cross",
                            side="buy",
                            posSide="net",
                            ordType="market",
                            sz=str(adjusted_quantity)
                        )
                        if market_order['code'] != "0":
                            raise Exception(f"Failed to place market order: {market_order['msg']}")
                        stop_loss_order_id = update_stop_loss(okx_trade_api, OKX_TRADING_PAIR, 'LONG', adjusted_stop_price, None, latest_close)
                        current_position = {
                            'side': 'LONG',
                            'entry_price': latest_close,
                            'size': adjusted_quantity,
                            'stop_loss': adjusted_stop_price,
                            'trend': latest_trend,
                            'open_time': str(datetime.now(timezone.utc)),
                            'order_id': market_order['data'][0]['ordId'],
                            'stop_loss_order_id': stop_loss_order_id
                        }
                        logger.info(
                            f"{Fore.GREEN}Reversed to LONG at {latest_close:.2f}, Stop Loss: {adjusted_stop_price:.2f}{Style.RESET_ALL}")

                    elif current_position['side'] == ('LONG' if latest_trend == 1 else 'SHORT'):
                        logger.debug(f"Current SL: {current_position.get('stop_loss')}, Adjusted SL: {adjusted_stop_price}")
                        if current_position.get('stop_loss') != adjusted_stop_price:
                            new_stop_loss_order_id = update_stop_loss(okx_trade_api, OKX_TRADING_PAIR, current_position['side'], adjusted_stop_price, current_position.get('stop_loss_order_id'), latest_close)
                            if new_stop_loss_order_id:
                                current_position['stop_loss'] = adjusted_stop_price
                                current_position['stop_loss_order_id'] = new_stop_loss_order_id
                                logger.info(f"{Fore.YELLOW}Updated stop-loss to {adjusted_stop_price:.2f}{Style.RESET_ALL}")

                    if current_position and current_position.get('stop_loss') is not None:
                        if (current_position['side'] == 'LONG' and latest_close <= current_position['stop_loss']) or \
                           (current_position['side'] == 'SHORT' and latest_close >= current_position['stop_loss']):
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
                                'order_id': current_position.get('order_id'),
                                'stop_loss_order_id': current_position.get('stop_loss_order_id')
                            }
                            trade_history.append(trade)
                            log_trade(conn, trade)
                            logger.info(
                                f"{Fore.RED}Stop-loss triggered: Closed {current_position['side']} at {latest_close:.2f}, P/L: {trade['profit_loss']:.2f} USDT{Style.RESET_ALL}")
                            current_position = None

                okx_position = sync_position_with_okx(okx_account_api, okx_trade_api, OKX_TRADING_PAIR)
                if okx_position:
                    if not current_position or current_position['side'] != okx_position['side'] or abs(
                            current_position['size'] - okx_position['size']) > 0.0001:
                        logger.warning(
                            f"{Fore.YELLOW}Position mismatch detected. Updating from OKX: {okx_position}{Style.RESET_ALL}")
                        current_position = okx_position
                elif okx_position is None and current_position:
                    logger.debug("No position on OKX, retaining current_position unless closed")
                else:
                    current_position = None

                logger.debug(f"Current Position: {current_position}, Latest Trend: {latest_trend}, Previous Trend: {previous_trend}")

                if not current_position and ((force_first_trade and closed_candle_count == 1) or (
                        previous_trend is not None and previous_trend != latest_trend)):
                    if latest_trend == 1:
                        market_order = okx_trade_api.place_order(
                            instId=OKX_TRADING_PAIR,
                            tdMode="cross",
                            side="buy",
                            posSide="net",
                            ordType="market",
                            sz=str(adjusted_quantity)
                        )
                        if market_order['code'] != "0":
                            raise Exception(f"Failed to place market order: {market_order['msg']}")
                        stop_loss_order_id = update_stop_loss(okx_trade_api, OKX_TRADING_PAIR, 'LONG', adjusted_stop_price, None, latest_close)
                        current_position = {
                            'side': 'LONG',
                            'entry_price': latest_close,
                            'size': adjusted_quantity,
                            'stop_loss': adjusted_stop_price,
                            'trend': latest_trend,
                            'open_time': str(datetime.now(timezone.utc)),
                            'order_id': market_order['data'][0]['ordId'],
                            'stop_loss_order_id': stop_loss_order_id
                        }
                        logger.info(
                            f"{Fore.GREEN}Opened LONG at {latest_close:.2f}, Stop Loss: {adjusted_stop_price:.2f}{Style.RESET_ALL}")
                    elif latest_trend == -1:
                        market_order = okx_trade_api.place_order(
                            instId=OKX_TRADING_PAIR,
                            tdMode="cross",
                            side="sell",
                            posSide="net",
                            ordType="market",
                            sz=str(adjusted_quantity)
                        )
                        if market_order['code'] != "0":
                            raise Exception(f"Failed to place market order: {market_order['msg']}")
                        stop_loss_order_id = update_stop_loss(okx_trade_api, OKX_TRADING_PAIR, 'SHORT', adjusted_stop_price, None, latest_close)
                        current_position = {
                            'side': 'SHORT',
                            'entry_price': latest_close,
                            'size': adjusted_quantity,
                            'stop_loss': adjusted_stop_price,
                            'trend': latest_trend,
                            'open_time': str(datetime.now(timezone.utc)),
                            'order_id': market_order['data'][0]['ordId'],
                            'stop_loss_order_id': stop_loss_order_id
                        }
                        logger.info(
                            f"{Fore.GREEN}Opened SHORT at {latest_close:.2f}, Stop Loss: {adjusted_stop_price:.2f}{Style.RESET_ALL}")

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
    parser = argparse.ArgumentParser(description="OKX Futures Trading Bot")
    parser.add_argument('--force-first-trade', action='store_true', help="Force trade on first closed candle")
    args = parser.parse_args()
    force_first_trade = args.force_first_trade

    conn = init_db()
    websocket_url = "wss://fstream.binance.com/stream"
    logger.warning(
        f"{Fore.YELLOW}WARNING: Now trading LIVE on OKX Futures mainnet! Force First Trade: {force_first_trade}{Style.RESET_ALL}")
    ws = websocket.WebSocketApp(websocket_url, on_open=on_open, on_message=on_message, on_error=on_error,
                                on_close=on_close)
    ws.run_forever()
    conn.close()
