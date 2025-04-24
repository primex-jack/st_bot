import json
import logging
from binance.um_futures import UMFutures
from datetime import datetime

#print("Binance Version:", binance.um__version__)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load configuration from config.json
def load_config():
    try:
        with open('config.json', 'r') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        logger.error("Config file 'config.json' not found.")
        exit(1)

# Initialize Binance Futures client
def init_client(api_key, api_secret):
    return UMFutures(key=api_key, secret=api_secret, base_url="https://fapi.binance.com")

# Get current open position
def get_open_position(client, symbol):
    try:
        positions = client.get_position_risk(symbol=symbol, recvWindow=10000)
        position = next((pos for pos in positions if pos['symbol'] == symbol and float(pos['positionAmt']) != 0), None)
        if position:
            logger.info(f"Found open position: {position}")
            return {
                'side': 'LONG' if float(position['positionAmt']) > 0 else 'SHORT',
                'size': abs(float(position['positionAmt']))
            }
        logger.info(f"No open position found for {symbol}")
        return None
    except Exception as e:
        logger.error(f"Error fetching position: {str(e)}")
        return None

# Close position with a market order
def close_position(client, symbol, position):
    try:
        side = 'SELL' if position['side'] == 'LONG' else 'BUY'
        order = client.new_order(
            symbol=symbol,
            side=side,
            type='MARKET',
            quantity=position['size'],
            reduceOnly=True  # Ensures it only closes the position
        )
        logger.info(f"Closed {position['side']} position with order: {order}")
        return order
    except Exception as e:
        logger.error(f"Failed to close position: {str(e)}")
        return None

# Main function
def main():
    # Load config
    config = load_config()
    api_key = config['binance_api_key']
    api_secret = config['binance_api_secret']
    trading_pair = config['trading_pair']
    bot_name = config.get('bot_name', 'UnnamedBot')

    logger.info(f"Starting position close script for {bot_name} - Trading Pair: {trading_pair}")

    # Initialize client
    client = init_client(api_key, api_secret)

    # Get open position
    position = get_open_position(client, trading_pair)
    if not position:
        logger.info(f"No action needed: No open positions for {trading_pair}")
        return

    # Close the position
    order = close_position(client, trading_pair, position)
    if order:
        logger.info(f"Successfully closed position at {datetime.now()}")
    else:
        logger.error("Failed to close position. Check logs for details.")

if __name__ == "__main__":
    main()
