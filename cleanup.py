import json
import logging
import os
from okxclient import TradeAPI
import redis
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('cleanup.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_config(config_path='config.json'):
    """Load configuration from JSON file."""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load config from {config_path}: {str(e)}")
        raise

def cleanup_okx_positions_and_orders(api_key, api_secret, passphrase, inst_id='XRP-USDT-SWAP'):
    """Close positions and cancel orders on OKX."""
    trade_api = TradeAPI(
        api_key=api_key,
        api_secret=api_secret,
        passphrase=passphrase,
        flag="0",  # Live trading
        debug=True
    )

    # Cancel all open limit orders
    try:
        orders = trade_api.get_order_list(instType="SWAP", instId=inst_id, ordType="limit")
        if orders['code'] == "0" and orders['data']:
            for order in orders['data']:
                if order['state'] in ['live', 'partially_filled']:
                    result = trade_api.cancel_order(instId=inst_id, ordId=order['ordId'])
                    logger.info(f"Canceled limit order {order['ordId']}: {result}")
        else:
            logger.info("No open limit orders found.")
    except Exception as e:
        logger.error(f"Error canceling limit orders: {str(e)}")

    # Cancel all algo orders (stop-loss, take-profit)
    try:
        algo_orders = trade_api.get_algo_order_list(instType="SWAP", instId=inst_id, ordType="conditional")
        if algo_orders['code'] == "0" and algo_orders['data']:
            for order in algo_orders['data']:
                result = trade_api.cancel_algo_order([{"instId": inst_id, "algoId": order['algoId']}])
                logger.info(f"Canceled algo order {order['algoId']}: {result}")
        else:
            logger.info("No open algo orders found.")
    except Exception as e:
        logger.error(f"Error canceling algo orders: {str(e)}")

    # Close open position
    try:
        result = trade_api.close_positions(instId=inst_id, mgnMode="isolated", posSide="net")
        if result['code'] == "0":
            logger.info(f"Closed position: {result}")
        else:
            logger.info(f"No open position to close: {result['msg']}")
    except Exception as e:
        logger.error(f"Error closing position: {str(e)}")

def delete_database(db_path='trade_history_v2.db'):
    """Delete the SQLite database file."""
    try:
        if os.path.exists(db_path):
            os.remove(db_path)
            logger.info(f"Deleted database: {db_path}")
        else:
            logger.info(f"Database not found: {db_path}")
    except Exception as e:
        logger.error(f"Error deleting database {db_path}: {str(e)}")

def clear_redis(redis_host='localhost', redis_port=6379, redis_db=0, queue_name='bot_v2_bot1_fill_queue'):
    """Clear the Redis queue."""
    try:
        r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
        deleted = r.delete(queue_name)
        if deleted:
            logger.info(f"Cleared Redis queue: {queue_name}")
        else:
            logger.info(f"Redis queue not found or already empty: {queue_name}")
        r.close()
    except Exception as e:
        logger.error(f"Error clearing Redis queue {queue_name}: {str(e)}")

def main():
    """Main cleanup function."""
    parser = argparse.ArgumentParser(description="Cleanup OKX positions, orders, database, and Redis.")
    parser.add_argument('--delete-db', action='store_true', help='Delete the SQLite database')
    parser.add_argument('--clear-redis', action='store_true', help='Clear the Redis queue')
    args = parser.parse_args()

    config = load_config()
    api_key = config.get('okx_api_key')
    api_secret = config.get('okx_api_secret')
    passphrase = config.get('okx_passphrase')
    inst_id = config.get('trading_pair', 'XRP-USDT-SWAP')

    if not all([api_key, api_secret, passphrase]):
        logger.error("Missing OKX API credentials in config.json")
        return

    cleanup_okx_positions_and_orders(api_key, api_secret, passphrase, inst_id)

    if args.delete_db:
        delete_database()

    if args.clear_redis:
        clear_redis()

if __name__ == "__main__":
    main()
