import json
from binance.um_futures import UMFutures
import argparse
import sys
print("Python Path:", sys.path)


def load_config(json_path):
    """
    Load the configuration from the specified JSON file.
    
    Args:
        json_path (str): Path to the JSON file.
    Returns:
        dict: Configuration data.
    """
    try:
        with open(json_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: JSON file '{json_path}' not found.")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in '{json_path}'.")
        sys.exit(1)

def initialize_client(api_key, api_secret):
    """
    Initialize the Binance Futures client with the API key and secret.
    
    Args:
        api_key (str): Binance API key.
        api_secret (str): Binance API secret.
    Returns:
        UMFutures: Binance Futures client instance.
    """
    return UMFutures(key=api_key, secret=api_secret, base_url="https://fapi.binance.com")

def get_futures_balance(client):
    """
    Fetch and print the futures account balance.
    
    Args:
        client (UMFutures): Binance Futures client instance.
    """
    try:
        account_info = client.account()
        balance = float(account_info['totalWalletBalance'])
        print(f"Futures Balance: {balance:.2f} USDT")
    except Exception as e:
        print(f"Error fetching futures balance: {str(e)}")

def get_open_positions(client):
    """
    Fetch and print any open positions in the futures account.
    
    Args:
        client (UMFutures): Binance Futures client instance.
    """
    try:
        positions = client.get_position_risk()
        open_positions = [pos for pos in positions if float(pos['positionAmt']) != 0]
        if open_positions:
            print("Open Positions:")
            for pos in open_positions:
                print(f" - {pos['symbol']}: {pos['positionAmt']} (Entry Price: {pos['entryPrice']})")
        else:
            print("No open positions.")
    except Exception as e:
        print(f"Error fetching open positions: {str(e)}")

def main():
    """
    Main function to execute the script.
    """
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Check Binance Futures balance and open positions.")
    parser.add_argument('--config', default='config.json', help='Path to the JSON configuration file')
    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)
    api_key = config['binance_api_key']
    api_secret = config['binance_api_secret']

    # Initialize Binance client
    client = initialize_client(api_key, api_secret)

    # Fetch and display futures balance and open positions
    get_futures_balance(client)
    get_open_positions(client)

if __name__ == "__main__":
    main()
