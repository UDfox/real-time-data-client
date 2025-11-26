#!/usr/bin/env python3
"""
Polymarket Crypto Prices Real-Time Subscription Test

This script demonstrates how to subscribe to cryptocurrency price updates
from Polymarket's real-time data WebSocket service.

Supported data sources:
1. crypto_prices - Binance source (symbols: btcusdt, ethusdt, solusdt, etc.)
2. crypto_prices_chainlink - Chainlink oracle source (symbols: btc/usd, eth/usd, etc.)

Reference: https://docs.polymarket.com/developers/RTDS/RTDS-crypto-prices

Usage:
    python test_crypto_prices.py

Requirements:
    pip install websocket-client
"""

import json
import time
from typing import Dict, Any
from polymarket_client import RealTimeDataClient, ConnectionStatus


# Available symbols for crypto_prices (Binance source)
BINANCE_SYMBOLS = [
    "btcusdt",   # Bitcoin/USDT
    "ethusdt",   # Ethereum/USDT
    "xrpusdt",   # XRP/USDT
    "solusdt",   # Solana/USDT
    "dogeusdt",  # Dogecoin/USDT
]

# Available symbols for crypto_prices_chainlink
CHAINLINK_SYMBOLS = [
    "btc/usd",   # Bitcoin/USD
    "eth/usd",   # Ethereum/USD
]


def format_price_message(message: Dict[str, Any]) -> str:
    """Format a crypto price message for display."""
    payload = message.get("payload", {})
    symbol = payload.get("symbol", "unknown")
    value = payload.get("value", 0)
    timestamp = payload.get("timestamp", 0)
    source = message.get("topic", "")
    
    # Convert timestamp to readable format
    if timestamp:
        time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp / 1000))
    else:
        time_str = "N/A"
    
    return f"[{source}] {symbol.upper()}: ${value:,.2f} @ {time_str}"


def on_message(client: RealTimeDataClient, message: Dict[str, Any]) -> None:
    """Handle incoming crypto price messages."""
    topic = message.get("topic", "")
    msg_type = message.get("type", "")
    
    if topic in ["crypto_prices", "crypto_prices_chainlink"]:
        if msg_type == "update":
            print(format_price_message(message))
        else:
            # Initial data dump or other message types
            print(f"\n{'='*60}")
            print(f"Topic: {topic}")
            print(f"Type: {msg_type}")
            print(f"Payload: {json.dumps(message.get('payload'), indent=2)}")
            print(f"{'='*60}\n")
    else:
        # Other topics
        print(f"[{topic}] {msg_type}: {message.get('payload')}")


def on_connect(client: RealTimeDataClient) -> None:
    """Handle connection establishment and subscribe to crypto prices."""
    print("\n" + "="*60)
    print("Connected to Polymarket Real-Time Data Service!")
    print("Subscribing to crypto price feeds...")
    print("="*60 + "\n")

    # Subscribe to Binance crypto prices
    # Option 1: Subscribe to ALL crypto prices (no filter)
    client.subscribe({
        "subscriptions": [
            {
                "topic": "crypto_prices",
                "type": "update",
                # No filters - receive all symbols
            }
        ]
    })

    # Option 2: Subscribe to specific symbols (uncomment to use)
    # client.subscribe({
    #     "subscriptions": [
    #         {
    #             "topic": "crypto_prices",
    #             "type": "update",
    #             "filters": "btcusdt,ethusdt,solusdt"  # Comma-separated list
    #         }
    #     ]
    # })

    # Subscribe to Chainlink crypto prices
    client.subscribe({
        "subscriptions": [
            {
                "topic": "crypto_prices_chainlink",
                "type": "*",  # Use "*" for all update types
                # Optional: filter for specific symbol
                # "filters": '{"symbol":"eth/usd"}'
            }
        ]
    })

    print("Subscribed to:")
    print("  - crypto_prices (Binance source) - All symbols")
    print("  - crypto_prices_chainlink (Chainlink source) - All symbols")
    print("\nWaiting for price updates...\n")


def on_status_change(status: ConnectionStatus) -> None:
    """Handle connection status changes."""
    status_emoji = {
        ConnectionStatus.CONNECTING: "🔄",
        ConnectionStatus.CONNECTED: "✅",
        ConnectionStatus.DISCONNECTED: "❌",
    }
    print(f"{status_emoji.get(status, '❓')} Connection status: {status.value}")


def main():
    """Main function to run the crypto price subscription test."""
    print("\n" + "="*60)
    print("Polymarket Crypto Prices Real-Time Subscription Test")
    print("="*60)
    print("\nAvailable Binance symbols:", ", ".join(BINANCE_SYMBOLS))
    print("Available Chainlink symbols:", ", ".join(CHAINLINK_SYMBOLS))
    print("\nPress Ctrl+C to disconnect and exit.\n")

    # Create and connect the client
    client = RealTimeDataClient(
        on_connect=on_connect,
        on_message=on_message,
        on_status_change=on_status_change,
        auto_reconnect=True,
    )

    client.connect()

    # Keep the main thread running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\nReceived interrupt signal. Disconnecting...")
        client.disconnect()
        print("Goodbye!")


if __name__ == "__main__":
    main()
