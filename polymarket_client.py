#!/usr/bin/env python3
"""
Polymarket Real-Time Data Client for Python

This script demonstrates how to connect to Polymarket's real-time data WebSocket service
using Python. It provides the same functionality as the TypeScript @polymarket/real-time-data-client library.

Usage:
    python polymarket_client.py

Requirements:
    pip install websocket-client

Example:
    from polymarket_client import RealTimeDataClient

    def on_message(client, message):
        print(f"Received: {message}")

    def on_connect(client):
        client.subscribe({
            "subscriptions": [
                {"topic": "activity", "type": "*"},
                {"topic": "crypto_prices", "type": "*", "filters": '{"symbol":"BTCUSDT"}'}
            ]
        })

    client = RealTimeDataClient(on_connect=on_connect, on_message=on_message)
    client.connect()
"""

import json
import threading
import time
from enum import Enum
from typing import Callable, Optional, Dict, Any

try:
    import websocket
except ImportError:
    print("Please install websocket-client: pip install websocket-client")
    raise

# Default WebSocket host
DEFAULT_HOST = "wss://ws-live-data.polymarket.com"
DEFAULT_PING_INTERVAL = 5  # seconds


class ConnectionStatus(Enum):
    """Represents websocket connection status"""
    CONNECTING = "CONNECTING"
    CONNECTED = "CONNECTED"
    DISCONNECTED = "DISCONNECTED"


class RealTimeDataClient:
    """
    A client for managing real-time WebSocket connections to Polymarket,
    handling messages, subscriptions, and automatic reconnections.
    """

    def __init__(
        self,
        on_connect: Optional[Callable[["RealTimeDataClient"], None]] = None,
        on_message: Optional[Callable[["RealTimeDataClient", Dict[str, Any]], None]] = None,
        on_status_change: Optional[Callable[[ConnectionStatus], None]] = None,
        host: str = DEFAULT_HOST,
        ping_interval: int = DEFAULT_PING_INTERVAL,
        auto_reconnect: bool = True,
    ):
        """
        Initialize the RealTimeDataClient.

        Args:
            on_connect: Callback function called when connection is established
            on_message: Callback function called when a message is received
            on_status_change: Callback function called when connection status changes
            host: WebSocket server URL
            ping_interval: Interval in seconds for sending ping messages
            auto_reconnect: Whether to automatically reconnect on disconnection
        """
        self.host = host
        self.ping_interval = ping_interval
        self.auto_reconnect = auto_reconnect
        self._on_connect = on_connect
        self._on_message = on_message
        self._on_status_change = on_status_change
        self._ws: Optional[websocket.WebSocketApp] = None
        self._ws_thread: Optional[threading.Thread] = None
        self._ping_thread: Optional[threading.Thread] = None
        self._running = False
        self._connected = False

    def _notify_status_change(self, status: ConnectionStatus) -> None:
        """Notify status change callback if set."""
        if self._on_status_change:
            self._on_status_change(status)

    def _on_ws_open(self, ws: websocket.WebSocketApp) -> None:
        """Handle WebSocket open event."""
        self._connected = True
        self._notify_status_change(ConnectionStatus.CONNECTED)
        print("Connected to Polymarket WebSocket")

        # Start ping thread
        self._start_ping_thread()

        # Call user's on_connect callback
        if self._on_connect:
            self._on_connect(self)

    def _on_ws_message(self, ws: websocket.WebSocketApp, message: str) -> None:
        """Handle incoming WebSocket messages."""
        if message:
            if "payload" in message:
                try:
                    parsed_message = json.loads(message)
                    if self._on_message:
                        self._on_message(self, parsed_message)
                except json.JSONDecodeError as e:
                    print(f"Failed to parse message: {e}")
            elif message == "pong":
                # Pong response received
                pass
            else:
                print(f"Received non-payload message: {message[:100]}")

    def _on_ws_error(self, ws: websocket.WebSocketApp, error: Exception) -> None:
        """Handle WebSocket errors."""
        print(f"WebSocket error: {error}")
        if self.auto_reconnect and self._running:
            print("Attempting to reconnect...")
            time.sleep(1)
            self._reconnect()

    def _on_ws_close(self, ws: websocket.WebSocketApp, close_status_code: int, close_msg: str) -> None:
        """Handle WebSocket close event."""
        self._connected = False
        self._notify_status_change(ConnectionStatus.DISCONNECTED)
        print(f"Disconnected. Code: {close_status_code}, Reason: {close_msg}")
        if self.auto_reconnect and self._running:
            print("Attempting to reconnect...")
            time.sleep(1)
            self._reconnect()

    def _start_ping_thread(self) -> None:
        """Start the ping thread to keep connection alive."""
        def ping_loop():
            while self._running and self._connected:
                time.sleep(self.ping_interval)
                if self._ws and self._connected:
                    try:
                        self._ws.send("ping")
                    except Exception as e:
                        print(f"Ping error: {e}")
                        break

        self._ping_thread = threading.Thread(target=ping_loop, daemon=True)
        self._ping_thread.start()

    def _reconnect(self) -> None:
        """Attempt to reconnect to the WebSocket server."""
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        self._create_connection()

    def _create_connection(self) -> None:
        """Create and start the WebSocket connection."""
        self._notify_status_change(ConnectionStatus.CONNECTING)
        self._ws = websocket.WebSocketApp(
            self.host,
            on_open=self._on_ws_open,
            on_message=self._on_ws_message,
            on_error=self._on_ws_error,
            on_close=self._on_ws_close,
        )

        self._ws_thread = threading.Thread(
            target=self._ws.run_forever,
            daemon=True
        )
        self._ws_thread.start()

    def connect(self) -> "RealTimeDataClient":
        """
        Establish a WebSocket connection to the server.

        Returns:
            self: The client instance for method chaining
        """
        self._running = True
        self._create_connection()
        return self

    def disconnect(self) -> None:
        """Close the WebSocket connection."""
        self._running = False
        self.auto_reconnect = False
        if self._ws:
            self._ws.close()
        print("Disconnected from Polymarket WebSocket")

    def subscribe(self, msg: Dict[str, Any]) -> None:
        """
        Subscribe to a data stream.

        Args:
            msg: Subscription message with 'subscriptions' list containing:
                - topic: Topic to subscribe to (e.g., 'activity', 'comments', 'crypto_prices')
                - type: Type of subscription (use '*' for all types)
                - filters: Optional filters as JSON string
                - clob_auth: Optional CLOB authentication credentials
        
        Example:
            client.subscribe({
                "subscriptions": [
                    {"topic": "activity", "type": "*"},
                    {"topic": "crypto_prices", "type": "update", "filters": '{"symbol":"BTCUSDT"}'}
                ]
            })
        """
        if not self._ws or not self._connected:
            print("Warning: Socket not open. Cannot subscribe.")
            return

        subscription_msg = {"action": "subscribe", **msg}
        try:
            self._ws.send(json.dumps(subscription_msg))
            print(f"Subscribed to: {msg}")
        except Exception as e:
            print(f"Subscribe error: {e}")

    def unsubscribe(self, msg: Dict[str, Any]) -> None:
        """
        Unsubscribe from a data stream.

        Args:
            msg: Unsubscription message with 'subscriptions' list containing topics to unsubscribe from

        Example:
            client.unsubscribe({
                "subscriptions": [
                    {"topic": "activity", "type": "trades"}
                ]
            })
        """
        if not self._ws or not self._connected:
            print("Warning: Socket not open. Cannot unsubscribe.")
            return

        unsubscription_msg = {"action": "unsubscribe", **msg}
        try:
            self._ws.send(json.dumps(unsubscription_msg))
            print(f"Unsubscribed from: {msg}")
        except Exception as e:
            print(f"Unsubscribe error: {e}")


def main():
    """Example usage of the RealTimeDataClient."""

    def on_message(client: RealTimeDataClient, message: Dict[str, Any]) -> None:
        """Handle incoming messages."""
        print(f"\n{'='*60}")
        print(f"Topic: {message.get('topic')}")
        print(f"Type: {message.get('type')}")
        print(f"Timestamp: {message.get('timestamp')}")
        print(f"Connection ID: {message.get('connection_id')}")
        print(f"Payload: {json.dumps(message.get('payload'), indent=2)}")
        print(f"{'='*60}\n")

    def on_connect(client: RealTimeDataClient) -> None:
        """Handle connection establishment."""
        print("\nConnection established! Subscribing to topics...\n")

        # Subscribe to various topics
        client.subscribe({
            "subscriptions": [
                # Activity (trades)
                {
                    "topic": "activity",
                    "type": "*",  # Use "*" to subscribe to all types
                    # Optional filters:
                    # "filters": '{"event_slug":"your-event-slug"}'
                },

                # Comments
                {
                    "topic": "comments",
                    "type": "*",
                    # Optional filters:
                    # "filters": '{"parentEntityID":20200,"parentEntityType":"Event"}'
                },

                # RFQ (Request for Quote)
                {
                    "topic": "rfq",
                    "type": "*",
                },

                # Crypto prices
                {
                    "topic": "crypto_prices",
                    "type": "*",
                    # Optional filter for specific symbol:
                    # "filters": '{"symbol":"BTCUSDT"}'
                },

                # CLOB market
                {
                    "topic": "clob_market",
                    "type": "*",
                    # Optional filters for specific markets (token IDs):
                    # "filters": '["71321045679252212594626385532706912750332728571942532289631379312455583992563"]'
                },

                # Uncomment below to subscribe to CLOB user (requires authentication)
                # {
                #     "topic": "clob_user",
                #     "type": "*",
                #     "clob_auth": {
                #         "key": "your-api-key",
                #         "secret": "your-api-secret",
                #         "passphrase": "your-passphrase"
                #     }
                # }
            ]
        })

    def on_status_change(status: ConnectionStatus) -> None:
        """Handle connection status changes."""
        print(f"Connection status: {status.value}")

    # Create and connect the client
    client = RealTimeDataClient(
        on_connect=on_connect,
        on_message=on_message,
        on_status_change=on_status_change,
        auto_reconnect=True,
    )

    print("Starting Polymarket Real-Time Data Client...")
    print(f"Connecting to: {client.host}")
    print("Press Ctrl+C to disconnect and exit.\n")

    client.connect()

    # Keep the main thread running
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nReceived interrupt signal. Disconnecting...")
        client.disconnect()
        print("Goodbye!")


if __name__ == "__main__":
    main()
