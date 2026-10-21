
import hmac
import json
import os
import threading
import time
from datetime import datetime, timezone
from html import escape

import requests
import websocket
from flask import Flask, jsonify, request

app = Flask(__name__)

DERIV_APP_ID = os.getenv("DERIV_APP_ID", "1089").strip()
DERIV_WS_URL = (
    "wss://ws.derivws.com/websockets/v3"
    f"?app_id={DERIV_APP_ID}"
)

PREFERRED_SYMBOLS = [
    "R_100", "R_75", "R_50", "R_25", "R_10",
    "1HZ100V", "1HZ75V", "1HZ50V", "1HZ25V", "1HZ10V",
    "stpRNG", "stpRNG2", "stpRNG3",
    "frxXAUUSD", "XAUUSD",
]

lock = threading.RLock()
started_at = time.time()
last_telegram_test = 0.0

state = {
    "service": "Sniper V3 Pro",
    "mode": "diagnostics_only",
    "signals_enabled": False,
    "deriv_connected": False,
    "catalogue_loaded": False,
    "telegram_configured": False,
    "last_message": "Waiting for Deriv connection",
    "last_tick_at": None,
    "instruments": {},
    "available_candidate_count": 0,
    "subscribed_count": 0,
    "errors": [],
}


def record_error(message):
    with lock:
        state["errors"].append({
            "time": datetime.now(timezone.utc).isoformat(),
            "message": str(message)[:300],
        })
        state["errors"] = state["errors"][-10:]
        state["last_message"] = str(message)[:300]


def choose_instruments(catalogue):
    by_symbol = {}

    for item in catalogue:
        symbol = item.get("symbol")
        if not symbol:
            continue
        if item.get("is_trading_suspended") is True:
            continue
        by_symbol[symbol] = item

    chosen = [
        symbol for symbol in PREFERRED_SYMBOLS
        if symbol in by_symbol
    ]

    for symbol, item in by_symbol.items():
        display = str(item.get("display_name", "")).lower()
        market = str(item.get("market", "")).lower()

        if (
            "step" in display
            or "step" in market
            or "gold" in display
            or "xau" in symbol.lower()
        ):
            if symbol not in chosen:
                chosen.append(symbol)

    return chosen[:14], by_symbol


def on_open(ws):
    with lock:
        state["deriv_connected"] = True
        state["last_message"] = "Connected; requesting market catalogue"

    ws.send(json.dumps({
        "active_symbols": "brief",
        "product_type": "basic",
        "req_id": 1,
    }))


def on_message(ws, message):
    try:
        data = json.loads(message)
    except (TypeError, json.JSONDecodeError):
        record_error("Received an unreadable Deriv message")
        return

    if data.get("error"):
        error = data["error"]
        record_error(
            "Deriv API error: "
            f"{error.get('code', 'unknown')} - "
            f"{error.get('message', 'No description')}"
        )
        return

    msg_type = data.get("msg_type")

    if msg_type == "active_symbols":
        catalogue = data.get("active_symbols", [])
        chosen, by_symbol = choose_instruments(catalogue)

        with lock:
            state["catalogue_loaded"] = True
            state["available_candidate_count"] = len(chosen)
            state["instruments"] = {
                symbol: {
                    "display_name": by_symbol[symbol].get(
                        "display_name", symbol
                    ),
                    "pip_precision": by_symbol[symbol].get("pip"),
                    "subscribed": False,
                    "tick_count": 0,
                    "last_price": None,
                    "last_tick_at": None,
                }
                for symbol in chosen
            }
            state["last_message"] = (
                f"Catalogue loaded; found {len(chosen)} candidates"
            )

        for index, symbol in enumerate(chosen, start=10):
            try:
                ws.send(json.dumps({
                    "ticks": symbol,
                    "subscribe": 1,
                    "req_id": index,
                }))
            except Exception as exc:
                record_error(f"Subscription failed for {symbol}: {exc}")

        with lock:
            state["subscribed_count"] = len(chosen)

        if not chosen:
            record_error("No preferred candidate markets were found")
        return

    if msg_type == "tick":
        tick = data.get("tick", {})
        symbol = tick.get("symbol")
        price = tick.get("quote")
        epoch = tick.get("epoch")

        if not symbol or price is None:
            return

        if isinstance(epoch, (int, float)):
            tick_time = datetime.fromtimestamp(
                epoch, timezone.utc
            ).isoformat()
        else:
            tick_time = datetime.now(timezone.utc).isoformat()

        with lock:
            instrument = state["instruments"].get(symbol)
            if instrument is not None:
                instrument["tick_count"] += 1
                instrument["last_price"] = price
                instrument["last_tick_at"] = tick_time
                instrument["subscribed"] = True
                state["last_tick_at"] = tick_time
                state["last_message"] = "Receiving market ticks"
        return

    if msg_type == "subscription":
        subscription = data.get("subscription", {})
        if subscription.get("id"):
            with lock:
                state["last_message"] = "Market subscription confirmed"


def on_error(ws, error):
    with lock:
        state["deriv_connected"] = False
    record_error(f"WebSocket error: {error}")


def on_close(ws, close_status_code, close_message):
    with lock:
        state["deriv_connected"] = False
    record_error(
        f"Deriv connection closed: {close_status_code} "
        f"{close_message or ''}"
    )


def deriv_connection_loop():
    while True:
        try:
            client = websocket.WebSocketApp(
                DERIV_WS_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            client.run_forever(
                ping_interval=30,
                ping_timeout=10,
            )
        except Exception as exc:
            record_error(f"Connection loop error: {exc}")

        with lock:
            state["deriv_connected"] = False

        time.sleep(5)


def send_telegram_message(message):
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    if not token or not chat_id:
        return False, "Telegram credentials are not configured"

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    try:
        response = requests.post(
            url,
            json={
                "chat_id": chat_id,
                "text": message,
            },
            timeout=15,
        )
        result = response.json()

        if response.ok and result.get("ok") is True:
            return True, "Telegram accepted the message"

        description = result.get("description", "Telegram rejected message")
        return False, str(description)[:200]

    except (requests.RequestException, ValueError) as exc:
        return False, f"Telegram request failed: {str(exc)[:150]}"


def start_background_connection():
    thread = threading.Thread(
        target=deriv_connection_loop,
        name="deriv-market-data",
        daemon=True,
    )
    thread.start()


@app.route("/")
def home():
    return (
        "<!doctype html><html><head>"
        "<meta name='viewport' content='width=device-width, initial-scale=1'>"
        "<title>Sniper V3 Pro</title></head><body>"
        "<h1>Sniper V3 Pro</h1>"
        "<p>Diagnostics service is running.</p>"
        "<p>Live trading signals are disabled.</p>"
        "<p><a href='/health'>Health</a> | "
        "<a href='/status'>Status</a> | "
        "<a href='/telegram-config'>Telegram configuration</a></p>"
        "<p><a href='/test-telegram'>Open Telegram test</a></p>"
        "</body></html>"
    )


@app.route("/health")
def health():
    with lock:
        connected = state["deriv_connected"]

    return jsonify({
        "ok": True,
        "service": "Sniper V3 Pro",
        "deriv_connected": connected,
        "signals_enabled": False,
    }), 200


@app.route("/status")
def status():
    with lock:
        result = json.loads(json.dumps(state))

    result["uptime_seconds"] = int(time.time() - started_at)
    now = datetime.now(timezone.utc)

    for instrument in result["instruments"].values():
        tick_time = instrument.get("last_tick_at")
        if tick_time:
            try:
                tick_dt = datetime.fromisoformat(
                    tick_time.replace("Z", "+00:00")
                )
                instrument["last_tick_age_seconds"] = max(
                    0, int((now - tick_dt).total_seconds())
                )
            except (ValueError, TypeError):
                instrument["last_tick_age_seconds"] = None
        else:
            instrument["last_tick_age_seconds"] = None

    return jsonify(result), 200


@app.route("/telegram-config")
def telegram_config():
    token_present = bool(
        os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    )
    chat_id_present = bool(
        os.getenv("TELEGRAM_CHAT_ID", "").strip()
    )

    return jsonify({
        "telegram_token_configured": token_present,
        "telegram_chat_id_configured": chat_id_present,
        "test_key_configured": bool(
            os.getenv("TEST_KEY", "").strip()
        ),
        "ready_to_test": (
            token_present
            and chat_id_present
            and bool(os.getenv("TEST_KEY", "").strip())
        ),
    }), 200


@app.route("/test-telegram", methods=["GET", "POST"])
def test_telegram():
    global last_telegram_test

    if request.method == "GET":
        return (
            "<!doctype html><html><head>"
            "<meta name='viewport' content='width=device-width, initial-scale=1'>"
            "<title>Telegram Test</title></head><body>"
            "<h1>Sniper V3 Pro - Telegram Test</h1>"
            "<p>This sends one test message to your configured chat.</p>"
            "<form method='post' action='/test-telegram'>"
            "<label>Test key: "
            "<input name='test_key' type='password' required "
            "autocomplete='off'></label>"
            "<button type='submit'>Send test message</button>"
            "</form></body></html>"
        )

    expected_key = os.getenv("TEST_KEY", "").strip()
    supplied_key = request.form.get("test_key", "").strip()

    if not expected_key:
        return "TEST_KEY has not been configured in Render.", 503

    if not hmac.compare_digest(supplied_key, expected_key):
        return "Unauthorized. Check your test key.", 401

    with lock:
        now = time.time()
        if now - last_telegram_test < 30:
            return "Please wait 30 seconds before another test.", 429
        last_telegram_test = now

    message = (
        "SNIPER V3 PRO\n\n"
        "Telegram delivery test successful.\n"
        "This is a connection test, not a trading signal.\n"
        "Live signals remain disabled."
    )

    success, detail = send_telegram_message(message)

    if success:
        return (
            "<h2>Test message sent successfully.</h2>"
            "<p>Check your Telegram chat.</p>"
        ), 200

    record_error(f"Telegram test failed: {detail}")
    return (
        "<h2>Telegram test failed.</h2>"
        f"<p>{escape(detail)}</p>"
        "<p>Check the Render environment variables and service logs.</p>"
    ), 502


start_background_connection()
