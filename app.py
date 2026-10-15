import json, threading, time, websocket, requests
from flask import Flask, jsonify
app = Flask(__name__)
SYMBOL = "R_100"
TELEGRAM_BOT_TOKEN = "YOUR_BOT_TOKEN_HERE"
TELEGRAM_CHAT_ID = "YOUR_CHAT_ID_HERE"
stats = {"real": 0, "fake": 0, "last_price": 0, "last_signal": "Waiting..."}
ticks_buffer = []
def send_telegram(text):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}, timeout=10)
    except Exception as e:
        print(f"Telegram error: {e}")
def sniper_v3_logic(price):
    ticks_buffer.append(price)
    if len(ticks_buffer) > 20: ticks_buffer.pop(0)
    if len(ticks_buffer) < 20: return None
    avg = sum(ticks_buffer[-20:]) / 20
    if price > avg + 0.5: return "REAL"
    elif price < avg - 0.5: return "FAKE"
    return None
def on_message(ws, message):
    data = json.loads(message)
    if 'tick' in data:
        price = float(data['tick']['quote'])
        stats["last_price"] = price
        signal = sniper_v3_logic(price)
        if signal:
            stats["last_signal"] = f"{signal} @ {price}"
            if signal == "REAL": stats["real"] += 1
            else: stats["fake"] += 1
            msg = f"🎯 Sniper V3 Signal\nType: {signal}\nPrice: {price}\nReal {stats['real']} | Fake {stats['fake']}"
            send_telegram(msg)
            print(msg)
def on_open(ws):
    print("✅ Connected to Deriv - No token needed")
    ws.send(json.dumps({"ticks": SYMBOL}))
def start_deriv():
    while True:
        try:
            ws = websocket.WebSocketApp("wss://ws.derivws.com/websockets/v3?app_id=1089", on_message=on_message, on_open=on_open)
            ws.run_forever(ping_interval=30, ping_timeout=10)
        except Exception as e:
            print(f"Error: {e}"); time.sleep(5)
@app.route("/")
def home():
    return jsonify(stats)
threading.Thread(target=start_deriv, daemon=True).start()
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
