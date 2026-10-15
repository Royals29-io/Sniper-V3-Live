import json, threading, time, websocket, os
from flask import Flask, jsonify
app = Flask(__name__)

stats = {"real":0,"fake":0,"last_price":0,"last_signal":"Starting..."}

def on_message(ws, msg):
    d = json.loads(msg)
    if "tick" in d:
        p = float(d["tick"]["quote"])
        stats["last_price"] = p
        stats["last_signal"] = f"LIVE {p}"
        print(p)

def on_open(ws):
    print("CONNECTED")
    stats["last_signal"] = "Connected"
    ws.send(json.dumps({"ticks":"R_100"}))

def ws_run():
    while True:
        try:
            ws = websocket.WebSocketApp("wss://ws.derivws.com/websockets/v3?app_id=1089", on_message=on_message, on_open=on_open)
            ws.run_forever(ping_interval=30)
        except Exception as e:
            print(e)
            time.sleep(3)

threading.Thread(target=ws_run, daemon=True).start()

@app.route("/")
def home():
    return jsonify(stats)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT",10000)))
