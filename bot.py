import os, requests, time, threading, json, websocket
from flask import Flask
from datetime import datetime
import pytz

WAT = pytz.timezone("Africa/Lagos")
app = Flask(__name__)

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT = os.getenv("TELEGRAM_CHAT")

SYMBOLS = [
    "Volatility 10 Index",
    "Volatility 25 Index",
    "Volatility 50 Index",
    "Volatility 75 Index",
    "Volatility 100 Index",
    "Step Index 100",
    "Step Index 200",
    "XAUUSD"
]

SYMBOL_MAP = {
    "Volatility 10 Index": "R_10",
    "Volatility 25 Index": "R_25",
    "Volatility 50 Index": "R_50",
    "Volatility 75 Index": "R_75",
    "Volatility 100 Index": "R_100",
    "Step Index 100": "ST100",
    "Step Index 200": "ST200",
    "XAUUSD": "frxXAUUSD"
}

last_signals = {}
stats = {"real": 0, "fake_blocked": 0}

def send_telegram(msg):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT, "text": msg}, timeout=10)
    except: pass

def get_candle_data(symbol):
    try:
        api_symbol = SYMBOL_MAP.get(symbol, "R_75")
        ws = websocket.create_connection("wss://ws.binaryws.com/websockets/v3?app_id=1089", timeout=8)
        req = {"ticks_history": api_symbol, "count": 5, "end": "latest", "style": "candles", "granularity": 60}
        ws.send(json.dumps(req))
        res = json.loads(ws.recv())
        ws.close()
        last = res["candles"][-1]
        O = float(last['open']); H = float(last['high']); L = float(last['low']); C = float(last['close'])
        pivot = round((H+L+C)/3, 5)
        R1 = round((2*pivot)-L, 5); S1 = round((2*pivot)-H, 5)
        return {"O":O,"H":H,"L":L,"C":C,"pivot":pivot,"R1":R1,"S1":S1}
    except Exception as e:
        print(f"Error {symbol}: {e}")
        return None

def analyze_trade(symbol, data):
    O,H,L,C = data['O'],data['H'],data['L'],data['C']
    pivot,R1,S1 = data['pivot'],data['R1'],data['S1']
    movePct = ((C-O)/O*100) if O else 0
    range_size = H-L
    upper_wick = H - max(O,C)
    lower_wick = min(O,C) - L
    strength = min(95, abs(movePct)*25 + range_size/max(O,1)*5000)
    fake=False; reasons=[]
    if abs(movePct) < 0.25: fake=True; reasons.append("Weak move")
    if range_size>0 and upper_wick > range_size*0.65 and movePct>0: fake=True; reasons.append("Upper wick trap")
    if range_size>0 and lower_wick > range_size*0.65 and movePct<0: fake=True; reasons.append("Lower wick trap")
    is_buy = movePct>0; entry = C
    if is_buy:
        sl = round(L * 0.998, 3)
        tp1 = round(entry + range_size * 0.8, 3)
        tp2 = round(entry + range_size * 1.5, 3)
        tp3 = round(entry + range_size * 2.5, 3)
        tp4 = round(entry + range_size * 4.0, 3)
        label = "BUY"
    else:
        sl = round(H * 1.002, 3)
        tp1 = round(entry - range_size * 0.8, 3)
        tp2 = round(entry - range_size * 1.5, 3)
        tp3 = round(entry - range_size * 2.5, 3)
        tp4 = round(entry - range_size * 4.0, 3)
        label = "SELL"
    return {"symbol":symbol,"label":label,"entry":round(entry,3),"tp1":tp1,"tp2":tp2,"tp3":tp3,"tp4":tp4,"sl":sl,"pivot":pivot,"fake":fake,"reasons":reasons,"strength":round(strength,1),"time":datetime.now(WAT).strftime("%H:%M:%S"),"range":round(range_size,5)}

def price_loop():
    while True:
        for sym in SYMBOLS:
            data = get_candle_data(sym)
            if not data: continue
            sig = analyze_trade(sym, data)
            last_signals[sym]=sig
            if sig['fake']:
                stats["fake_blocked"]+=1
                continue
            stats["real"]+=1
            msg = f"""{sig['label']} NOW
Entry: {sig['entry']}
TP1: {sig['tp1']}
TP2: {sig['tp2']}
TP3: {sig['tp3']}
TP4: {sig['tp4']}
SL: {sig['sl']}"""
            send_telegram(msg)
            time.sleep(2)
        time.sleep(55)

@app.route("/")
def home():
    html = f"<h2>Sniper V3 LIVE - {datetime.now(WAT).strftime('%H:%M:%S')} WAT</h2>"
    html += f"<p>Real: {stats['real']} | Fake Blocked: {stats['fake_blocked']}</p><hr>"
    for sym, s in last_signals.items():
        if not s: continue
        color = "green" if s['label']=="BUY" else "red"
        html += f"<div style='border:2px solid {color};margin:10px;padding:10px;'><b>{sym} {s['label']}</b><br>Pivot:{s['pivot']} Entry:{s['entry']} SL:{s['sl']}<br>TPs: {s['tp1']},{s['tp2']},{s['tp3']},{s['tp4']}</div>"
    return html

if __name__ == "__main__":
    threading.Thread(target=price_loop, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 10000)))
