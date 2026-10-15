import os, requests, time, threading
from flask import Flask
from datetime import datetime
import pytz
WAT = pytz.timezone("Africa/Lagos")
app = Flask(__name__)
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT = os.getenv("TELEGRAM_CHAT")
SYMBOLS = ["R_10","R_25","R_50","R_75","R_100","ST100","ST200","frxXAUUSD"]
NAMES = {"R_10":"Vol 10","R_25":"Vol 25","R_50":"Vol 50","R_75":"Vol 75","R_100":"Vol 100","ST100":"Step 100","ST200":"Step 200","frxXAUUSD":"XAUUSD"}
last_signals={}; stats={"real":0,"fake":0}
def tg(m):
 try: requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage", json={"chat_id":TELEGRAM_CHAT,"text":m}, timeout=10)
 except: pass
def get_candle(sym):
 try:
  r=requests.get(f"https://api.deriv.com/api/ticks_history?app_id=1089&ticks_history={sym}&count=5&end=latest&style=candles&granularity=60", timeout=10).json()
  # fallback to direct ws api via http
  if "candles" not in r:
   import websocket, json
   ws=websocket.create_connection("wss://ws.binaryws.com/websockets/v3?app_id=1089", timeout=10)
   ws.send(json.dumps({"ticks_history":sym,"count":5,"end":"latest","style":"candles","granularity":60}))
   r=json.loads(ws.recv()); ws.close()
  c=r["candles"][-1]
  O=float(c['open']); H=float(c['high']); L=float(c['low']); C=float(c['close'])
  return O,H,L,C
 except Exception as e:
  print(f"fail {sym} {e}"); return None
def loop():
 while True:
  for sym in SYMBOLS:
   d=get_candle(sym)
   if not d: continue
   O,H,L,C=d
   rng=H-L; move=(C-O)/O*100 if O else 0
   fake=abs(move)<0.15
   if fake: stats["fake"]+=1; continue
   buy=move>0; entry=C
   if buy: sl=round(L*0.998,3); tp1=round(C+rng*0.8,3); tp2=round(C+rng*1.5,3); tp3=round(C+rng*2.5,3); tp4=round(C+rng*4,3); label="BUY"
   else: sl=round(H*1.002,3); tp1=round(C-rng*0.8,3); tp2=round(C-rng*1.5,3); tp3=round(C-rng*2.5,3); tp4=round(C-rng*4,3); label="SELL"
   stats["real"]+=1
   last_signals[sym]={"label":label,"entry":entry,"sl":sl,"tp1":tp1,"tp2":tp2,"tp3":tp3,"tp4":tp4,"time":datetime.now(WAT).strftime("%H:%M:%S"),"move":round(move,2)}
   tg(f"{label} NOW {NAMES[sym]}\nEntry: {entry}\nTP1: {tp1}\nTP2: {tp2}\nTP3: {tp3}\nTP4: {tp4}\nSL: {sl}")
   time.sleep(1)
  time.sleep(50)
@app.route("/")
def home():
 h=f"<h3>Sniper V3 LIVE - {datetime.now(WAT).strftime('%H:%M:%S')} WAT</h3><p>Real:{stats['real']} Fake:{stats['fake']}</p><hr>"
 for k,v in last_signals.items(): h+=f"<div style='border:1px solid green;padding:8px;margin:5px;'><b>{NAMES[k]} {v['label']} {v['move']}%</b><br>Entry:{v['entry']} SL:{v['sl']}<br>TP:{v['tp1']},{v['tp2']},{v['tp3']},{v['tp4']}<br>{v['time']}</div>"
 return h
threading.Thread(target=loop, daemon=True).start()
