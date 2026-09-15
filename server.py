from flask import Flask,jsonify
import requests,os,json,gzip,threading,time
from datetime import datetime,timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor,as_completed
from urllib.parse import quote

app=Flask(__name__)
TOKEN=os.getenv("UPSTOX_ACCESS_TOKEN","").strip()
IST=ZoneInfo("Asia/Kolkata")

MIN_PRICE=100.0
MIN_SCORE=60.0
CANDLE_BATCH=450
MAX_WORKERS=20
SCAN_INTERVAL=65
MAX_DISPLAY=200

INSTRUMENT_URL="https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
QUOTE_URL="https://api.upstox.com/v3/market-quote/quotes"
CANDLE_URL="https://api.upstox.com/v3/historical-candle/intraday/{}/minutes/5"

INSTRUMENTS=[]
SYMBOL_MAP={}
TODAY_ACTUAL={}
RESULTS={"signals":[]}
SCAN_RUNNING=False
BACKGROUND_RUNNING=False
LAST_SCAN=""
LAST_ERROR=""
BATCH_INDEX=0
BATCH_INFO=""
LOCK=threading.Lock()

def hdr():
    return {"Accept":"application/json","Content-Type":"application/json",
            "Authorization":"Bearer "+TOKEN,
            "User-Agent":"ShootingStarRankScanner/Final"}

def sf(v,d=0):
    try:return float(v)
    except:return d

def clamp(v):
    return max(0,min(100,sf(v)))

def chunks(a,n):
    for i in range(0,len(a),n):yield a[i:i+n]

def load_instruments():
    global INSTRUMENTS,SYMBOL_MAP
    if INSTRUMENTS:return INSTRUMENTS
    r=requests.get(INSTRUMENT_URL,timeout=30);r.raise_for_status()
    data=json.loads(gzip.decompress(r.content).decode())
    for x in data:
        if x.get("segment")=="NSE_EQ" and x.get("instrument_type")=="EQ":
            k=x.get("instrument_key");s=(x.get("trading_symbol") or "").strip().upper()
            if k and s:
                INSTRUMENTS.append({"key":k,"symbol":s,"name":x.get("name",s)})
    INSTRUMENTS.sort(key=lambda x:x["symbol"])
    SYMBOL_MAP={x["symbol"]:x for x in INSTRUMENTS}
    print("NSE EQ:",len(INSTRUMENTS))
    return INSTRUMENTS

def quote_batch(batch):
    try:
        keys=",".join(x["key"] for x in batch)
        r=requests.get(QUOTE_URL,headers=hdr(),params={"instrument_key":keys},timeout=20)
        if r.status_code!=200:return []
        out=[]
        for rk,q in r.json().get("data",{}).items():
            if not isinstance(q,dict):continue
            sym=rk.split(":",1)[1].strip().upper() if ":" in rk else ""
            info=SYMBOL_MAP.get(sym)
            if not info:continue
            p=sf(q.get("last_price"));pc=sf(q.get("prev_close_price"))
            if p<MIN_PRICE or pc<=0:continue
            out.append({"symbol":sym,"key":info["key"],"price":p,
                        "prev":pc,"change":(p-pc)/pc*100,
                        "volume":sf(q.get("volume"))})
        return out
    except Exception as e:
        print("Quote:",e);return []

def live_quotes():
    load_instruments()
    bs=list(chunks(INSTRUMENTS,500));out=[]
    with ThreadPoolExecutor(max_workers=min(6,len(bs))) as ex:
        fs=[ex.submit(quote_batch,b) for b in bs]
        for f in as_completed(fs):
            try:out+=f.result()
            except:pass
    print("Live >=100:",len(out))
    return out

def candles(key):
    try:
        u=CANDLE_URL.format(quote(key,safe=""))
        r=requests.get(u,headers=hdr(),timeout=15)
        if r.status_code!=200:return []
        return r.json().get("data",{}).get("candles",[]) or []
    except Exception as e:
        print("Candle:",e);return []

def ptime(ts):
    try:
        d=datetime.fromisoformat(str(ts).replace("Z","+00:00"))
        if d.tzinfo is None:d=d.replace(tzinfo=IST)
        return d.astimezone(IST)
    except:return None

def completed(ts,now):
    d=ptime(ts)
    return bool(d and d+timedelta(minutes=5)<=now)

def label(ts):
    d=ptime(ts)
    return "" if not d else d.strftime("%H:%M")+"–"+(d+timedelta(minutes=5)).strftime("%H:%M")

def ss_score(c,prev):
    try:
        ts,o,h,l,cl=c[:5]
        o,h,l,cl=map(float,(o,h,l,cl))
    except:return 0,False,""
    rng=max(h-l,.000001);body=abs(cl-o)
    body_safe=max(body,rng*.015,.01)
    uw=h-max(o,cl);lw=min(o,cl)-l

    upper=clamp(((uw/body_safe)-1)/2.0*100)
    body_pos=clamp((h-max(o,cl))/rng/.75*100)
    lower=clamp((1-lw/rng/.35)*100)
    near_low=clamp((1-(cl-l)/rng/.55)*100)

    trend=0
    if len(prev)>=4:
        try:
            a=sf(prev[-4][4]);b=sf(prev[-1][4])
            if a>0:trend=clamp((b-a)/a/0.8*100)
        except:pass

    bearish=100 if cl<o else clamp((1-(body/rng)/.30)*100)

    score=upper*.30+body_pos*.20+lower*.15+near_low*.15+trend*.10+bearish*.10

    actual=(
        uw>=2.2*body_safe and
        lw<=.28*rng and
        body_pos>=.60 and
        (cl-l)/rng<=.40 and
        (cl<=o or body<=.12*rng) and
        trend>=35 and
        score>=MIN_SCORE
    )
    return clamp(score),actual,label(ts)

def analyze(row):
    cs=candles(row["key"])
    if not cs:return None
    cs=list(reversed(cs))
    now=datetime.now(IST)
    done=[c for c in cs if completed(c[0],now)]
    dev=next((c for c in cs if not completed(c[0],now)),None)
    actual=[]

    for i,c in enumerate(done):
        score,ok,t=ss_score(c,done[:i])
        if ok:
            actual.append({
                "symbol":row["symbol"],"price":row["price"],
                "change":row["change"],"time":t,
                "timestamp":c[0],"score":score,"status":"Actual"
            })

    developing=None
    if dev:
        score,_,t=ss_score(dev,done)
        if score>=MIN_SCORE:
            developing={
                "symbol":row["symbol"],"price":row["price"],
                "change":row["change"],"time":t,
                "timestamp":dev[0],"score":score,
                "status":"Developing"
            }
    return actual,developing

def rotating(candidates):
    global BATCH_INDEX
    n=len(candidates)
    if not n:return []
    if n<=CANDLE_BATCH:
        BATCH_INDEX=0;return candidates
    start=(BATCH_INDEX*CANDLE_BATCH)%n
    end=start+CANDLE_BATCH
    BATCH_INDEX+=1
    return candidates[start:end] if end<=n else candidates[start:]+candidates[:end-n]

def scan():
    global LAST_SCAN,LAST_ERROR,BATCH_INFO,RESULTS,TODAY_ACTUAL
    if not TOKEN:raise RuntimeError("UPSTOX_ACCESS_TOKEN Render Environment Variable में नहीं मिला।")
    load_instruments()
    quotes=live_quotes()
    if not quotes:raise RuntimeError("Upstox से live quotes नहीं मिले।")

    quotes.sort(key=lambda x:(x["change"],x["volume"]),reverse=True)
    batch=rotating(quotes)
    BATCH_INFO=f"This scan: {len(batch)} stocks • {len(quotes)} NSE EQ universe"
    print(BATCH_INFO)

    developing=[]
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        fs=[ex.submit(analyze,x) for x in batch]
        for f in as_completed(fs):
            try:
                res=f.result()
                if not res:continue
                actual,dev=res

                for x in actual:
                    k=x["symbol"]+"|"+x["timestamp"]
                    TODAY_ACTUAL[k]=x

                if dev:developing.append(dev)
            except Exception as e:
                print("Analysis:",e)

    allrows=list(TODAY_ACTUAL.values())+developing
    allrows.sort(key=lambda x:x["score"],reverse=True)

    final=[]
    for rank,x in enumerate(allrows[:MAX_DISPLAY],1):
        final.append({
            "rank":rank,
            "symbol":x["symbol"],
            "price":round(x["price"],2),
            "change":round(x["change"],2),
            "time":x["time"],
            "score":round(x["score"],1),
            "status":x["status"]
        })

    RESULTS={"signals":final}
    LAST_SCAN=datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    LAST_ERROR=""
    print("Today's Actual:",len(TODAY_ACTUAL))

def worker():
    global SCAN_RUNNING,LAST_ERROR
    try:scan()
    except Exception as e:
        LAST_ERROR=str(e)[:500];print("SCAN ERROR:",e)
    finally:SCAN_RUNNING=False

def start_scan():
    global SCAN_RUNNING
    with LOCK:
        if SCAN_RUNNING:return False
        SCAN_RUNNING=True
    threading.Thread(target=worker,daemon=True).start()
    return True

def market_open():
    t=datetime.now(IST).time()
    return t>=datetime.strptime("09:15","%H:%M").time() and t<=datetime.strptime("15:35","%H:%M").time()

def background():
    global BACKGROUND_RUNNING
    if BACKGROUND_RUNNING:return
    BACKGROUND_RUNNING=True
    while True:
        try:
            if market_open() and not SCAN_RUNNING:start_scan()
        except Exception as e:print("Background:",e)
        time.sleep(SCAN_INTERVAL)

HTML="""
<!doctype html><html lang="hi"><head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Shooting Star Rank Scanner</title>
<style>
*{box-sizing:border-box}body{margin:0;padding:6px;background:#11151a;color:#e8edf3;font-family:Arial}
.card{background:#1a2027;border:1px solid #303944;border-radius:9px;padding:8px;margin-bottom:8px}
h1{font-size:20px;margin:2px 0 3px}.sub,.status,.note{color:#9fa8b3;font-size:12px}
button{background:#2864e8;color:#fff;border:0;border-radius:7px;padding:9px 14px;font-size:14px;font-weight:bold}
.status{margin-top:7px}.wrap{overflow-x:auto}table{width:100%;min-width:650px;border-collapse:collapse;font-size:12px}
th{background:#252c35;padding:7px 4px;white-space:nowrap}td{padding:7px 4px;border-bottom:1px solid #2c333c;text-align:center;white-space:nowrap}
.actual{background:#123529}.developing{background:#302711}.score{font-weight:bold;font-size:14px}.note{line-height:1.5}
</style></head><body><div class="card">
<h1>⭐ Actual Shooting Star Rank Scanner</h1>
<div class="sub">NSE EQ • ₹100+ • 5-Minute</div><br>
<button onclick="runScan()">SCAN NOW</button><div id="status" class="status">Scanner ready</div>
</div>
<div class="card"><b>⭐ SHOOTING STAR RESULTS — TODAY</b><div class="wrap"><table>
<thead><tr><th>Rank</th><th>Share</th><th>Price</th><th>Chg%</th><th>SS Time</th><th>Score</th><th>Status</th></tr></thead>
<tbody id="rows"></tbody></table></div></div>
<div class="card note">
<b>Scanner Rules</b><br><br>
5-minute timeframe<br>
NSE Equity EQ only<br>
Price ≥ ₹100<br>
20-Day Turnover filter: Removed<br><br>
<b>Actual</b> = आज की completed 5-minute candle में बना qualifying Shooting Star.<br>
<b>Developing</b> = वर्तमान चल रही 5-minute candle की Shooting Star structure.<br>
<b>SS Time</b> = जिस 5-minute candle में Shooting Star बना उसका समय।<br>
Score के आधार पर सभी results एक ही table में rank होंगे।
</div></div>
<script>
function draw(rows){
 const b=document.getElementById("rows");b.innerHTML="";
 if(!rows||!rows.length){b.innerHTML="<tr><td colspan='7'>अभी कोई qualifying result नहीं मिला</td></tr>";return}
 rows.forEach(x=>{
  const tr=document.createElement("tr");tr.className=x.status=="Actual"?"actual":"developing";
  tr.innerHTML="<td><b>"+x.rank+"</b></td><td><b>"+x.symbol+"</b></td><td>₹"+Number(x.price).toFixed(2)+"</td><td>"+Number(x.change).toFixed(2)+"%</td><td>"+x.time+"</td><td class='score'>"+Number(x.score).toFixed(1)+"</td><td>"+x.status+"</td>";
  b.appendChild(tr);
 });
}
async function load(){
 try{
  const d=await(await fetch("/api/results?t="+Date.now())).json();
  document.getElementById("status").innerText=d.error?"⚠ "+d.error:
   d.running?"Scanner चल रहा है...":
   "Last scan: "+(d.updated_at||"-")+" • "+(d.batch_info||"");
  draw(d.results.signals||[]);
 }catch(e){document.getElementById("status").innerText="Connection problem"}
}
async function runScan(){
 document.getElementById("status").innerText="Scanner शुरू हो रहा है...";
 await fetch("/api/start");load();
}
load();setInterval(load,5000);
</script></body></html>
"""

@app.route("/")
def home():return HTML

@app.route("/api/start")
def api_start():return jsonify({"started":start_scan(),"running":SCAN_RUNNING})

@app.route("/api/results")
def api_results():
    return jsonify({"running":SCAN_RUNNING,"error":LAST_ERROR,
                    "updated_at":LAST_SCAN,"batch_info":BATCH_INFO,
                    "results":RESULTS})

@app.route("/api/health")
def health():
    return jsonify({"ok":True,"token_configured":bool(TOKEN),
                    "nse_eq_stocks":len(INSTRUMENTS),
                    "scan_running":SCAN_RUNNING,
                    "last_scan":LAST_SCAN,
                    "last_error":LAST_ERROR,
                    "today_actual_signals":len(TODAY_ACTUAL)})

threading.Thread(target=background,daemon=True).start()

if __name__=="__main__":
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","5000")),debug=False)
