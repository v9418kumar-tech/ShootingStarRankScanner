from flask import Flask, jsonify
import requests
import os
import json
import gzip
import threading
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote


app = Flask(__name__)


# ============================================================
# ACTUAL SHOOTING STAR RANK SCANNER
# ============================================================
#
# NSE EQ
# Price >= ₹100
# 5-Minute Candles
#
# 20-Day Turnover:
# COMPLETELY REMOVED
#
# Actual:
# आज की सभी completed 5-minute candles
# में qualifying Shooting Star
#
# Developing:
# वर्तमान चल रही 5-minute candle
#
# SS Time:
# जिस 5-minute candle में Shooting Star बना
# उसका exact candle time
# ============================================================


# ============================================================
# SETTINGS
# ============================================================

TOKEN = os.getenv(
    "UPSTOX_ACCESS_TOKEN",
    ""
).strip()


IST = ZoneInfo(
    "Asia/Kolkata"
)


# Minimum share price
MIN_PRICE = 100.0


# Minimum Shooting Star score
MIN_SCORE = 60.0


# एक scan में अधिकतम candles
CANDLE_BATCH_SIZE = 450


# Parallel candle requests
MAX_CANDLE_WORKERS = 20


# अगले scan के लिए लगभग इतना अंतर
SCAN_INTERVAL_SECONDS = 65


# Display limits
MAX_ACTUAL_DISPLAY = 200
MAX_DEVELOPING_DISPLAY = 100


# ============================================================
# UPSTOX URLs
# ============================================================

INSTRUMENT_URL = (
    "https://assets.upstox.com/market-quote/"
    "instruments/exchange/complete.json.gz"
)


QUOTE_URL = (
    "https://api.upstox.com/v3/market-quote/quotes"
)


INTRADAY_URL = (
    "https://api.upstox.com/v3/historical-candle/"
    "intraday/{key}/minutes/5"
)


# ============================================================
# GLOBAL STATE
# ============================================================

INSTRUMENTS = []

INSTRUMENT_MAP = {}

SYMBOL_MAP = {}


LIVE_RESULTS = {

    "developing": [],

    "actual": []
}


# आज के मिले Actual Shooting Stars
#
# Key:
# SYMBOL | CANDLE_TIMESTAMP
#
TODAY_ACTUAL_SIGNALS = {}


SCAN_RUNNING = False

BACKGROUND_RUNNING = False

LAST_SCAN_TIME = ""

LAST_ERROR = ""

LAST_BATCH_INFO = ""

SCAN_LOCK = threading.Lock()

BATCH_INDEX = 0


# ============================================================
# BASIC HELPERS
# ============================================================

def headers():

    return {

        "Accept":
            "application/json",

        "Content-Type":
            "application/json",

        "Authorization":
            "Bearer " + TOKEN,

        "User-Agent":
            "ActualShootingStarRankScanner/Final"
    }


def chunks(
    items,
    size
):

    for i in range(
        0,
        len(items),
        size
    ):

        yield items[
            i:i + size
        ]


def safe_float(
    value,
    default=0.0
):

    try:

        return float(
            value
        )

    except Exception:

        return default


def clamp(
    value
):

    try:

        value = float(
            value
        )

    except Exception:

        value = 0.0


    return max(
        0.0,
        min(
            100.0,
            value
        )
    )


# ============================================================
# LOAD NSE EQ INSTRUMENTS
# ============================================================

def load_instruments():

    global INSTRUMENTS
    global INSTRUMENT_MAP
    global SYMBOL_MAP


    if INSTRUMENTS:

        return INSTRUMENTS


    print(
        "Loading NSE EQ instruments..."
    )


    response = requests.get(

        INSTRUMENT_URL,

        timeout=30
    )


    response.raise_for_status()


    raw = gzip.decompress(
        response.content
    )


    data = json.loads(
        raw.decode(
            "utf-8"
        )
    )


    instruments = []


    for item in data:

        if not isinstance(
            item,
            dict
        ):

            continue


        if item.get(
            "segment"
        ) != "NSE_EQ":

            continue


        if item.get(
            "instrument_type"
        ) != "EQ":

            continue


        instrument_key = item.get(
            "instrument_key"
        )


        symbol = (

            item.get(
                "trading_symbol"
            )

            or

            item.get(
                "short_name"
            )
        )


        if not instrument_key:

            continue


        if not symbol:

            continue


        symbol = str(
            symbol
        ).strip().upper()


        instruments.append({

            "instrument_key":
                instrument_key,

            "symbol":
                symbol,

            "name":
                item.get(
                    "name",
                    symbol
                )
        })


    instruments.sort(

        key=lambda x:
            x["symbol"]
    )


    INSTRUMENTS = instruments


    INSTRUMENT_MAP = {

        x[
            "instrument_key"
        ]: x

        for x in instruments
    }


    SYMBOL_MAP = {

        x[
            "symbol"
        ]: x

        for x in instruments
    }


    print(
        "NSE EQ instruments:",
        len(INSTRUMENTS)
    )


    return INSTRUMENTS


# ============================================================
# LIVE QUOTE BATCH
# ============================================================

def fetch_quote_batch(
    batch
):

    keys = ",".join(

        x[
            "instrument_key"
        ]

        for x in batch
    )


    try:

        response = requests.get(

            QUOTE_URL,

            headers=headers(),

            params={

                "instrument_key":
                    keys
            },

            timeout=20
        )


        if response.status_code != 200:

            print(

                "Quote HTTP error:",

                response.status_code,

                response.text[:200]
            )

            return []


        payload = response.json()


        data = payload.get(
            "data",
            {}
        )


        results = []


        for response_key, quote_data in data.items():

            if not isinstance(
                quote_data,
                dict
            ):

                continue


            # ------------------------------------------------
            # Upstox response key:
            #
            # NSE_EQ:SYMBOL
            #
            # इसलिए symbol से mapping करना सबसे reliable है।
            # ------------------------------------------------

            symbol = ""


            if ":" in response_key:

                symbol = (

                    response_key
                    .split(
                        ":",
                        1
                    )[1]
                    .strip()
                    .upper()
                )


            info = SYMBOL_MAP.get(
                symbol
            )


            if not info:

                continue


            price = safe_float(

                quote_data.get(
                    "last_price"
                )
            )


            previous_close = safe_float(

                quote_data.get(
                    "prev_close_price"
                )
            )


            volume = safe_float(

                quote_data.get(
                    "volume"
                )
            )


            if price < MIN_PRICE:

                continue


            if previous_close <= 0:

                continue


            change = (

                (
                    price
                    -
                    previous_close
                )

                /

                previous_close

            ) * 100.0


            results.append({

                "symbol":
                    symbol,

                "name":
                    info[
                        "name"
                    ],

                "instrument_key":
                    info[
                        "instrument_key"
                    ],

                "price":
                    price,

                "prev_close":
                    previous_close,

                "volume":
                    volume,

                "change":
                    change
            })


        return results


    except Exception as e:

        print(
            "Quote batch exception:",
            repr(e)
        )

        return []


# ============================================================
# GET ALL LIVE QUOTES
# ============================================================

def fetch_all_live_quotes():

    load_instruments()


    batches = list(

        chunks(
            INSTRUMENTS,
            500
        )
    )


    results = []


    with ThreadPoolExecutor(

        max_workers=min(
            6,
            len(batches)
        )

    ) as executor:


        futures = [

            executor.submit(

                fetch_quote_batch,

                batch
            )

            for batch in batches
        ]


        for future in as_completed(
            futures
        ):

            try:

                results.extend(
                    future.result()
                )

            except Exception:

                pass


    print(
        "Live stocks >= ₹100:",
        len(results)
    )


    return results


# ============================================================
# GET 5-MINUTE CANDLES
# ============================================================

def get_5m_candles(
    instrument_key
):

    try:

        encoded_key = quote(

            instrument_key,

            safe=""
        )


        url = INTRADAY_URL.format(

            key=encoded_key
        )


        response = requests.get(

            url,

            headers=headers(),

            timeout=15
        )


        if response.status_code != 200:

            return []


        payload = response.json()


        candles = (

            payload
            .get(
                "data",
                {}
            )
            .get(
                "candles",
                []
            )
        )


        return candles or []


    except Exception as e:

        print(
            "Candle error:",
            repr(e)
        )

        return []


# ============================================================
# TIME HELPERS
# ============================================================

def parse_candle_time(
    timestamp
):

    try:

        dt = datetime.fromisoformat(

            str(
                timestamp
            ).replace(
                "Z",
                "+00:00"
            )
        )


        if dt.tzinfo is None:

            dt = dt.replace(
                tzinfo=IST
            )


        return dt.astimezone(
            IST
        )


    except Exception:

        return None


def candle_completed(
    timestamp,
    now=None
):

    dt = parse_candle_time(
        timestamp
    )


    if not dt:

        return False


    if now is None:

        now = datetime.now(
            IST
        )


    candle_end = (

        dt

        +

        timedelta(
            minutes=5
        )
    )


    return candle_end <= now


def candle_time_label(
    timestamp
):

    dt = parse_candle_time(
        timestamp
    )


    if not dt:

        return ""


    end = (

        dt

        +

        timedelta(
            minutes=5
        )
    )


    return (

        dt.strftime(
            "%H:%M"
        )

        +

        "–"

        +

        end.strftime(
            "%H:%M"
        )
    )


# ============================================================
# SHOOTING STAR SCORE
# ============================================================

def shooting_star_score(
    candle,
    previous_candles
):

    try:

        timestamp = candle[0]

        open_price = float(
            candle[1]
        )

        high_price = float(
            candle[2]
        )

        low_price = float(
            candle[3]
        )

        close_price = float(
            candle[4]
        )

    except Exception:

        return {

            "score":
                0.0,

            "actual":
                False,

            "time":
                ""
        }


    candle_range = max(

        high_price
        -
        low_price,

        0.000001
    )


    body = abs(

        close_price
        -
        open_price
    )


    body_safe = max(

        body,

        candle_range
        *
        0.02,

        0.01
    )


    upper_wick = (

        high_price

        -

        max(
            open_price,
            close_price
        )
    )


    lower_wick = (

        min(
            open_price,
            close_price
        )

        -

        low_price
    )


    # ========================================================
    # 1. Upper Wick Score
    # ========================================================

    upper_ratio = (

        upper_wick
        /
        body_safe
    )


    upper_score = clamp(

        (
            upper_ratio
            -
            1.0
        )

        /

        2.0

        *

        100.0
    )


    # ========================================================
    # 2. Body Position
    # ========================================================

    body_top = max(

        open_price,
        close_price
    )


    body_position = (

        (
            high_price
            -
            body_top
        )

        /

        candle_range
    )


    body_position_score = clamp(

        body_position
        /
        0.75
        *
        100.0
    )


    # ========================================================
    # 3. Lower Wick
    # ========================================================

    lower_ratio = (

        lower_wick
        /
        candle_range
    )


    lower_score = clamp(

        (
            1.0

            -

            lower_ratio
            /
            0.35
        )

        *

        100.0
    )


    # ========================================================
    # 4. Close Near Low
    # ========================================================

    close_from_low = (

        (
            close_price
            -
            low_price
        )

        /

        candle_range
    )


    close_low_score = clamp(

        (
            1.0

            -

            close_from_low
            /
            0.60
        )

        *

        100.0
    )


    # ========================================================
    # 5. Previous Uptrend
    # ========================================================

    uptrend_score = 0.0


    if len(
        previous_candles
    ) >= 3:


        try:

            first_close = float(

                previous_candles[-3][4]
            )


            latest_close = float(

                previous_candles[-1][4]
            )


            if first_close > 0:

                move = (

                    (
                        latest_close
                        -
                        first_close
                    )

                    /

                    first_close

                ) * 100.0


                uptrend_score = clamp(

                    move
                    /
                    1.0
                    *
                    100.0
                )


        except Exception:

            uptrend_score = 0.0


    # ========================================================
    # 6. Bearish / Small Body
    # ========================================================

    if close_price < open_price:

        bearish_score = 100.0


    else:

        body_fraction = (

            body
            /
            candle_range
        )


        bearish_score = clamp(

            (
                1.0

                -

                body_fraction
                /
                0.35
            )

            *

            100.0
        )


    # ========================================================
    # FINAL SCORE
    # ========================================================

    final_score = (

        upper_score
        *
        0.30

        +

        body_position_score
        *
        0.20

        +

        lower_score
        *
        0.15

        +

        close_low_score
        *
        0.15

        +

        uptrend_score
        *
        0.10

        +

        bearish_score
        *
        0.10
    )


    # ========================================================
    # ACTUAL SHOOTING STAR GATE
    # ========================================================

    actual = (

        upper_wick
        >=
        2.0
        *
        body_safe

        and

        lower_wick
        <=
        0.35
        *
        candle_range

        and

        body_position
        >=
        0.55

        and

        close_from_low
        <=
        0.45

        and

        (
            close_price
            <=
            open_price

            or

            body
            <=
            0.15
            *
            candle_range
        )

        and

        uptrend_score
        >=
        30.0
    )


    return {

        "score":
            clamp(
                final_score
            ),

        "actual":
            actual,

        "time":
            timestamp
    }


# ============================================================
# ANALYZE ONE STOCK
# ============================================================

def analyze_stock(
    row
):

    candles = get_5m_candles(

        row[
            "instrument_key"
        ]
    )


    if not candles:

        return None


    # Upstox generally returns newest first.
    # Reverse करके oldest -> newest.
    candles = list(
        reversed(
            candles
        )
    )


    now = datetime.now(
        IST
    )


    completed = []

    developing_candle = None


    for candle in candles:

        if not candle:

            continue


        if candle_completed(

            candle[0],

            now
        ):

            completed.append(
                candle
            )

        else:

            developing_candle = candle


    actual_signals = []


    # ========================================================
    # IMPORTANT:
    # आज की ALL completed candles scan करें
    # ========================================================

    for index, candle in enumerate(
        completed
    ):


        previous_candles = (

            completed[
                :index
            ]
        )


        result = shooting_star_score(

            candle,

            previous_candles
        )


        if (

            result["actual"]

            and

            result["score"]
            >=
            MIN_SCORE

        ):


            actual_signals.append({

                "symbol":
                    row["symbol"],

                "price":
                    row["price"],

                "change":
                    row["change"],

                "time":
                    candle_time_label(
                        candle[0]
                    ),

                "timestamp":
                    candle[0],

                "score":
                    result[
                        "score"
                    ]
            })


    # ========================================================
    # DEVELOPING CANDLE
    # ========================================================

    developing = None


    if developing_candle:

        developing_result = (
            shooting_star_score(

                developing_candle,

                completed
            )
        )


        developing = {

            "time":
                candle_time_label(
                    developing_candle[0]
                ),

            "score":
                developing_result[
                    "score"
                ]
        }


    return {

        "symbol":
            row["symbol"],

        "price":
            row["price"],

        "change":
            row["change"],

        "actual_signals":
            actual_signals,

        "developing":
            developing
    }


# ============================================================
# SELECT CANDIDATES
# ============================================================

def build_candidates(
    quotes
):

    candidates = []


    for row in quotes:

        if row["price"] < MIN_PRICE:

            continue


        candidates.append(
            row
        )


    # --------------------------------------------------------
    # Strong positive shares first.
    #
    # Shooting Star generally becomes more useful after
    # an upward move.
    # --------------------------------------------------------

    candidates.sort(

        key=lambda x:

            (
                x["change"],

                x["volume"]
            ),

        reverse=True
    )


    return candidates


# ============================================================
# ROTATING 450 STOCK BATCH
# ============================================================

def get_rotating_batch(
    candidates
):

    global BATCH_INDEX


    total = len(
        candidates
    )


    if total == 0:

        return []


    if total <= CANDLE_BATCH_SIZE:

        BATCH_INDEX = 0

        return candidates


    start = (

        BATCH_INDEX
        *
        CANDLE_BATCH_SIZE
    ) % total


    end = (

        start
        +
        CANDLE_BATCH_SIZE
    )


    if end <= total:

        batch = candidates[
            start:end
        ]

    else:

        batch = (

            candidates[start:]

            +

            candidates[
                :end - total
            ]
        )


    BATCH_INDEX += 1


    return batch


# ============================================================
# MAIN SCAN
# ============================================================

def perform_scan():

    global LAST_SCAN_TIME

    global LAST_BATCH_INFO

    global LIVE_RESULTS


    if not TOKEN:

        raise RuntimeError(

            "UPSTOX_ACCESS_TOKEN "
            "Render Environment Variable "
            "में नहीं मिला।"
        )


    print(
        "Starting Shooting Star scan..."
    )


    # ========================================================
    # 1. Load NSE EQ
    # ========================================================

    load_instruments()


    # ========================================================
    # 2. Get live quotes
    # ========================================================

    quotes = fetch_all_live_quotes()


    if not quotes:

        raise RuntimeError(

            "Upstox से live quotes नहीं मिले।"
        )


    # ========================================================
    # 3. Build candidates
    # ========================================================

    candidates = build_candidates(
        quotes
    )


    if not candidates:

        raise RuntimeError(

            "₹100 या उससे ऊपर का कोई NSE EQ stock नहीं मिला।"
        )


    # ========================================================
    # 4. Get rotating batch
    # ========================================================

    batch = get_rotating_batch(
        candidates
    )


    LAST_BATCH_INFO = (

        "This scan: "

        +

        str(
            len(batch)
        )

        +

        " stocks • "

        +

        str(
            len(candidates)
        )

        +

        " NSE EQ stocks universe"
    )


    print(
        LAST_BATCH_INFO
    )


    # ========================================================
    # 5. Analyze candles
    # ========================================================

    developing_rows = []


    with ThreadPoolExecutor(

        max_workers=
            MAX_CANDLE_WORKERS

    ) as executor:


        futures = [

            executor.submit(

                analyze_stock,

                row
            )

            for row in batch
        ]


        for future in as_completed(
            futures
        ):


            try:

                result = (
                    future.result()
                )


                if not result:

                    continue


                # =================================================
                # DEVELOPING
                # =================================================

                developing_data = (

                    result[
                        "developing"
                    ]
                )


                if developing_data:

                    if (

                        developing_data[
                            "score"
                        ]

                        >=

                        MIN_SCORE

                    ):


                        developing_rows.append({

                            "symbol":
                                result[
                                    "symbol"
                                ],

                            "price":
                                result[
                                    "price"
                                ],

                            "change":
                                result[
                                    "change"
                                ],

                            "time":
                                developing_data[
                                    "time"
                                ],

                            "score":
                                developing_data[
                                    "score"
                                ],

                            "status":
                                "Developing"
                        })


                # =================================================
                # ACTUAL
                # =================================================

                for signal in result[
                    "actual_signals"
                ]:


                    key = (

                        signal[
                            "symbol"
                        ]

                        +

                        "|"

                        +

                        signal[
                            "timestamp"
                        ]
                    )


                    # Same candle को दोबारा add नहीं करें
                    if key not in TODAY_ACTUAL_SIGNALS:

                        TODAY_ACTUAL_SIGNALS[
                            key
                        ] = signal


            except Exception as e:

                print(
                    "Stock analysis error:",
                    repr(e)
                )


    # ========================================================
    # DEVELOPING SORT
    # ========================================================

    developing_rows.sort(

        key=lambda x:

            x["score"],

        reverse=True
    )


    developing_final = []


    for rank, row in enumerate(

        developing_rows[
            :MAX_DEVELOPING_DISPLAY
        ],

        start=1
    ):


        developing_final.append({

            "rank":
                rank,

            "symbol":
                row["symbol"],

            "price":
                round(
                    row["price"],
                    2
                ),

            "change":
                round(
                    row["change"],
                    2
                ),

            "time":
                row["time"],

            "score":
                round(
                    row["score"],
                    1
                ),

            "status":
                "Developing"
        })


    # ========================================================
    # ACTUAL SORT
    # ========================================================

    actual_all = list(

        TODAY_ACTUAL_SIGNALS.values()
    )


    actual_all.sort(

        key=lambda x:

            (
                -float(
                    x["score"]
                ),

                x["symbol"],

                x["timestamp"]
            )
    )


    actual_final = []


    for rank, row in enumerate(

        actual_all[
            :MAX_ACTUAL_DISPLAY
        ],

        start=1
    ):


        actual_final.append({

            "rank":
                rank,

            "symbol":
                row["symbol"],

            "price":
                round(
                    row["price"],
                    2
                ),

            "change":
                round(
                    row["change"],
                    2
                ),

            "time":
                row["time"],

            "score":
                round(
                    row["score"],
                    1
                ),

            "status":
                "Actual"
        })


    # ========================================================
    # SAVE RESULTS
    # ========================================================

    LIVE_RESULTS = {

        "developing":
            developing_final,

        "actual":
            actual_final
    }


    LAST_SCAN_TIME = (

        datetime.now(
            IST
        ).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )


    print(
        "Developing:",
        len(
            developing_final
        )
    )


    print(
        "Actual today:",
        len(
            actual_final
        )
    )


    return LIVE_RESULTS


# ============================================================
# BACKGROUND WORKER
# ============================================================

def scan_worker():

    global SCAN_RUNNING

    global LAST_ERROR


    try:

        perform_scan()

        LAST_ERROR = ""


    except Exception as e:

        LAST_ERROR = str(
            e
        )[:500]


        print(
            "SCAN ERROR:",
            repr(e)
        )


    finally:

        SCAN_RUNNING = False


# ============================================================
# START SCAN
# ============================================================

def start_scan():

    global SCAN_RUNNING


    with SCAN_LOCK:

        if SCAN_RUNNING:

            return False


        SCAN_RUNNING = True


    threading.Thread(

        target=scan_worker,

        daemon=True

    ).start()


    return True


# ============================================================
# HTML
# ============================================================

HTML = """
<!DOCTYPE html>

<html lang="hi">

<head>

<meta charset="UTF-8">

<meta name="viewport"
      content="width=device-width, initial-scale=1">

<title>
Actual Shooting Star Rank Scanner
</title>


<style>

*{
    box-sizing:border-box;
}


body{

    margin:0;

    padding:6px;

    background:#11151a;

    color:#e8edf3;

    font-family:
        Arial,
        Helvetica,
        sans-serif;
}


.container{

    max-width:1200px;

    margin:auto;
}


.card{

    background:#1a2027;

    border:1px solid #303944;

    border-radius:9px;

    padding:8px;

    margin-bottom:8px;
}


h1{

    font-size:20px;

    margin:2px 0 3px;
}


.subtitle{

    color:#9fa8b3;

    font-size:12px;

    margin-bottom:7px;
}


button{

    background:#2864e8;

    color:white;

    border:0;

    border-radius:7px;

    padding:9px 14px;

    font-size:14px;

    font-weight:bold;
}


button:active{

    transform:scale(.98);
}


.status{

    margin-top:7px;

    color:#c3ccd6;

    font-size:12px;

    line-height:1.5;
}


.section{

    font-size:14px;

    font-weight:bold;

    margin:2px 0 5px;
}


.table-wrap{

    overflow-x:auto;

    -webkit-overflow-scrolling:touch;
}


table{

    width:100%;

    border-collapse:collapse;

    min-width:650px;

    background:#171c22;

    font-size:12px;
}


th{

    background:#252c35;

    padding:6px 4px;

    white-space:nowrap;
}


td{

    padding:6px 4px;

    border-bottom:
        1px solid #2c333c;

    text-align:center;

    white-space:nowrap;
}


.score{

    font-weight:bold;

    font-size:14px;
}


.dev-row{

    background:#302711;
}


.actual-row{

    background:#123529;
}


.empty{

    color:#89939e;

    text-align:center;

    padding:12px;
}


.note{

    color:#8e98a4;

    font-size:11px;

    line-height:1.5;
}

</style>

</head>


<body>


<div class="container">


<!-- =====================================================
     HEADER
     ===================================================== -->

<div class="card">

<h1>
⭐ Actual Shooting Star Rank Scanner
</h1>


<div class="subtitle">

NSE EQ • ₹100+ • 5-Minute

</div>


<button onclick="runScan()">

SCAN NOW

</button>


<div id="status"
     class="status">

Scanner ready

</div>

</div>


<!-- =====================================================
     DEVELOPING
     ===================================================== -->

<div class="card">

<div class="section">

🔴 DEVELOPING SHOOTING STAR

</div>


<div class="table-wrap">


<table>

<thead>

<tr>

<th>Rank</th>

<th>Share</th>

<th>Price</th>

<th>Chg%</th>

<th>SS Time</th>

<th>Score</th>

<th>Status</th>

</tr>

</thead>


<tbody id="developingRows">

</tbody>


</table>

</div>

</div>


<!-- =====================================================
     ACTUAL
     ===================================================== -->

<div class="card">

<div class="section">

🟢 ACTUAL SHOOTING STAR — TODAY

</div>


<div class="table-wrap">


<table>

<thead>

<tr>

<th>Rank</th>

<th>Share</th>

<th>Price</th>

<th>Chg%</th>

<th>SS Time</th>

<th>Score</th>

<th>Status</th>

</tr>

</thead>


<tbody id="actualRows">

</tbody>


</table>

</div>

</div>


<!-- =====================================================
     RULES
     ===================================================== -->

<div class="card note">

<b>Scanner Rules</b>

<br><br>


5-minute timeframe

<br>

NSE Equity EQ only

<br>

Price ≥ ₹100

<br>

20-Day Turnover filter:
<strong>Removed</strong>

<br><br>


<b>Actual</b>

=

आज की सभी completed 5-minute candles
में बना हुआ qualifying Shooting Star।

<br><br>


<b>SS Time</b>

=

जिस 5-minute candle में Shooting Star बना,
उस candle का समय।

<br><br>


<b>Developing</b>

=

वर्तमान चल रही 5-minute candle की structure।

<br><br>


अलग-अलग scans में अलग NSE EQ batches
scan होते हैं और आज मिले Actual signals
एक ही दिन की list में जमा होते रहते हैं।

</div>


</div>


<script>


function drawRows(
    elementId,
    rows,
    rowClass
){

    const tbody =
        document.getElementById(
            elementId
        );


    tbody.innerHTML = "";


    if(
        !rows ||
        rows.length === 0
    ){

        tbody.innerHTML =

            "<tr>" +

            "<td colspan='7' " +

            "class='empty'>" +

            "अभी कोई qualifying result नहीं मिला" +

            "</td>" +

            "</tr>";


        return;
    }


    rows.forEach(
        function(x){

            const tr =
                document.createElement(
                    "tr"
                );


            tr.className =
                rowClass;


            tr.innerHTML =

                "<td><b>" +

                x.rank +

                "</b></td>" +


                "<td><b>" +

                x.symbol +

                "</b></td>" +


                "<td>₹" +

                Number(
                    x.price
                ).toFixed(2) +

                "</td>" +


                "<td>" +

                Number(
                    x.change
                ).toFixed(2) +

                "%</td>" +


                "<td>" +

                x.time +

                "</td>" +


                "<td class='score'>" +

                Number(
                    x.score
                ).toFixed(1) +

                "</td>" +


                "<td>" +

                x.status +

                "</td>";


            tbody.appendChild(
                tr
            );

        }
    );

}


async function loadResults(){

    try{

        const response =
            await fetch(

                "/api/results?ts=" +

                Date.now()
            );


        const data =
            await response.json();


        const status =
            document.getElementById(
                "status"
            );


        if(data.error){

            status.innerText =

                "⚠ " +

                data.error;

        }

        else if(data.running){

            status.innerText =

                "Scanner चल रहा है...";

        }

        else{

            status.innerText =

                "Last scan: " +

                (
                    data.updated_at
                    ||
                    "-"
                )

                +

                " • "

                +

                (
                    data.batch_info
                    ||
                    ""
                )

                +

                " • Today's Actual: "

                +

                (
                    data.results.actual
                    ||
                    []
                ).length;
        }


        drawRows(

            "developingRows",

            data.results.developing,

            "dev-row"
        );


        drawRows(

            "actualRows",

            data.results.actual,

            "actual-row"
        );


    }

    catch(error){

        document.getElementById(
            "status"
        ).innerText =

            "Connection problem";

    }

}


async function runScan(){

    const status =
        document.getElementById(
            "status"
        );


    status.innerText =

        "Scanner शुरू हो रहा है...";


    try{

        await fetch(
            "/api/start"
        );


        loadResults();


    }

    catch(error){

        status.innerText =

            "Scanner start error";

    }

}


loadResults();


setInterval(

    loadResults,

    5000

);


</script>


</body>

</html>
"""


# ============================================================
# ROUTES
# ============================================================

@app.route("/")
def home():

    return HTML


@app.route(
    "/api/start"
)
def api_start():

    started = start_scan()


    return jsonify({

        "started":
            started,

        "running":
            SCAN_RUNNING
    })


@app.route(
    "/api/results"
)
def api_results():

    return jsonify({

        "running":
            SCAN_RUNNING,

        "error":
            LAST_ERROR,

        "updated_at":
            LAST_SCAN_TIME,

        "batch_info":
            LAST_BATCH_INFO,

        "results":
            LIVE_RESULTS
    })


@app.route(
    "/api/health"
)
def health():

    return jsonify({

        "ok":
            True,

        "token_configured":
            bool(TOKEN),

        "nse_eq_stocks":
            len(
                INSTRUMENTS
            ),

        "scan_running":
            SCAN_RUNNING,

        "last_scan":
            LAST_SCAN_TIME,

        "last_error":
            LAST_ERROR,

        "today_actual_signals":
            len(
                TODAY_ACTUAL_SIGNALS
            )
    })


# ============================================================
# LOCAL RUN
# ============================================================

if __name__ == "__main__":

    app.run(

        host="0.0.0.0",

        port=int(

            os.getenv(
                "PORT",
                "5000"
            )
        ),

        debug=False
    )
