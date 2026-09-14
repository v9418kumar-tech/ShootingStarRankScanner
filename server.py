from flask import Flask, jsonify
import requests
import os
import json
import gzip
import threading
from datetime import datetime, timedelta, date, timezone
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote

app = Flask(__name__)

# ============================================================
# SHOOTING STAR RANK SCANNER
# ============================================================
# 5-Minute NSE Equity Scanner
#
# DEVELOPING:
# Current 5-minute candle, live quality score.
#
# ACTUAL:
# Latest completed 5-minute candle which satisfies
# the Shooting Star structure.
#
# IMPORTANT:
# Developing and Actual are ranked separately.
# Confirmation candle is NOT required for Actual.
# ============================================================


# ============================================================
# SETTINGS
# ============================================================

TOKEN = os.getenv(
    "UPSTOX_ACCESS_TOKEN",
    ""
).strip()

# Indian Standard Time
IST = ZoneInfo("Asia/Kolkata")

# Minimum share price
MIN_PRICE = 100.0

# Minimum 20-day average turnover
# ₹10 crore
MIN_AVG_TURNOVER = 100_000_000.0

# Minimum score shown in scanner
MIN_SCORE = 60.0

# Number of intraday candidates processed.
# This keeps the first version fast and within API limits.
MAX_INTRADAY_STOCKS = 450

# Parallel candle requests
MAX_CANDLE_WORKERS = 20


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

NSE_BHAV_URL = (
    "https://nsearchives.nseindia.com/products/"
    "content/sec_bhavdata_full_{}.csv"
)


# ============================================================
# CACHE
# ============================================================

CACHE_FILE = (
    "shooting_star_turnover_cache.json"
)


# ============================================================
# GLOBAL STATE
# ============================================================

INSTRUMENTS = []

INSTRUMENT_MAP = {}

AVG_TURNOVER = {}

LIVE_RESULTS = {
    "developing": [],
    "actual": []
}

SCAN_RUNNING = False

LAST_SCAN_TIME = ""

LAST_ERROR = ""

SCAN_LOCK = threading.Lock()


# ============================================================
# HELPERS
# ============================================================

def headers():

    return {
        "Accept":
            "application/json",

        "Content-Type":
            "application/json",

        "Authorization":
            f"Bearer {TOKEN}",

        "User-Agent":
            "ShootingStarRankScanner/1.0"
    }


def chunks(items, size):

    for i in range(
        0,
        len(items),
        size
    ):

        yield items[
            i:i + size
        ]


def clamp(value):

    return max(
        0.0,
        min(
            100.0,
            float(value)
        )
    )


# ============================================================
# LOAD NSE EQ INSTRUMENTS
# ============================================================

def load_instruments():

    global INSTRUMENTS
    global INSTRUMENT_MAP

    if INSTRUMENTS:

        return INSTRUMENTS

    print(
        "Loading NSE EQ instruments..."
    )

    r = requests.get(
        INSTRUMENT_URL,
        timeout=30
    )

    r.raise_for_status()

    raw = gzip.decompress(
        r.content
    )

    data = json.loads(
        raw.decode("utf-8")
    )

    instruments = []

    for x in data:

        if not isinstance(
            x,
            dict
        ):
            continue

        if x.get(
            "segment"
        ) != "NSE_EQ":

            continue

        if x.get(
            "instrument_type"
        ) != "EQ":

            continue

        key = x.get(
            "instrument_key"
        )

        symbol = (
            x.get(
                "trading_symbol"
            )
            or
            x.get(
                "short_name"
            )
        )

        if not key or not symbol:

            continue

        instruments.append({

            "instrument_key":
                key,

            "symbol":
                symbol,

            "name":
                x.get(
                    "name",
                    symbol
                )
        })

    INSTRUMENTS = instruments

    INSTRUMENT_MAP = {

        x[
            "instrument_key"
        ]: x

        for x in instruments
    }

    print(
        f"Loaded "
        f"{len(INSTRUMENTS)} "
        f"NSE EQ instruments"
    )

    return INSTRUMENTS


# ============================================================
# LIVE QUOTES
# ============================================================

def fetch_quote_batch(batch):

    keys = ",".join(

        x[
            "instrument_key"
        ]

        for x in batch
    )

    try:

        r = requests.get(

            QUOTE_URL,

            headers=headers(),

            params={
                "instrument_key":
                    keys
            },

            timeout=15
        )

        if r.status_code != 200:

            print(
                "Quote error:",
                r.status_code,
                r.text[:200]
            )

            return []

        data = r.json().get(
            "data",
            {}
        )

        results = []

        for response_key, q in data.items():

            if not isinstance(
                q,
                dict
            ):

                continue

            instrument_key = (

                q.get(
                    "instrument_token"
                )

                or

                response_key
            )

            info = INSTRUMENT_MAP.get(
                instrument_key
            )

            if not info:

                continue

            price = float(
                q.get(
                    "last_price"
                )
                or 0
            )

            previous_close = float(
                q.get(
                    "prev_close_price"
                )
                or 0
            )

            volume = float(
                q.get(
                    "volume"
                )
                or 0
            )

            if price < MIN_PRICE:

                continue

            if previous_close <= 0:

                continue

            results.append({

                "symbol":
                    info["symbol"],

                "name":
                    info["name"],

                "instrument_key":
                    instrument_key,

                "price":
                    price,

                "prev_close":
                    previous_close,

                "volume":
                    volume
            })

        return results

    except Exception as e:

        print(
            "Quote batch error:",
            repr(e)
        )

        return []


def fetch_all_live_quotes():

    load_instruments()

    batches = list(
        chunks(
            INSTRUMENTS,
            500
        )
    )

    if not batches:

        return []

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

            except Exception as e:

                print(
                    "Quote worker error:",
                    repr(e)
                )

    print(
        f"Live quotes received: "
        f"{len(results)}"
    )

    return results


# ============================================================
# NSE SESSION
# ============================================================

def nse_session():

    s = requests.Session()

    s.headers.update({

        "User-Agent":
            "Mozilla/5.0 "
            "(Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "Chrome/139.0 Safari/537.36",

        "Accept":
            "text/csv,text/plain,"
            "application/json,*/*",

        "Accept-Language":
            "en-US,en;q=0.9",

        "Referer":
            "https://www.nseindia.com/"
    })

    try:

        s.get(
            "https://www.nseindia.com/",
            timeout=8
        )

    except Exception:

        pass

    return s


# ============================================================
# NSE BHAVCOPY
# ============================================================

def get_bhavcopy(date_obj):

    try:

        date_str = date_obj.strftime(
            "%d%m%Y"
        )

        url = NSE_BHAV_URL.format(
            date_str
        )

        r = nse_session().get(
            url,
            timeout=20
        )

        if r.status_code != 200:

            return {}

        if not r.text.strip():

            return {}

        lines = r.text.splitlines()

        if len(lines) < 2:

            return {}

        header = [

            x.strip().upper()

            for x in lines[0].split(",")
        ]

        index = {

            name: i

            for i, name in enumerate(
                header
            )
        }

        required = (

            "SYMBOL",
            "CLOSE_PRICE",
            "SERIES"
        )

        if not all(
            x in index
            for x in required
        ):

            return {}

        turnover_index = index.get(
            "TOTTRDVAL"
        )

        volume_index = (
            index.get(
                "TTL_TRD_QNTY"
            )
            or
            index.get(
                "TOTTRDQTY"
            )
        )

        result = {}

        for line in lines[1:]:

            try:

                parts = [

                    x.strip()

                    for x in line.split(",")
                ]

                if len(parts) <= max(
                    index.values()
                ):

                    continue

                series = (

                    parts[
                        index["SERIES"]
                    ]
                    .upper()
                )

                if series != "EQ":

                    continue

                symbol = (

                    parts[
                        index["SYMBOL"]
                    ]
                    .upper()
                    .strip()
                )

                close = float(

                    parts[
                        index[
                            "CLOSE_PRICE"
                        ]
                    ]
                    .replace(
                        ",",
                        ""
                    )
                )

                if turnover_index is not None:

                    turnover = float(

                        parts[
                            turnover_index
                        ]
                        .replace(
                            ",",
                            ""
                        )
                    )

                elif volume_index is not None:

                    volume = float(

                        parts[
                            volume_index
                        ]
                        .replace(
                            ",",
                            ""
                        )
                    )

                    turnover = (
                        close * volume
                    )

                else:

                    continue

                if turnover > 0:

                    result[symbol] = turnover

            except Exception:

                continue

        return result

    except Exception as e:

        print(
            "Bhavcopy error:",
            date_obj,
            repr(e)
        )

        return {}


# ============================================================
# PREVIOUS 20 TRADING DAYS
# ============================================================

def previous_trading_dates():

    dates = []

    d = (

        datetime.now().date()
        -
        timedelta(days=1)
    )

    while len(dates) < 20:

        if d.weekday() < 5:

            dates.append(d)

        d -= timedelta(
            days=1
        )

    return dates


# ============================================================
# TURNOVER CACHE
# ============================================================

def load_turnover_cache():

    global AVG_TURNOVER

    try:

        with open(
            CACHE_FILE,
            "r",
            encoding="utf-8"
        ) as f:

            saved = json.load(f)

        if (

            saved.get(
                "date"
            )
            ==
            str(date.today())

            and

            saved.get(
                "data"
            )
        ):

            AVG_TURNOVER = {

                k:
                    float(v)

                for k, v
                in saved[
                    "data"
                ].items()
            }

            print(
                "20D turnover loaded "
                "from cache:",
                len(AVG_TURNOVER)
            )

            return True

    except Exception:

        pass

    return False


def build_turnover_cache(
    symbols
):

    global AVG_TURNOVER

    print(
        "Building 20-day "
        "average turnover..."
    )

    sums = {

        s: 0.0

        for s in symbols
    }

    counts = {

        s: 0

        for s in symbols
    }

    dates = previous_trading_dates()

    with ThreadPoolExecutor(
        max_workers=6
    ) as executor:

        futures = [

            executor.submit(
                get_bhavcopy,
                d
            )

            for d in dates
        ]

        for future in as_completed(
            futures
        ):

            try:

                day = future.result()

                for symbol in symbols:

                    turnover = day.get(
                        symbol
                    )

                    if turnover is None:

                        continue

                    sums[symbol] += (
                        turnover
                    )

                    counts[symbol] += 1

            except Exception:

                pass

    AVG_TURNOVER = {

        s:
            sums[s] / 20.0

        for s in symbols

        if counts[s] == 20
    }

    try:

        with open(
            CACHE_FILE,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(

                {
                    "date":
                        str(date.today()),

                    "data":
                        AVG_TURNOVER
                },

                f
            )

    except Exception as e:

        print(
            "Cache write error:",
            repr(e)
        )

    print(
        "20D average turnover calculated:",
        len(AVG_TURNOVER)
    )


# ============================================================
# 5-MINUTE CANDLES
# ============================================================

def get_5m_candles(
    instrument_key
):

    try:

        # Instrument key can contain
        # special characters such as "|".
        encoded_key = quote(
            instrument_key,
            safe=""
        )

        url = INTRADAY_URL.format(
            key=encoded_key
        )

        r = requests.get(

            url,

            headers=headers(),

            timeout=12
        )

        if r.status_code != 200:

            return []

        payload = r.json()

        candles = (

            payload
            .get("data", {})
            .get("candles", [])
        )

        return candles or []

    except Exception as e:

        print(
            "Candle error:",
            repr(e)
        )

        return []


# ============================================================
# SHOOTING STAR SCORE
# ============================================================

def shooting_star_score(
    candle,
    previous_candles
):

    try:

        # Standard Upstox candle:
        #
        # [timestamp, open, high,
        #  low, close, volume, oi]
        #
        timestamp = candle[0]

        o = float(candle[1])

        h = float(candle[2])

        l = float(candle[3])

        c = float(candle[4])

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
        h - l,
        0.000001
    )

    body = abs(
        c - o
    )

    body_safe = max(

        body,

        candle_range * 0.02,

        0.01
    )

    upper_wick = (
        h
        -
        max(
            o,
            c
        )
    )

    lower_wick = (
        min(
            o,
            c
        )
        -
        l
    )

    # ========================================================
    # 1. UPPER WICK STRENGTH
    # Weight = 30
    #
    # 2x body = strong
    # 3x body or more = maximum
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
        100
    )

    # ========================================================
    # 2. BODY POSITION
    # Weight = 20
    #
    # Body should be close to bottom.
    # ========================================================

    body_top = max(
        o,
        c
    )

    body_position = (

        (
            h
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
        100
    )

    # ========================================================
    # 3. LOWER WICK
    # Weight = 15
    #
    # Smaller lower wick = better.
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
        100
    )

    # ========================================================
    # 4. CLOSE NEAR LOW
    # Weight = 15
    # ========================================================

    close_from_low = (

        (
            c
            -
            l
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
        100
    )

    # ========================================================
    # 5. PREVIOUS UPTREND
    # Weight = 10
    #
    # Only completed previous candles are used.
    # ========================================================

    uptrend_score = 0.0

    if len(
        previous_candles
    ) >= 3:

        c1 = float(
            previous_candles[-3][4]
        )

        c3 = float(
            previous_candles[-1][4]
        )

        move = (

            (
                c3
                -
                c1
            )
            /
            max(
                c1,
                0.01
            )
        ) * 100

        uptrend_score = clamp(

            move
            /
            1.0
            *
            100
        )

    # ========================================================
    # 6. BEARISH / SMALL BODY
    # Weight = 10
    # ========================================================

    if c < o:

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
            100
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
    #
    # These are the structural requirements.
    #
    # No next-candle confirmation required.
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
            c <= o
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
        30
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
# CANDLE TIME
# ============================================================

def candle_time_label(
    timestamp
):

    try:

        dt = datetime.fromisoformat(

            timestamp.replace(
                "Z",
                "+00:00"
            )
        )

        # Convert Upstox UTC candle time
        # to Indian Standard Time.
        if dt.tzinfo is None:

            dt = dt.replace(
                tzinfo=timezone.utc
            )

        dt = dt.astimezone(
            IST
        )

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

    except Exception:

        return ""


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

    if len(candles) < 2:

        return None

    # Upstox normally returns
    # newest candle first.
    #
    # Reverse it so that:
    # oldest -> newest
    candles = list(
        reversed(
            candles
        )
    )

    # --------------------------------------------------------
    # Current candle
    # --------------------------------------------------------

    current = candles[-1]

    previous_for_current = (
        candles[:-1]
    )

    developing = (
        shooting_star_score(
            current,
            previous_for_current
        )
    )

    # --------------------------------------------------------
    # Latest completed candle
    # --------------------------------------------------------

    completed = candles[-2]

    previous_for_completed = (
        candles[:-2]
    )

    actual = (
        shooting_star_score(
            completed,
            previous_for_completed
        )
    )

    # --------------------------------------------------------
    # Previous-close change
    # --------------------------------------------------------

    change = (

        (
            row["price"]
            -
            row["prev_close"]
        )
        /
        row["prev_close"]
    ) * 100

    avg_turnover = (
        AVG_TURNOVER.get(
            row["symbol"],
            0.0
        )
    )

    return {

        "symbol":
            row["symbol"],

        "price":
            row["price"],

        "change":
            change,

        "avg_turnover":
            avg_turnover,

        "developing": {

            "time":
                candle_time_label(
                    current[0]
                ),

            "score":
                developing[
                    "score"
                ]
        },

        "actual": {

            "time":
                candle_time_label(
                    completed[0]
                ),

            "score":
                actual[
                    "score"
                ],

            "is_actual":
                actual[
                    "actual"
                ]
        }
    }


# ============================================================
# MAIN SCAN
# ============================================================

def perform_scan():

    global LAST_SCAN_TIME

    if not TOKEN:

        raise RuntimeError(

            "UPSTOX_ACCESS_TOKEN "
            "Render Environment Variable "
            "में नहीं मिला।"
        )

    print(
        "Starting Shooting Star scan..."
    )

    # --------------------------------------------------------
    # 1. LIVE QUOTES
    # --------------------------------------------------------

    quotes = (
        fetch_all_live_quotes()
    )

    if not quotes:

        raise RuntimeError(

            "Upstox से live quotes नहीं मिले।"
        )

    # --------------------------------------------------------
    # 2. SYMBOL LIST
    # --------------------------------------------------------

    symbols = {

        x["symbol"]

        for x in quotes
    }

    # --------------------------------------------------------
    # 3. 20-DAY TURNOVER
    #
    # First scan may take longer because
    # turnover cache must be created.
    #
    # Later scans use the cache.
    # --------------------------------------------------------

    if not load_turnover_cache():

        build_turnover_cache(
            symbols
        )

    # --------------------------------------------------------
    # 4. FINAL UNIVERSE
    #
    # NSE EQ
    # Price >= ₹100
    # Average turnover >= ₹10 Cr
    # --------------------------------------------------------

    candidates = [

        x

        for x in quotes

        if AVG_TURNOVER.get(
            x["symbol"],
            0.0
        )
        >=
        MIN_AVG_TURNOVER
    ]

    print(
        "Final liquidity universe:",
        len(candidates)
    )

    # --------------------------------------------------------
    # 5. SPEED OPTIMIZATION
    #
    # Process the strongest current gainers first.
    # This is only a request-order optimization.
    # It does NOT change the final Shooting Star score.
    # --------------------------------------------------------

    candidates.sort(

        key=lambda x:

            (
                (
                    x["price"]
                    -
                    x["prev_close"]
                )
                /
                x["prev_close"]
            ),

        reverse=True
    )

    candidates = candidates[
        :MAX_INTRADAY_STOCKS
    ]

    print(
        "Intraday candle candidates:",
        len(candidates)
    )

    # --------------------------------------------------------
    # 6. 5-MINUTE CANDLE ANALYSIS
    # --------------------------------------------------------

    developing = []

    actual = []

    with ThreadPoolExecutor(

        max_workers=
            MAX_CANDLE_WORKERS

    ) as executor:

        futures = [

            executor.submit(
                analyze_stock,
                row
            )

            for row in candidates
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

                # ------------------------------------------------
                # DEVELOPING
                # ------------------------------------------------

                if (

                    result[
                        "developing"
                    ][
                        "score"
                    ]

                    >=

                    MIN_SCORE
                ):

                    developing.append(
                        result
                    )

                # ------------------------------------------------
                # ACTUAL
                # ------------------------------------------------

                if (

                    result[
                        "actual"
                    ][
                        "is_actual"
                    ]

                    and

                    result[
                        "actual"
                    ][
                        "score"
                    ]

                    >=

                    MIN_SCORE
                ):

                    actual.append(
                        result
                    )

            except Exception as e:

                print(
                    "Analysis error:",
                    repr(e)
                )

    # ========================================================
    # DEVELOPING RANK
    # ========================================================

    developing.sort(

        key=lambda x:

            x[
                "developing"
            ][
                "score"
            ],

        reverse=True
    )

    developing_rows = []

    for i, x in enumerate(

        developing,

        start=1
    ):

        developing_rows.append({

            "rank":
                i,

            "symbol":
                x["symbol"],

            "price":
                x["price"],

            "change":
                x["change"],

            "time":
                x[
                    "developing"
                ][
                    "time"
                ],

            "score":
                x[
                    "developing"
                ][
                    "score"
                ],

            "status":
                "Developing"
        })

    # ========================================================
    # ACTUAL RANK
    # ========================================================

    actual.sort(

        key=lambda x:

            x[
                "actual"
            ][
                "score"
            ],

        reverse=True
    )

    actual_rows = []

    for i, x in enumerate(

        actual,

        start=1
    ):

        actual_rows.append({

            "rank":
                i,

            "symbol":
                x["symbol"],

            "price":
                x["price"],

            "change":
                x["change"],

            "time":
                x[
                    "actual"
                ][
                    "time"
                ],

            "score":
                x[
                    "actual"
                ][
                    "score"
                ],

            "status":
                "Actual"
        })

    # ========================================================
    # SCAN COMPLETE TIME — INDIA / IST
    # ========================================================

    LAST_SCAN_TIME = (

        datetime.now(
            IST
        ).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
    )

    print(
        "Developing results:",
        len(developing_rows)
    )

    print(
        "Actual results:",
        len(actual_rows)
    )

    return {

        "developing":
            developing_rows,

        "actual":
            actual_rows
    }


# ============================================================
# BACKGROUND SCAN
# ============================================================

def scan_worker():

    global SCAN_RUNNING

    global LIVE_RESULTS

    global LAST_ERROR

    try:

        LIVE_RESULTS = (
            perform_scan()
        )

        LAST_ERROR = ""

    except Exception as e:

        LAST_ERROR = repr(e)

        print(
            "SCAN ERROR:",
            repr(e)
        )

    finally:

        SCAN_RUNNING = False


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
Shooting Star Rank Scanner
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
    padding:5px 4px;
    white-space:nowrap;
}

td{
    padding:5px 4px;
    border-bottom:1px solid #2c333c;
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
    line-height:1.45;
}

</style>

</head>

<body>

<div class="container">


<div class="card">

<h1>
Actual Shooting Star Rank Scanner
</h1>

<div class="subtitle">

NSE EQ • ₹100+ •
20D Avg Turnover ≥ ₹10 Cr •
5-Minute

</div>

<button onclick="runScan()">
SCAN NOW
</button>

<div id="status"
     class="status">

Ready

</div>

</div>


<!-- ======================================================
     DEVELOPING
     ====================================================== -->

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


<!-- ======================================================
     ACTUAL
     ====================================================== -->

<div class="card">

<div class="section">
🟢 ACTUAL SHOOTING STAR
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


<div class="card note">

<b>Scanner Rules</b><br><br>

5-minute timeframe<br>

NSE Equity EQ only<br>

Price ≥ ₹100<br>

20-Day Average Turnover ≥ ₹10 Crore<br>

Change % = Previous Close के मुकाबले<br><br>

<b>Developing</b> =
current 5-minute candle की live structure।<br>

<b>Actual</b> =
latest completed 5-minute candle जो
Shooting Star के निर्धारित structural criteria
पूरे करती है।<br><br>

Actual Shooting Star के लिए
अगली confirmation candle जरूरी नहीं है।<br><br>

Developing Score probability नहीं है।
यह current candle की Shooting Star
structure quality को 0–100 में बताता है।

</div>


</div>


<script>


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

        poll();

    }catch(e){

        status.innerText =
            "Scanner start error";

    }

}


async function poll(){

    try{

        const r =
            await fetch(
                "/api/results"
            );

        const data =
            await r.json();

        const status =
            document.getElementById(
                "status"
            );


        if(data.running){

            status.innerText =
                "Live quotes और 5-minute candles scan हो रहे हैं...";

            setTimeout(
                poll,
                800
            );

            return;

        }


        if(data.error){

            status.innerText =
                "Error: " +
                data.error;

            return;

        }


        status.innerText =
            "Scan complete • " +
            data.updated_at;


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


    }catch(e){

        setTimeout(
            poll,
            1500
        );

    }

}


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
            "इस समय कोई qualifying result नहीं मिला" +
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
            len(INSTRUMENTS),

        "scan_running":
            SCAN_RUNNING,

        "last_scan":
            LAST_SCAN_TIME,

        "last_error":
            LAST_ERROR
    })


# ============================================================
# START
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
