from flask import Flask, jsonify
import requests
import os
import json
import gzip
import io
import csv
import zipfile
import threading
import time
from datetime import datetime, timedelta, date, timezone
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import quote


app = Flask(__name__)


# ============================================================
# SHOOTING STAR RANK SCANNER
# ============================================================
#
# 5-Minute NSE Equity Scanner
#
# DEVELOPING:
# Current 5-minute candle की live structure.
#
# ACTUAL:
# आज की completed 5-minute candles में
# Shooting Star structure.
#
# IMPORTANT:
# आज की पुरानी completed candles भी scan होंगी।
# इसलिए सुबह बना Shooting Star बाद में भी दिखाई दे सकता है।
#
# Liquidity:
# 20-Day Average Turnover >= ₹5 Crore
#
# Price:
# >= ₹100
#
# Universe:
# NSE EQ only
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


# ------------------------------------------------------------
# LIQUIDITY SAFETY
# ------------------------------------------------------------
#
# पहले ₹10 Crore था।
# अब ₹5 Crore रखा गया है।
#
# इससे बहुत छोटे/कम कारोबार वाले shares बाहर रहेंगे,
# लेकिन ₹10 Cr से ₹5 Cr के बीच वाले अच्छे shares
# भी scanner में आ सकेंगे।
# ------------------------------------------------------------

MIN_AVG_TURNOVER = 50_000_000.0


# Minimum Shooting Star score
MIN_SCORE = 60.0


# ------------------------------------------------------------
# API SAFETY
# ------------------------------------------------------------
#
# Upstox standard APIs की 500 requests/min limit के अंदर
# रहने के लिए एक scan में लगभग 450 candle requests रखे गए हैं।
#
# बाकी requests:
# - quote batches
# - NSE turnover files
#
# अगले scan में अगला 450-stock block लिया जाएगा।
# ------------------------------------------------------------

CANDLE_BATCH_SIZE = 450


MAX_CANDLE_WORKERS = 20


# Scan interval
SCAN_INTERVAL_SECONDS = 65


# Display limits
MAX_ACTUAL_DISPLAY = 200
MAX_DEVELOPING_DISPLAY = 100


# ============================================================
# UPSTOX URLS
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
# CURRENT NSE UDiFF BHAVCOPY
# ============================================================
#
# NSE के पुराने Bhavcopy formats जुलाई 2024 में
# discontinue हो चुके हैं।
#
# Current UDiFF direct file format:
#
# BhavCopy_NSE_CM_0_0_0_YYYYMMDD_F_0000.csv.zip
#
# ============================================================

NSE_BHAV_URL = (
    "https://nsearchives.nseindia.com/content/cm/"
    "BhavCopy_NSE_CM_0_0_0_{date}_F_0000.csv.zip"
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


# आज के सभी discovered Actual signals
# key = SYMBOL|CANDLE_TIMESTAMP
TODAY_ACTUAL_SIGNALS = {}


SCAN_RUNNING = False

BACKGROUND_RUNNING = False

LAST_SCAN_TIME = ""

LAST_ERROR = ""

LAST_BATCH_INFO = ""

SCAN_LOCK = threading.Lock()

BATCH_INDEX = 0


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
            "Bearer " + TOKEN,

        "User-Agent":
            "ShootingStarRankScanner/Final"
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


# ============================================================
# MARKET TIME
# ============================================================

def market_open_now():

    now = datetime.now(
        IST
    )

    t = now.time()

    return (
        t >= datetime.strptime(
            "09:15",
            "%H:%M"
        ).time()

        and

        t <= datetime.strptime(
            "15:35",
            "%H:%M"
        ).time()
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
        raw.decode(
            "utf-8"
        )
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


    print(
        "Loaded",
        len(INSTRUMENTS),
        "NSE EQ instruments"
    )


    return INSTRUMENTS


# ============================================================
# LIVE QUOTES
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
                r.status_code
            )

            return []


        data = (
            r.json()
            .get(
                "data",
                {}
            )
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

                response_key.replace(
                    ":",
                    "|",
                    1
                )
            )


            info = INSTRUMENT_MAP.get(
                instrument_key
            )


            if not info:

                continue


            price = safe_float(
                q.get(
                    "last_price"
                )
            )


            previous_close = safe_float(
                q.get(
                    "prev_close_price"
                )
            )


            volume = safe_float(
                q.get(
                    "volume"
                )
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
        "Live quotes received:",
        len(results)
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
            "application/zip,*/*",

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
# NSE UDiFF BHAVCOPY
# ============================================================

def get_bhavcopy(
    date_obj
):

    try:

        date_str = date_obj.strftime(
            "%Y%m%d"
        )


        url = NSE_BHAV_URL.format(
            date=date_str
        )


        r = nse_session().get(

            url,

            timeout=20
        )


        if r.status_code != 200:

            print(
                "NSE bhavcopy HTTP:",
                r.status_code,
                date_str
            )

            return {}


        if len(r.content) < 500:

            return {}


        # ----------------------------------------------------
        # Current UDiFF is ZIP containing CSV.
        # ----------------------------------------------------

        raw_csv = None


        try:

            with zipfile.ZipFile(
                io.BytesIO(
                    r.content
                )
            ) as z:

                csv_names = [

                    name

                    for name in z.namelist()

                    if name.lower().endswith(
                        ".csv"
                    )
                ]


                if not csv_names:

                    return {}


                raw_csv = z.read(
                    csv_names[0]
                )


        except zipfile.BadZipFile:

            # Some mirrors may return CSV directly.
            raw_csv = r.content


        if not raw_csv:

            return {}


        text = raw_csv.decode(

            "utf-8-sig",

            errors="replace"
        )


        reader = csv.DictReader(

            io.StringIO(
                text
            )
        )


        result = {}


        for row in reader:

            if not row:

                continue


            normalized = {

                str(k)
                .strip()
                .upper():

                str(v)
                .strip()

                for k, v in row.items()

                if k is not None
            }


            series = (

                normalized.get(
                    "SCTY SRS"
                )

                or

                normalized.get(
                    "SCTYSRS"
                )

                or

                normalized.get(
                    "SERIES"
                )

                or

                ""
            ).upper()


            if series != "EQ":

                continue


            symbol = (

                normalized.get(
                    "TCKRSYMB"
                )

                or

                normalized.get(
                    "TCKR SYMB"
                )

                or

                normalized.get(
                    "SYMBOL"
                )

                or

                ""
            ).strip().upper()


            if not symbol:

                continue


            turnover_text = (

                normalized.get(
                    "TTLTRFVAL"
                )

                or

                normalized.get(
                    "TTL TRF VAL"
                )

                or

                normalized.get(
                    "TOTTRDVAL"
                )

                or

                ""
            )


            volume_text = (

                normalized.get(
                    "TTLTRDG VOL"
                )

                or

                normalized.get(
                    "TTLTRDQTY"
                )

                or

                normalized.get(
                    "TTL_TRD_QNTY"
                )

                or

                normalized.get(
                    "TOTTRDQTY"
                )

                or

                ""
            )


            close_text = (

                normalized.get(
                    "CLSPRIC"
                )

                or

                normalized.get(
                    "CLS PRIC"
                )

                or

                normalized.get(
                    "CLOSE_PRICE"
                )

                or

                normalized.get(
                    "CLOSE"
                )

                or

                ""
            )


            turnover = safe_float(

                turnover_text
                .replace(
                    ",",
                    ""
                )
            )


            close = safe_float(

                close_text
                .replace(
                    ",",
                    ""
                )
            )


            volume = safe_float(

                volume_text
                .replace(
                    ",",
                    ""
                )
            )


            if turnover <= 0:

                if (
                    close > 0
                    and
                    volume > 0
                ):

                    turnover = (
                        close
                        *
                        volume
                    )


            if turnover > 0:

                result[
                    symbol
                ] = turnover


        print(

            "NSE turnover:",
            date_str,
            len(result)
        )


        return result


    except Exception as e:

        print(

            "Bhavcopy error:",
            date_obj,
            repr(e)
        )


        return {}


# ============================================================
# PREVIOUS TRADING DATES
# ============================================================

def previous_weekdays(
    max_days=30
):

    dates = []


    d = (

        datetime.now(
            IST
        ).date()

        -

        timedelta(
            days=1
        )
    )


    while len(dates) < max_days:

        if d.weekday() < 5:

            dates.append(
                d
            )


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

            saved = json.load(
                f
            )


        if (

            saved.get(
                "date"
            )

            ==

            str(
                date.today()
            )

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

                len(
                    AVG_TURNOVER
                )
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


    dates = previous_weekdays(
        30
    )


    # Maximum 30 NSE requests.
    # We stop after 20 successful trading days
    # have been collected.
    successful_days = 0


    with ThreadPoolExecutor(

        max_workers=6

    ) as executor:


        futures = {

            executor.submit(
                get_bhavcopy,
                d
            ): d

            for d in dates
        }


        for future in as_completed(
            futures
        ):


            try:

                day = future.result()


                if not day:

                    continue


                successful_days += 1


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


                if successful_days >= 20:

                    break


            except Exception:

                pass


    # Only symbols having all 20 days.
    AVG_TURNOVER = {

        s:

            sums[s]
            /
            20.0

        for s in symbols

        if counts[s] >= 20
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
                        str(
                            date.today()
                        ),

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

        len(
            AVG_TURNOVER
        )
    )


    return len(
        AVG_TURNOVER
    )


# ============================================================
# 5-MINUTE CANDLES
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
# CANDLE TIME HELPERS
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


    end = (

        dt

        +

        timedelta(
            minutes=5
        )
    )


    return end <= now


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

        o = float(
            candle[1]
        )

        h = float(
            candle[2]
        )

        l = float(
            candle[3]
        )

        c = float(
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

        h - l,

        0.000001
    )


    body = abs(
        c - o
    )


    body_safe = max(

        body,

        candle_range
        *
        0.02,

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


    # --------------------------------------------------------
    # 1. Upper wick
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # 2. Body near bottom
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # 3. Small lower wick
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # 4. Close near low
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # 5. Previous short-term uptrend
    # --------------------------------------------------------

    uptrend_score = 0.0


    if len(
        previous_candles
    ) >= 3:


        try:

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


        except Exception:

            uptrend_score = 0.0


    # --------------------------------------------------------
    # 6. Bearish / small body
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # FINAL SCORE
    # --------------------------------------------------------

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


    # --------------------------------------------------------
    # ACTUAL SHOOTING STAR STRUCTURE
    # --------------------------------------------------------

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


    # Upstox returns newest candle first.
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


        timestamp = candle[0]


        if candle_completed(
            timestamp,
            now
        ):

            completed.append(
                candle
            )

        else:

            developing_candle = candle


    actual_signals = []


    # --------------------------------------------------------
    # Scan ALL completed candles from today.
    # --------------------------------------------------------

    for index, candle in enumerate(
        completed
    ):


        previous_candles = (
            completed[:index]
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
                    (
                        (
                            row["price"]
                            -
                            row["prev_close"]
                        )
                        /
                        row["prev_close"]
                    )
                    *
                    100,

                "time":
                    candle_time_label(
                        candle[0]
                    ),

                "timestamp":
                    candle[0],

                "score":
                    result["score"],

                "avg_turnover":
                    AVG_TURNOVER.get(
                        row["symbol"],
                        0.0
                    )
            })


    # --------------------------------------------------------
    # Current developing candle
    # --------------------------------------------------------

    developing_result = None


    if developing_candle:

        index = len(
            completed
        )


        previous_candles = (
            completed
        )


        developing_result = (
            shooting_star_score(

                developing_candle,

                previous_candles
            )
        )


    return {

        "symbol":
            row["symbol"],

        "price":
            row["price"],

        "change":
            (
                (
                    row["price"]
                    -
                    row["prev_close"]
                )
                /
                row["prev_close"]
            )
            *
            100,

        "actual_signals":
            actual_signals,

        "developing":

            {

                "time":

                    (
                        candle_time_label(
                            developing_candle[0]
                        )

                        if

                        developing_candle

                        else

                        ""
                    ),

                "score":

                    (
                        developing_result[
                            "score"
                        ]

                        if

                        developing_result

                        else

                        0.0
                    )
            }
    }


# ============================================================
# BUILD CANDIDATE UNIVERSE
# ============================================================

def build_candidates(
    quotes
):

    candidates = []


    for row in quotes:

        avg_turnover = (

            AVG_TURNOVER.get(
                row["symbol"],
                0.0
            )
        )


        if avg_turnover < MIN_AVG_TURNOVER:

            continue


        candidates.append(
            row
        )


    return candidates


# ============================================================
# ROTATING BATCH
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


    start = (

        BATCH_INDEX
        *
        CANDLE_BATCH_SIZE
    ) % total


    if total <= CANDLE_BATCH_SIZE:

        batch = candidates


    else:

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

    global TODAY_ACTUAL_SIGNALS


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
    # 1. Instruments
    # --------------------------------------------------------

    load_instruments()


    # --------------------------------------------------------
    # 2. Live quotes
    # --------------------------------------------------------

    quotes = (
        fetch_all_live_quotes()
    )


    if not quotes:

        raise RuntimeError(

            "Upstox से live quotes नहीं मिले।"
        )


    # --------------------------------------------------------
    # 3. Turnover cache
    # --------------------------------------------------------

    if not load_turnover_cache():

        build_turnover_cache(

            {
                x["symbol"]

                for x in quotes
            }
        )


    # --------------------------------------------------------
    # 4. Liquidity universe
    # --------------------------------------------------------

    candidates = build_candidates(
        quotes
    )


    if not candidates:

        raise RuntimeError(

            "₹5 Crore liquidity condition "
            "पूरी करने वाले stocks नहीं मिले।"
        )


    # --------------------------------------------------------
    # 5. Rotating 450-stock batch
    # --------------------------------------------------------

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

        " liquid stocks universe"
    )


    print(
        LAST_BATCH_INFO
    )


    # --------------------------------------------------------
    # 6. 5-minute candle analysis
    # --------------------------------------------------------

    developing = []

    new_actual = []


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


                # ------------------------------------------------
                # Developing
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

                    developing.append({

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
                            result[
                                "developing"
                            ][
                                "time"
                            ],

                        "score":
                            result[
                                "developing"
                            ][
                                "score"
                            ],

                        "status":
                            "Developing"
                    })


                # ------------------------------------------------
                # Actual
                # ------------------------------------------------

                for signal in result[
                    "actual_signals"
                ]:


                    signal_key = (

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


                    if signal_key not in TODAY_ACTUAL_SIGNALS:

                        TODAY_ACTUAL_SIGNALS[
                            signal_key
                        ] = signal


                        new_actual.append(
                            signal
                        )


            except Exception as e:

                print(
                    "Analysis error:",
                    repr(e)
                )


    # --------------------------------------------------------
    # Developing ranking
    # --------------------------------------------------------

    developing.sort(

        key=lambda x:

            x[
                "score"
            ],

        reverse=True
    )


    developing_rows = []


    for i, x in enumerate(

        developing[
            :MAX_DEVELOPING_DISPLAY
        ],

        start=1
    ):


        developing_rows.append({

            "rank":
                i,

            "symbol":
                x[
                    "symbol"
                ],

            "price":
                round(
                    x[
                        "price"
                    ],
                    2
                ),

            "change":
                round(
                    x[
                        "change"
                    ],
                    2
                ),

            "time":
                x[
                    "time"
                ],

            "score":
                round(
                    x[
                        "score"
                    ],
                    1
                ),

            "status":
                "Developing"
        })


    # --------------------------------------------------------
    # Actual ranking
    # --------------------------------------------------------

    actual_all = list(
        TODAY_ACTUAL_SIGNALS.values()
    )


    actual_all.sort(

        key=lambda x:

            (
                -float(
                    x[
                        "score"
                    ]
                ),

                x[
                    "symbol"
                ],

                x[
                    "timestamp"
                ]
            )
    )


    actual_rows = []


    for i, x in enumerate(

        actual_all[
            :MAX_ACTUAL_DISPLAY
        ],

        start=1
    ):


        actual_rows.append({

            "rank":
                i,

            "symbol":
                x[
                    "symbol"
                ],

            "price":
                round(
                    x[
                        "price"
                    ],
                    2
                ),

            "change":
                round(
                    x[
                        "change"
                    ],
                    2
                ),

            "time":
                x[
                    "time"
                ],

            "score":
                round(
                    x[
                        "score"
                    ],
                    1
                ),

            "status":
                "Actual"
        })


    # --------------------------------------------------------
    # Save results
    # --------------------------------------------------------

    LIVE_RESULTS = {

        "developing":
            developing_rows,

        "actual":
            actual_rows
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
            developing_rows
        )
    )


    print(
        "Actual today:",
        len(
            actual_rows
        )
    )


    return LIVE_RESULTS


# ============================================================
# BACKGROUND SCAN WORKER
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

        LAST_ERROR = repr(
            e
        )

        print(
            "SCAN ERROR:",
            repr(e)
        )


    finally:

        SCAN_RUNNING = False


# ============================================================
# START ONE SCAN
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
# AUTOMATIC BACKGROUND SCANNER
# ============================================================

def background_loop():

    global BACKGROUND_RUNNING


    if BACKGROUND_RUNNING:

        return


    BACKGROUND_RUNNING = True


    print(
        "Background scanner started."
    )


    while True:


        try:

            if market_open_now():

                if not SCAN_RUNNING:

                    start_scan()


            time.sleep(
                SCAN_INTERVAL_SECONDS
            )


        except Exception as e:

            print(
                "Background loop error:",
                repr(e)
            )

            time.sleep(
                SCAN_INTERVAL_SECONDS
            )


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
    line-height:1.5;
}

</style>

</head>

<body>

<div class="container">


<div class="card">

<h1>
⭐ Shooting Star Rank Scanner
</h1>

<div class="subtitle">

NSE EQ • ₹100+ •
20D Avg Turnover ≥ ₹5 Cr •
5-Minute

</div>

<button onclick="runScan()">
SCAN NOW
</button>

<div id="status"
     class="status">

Scanner ready

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


<div class="card note">

<b>Scanner Rules</b><br><br>

5-minute timeframe<br>

NSE Equity EQ only<br>

Price ≥ ₹100<br>

20-Day Average Turnover ≥ ₹5 Crore<br><br>

<b>Actual</b> =
आज की सभी completed 5-minute candles में
बना हुआ qualifying Shooting Star।<br><br>

<b>SS Time</b> =
जिस 5-minute candle में Shooting Star बना,
उसका समय।<br><br>

<b>Developing</b> =
वर्तमान चल रही 5-minute candle की structure।<br><br>

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

        const r =
            await fetch(
                "/api/results?ts=" +
                Date.now()
            );


        const data =
            await r.json();


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
                (data.updated_at || "-")

                +

                " • "

                +

                (data.batch_info || "")

                +

                " • "

                +

                "Today's Actual: " +

                (
                    data.results.actual
                    || []
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

    catch(e){

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

    catch(e){

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

        "liquid_stocks":
            len(
                AVG_TURNOVER
            ),

        "scan_running":
            SCAN_RUNNING,

        "background_running":
            BACKGROUND_RUNNING,

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
# START
# ============================================================

if __name__ == "__main__":


    # Start automatic market scanner.
    threading.Thread(

        target=background_loop,

        daemon=True,

        name="shooting-star-background"

    ).start()


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
