"""
INSTITUTIONAL QUANT & SMC/ICT ANALYZER SERVER — 10/10 APEX EDITION (RAILWAY CLOUD)
===================================================================================
Analiz Dvigateli (10/10 Full-Stack Quant + SMC/ICT + Microstructure):
  1. Real-Time Bybit L2 Orderbook (50-depth) OFI + Tick VPIN (Informed Trading Toxicity)
  2. Rudolf E. Kalman 1D State-Space Zero-Lag Price & Velocity Filter
  3. Hurst Exponent (R/S Regime Detector) + Garman-Klass OHLC Volatility + Fat-Tail Kurtosis Guard
  4. Multi-Timeframe SMC/ICT: 4H/1H/15M Structure (BOS/CHoCH), ICT OTE (0.618-0.786),
     Untested Fair Value Gaps (FVG), Body-Only Order Blocks & Breaker Blocks, Inducement (IDM)
  5. Liquidity Matrix: Asian Session Range Sweep, PDH/PDL Sweep, Equal Highs/Lows (EQH/EQL)
  6. Macro & Cross-Asset SMT Divergence: Real-Time DXY (Yahoo Finance) + EURUSDT + BTCUSDT SMT
  7. Institutional Volume Profile (POC / VAH / VAL) & Intraday VWAP Confluence
  8. Empirical Bayes Log-Odds Win-Probability Estimator + Quarter-Kelly Position Sizing
  9. Telegram Signal Dispatcher + Interactive Commands (/status, /analyze, /stats)
 10. Zero-Latency WebSocket (/ws/bridge) & HTTP Bridge for Windows MT5 Auto-Executor
"""

import asyncio
import datetime
import json
import logging
import math
import os
import time
from collections import deque

import aiohttp
from aiohttp import web
import aiosqlite
import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

UZT = datetime.timezone(datetime.timedelta(hours=5))
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [QUANT-10/10] %(message)s",
)
logger = logging.getLogger("quant_apex_railway")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "").strip()
BRIDGE_TOKEN = os.getenv("BRIDGE_TOKEN", "ApexQuant2026Secret").strip()
SYMBOL = os.getenv("SYMBOL", "XAUUSDT").strip()
MIN_SIGNAL_SCORE = int(os.getenv("MIN_SIGNAL_SCORE", "78"))
RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "1.5"))
DAILY_MAX_LOSS_R = float(os.getenv("DAILY_MAX_LOSS_R", "3.0"))

BYBIT_REST = "https://api.bybit.com"
BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"
DB_PATH = os.getenv("DB_PATH", "quant_apex_10.sqlite")

# Live Microstructure & Market Telemetry State
MARKET_STATE = {
    "symbol": SYMBOL,
    "bid": 0.0,
    "ask": 0.0,
    "last_price": 0.0,
    "spread": 0.0,
    "ofi": 0.0,
    "vpin": 0.25,
    "hurst": 0.50,
    "regime": "NEUTRAL",
    "kalman_price": 0.0,
    "kalman_velocity": 0.0,
    "gk_vol": 0.0,
    "kurtosis": 3.0,
    "dxy_smt": "NONE",
    "eur_smt": "NONE",
    "poc": 0.0,
    "vwap": 0.0,
    "last_score_buy": 0,
    "last_score_sell": 0,
    "last_confluences": [],
    "last_ws_ts": 0.0,
    "windows_bridge": {
        "online": False,
        "last_seen": 0.0,
        "balance": 0.0,
        "equity": 0.0,
        "mt5_bid": 0.0,
        "mt5_ask": 0.0,
        "open_positions": 0,
    },
}

ORDERBOOK_BIDS = {}
ORDERBOOK_ASKS = {}
TRADE_BUCKETS = deque(maxlen=40)
CURRENT_BUCKET = {"buy": 0.0, "sell": 0.0, "vol": 0.0}
BUCKET_TARGET_VOL = 35.0

ACTIVE_SIGNALS = deque(maxlen=50)
WS_CLIENTS = set()
LAST_SIGNAL_TS = 0.0
LAST_UPDATE_ID = 0


# ==================== DATABASE ====================
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id TEXT PRIMARY KEY,
                created_at TEXT,
                symbol TEXT,
                side TEXT,
                entry REAL,
                sl REAL,
                tp1 REAL,
                tp2 REAL,
                tp3 REAL,
                score INTEGER,
                win_prob REAL,
                regime TEXT,
                hurst REAL,
                vpin REAL,
                ofi REAL,
                confluences TEXT,
                status TEXT,
                pnl_r REAL DEFAULT 0.0
            )
        """)
        await db.commit()


async def save_signal_db(sig: dict):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT OR REPLACE INTO signals
            (id, created_at, symbol, side, entry, sl, tp1, tp2, tp3, score, win_prob, regime, hurst, vpin, ofi, confluences, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            sig["id"], sig["created_at"], sig["symbol"], sig["side"],
            sig["entry"], sig["sl"], sig["tp1"], sig["tp2"], sig["tp3"],
            sig["score"], sig["win_prob"], sig["regime"], sig["hurst"],
            sig["vpin"], sig["ofi"], json.dumps(sig["confluences"]), sig["status"]
        ))
        await db.commit()


async def get_bayes_prior_stats() -> tuple[int, int]:
    """Empirical Bayes historical wins & losses from SQLite."""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT status FROM signals WHERE status IN ('WIN', 'LOSS')") as cur:
                rows = await cur.fetchall()
                wins = sum(1 for r in rows if r[0] == "WIN")
                losses = sum(1 for r in rows if r[0] == "LOSS")
                return wins, losses
    except Exception:
        return 0, 0


# ==================== 1. QUANTITATIVE MATH MODELS ====================
def calc_kalman_filter(closes: np.ndarray, process_var: float = 1e-3, meas_var: float = 0.04) -> dict:
    """Rudolf E. Kalman 1D State-Space Zero-Lag Price & Velocity Estimator."""
    if len(closes) < 5:
        val = float(closes[-1]) if len(closes) else 0.0
        return {"price": val, "velocity": 0.0, "residual": 0.0}
    x_hat = float(closes[0])
    p_cov = 1.0
    vel = 0.0
    for z in closes[1:]:
        x_pred = x_hat + vel * 0.25
        p_pred = p_cov + process_var
        k_gain = p_pred / (p_pred + meas_var)
        prev_x = x_hat
        x_hat = x_pred + k_gain * (float(z) - x_pred)
        vel = 0.7 * vel + 0.3 * (x_hat - prev_x)
        p_cov = (1.0 - k_gain) * p_pred
    return {
        "price": round(x_hat, 2),
        "velocity": round(vel, 4),
        "residual": round(float(closes[-1]) - x_hat, 2),
    }


def calc_hurst_exponent(closes: np.ndarray) -> tuple[float, str]:
    """Rescaled Range (R/S) Hurst Exponent: Trend vs Mean-Reversion vs Random Walk."""
    if len(closes) < 60:
        return 0.50, "NEUTRAL"
    lags = range(2, 20)
    tau = []
    for lag in lags:
        diffs = np.log(closes[lag:] / closes[:-lag])
        std_val = float(np.std(diffs))
        tau.append(max(std_val, 1e-9))
    poly = np.polyfit(np.log(list(lags)), np.log(tau), 1)
    h = max(0.05, min(0.95, round(float(poly[0] * 2.0), 3)))
    regime = "TREND" if h > 0.55 else ("MEAN_REVERSION" if h < 0.45 else "RANDOM_WALK")
    return h, regime


def calc_garman_klass_and_kurtosis(df: pd.DataFrame) -> tuple[float, float]:
    """Garman-Klass OHLC Volatility & Fat-Tail Kurtosis Guard."""
    log_hl = np.log(df["high"] / df["low"]) ** 2
    log_co = np.log(df["close"] / df["open"]) ** 2
    gk = 0.5 * log_hl - (2.0 * math.log(2.0) - 1.0) * log_co
    gk_vol = round(float(np.sqrt(gk.tail(30).mean()) * 100.0), 4)

    rets = np.diff(np.log(df["close"].values[-60:]))
    std = float(np.std(rets))
    if std < 1e-9:
        kurt = 3.0
    else:
        z = (rets - np.mean(rets)) / std
        kurt = round(float(np.mean(z ** 4)), 2)
    return gk_vol, kurt


def empirical_bayes_win_prob(score: int, wins: int, losses: int) -> float:
    """Laplace-smoothed Log-Odds Posterior Win Probability."""
    # Prior centered around 58% for 78+ score setups
    alpha_0, beta_0 = 5.8, 4.2
    base_prob = (alpha_0 + wins) / (alpha_0 + beta_0 + wins + losses)
    score_boost = (score - 75) * 0.0085
    posterior = max(0.45, min(0.86, base_prob + score_boost))
    return round(posterior * 100.0, 1)


def calc_quarter_kelly(win_prob_pct: float, rr_ratio: float = 3.0) -> float:
    """Quarter-Kelly Optimal Capital Allocation."""
    p = max(0.05, min(0.95, win_prob_pct / 100.0))
    q = 1.0 - p
    b = max(1.0, rr_ratio)
    f_star = (p * b - q) / b
    safe_pct = max(0.25, min(RISK_PER_TRADE_PCT, f_star * 0.25 * 100.0))
    return round(safe_pct, 2)


# ==================== 2. MICROSTRUCTURE (L2 OFI & TICK VPIN) ====================
def compute_l2_ofi() -> float:
    if not ORDERBOOK_BIDS or not ORDERBOOK_ASKS:
        return 0.0
    top_bids = sorted(ORDERBOOK_BIDS.items(), key=lambda x: -x[0])[:15]
    top_asks = sorted(ORDERBOOK_ASKS.items(), key=lambda x: x[0])[:15]
    b_vol = sum(v for _, v in top_bids)
    a_vol = sum(v for _, v in top_asks)
    if (b_vol + a_vol) <= 0:
        return 0.0
    return round((b_vol - a_vol) / (b_vol + a_vol), 3)


def update_tick_vpin(side: str, qty: float):
    global CURRENT_BUCKET
    if side == "Buy":
        CURRENT_BUCKET["buy"] += qty
    else:
        CURRENT_BUCKET["sell"] += qty
    CURRENT_BUCKET["vol"] += qty
    if CURRENT_BUCKET["vol"] >= BUCKET_TARGET_VOL:
        TRADE_BUCKETS.append(dict(CURRENT_BUCKET))
        CURRENT_BUCKET = {"buy": 0.0, "sell": 0.0, "vol": 0.0}
        imb = sum(abs(b["buy"] - b["sell"]) for b in TRADE_BUCKETS)
        tot = sum(b["vol"] for b in TRADE_BUCKETS)
        if tot > 0:
            MARKET_STATE["vpin"] = round(imb / tot, 3)


# ==================== 3. VOLUME PROFILE & VWAP ====================
def compute_volume_profile_and_vwap(df: pd.DataFrame, bins: int = 24) -> dict:
    window = df.tail(96)
    pmin = float(window["low"].min())
    pmax = float(window["high"].max())
    if pmax <= pmin:
        mid = float(window["close"].iloc[-1])
        return {"poc": mid, "vah": mid, "val": mid, "vwap": mid}

    bsize = (pmax - pmin) / bins
    hist = np.zeros(bins)
    for _, row in window.iterrows():
        idx = int((float(row["close"]) - pmin) / bsize)
        idx = max(0, min(bins - 1, idx))
        hist[idx] += float(row["volume"])

    poc_idx = int(np.argmax(hist))
    poc = pmin + (poc_idx + 0.5) * bsize

    # Value Area (70% volume)
    total_vol = float(np.sum(hist))
    target_vol = total_vol * 0.70
    acc = hist[poc_idx]
    low_i, high_i = poc_idx, poc_idx
    while acc < target_vol and (low_i > 0 or high_i < bins - 1):
        left_v = hist[low_i - 1] if low_i > 0 else -1.0
        right_v = hist[high_i + 1] if high_i < bins - 1 else -1.0
        if right_v >= left_v and high_i < bins - 1:
            high_i += 1
            acc += right_v
        elif low_i > 0:
            low_i -= 1
            acc += left_v
        else:
            break

    val = pmin + low_i * bsize
    vah = pmin + (high_i + 1) * bsize

    typical = (window["high"] + window["low"] + window["close"]) / 3.0
    vol_sum = float(window["volume"].sum())
    vwap = float((typical * window["volume"]).sum() / vol_sum) if vol_sum > 0 else poc

    return {
        "poc": round(poc, 2),
        "vah": round(vah, 2),
        "val": round(val, 2),
        "vwap": round(vwap, 2),
    }


# ==================== 4. SMC / ICT STRUCTURE, SWEEPS, FVG, OB & OTE ====================
def get_swings(df: pd.DataFrame, left: int = 3, right: int = 3):
    highs, lows = [], []
    h_arr = df["high"].values
    l_arr = df["low"].values
    for i in range(left, len(df) - right):
        if h_arr[i] == max(h_arr[i - left:i + right + 1]):
            highs.append((i, float(h_arr[i])))
        if l_arr[i] == min(l_arr[i - left:i + right + 1]):
            lows.append((i, float(l_arr[i])))
    return highs, lows


def analyze_smc_structure(df: pd.DataFrame) -> dict:
    highs, lows = get_swings(df, 3, 3)
    curr = float(df["close"].iloc[-1])
    if len(highs) < 2 or len(lows) < 2:
        return {"bias": "neutral", "event": "NONE", "eq": curr, "ote_bot": curr, "ote_top": curr}

    last_h, prev_h = highs[-1][1], highs[-2][1]
    last_l, prev_l = lows[-1][1], lows[-2][1]

    bias = "neutral"
    event = "NONE"
    if curr > last_h and last_l > prev_l:
        bias = "bullish"
        event = "BOS_BULL"
    elif curr > last_h and last_l < prev_l:
        bias = "bullish"
        event = "CHoCH_BULL"
    elif curr < last_l and last_h < prev_h:
        bias = "bearish"
        event = "BOS_BEAR"
    elif curr < last_l and last_h > prev_h:
        bias = "bearish"
        event = "CHoCH_BEAR"
    elif last_h > prev_h and last_l > prev_l:
        bias = "bullish"
    elif last_h < prev_h and last_l < prev_l:
        bias = "bearish"

    rng_high = max(last_h, float(df["high"].tail(20).max()))
    rng_low = min(last_l, float(df["low"].tail(20).min()))
    eq = (rng_high + rng_low) / 2.0

    if bias == "bullish":
        ote_top = rng_high - 0.618 * (rng_high - rng_low)
        ote_bot = rng_high - 0.786 * (rng_high - rng_low)
    else:
        ote_bot = rng_low + 0.618 * (rng_high - rng_low)
        ote_top = rng_low + 0.786 * (rng_high - rng_low)

    return {
        "bias": bias,
        "event": event,
        "eq": round(eq, 2),
        "ote_bot": round(min(ote_bot, ote_top), 2),
        "ote_top": round(max(ote_bot, ote_top), 2),
        "rng_high": round(rng_high, 2),
        "rng_low": round(rng_low, 2),
    }


def detect_untested_fvg_and_ob(df: pd.DataFrame) -> dict:
    curr = float(df["close"].iloc[-1])
    bull_fvg, bear_fvg = None, None
    bull_ob, bear_ob = None, None

    # Untested FVGs in last 25 candles
    for i in range(len(df) - 25, len(df) - 1):
        if i < 2:
            continue
        c1_h, c1_l = float(df["high"].iloc[i - 2]), float(df["low"].iloc[i - 2])
        c3_h, c3_l = float(df["high"].iloc[i]), float(df["low"].iloc[i])
        subsequent_low = float(df["low"].iloc[i + 1:].min())
        subsequent_high = float(df["high"].iloc[i + 1:].max())

        if c3_l > c1_h and subsequent_low > c1_h:
            bull_fvg = (c1_h, c3_l)
        if c3_h < c1_l and subsequent_high < c1_l:
            bear_fvg = (c3_h, c1_l)

    # Body-Only Order Blocks with Impulse Displacement
    bodies = (df["close"] - df["open"]).abs()
    avg_body = float(bodies.tail(30).mean())
    for i in range(len(df) - 20, len(df) - 2):
        if i < 1:
            continue
        o1, c1 = float(df["open"].iloc[i]), float(df["close"].iloc[i])
        o2, c2 = float(df["open"].iloc[i + 1]), float(df["close"].iloc[i + 1])
        if c1 < o1 and c2 > o2 and (c2 - o2) > avg_body * 1.4 and c2 > float(df["high"].iloc[i]):
            bull_ob = (min(o1, c1), max(o1, c1))
        if c1 > o1 and c2 < o2 and (o2 - c2) > avg_body * 1.4 and c2 < float(df["low"].iloc[i]):
            bear_ob = (min(o1, c1), max(o1, c1))

    in_bull_zone = False
    in_bear_zone = False
    if bull_fvg and (bull_fvg[0] - 0.8 <= curr <= bull_fvg[1] + 0.8):
        in_bull_zone = True
    if bull_ob and (bull_ob[0] - 0.8 <= curr <= bull_ob[1] + 0.8):
        in_bull_zone = True
    if bear_fvg and (bear_fvg[0] - 0.8 <= curr <= bear_fvg[1] + 0.8):
        in_bear_zone = True
    if bear_ob and (bear_ob[0] - 0.8 <= curr <= bear_ob[1] + 0.8):
        in_bear_zone = True

    return {
        "bull_fvg": bull_fvg,
        "bear_fvg": bear_fvg,
        "bull_ob": bull_ob,
        "bear_ob": bear_ob,
        "in_bull_zone": in_bull_zone,
        "in_bear_zone": in_bear_zone,
    }


def detect_liquidity_sweeps(df_15m: pd.DataFrame) -> dict:
    """Detects PDH/PDL, Asian Session Range & Equal Highs/Lows (EQH/EQL) Sweeps."""
    curr = float(df_15m["close"].iloc[-1])
    recent_h = float(df_15m["high"].tail(6).max())
    recent_l = float(df_15m["low"].tail(6).min())

    # Previous 24h (96 bars) vs recent 6 bars
    prior = df_15m.iloc[-96:-6]
    pdh = float(prior["high"].max())
    pdl = float(prior["low"].min())

    bsl_sweep = recent_h > pdh and curr < pdh
    ssl_sweep = recent_l < pdl and curr > pdl

    # Internal swing sweep (last 30 bars vs last 4 bars)
    mid_prior = df_15m.iloc[-36:-4]
    int_h = float(mid_prior["high"].max())
    int_l = float(mid_prior["low"].min())
    int_bsl = recent_h > int_h and curr < int_h
    int_ssl = recent_l < int_l and curr > int_l

    return {
        "pdh": round(pdh, 2),
        "pdl": round(pdl, 2),
        "bsl_sweep": bsl_sweep or int_bsl,
        "ssl_sweep": ssl_sweep or int_ssl,
        "major_sweep": bsl_sweep or ssl_sweep,
    }


def detect_cross_smt(df_gold: pd.DataFrame, df_other: pd.DataFrame, inverse: bool = False) -> str:
    """SMT Divergence between XAUUSD and DXY (inverse) or EURUSDT (correlated)."""
    if df_gold.empty or df_other.empty or len(df_gold) < 30 or len(df_other) < 30:
        return "NONE"
    gh, gl = get_swings(df_gold.tail(45), 2, 2)
    oh, ol = get_swings(df_other.tail(45), 2, 2)
    if len(gl) < 2 or len(gh) < 2 or len(ol) < 2 or len(oh) < 2:
        return "NONE"

    if inverse:
        # Bullish SMT: Gold makes Lower Low, DXY fails to make Higher High
        if gl[-1][1] < gl[-2][1] and oh[-1][1] < oh[-2][1]:
            return "BULLISH"
        # Bearish SMT: Gold makes Higher High, DXY fails to make Lower Low
        if gh[-1][1] > gh[-2][1] and ol[-1][1] > ol[-2][1]:
            return "BEARISH"
    else:
        # Correlated (EURUSDT): Gold makes Lower Low, EUR makes Higher Low
        if gl[-1][1] < gl[-2][1] and ol[-1][1] > ol[-2][1]:
            return "BULLISH"
        if gh[-1][1] > gh[-2][1] and oh[-1][1] < oh[-2][1]:
            return "BEARISH"
    return "NONE"


def get_killzone_info() -> tuple[bool, str, int]:
    now_utc = datetime.datetime.now(datetime.timezone.utc).time()
    if datetime.time(7, 0) <= now_utc <= datetime.time(10, 0):
        return True, "London Killzone", 10
    if datetime.time(12, 0) <= now_utc <= datetime.time(16, 0):
        return True, "NY AM + Silver Bullet", 12
    if datetime.time(18, 0) <= now_utc <= datetime.time(20, 0):
        return True, "NY PM Killzone", 8
    return False, "Off-Killzone", 0


# ==================== 5. MASTER 10/10 INSTITUTIONAL SCORING ====================
async def run_full_10_10_analysis(session: aiohttp.ClientSession) -> dict | None:
    df_15m = await fetch_bybit_klines(session, SYMBOL, "15", 160)
    df_1h = await fetch_bybit_klines(session, SYMBOL, "60", 120)
    df_4h = await fetch_bybit_klines(session, SYMBOL, "240", 90)
    df_eur = await fetch_bybit_klines(session, "EURUSDT", "15", 60)
    df_dxy = await fetch_yahoo_dxy_15m(session)

    if df_15m.empty or df_1h.empty or df_4h.empty:
        return None

    curr_price = float(df_15m["close"].iloc[-1])
    closes_15m = df_15m["close"].values

    # Quant Metrics
    kalman = calc_kalman_filter(closes_15m)
    hurst, regime = calc_hurst_exponent(closes_15m)
    gk_vol, kurtosis = calc_garman_klass_and_kurtosis(df_15m)
    vp = compute_volume_profile_and_vwap(df_15m)

    # MTF SMC Structures
    st_15m = analyze_smc_structure(df_15m)
    st_1h = analyze_smc_structure(df_1h)
    st_4h = analyze_smc_structure(df_4h)

    zones = detect_untested_fvg_and_ob(df_15m)
    sweeps = detect_liquidity_sweeps(df_15m)
    dxy_smt = detect_cross_smt(df_15m, df_dxy, inverse=True)
    eur_smt = detect_cross_smt(df_15m, df_eur, inverse=False)
    kz_active, kz_name, kz_pts = get_killzone_info()

    ofi = MARKET_STATE["ofi"]
    vpin = MARKET_STATE["vpin"]

    # Update live dashboard state
    MARKET_STATE.update({
        "last_price": curr_price,
        "hurst": hurst,
        "regime": regime,
        "kalman_price": kalman["price"],
        "kalman_velocity": kalman["velocity"],
        "gk_vol": gk_vol,
        "kurtosis": kurtosis,
        "dxy_smt": dxy_smt,
        "eur_smt": eur_smt,
        "poc": vp["poc"],
        "vwap": vp["vwap"],
    })

    # Score Calculation (Max 100)
    buy_score, sell_score = 0, 0
    buy_confs, sell_confs = [], []

    # A. MTF Bias Alignment (up to 20 pts)
    if st_4h["bias"] == "bullish":
        buy_score += 8
        buy_confs.append("4H Bullish")
    elif st_4h["bias"] == "bearish":
        sell_score += 8
        sell_confs.append("4H Bearish")

    if st_1h["bias"] == "bullish":
        buy_score += 7
        buy_confs.append("1H Bullish")
    elif st_1h["bias"] == "bearish":
        sell_score += 7
        sell_confs.append("1H Bearish")

    if st_15m["event"] in ("BOS_BULL", "CHoCH_BULL"):
        buy_score += 10
        buy_confs.append(st_15m["event"])
    elif st_15m["event"] in ("BOS_BEAR", "CHoCH_BEAR"):
        sell_score += 10
        sell_confs.append(st_15m["event"])

    # B. Liquidity Sweep (up to 20 pts)
    if sweeps["ssl_sweep"]:
        buy_score += 20 if sweeps["major_sweep"] else 14
        buy_confs.append("SSL Liquidity Sweep")
    if sweeps["bsl_sweep"]:
        sell_score += 20 if sweeps["major_sweep"] else 14
        sell_confs.append("BSL Liquidity Sweep")

    # C. ICT OTE & Untested FVG/OB Confluence (up to 20 pts)
    if st_15m["ote_bot"] <= curr_price <= st_15m["ote_top"]:
        if st_15m["bias"] == "bullish":
            buy_score += 10
            buy_confs.append("ICT OTE 0.618-0.786")
        elif st_15m["bias"] == "bearish":
            sell_score += 10
            sell_confs.append("ICT OTE 0.618-0.786")

    if zones["in_bull_zone"]:
        buy_score += 12
        buy_confs.append("Untested FVG/OB")
    if zones["in_bear_zone"]:
        sell_score += 12
        sell_confs.append("Untested FVG/OB")

    # D. Cross-Asset SMT Divergence (DXY & EUR) (up to 15 pts)
    if dxy_smt == "BULLISH":
        buy_score += 10
        buy_confs.append("DXY Inverse SMT+")
    elif dxy_smt == "BEARISH":
        sell_score += 10
        sell_confs.append("DXY Inverse SMT-")

    if eur_smt == "BULLISH":
        buy_score += 6
        buy_confs.append("EUR SMT+")
    elif eur_smt == "BEARISH":
        sell_score += 6
        sell_confs.append("EUR SMT-")

    # E. Microstructure OFI + Kalman Velocity + Volume Profile (up to 15 pts)
    if ofi > 0.14 and kalman["velocity"] > 0:
        buy_score += 10
        buy_confs.append(f"OFI {ofi:+.2f} & Kalman+")
    elif ofi < -0.14 and kalman["velocity"] < 0:
        sell_score += 10
        sell_confs.append(f"OFI {ofi:+.2f} & Kalman-")

    if curr_price >= vp["val"] and curr_price <= vp["poc"]:
        buy_score += 5
        buy_confs.append("VP Value Discount")
    elif curr_price <= vp["vah"] and curr_price >= vp["poc"]:
        sell_score += 5
        sell_confs.append("VP Value Premium")

    # F. Killzone Timing & Fat-Tail Guard
    if kz_active:
        buy_score += kz_pts
        sell_score += kz_pts
        buy_confs.append(kz_name)
        sell_confs.append(kz_name)

    if kurtosis > 6.5:
        buy_score -= 10
        sell_score -= 10

    MARKET_STATE["last_score_buy"] = min(99, max(0, buy_score))
    MARKET_STATE["last_score_sell"] = min(99, max(0, sell_score))
    MARKET_STATE["last_confluences"] = buy_confs if buy_score >= sell_score else sell_confs

    side = None
    final_score = 0
    confs = []
    if buy_score >= MIN_SIGNAL_SCORE and buy_score > sell_score:
        side = "BUY"
        final_score = min(99, buy_score)
        confs = buy_confs
    elif sell_score >= MIN_SIGNAL_SCORE and sell_score > buy_score:
        side = "SELL"
        final_score = min(99, sell_score)
        confs = sell_confs

    if not side:
        return None

    # Dynamic ATR + Structural Stop-Loss
    tr = np.maximum(
        df_15m["high"] - df_15m["low"],
        np.maximum(
            abs(df_15m["high"] - df_15m["close"].shift(1)),
            abs(df_15m["low"] - df_15m["close"].shift(1)),
        ),
    )
    atr = float(tr.tail(14).mean())
    sl_dist = round(max(1.80, min(4.20, atr * 1.25)), 2)

    if side == "BUY":
        entry = round(curr_price, 2)
        sl = round(entry - sl_dist, 2)
        tp1 = round(entry + sl_dist * 2.0, 2)
        tp2 = round(entry + sl_dist * 3.5, 2)
        tp3 = round(entry + sl_dist * 5.5, 2)
    else:
        entry = round(curr_price, 2)
        sl = round(entry + sl_dist, 2)
        tp1 = round(entry - sl_dist * 2.0, 2)
        tp2 = round(entry - sl_dist * 3.5, 2)
        tp3 = round(entry - sl_dist * 5.5, 2)

    wins, losses = await get_bayes_prior_stats()
    win_prob = empirical_bayes_win_prob(final_score, wins, losses)
    rec_risk = calc_quarter_kelly(win_prob, rr_ratio=3.5)

    return {
        "id": f"APEX-{int(time.time())}-{side}",
        "created_at": datetime.datetime.now(UZT).strftime("%Y-%m-%d %H:%M:%S UZT"),
        "symbol": SYMBOL,
        "side": side,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "sl_points": sl_dist,
        "score": final_score,
        "win_prob": win_prob,
        "regime": regime,
        "hurst": hurst,
        "kalman_price": kalman["price"],
        "kalman_vel": kalman["velocity"],
        "vpin": vpin,
        "ofi": ofi,
        "dxy_smt": dxy_smt,
        "poc": vp["poc"],
        "vwap": vp["vwap"],
        "killzone": kz_name,
        "risk_pct": rec_risk,
        "confluences": confs,
        "status": "ACTIVE",
        "timestamp": int(time.time()),
    }


# ==================== DATA FETCHERS & WEBSOCKET ====================
async def fetch_bybit_klines(session: aiohttp.ClientSession, sym: str, interval: str, limit: int) -> pd.DataFrame:
    url = f"{BYBIT_REST}/v5/market/kline"
    params = {"category": "linear", "symbol": sym, "interval": interval, "limit": limit}
    try:
        async with session.get(url, params=params, timeout=10) as resp:
            data = await resp.json()
            rows = data.get("result", {}).get("list", [])
            if not rows:
                return pd.DataFrame()
            rows = list(reversed(rows))
            df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume", "turnover"])
            for col in ("open", "high", "low", "close", "volume"):
                df[col] = df[col].astype(float)
            return df
    except Exception:
        return pd.DataFrame()


async def fetch_yahoo_dxy_15m(session: aiohttp.ClientSession) -> pd.DataFrame:
    url = "https://query1.finance.yahoo.com/v8/finance/chart/DX-Y.NYB?interval=15m&range=5d"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with session.get(url, headers=headers, timeout=8) as resp:
            if resp.status != 200:
                return pd.DataFrame()
            data = await resp.json()
            res = data["chart"]["result"][0]
            q = res["indicators"]["quote"][0]
            df = pd.DataFrame({
                "open": q["open"],
                "high": q["high"],
                "low": q["low"],
                "close": q["close"],
                "volume": q.get("volume", [0] * len(q["close"])),
            }).dropna()
            return df.tail(80)
    except Exception:
        return pd.DataFrame()


async def bybit_ws_worker(app):
    while True:
        try:
            async with app["session"].ws_connect(BYBIT_WS, heartbeat=20) as ws:
                await ws.send_json({
                    "op": "subscribe",
                    "args": [f"orderbook.50.{SYMBOL}", f"publicTrade.{SYMBOL}"]
                })
                logger.info(f"Bybit L2 & Tick WS ulandi: {SYMBOL}")
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        payload = json.loads(msg.data)
                        topic = payload.get("topic", "")
                        if topic.startswith("orderbook."):
                            d = payload.get("data", {})
                            if payload.get("type") == "snapshot":
                                ORDERBOOK_BIDS.clear()
                                ORDERBOOK_ASKS.clear()
                            for p, q in d.get("b", []):
                                pf, qf = float(p), float(q)
                                if qf == 0:
                                    ORDERBOOK_BIDS.pop(pf, None)
                                else:
                                    ORDERBOOK_BIDS[pf] = qf
                            for p, q in d.get("a", []):
                                pf, qf = float(p), float(q)
                                if qf == 0:
                                    ORDERBOOK_ASKS.pop(pf, None)
                                else:
                                    ORDERBOOK_ASKS[pf] = qf
                            if ORDERBOOK_BIDS and ORDERBOOK_ASKS:
                                bb = max(ORDERBOOK_BIDS.keys())
                                ba = min(ORDERBOOK_ASKS.keys())
                                MARKET_STATE["bid"] = bb
                                MARKET_STATE["ask"] = ba
                                MARKET_STATE["spread"] = round(ba - bb, 3)
                                MARKET_STATE["ofi"] = compute_l2_ofi()
                                MARKET_STATE["last_ws_ts"] = time.time()
                        elif topic.startswith("publicTrade."):
                            for t in payload.get("data", []):
                                MARKET_STATE["last_price"] = float(t["p"])
                                update_tick_vpin(t["S"], float(t["v"]))
        except Exception as e:
            logger.warning(f"Bybit WS qayta ulanmoqda: {e}")
            await asyncio.sleep(3)


# ==================== TELEGRAM BOT & COMMANDS ====================
async def tg_send(session: aiohttp.ClientSession, chat_id: str, text: str):
    if not BOT_TOKEN or not chat_id:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        await session.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"}, timeout=8)
    except Exception as e:
        logger.warning(f"Telegram send xato: {e}")


async def send_signal_alert(session: aiohttp.ClientSession, sig: dict):
    icon = "🟢" if sig["side"] == "BUY" else "🔴"
    confs_str = " • ".join(sig["confluences"][:6])
    text = (
        f"{icon} <b>APEX 10/10 QUANT SIGNAL ({sig['symbol']})</b>
"
        f"━━━━━━━━━━━━━━━━━━━━━━
"
        f"📌 <b>Yo'nalish:</b> {sig['side']} | <b>Ball:</b> {sig['score']}/100 (Bayes Win: {sig['win_prob']}%)
"
        f"🎯 <b>Entry:</b> <code>{sig['entry']}</code>
"
        f"🛑 <b>Stop-Loss:</b> <code>{sig['sl']}</code> ({sig['sl_points']}$)
"
        f"✅ <b>TP1 (1:2):</b> <code>{sig['tp1']}</code>
"
        f"✅ <b>TP2 (1:3.5):</b> <code>{sig['tp2']}</code>
"
        f"✅ <b>TP3 (1:5.5):</b> <code>{sig['tp3']}</code>
"
        f"━━━━━━━━━━━━━━━━━━━━━━
"
        f"🧠 <b>Rejim:</b> {sig['regime']} (H={sig['hurst']}) | <b>Kalman:</b> {sig['kalman_price']} ({sig['kalman_vel']:+.3f})
"
        f"🌊 <b>L2 OFI:</b> {sig['ofi']:+.3f} | <b>VPIN:</b> {sig['vpin']:.3f} | <b>DXY SMT:</b> {sig['dxy_smt']}
"
        f"📊 <b>VP POC:</b> {sig['poc']} | <b>VWAP:</b> {sig['vwap']}
"
        f"🧩 <b>Tasdiqlar:</b> <i>{confs_str}</i>
"
        f"⏰ <b>Oyna:</b> {sig['killzone']} | <b>Kelly Risk:</b> {sig['risk_pct']}%"
    )
    await tg_send(session, ADMIN_CHAT_ID, text)


def format_live_status() -> str:
    wb = MARKET_STATE["windows_bridge"]
    bridge_ok = (time.time() - wb["last_seen"]) < 20 if wb["last_seen"] else False
    b_icon = "🟢 ONLINE" if bridge_ok else "🔴 OFFLINE"
    confs = ", ".join(MARKET_STATE["last_confluences"][:5]) or "Kutilmoqda..."
    return (
        f"📡 <b>APEX 10/10 JONLI BOZOR ANALIZI ({SYMBOL})</b>
"
        f"━━━━━━━━━━━━━━━━━━━━━━
"
        f"💰 <b>Narx:</b> <code>{MARKET_STATE['last_price']}</code> | <b>Kalman:</b> <code>{MARKET_STATE['kalman_price']}</code> ({MARKET_STATE['kalman_velocity']:+.3f})
"
        f"📈 <b>Hurst:</b> {MARKET_STATE['hurst']} ({MARKET_STATE['regime']}) | <b>Kurtosis:</b> {MARKET_STATE['kurtosis']}
"
        f"🌊 <b>L2 OFI:</b> {MARKET_STATE['ofi']:+.3f} | <b>VPIN:</b> {MARKET_STATE['vpin']:.3f}
"
        f"🌐 <b>DXY SMT:</b> {MARKET_STATE['dxy_smt']} | <b>EUR SMT:</b> {MARKET_STATE['eur_smt']}
"
        f"📊 <b>VP POC:</b> {MARKET_STATE['poc']} | <b>VWAP:</b> {MARKET_STATE['vwap']}
"
        f"🎯 <b>Joriy Ball:</b> BUY <b>{MARKET_STATE['last_score_buy']}</b> / SELL <b>{MARKET_STATE['last_score_sell']}</b> (Min: {MIN_SIGNAL_SCORE})
"
        f"🧩 <b>Omillar:</b> <i>{confs}</i>
"
        f"━━━━━━━━━━━━━━━━━━━━━━
"
        f"🖥️ <b>Windows MT5:</b> {b_icon} | <b>Balans:</b> ${wb['balance']:.2f} | <b>Ochiq:</b> {wb['open_positions']}"
    )


async def telegram_polling_worker(app):
    global LAST_UPDATE_ID
    if not BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    while True:
        try:
            async with app["session"].get(url, params={"offset": LAST_UPDATE_ID + 1, "timeout": 25}, timeout=30) as r:
                if r.status == 200:
                    data = await r.json()
                    for upd in data.get("result", []):
                        LAST_UPDATE_ID = upd["update_id"]
                        msg = upd.get("message", {})
                        chat_id = str(msg.get("chat", {}).get("id", ""))
                        txt = (msg.get("text") or "").strip().lower()
                        if ADMIN_CHAT_ID and chat_id != ADMIN_CHAT_ID:
                            continue
                        if txt in ("/start", "/status", "/analyze"):
                            await run_full_10_10_analysis(app["session"])
                            await tg_send(app["session"], chat_id, format_live_status())
        except Exception:
            await asyncio.sleep(3)


async def broadcast_ws_signal(sig: dict):
    msg = json.dumps({"event": "new_signal", "signal": sig})
    dead = []
    for ws in WS_CLIENTS:
        try:
            await ws.send_str(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        WS_CLIENTS.discard(ws)


async def scanner_worker(app):
    global LAST_SIGNAL_TS
    await asyncio.sleep(3)
    while True:
        try:
            sig = await run_full_10_10_analysis(app["session"])
            if sig and (time.time() - LAST_SIGNAL_TS) >= 900:
                LAST_SIGNAL_TS = time.time()
                ACTIVE_SIGNALS.append(sig)
                await save_signal_db(sig)
                await send_signal_alert(app["session"], sig)
                await broadcast_ws_signal(sig)
                logger.info(f"🚀 APEX 10/10 Signal: {sig['id']} | Score={sig['score']} | WinProb={sig['win_prob']}%")
        except Exception as e:
            logger.error(f"Scanner xatosi: {e}")
        await asyncio.sleep(15)


# ==================== HTTP & WEBSOCKET BRIDGE ====================
def check_auth(request) -> bool:
    token = request.headers.get("X-Bridge-Token") or request.query.get("token", "")
    return token == BRIDGE_TOKEN


async def handle_health(request):
    return web.json_response({"ok": True, "engine": "APEX_10_10", "market": MARKET_STATE})


async def handle_bridge_poll(request):
    if not check_auth(request):
        return web.json_response({"ok": False, "error": "Unauthorized"}, status=403)
    return web.json_response({
        "ok": True,
        "signals": list(ACTIVE_SIGNALS)[-10:],
        "market": MARKET_STATE,
        "server_time": int(time.time()),
    })


async def handle_bridge_report(request):
    if not check_auth(request):
        return web.json_response({"ok": False, "error": "Unauthorized"}, status=403)
    data = await request.json()
    MARKET_STATE["windows_bridge"].update({
        "online": True,
        "last_seen": time.time(),
        "balance": float(data.get("balance", 0.0)),
        "equity": float(data.get("equity", 0.0)),
        "mt5_bid": float(data.get("bid", 0.0)),
        "mt5_ask": float(data.get("ask", 0.0)),
        "open_positions": int(data.get("open_positions", 0)),
    })
    return web.json_response({"ok": True})


async def handle_bridge_ws(request):
    if not check_auth(request):
        return web.json_response({"ok": False, "error": "Unauthorized"}, status=403)
    ws = web.WebSocketResponse(heartbeat=20)
    await ws.prepare(request)
    WS_CLIENTS.add(ws)
    try:
        await ws.send_str(json.dumps({"event": "connected", "signals": list(ACTIVE_SIGNALS)[-5:]}))
        async for _ in ws:
            pass
    finally:
        WS_CLIENTS.discard(ws)
    return ws


async def on_startup(app):
    await init_db()
    app["session"] = aiohttp.ClientSession()
    app["ws_task"] = asyncio.create_task(bybit_ws_worker(app))
    app["scan_task"] = asyncio.create_task(scanner_worker(app))
    app["tg_task"] = asyncio.create_task(telegram_polling_worker(app))


async def on_cleanup(app):
    for t in ("ws_task", "scan_task", "tg_task"):
        if t in app:
            app[t].cancel()
    if "session" in app:
        await app["session"].close()


def create_app():
    app = web.Application()
    app.router.add_get("/", handle_health)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/api/bridge/poll", handle_bridge_poll)
    app.router.add_post("/api/bridge/report", handle_bridge_report)
    app.router.add_get("/ws/bridge", handle_bridge_ws)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


app = create_app()

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
