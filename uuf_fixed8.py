"""
INSTITUTIONAL QUANT & SMC/ICT ANALYZER SERVER — v25 UNIFIED APEX EDITION
=========================================================================
Eng kuchli tahlil modullarining birlashtirilgan, toza va tezkor yakuniy versiyasi:
  1. Microstructure: Bybit L2 Orderbook (50-depth) OFI + Candle OFI + Tick VPIN + Live Spread Guard
  2. Quant Filters: López de Prado CUSUM Filter, 1D Kalman State-Space Filter, Hurst (R/S) Regime,
     Garman-Klass OHLC Volatility, Taleb Fat-Tail Kurtosis & Black-Swan Jump Guard
  3. MTF SMC/ICT: 1D / 4H / 1H / M45 / M30 / 15m / 5m Structure (BOS/CHoCH), 5m LTF Trigger,
     ICT OTE (0.618-0.786), Untested FVG, Inversion FVG (IFVG), Body-Only OB & Breaker Blocks
  4. Liquidity & Sessions: PDH/PDL Sweep, Asia Range Sweep, EQH/EQL, NY Midnight Open, Killzones & Silver Bullet
  5. Macro & Cross-Asset SMT: DXY (DX-Y.NYB) + US10Y (^TNX) + EURUSDT + BTCUSDT SMT + RSI Divergence
  6. Volume & News: Volume Profile (POC/VAH/VAL), Intraday VWAP, ForexFactory High-Impact USD News Filter
  7. Risk & Learning: Empirical Bayes Win-Probability (SQLite WIN/LOSS Auto-Tracker), Quarter-Kelly Sizing,
     Daily Drawdown Circuit Breaker (-3.0R)
  8. Delivery: Telegram Interactive Bot (/status, /analyze, /stats, /news) + WS/REST MT5 Bridge
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
    format="%(asctime)s [APEX-v25] %(message)s",
)
logger = logging.getLogger("apex_quant_v25")

# ==================== KONFIGURATSIYA ====================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "").strip()
BRIDGE_TOKEN = os.getenv("BRIDGE_TOKEN", "ApexQuant2026Secret").strip()
SYMBOL = os.getenv("SYMBOL", "XAUUSDT").strip()
BTC_SYMBOL = os.getenv("BTC_SYMBOL", "BTCUSDT").strip()
EUR_SYMBOL = os.getenv("EUR_SYMBOL", "EURUSDT").strip()

MIN_SIGNAL_SCORE = int(os.getenv("MIN_SIGNAL_SCORE", "78"))
RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "1.5"))
DAILY_MAX_LOSS_R = float(os.getenv("DAILY_MAX_LOSS_R", "3.0"))
MAX_SPREAD_PTS = float(os.getenv("MAX_SPREAD_PTS", "0.70"))
SIGNAL_COOLDOWN_SEC = int(os.getenv("SIGNAL_COOLDOWN_SEC", "900"))

BYBIT_REST = os.getenv("BYBIT_REST_URL", "https://api.bybit.com").rstrip("/")
BYBIT_WS = os.getenv("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/linear")
NEWS_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
DB_PATH = os.getenv("DB_PATH", "apex_quant_v25.sqlite")

# Live Telemetry State
MARKET_STATE = {
    "symbol": SYMBOL,
    "bid": 0.0,
    "ask": 0.0,
    "last_price": 0.0,
    "spread": 0.0,
    "ofi": 0.0,
    "l2_ofi": 0.0,
    "vpin": 0.25,
    "hurst": 0.50,
    "regime": "NEUTRAL",
    "kalman_price": 0.0,
    "kalman_velocity": 0.0,
    "gk_vol": 0.0,
    "kurtosis": 3.0,
    "cusum_dir": "NONE",
    "dxy_smt": "NONE",
    "eur_smt": "NONE",
    "btc_smt": "NONE",
    "us10y_bias": "neutral",
    "poc": 0.0,
    "vwap": 0.0,
    "news_blocked": False,
    "news_title": None,
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

ORDERBOOK_BIDS: dict[float, float] = {}
ORDERBOOK_ASKS: dict[float, float] = {}
TRADE_BUCKETS: deque = deque(maxlen=45)
CURRENT_BUCKET = {"buy": 0.0, "sell": 0.0, "vol": 0.0}
BUCKET_TARGET_VOL = 35.0

ACTIVE_SIGNALS: deque = deque(maxlen=50)
WS_CLIENTS: set = set()
LAST_SIGNAL_TS = 0.0
LAST_UPDATE_ID = 0

_NEWS_CACHE: dict = {"events": [], "ts": 0.0}
_MACRO_CACHE: dict = {"ts": 0.0, "df_dxy": pd.DataFrame(), "df_tnx": pd.DataFrame()}


# ==================== 1. DATABASE & AUTO-OUTCOME TRACKER ====================
async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id TEXT PRIMARY KEY,
                created_at TEXT,
                date_uzt TEXT,
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
    date_uzt = datetime.datetime.now(UZT).strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT OR REPLACE INTO signals
            (id, created_at, date_uzt, symbol, side, entry, sl, tp1, tp2, tp3,
             score, win_prob, regime, hurst, vpin, ofi, confluences, status, pnl_r)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            sig["id"], sig["created_at"], date_uzt, sig["symbol"], sig["side"],
            sig["entry"], sig["sl"], sig["tp1"], sig["tp2"], sig["tp3"],
            sig["score"], sig["win_prob"], sig["regime"], sig["hurst"],
            sig["vpin"], sig["ofi"], json.dumps(sig["confluences"]),
            sig["status"], 0.0
        ))
        await db.commit()


async def update_open_signals_status(curr_price: float):
    """Jonli narx asosida ACTIVE signallarning TP/SL natijasini avtomatik yopadi (Bayes uchun)."""
    if curr_price <= 0:
        return
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT id, side, entry, sl, tp1, tp2, tp3 FROM signals WHERE status = 'ACTIVE'"
            ) as cur:
                rows = await cur.fetchall()
            for row_id, side, entry, sl, tp1, tp2, tp3 in rows:
                new_status = None
                pnl_r = 0.0
                if side == "BUY":
                    if curr_price <= sl:
                        new_status, pnl_r = "LOSS", -1.0
                    elif curr_price >= tp3:
                        new_status, pnl_r = "WIN", 5.5
                    elif curr_price >= tp2:
                        new_status, pnl_r = "WIN", 3.5
                    elif curr_price >= tp1:
                        new_status, pnl_r = "WIN", 2.0
                elif side == "SELL":
                    if curr_price >= sl:
                        new_status, pnl_r = "LOSS", -1.0
                    elif curr_price <= tp3:
                        new_status, pnl_r = "WIN", 5.5
                    elif curr_price <= tp2:
                        new_status, pnl_r = "WIN", 3.5
                    elif curr_price <= tp1:
                        new_status, pnl_r = "WIN", 2.0
                if new_status:
                    await db.execute(
                        "UPDATE signals SET status = ?, pnl_r = ? WHERE id = ?",
                        (new_status, pnl_r, row_id),
                    )
            await db.commit()
    except Exception as e:
        logger.warning(f"Outcome tracker xatosi: {e}")


async def get_bayes_and_daily_stats() -> dict:
    """Empirical Bayes tarixiy statistikasi va kunlik Circuit Breaker holati."""
    today = datetime.datetime.now(UZT).strftime("%Y-%m-%d")
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT status, pnl_r, date_uzt FROM signals") as cur:
                rows = await cur.fetchall()
            wins = sum(1 for s, _, _ in rows if s == "WIN")
            losses = sum(1 for s, _, _ in rows if s == "LOSS")
            daily_r = sum(float(r or 0.0) for s, r, d in rows if d == today and s in ("WIN", "LOSS"))
            return {
                "wins": wins,
                "losses": losses,
                "daily_r": round(daily_r, 2),
                "circuit_broken": daily_r <= -abs(DAILY_MAX_LOSS_R),
            }
    except Exception:
        return {"wins": 0, "losses": 0, "daily_r": 0.0, "circuit_broken": False}


# ==================== 2. QUANTITATIVE MATH MODELS ====================
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
        tau.append(max(float(np.std(diffs)), 1e-9))
    poly = np.polyfit(np.log(list(lags)), np.log(tau), 1)
    h = max(0.05, min(0.95, round(float(poly[0] * 2.0), 3)))
    regime = "TREND" if h > 0.55 else ("MEAN_REVERSION" if h < 0.45 else "RANDOM_WALK")
    return h, regime


def calc_garman_klass_and_tail_guard(df: pd.DataFrame) -> dict:
    """Garman-Klass OHLC Volatility + Taleb Fat-Tail Kurtosis & Jump Risk Guard."""
    log_hl = np.log(df["high"] / df["low"]) ** 2
    log_co = np.log(df["close"] / df["open"]) ** 2
    gk = 0.5 * log_hl - (2.0 * math.log(2.0) - 1.0) * log_co
    gk_vol = round(float(np.sqrt(gk.tail(30).mean()) * 100.0), 4)

    closes = df["close"].values[-60:]
    rets = np.diff(closes)
    std = float(np.std(rets))
    if std < 1e-9:
        return {"gk_vol": gk_vol, "kurtosis": 3.0, "is_jump": False, "safe": True}
    z = (rets - np.mean(rets)) / std
    kurt = round(float(np.mean(z ** 4)), 2)

    last_move = abs(float(df["close"].iloc[-1]) - float(df["open"].iloc[-1]))
    is_jump = (last_move >= std * 3.4) and (last_move >= 5.0)
    safe = not ((kurt >= 12.0 and is_jump) or kurt > 8.5)
    return {"gk_vol": gk_vol, "kurtosis": kurt, "is_jump": is_jump, "safe": safe}


def calc_cusum_filter(df: pd.DataFrame, atr: float, threshold_mult: float = 1.10) -> dict:
    """Marcos López de Prado: Symmetric CUSUM Event-Driven Filter (flet shovqinini kesadi)."""
    if len(df) < 20 or atr <= 0:
        return {"event": False, "direction": "NONE"}
    threshold = atr * threshold_mult
    closes = df["close"].values[-16:]
    s_pos, s_neg = 0.0, 0.0
    for i in range(1, len(closes)):
        diff = float(closes[i] - closes[i - 1])
        s_pos = max(0.0, s_pos + diff)
        s_neg = min(0.0, s_neg + diff)
    if s_pos >= threshold:
        return {"event": True, "direction": "BULLISH", "val": round(s_pos, 2)}
    if abs(s_neg) >= threshold:
        return {"event": True, "direction": "BEARISH", "val": round(s_neg, 2)}
    return {"event": False, "direction": "NONE", "val": 0.0}


def empirical_bayes_win_prob(score: int, wins: int, losses: int) -> float:
    """Laplace-smoothed Log-Odds Posterior Win Probability."""
    alpha_0, beta_0 = 5.8, 4.2
    base_prob = (alpha_0 + wins) / (alpha_0 + beta_0 + wins + losses)
    score_boost = (score - 75) * 0.0085
    posterior = max(0.45, min(0.88, base_prob + score_boost))
    return round(posterior * 100.0, 1)


def calc_quarter_kelly(win_prob_pct: float, rr_ratio: float = 3.5) -> float:
    """Quarter-Kelly Optimal Capital Allocation."""
    p = max(0.05, min(0.95, win_prob_pct / 100.0))
    q = 1.0 - p
    b = max(1.0, rr_ratio)
    f_star = (p * b - q) / b
    safe_pct = max(0.25, min(RISK_PER_TRADE_PCT, f_star * 0.25 * 100.0))
    return round(safe_pct, 2)


# ==================== 3. MICROSTRUCTURE (L2 OFI, CANDLE OFI, VPIN, SPREAD) ====================
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


def compute_hybrid_ofi(df: pd.DataFrame, lookback: int = 12) -> float:
    """Candle-level OFI + Real-Time 50-depth L2 OrderBook Imbalance uyg'unligi."""
    win = df.tail(lookback)
    rng = np.maximum(win["high"] - win["low"], 0.01)
    buy_w = np.maximum(0.0, (win["close"] - win["low"]) / rng)
    sell_w = np.maximum(0.0, (win["high"] - win["close"]) / rng)
    bv = float((win["volume"] * buy_w).sum())
    sv = float((win["volume"] * sell_w).sum())
    candle_ofi = (bv - sv) / max(bv + sv, 1e-6)

    l2_fresh = (time.time() - MARKET_STATE["last_ws_ts"]) <= 15.0
    if l2_fresh:
        return round(0.55 * MARKET_STATE["l2_ofi"] + 0.45 * candle_ofi, 3)
    return round(candle_ofi, 3)


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


def check_live_spread_guard() -> tuple[bool, float]:
    wb = MARKET_STATE["windows_bridge"]
    if wb["last_seen"] and (time.time() - wb["last_seen"]) <= 30:
        if wb["mt5_ask"] > wb["mt5_bid"] > 0:
            sp = round(wb["mt5_ask"] - wb["mt5_bid"], 3)
            return sp <= MAX_SPREAD_PTS, sp
    if (time.time() - MARKET_STATE["last_ws_ts"]) <= 15:
        sp = MARKET_STATE["spread"]
        return sp <= MAX_SPREAD_PTS, sp
    return True, 0.25


# ==================== 4. VOLUME PROFILE, VWAP & RSI DIVERGENCE ====================
def compute_volume_profile_and_vwap(df: pd.DataFrame, bins: int = 24) -> dict:
    window = df.tail(96)
    pmin, pmax = float(window["low"].min()), float(window["high"].max())
    if pmax <= pmin:
        mid = float(window["close"].iloc[-1])
        return {"poc": mid, "vah": mid, "val": mid, "vwap": mid}

    bsize = (pmax - pmin) / bins
    hist = np.zeros(bins)
    for _, row in window.iterrows():
        idx = max(0, min(bins - 1, int((float(row["close"]) - pmin) / bsize)))
        hist[idx] += float(row["volume"])

    poc_idx = int(np.argmax(hist))
    poc = pmin + (poc_idx + 0.5) * bsize
    target_vol = float(np.sum(hist)) * 0.70
    acc = hist[poc_idx]
    low_i, high_i = poc_idx, poc_idx
    while acc < target_vol and (low_i > 0 or high_i < bins - 1):
        lv = hist[low_i - 1] if low_i > 0 else -1.0
        rv = hist[high_i + 1] if high_i < bins - 1 else -1.0
        if rv >= lv and high_i < bins - 1:
            high_i += 1
            acc += rv
        elif low_i > 0:
            low_i -= 1
            acc += lv
        else:
            break
    val, vah = pmin + low_i * bsize, pmin + (high_i + 1) * bsize
    typical = (window["high"] + window["low"] + window["close"]) / 3.0
    vsum = float(window["volume"].sum())
    vwap = float((typical * window["volume"]).sum() / vsum) if vsum > 0 else poc
    return {"poc": round(poc, 2), "vah": round(vah, 2), "val": round(val, 2), "vwap": round(vwap, 2)}


def detect_rsi_divergence(df: pd.DataFrame, period: int = 14) -> tuple[bool, bool]:
    if len(df) < 35:
        return False, False
    delta = df["close"].diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean().replace(0, 1e-9)
    rsi = 100.0 - (100.0 / (1.0 + gain / loss))
    win = df.tail(24)
    rwin = rsi.tail(24)
    mid = 12
    p_low1, p_low2 = float(win["low"].iloc[:mid].min()), float(win["low"].iloc[mid:].min())
    r_low1, r_low2 = float(rwin.iloc[:mid].min()), float(rwin.iloc[mid:].min())
    p_high1, p_high2 = float(win["high"].iloc[:mid].max()), float(win["high"].iloc[mid:].max())
    r_high1, r_high2 = float(rwin.iloc[:mid].max()), float(rwin.iloc[mid:].max())
    bull_div = (p_low2 < p_low1) and (r_low2 > r_low1 + 2.0)
    bear_div = (p_high2 > p_high1) and (r_high2 < r_high1 - 2.0)
    return bull_div, bear_div


# ==================== 5. ADVANCED SMC / ICT ENGINE ====================
def resample_15m_df(df_15m: pd.DataFrame, group_bars: int) -> pd.DataFrame:
    """15m shamlardan M30 (group=2) yoki M45 (group=3) hosil qiladi."""
    if len(df_15m) < group_bars * 10:
        return pd.DataFrame()
    n = (len(df_15m) // group_bars) * group_bars
    sub = df_15m.iloc[-n:].reset_index(drop=True)
    groups = sub.index // group_bars
    return sub.groupby(groups).agg({
        "ts": "first", "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum"
    }).reset_index(drop=True)


def get_swings(df: pd.DataFrame, left: int = 3, right: int = 3):
    highs, lows = [], []
    h_arr, l_arr = df["high"].values, df["low"].values
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
    bias, event = "neutral", "NONE"
    if curr > last_h and last_l > prev_l:
        bias, event = "bullish", "BOS_BULL"
    elif curr > last_h and last_l < prev_l:
        bias, event = "bullish", "CHoCH_BULL"
    elif curr < last_l and last_h < prev_h:
        bias, event = "bearish", "BOS_BEAR"
    elif curr < last_l and last_h > prev_h:
        bias, event = "bearish", "CHoCH_BEAR"
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
        "bias": bias, "event": event, "eq": round(eq, 2),
        "ote_bot": round(min(ote_bot, ote_top), 2),
        "ote_top": round(max(ote_bot, ote_top), 2),
    }


def detect_smc_zones_and_ifvg(df: pd.DataFrame) -> dict:
    """Untested FVG, Inversion FVG (IFVG), Body-Only Order Blocks & Breaker Blocks."""
    curr = float(df["close"].iloc[-1])
    bull_zones, bear_zones = [], []

    # 1. Untested FVG & Inversion FVG (IFVG)
    for i in range(max(2, len(df) - 30), len(df) - 1):
        c1_h, c1_l = float(df["high"].iloc[i - 2]), float(df["low"].iloc[i - 2])
        c3_h, c3_l = float(df["high"].iloc[i]), float(df["low"].iloc[i])
        sub_low = float(df["low"].iloc[i + 1:].min())
        sub_high = float(df["high"].iloc[i + 1:].max())

        if c3_l > c1_h:
            if sub_low > c1_h:
                bull_zones.append(("FVG", c1_h, c3_l))
            elif curr < c1_h:  # Buzilgan Bull FVG -> Bearish IFVG
                bear_zones.append(("IFVG", c1_h, c3_l))
        if c3_h < c1_l:
            if sub_high < c1_l:
                bear_zones.append(("FVG", c3_h, c1_l))
            elif curr > c1_l:  # Buzilgan Bear FVG -> Bullish IFVG
                bull_zones.append(("IFVG", c3_h, c1_l))

    # 2. Body-Only Order Blocks & Breaker Blocks
    avg_body = float((df["close"] - df["open"]).abs().tail(30).mean())
    for i in range(max(1, len(df) - 25), len(df) - 2):
        o1, c1 = float(df["open"].iloc[i]), float(df["close"].iloc[i])
        o2, c2 = float(df["open"].iloc[i + 1]), float(df["close"].iloc[i + 1])
        if c1 < o1 and c2 > o2 and (c2 - o2) > avg_body * 1.4 and c2 > float(df["high"].iloc[i]):
            lo, hi = min(o1, c1), max(o1, c1)
            if curr >= lo - 0.8:
                bull_zones.append(("OB", lo, hi))
            elif curr < lo:
                bear_zones.append(("Breaker", lo, hi))
        if c1 > o1 and c2 < o2 and (o2 - c2) > avg_body * 1.4 and c2 < float(df["low"].iloc[i]):
            lo, hi = min(o1, c1), max(o1, c1)
            if curr <= hi + 0.8:
                bear_zones.append(("OB", lo, hi))
            elif curr > hi:
                bull_zones.append(("Breaker", lo, hi))

    in_bull = [k for k, lo, hi in bull_zones if lo - 0.8 <= curr <= hi + 0.8]
    in_bear = [k for k, lo, hi in bear_zones if lo - 0.8 <= curr <= hi + 0.8]
    return {"bull_hits": list(set(in_bull)), "bear_hits": list(set(in_bear))}


def detect_bank_manipulation_x3(df: pd.DataFrame) -> dict:
    """M30/M45 Forming X3 + Negation (3 ta ketma-ket bir xil shamdan keyingi keskin yutish)."""
    if df.empty or len(df) < 10:
        return {"bull": False, "bear": False}
    last4 = df.tail(4)
    o = last4["open"].values
    c = last4["close"].values
    # 3 ta qizil shamdan keyin yashil yutish (Bullish Negation)
    bull_neg = (c[0] < o[0] and c[1] < o[1] and c[2] < o[2] and c[3] > o[3] and c[3] > o[2])
    # 3 ta yashil shamdan keyin qizil yutish (Bearish Negation)
    bear_neg = (c[0] > o[0] and c[1] > o[1] and c[2] > o[2] and c[3] < o[3] and c[3] < o[2])
    return {"bull": bool(bull_neg), "bear": bool(bear_neg)}


def detect_liquidity_matrix(df_15m: pd.DataFrame) -> dict:
    """PDH/PDL, Asia Range (00-06 UTC), EQH/EQL, Internal Sweeps & NY Midnight Open."""
    curr = float(df_15m["close"].iloc[-1])
    recent_h = float(df_15m["high"].tail(6).max())
    recent_l = float(df_15m["low"].tail(6).min())

    prior = df_15m.iloc[-96:-6]
    pdh, pdl = float(prior["high"].max()), float(prior["low"].min())
    bsl_major = recent_h > pdh and curr < pdh
    ssl_major = recent_l < pdl and curr > pdl

    mid_prior = df_15m.iloc[-36:-4]
    int_h, int_l = float(mid_prior["high"].max()), float(mid_prior["low"].min())
    bsl_int = recent_h > int_h and curr < int_h
    ssl_int = recent_l < int_l and curr > int_l

    # EQH / EQL (oxirgi 48 shamda 0.06% aniqlikda)
    highs, lows = get_swings(df_15m.tail(48), 2, 2)
    eqh = any(abs(highs[i][1] - highs[j][1]) / highs[i][1] < 0.0006
              for i in range(len(highs)) for j in range(i + 1, len(highs)))
    eql = any(abs(lows[i][1] - lows[j][1]) / lows[i][1] < 0.0006
              for i in range(len(lows)) for j in range(i + 1, len(lows)))

    # Asia & NY Midnight Open (ts ms dan UTC soat)
    asia_high, asia_low, ny_open = None, None, None
    if "ts" in df_15m.columns:
        ts_dt = pd.to_datetime(df_15m["ts"].astype(np.int64), unit="ms", utc=True)
        asia_mask = (ts_dt.dt.hour >= 0) & (ts_dt.dt.hour < 6)
        if asia_mask.any():
            asia_df = df_15m[asia_mask].tail(24)
            asia_high, asia_low = float(asia_df["high"].max()), float(asia_df["low"].min())
        ny_mask = (ts_dt.dt.hour == 5) & (ts_dt.dt.minute == 0)
        if ny_mask.any():
            ny_open = float(df_15m[ny_mask].iloc[-1]["open"])

    asia_bsl = bool(asia_high and recent_h > asia_high and curr < asia_high)
    asia_ssl = bool(asia_low and recent_l < asia_low and curr > asia_low)

    return {
        "pdh": round(pdh, 2), "pdl": round(pdl, 2),
        "bsl_sweep": bsl_major or bsl_int or asia_bsl,
        "ssl_sweep": ssl_major or ssl_int or asia_ssl,
        "major_sweep": bsl_major or ssl_major or asia_bsl or asia_ssl,
        "eqh": eqh, "eql": eql,
        "ny_open": round(ny_open, 2) if ny_open else None,
    }


def check_5m_ltf_trigger(df_5m: pd.DataFrame, side: str) -> tuple[bool, str]:
    """5m LTF kirish tasdig'i (CHoCH/BOS yoki 5m Engulfing/FVG)."""
    if len(df_5m) < 25:
        return False, "No Data"
    st5 = analyze_smc_structure(df_5m)
    o1, c1 = float(df_5m["open"].iloc[-2]), float(df_5m["close"].iloc[-2])
    o2, c2 = float(df_5m["open"].iloc[-1]), float(df_5m["close"].iloc[-1])

    if side == "BUY":
        if st5["event"] in ("BOS_BULL", "CHoCH_BULL"):
            return True, f"5m {st5['event']}"
        if c1 < o1 and c2 > o2 and c2 > o1 and (c2 - o2) > (o1 - c1):
            return True, "5m Bull Engulfing"
    else:
        if st5["event"] in ("BOS_BEAR", "CHoCH_BEAR"):
            return True, f"5m {st5['event']}"
        if c1 > o1 and c2 < o2 and c2 < o1 and (o2 - c2) > (c1 - o1):
            return True, "5m Bear Engulfing"
    return False, "Waiting LTF"


def detect_cross_smt(df_gold: pd.DataFrame, df_other: pd.DataFrame, inverse: bool = False) -> str:
    if df_gold.empty or df_other.empty or len(df_gold) < 30 or len(df_other) < 30:
        return "NONE"
    gh, gl = get_swings(df_gold.tail(45), 2, 2)
    oh, ol = get_swings(df_other.tail(45), 2, 2)
    if len(gl) < 2 or len(gh) < 2 or len(ol) < 2 or len(oh) < 2:
        return "NONE"
    if inverse:
        if gl[-1][1] < gl[-2][1] and oh[-1][1] < oh[-2][1]:
            return "BULLISH"
        if gh[-1][1] > gh[-2][1] and ol[-1][1] > ol[-2][1]:
            return "BEARISH"
    else:
        if gl[-1][1] < gl[-2][1] and ol[-1][1] > ol[-2][1]:
            return "BULLISH"
        if gh[-1][1] > gh[-2][1] and oh[-1][1] < oh[-2][1]:
            return "BEARISH"
    return "NONE"


def get_killzone_info() -> tuple[bool, str, int]:
    now_utc = datetime.datetime.now(datetime.timezone.utc).time()
    if datetime.time(7, 0) <= now_utc <= datetime.time(10, 0):
        return True, "London Killzone", 8
    if datetime.time(12, 0) <= now_utc <= datetime.time(16, 0):
        return True, "NY AM + Silver Bullet", 10
    if datetime.time(18, 0) <= now_utc <= datetime.time(20, 0):
        return True, "NY PM Killzone", 6
    return False, "Off-Killzone", 0


# ==================== 6. NEWS & EXTERNAL DATA FETCHERS ====================
async def check_news_window(session: aiohttp.ClientSession) -> tuple[bool, str | None]:
    now_ts = time.time()
    if now_ts - _NEWS_CACHE["ts"] > 3600:
        try:
            async with session.get(NEWS_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=8) as r:
                if r.status == 200:
                    data = await r.json(content_type=None)
                    evs = []
                    for item in data:
                        if item.get("country") == "USD" and item.get("impact") == "High" and item.get("date"):
                            dt = datetime.datetime.fromisoformat(
                                item["date"].replace("Z", "+00:00")
                            ).astimezone(datetime.timezone.utc)
                            evs.append({"title": item.get("title", "USD News"), "time": dt})
                    _NEWS_CACHE["events"] = evs
                    _NEWS_CACHE["ts"] = now_ts
        except Exception:
            _NEWS_CACHE["ts"] = now_ts - 3300

    now_dt = datetime.datetime.now(datetime.timezone.utc)
    for ev in _NEWS_CACHE["events"]:
        diff_min = (ev["time"] - now_dt).total_seconds() / 60.0
        if -30.0 <= diff_min <= 30.0:
            return True, f"{ev['title']} ({int(abs(diff_min))}m)"
    return False, None


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
            return df.iloc[:-1] if len(df) > 2 else df  # Faqat yopilgan shamlar
    except Exception:
        return pd.DataFrame()


async def fetch_yahoo_15m(session: aiohttp.ClientSession, ticker: str) -> pd.DataFrame:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=15m&range=5d"
    try:
        async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8) as resp:
            if resp.status != 200:
                return pd.DataFrame()
            data = await resp.json()
            q = data["chart"]["result"][0]["indicators"]["quote"][0]
            return pd.DataFrame({
                "open": q["open"], "high": q["high"], "low": q["low"],
                "close": q["close"], "volume": q.get("volume", [0] * len(q["close"])),
            }).dropna().tail(80)
    except Exception:
        return pd.DataFrame()


async def get_macro_data(session: aiohttp.ClientSession) -> tuple[pd.DataFrame, pd.DataFrame]:
    if time.time() - _MACRO_CACHE["ts"] > 120:
        dxy, tnx = await asyncio.gather(
            fetch_yahoo_15m(session, "DX-Y.NYB"),
            fetch_yahoo_15m(session, "^TNX"),
        )
        _MACRO_CACHE.update({"ts": time.time(), "df_dxy": dxy, "df_tnx": tnx})
    return _MACRO_CACHE["df_dxy"], _MACRO_CACHE["df_tnx"]


# ==================== 7. MASTER UNIFIED v25 ANALYZER ====================
async def run_unified_v25_analysis(session: aiohttp.ClientSession) -> dict | None:
    df_5m, df_15m, df_1h, df_4h, df_1d, df_eur, df_btc = await asyncio.gather(
        fetch_bybit_klines(session, SYMBOL, "5", 100),
        fetch_bybit_klines(session, SYMBOL, "15", 180),
        fetch_bybit_klines(session, SYMBOL, "60", 120),
        fetch_bybit_klines(session, SYMBOL, "240", 90),
        fetch_bybit_klines(session, SYMBOL, "D", 45),
        fetch_bybit_klines(session, EUR_SYMBOL, "15", 60),
        fetch_bybit_klines(session, BTC_SYMBOL, "15", 60),
    )
    if df_15m.empty or df_1h.empty or df_4h.empty or df_5m.empty:
        return None

    (df_dxy, df_tnx), (news_blocked, news_title) = await asyncio.gather(
        get_macro_data(session),
        check_news_window(session),
    )

    curr_price = MARKET_STATE["last_price"] or float(df_15m["close"].iloc[-1])
    await update_open_signals_status(curr_price)

    # ATR-15m
    tr = np.maximum(
        df_15m["high"] - df_15m["low"],
        np.maximum(
            abs(df_15m["high"] - df_15m["close"].shift(1)),
            abs(df_15m["low"] - df_15m["close"].shift(1)),
        ),
    )
    atr15 = float(tr.tail(14).mean())

    # 1. Quant & Microstructure
    closes_15m = df_15m["close"].values
    kalman = calc_kalman_filter(closes_15m)
    hurst, regime = calc_hurst_exponent(closes_15m)
    tail = calc_garman_klass_and_tail_guard(df_15m)
    cusum = calc_cusum_filter(df_15m, atr15)
    ofi = compute_hybrid_ofi(df_15m)
    vpin = MARKET_STATE["vpin"]
    spread_ok, spread_val = check_live_spread_guard()
    vp = compute_volume_profile_and_vwap(df_15m)
    bull_div, bear_div = detect_rsi_divergence(df_15m)

    # 2. MTF SMC/ICT
    st_15m = analyze_smc_structure(df_15m)
    st_1h = analyze_smc_structure(df_1h)
    st_4h = analyze_smc_structure(df_4h)
    st_1d = analyze_smc_structure(df_1d) if not df_1d.empty else {"bias": "neutral"}
    zones = detect_smc_zones_and_ifvg(df_15m)
    sweeps = detect_liquidity_matrix(df_15m)

    df_m30 = resample_15m_df(df_15m, 2)
    df_m45 = resample_15m_df(df_15m, 3)
    m30_x3 = detect_bank_manipulation_x3(df_m30)
    m45_x3 = detect_bank_manipulation_x3(df_m45)

    ltf_buy_ok, ltf_buy_lbl = check_5m_ltf_trigger(df_5m, "BUY")
    ltf_sell_ok, ltf_sell_lbl = check_5m_ltf_trigger(df_5m, "SELL")

    # 3. Macro & Cross-Asset SMT
    dxy_smt = detect_cross_smt(df_15m, df_dxy, inverse=True)
    eur_smt = detect_cross_smt(df_15m, df_eur, inverse=False)
    btc_smt = detect_cross_smt(df_15m, df_btc, inverse=False)
    us10y_bias = analyze_smc_structure(df_tnx)["bias"] if not df_tnx.empty else "neutral"
    kz_active, kz_name, kz_pts = get_killzone_info()

    MARKET_STATE.update({
        "last_price": round(curr_price, 2),
        "ofi": ofi,
        "hurst": hurst,
        "regime": regime,
        "kalman_price": kalman["price"],
        "kalman_velocity": kalman["velocity"],
        "gk_vol": tail["gk_vol"],
        "kurtosis": tail["kurtosis"],
        "cusum_dir": cusum["direction"],
        "dxy_smt": dxy_smt,
        "eur_smt": eur_smt,
        "btc_smt": btc_smt,
        "us10y_bias": us10y_bias,
        "poc": vp["poc"],
        "vwap": vp["vwap"],
        "news_blocked": news_blocked,
        "news_title": news_title,
    })

    # ==================== SCORING ENGINE (MAX 100) ====================
    buy_score, sell_score = 0, 0
    buy_confs, sell_confs = [], []

    # A. MTF Alignment (max 22 pts)
    for lbl, st, pts in (("1D", st_1d, 4), ("4H", st_4h, 6), ("1H", st_1h, 6)):
        if st["bias"] == "bullish":
            buy_score += pts
            buy_confs.append(f"{lbl} Bull")
        elif st["bias"] == "bearish":
            sell_score += pts
            sell_confs.append(f"{lbl} Bear")

    if st_15m["event"] in ("BOS_BULL", "CHoCH_BULL"):
        buy_score += 6
        buy_confs.append(st_15m["event"])
    elif st_15m["event"] in ("BOS_BEAR", "CHoCH_BEAR"):
        sell_score += 6
        sell_confs.append(st_15m["event"])

    # B. Liquidity Sweep + Bank X3 Manipulation (max 20 pts)
    if sweeps["ssl_sweep"]:
        buy_score += 16 if sweeps["major_sweep"] else 11
        buy_confs.append("SSL/Asia/PDL Sweep")
    if sweeps["bsl_sweep"]:
        sell_score += 16 if sweeps["major_sweep"] else 11
        sell_confs.append("BSL/Asia/PDH Sweep")

    if m30_x3["bull"] or m45_x3["bull"]:
        buy_score += 5
        buy_confs.append("M30/M45 X3 Negation")
    if m30_x3["bear"] or m45_x3["bear"]:
        sell_score += 5
        sell_confs.append("M30/M45 X3 Negation")

    # C. ICT OTE + Zones (FVG / IFVG / OB / Breaker) + 5m LTF Trigger (max 28 pts)
    if st_15m["ote_bot"] <= curr_price <= st_15m["ote_top"]:
        if st_15m["bias"] == "bullish":
            buy_score += 8
            buy_confs.append("ICT OTE")
        elif st_15m["bias"] == "bearish":
            sell_score += 8
            sell_confs.append("ICT OTE")

    if zones["bull_hits"]:
        buy_score += 10
        buy_confs.append(f"Zone:{'/'.join(zones['bull_hits'][:2])}")
    if zones["bear_hits"]:
        sell_score += 10
        sell_confs.append(f"Zone:{'/'.join(zones['bear_hits'][:2])}")

    if ltf_buy_ok:
        buy_score += 10
        buy_confs.append(ltf_buy_lbl)
    if ltf_sell_ok:
        sell_score += 10
        sell_confs.append(ltf_sell_lbl)

    # D. Quant: CUSUM + Hybrid OFI + Kalman + VP/VWAP + RSI Div (max 22 pts)
    if cusum["direction"] == "BULLISH":
        buy_score += 6
        buy_confs.append("CUSUM Wave+")
    elif cusum["direction"] == "BEARISH":
        sell_score += 6
        sell_confs.append("CUSUM Wave-")

    if ofi > 0.14 and kalman["velocity"] > 0:
        buy_score += 8
        buy_confs.append(f"OFI {ofi:+.2f} & Kalman+")
    elif ofi < -0.14 and kalman["velocity"] < 0:
        sell_score += 8
        sell_confs.append(f"OFI {ofi:+.2f} & Kalman-")

    if vp["val"] <= curr_price <= vp["poc"] or curr_price > vp["vwap"]:
        buy_score += 4
        buy_confs.append("VP/VWAP Bull")
    if vp["poc"] <= curr_price <= vp["vah"] or curr_price < vp["vwap"]:
        sell_score += 4
        sell_confs.append("VP/VWAP Bear")

    if bull_div:
        buy_score += 4
        buy_confs.append("RSI Bull Div")
    if bear_div:
        sell_score += 4
        sell_confs.append("RSI Bear Div")

    # E. Macro SMT (DXY, US10Y, EUR, BTC) + NY Open (max 15 pts)
    if dxy_smt == "BULLISH":
        buy_score += 7
        buy_confs.append("DXY SMT+")
    elif dxy_smt == "BEARISH":
        sell_score += 7
        sell_confs.append("DXY SMT-")

    if eur_smt == "BULLISH" or btc_smt == "BULLISH":
        buy_score += 4
        buy_confs.append("EUR/BTC SMT+")
    if eur_smt == "BEARISH" or btc_smt == "BEARISH":
        sell_score += 4
        sell_confs.append("EUR/BTC SMT-")

    if us10y_bias == "bearish":
        buy_score += 3
        buy_confs.append("US10Y Yield-")
    elif us10y_bias == "bullish":
        sell_score += 3
        sell_confs.append("US10Y Yield+")

    if sweeps["ny_open"]:
        if curr_price < sweeps["ny_open"]:
            buy_score += 3
        elif curr_price > sweeps["ny_open"]:
            sell_score += 3

    # F. Killzones & Risk Penalties (News, Spread, Tail Risk, Toxic VPIN)
    if kz_active:
        buy_score += kz_pts
        sell_score += kz_pts
        buy_confs.append(kz_name)
        sell_confs.append(kz_name)

    penalty = 0
    if not tail["safe"]:
        penalty += 14
    if not spread_ok:
        penalty += 12
    if vpin > 0.72:
        penalty += 8
    if news_blocked:
        penalty += 25

    buy_score = min(99, max(0, buy_score - penalty))
    sell_score = min(99, max(0, sell_score - penalty))

    MARKET_STATE["last_score_buy"] = buy_score
    MARKET_STATE["last_score_sell"] = sell_score
    MARKET_STATE["last_confluences"] = buy_confs if buy_score >= sell_score else sell_confs

    # Circuit Breaker & LTF Filter
    stats = await get_bayes_and_daily_stats()
    if stats["circuit_broken"] or news_blocked or not spread_ok:
        return None

    side, final_score, confs = None, 0, []
    if buy_score >= MIN_SIGNAL_SCORE and buy_score > sell_score and ltf_buy_ok:
        side, final_score, confs = "BUY", buy_score, buy_confs
    elif sell_score >= MIN_SIGNAL_SCORE and sell_score > buy_score and ltf_sell_ok:
        side, final_score, confs = "SELL", sell_score, sell_confs

    if not side:
        return None

    sl_dist = round(max(1.80, min(4.20, atr15 * 1.25)), 2)
    entry = round(curr_price, 2)
    if side == "BUY":
        sl, tp1, tp2, tp3 = (
            round(entry - sl_dist, 2),
            round(entry + sl_dist * 2.0, 2),
            round(entry + sl_dist * 3.5, 2),
            round(entry + sl_dist * 5.5, 2),
        )
    else:
        sl, tp1, tp2, tp3 = (
            round(entry + sl_dist, 2),
            round(entry - sl_dist * 2.0, 2),
            round(entry - sl_dist * 3.5, 2),
            round(entry - sl_dist * 5.5, 2),
        )

    win_prob = empirical_bayes_win_prob(final_score, stats["wins"], stats["losses"])
    rec_risk = calc_quarter_kelly(win_prob, rr_ratio=3.5)

    return {
        "id": f"APEX25-{int(time.time())}-{side}",
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
        "spread": spread_val,
        "dxy_smt": dxy_smt,
        "poc": vp["poc"],
        "vwap": vp["vwap"],
        "killzone": kz_name,
        "risk_pct": rec_risk,
        "confluences": confs,
        "status": "ACTIVE",
        "timestamp": int(time.time()),
    }


# ==================== 8. BYBIT WEBSOCKET WORKER ====================
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
                                ORDERBOOK_BIDS.pop(pf, None) if qf == 0 else ORDERBOOK_BIDS.__setitem__(pf, qf)
                            for p, q in d.get("a", []):
                                pf, qf = float(p), float(q)
                                ORDERBOOK_ASKS.pop(pf, None) if qf == 0 else ORDERBOOK_ASKS.__setitem__(pf, qf)
                            if ORDERBOOK_BIDS and ORDERBOOK_ASKS:
                                bb, ba = max(ORDERBOOK_BIDS), min(ORDERBOOK_ASKS)
                                MARKET_STATE["bid"] = bb
                                MARKET_STATE["ask"] = ba
                                MARKET_STATE["spread"] = round(ba - bb, 3)
                                MARKET_STATE["l2_ofi"] = compute_l2_ofi()
                                MARKET_STATE["last_ws_ts"] = time.time()
                        elif topic.startswith("publicTrade."):
                            for t in payload.get("data", []):
                                MARKET_STATE["last_price"] = float(t["p"])
                                update_tick_vpin(t["S"], float(t["v"]))
        except Exception as e:
            logger.warning(f"Bybit WS qayta ulanmoqda: {e}")
            await asyncio.sleep(3)


# ==================== 9. TELEGRAM BOT & COMMANDS ====================
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
    confs_str = " • ".join(sig["confluences"][:8])
    text = (
        f"{icon} <b>APEX v25 UNIFIED SIGNAL ({sig['symbol']})</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 <b>Yo'nalish:</b> {sig['side']} | <b>Ball:</b> {sig['score']}/100 (Bayes Win: {sig['win_prob']}%)\n"
        f"🎯 <b>Entry:</b> <code>{sig['entry']}</code>\n"
        f"🛑 <b>Stop-Loss:</b> <code>{sig['sl']}</code> ({sig['sl_points']}$)\n"
        f"✅ <b>TP1 (1:2):</b> <code>{sig['tp1']}</code>\n"
        f"✅ <b>TP2 (1:3.5):</b> <code>{sig['tp2']}</code>\n"
        f"✅ <b>TP3 (1:5.5):</b> <code>{sig['tp3']}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🧠 <b>Rejim:</b> {sig['regime']} (H={sig['hurst']}) | <b>Kalman:</b> {sig['kalman_price']} ({sig['kalman_vel']:+.3f})\n"
        f"🌊 <b>Hybrid OFI:</b> {sig['ofi']:+.3f} | <b>VPIN:</b> {sig['vpin']:.3f} | <b>DXY SMT:</b> {sig['dxy_smt']}\n"
        f"📊 <b>VP POC:</b> {sig['poc']} | <b>VWAP:</b> {sig['vwap']} | <b>Spread:</b> {sig['spread']}\n"
        f"🧩 <b>Tasdiqlar:</b> <i>{confs_str}</i>\n"
        f"⏰ <b>Oyna:</b> {sig['killzone']} | <b>Kelly Risk:</b> {sig['risk_pct']}%"
    )
    await tg_send(session, ADMIN_CHAT_ID, text)


def format_live_status(stats: dict) -> str:
    wb = MARKET_STATE["windows_bridge"]
    bridge_ok = (time.time() - wb["last_seen"]) < 20 if wb["last_seen"] else False
    b_icon = "🟢 ONLINE" if bridge_ok else "🔴 OFFLINE"
    confs = ", ".join(MARKET_STATE["last_confluences"][:6]) or "Kutilmoqda..."
    news_str = f"🛑 {MARKET_STATE['news_title']}" if MARKET_STATE["news_blocked"] else "✅ Xavfsiz"
    return (
        f"📡 <b>APEX v25 JONLI BOZOR ANALIZI ({SYMBOL})</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 <b>Narx:</b> <code>{MARKET_STATE['last_price']}</code> | <b>Kalman:</b> <code>{MARKET_STATE['kalman_price']}</code> ({MARKET_STATE['kalman_velocity']:+.3f})\n"
        f"📈 <b>Hurst:</b> {MARKET_STATE['hurst']} ({MARKET_STATE['regime']}) | <b>CUSUM:</b> {MARKET_STATE['cusum_dir']}\n"
        f"🌊 <b>OFI:</b> {MARKET_STATE['ofi']:+.3f} | <b>VPIN:</b> {MARKET_STATE['vpin']:.3f} | <b>Kurtosis:</b> {MARKET_STATE['kurtosis']}\n"
        f"🌐 <b>SMT:</b> DXY={MARKET_STATE['dxy_smt']} | EUR={MARKET_STATE['eur_smt']} | BTC={MARKET_STATE['btc_smt']}\n"
        f"📊 <b>VP POC:</b> {MARKET_STATE['poc']} | <b>VWAP:</b> {MARKET_STATE['vwap']} | <b>News:</b> {news_str}\n"
        f"🎯 <b>Joriy Ball:</b> BUY <b>{MARKET_STATE['last_score_buy']}</b> / SELL <b>{MARKET_STATE['last_score_sell']}</b> (Min: {MIN_SIGNAL_SCORE})\n"
        f"🧩 <b>Omillar:</b> <i>{confs}</i>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>Bayes Tarix:</b> {stats['wins']}W / {stats['losses']}L | <b>Bugun:</b> {stats['daily_r']:+.1f}R\n"
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
                        if txt in ("/start", "/status", "/analyze", "/stats"):
                            await run_unified_v25_analysis(app["session"])
                            stats = await get_bayes_and_daily_stats()
                            await tg_send(app["session"], chat_id, format_live_status(stats))
                        elif txt == "/news":
                            blocked, title = await check_news_window(app["session"])
                            msg_n = f"🛑 News Block: {title}" if blocked else "✅ Hozir USD High-Impact yangiliklar yo'q."
                            await tg_send(app["session"], chat_id, msg_n)
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
            sig = await run_unified_v25_analysis(app["session"])
            if sig and (time.time() - LAST_SIGNAL_TS) >= SIGNAL_COOLDOWN_SEC:
                LAST_SIGNAL_TS = time.time()
                ACTIVE_SIGNALS.append(sig)
                await save_signal_db(sig)
                await send_signal_alert(app["session"], sig)
                await broadcast_ws_signal(sig)
                logger.info(f"🚀 APEX v25 Signal: {sig['id']} | Score={sig['score']} | WinProb={sig['win_prob']}%")
        except Exception as e:
            logger.error(f"Scanner xatosi: {e}")
        await asyncio.sleep(15)


# ==================== 10. HTTP & WEBSOCKET BRIDGE ====================
def check_auth(request) -> bool:
    token = request.headers.get("X-Bridge-Token") or request.query.get("token", "")
    return token == BRIDGE_TOKEN


async def handle_health(request):
    return web.json_response({"ok": True, "engine": "APEX_v25_UNIFIED", "market": MARKET_STATE})


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