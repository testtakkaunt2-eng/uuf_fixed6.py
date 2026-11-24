"""
RAILWAY CLOUD SERVER — APEX v28 QUANT, MICROSTRUCTURE & SMC/ICT ANALYZER (20$ REAL BALANCE EDITION)
====================================================================================================
20$ real balans (Standard yoki Cent) uchun maxsus moslashtirilgan, xatolari tuzatilgan va
juda kam odam biladigan mikrostruktura / statistik / dinamik tizim indikatorlari qo'shilgan
to'liq Python server (uuf_fixed9.py -> apex_v28_20usd.py).

Yangi qo'shilgan kam uchraydigan (niche) indikatorlar va mexanizmlar:
1. Kyle's Lambda (Order Flow Price Impact / Market Depth Resilience)
2. Amihud Illiquidity & Liquidity Vacuum / Thin-Book Detector
3. Hawkes-style / Arrival Burst Intensity & Tick Run-Length Asymmetry
4. Permutation Entropy (Ordinal Patterns — Bandt & Pompe, moliyaviy vaqt qatorlarida shovqin o'lchovi)
5. Detrended Fluctuation Analysis (DFA) Fractal Scaling Exponent (alpha)
6. Ornstein-Uhlenbeck (OU) Mean-Reversion Half-Life & Z-Spread
7. Yangi Qo'shilgan Instrumentlar: ^GSPC (S&P 500) SMT va QQQ/NDX yoki XAU/XAG Ratio (Gold-Silver Ratio) dinamikasi
8. 20$ Real Balans Himoyasi:
   - Aniq Position Sizing (ACCOUNT_MODE=standard|cent, PIP_VALUE_PER_001_LOT, MIN_LOT, LOT_STEP)
   - Agar minimal lot risk limitidan oshsa -> savdoni xavfsiz o'tkazib yuborish (ZERO OVER-LEVERAGE)
   - Maksimal 1 ta parallel XAUUSD pozitsiya, qattiq kunlik zarar limiti ($ / R), ketma-ket 2 LOSS da Anti-Tilt pauza
   - Partial TP1 + BE+Buffer + Time-Based Stagnation Exit + MT5 Execution ACK endpoint (/api/bridge/ack)
"""

import argparse
import asyncio
import datetime
import json
import logging
import math
import os
import time
from collections import deque
from dataclasses import dataclass
from itertools import permutations
from pathlib import Path
from zoneinfo import ZoneInfo

import aiohttp
from aiohttp import web
import aiosqlite
import numpy as np
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [APEX-v28-20USD] %(levelname)s: %(message)s",
)
logger = logging.getLogger("apex_v28_20usd")

UZT = datetime.timezone(datetime.timedelta(hours=5))
NY_TZ = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class Config:
    bot_token: str = os.getenv("BOT_TOKEN", "").strip()
    admin_chat_id: str = os.getenv("ADMIN_CHAT_ID", "").strip()
    bridge_token: str = os.getenv("BRIDGE_TOKEN", "").strip()
    allow_query_token: bool = os.getenv("ALLOW_QUERY_TOKEN", "0").strip() == "1"

    # Asosiy va qo'shimcha instrumentlar
    symbol: str = os.getenv("SYMBOL", "XAUUSDT").strip()
    silver_symbol: str = os.getenv("SILVER_SYMBOL", "XAGUSDT").strip()
    btc_symbol: str = os.getenv("BTC_SYMBOL", "BTCUSDT").strip()
    eur_symbol: str = os.getenv("EUR_SYMBOL", "EURUSDT").strip()

    # Broker va narx sinxronizatsiyasi
    active_broker: str = os.getenv("ACTIVE_BROKER", "exness").strip().lower()
    auto_sync_enabled: bool = os.getenv("AUTO_SYNC_ENABLED", "1").strip() != "0"
    default_offset: float = float(os.getenv("PRICE_OFFSET", "0.0"))
    exness_cal: float = float(os.getenv("EXNESS_CALIBRATION", "0.0"))
    xm_cal: float = float(os.getenv("XM_CALIBRATION", "0.25"))
    max_offset_jump: float = float(os.getenv("MAX_OFFSET_JUMP", "6.0"))
    bridge_fresh_sec: float = float(os.getenv("BRIDGE_FRESH_SEC", "12.0"))

    # 20$ REAL HISOB VA RISK SOZLAMALARI
    account_mode: str = os.getenv("ACCOUNT_MODE", "standard").strip().lower()  # "standard" yoki "cent"
    fallback_balance_usd: float = float(os.getenv("FALLBACK_BALANCE_USD", "20.0"))
    # Standard XAUUSD: 0.01 lotda 1.00$ narx harakati ~ 0.10$ (100 oz * 0.01 = 1 oz -> $1/oz, lekin broker lot/pip qiymatiga qarab)
    # Ko'p brokerlarda 0.01 lot XAUUSD uchun $1.00 = $0.10 (10 cent) yoki $1.00. Sozlanuvchan qilindi:
    usd_per_001_lot_per_dollar: float = float(os.getenv("USD_PER_001_LOT_PER_DOLLAR", "0.10"))
    min_lot: float = float(os.getenv("MIN_LOT", "0.01"))
    lot_step: float = float(os.getenv("LOT_STEP", "0.01"))
    max_lot_20usd: float = float(os.getenv("MAX_LOT_20USD", "0.02"))
    max_open_positions: int = int(os.getenv("MAX_OPEN_POSITIONS", "1"))
    risk_per_trade_pct: float = float(os.getenv("RISK_PER_TRADE_PCT", "3.0"))  # 20$ da 3% = $0.60 risk
    daily_max_loss_r: float = float(os.getenv("DAILY_MAX_LOSS_R", "2.0"))
    daily_max_loss_usd: float = float(os.getenv("DAILY_MAX_LOSS_USD", "2.00"))
    anti_tilt_pause_sec: int = int(os.getenv("ANTI_TILT_PAUSE_SEC", "7200"))  # 2 soat pauza

    # Signal, spread va vaqt chegaralari
    min_signal_score: int = int(os.getenv("MIN_SIGNAL_SCORE", "76"))
    max_spread_pts: float = float(os.getenv("MAX_SPREAD_PTS", "0.35"))  # 20$ balans uchun qattiq spread filtri
    signal_cooldown_sec: int = int(os.getenv("SIGNAL_COOLDOWN_SEC", "1200"))
    max_trade_age_sec: int = int(os.getenv("MAX_TRADE_AGE_SEC", "14400"))  # 4 soat stagnation exit
    be_buffer_pts: float = float(os.getenv("BE_BUFFER_PTS", "0.15"))  # BE + spread/komissiya buferi
    daily_report_hour_uzt: int = int(os.getenv("DAILY_REPORT_HOUR_UZT", "22"))

    # Quant & Microstructure chegaralari
    bucket_target_vol: float = float(os.getenv("BUCKET_TARGET_VOL", "35.0"))
    sl_min_pts: float = float(os.getenv("SL_MIN_PTS", "1.20"))
    sl_max_pts: float = float(os.getenv("SL_MAX_PTS", "3.00"))  # 20$ balansda haddan tashqari keng SL ni cheklash
    max_vwap_zscore: float = float(os.getenv("MAX_VWAP_ZSCORE", "2.25"))
    max_vpin: float = float(os.getenv("MAX_VPIN", "0.70"))
    max_entropy: float = float(os.getenv("MAX_ENTROPY", "0.92"))
    min_adx_trend: float = float(os.getenv("MIN_ADX_TREND", "17.0"))

    bybit_rest: str = os.getenv("BYBIT_REST_URL", "https://api.bybit.com").rstrip("/")
    bybit_ws: str = os.getenv("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/linear")
    swissquote_xau_url: str = "https://forex-data-feed.swissquote.com/public-quotes/bboquotes/instrument/XAU/USD"
    yahoo_xau_spot_url: str = "https://query1.finance.yahoo.com/v8/finance/chart/XAUUSD=X"
    news_url: str = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
    db_path: str = os.getenv("DB_PATH", "/data/apex_quant_v28.sqlite")


CFG = Config()


def resolve_db_path(raw_path: str) -> str:
    p = Path(raw_path)
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        return str(p)
    except OSError:
        fallback = Path("apex_quant_v28.sqlite")
        logger.warning(f"DB papkasi ({p.parent}) yaratilmadi, lokal fayl ishlatiladi: {fallback}")
        return str(fallback)


ACTIVE_DB_PATH = resolve_db_path(CFG.db_path)

STATE_LOCK = asyncio.Lock()
ORDERBOOK_LOCK = asyncio.Lock()

MARKET_STATE: dict = {
    "symbol": CFG.symbol,
    "active_broker": CFG.active_broker if CFG.active_broker in ("exness", "xm") else "exness",
    "trading_paused": False,
    "pause_until_ts": 0.0,
    "bybit_raw_price": 0.0,
    "spot_base_price": 0.0,
    "exness_price": 0.0,
    "xm_price": 0.0,
    "broker_price": 0.0,
    "price_offset": CFG.default_offset,
    "sync_source": "init",
    "offset_anomaly": False,
    "bid": 0.0,
    "ask": 0.0,
    "last_price": 0.0,
    "spread": 0.0,
    # Klassik va yangi mikrostruktura / statistik metrikalar
    "ofi": 0.0,
    "l2_ofi": 0.0,
    "vpin": 0.25,
    "kyles_lambda": 0.0,
    "amihud_illiq": 0.0,
    "arrival_burst": 0.0,
    "perm_entropy": 0.50,
    "dfa_alpha": 0.50,
    "ou_halflife": 0.0,
    "ou_zscore": 0.0,
    "xau_xag_ratio": 0.0,
    "xau_xag_zscore": 0.0,
    "hurst": 0.50,
    "entropy": 0.50,
    "regime": "NEUTRAL",
    "kalman_price": 0.0,
    "kalman_velocity": 0.0,
    "garch_vol": 0.0,
    "kurtosis": 3.0,
    "vwap_zscore": 0.0,
    "cusum_dir": "NONE",
    "cvd_div": "NONE",
    "dxy_smt": "NONE",
    "silver_smt": "NONE",
    "eur_smt": "NONE",
    "btc_smt": "NONE",
    "spx_smt": "NONE",
    "us10y_bias": "neutral",
    "vix_level": 0.0,
    "quarter_phase": "Q1",
    "poc": 0.0,
    "vwap": 0.0,
    "bid_wall": 0.0,
    "ask_wall": 0.0,
    "news_blocked": False,
    "news_title": None,
    "market_open": True,
    "last_score_buy": 0,
    "last_score_sell": 0,
    "last_confluences": [],
    "last_ws_ts": 0.0,
    "last_closed_bar_ts": 0,
    "windows_bridge": {
        "online": False,
        "last_seen": 0.0,
        "balance": 0.0,
        "equity": 0.0,
        "free_margin": 0.0,
        "mt5_bid": 0.0,
        "mt5_ask": 0.0,
        "open_positions": 0,
        "last_ack": None,
    },
}

ORDERBOOK_BIDS: dict[float, float] = {}
ORDERBOOK_ASKS: dict[float, float] = {}
TRADE_BUCKETS: deque = deque(maxlen=45)
CURRENT_BUCKET = {"buy": 0.0, "sell": 0.0, "vol": 0.0}
TICK_WINDOW: deque = deque(maxlen=400)  # (ts, price, qty, is_buy)
IMPACT_SAMPLES: deque = deque(maxlen=120)  # (abs_delta_q, abs_dp)

ACTIVE_SIGNALS: deque = deque(maxlen=50)
BRIDGE_EVENTS: deque = deque(maxlen=100)
WS_CLIENTS: set = set()
EVENT_SEQ = 0
LAST_SIGNAL_TS = 0.0
LAST_UPDATE_ID = 0
LAST_DAILY_REPORT_DATE = ""
WS_ALERT_SENT = False

_NEWS_CACHE: dict = {"events": [], "ts": 0.0}
_MACRO_CACHE: dict = {"ts": 0.0, "df_dxy": pd.DataFrame(), "df_tnx": pd.DataFrame(), "vix": 0.0, "df_spx": pd.DataFrame()}
_OFFSET_HISTORY: deque = deque(maxlen=15)
_PERMUTATIONS_6 = {p: idx for idx, p in enumerate(permutations(range(3)))}


# ==================== 0. FOREX SOATLARI, 20$ BALANS VA BROKER NARX ====================
def is_forex_market_open(now_utc: datetime.datetime | None = None) -> bool:
    """Forex/Oltin (Exness/XM): Yakshanba 22:00 UTC dan Juma 22:00 UTC gacha ochiq."""
    now = now_utc or datetime.datetime.now(datetime.timezone.utc)
    wd = now.weekday()
    if wd == 5:
        return False
    if wd == 6 and now.hour < 22:
        return False
    if wd == 4 and now.hour >= 22:
        return False
    return True


def broker_cal_delta(target_broker: str, active_broker: str) -> float:
    tb = target_broker.lower()
    ab = active_broker.lower()
    if tb == ab:
        return 0.0
    if tb == "xm" and ab == "exness":
        return CFG.xm_cal - CFG.exness_cal
    if tb == "exness" and ab == "xm":
        return CFG.exness_cal - CFG.xm_cal
    return 0.0


def to_broker_price(bybit_price: float, broker: str | None = None) -> float:
    if bybit_price <= 0:
        return 0.0
    active_b = MARKET_STATE["active_broker"]
    target_b = (broker or active_b).lower()
    off = float(MARKET_STATE["price_offset"]) + broker_cal_delta(target_b, active_b)
    return round(bybit_price + off, 2)


def get_effective_balance_usd() -> float:
    """Bridge'dan kelgan balansni (Cent rejimida 100 ga bo'lib) yoki 20$ bazaviy balansni qaytaradi."""
    wb = MARKET_STATE["windows_bridge"]
    bridge_fresh = bool(wb["last_seen"] and (time.time() - wb["last_seen"]) <= CFG.bridge_fresh_sec * 3)
    raw_bal = float(wb.get("balance") or 0.0)
    if bridge_fresh and raw_bal > 0:
        if CFG.account_mode == "cent":
            # Agar bridge USC (masalan 2000.0) yuborsa USD ga o'tkazamiz
            return round(raw_bal / 100.0, 2) if raw_bal > 150.0 else round(raw_bal, 2)
        return round(raw_bal, 2)
    return round(CFG.fallback_balance_usd, 2)


def calc_position_size_20usd(balance_usd: float, risk_pct: float, sl_points: float, anti_tilt: bool = False) -> dict:
    """
    20$ real balans uchun aniq lot kalkulyatori:
    - Maksimal ruxsat etilgan zarar ($) = balance_usd * (risk_pct / 100)
    - 0.01 lot uchun SL dagi taxminiy zarar ($) = sl_points * usd_per_001_lot_per_dollar
    - Agar minimal lot (0.01) dagi zarar ruxsat etilgan limitdan oshsa -> savdoni xavfsiz o'tkazib yuboradi!
    """
    eff_risk_pct = risk_pct * (0.5 if anti_tilt else 1.0)
    max_loss_usd = round(max(0.10, balance_usd * (eff_risk_pct / 100.0)), 2)
    per_001_loss = max(0.01, sl_points * CFG.usd_per_001_lot_per_dollar)

    # Necha bo'lak 0.01 lot sig'adi?
    units_001 = int(math.floor(max_loss_usd / per_001_loss))
    if units_001 < 1:
        return {
            "allowed": False,
            "reason": f"20$ balans + {sl_points}$ SL uchun 0.01 lot zarari (${per_001_loss:.2f}) > risk limit (${max_loss_usd:.2f})",
            "lot": 0.0,
            "max_loss_usd": max_loss_usd,
            "est_loss_usd": round(per_001_loss, 2),
            "eff_risk_pct": round(eff_risk_pct, 2),
        }

    raw_lot = round(units_001 * CFG.lot_step, 2)
    lot = max(CFG.min_lot, min(CFG.max_lot_20usd, raw_lot))
    actual_units = max(1, int(round(lot / CFG.lot_step)))
    est_loss = round(actual_units * per_001_loss, 2)

    # Qo'shimcha qattiq himoya: 1 bitimda balansning 15% dan ortig'iga zarar ko'rmaslik
    if est_loss > balance_usd * 0.15:
        return {
            "allowed": False,
            "reason": f"Taxminiy zarar (${est_loss:.2f}) balansning 15% (${balance_usd * 0.15:.2f}) dan yuqori",
            "lot": 0.0,
            "max_loss_usd": max_loss_usd,
            "est_loss_usd": est_loss,
            "eff_risk_pct": round(eff_risk_pct, 2),
        }

    return {
        "allowed": True,
        "reason": "OK",
        "lot": lot,
        "max_loss_usd": max_loss_usd,
        "est_loss_usd": est_loss,
        "eff_risk_pct": round(eff_risk_pct, 2),
    }


async def fetch_spot_xauusd_price(session: aiohttp.ClientSession) -> tuple[float | None, str | None]:
    wb = MARKET_STATE["windows_bridge"]
    if wb["last_seen"] and (time.time() - wb["last_seen"]) <= CFG.bridge_fresh_sec:
        if wb["mt5_bid"] > 1000 and wb["mt5_ask"] > 1000 and wb["mt5_ask"] >= wb["mt5_bid"]:
            mid = round((wb["mt5_bid"] + wb["mt5_ask"]) / 2.0, 2)
            active_cal = CFG.exness_cal if MARKET_STATE["active_broker"] == "exness" else CFG.xm_cal
            return round(mid - active_cal, 2), "MT5 Live Bridge"

    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        async with session.get(CFG.swissquote_xau_url, headers=headers, timeout=7) as r:
            if r.status == 200:
                data = await r.json(content_type=None)
                if isinstance(data, list) and data:
                    prices = data[0].get("spreadProfilePrices") or []
                    if prices:
                        bid, ask = float(prices[0].get("bid", 0)), float(prices[0].get("ask", 0))
                        if bid > 1000 and ask > 1000:
                            return round((bid + ask) / 2.0, 2), "Swissquote Spot"
            else:
                logger.debug(f"Swissquote HTTP {r.status}")
    except Exception as e:
        logger.debug(f"Swissquote fetch xato: {e}")

    try:
        async with session.get(CFG.yahoo_xau_spot_url, params={"interval": "1m", "range": "1d"}, headers=headers, timeout=7) as r:
            if r.status == 200:
                d = await r.json(content_type=None)
                reg_p = float(d["chart"]["result"][0]["meta"].get("regularMarketPrice") or 0)
                if reg_p > 1000:
                    return round(reg_p, 2), "Yahoo Spot"
            else:
                logger.debug(f"Yahoo XAU HTTP {r.status}")
    except Exception as e:
        logger.debug(f"Yahoo XAU fetch xato: {e}")
    return None, None


async def run_auto_sync_once(session: aiohttp.ClientSession, force: bool = False) -> dict | None:
    if not CFG.auto_sync_enabled and not force:
        return None
    raw_bybit = MARKET_STATE["bybit_raw_price"]
    if raw_bybit <= 1000:
        return None
    spot_base, src = await fetch_spot_xauusd_price(session)
    if spot_base is None:
        return None

    exness_p = round(spot_base + CFG.exness_cal, 2)
    xm_p = round(spot_base + CFG.xm_cal, 2)
    target_p = exness_p if MARKET_STATE["active_broker"] == "exness" else xm_p
    new_off = round(target_p - raw_bybit, 2)

    async with STATE_LOCK:
        anomaly = False
        if len(_OFFSET_HISTORY) >= 3:
            med = float(np.median(list(_OFFSET_HISTORY)))
            if abs(new_off - med) > CFG.max_offset_jump and not force:
                anomaly = True
                logger.warning(f"Offset anomaliyasi aniqlandi: {new_off} vs median {med}")
        if not anomaly:
            _OFFSET_HISTORY.append(new_off)
            MARKET_STATE["price_offset"] = new_off

        MARKET_STATE.update({
            "spot_base_price": spot_base,
            "exness_price": exness_p,
            "xm_price": xm_p,
            "broker_price": target_p,
            "sync_source": src or "Spot",
            "offset_anomaly": anomaly,
        })
    return {
        "bybit_raw": round(raw_bybit, 2),
        "exness": exness_p,
        "xm": xm_p,
        "offset": MARKET_STATE["price_offset"],
        "source": src,
    }


async def auto_sync_worker(app):
    await asyncio.sleep(3)
    while True:
        try:
            if is_forex_market_open():
                await run_auto_sync_once(app["session"])
        except Exception as e:
            logger.warning(f"Auto-sync xato: {e}")
        await asyncio.sleep(25)


# ==================== 1. DATABASE, BRIDGE EVENTS & OUTCOME TRACKER ====================
async def init_db():
    async with aiosqlite.connect(ACTIVE_DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id TEXT PRIMARY KEY,
                created_at TEXT,
                created_ts INTEGER DEFAULT 0,
                date_uzt TEXT,
                symbol TEXT,
                side TEXT,
                lot REAL DEFAULT 0.01,
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
                be_moved INTEGER DEFAULT 0,
                partial_taken INTEGER DEFAULT 0,
                flip_warned INTEGER DEFAULT 0,
                pnl_r REAL DEFAULT 0.0,
                pnl_usd REAL DEFAULT 0.0,
                mt5_ticket TEXT DEFAULT ''
            )
        """)
        async with db.execute("PRAGMA table_info(signals)") as cur:
            cols = {row[1] for row in await cur.fetchall()}
        migrations = {
            "created_ts": "INTEGER DEFAULT 0",
            "lot": "REAL DEFAULT 0.01",
            "partial_taken": "INTEGER DEFAULT 0",
            "flip_warned": "INTEGER DEFAULT 0",
            "pnl_usd": "REAL DEFAULT 0.0",
            "mt5_ticket": "TEXT DEFAULT ''",
        }
        for col_name, col_def in migrations.items():
            if col_name not in cols:
                await db.execute(f"ALTER TABLE signals ADD COLUMN {col_name} {col_def}")
        await db.commit()


async def push_bridge_event(payload: dict) -> dict:
    global EVENT_SEQ
    async with STATE_LOCK:
        EVENT_SEQ += 1
        ev = {"seq": EVENT_SEQ, "ts": int(time.time()), **payload}
        BRIDGE_EVENTS.append(ev)
    await broadcast_ws_raw(ev)
    return ev


async def save_signal_db(sig: dict):
    date_uzt = datetime.datetime.now(UZT).strftime("%Y-%m-%d")
    async with aiosqlite.connect(ACTIVE_DB_PATH) as db:
        await db.execute("""
            INSERT OR REPLACE INTO signals
            (id, created_at, created_ts, date_uzt, symbol, side, lot, entry, sl, tp1, tp2, tp3,
             score, win_prob, regime, hurst, vpin, ofi, confluences, status, be_moved, partial_taken, flip_warned, pnl_r, pnl_usd, mt5_ticket)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0, 0.0, 0.0, '')
        """, (
            sig["id"], sig["created_at"], int(sig.get("timestamp") or time.time()), date_uzt,
            sig["symbol"], sig["side"], float(sig.get("lot", 0.01)),
            sig["entry"], sig["sl"], sig["tp1"], sig["tp2"], sig["tp3"],
            sig["score"], sig["win_prob"], sig["regime"], sig["hurst"],
            sig["vpin"], sig["ofi"], json.dumps(sig["confluences"]), sig["status"]
        ))
        await db.commit()


async def update_open_signals_status(
    session: aiohttp.ClientSession,
    broker_bid: float,
    broker_ask: float,
    bar_low: float,
    bar_high: float,
    st_5m: dict,
    ofi: float,
):
    """
    20$ real balans uchun takomillashtirilgan pozitsiya boshqaruvi:
    - TP1 urilsa: Partial lock (+0.75R) + SL ni Entry + BE_BUFFER ga ko'chirish
    - TP2 (+3.25R) va TP3 (+5.0R) kuzatuvi
    - Time-based stagnation exit (4 soatdan oshsa va BE ham urilmagan bo'lsa, kichik foyda/zararda yopish yoki ogohlantirish)
    - Flip ogohlantirish
    """
    if broker_bid <= 0 or broker_ask <= 0:
        return
    now_ts = int(time.time())
    try:
        async with aiosqlite.connect(ACTIVE_DB_PATH) as db:
            async with db.execute(
                """SELECT id, side, lot, entry, sl, tp1, tp2, tp3, be_moved, partial_taken, flip_warned, created_ts
                   FROM signals WHERE status IN ('ACTIVE', 'BE_ACTIVE')"""
            ) as cur:
                rows = await cur.fetchall()

            for row_id, side, lot, entry, sl, tp1, tp2, tp3, be_moved, partial_taken, flip_warned, created_ts in rows:
                new_status, pnl_r = None, 0.0
                hit_tp1 = False
                lot_val = float(lot or 0.01)
                sl_dist = max(0.50, abs(float(entry) - float(sl))) if not be_moved else 1.50
                est_1r_usd = round(sl_dist * (lot_val / 0.01) * CFG.usd_per_001_lot_per_dollar, 3)

                if side == "BUY":
                    eff_low = min(broker_bid, bar_low) if bar_low > 0 else broker_bid
                    eff_high = max(broker_bid, bar_high) if bar_high > 0 else broker_bid
                    if eff_low <= sl:
                        if be_moved:
                            new_status = "BE_WIN"
                            pnl_r = 0.75 if partial_taken else 0.0
                        else:
                            new_status = "LOSS"
                            pnl_r = -1.0
                    elif eff_high >= tp3:
                        new_status, pnl_r = "WIN", 5.0
                    elif eff_high >= tp2:
                        new_status, pnl_r = "WIN", 3.25
                    elif eff_high >= tp1 and not be_moved:
                        hit_tp1 = True

                elif side == "SELL":
                    eff_high = max(broker_ask, bar_high) if bar_high > 0 else broker_ask
                    eff_low = min(broker_ask, bar_low) if bar_low > 0 else broker_ask
                    if eff_high >= sl:
                        if be_moved:
                            new_status = "BE_WIN"
                            pnl_r = 0.75 if partial_taken else 0.0
                        else:
                            new_status = "LOSS"
                            pnl_r = -1.0
                    elif eff_low <= tp3:
                        new_status, pnl_r = "WIN", 5.0
                    elif eff_low <= tp2:
                        new_status, pnl_r = "WIN", 3.25
                    elif eff_low <= tp1 and not be_moved:
                        hit_tp1 = True

                if hit_tp1:
                    be_sl = round(float(entry) + CFG.be_buffer_pts, 2) if side == "BUY" else round(float(entry) - CFG.be_buffer_pts, 2)
                    await db.execute(
                        "UPDATE signals SET status = 'BE_ACTIVE', sl = ?, be_moved = 1, partial_taken = 1 WHERE id = ?",
                        (be_sl, row_id),
                    )
                    await push_bridge_event({
                        "event": "breakeven_partial",
                        "signal_id": row_id,
                        "new_sl": be_sl,
                        "partial_close_pct": 50,
                    })
                    await tg_send(
                        session,
                        CFG.admin_chat_id,
                        f"✅ <b>TP1 URILDI ({row_id})</b>
"
                        f"• 50% qism foyda qayd etildi, SL <code>{be_sl}</code> (BE+buffer) ga surildi.",
                    )

                elif new_status:
                    pnl_usd = round(pnl_r * est_1r_usd, 2)
                    await db.execute(
                        "UPDATE signals SET status = ?, pnl_r = ?, pnl_usd = ? WHERE id = ?",
                        (new_status, pnl_r, pnl_usd, row_id),
                    )
                    await push_bridge_event({
                        "event": "signal_closed",
                        "signal_id": row_id,
                        "status": new_status,
                        "pnl_r": pnl_r,
                        "pnl_usd": pnl_usd,
                    })
                    icon = "🏆" if pnl_r > 0 else ("⚖️" if pnl_r == 0 else "🛑")
                    await tg_send(
                        session,
                        CFG.admin_chat_id,
                        f"{icon} <b>Signal Yopildi ({row_id}):</b> {new_status} (<code>{pnl_r:+.2f}R</code> | <code>${pnl_usd:+.2f}</code>)",
                    )

                elif created_ts and (now_ts - int(created_ts)) >= CFG.max_trade_age_sec and not be_moved:
                    # 4 soat davomida TP1 ga bormagan bitimni vaqt bo'yicha yopish (20$ balansni ushlab turmaslik uchun)
                    cur_px = broker_bid if side == "BUY" else broker_ask
                    raw_r = ((cur_px - float(entry)) if side == "BUY" else (float(entry) - cur_px)) / max(0.50, sl_dist)
                    clamped_r = round(max(-0.8, min(0.8, raw_r)), 2)
                    pnl_usd = round(clamped_r * est_1r_usd, 2)
                    await db.execute(
                        "UPDATE signals SET status = 'TIME_EXIT', pnl_r = ?, pnl_usd = ? WHERE id = ?",
                        (clamped_r, pnl_usd, row_id),
                    )
                    await push_bridge_event({"event": "time_exit", "signal_id": row_id, "pnl_r": clamped_r})
                    await tg_send(
                        session,
                        CFG.admin_chat_id,
                        f"⏱️ <b>Time-Exit ({row_id}):</b> {CFG.max_trade_age_sec // 3600}h kutilgach yopildi (<code>{clamped_r:+.2f}R</code> | <code>${pnl_usd:+.2f}</code>)",
                    )

                elif not flip_warned and (
                    (side == "BUY" and st_5m["event"] == "CHoCH_BEAR" and ofi < -0.25)
                    or (side == "SELL" and st_5m["event"] == "CHoCH_BULL" and ofi > 0.25)
                ):
                    await db.execute("UPDATE signals SET flip_warned = 1 WHERE id = ?", (row_id,))
                    await push_bridge_event({"event": "flip_warning", "signal_id": row_id, "side": side, "ofi": ofi})
                    await tg_send(
                        session,
                        CFG.admin_chat_id,
                        f"⚠️ <b>FLIP OGOHLANTIRISH ({row_id})</b>
"
                        f"{side} pozitsiyaga qarshi 5m {st_5m['event']} va OFI={ofi:+.2f} aniqlandi.",
                    )

            await db.commit()
    except Exception as e:
        logger.error(f"Outcome tracker xato: {e}")


async def get_stats_and_psychology() -> dict:
    """Empirical Bayes, Kunlik Circuit Breaker (R va $), Anti-Tilt Psixologiya Guard va Ochiq Signallar soni."""
    today = datetime.datetime.now(UZT).strftime("%Y-%m-%d")
    now_ts = time.time()
    try:
        async with aiosqlite.connect(ACTIVE_DB_PATH) as db:
            async with db.execute(
                "SELECT status, pnl_r, pnl_usd, date_uzt, score, confluences, created_ts FROM signals ORDER BY created_at DESC"
            ) as cur:
                rows = await cur.fetchall()

            wins = sum(1 for s, _, _, _, _, _, _ in rows if s == "WIN")
            be_wins = sum(1 for s, _, _, _, _, _, _ in rows if s == "BE_WIN")
            losses = sum(1 for s, _, _, _, _, _, _ in rows if s == "LOSS")
            open_db_cnt = sum(1 for s, _, _, _, _, _, _ in rows if s in ("ACTIVE", "BE_ACTIVE"))

            closed_statuses = ("WIN", "BE_WIN", "LOSS", "TIME_EXIT")
            daily_r = sum(float(r or 0.0) for s, r, _, d, _, _, _ in rows if d == today and s in closed_statuses)
            daily_usd = sum(float(u or 0.0) for s, _, u, d, _, _, _ in rows if d == today and s in closed_statuses)
            today_cnt = sum(1 for _, _, _, d, _, _, _ in rows if d == today)

            consec_losses = 0
            last_loss_ts = 0
            for s, _, _, _, _, _, cts in rows:
                if s == "LOSS":
                    consec_losses += 1
                    if last_loss_ts == 0:
                        last_loss_ts = int(cts or 0)
                elif s in ("WIN", "BE_WIN"):
                    break

            anti_tilt_active = consec_losses >= 2 and (last_loss_ts == 0 or (now_ts - last_loss_ts) < CFG.anti_tilt_pause_sec)
            circuit_broken = (daily_r <= -abs(CFG.daily_max_loss_r)) or (daily_usd <= -abs(CFG.daily_max_loss_usd))

            bucket_stats: dict[int, list[int]] = {}
            conf_stats: dict[str, list[int]] = {}
            for s, _, _, _, sc, c_json, _ in rows[:80]:
                if s not in ("WIN", "LOSS"):
                    continue
                b_key = (int(sc or 76) // 5) * 5
                bs = bucket_stats.setdefault(b_key, [0, 0])
                bs[0 if s == "WIN" else 1] += 1
                if c_json:
                    for c in json.loads(c_json):
                        st = conf_stats.setdefault(c, [0, 0])
                        st[0 if s == "WIN" else 1] += 1

            return {
                "wins": wins,
                "be_wins": be_wins,
                "losses": losses,
                "open_db_cnt": open_db_cnt,
                "daily_r": round(daily_r, 2),
                "daily_usd": round(daily_usd, 2),
                "today_cnt": today_cnt,
                "consec_losses": consec_losses,
                "anti_tilt_active": anti_tilt_active,
                "circuit_broken": circuit_broken,
                "conf_stats": conf_stats,
                "bucket_stats": bucket_stats,
            }
    except Exception as e:
        logger.error(f"Stats DB xato: {e}")
        return {
            "wins": 0,
            "be_wins": 0,
            "losses": 0,
            "open_db_cnt": 0,
            "daily_r": 0.0,
            "daily_usd": 0.0,
            "today_cnt": 0,
            "consec_losses": 0,
            "anti_tilt_active": False,
            "circuit_broken": False,
            "conf_stats": {},
            "bucket_stats": {},
        }


# ==================== 2. QUANT, FRACTAL, ENTROPY & ORNSTEIN-UHLENBECK ====================
def calc_kalman_filter(closes: np.ndarray, process_var: float = 1e-3, meas_var: float = 0.04) -> dict:
    if len(closes) < 5:
        val = float(closes[-1]) if len(closes) else 0.0
        return {"price": val, "velocity": 0.0, "residual": 0.0}
    x_hat, p_cov, vel = float(closes[0]), 1.0, 0.0
    for z in closes[1:]:
        x_pred = x_hat + vel * 0.25
        p_pred = p_cov + process_var
        k_gain = p_pred / (p_pred + meas_var)
        prev_x = x_hat
        x_hat = x_pred + k_gain * (float(z) - x_pred)
        vel = 0.7 * vel + 0.3 * (x_hat - prev_x)
        p_cov = (1.0 - k_gain) * p_pred
    return {"price": round(x_hat, 2), "velocity": round(vel, 4), "residual": round(float(closes[-1]) - x_hat, 2)}


def calc_hurst_and_entropy(closes: np.ndarray) -> tuple[float, str, float]:
    """Klassik Rescaled Range (R/S) Hurst Exponenti + Shannon Entropiyasi."""
    if len(closes) < 80:
        return 0.50, "INSUFFICIENT_DATA", 0.50

    log_rets = np.diff(np.log(np.maximum(closes, 1e-9)))
    lags = [8, 12, 16, 24, 32, 48]
    rs_vals, valid_lags = [], []

    for lag in lags:
        if len(log_rets) < lag * 2:
            continue
        chunks = len(log_rets) // lag
        rs_chunk = []
        for i in range(chunks):
            seg = log_rets[i * lag : (i + 1) * lag]
            std = float(np.std(seg, ddof=1))
            if std <= 1e-12:
                continue
            dev = np.cumsum(seg - np.mean(seg))
            r_span = float(np.max(dev) - np.min(dev))
            if r_span > 0:
                rs_chunk.append(r_span / std)
        if rs_chunk:
            rs_vals.append(float(np.mean(rs_chunk)))
            valid_lags.append(lag)

    if len(valid_lags) >= 3:
        slope, _ = np.polyfit(np.log(valid_lags), np.log(rs_vals), 1)
        h = max(0.05, min(0.95, round(float(slope), 3)))
    else:
        h = 0.50

    regime = "TREND" if h > 0.55 else ("MEAN_REVERSION" if h < 0.45 else "RANDOM_WALK")

    rets = np.diff(closes[-60:])
    hist, _ = np.histogram(rets, bins=10, density=True)
    probs = hist[hist > 0] / np.sum(hist[hist > 0])
    entropy = round(float(-np.sum(probs * np.log2(probs)) / math.log2(10)), 3) if len(probs) > 1 else 0.5
    return h, regime, entropy


def calc_permutation_entropy(closes: np.ndarray, order: int = 3) -> float:
    """
    Bandt & Pompe Permutation Entropy (kam odam biladigan ordinal pattern entropiyasi):
    Narx ketma-ketligidagi tartib naqshlari (permutations) qanchalik tartibli yoki tasodifiyligini o'lchaydi.
    0.0 = juda tartibli / deterministik; 1.0 = to'liq shovqin / tasodifiy.
    """
    if len(closes) < 40:
        return 0.55
    arr = closes[-120:]
    patterns = []
    for i in range(len(arr) - order + 1):
        window = arr[i : i + order]
        pat = tuple(np.argsort(window, kind="mergesort"))
        idx = _PERMUTATIONS_6.get(pat)
        if idx is not None:
            patterns.append(idx)
    if not patterns:
        return 0.55
    counts = np.bincount(patterns, minlength=6)
    probs = counts[counts > 0] / len(patterns)
    pe = -float(np.sum(probs * np.log2(probs))) / math.log2(6.0)
    return round(max(0.0, min(1.0, pe)), 3)


def calc_dfa_alpha(closes: np.ndarray) -> float:
    """
    Detrended Fluctuation Analysis (DFA) Scaling Exponent (alpha):
    Trend ichidagi chiziqli drift olib tashlangandan keyingi haqiqiy fraktal uzoq muddatli xotira.
    alpha > 0.55: barqaror trend/persistentsiya; alpha < 0.45: mean-reverting.
    """
    if len(closes) < 96:
        return 0.50
    y = np.cumsum(closes[-140:] - np.mean(closes[-140:]))
    scales = [8, 12, 16, 24, 32]
    flucts, used = [], []
    for s in scales:
        n_seg = len(y) // s
        if n_seg < 2:
            continue
        rms_list = []
        x_idx = np.arange(s, dtype=float)
        for k in range(n_seg):
            seg = y[k * s : (k + 1) * s]
            slope, intercept = np.polyfit(x_idx, seg, 1)
            trend = slope * x_idx + intercept
            rms = math.sqrt(float(np.mean((seg - trend) ** 2)))
            if rms > 1e-12:
                rms_list.append(rms)
        if rms_list:
            flucts.append(float(np.mean(rms_list)))
            used.append(s)
    if len(used) < 3:
        return 0.50
    alpha, _ = np.polyfit(np.log(used), np.log(flucts), 1)
    return round(max(0.10, min(1.20, float(alpha))), 3)


def calc_ornstein_uhlenbeck(closes: np.ndarray) -> dict:
    """
    Ornstein-Uhlenbeck (OU) jarayoni: dX_t = theta * (mu - X_t) dt + sigma dW_t
    - half_life: o'rtachaga qaytish yarim davri (barlarda)
    - zscore: mu dan necha sigma uzoqlikda ekani
    """
    if len(closes) < 50:
        return {"halflife": 0.0, "zscore": 0.0, "mu": float(closes[-1]) if len(closes) else 0.0}
    x = closes[-80:].astype(float)
    x_lag = x[:-1]
    dx = np.diff(x)
    var_lag = float(np.var(x_lag))
    if var_lag < 1e-12:
        return {"halflife": 0.0, "zscore": 0.0, "mu": float(x[-1])}
    cov = float(np.cov(x_lag, dx)[0, 1])
    theta = -cov / var_lag
    if theta <= 1e-5:
        return {"halflife": 999.0, "zscore": 0.0, "mu": float(np.mean(x))}
    mu = float(np.mean(x_lag) + np.mean(dx) / max(theta, 1e-5))
    halflife = round(min(200.0, math.log(2.0) / theta), 1)
    resid = x - mu
    sigma_eq = max(float(np.std(resid)), 0.25)
    z = round(float((x[-1] - mu) / sigma_eq), 2)
    return {"halflife": halflife, "zscore": z, "mu": round(mu, 2)}


def calc_volatility_and_tail_guard(df: pd.DataFrame) -> dict:
    """Garman-Klass + EWMA (GARCH-uslubi) Volatillik + Taleb Kurtosis & Jump Guard."""
    log_hl = np.log(df["high"] / df["low"]) ** 2
    log_co = np.log(df["close"] / df["open"]) ** 2
    gk = 0.5 * log_hl - (2.0 * math.log(2.0) - 1.0) * log_co
    gk_vol = round(float(np.sqrt(gk.tail(30).mean()) * 100.0), 4)

    closes = df["close"].values[-60:]
    rets = np.diff(closes)
    ewma_var = float(pd.Series(rets ** 2).ewm(alpha=0.12).mean().iloc[-1])
    garch_pts = round(math.sqrt(max(ewma_var, 1e-6)), 2)

    std = float(np.std(rets))
    if std < 1e-9:
        return {"gk_vol": gk_vol, "garch_pts": garch_pts, "kurtosis": 3.0, "safe": True}
    z = (rets - np.mean(rets)) / std
    kurt = round(float(np.mean(z ** 4)), 2)
    last_move = abs(float(df["close"].iloc[-1]) - float(df["open"].iloc[-1]))
    is_jump = (last_move >= std * 3.4) and (last_move >= 5.0)
    safe = not ((kurt >= 12.0 and is_jump) or kurt > 8.5)
    return {"gk_vol": gk_vol, "garch_pts": garch_pts, "kurtosis": kurt, "safe": safe}


def calc_adx(df: pd.DataFrame, period: int = 14) -> float:
    if len(df) < period * 2:
        return 25.0
    up = df["high"].diff()
    down = -df["low"].diff()
    plus_dm = np.where((up > down) & (up > 0), up, 0.0)
    minus_dm = np.where((down > up) & (down > 0), down, 0.0)
    tr = np.maximum(
        df["high"] - df["low"],
        np.maximum(abs(df["high"] - df["close"].shift(1)), abs(df["low"] - df["close"].shift(1))),
    )
    atr = tr.rolling(period).mean().replace(0, 1e-9)
    p_di = 100 * pd.Series(plus_dm).rolling(period).mean() / atr
    m_di = 100 * pd.Series(minus_dm).rolling(period).mean() / atr
    dx = 100 * (p_di - m_di).abs() / (p_di + m_di).replace(0, 1e-9)
    return round(float(dx.rolling(period).mean().iloc[-1]), 1)


def calc_cusum_filter(df: pd.DataFrame, atr: float, threshold_mult: float = 1.10) -> dict:
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
        return {"event": True, "direction": "BULLISH"}
    if abs(s_neg) >= threshold:
        return {"event": True, "direction": "BEARISH"}
    return {"event": False, "direction": "NONE"}


def empirical_bayes_win_prob(
    score: int,
    wins: int,
    losses: int,
    bucket_stats: dict[int, list[int]] | None = None,
) -> float:
    alpha_0, beta_0 = 5.5, 4.5
    b_key = (score // 5) * 5
    bw, bl = (bucket_stats or {}).get(b_key, [0, 0])
    if (bw + bl) >= 8:
        post = (alpha_0 + bw * 1.5) / (alpha_0 + beta_0 + (bw + bl) * 1.5)
    else:
        base_prob = (alpha_0 + wins) / (alpha_0 + beta_0 + wins + losses)
        score_adj = max(-0.06, min(0.10, (score - 76) * 0.0045))
        post = base_prob + score_adj
    return round(max(0.42, min(0.84, post)) * 100.0, 1)


def calc_quarter_kelly(win_prob_pct: float, rr_ratio: float = 3.25, anti_tilt: bool = False) -> float:
    p = max(0.05, min(0.95, win_prob_pct / 100.0))
    b = max(1.0, rr_ratio)
    f_star = (p * b - (1.0 - p)) / b
    cap = CFG.risk_per_trade_pct * (0.5 if anti_tilt else 1.0)
    return round(max(0.50, min(cap, f_star * 0.25 * 100.0)), 2)


# ==================== 3. MICROSTRUCTURE (KYLE LAMBDA, AMIHUD, BURST, VPIN, OFI) ====================
def compute_l2_metrics() -> tuple[float, float, float]:
    if not ORDERBOOK_BIDS or not ORDERBOOK_ASKS:
        return 0.0, 0.0, 0.0
    top_bids = sorted(ORDERBOOK_BIDS.items(), key=lambda x: -x[0])[:15]
    top_asks = sorted(ORDERBOOK_ASKS.items(), key=lambda x: x[0])[:15]
    b_vol = sum(v for _, v in top_bids)
    a_vol = sum(v for _, v in top_asks)
    ofi = round((b_vol - a_vol) / (b_vol + a_vol), 3) if (b_vol + a_vol) > 0 else 0.0
    bid_wall = max(top_bids, key=lambda x: x[1])[0] if top_bids else 0.0
    ask_wall = max(top_asks, key=lambda x: x[1])[0] if top_asks else 0.0
    return ofi, bid_wall, ask_wall


def compute_hybrid_ofi_and_cvd(df: pd.DataFrame, lookback: int = 12) -> tuple[float, str]:
    win = df.tail(lookback)
    rng = np.maximum(win["high"] - win["low"], 0.01)
    buy_w = np.maximum(0.0, (win["close"] - win["low"]) / rng)
    sell_w = np.maximum(0.0, (win["high"] - win["close"]) / rng)
    delta_series = win["volume"] * (buy_w - sell_w)
    bv = float((win["volume"] * buy_w).sum())
    sv = float((win["volume"] * sell_w).sum())
    candle_ofi = (bv - sv) / max(bv + sv, 1e-6)

    cvd = delta_series.cumsum().values
    cvd_div = "NONE"
    if len(cvd) >= 8:
        if float(win["low"].iloc[-1]) < float(win["low"].iloc[:6].min()) and cvd[-1] > cvd[5]:
            cvd_div = "BULLISH"
        elif float(win["high"].iloc[-1]) > float(win["high"].iloc[:6].max()) and cvd[-1] < cvd[5]:
            cvd_div = "BEARISH"

    l2_fresh = (time.time() - MARKET_STATE["last_ws_ts"]) <= 15.0
    ofi = round(0.55 * MARKET_STATE["l2_ofi"] + 0.45 * candle_ofi, 3) if l2_fresh else round(candle_ofi, 3)
    return ofi, cvd_div


def calc_amihud_illiquidity(df: pd.DataFrame, lookback: int = 24) -> float:
    """Amihud Illiquidity Ratio: |Return| / Dollar Volume (suyuqlik siyrakligini / slippage xavfini ko'rsatadi)."""
    if len(df) < lookback + 2:
        return 0.0
    sub = df.tail(lookback).copy()
    rets = sub["close"].pct_change().abs().fillna(0.0)
    turnover = (sub["close"] * sub["volume"]).replace(0, np.nan)
    ratio = (rets / turnover).dropna()
    if ratio.empty:
        return 0.0
    # O'qishga qulay skala (x10^6)
    return round(float(ratio.mean() * 1e6), 4)


def update_tick_microstructure(side: str, price: float, qty: float):
    """
    Tick oqimidan VPIN, Kyle's Lambda (price impact) va Arrival Burst Intensity ni yangilaydi.
    """
    global CURRENT_BUCKET
    now_t = time.time()
    is_buy = (side == "Buy")
    signed_q = qty if is_buy else -qty

    prev_tick = TICK_WINDOW[-1] if TICK_WINDOW else None
    TICK_WINDOW.append((now_t, price, qty, is_buy))

    if prev_tick is not None:
        dp = abs(price - prev_tick[1])
        dq = max(abs(signed_q), 1e-4)
        if dp > 0:
            IMPACT_SAMPLES.append((dq, dp))

    # Kyle's Lambda approx: cov(|dp|, q_imb) / var(q_imb) yoki mean(dp / sqrt(q))
    if len(IMPACT_SAMPLES) >= 15:
        imp_arr = np.array([dp / math.sqrt(dq) for dq, dp in IMPACT_SAMPLES], dtype=float)
        MARKET_STATE["kyles_lambda"] = round(float(np.median(imp_arr)), 4)

    # Arrival Burst Intensity: oxirgi 5 soniyadagi tick tezligi vs o'rtacha tick tezligi
    recent_5s = sum(1 for t, _, _, _ in TICK_WINDOW if (now_t - t) <= 5.0)
    span_sec = max(1.0, now_t - TICK_WINDOW[0][0]) if len(TICK_WINDOW) > 1 else 1.0
    avg_5s_rate = (len(TICK_WINDOW) / span_sec) * 5.0
    burst = round(recent_5s / max(0.5, avg_5s_rate), 2)
    MARKET_STATE["arrival_burst"] = min(10.0, burst)

    CURRENT_BUCKET["buy" if is_buy else "sell"] += qty
    CURRENT_BUCKET["vol"] += qty
    if CURRENT_BUCKET["vol"] >= CFG.bucket_target_vol:
        TRADE_BUCKETS.append(dict(CURRENT_BUCKET))
        CURRENT_BUCKET = {"buy": 0.0, "sell": 0.0, "vol": 0.0}
        imb = sum(abs(b["buy"] - b["sell"]) for b in TRADE_BUCKETS)
        tot = sum(b["vol"] for b in TRADE_BUCKETS)
        if tot > 0:
            MARKET_STATE["vpin"] = round(imb / tot, 3)


def check_live_spread_guard() -> tuple[bool, float]:
    wb = MARKET_STATE["windows_bridge"]
    if wb["last_seen"] and (time.time() - wb["last_seen"]) <= CFG.bridge_fresh_sec and wb["mt5_ask"] > wb["mt5_bid"] > 0:
        sp = round(wb["mt5_ask"] - wb["mt5_bid"], 3)
        return sp <= CFG.max_spread_pts, sp
    if (time.time() - MARKET_STATE["last_ws_ts"]) <= 15.0 and MARKET_STATE["spread"] > 0:
        return MARKET_STATE["spread"] <= CFG.max_spread_pts, MARKET_STATE["spread"]
    return True, 0.25


# ==================== 4. VOLUME PROFILE, XAU/XAG RATIO, VWAP Z-SCORE & RSI DIV ====================
def compute_vp_and_vwap_zscore(df: pd.DataFrame, bins: int = 24) -> dict:
    window = df.tail(96)
    pmin, pmax = float(window["low"].min()), float(window["high"].max())
    curr = float(window["close"].iloc[-1])
    if pmax <= pmin:
        return {"poc": curr, "vah": curr, "val": curr, "vwap": curr, "zscore": 0.0}

    bsize = (pmax - pmin) / bins
    hist = np.zeros(bins)
    for _, row in window.iterrows():
        idx = max(0, min(bins - 1, int((float(row["close"]) - pmin) / bsize)))
        hist[idx] += float(row["volume"])
    poc_idx = int(np.argmax(hist))
    poc = pmin + (poc_idx + 0.5) * bsize

    target_vol, acc, low_i, high_i = float(np.sum(hist)) * 0.70, hist[poc_idx], poc_idx, poc_idx
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
    vwap_std = max(float(np.std(window["close"] - vwap)), 0.5)
    zscore = round((curr - vwap) / vwap_std, 2)
    return {"poc": round(poc, 2), "vah": round(vah, 2), "val": round(val, 2), "vwap": round(vwap, 2), "zscore": zscore}


def calc_gold_silver_ratio(df_gold: pd.DataFrame, df_xag: pd.DataFrame) -> dict:
    """Gold/Silver Ratio (XAU/XAG) va uning 40-barlik Z-score ko'rsatkichi."""
    if df_gold.empty or df_xag.empty or len(df_gold) < 40 or len(df_xag) < 40:
        return {"ratio": 0.0, "zscore": 0.0, "signal": "NONE"}
    n = min(len(df_gold), len(df_xag), 60)
    g = df_gold["close"].tail(n).values.astype(float)
    s = np.maximum(df_xag["close"].tail(n).values.astype(float), 1e-6)
    ratio = g / s
    mean_r = float(np.mean(ratio[:-1]))
    std_r = max(float(np.std(ratio[:-1])), 1e-6)
    z = round(float((ratio[-1] - mean_r) / std_r), 2)
    # Ratio keskin oshsa (oltin kumushdan qimmatlashsa), ko'pincha mean-reversion yoki risk-off harakat kuzatiladi
    sig = "HIGH_RATIO" if z >= 1.75 else ("LOW_RATIO" if z <= -1.75 else "NONE")
    return {"ratio": round(float(ratio[-1]), 2), "zscore": z, "signal": sig}


def detect_rsi_divergence(df: pd.DataFrame, period: int = 14) -> tuple[bool, bool]:
    if len(df) < 35:
        return False, False
    delta = df["close"].diff()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean().replace(0, 1e-9)
    rsi = 100.0 - (100.0 / (1.0 + gain / loss))
    win, rwin = df.tail(24), rsi.tail(24)
    bull_div = float(win["low"].iloc[12:].min()) < float(win["low"].iloc[:12].min()) and float(rwin.iloc[12:].min()) > float(rwin.iloc[:12].min()) + 2.0
    bear_div = float(win["high"].iloc[12:].max()) > float(win["high"].iloc[:12].max()) and float(rwin.iloc[12:].max()) < float(rwin.iloc[:12].max()) - 2.0
    return bull_div, bear_div


# ==================== 5. SMC / ICT, QUARTERLY THEORY & LIQUIDITY ====================
def get_quarterly_phase(now_ny: datetime.datetime | None = None) -> str:
    h = (now_ny or datetime.datetime.now(NY_TZ)).hour
    if 18 <= h or h < 0:
        return "Q1-Asia (Accumulation)"
    if 0 <= h < 6:
        return "Q2-London (Manipulation)"
    if 6 <= h < 12:
        return "Q3-NY-AM (Distribution)"
    return "Q4-NY-PM (Continuation)"


def resample_15m_df(df_15m: pd.DataFrame, group_bars: int) -> pd.DataFrame:
    if len(df_15m) < group_bars * 10:
        return pd.DataFrame()
    n = (len(df_15m) // group_bars) * group_bars
    sub = df_15m.iloc[-n:].reset_index(drop=True)
    return sub.groupby(sub.index // group_bars).agg({
        "ts": "first",
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }).reset_index(drop=True)


def get_swings(df: pd.DataFrame, left: int = 3, right: int = 3):
    highs, lows = [], []
    h_arr, l_arr = df["high"].values, df["low"].values
    for i in range(left, len(df) - right):
        if h_arr[i] == max(h_arr[i - left : i + right + 1]):
            highs.append((i, float(h_arr[i])))
        if l_arr[i] == min(l_arr[i - left : i + right + 1]):
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
        ote_top, ote_bot = rng_high - 0.618 * (rng_high - rng_low), rng_high - 0.786 * (rng_high - rng_low)
    else:
        ote_bot, ote_top = rng_low + 0.618 * (rng_high - rng_low), rng_low + 0.786 * (rng_high - rng_low)
    return {
        "bias": bias,
        "event": event,
        "eq": round(eq, 2),
        "ote_bot": round(min(ote_bot, ote_top), 2),
        "ote_top": round(max(ote_bot, ote_top), 2),
    }


def detect_smc_zones_bpr_and_htf(df: pd.DataFrame, df_1h: pd.DataFrame, atr15: float) -> dict:
    curr = float(df["close"].iloc[-1])
    tol = max(0.80, atr15 * 0.35)
    bull_zones, bear_zones = [], []
    bull_fvgs, bear_fvgs = [], []

    for i in range(max(2, len(df) - 30), len(df) - 1):
        c1_h, c1_l = float(df["high"].iloc[i - 2]), float(df["low"].iloc[i - 2])
        c3_h, c3_l = float(df["high"].iloc[i]), float(df["low"].iloc[i])
        sub_low, sub_high = float(df["low"].iloc[i + 1 :].min()), float(df["high"].iloc[i + 1 :].max())
        if c3_l > c1_h:
            bull_fvgs.append((c1_h, c3_l))
            if sub_low > c1_h:
                bull_zones.append(("FVG", c1_h, c3_l))
            elif curr < c1_h:
                bear_zones.append(("IFVG", c1_h, c3_l))
        if c3_h < c1_l:
            bear_fvgs.append((c3_h, c1_l))
            if sub_high < c1_l:
                bear_zones.append(("FVG", c3_h, c1_l))
            elif curr > c1_l:
                bull_zones.append(("IFVG", c3_h, c1_l))

    for b_lo, b_hi in bull_fvgs[-4:]:
        for s_lo, s_hi in bear_fvgs[-4:]:
            ov_lo, ov_hi = max(b_lo, s_lo), min(b_hi, s_hi)
            if ov_hi > ov_lo:
                bull_zones.append(("BPR", ov_lo, ov_hi))
                bear_zones.append(("BPR", ov_lo, ov_hi))

    avg_body = float((df["close"] - df["open"]).abs().tail(30).mean())
    for i in range(max(1, len(df) - 25), len(df) - 2):
        o1, c1 = float(df["open"].iloc[i]), float(df["close"].iloc[i])
        o2, c2 = float(df["open"].iloc[i + 1]), float(df["close"].iloc[i + 1])
        if c1 < o1 and c2 > o2 and (c2 - o2) > avg_body * 1.4 and c2 > float(df["high"].iloc[i]):
            lo, hi = min(o1, c1), max(o1, c1)
            bull_zones.append(("OB", lo, hi)) if curr >= lo - tol else bear_zones.append(("Breaker", lo, hi))
        if c1 > o1 and c2 < o2 and (o2 - c2) > avg_body * 1.4 and c2 < float(df["low"].iloc[i]):
            lo, hi = min(o1, c1), max(o1, c1)
            bear_zones.append(("OB", lo, hi)) if curr <= hi + tol else bull_zones.append(("Breaker", lo, hi))

    in_htf_bull, in_htf_bear = False, False
    for i in range(max(2, len(df_1h) - 20), len(df_1h) - 1):
        c1_h, c1_l = float(df_1h["high"].iloc[i - 2]), float(df_1h["low"].iloc[i - 2])
        c3_h, c3_l = float(df_1h["high"].iloc[i]), float(df_1h["low"].iloc[i])
        if c3_l > c1_h and (c1_h - tol <= curr <= c3_l + tol):
            in_htf_bull = True
        if c3_h < c1_l and (c3_h - tol <= curr <= c1_l + tol):
            in_htf_bear = True

    return {
        "bull_hits": list({k for k, lo, hi in bull_zones if lo - tol <= curr <= hi + tol}),
        "bear_hits": list({k for k, lo, hi in bear_zones if lo - tol <= curr <= hi + tol}),
        "htf_bull": in_htf_bull,
        "htf_bear": in_htf_bear,
    }


def detect_bank_manipulation_x3(df: pd.DataFrame) -> dict:
    if df.empty or len(df) < 10:
        return {"bull": False, "bear": False}
    last4 = df.tail(4)
    o, c = last4["open"].values, last4["close"].values
    return {
        "bull": bool(c[0] < o[0] and c[1] < o[1] and c[2] < o[2] and c[3] > o[3] and c[3] > o[2]),
        "bear": bool(c[0] > o[0] and c[1] > o[1] and c[2] > o[2] and c[3] < o[3] and c[3] < o[2]),
    }


def detect_liquidity_matrix(df_15m: pd.DataFrame, df_1d: pd.DataFrame) -> dict:
    curr = float(df_15m["close"].iloc[-1])
    recent_h, recent_l = float(df_15m["high"].tail(6).max()), float(df_15m["low"].tail(6).min())
    prior = df_15m.iloc[-96:-6]
    pdh, pdl = float(prior["high"].max()), float(prior["low"].min())
    bsl_major = recent_h > pdh and curr < pdh
    ssl_major = recent_l < pdl and curr > pdl

    pwh, pwl, weekly_open = None, None, None
    if len(df_1d) >= 10:
        pwh = float(df_1d["high"].iloc[-8:-1].max())
        pwl = float(df_1d["low"].iloc[-8:-1].min())
        weekly_open = float(df_1d["open"].iloc[-min(5, len(df_1d))])
        if recent_h > pwh and curr < pwh:
            bsl_major = True
        if recent_l < pwl and curr > pwl:
            ssl_major = True

    mid_prior = df_15m.iloc[-36:-4]
    bsl_int = recent_h > float(mid_prior["high"].max()) and curr < float(mid_prior["high"].max())
    ssl_int = recent_l < float(mid_prior["low"].min()) and curr > float(mid_prior["low"].min())

    asia_high, asia_low, ny_open = None, None, None
    if "ts" in df_15m.columns:
        ts_utc = pd.to_datetime(df_15m["ts"].astype(np.int64), unit="ms", utc=True)
        asia_mask = (ts_utc.dt.hour >= 0) & (ts_utc.dt.hour < 6)
        if asia_mask.any():
            asia_df = df_15m[asia_mask].tail(24)
            asia_high, asia_low = float(asia_df["high"].max()), float(asia_df["low"].min())
        ts_ny = ts_utc.dt.tz_convert(NY_TZ)
        ny_mask = (ts_ny.dt.hour == 0) & (ts_ny.dt.minute == 0)
        if ny_mask.any():
            ny_open = float(df_15m[ny_mask].iloc[-1]["open"])

    asia_bsl = bool(asia_high and recent_h > asia_high and curr < asia_high)
    asia_ssl = bool(asia_low and recent_l < asia_low and curr > asia_low)

    return {
        "bsl_sweep": bsl_major or bsl_int or asia_bsl,
        "ssl_sweep": ssl_major or ssl_int or asia_ssl,
        "major_sweep": bsl_major or ssl_major or asia_bsl or asia_ssl,
        "ny_open": round(ny_open, 2) if ny_open else None,
        "weekly_open": round(weekly_open, 2) if weekly_open else None,
    }


def check_5m_ltf_trigger(df_5m: pd.DataFrame, side: str) -> tuple[bool, str]:
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


def get_killzone_info(now_utc_time: datetime.time | None = None) -> tuple[bool, str, int]:
    t = now_utc_time or datetime.datetime.now(datetime.timezone.utc).time()
    if datetime.time(7, 0) <= t <= datetime.time(10, 0):
        return True, "London Killzone", 8
    if datetime.time(12, 0) <= t <= datetime.time(16, 0):
        return True, "NY AM + Silver Bullet", 10
    if datetime.time(18, 0) <= t <= datetime.time(20, 0):
        return True, "NY PM Killzone", 6
    return False, "Off-Killzone", 0


# ==================== 6. ORTOGONAL 6-OMILLI SCORING ENGINE (v28) ====================
def compute_orthogonal_scores(
    curr_raw: float,
    st_1d: dict,
    st_4h: dict,
    st_1h: dict,
    st_15m: dict,
    sweeps: dict,
    zones: dict,
    m30_x3: dict,
    m45_x3: dict,
    ltf_buy: tuple[bool, str],
    ltf_sell: tuple[bool, str],
    ofi: float,
    kalman_vel: float,
    cusum_dir: str,
    cvd_div: str,
    bull_div: bool,
    bear_div: bool,
    dxy_smt: str,
    silver_smt: str,
    eur_smt: str,
    btc_smt: str,
    spx_smt: str,
    us10y_bias: str,
    kz_info: tuple[bool, str, int],
    vp_zscore: float,
    ou_zscore: float,
    perm_entropy: float,
    dfa_alpha: float,
    arrival_burst: float,
    vol_safe: bool,
    spread_ok: bool,
    vpin: float,
    entropy: float,
    adx15: float,
    regime: str,
    news_blocked: bool,
    conf_stats: dict[str, list[int]] | None = None,
) -> tuple[int, int, list[str], list[str]]:
    buy_confs, sell_confs = [], []

    # 1. HTF Alignment (max 20)
    htf_b, htf_s = 0, 0
    for lbl, st, pts in (("1D", st_1d, 6), ("4H", st_4h, 8), ("1H", st_1h, 6)):
        if st["bias"] == "bullish":
            htf_b += pts
            buy_confs.append(f"{lbl} Bull")
        elif st["bias"] == "bearish":
            htf_s += pts
            sell_confs.append(f"{lbl} Bear")
    htf_b, htf_s = min(20, htf_b), min(20, htf_s)

    # 2. Liquidity Sweep & Session Open (max 20)
    liq_b, liq_s = 0, 0
    if sweeps["ssl_sweep"]:
        liq_b += 16 if sweeps["major_sweep"] else 11
        buy_confs.append("SSL/PDL/PWL/Asia Sweep")
    if sweeps["bsl_sweep"]:
        liq_s += 16 if sweeps["major_sweep"] else 11
        sell_confs.append("BSL/PDH/PWH/Asia Sweep")
    if sweeps["ny_open"]:
        if curr_raw < sweeps["ny_open"]:
            liq_b += 4
        elif curr_raw > sweeps["ny_open"]:
            liq_s += 4
    liq_b, liq_s = min(20, liq_b), min(20, liq_s)

    # 3. POI & OTE (max 20)
    poi_b, poi_s = 0, 0
    if zones["bull_hits"]:
        poi_b += 10
        buy_confs.append(f"POI:{'/'.join(zones['bull_hits'][:2])}")
    if zones["bear_hits"]:
        poi_s += 10
        sell_confs.append(f"POI:{'/'.join(zones['bear_hits'][:2])}")
    if zones["htf_bull"]:
        poi_b += 4
        buy_confs.append("1H HTF POI")
    if zones["htf_bear"]:
        poi_s += 4
        sell_confs.append("1H HTF POI")
    if st_15m["ote_bot"] <= curr_raw <= st_15m["ote_top"]:
        if st_15m["bias"] == "bullish":
            poi_b += 6
            buy_confs.append("ICT OTE")
        elif st_15m["bias"] == "bearish":
            poi_s += 6
            sell_confs.append("ICT OTE")
    poi_b, poi_s = min(20, poi_b), min(20, poi_s)

    # 4. LTF Trigger & 15m/30m/45m Shift (max 15)
    ltf_b, ltf_s = 0, 0
    if ltf_buy[0]:
        ltf_b += 10
        buy_confs.append(ltf_buy[1])
    if ltf_sell[0]:
        ltf_s += 10
        sell_confs.append(ltf_sell[1])
    if st_15m["event"] in ("BOS_BULL", "CHoCH_BULL") or m30_x3["bull"] or m45_x3["bull"]:
        ltf_b += 5
        buy_confs.append(st_15m["event"] if st_15m["event"] in ("BOS_BULL", "CHoCH_BULL") else "M30/M45 X3")
    if st_15m["event"] in ("BOS_BEAR", "CHoCH_BEAR") or m30_x3["bear"] or m45_x3["bear"]:
        ltf_s += 5
        sell_confs.append(st_15m["event"] if st_15m["event"] in ("BOS_BEAR", "CHoCH_BEAR") else "M30/M45 X3")
    ltf_b, ltf_s = min(15, ltf_b), min(15, ltf_s)

    # 5. Orderflow, Momentum & OU/DFA/Burst (max 15)
    of_b, of_s = 0, 0
    if ofi > 0.14 and kalman_vel > 0:
        of_b += 7
        buy_confs.append(f"OFI {ofi:+.2f} & Kalman+")
    elif ofi < -0.14 and kalman_vel < 0:
        of_s += 7
        sell_confs.append(f"OFI {ofi:+.2f} & Kalman-")
    if cusum_dir == "BULLISH" or (ou_zscore <= -1.65 and dfa_alpha < 0.52):
        of_b += 4
        buy_confs.append("CUSUM+/OU-Reversion")
    elif cusum_dir == "BEARISH" or (ou_zscore >= 1.65 and dfa_alpha < 0.52):
        of_s += 4
        sell_confs.append("CUSUM-/OU-Reversion")
    if cvd_div == "BULLISH" or bull_div:
        of_b += 4
        buy_confs.append("CVD/RSI Bull Div")
    if cvd_div == "BEARISH" or bear_div:
        of_s += 4
        sell_confs.append("CVD/RSI Bear Div")
    of_b, of_s = min(15, of_b), min(15, of_s)

    # 6. Cross-Asset SMT (DXY, XAG, EUR, BTC, SPX) & Killzone (max 10)
    mac_b, mac_s = 0, 0
    if dxy_smt == "BULLISH":
        mac_b += 4
        buy_confs.append("DXY SMT+")
    elif dxy_smt == "BEARISH":
        mac_s += 4
        sell_confs.append("DXY SMT-")
    if silver_smt == "BULLISH" or eur_smt == "BULLISH" or btc_smt == "BULLISH" or spx_smt == "BULLISH" or us10y_bias == "bearish":
        mac_b += 2
        buy_confs.append("Cross-Asset+")
    if silver_smt == "BEARISH" or eur_smt == "BEARISH" or btc_smt == "BEARISH" or spx_smt == "BEARISH" or us10y_bias == "bullish":
        mac_s += 2
        sell_confs.append("Cross-Asset-")
    kz_active, kz_name, kz_pts = kz_info
    if kz_active:
        mac_b += min(4, kz_pts // 2)
        mac_s += min(4, kz_pts // 2)
        buy_confs.append(kz_name)
        sell_confs.append(kz_name)
    mac_b, mac_s = min(10, mac_b), min(10, mac_s)

    buy_score = htf_b + liq_b + poi_b + ltf_b + of_b + mac_b
    sell_score = htf_s + liq_s + poi_s + ltf_s + of_s + mac_s

    if conf_stats:
        for c in buy_confs:
            w, l = conf_stats.get(c, [0, 0])
            if w + l >= 6:
                buy_score += 2 if (w / (w + l)) >= 0.70 else (-2 if (w / (w + l)) <= 0.35 else 0)
        for c in sell_confs:
            w, l = conf_stats.get(c, [0, 0])
            if w + l >= 6:
                sell_score += 2 if (w / (w + l)) >= 0.70 else (-2 if (w / (w + l)) <= 0.35 else 0)

    buy_pen = 12 if vp_zscore > CFG.max_vwap_zscore else 0
    sell_pen = 12 if vp_zscore < -CFG.max_vwap_zscore else 0
    common_pen = 0
    if not vol_safe:
        common_pen += 14
    if not spread_ok:
        common_pen += 14
    if vpin > CFG.max_vpin:
        common_pen += 8
    if entropy > CFG.max_entropy or perm_entropy > 0.92:
        common_pen += 8
    if arrival_burst >= 4.0:
        # Keskin tick portlashida (slippage xavfi yuqori) qo'shimcha jarima
        common_pen += 6
    if adx15 < CFG.min_adx_trend and regime == "TREND":
        common_pen += 6
    if news_blocked:
        common_pen += 25

    buy_score = min(99, max(0, buy_score - buy_pen - common_pen))
    sell_score = min(99, max(0, sell_score - sell_pen - common_pen))
    return buy_score, sell_score, buy_confs, sell_confs


# ==================== 7. NEWS & EXTERNAL DATA FETCHERS ====================
async def check_news_window(session: aiohttp.ClientSession) -> tuple[bool, str | None]:
    now_ts = time.time()
    if now_ts - _NEWS_CACHE["ts"] > 3600:
        try:
            async with session.get(CFG.news_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8) as r:
                if r.status == 200:
                    data = await r.json(content_type=None)
                    evs = []
                    for item in data:
                        if item.get("country") == "USD" and item.get("impact") == "High" and item.get("date"):
                            dt = datetime.datetime.fromisoformat(item["date"].replace("Z", "+00:00")).astimezone(datetime.timezone.utc)
                            evs.append({"title": item.get("title", "USD News"), "time": dt})
                    _NEWS_CACHE["events"], _NEWS_CACHE["ts"] = evs, now_ts
                else:
                    logger.warning(f"News feed HTTP {r.status}")
                    _NEWS_CACHE["ts"] = now_ts - 3300
        except Exception as e:
            logger.warning(f"News feed xato: {e}")
            _NEWS_CACHE["ts"] = now_ts - 3300

    now_dt = datetime.datetime.now(datetime.timezone.utc)
    for ev in _NEWS_CACHE["events"]:
        diff = (ev["time"] - now_dt).total_seconds() / 60.0
        if -30.0 <= diff <= 30.0:
            return True, f"{ev['title']} ({int(abs(diff))}m)"
    return False, None


async def fetch_bybit_klines(session: aiohttp.ClientSession, sym: str, interval: str, limit: int) -> pd.DataFrame:
    url = f"{CFG.bybit_rest}/v5/market/kline"
    params = {"category": "linear", "symbol": sym, "interval": interval, "limit": limit}
    try:
        async with session.get(url, params=params, timeout=10) as resp:
            if resp.status != 200:
                logger.warning(f"Bybit kline HTTP {resp.status} ({sym} {interval})")
                return pd.DataFrame()
            data = await resp.json()
            if data.get("retCode") not in (0, None):
                logger.warning(f"Bybit kline retCode={data.get('retCode')}: {data.get('retMsg')}")
                return pd.DataFrame()
            rows = data.get("result", {}).get("list", [])
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame(list(reversed(rows)), columns=["ts", "open", "high", "low", "close", "volume", "turnover"])
            for col in ("open", "high", "low", "close", "volume"):
                df[col] = df[col].astype(float)
            return df.iloc[:-1].reset_index(drop=True) if len(df) > 2 else df
    except Exception as e:
        logger.warning(f"Bybit kline fetch xato ({sym} {interval}): {e}")
        return pd.DataFrame()


async def fetch_yahoo_15m(session: aiohttp.ClientSession, ticker: str) -> pd.DataFrame:
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?interval=15m&range=5d"
    try:
        async with session.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=8) as resp:
            if resp.status != 200:
                logger.debug(f"Yahoo {ticker} HTTP {resp.status}")
                return pd.DataFrame()
            data = await resp.json()
            q = data["chart"]["result"][0]["indicators"]["quote"][0]
            return pd.DataFrame({
                "open": q["open"],
                "high": q["high"],
                "low": q["low"],
                "close": q["close"],
                "volume": q.get("volume", [0] * len(q["close"])),
            }).dropna().tail(80).reset_index(drop=True)
    except Exception as e:
        logger.debug(f"Yahoo {ticker} fetch xato: {e}")
        return pd.DataFrame()


async def get_macro_data(session: aiohttp.ClientSession) -> dict:
    if time.time() - _MACRO_CACHE["ts"] > 120:
        dxy, tnx, vix_df, spx_df = await asyncio.gather(
            fetch_yahoo_15m(session, "DX-Y.NYB"),
            fetch_yahoo_15m(session, "^TNX"),
            fetch_yahoo_15m(session, "^VIX"),
            fetch_yahoo_15m(session, "^GSPC"),
        )
        vix_val = round(float(vix_df["close"].iloc[-1]), 2) if not vix_df.empty else 0.0
        _MACRO_CACHE.update({"ts": time.time(), "df_dxy": dxy, "df_tnx": tnx, "vix": vix_val, "df_spx": spx_df})
    return _MACRO_CACHE


# ==================== 8. MASTER v28 ANALYZER (20$ REAL BALANCE GUARD) ====================
async def run_unified_v28_analysis(session: aiohttp.ClientSession) -> dict | None:
    mkt_open = is_forex_market_open()
    now_ts = time.time()

    df_5m, df_15m, df_1h, df_4h, df_1d, df_xag, df_eur, df_btc = await asyncio.gather(
        fetch_bybit_klines(session, CFG.symbol, "5", 160),
        fetch_bybit_klines(session, CFG.symbol, "15", 200),
        fetch_bybit_klines(session, CFG.symbol, "60", 120),
        fetch_bybit_klines(session, CFG.symbol, "240", 90),
        fetch_bybit_klines(session, CFG.symbol, "D", 45),
        fetch_bybit_klines(session, CFG.silver_symbol, "15", 80),
        fetch_bybit_klines(session, CFG.eur_symbol, "15", 60),
        fetch_bybit_klines(session, CFG.btc_symbol, "15", 60),
    )
    if df_15m.empty or df_1h.empty or df_4h.empty or df_5m.empty:
        return None

    macro, (news_blocked, news_title) = await asyncio.gather(
        get_macro_data(session),
        check_news_window(session),
    )

    closed_bar_close = float(df_5m["close"].iloc[-1])
    closed_bar_ts = int(df_5m["ts"].iloc[-1]) if "ts" in df_5m.columns else 0
    curr_raw = MARKET_STATE["bybit_raw_price"] or closed_bar_close
    closed_bar_ts = int(df_5m["ts"].iloc[-1]) if "ts" in df_5m.columns else 0

    async with STATE_LOCK:
        MARKET_STATE["market_open"] = mkt_open
        MARKET_STATE["bybit_raw_price"] = round(curr_raw, 2)
        MARKET_STATE["last_closed_bar_ts"] = closed_bar_ts
        if MARKET_STATE["trading_paused"] and MARKET_STATE["pause_until_ts"] > 0 and now_ts >= MARKET_STATE["pause_until_ts"]:
            MARKET_STATE["trading_paused"] = False
            MARKET_STATE["pause_until_ts"] = 0.0

    await run_auto_sync_once(session)

    broker_price = to_broker_price(curr_raw)
    spread_ok, spread_val = check_live_spread_guard()
    half_sp = round(spread_val / 2.0, 3)

    wb = MARKET_STATE["windows_bridge"]
    if wb["last_seen"] and (now_ts - wb["last_seen"]) <= CFG.bridge_fresh_sec and wb["mt5_ask"] > wb["mt5_bid"] > 0:
        broker_bid, broker_ask = round(wb["mt5_bid"], 2), round(wb["mt5_ask"], 2)
    else:
        broker_bid = round(broker_price - half_sp, 2)
        broker_ask = round(broker_price + half_sp, 2)

    bar_low_broker = to_broker_price(float(df_5m["low"].iloc[-1]))
    bar_high_broker = to_broker_price(float(df_5m["high"].iloc[-1]))

    st_5m = analyze_smc_structure(df_5m)
    ofi, cvd_div = compute_hybrid_ofi_and_cvd(df_15m)
    await update_open_signals_status(session, broker_bid, broker_ask, bar_low_broker, bar_high_broker, st_5m, ofi)

    tr = np.maximum(
        df_15m["high"] - df_15m["low"],
        np.maximum(abs(df_15m["high"] - df_15m["close"].shift(1)), abs(df_15m["low"] - df_15m["close"].shift(1))),
    )
    atr15 = float(tr.tail(14).mean())

    closes_15m = df_15m["close"].values
    kalman = calc_kalman_filter(closes_15m)
    hurst, regime, entropy = calc_hurst_and_entropy(closes_15m)
    perm_ent = calc_permutation_entropy(closes_15m, order=3)
    dfa_alpha = calc_dfa_alpha(closes_15m)
    ou = calc_ornstein_uhlenbeck(closes_15m)
    amihud = calc_amihud_illiquidity(df_15m)
    xau_xag = calc_gold_silver_ratio(df_15m, df_xag)
    vol_guard = calc_volatility_and_tail_guard(df_15m)
    adx15 = calc_adx(df_15m)
    cusum = calc_cusum_filter(df_15m, atr15)
    vp = compute_vp_and_vwap_zscore(df_15m)
    bull_div, bear_div = detect_rsi_divergence(df_15m)

    st_1h = analyze_smc_structure(df_1h)
    st_4h = analyze_smc_structure(df_4h)
    st_1d = analyze_smc_structure(df_1d) if not df_1d.empty else {"bias": "neutral"}
    zones = detect_smc_zones_bpr_and_htf(df_15m, df_1h, atr15)
    sweeps = detect_liquidity_matrix(df_15m, df_1d)

    m30_x3 = detect_bank_manipulation_x3(resample_15m_df(df_15m, 2))
    m45_x3 = detect_bank_manipulation_x3(resample_15m_df(df_15m, 3))
    ltf_buy = check_5m_ltf_trigger(df_5m, "BUY")
    ltf_sell = check_5m_ltf_trigger(df_5m, "SELL")

    dxy_smt = detect_cross_smt(df_15m, macro["df_dxy"], inverse=True)
    silver_smt = detect_cross_smt(df_15m, df_xag, inverse=False)
    eur_smt = detect_cross_smt(df_15m, df_eur, inverse=False)
    btc_smt = detect_cross_smt(df_15m, df_btc, inverse=False)
    spx_smt = detect_cross_smt(df_15m, macro["df_spx"], inverse=False)
    us10y_bias = analyze_smc_structure(macro["df_tnx"])["bias"] if not macro["df_tnx"].empty else "neutral"
    kz_info = get_killzone_info()
    q_phase = get_quarterly_phase()

    stats = await get_stats_and_psychology()
    buy_score, sell_score, buy_confs, sell_confs = compute_orthogonal_scores(
        curr_raw=closed_bar_close,
        st_1d=st_1d,
        st_4h=st_4h,
        st_1h=st_1h,
        st_15m=st_5m,
        sweeps=sweeps,
        zones=zones,
        m30_x3=m30_x3,
        m45_x3=m45_x3,
        ltf_buy=ltf_buy,
        ltf_sell=ltf_sell,
        ofi=ofi,
        kalman_vel=kalman["velocity"],
        cusum_dir=cusum["direction"],
        cvd_div=cvd_div,
        bull_div=bull_div,
        bear_div=bear_div,
        dxy_smt=dxy_smt,
        silver_smt=silver_smt,
        eur_smt=eur_smt,
        btc_smt=btc_smt,
        spx_smt=spx_smt,
        us10y_bias=us10y_bias,
        kz_info=kz_info,
        vp_zscore=vp["zscore"],
        ou_zscore=ou["zscore"],
        perm_entropy=perm_ent,
        dfa_alpha=dfa_alpha,
        arrival_burst=MARKET_STATE["arrival_burst"],
        vol_safe=vol_guard["safe"],
        spread_ok=spread_ok,
        vpin=MARKET_STATE["vpin"],
        entropy=entropy,
        adx15=adx15,
        regime=regime,
        news_blocked=news_blocked,
        conf_stats=stats["conf_stats"],
    )

    async with STATE_LOCK:
        MARKET_STATE.update({
            "broker_price": broker_price,
            "exness_price": to_broker_price(curr_raw, "exness"),
            "xm_price": to_broker_price(curr_raw, "xm"),
            "last_price": broker_price,
            "ofi": ofi,
            "amihud_illiq": amihud,
            "perm_entropy": perm_ent,
            "dfa_alpha": dfa_alpha,
            "ou_halflife": ou["halflife"],
            "ou_zscore": ou["zscore"],
            "xau_xag_ratio": xau_xag["ratio"],
            "xau_xag_zscore": xau_xag["zscore"],
            "hurst": hurst,
            "entropy": entropy,
            "regime": regime,
            "kalman_price": to_broker_price(kalman["price"]),
            "kalman_velocity": kalman["velocity"],
            "garch_vol": vol_guard["garch_pts"],
            "kurtosis": vol_guard["kurtosis"],
            "vwap_zscore": vp["zscore"],
            "cusum_dir": cusum["direction"],
            "cvd_div": cvd_div,
            "dxy_smt": dxy_smt,
            "silver_smt": silver_smt,
            "eur_smt": eur_smt,
            "btc_smt": btc_smt,
            "spx_smt": spx_smt,
            "us10y_bias": us10y_bias,
            "vix_level": macro["vix"],
            "quarter_phase": q_phase,
            "poc": to_broker_price(vp["poc"]),
            "vwap": to_broker_price(vp["vwap"]),
            "news_blocked": news_blocked,
            "news_title": news_title,
            "last_score_buy": buy_score,
            "last_score_sell": sell_score,
            "last_confluences": buy_confs if buy_score >= sell_score else sell_confs,
        })

    if MARKET_STATE["trading_paused"]:
        return None
    if not mkt_open or stats["circuit_broken"] or news_blocked or not spread_ok or MARKET_STATE["offset_anomaly"]:
        return None

    # Maksimal ochiq pozitsiyalar soni (20$ balans uchun odatda 1 ta)
    bridge_pos = int(wb.get("open_positions") or 0)
    active_pos_count = max(bridge_pos, int(stats["open_db_cnt"]))
    if active_pos_count >= CFG.max_open_positions:
        return None

    req_score = CFG.min_signal_score + (6 if stats["anti_tilt_active"] else 0)
    side, final_score, confs = None, 0, []
    if buy_score >= req_score and buy_score > sell_score and ltf_buy[0]:
        side, final_score, confs = "BUY", buy_score, buy_confs
    elif sell_score >= req_score and sell_score > buy_score and ltf_sell[0]:
        side, final_score, confs = "SELL", sell_score, sell_confs

    if not side:
        return None

    sl_dist = round(max(CFG.sl_min_pts, min(CFG.sl_max_pts, max(atr15 * 1.20, vol_guard["garch_pts"] * 1.5))), 2)
    bal_usd = get_effective_balance_usd()
    pos_sizing = calc_position_size_20usd(
        balance_usd=bal_usd,
        risk_pct=CFG.risk_per_trade_pct,
        sl_points=sl_dist,
        anti_tilt=stats["anti_tilt_active"],
    )
    if not pos_sizing["allowed"]:
        logger.info(f"20$ Risk Guard signalni o'tkazib yubordi: {pos_sizing['reason']}")
        return None

    entry = broker_ask if side == "BUY" else broker_bid
    if side == "BUY":
        sl = round(entry - sl_dist, 2)
        tp1 = round(entry + sl_dist * 2.0, 2)
        tp2 = round(entry + sl_dist * 3.25, 2)
        tp3 = round(entry + sl_dist * 5.0, 2)
    else:
        sl = round(entry + sl_dist, 2)
        tp1 = round(entry - sl_dist * 2.0, 2)
        tp2 = round(entry - sl_dist * 3.25, 2)
        tp3 = round(entry - sl_dist * 5.0, 2)

    win_prob = empirical_bayes_win_prob(final_score, stats["wins"], stats["losses"], stats["bucket_stats"])
    rec_risk = calc_quarter_kelly(win_prob, rr_ratio=3.25, anti_tilt=stats["anti_tilt_active"])

    return {
        "id": f"APEX28-{int(now_ts)}-{side}",
        "created_at": datetime.datetime.now(UZT).strftime("%Y-%m-%d %H:%M:%S UZT"),
        "symbol": "XAUUSD",
        "broker": MARKET_STATE["active_broker"].upper(),
        "account_mode": CFG.account_mode.upper(),
        "balance_usd": bal_usd,
        "lot": pos_sizing["lot"],
        "est_loss_usd": pos_sizing["est_loss_usd"],
        "bybit_raw": round(curr_raw, 2),
        "offset": MARKET_STATE["price_offset"],
        "side": side,
        "entry": entry,
        "entry_exness": to_broker_price(curr_raw, "exness"),
        "entry_xm": to_broker_price(curr_raw, "xm"),
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "sl_points": sl_dist,
        "score": final_score,
        "win_prob": win_prob,
        "regime": regime,
        "hurst": hurst,
        "dfa_alpha": dfa_alpha,
        "perm_entropy": perm_ent,
        "ou_halflife": ou["halflife"],
        "ou_zscore": ou["zscore"],
        "kyles_lambda": MARKET_STATE["kyles_lambda"],
        "kalman_price": MARKET_STATE["kalman_price"],
        "kalman_vel": kalman["velocity"],
        "vpin": MARKET_STATE["vpin"],
        "ofi": ofi,
        "spread": spread_val,
        "dxy_smt": dxy_smt,
        "silver_smt": silver_smt,
        "spx_smt": spx_smt,
        "xau_xag_ratio": xau_xag["ratio"],
        "poc": MARKET_STATE["poc"],
        "vwap": MARKET_STATE["vwap"],
        "killzone": kz_info[1],
        "quarter_phase": q_phase,
        "risk_pct": rec_risk,
        "confluences": confs,
        "status": "ACTIVE",
        "timestamp": int(now_ts),
    }


# ==================== 9. WALK-FORWARD BACKTEST ENGINE (v28) ====================
def run_walkforward_backtest(df_15m: pd.DataFrame, df_1h: pd.DataFrame, df_4h: pd.DataFrame, spread_pts: float = 0.25) -> dict:
    if len(df_15m) < 220 or len(df_1h) < 60 or len(df_4h) < 30:
        return {"error": "Backtest uchun barlar soni yetarli emas"}

    trades = []
    open_trade = None
    last_sig_idx = -999

    for i in range(180, len(df_15m) - 1):
        bar = df_15m.iloc[i]
        next_bar = df_15m.iloc[i + 1]
        hi, lo = float(next_bar["high"]), float(next_bar["low"])

        if open_trade:
            if open_trade["side"] == "BUY":
                if lo <= open_trade["sl"]:
                    trades.append(0.75 if open_trade["partial"] else (0.0 if open_trade["be"] else -1.0))
                    open_trade = None
                elif hi >= open_trade["tp3"]:
                    trades.append(5.0)
                    open_trade = None
                elif hi >= open_trade["tp2"]:
                    trades.append(3.25)
                    open_trade = None
                elif hi >= open_trade["tp1"] and not open_trade["be"]:
                    open_trade["be"] = True
                    open_trade["partial"] = True
                    open_trade["sl"] = round(open_trade["entry"] + CFG.be_buffer_pts, 2)
            else:
                if hi >= open_trade["sl"]:
                    trades.append(0.75 if open_trade["partial"] else (0.0 if open_trade["be"] else -1.0))
                    open_trade = None
                elif lo <= open_trade["tp3"]:
                    trades.append(5.0)
                    open_trade = None
                elif lo <= open_trade["tp2"]:
                    trades.append(3.25)
                    open_trade = None
                elif lo <= open_trade["tp1"] and not open_trade["be"]:
                    open_trade["be"] = True
                    open_trade["partial"] = True
                    open_trade["sl"] = round(open_trade["entry"] - CFG.be_buffer_pts, 2)
            continue

        if i - last_sig_idx < 8:
            continue

        sub_15 = df_15m.iloc[i - 180 : i + 1].reset_index(drop=True)
        cur_ts = int(bar["ts"]) if "ts" in df_15m.columns else 0
        sub_1h = df_1h[df_1h["ts"].astype(np.int64) <= cur_ts].tail(120).reset_index(drop=True) if cur_ts else df_1h
        sub_4h = df_4h[df_4h["ts"].astype(np.int64) <= cur_ts].tail(90).reset_index(drop=True) if cur_ts else df_4h
        if len(sub_1h) < 30 or len(sub_4h) < 20:
            continue

        tr = np.maximum(
            sub_15["high"] - sub_15["low"],
            np.maximum(abs(sub_15["high"] - sub_15["close"].shift(1)), abs(sub_15["low"] - sub_15["close"].shift(1))),
        )
        atr15 = float(tr.tail(14).mean())
        closes = sub_15["close"].values
        kalman = calc_kalman_filter(closes)
        hurst, regime, entropy = calc_hurst_and_entropy(closes)
        perm_ent = calc_permutation_entropy(closes, order=3)
        dfa_alpha = calc_dfa_alpha(closes)
        ou = calc_ornstein_uhlenbeck(closes)
        vol_g = calc_volatility_and_tail_guard(sub_15)
        adx15 = calc_adx(sub_15)
        cusum = calc_cusum_filter(sub_15, atr15)
        vp = compute_vp_and_vwap_zscore(sub_15)
        b_div, s_div = detect_rsi_divergence(sub_15)
        ofi, cvd_div = compute_hybrid_ofi_and_cvd(sub_15)

        st_15 = analyze_smc_structure(sub_15)
        st_1h = analyze_smc_structure(sub_1h)
        st_4h = analyze_smc_structure(sub_4h)
        zones = detect_smc_zones_bpr_and_htf(sub_15, sub_1h, atr15)
        sweeps = detect_liquidity_matrix(sub_15, pd.DataFrame())
        m30_x3 = detect_bank_manipulation_x3(resample_15m_df(sub_15, 2))
        m45_x3 = detect_bank_manipulation_x3(resample_15m_df(sub_15, 3))

        ltf_b = (st_15["event"] in ("BOS_BULL", "CHoCH_BULL"), st_15["event"])
        ltf_s = (st_15["event"] in ("BOS_BEAR", "CHoCH_BEAR"), st_15["event"])

        ts_dt = datetime.datetime.fromtimestamp(cur_ts / 1000.0, tz=datetime.timezone.utc) if cur_ts else None
        kz = get_killzone_info(ts_dt.time() if ts_dt else None)
        curr_c = float(bar["close"])

        bs, ss, _, _ = compute_orthogonal_scores(
            curr_raw=curr_c,
            st_1d={"bias": "neutral"},
            st_4h=st_4h,
            st_1h=st_1h,
            st_15m=st_15,
            sweeps=sweeps,
            zones=zones,
            m30_x3=m30_x3,
            m45_x3=m45_x3,
            ltf_buy=ltf_b,
            ltf_sell=ltf_s,
            ofi=ofi,
            kalman_vel=kalman["velocity"],
            cusum_dir=cusum["direction"],
            cvd_div=cvd_div,
            bull_div=b_div,
            bear_div=s_div,
            dxy_smt="NONE",
            silver_smt="NONE",
            eur_smt="NONE",
            btc_smt="NONE",
            spx_smt="NONE",
            us10y_bias="neutral",
            kz_info=kz,
            vp_zscore=vp["zscore"],
            ou_zscore=ou["zscore"],
            perm_entropy=perm_ent,
            dfa_alpha=dfa_alpha,
            arrival_burst=1.0,
            vol_safe=vol_g["safe"],
            spread_ok=(spread_pts <= CFG.max_spread_pts),
            vpin=0.25,
            entropy=entropy,
            adx15=adx15,
            regime=regime,
            news_blocked=False,
        )

        side = "BUY" if bs >= CFG.min_signal_score and bs > ss and ltf_b[0] else (
            "SELL" if ss >= CFG.min_signal_score and ss > bs and ltf_s[0] else None
        )
        if not side:
            continue

        sl_dist = round(max(CFG.sl_min_pts, min(CFG.sl_max_pts, max(atr15 * 1.20, vol_g["garch_pts"] * 1.5))), 2)
        entry = round(curr_c + (spread_pts / 2.0 if side == "BUY" else -spread_pts / 2.0), 2)
        open_trade = {
            "side": side,
            "entry": entry,
            "sl": round(entry - sl_dist, 2) if side == "BUY" else round(entry + sl_dist, 2),
            "tp1": round(entry + sl_dist * 2.0, 2) if side == "BUY" else round(entry - sl_dist * 2.0, 2),
            "tp2": round(entry + sl_dist * 3.25, 2) if side == "BUY" else round(entry - sl_dist * 3.25, 2),
            "tp3": round(entry + sl_dist * 5.0, 2) if side == "BUY" else round(entry - sl_dist * 5.0, 2),
            "be": False,
            "partial": False,
        }
        last_sig_idx = i

    if not trades:
        return {"trades": 0, "winrate": 0.0, "net_r": 0.0, "expectancy_r": 0.0, "max_dd_r": 0.0, "profit_factor": 0.0}

    wins = [t for t in trades if t > 0]
    losses = [t for t in trades if t < 0]
    eq = np.cumsum(trades)
    peak = np.maximum.accumulate(eq)
    max_dd = float(np.max(peak - eq)) if len(eq) else 0.0
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    pf = round(gross_win / gross_loss, 2) if gross_loss > 0 else float("inf")
    decided = len(wins) + len(losses)
    return {
        "trades": len(trades),
        "wins": len(wins),
        "be": sum(1 for t in trades if t == 0.0),
        "losses": len(losses),
        "winrate": round((len(wins) / decided) * 100.0, 1) if decided else 0.0,
        "net_r": round(float(sum(trades)), 2),
        "expectancy_r": round(float(np.mean(trades)), 3),
        "max_dd_r": round(max_dd, 2),
        "profit_factor": pf,
    }


# ==================== 10. BYBIT WS & HEALTH MONITOR ====================
async def bybit_ws_worker(app):
    global WS_ALERT_SENT
    while True:
        try:
            async with app["session"].ws_connect(CFG.bybit_ws, heartbeat=20) as ws:
                await ws.send_json({"op": "subscribe", "args": [f"orderbook.50.{CFG.symbol}", f"publicTrade.{CFG.symbol}"]})
                WS_ALERT_SENT = False
                logger.info(f"Bybit L2 & Tick WS ulandi: {CFG.symbol}")
                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        payload = json.loads(msg.data)
                        topic = payload.get("topic", "")
                        if topic.startswith("orderbook."):
                            d = payload.get("data", {})
                            async with ORDERBOOK_LOCK:
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
                                    l2_ofi, b_wall, a_wall = compute_l2_metrics()
                                    b_wall_b = to_broker_price(b_wall)
                                    a_wall_b = to_broker_price(a_wall)
                                    now_w = time.time()
                                    async with STATE_LOCK:
                                        MARKET_STATE.update({
                                            "bid": bb,
                                            "ask": ba,
                                            "spread": round(ba - bb, 3),
                                            "l2_ofi": l2_ofi,
                                            "bid_wall": b_wall_b,
                                            "ask_wall": a_wall_b,
                                            "last_ws_ts": now_w,
                                        })
                        elif topic.startswith("publicTrade."):
                            for t in payload.get("data", []):
                                raw_p = float(t["p"])
                                qty_v = float(t["v"])
                                side_s = str(t["S"])
                                update_tick_microstructure(side_s, raw_p, qty_v)
                                brk_p = to_broker_price(raw_p)
                                async with STATE_LOCK:
                                    MARKET_STATE["bybit_raw_price"] = round(raw_p, 2)
                                    MARKET_STATE["last_price"] = brk_p
        except Exception as e:
            logger.warning(f"Bybit WS qayta ulanmoqda: {e}")
            if not WS_ALERT_SENT and is_forex_market_open():
                WS_ALERT_SENT = True
                await tg_send(app["session"], CFG.admin_chat_id, "⚠️ Bybit WebSocket uzildi, qayta ulanmoqda...")
            await asyncio.sleep(3)


# ==================== 11. TELEGRAM BOT & DAILY REPORT ====================
async def tg_send(session: aiohttp.ClientSession, chat_id: str, text: str) -> bool:
    if not CFG.bot_token or not chat_id:
        return False
    try:
        async with session.post(
            f"https://api.telegram.org/bot{CFG.bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": text, "parse_mode": "HTML"},
            timeout=8,
        ) as resp:
            if resp.status != 200:
                body = await resp.text()
                logger.warning(f"Telegram sendMessage HTTP {resp.status}: {body[:200]}")
                return False
            return True
    except Exception as e:
        logger.warning(f"Telegram sendMessage xato: {e}")
        return False


async def send_signal_alert(session: aiohttp.ClientSession, sig: dict) -> bool:
    icon = "🟢" if sig["side"] == "BUY" else "🔴"
    confs_str = " • ".join(sig["confluences"][:8])
    text = (
        f"{icon} <b>APEX v28 SIGNAL (XAUUSD — {sig['broker']})</b>
"
        f"━━━━━━━━━━━━━━━━━━━━━━
"
        f"📌 <b>Yo'nalish:</b> {sig['side']} | <b>Ball:</b> {sig['score']}/100 (Bayes Win: {sig['win_prob']}%)
"
        f"💼 <b>20$ Balans (${sig['balance_usd']:.2f}):</b> Lot <code>{sig['lot']:.2f}</code> | Max Risk: <code>${sig['est_loss_usd']:.2f}</code> ({sig['risk_pct']}%)
"
        f"🎯 <b>Entry ({sig['broker']}):</b> <code>{sig['entry']}</code>
"
        f"💱 <i>Exness: {sig['entry_exness']} | XM: {sig['entry_xm']} | Offset: {sig['offset']:+.2f}</i>
"
        f"🛑 <b>Stop-Loss:</b> <code>{sig['sl']}</code> ({sig['sl_points']}$)
"
        f"✅ <b>TP1 (50% + BE):</b> <code>{sig['tp1']}</code>
"
        f"✅ <b>TP2 (3.25R):</b> <code>{sig['tp2']}</code>
"
        f"✅ <b>TP3 (5.0R):</b> <code>{sig['tp3']}</code>
"
        f"━━━━━━━━━━━━━━━━━━━━━━
"
        f"🧠 <b>Rejim:</b> {sig['regime']} (H={sig['hurst']} | DFA={sig['dfa_alpha']} | PE={sig['perm_entropy']})
"
        f"🌊 <b>OFI:</b> {sig['ofi']:+.3f} | <b>VPIN:</b> {sig['vpin']:.3f} | <b>Kyle λ:</b> {sig['kyles_lambda']:.4f}
"
        f"📉 <b>OU HL:</b> {sig['ou_halflife']} (Z={sig['ou_zscore']:+.2f}) | <b>XAU/XAG:</b> {sig['xau_xag_ratio']}
"
        f"🌐 <b>SMT:</b> DXY={sig['dxy_smt']} | XAG={sig['silver_smt']} | SPX={sig['spx_smt']}
"
        f"🧩 <b>Tasdiqlar:</b> <i>{confs_str}</i>
"
        f"⏰ <b>Oyna:</b> {sig['killzone']} ({sig['quarter_phase']})"
    )
    return await tg_send(session, CFG.admin_chat_id, text)


def format_live_status(stats: dict) -> str:
    wb = MARKET_STATE["windows_bridge"]
    bridge_ok = (time.time() - wb["last_seen"]) < CFG.bridge_fresh_sec if wb["last_seen"] else False
    b_icon = "🟢 ONLINE" if bridge_ok else "🔴 OFFLINE"
    confs = ", ".join(MARKET_STATE["last_confluences"][:6]) or "Kutilmoqda..."
    news_str = f"🛑 {MARKET_STATE['news_title']}" if MARKET_STATE["news_blocked"] else "✅ Xavfsiz"
    mkt_str = "🟢 OCHIQ" if MARKET_STATE["market_open"] else "🔴 YOPIQ"
    brk = MARKET_STATE["active_broker"].upper()
    pause_str = "⏸️ PAUZA" if MARKET_STATE["trading_paused"] else ("🛑 CIRCUIT" if stats["circuit_broken"] else "🟢 AKTIV")
    eff_bal = get_effective_balance_usd()
    return (
        f"📡 <b>APEX v28 JONLI ANALIZ (XAUUSD — {brk})</b>
"
        f"━━━━━━━━━━━━━━━━━━━━━━
"
        f"🏛️ <b>Bozor:</b> {mkt_str} | <b>Holat:</b> {pause_str}
"
        f"💰 <b>{brk}:</b> <code>{MARKET_STATE['last_price']}</code> | <b>Exness:</b> {MARKET_STATE['exness_price']} | <b>XM:</b> {MARKET_STATE['xm_price']}
"
        f"🔄 <b>Bybit Raw:</b> {MARKET_STATE['bybit_raw_price']} | <b>Offset:</b> {MARKET_STATE['price_offset']:+.2f}$ ({MARKET_STATE['sync_source']})
"
        f"📈 <b>Hurst:</b> {MARKET_STATE['hurst']} ({MARKET_STATE['regime']}) | <b>DFA:</b> {MARKET_STATE['dfa_alpha']} | <b>PE:</b> {MARKET_STATE['perm_entropy']}
"
        f"🌊 <b>OFI:</b> {MARKET_STATE['ofi']:+.3f} | <b>VPIN:</b> {MARKET_STATE['vpin']:.3f} | <b>Kyle λ:</b> {MARKET_STATE['kyles_lambda']:.4f}
"
        f"📉 <b>OU Z:</b> {MARKET_STATE['ou_zscore']:+.2f} (HL={MARKET_STATE['ou_halflife']}) | <b>Amihud:</b> {MARKET_STATE['amihud_illiq']:.4f}
"
        f"🌐 <b>SMT:</b> DXY={MARKET_STATE['dxy_smt']} | XAG={MARKET_STATE['silver_smt']} | SPX={MARKET_STATE['spx_smt']} | VIX={MARKET_STATE['vix_level']}
"
        f"📊 <b>XAU/XAG:</b> {MARKET_STATE['xau_xag_ratio']} | <b>News:</b> {news_str}
"
        f"🎯 <b>Ball:</b> BUY <b>{MARKET_STATE['last_score_buy']}</b> / SELL <b>{MARKET_STATE['last_score_sell']}</b> (Min: {CFG.min_signal_score})
"
        f"🧩 <b>Omillar:</b> <i>{confs}</i>
"
        f"━━━━━━━━━━━━━━━━━━━━━━
"
        f"💼 <b>Balans:</b> <code>${eff_bal:.2f}</code> ({CFG.account_mode.upper()}) | <b>Bugun:</b> <code>{stats['daily_r']:+.2f}R</code> (<code>${stats['daily_usd']:+.2f}</code>)
"
        f"📊 <b>Bayes:</b> {stats['wins']}W / {stats['be_wins']}BE / {stats['losses']}L | <b>MT5:</b> {b_icon} | <b>Ochiq:</b> {wb['open_positions']}"
    )


async def telegram_polling_worker(app):
    global LAST_UPDATE_ID
    if not CFG.bot_token:
        return
    if not CFG.admin_chat_id:
        logger.warning("ADMIN_CHAT_ID bo'sh — Telegram buyruqlar faqat ADMIN_CHAT_ID bilan ishlaydi.")
        return

    url = f"https://api.telegram.org/bot{CFG.bot_token}/getUpdates"
    while True:
        try:
            async with app["session"].get(url, params={"offset": LAST_UPDATE_ID + 1, "timeout": 25}, timeout=30) as r:
                if r.status == 200:
                    data = await r.json()
                    for upd in data.get("result", []):
                        LAST_UPDATE_ID = upd["update_id"]
                        msg = upd.get("message", {})
                        chat_id = str(msg.get("chat", {}).get("id", ""))
                        if chat_id != CFG.admin_chat_id:
                            continue
                        raw_txt = (msg.get("text") or "").strip()
                        txt = raw_txt.lower()
                        if txt in ("/start", "/status", "/analyze", "/stats", "/balance"):
                            await run_unified_v28_analysis(app["session"])
                            stats = await get_stats_and_psychology()
                            await tg_send(app["session"], chat_id, format_live_status(stats))
                        elif txt == "/pause":
                            async with STATE_LOCK:
                                MARKET_STATE["trading_paused"] = True
                                MARKET_STATE["pause_until_ts"] = time.time() + 14400
                            await tg_send(app["session"], chat_id, "⏸️ Savdo signallari 4 soatga vaqtincha to'xtatildi. Davom ettirish: <code>/resume</code>")
                        elif txt == "/resume":
                            async with STATE_LOCK:
                                MARKET_STATE["trading_paused"] = False
                                MARKET_STATE["pause_until_ts"] = 0.0
                            await tg_send(app["session"], chat_id, "▶️ Savdo signallari qayta faollashtirildi.")
                        elif txt == "/sync":
                            info = await run_auto_sync_once(app["session"], force=True)
                            txt_s = (
                                f"🔄 <b>Sinxronizatsiya ({info['source']})</b>
"
                                f"Bybit: <code>{info['bybit_raw']}</code>
"
                                f"Exness: <code>{info['exness']}</code> | XM: <code>{info['xm']}</code>
"
                                f"Offset: <code>{info['offset']:+.2f}$</code>"
                            ) if info else "⚠️ Spot narx hozircha olinmadi."
                            await tg_send(app["session"], chat_id, txt_s)
                        elif txt.startswith("/broker"):
                            parts = txt.split()
                            if len(parts) >= 2 and parts[1] in ("exness", "xm"):
                                async with STATE_LOCK:
                                    MARKET_STATE["active_broker"] = parts[1]
                                await run_auto_sync_once(app["session"], force=True)
                                await tg_send(
                                    app["session"],
                                    chat_id,
                                    f"✅ Aktiv broker: <b>{parts[1].upper()}</b> (Offset: {MARKET_STATE['price_offset']:+.2f}$)",
                                )
                            else:
                                await tg_send(app["session"], chat_id, "ℹ️ <code>/broker exness</code> yoki <code>/broker xm</code>")
                        elif txt.startswith("/offset"):
                            parts = raw_txt.split()
                            if len(parts) >= 2:
                                try:
                                    val = float(parts[1])
                                    async with STATE_LOCK:
                                        MARKET_STATE["price_offset"] = round(val, 2)
                                        MARKET_STATE["offset_anomaly"] = False
                                    await tg_send(app["session"], chat_id, f"✅ Qo'lda offset: <code>{val:+.2f}$</code>")
                                except ValueError:
                                    await tg_send(app["session"], chat_id, "⚠️ Misol: <code>/offset -8.20</code>")
                        elif txt == "/news":
                            blocked, title = await check_news_window(app["session"])
                            await tg_send(app["session"], chat_id, f"🛑 News: {title}" if blocked else "✅ Hozir USD High-Impact yangiliklar yo'q.")
                        elif txt == "/backtest":
                            df15, df1h, df4h = await asyncio.gather(
                                fetch_bybit_klines(app["session"], CFG.symbol, "15", 500),
                                fetch_bybit_klines(app["session"], CFG.symbol, "60", 200),
                                fetch_bybit_klines(app["session"], CFG.symbol, "240", 100),
                            )
                            res = run_walkforward_backtest(df15, df1h, df4h)
                            if "error" in res:
                                await tg_send(app["session"], chat_id, f"⚠️ {res['error']}")
                            else:
                                await tg_send(
                                    app["session"],
                                    chat_id,
                                    f"🧪 <b>WALK-FORWARD BACKTEST (v28)</b>
"
                                    f"• Bitimlar: {res['trades']} ({res['wins']}W / {res['be']}BE / {res['losses']}L)
"
                                    f"• Winrate: <b>{res['winrate']}%</b> | PF: <b>{res['profit_factor']}</b>
"
                                    f"• Net: <b>{res['net_r']:+.2f}R</b> | Exp: <b>{res['expectancy_r']:+.3f}R</b> | MaxDD: <b>{res['max_dd_r']}R</b>",
                                )
                else:
                    logger.warning(f"Telegram getUpdates HTTP {r.status}")
                    await asyncio.sleep(3)
        except Exception as e:
            logger.warning(f"Telegram polling xato: {e}")
            await asyncio.sleep(3)


async def broadcast_ws_raw(payload: dict):
    msg = json.dumps(payload)
    dead = []
    for ws in list(WS_CLIENTS):
        try:
            await ws.send_str(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        WS_CLIENTS.discard(ws)


async def scanner_worker(app):
    global LAST_SIGNAL_TS, LAST_DAILY_REPORT_DATE
    await asyncio.sleep(3)
    while True:
        try:
            sig = await run_unified_v28_analysis(app["session"])
            if sig and (time.time() - LAST_SIGNAL_TS) >= CFG.signal_cooldown_sec:
                LAST_SIGNAL_TS = time.time()
                async with STATE_LOCK:
                    ACTIVE_SIGNALS.append(sig)
                await save_signal_db(sig)
                sent = await send_signal_alert(app["session"], sig)
                if not sent:
                    logger.warning(f"Signal Telegram'ga yuborilmadi, faqat Bridge'ga uzatildi: {sig['id']}")
                await push_bridge_event({"event": "new_signal", "signal": sig})
                logger.info(f"🚀 APEX v28 Signal ({sig['broker']}): {sig['id']} | Lot={sig['lot']} | Entry={sig['entry']}")

            now_uzt = datetime.datetime.now(UZT)
            today_str = now_uzt.strftime("%Y-%m-%d")
            if now_uzt.hour >= CFG.daily_report_hour_uzt and LAST_DAILY_REPORT_DATE != today_str:
                LAST_DAILY_REPORT_DATE = today_str
                st = await get_stats_and_psychology()
                await tg_send(
                    app["session"],
                    CFG.admin_chat_id,
                    f"📅 <b>KUNLIK HISOBOT ({today_str})</b>
"
                    f"• Bugungi signallar: {st['today_cnt']}
"
                    f"• Kunlik natija: <b>{st['daily_r']:+.2f}R</b> (<b>${st['daily_usd']:+.2f}</b>)
"
                    f"• Umumiy Bayes: {st['wins']}W / {st['be_wins']}BE / {st['losses']}L",
                )
        except Exception as e:
            logger.error(f"Scanner xato: {e}")
        await asyncio.sleep(15)


# ==================== 12. HTTP, WS BRIDGE & MT5 EXECUTION ACK ====================
def check_auth(request) -> bool:
    if not CFG.bridge_token or len(CFG.bridge_token) < 12:
        return False
    hdr = request.headers.get("X-Bridge-Token", "")
    if hdr == CFG.bridge_token:
        return True
    if CFG.allow_query_token and request.query.get("token", "") == CFG.bridge_token:
        return True
    return False


async def handle_health(request):
    return web.json_response({"ok": True, "engine": "APEX_v28_20USD", "market_open": MARKET_STATE["market_open"]})


async def handle_bridge_poll(request):
    if not check_auth(request):
        return web.json_response({"ok": False, "error": "Unauthorized"}, status=403)
    try:
        after_seq = int(request.query.get("after_seq", "0"))
    except ValueError:
        after_seq = 0
    async with STATE_LOCK:
        events = [e for e in BRIDGE_EVENTS if e.get("seq", 0) > after_seq][-20:]
        signals = list(ACTIVE_SIGNALS)[-10:]
        state_copy = dict(MARKET_STATE)
    return web.json_response({
        "ok": True,
        "signals": signals,
        "events": events,
        "last_seq": EVENT_SEQ,
        "market": state_copy,
        "server_time": int(time.time()),
    })


async def handle_bridge_report(request):
    if not check_auth(request):
        return web.json_response({"ok": False, "error": "Unauthorized"}, status=403)
    try:
        data = await request.json()
        if not isinstance(data, dict):
            return web.json_response({"ok": False, "error": "Invalid JSON object"}, status=400)
        bal = float(data.get("balance", 0.0))
        eq = float(data.get("equity", 0.0))
        free_m = float(data.get("free_margin", eq))
        bid = float(data.get("bid", 0.0))
        ask = float(data.get("ask", 0.0))
        pos = int(data.get("open_positions", 0))
    except (ValueError, TypeError, json.JSONDecodeError) as e:
        return web.json_response({"ok": False, "error": f"Bad payload: {e}"}, status=400)

    async with STATE_LOCK:
        MARKET_STATE["windows_bridge"].update({
            "online": True,
            "last_seen": time.time(),
            "balance": bal,
            "equity": eq,
            "free_margin": free_m,
            "mt5_bid": bid,
            "mt5_ask": ask,
            "open_positions": pos,
        })
    return web.json_response({"ok": True})


async def handle_bridge_ack(request):
    """Windows MT5 order ochganda yoki rad etganda holatni qaytaradigan endpoint."""
    if not check_auth(request):
        return web.json_response({"ok": False, "error": "Unauthorized"}, status=403)
    try:
        data = await request.json()
        if not isinstance(data, dict):
            return web.json_response({"ok": False, "error": "Invalid JSON object"}, status=400)
        sig_id = str(data.get("signal_id") or "").strip()
        executed = bool(data.get("executed", False))
        ticket = str(data.get("ticket") or "").strip()
        filled_price = float(data.get("filled_price") or 0.0)
        err_msg = str(data.get("error") or "").strip()
    except (ValueError, TypeError, json.JSONDecodeError) as e:
        return web.json_response({"ok": False, "error": f"Bad payload: {e}"}, status=400)

    async with STATE_LOCK:
        MARKET_STATE["windows_bridge"]["last_ack"] = {
            "ts": int(time.time()),
            "signal_id": sig_id,
            "executed": executed,
            "ticket": ticket,
            "filled_price": filled_price,
            "error": err_msg,
        }

    if sig_id:
        async with aiosqlite.connect(ACTIVE_DB_PATH) as db:
            if executed:
                if filled_price > 1000:
                    await db.execute(
                        "UPDATE signals SET entry = ?, mt5_ticket = ? WHERE id = ?",
                        (round(filled_price, 2), ticket, sig_id),
                    )
                else:
                    await db.execute("UPDATE signals SET mt5_ticket = ? WHERE id = ?", (ticket, sig_id))
            else:
                await db.execute("UPDATE signals SET status = 'REJECTED_MT5' WHERE id = ?", (sig_id,))
            await db.commit()

    return web.json_response({"ok": True})


async def handle_bridge_ws(request):
    if not check_auth(request):
        return web.json_response({"ok": False, "error": "Unauthorized"}, status=403)
    ws = web.WebSocketResponse(heartbeat=20)
    await ws.prepare(request)
    WS_CLIENTS.add(ws)
    try:
        async with STATE_LOCK:
            recent = list(ACTIVE_SIGNALS)[-5:]
        await ws.send_str(json.dumps({"event": "connected", "last_seq": EVENT_SEQ, "signals": recent}))
        async for _ in ws:
            pass
    finally:
        WS_CLIENTS.discard(ws)
    return ws


async def on_startup(app):
    if not CFG.bridge_token or len(CFG.bridge_token) < 12:
        logger.critical("BRIDGE_TOKEN o'rnatilmagan yoki 12 belgidan qisqa! Bridge endpointlari bloklanadi.")
    await init_db()
    app["session"] = aiohttp.ClientSession()
    app["ws_task"] = asyncio.create_task(bybit_ws_worker(app))
    app["sync_task"] = asyncio.create_task(auto_sync_worker(app))
    app["scan_task"] = asyncio.create_task(scanner_worker(app))
    app["tg_task"] = asyncio.create_task(telegram_polling_worker(app))


async def on_cleanup(app):
    for t in ("ws_task", "sync_task", "scan_task", "tg_task"):
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
    app.router.add_post("/api/bridge/ack", handle_bridge_ack)
    app.router.add_get("/ws/bridge", handle_bridge_ws)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app


async def _cli_backtest():
    async with aiohttp.ClientSession() as session:
        df15, df1h, df4h = await asyncio.gather(
            fetch_bybit_klines(session, CFG.symbol, "15", 800),
            fetch_bybit_klines(session, CFG.symbol, "60", 300),
            fetch_bybit_klines(session, CFG.symbol, "240", 150),
        )
        res = run_walkforward_backtest(df15, df1h, df4h)
        print(json.dumps(res, indent=2))


app = create_app()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="APEX v28 Quant Server (20$ Real Balance Edition)")
    parser.add_argument("--backtest", action="store_true", help="Tarixiy walk-forward backtestni ishga tushirish")
    args = parser.parse_args()
    if args.backtest:
        asyncio.run(_cli_backtest())
    else:
        web.run_app(app, host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
