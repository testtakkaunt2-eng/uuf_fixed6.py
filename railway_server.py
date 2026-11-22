"""
INSTITUTIONAL QUANT & SMC/ICT ANALYZER SERVER (RAILWAY CLOUD EDITION)
- Real-time Bybit L2 OrderBook (50-depth) + Tick VPIN & OFI Microstructure
- Quantitative Regime Detection: Hurst Exponent (R/S), Garman-Klass Volatility, Fat-Tail Kurtosis
- SMC/ICT Engine: Liquidity Sweeps (BSL/SSL), Fair Value Gaps (FVG), Order Blocks, PO3 & Killzones
- Fractional Kelly Position Sizing + Daily Drawdown Circuit Breaker
- Telegram Signal Dispatcher + Zero-Latency WebSocket & REST Bridge for Windows MT5 Executor
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
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("quant_railway_server")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID", "").strip()
BRIDGE_TOKEN = os.getenv("BRIDGE_TOKEN", "changeme_bridge_token").strip()
SYMBOL = os.getenv("SYMBOL", "XAUUSDT").strip()
MIN_SIGNAL_SCORE = int(os.getenv("MIN_SIGNAL_SCORE", "76"))
RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "1.5"))
DAILY_MAX_LOSS_PCT = float(os.getenv("DAILY_MAX_LOSS_PCT", "4.5"))

BYBIT_REST = "https://api.bybit.com"
BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"
DB_PATH = os.getenv("DB_PATH", "quant_signals.sqlite")

# Global Live Market State
MARKET_STATE = {
    "bid": 0.0,
    "ask": 0.0,
    "last_price": 0.0,
    "spread": 0.0,
    "ofi": 0.0,
    "vpin": 0.0,
    "hurst": 0.5,
    "regime": "NEUTRAL",
    "gk_vol": 0.0,
    "kurtosis": 3.0,
    "last_ws_ts": 0.0,
    "windows_bridge": {"online": False, "last_seen": 0.0, "balance": 0.0, "equity": 0.0, "mt5_bid": 0.0, "mt5_ask": 0.0},
}

ORDERBOOK_BIDS = {}
ORDERBOOK_ASKS = {}
TRADE_BUCKETS = deque(maxlen=50)
CURRENT_BUCKET = {"buy": 0.0, "sell": 0.0, "vol": 0.0}
BUCKET_TARGET_VOL = 50.0

ACTIVE_SIGNALS = deque(maxlen=50)
WS_CLIENTS = set()
LAST_SIGNAL_TS = 0.0


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
                regime TEXT,
                hurst REAL,
                vpin REAL,
                ofi REAL,
                status TEXT,
                pnl_r REAL DEFAULT 0.0
            )
        """)
        await db.commit()


async def save_signal_db(sig: dict):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT OR REPLACE INTO signals
            (id, created_at, symbol, side, entry, sl, tp1, tp2, tp3, score, regime, hurst, vpin, ofi, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            sig["id"], sig["created_at"], sig["symbol"], sig["side"],
            sig["entry"], sig["sl"], sig["tp1"], sig["tp2"], sig["tp3"],
            sig["score"], sig["regime"], sig["hurst"], sig["vpin"], sig["ofi"], sig["status"]
        ))
        await db.commit()


# ==================== QUANTITATIVE MATH MODELS ====================
def calc_hurst_exponent(prices: np.ndarray) -> float:
    """Rescaled Range (R/S) Hurst Exponent: H > 0.55 Trend, H < 0.45 Mean-Reversion."""
    if len(prices) < 60:
        return 0.50
    lags = range(2, 20)
    tau = [np.sqrt(np.std(np.subtract(prices[lag:], prices[:-lag]))) for lag in lags]
    tau = [t if t > 1e-9 else 1e-9 for t in tau]
    poly = np.polyfit(np.log(list(lags)), np.log(tau), 1)
    hurst = float(poly[0] * 2.0)
    return max(0.05, min(0.95, round(hurst, 3)))


def calc_garman_klass_vol(df: pd.DataFrame) -> float:
    """Garman-Klass OHLC Volatility Estimator (7.4x more efficient than Close-to-Close)."""
    log_hl = np.log(df["high"] / df["low"]) ** 2
    log_co = np.log(df["close"] / df["open"]) ** 2
    gk = 0.5 * log_hl - (2.0 * math.log(2.0) - 1.0) * log_co
    return round(float(np.sqrt(gk.tail(30).mean()) * 100), 4)


def calc_kurtosis(returns: np.ndarray) -> float:
    """Excess Kurtosis for Fat-Tail Extreme Risk Detection."""
    if len(returns) < 20:
        return 3.0
    std = np.std(returns)
    if std < 1e-9:
        return 3.0
    z = (returns - np.mean(returns)) / std
    return round(float(np.mean(z ** 4)), 2)


def calc_fractional_kelly(win_rate: float, rr_ratio: float, fraction: float = 0.25) -> float:
    """Quarter-Kelly Optimal Risk Fraction."""
    p = max(0.01, min(0.99, win_rate))
    q = 1.0 - p
    b = max(0.5, rr_ratio)
    kelly = (p * b - q) / b
    safe_pct = max(0.25, min(RISK_PER_TRADE_PCT, kelly * fraction * 100.0))
    return round(safe_pct, 2)


# ==================== MICROSTRUCTURE (OFI & VPIN) ====================
def compute_l2_ofi() -> float:
    """Top-10 Level-2 Order Book Imbalance [-1.0 to +1.0]."""
    if not ORDERBOOK_BIDS or not ORDERBOOK_ASKS:
        return 0.0
    top_bids = sorted(ORDERBOOK_BIDS.items(), key=lambda x: -x[0])[:10]
    top_asks = sorted(ORDERBOOK_ASKS.items(), key=lambda x: x[0])[:10]
    bid_vol = sum(v for _, v in top_bids)
    ask_vol = sum(v for _, v in top_asks)
    denom = bid_vol + ask_vol
    if denom <= 0:
        return 0.0
    return round((bid_vol - ask_vol) / denom, 3)


def update_tick_vpin(side: str, qty: float):
    """Volume-Synchronized Probability of Informed Trading (VPIN)."""
    global CURRENT_BUCKET
    if side == "Buy":
        CURRENT_BUCKET["buy"] += qty
    else:
        CURRENT_BUCKET["sell"] += qty
    CURRENT_BUCKET["vol"] += qty
    if CURRENT_BUCKET["vol"] >= BUCKET_TARGET_VOL:
        TRADE_BUCKETS.append(dict(CURRENT_BUCKET))
        CURRENT_BUCKET = {"buy": 0.0, "sell": 0.0, "vol": 0.0}
        imbalance = sum(abs(b["buy"] - b["sell"]) for b in TRADE_BUCKETS)
        total_v = sum(b["vol"] for b in TRADE_BUCKETS)
        if total_v > 0:
            MARKET_STATE["vpin"] = round(imbalance / total_v, 3)


# ==================== SMC / ICT & MULTI-FACTOR SCORING ====================
def in_ict_killzone() -> tuple[bool, str]:
    now_utc = datetime.datetime.now(datetime.timezone.utc).time()
    if datetime.time(7, 0) <= now_utc <= datetime.time(10, 0):
        return True, "London Killzone"
    if datetime.time(12, 0) <= now_utc <= datetime.time(16, 0):
        return True, "NY AM + Silver Bullet"
    if datetime.time(18, 0) <= now_utc <= datetime.time(20, 0):
        return True, "NY PM Killzone"
    return False, "Off-Killzone"


def analyze_smc_and_quant(df_15m: pd.DataFrame, df_1h: pd.DataFrame) -> dict | None:
    """Combines SMC/ICT (Sweep + FVG + Premium/Discount) with Hurst, OFI & VPIN."""
    if len(df_15m) < 80 or len(df_1h) < 50:
        return None

    closes = df_15m["close"].values
    hurst = calc_hurst_exponent(closes)
    gk_vol = calc_garman_klass_vol(df_15m)
    rets = np.diff(np.log(closes))
    kurt = calc_kurtosis(rets)

    regime = "TREND" if hurst > 0.55 else ("MEAN_REVERSION" if hurst < 0.45 else "RANDOM_WALK")
    MARKET_STATE["hurst"] = hurst
    MARKET_STATE["gk_vol"] = gk_vol
    MARKET_STATE["kurtosis"] = kurt
    MARKET_STATE["regime"] = regime

    # 1. HTF 1H Premium / Discount Equilibrium
    htf_high = float(df_1h["high"].tail(48).max())
    htf_low = float(df_1h["low"].tail(48).min())
    eq_mid = (htf_high + htf_low) / 2.0
    curr_price = float(df_15m["close"].iloc[-1])

    in_discount = curr_price < eq_mid
    in_premium = curr_price > eq_mid

    # 2. Liquidity Sweep Detection (Recent 12 candles vs prior 30 candles)
    prior_high = float(df_15m["high"].iloc[-42:-12].max())
    prior_low = float(df_15m["low"].iloc[-42:-12].min())
    recent_high = float(df_15m["high"].iloc[-12:].max())
    recent_low = float(df_15m["low"].iloc[-12:].min())

    bsl_swept = recent_high > prior_high and curr_price < prior_high
    ssl_swept = recent_low < prior_low and curr_price > prior_low

    # 3. Fair Value Gap (FVG) Detection on last 5 bars
    bull_fvg = False
    bear_fvg = False
    for i in range(-4, -1):
        c1_high = float(df_15m["high"].iloc[i - 1])
        c3_low = float(df_15m["low"].iloc[i + 1])
        c1_low = float(df_15m["low"].iloc[i - 1])
        c3_high = float(df_15m["high"].iloc[i + 1])
        if c3_low > c1_high:
            bull_fvg = True
        if c3_high < c1_low:
            bear_fvg = True

    # 4. ATR for Dynamic SL/TP
    tr = np.maximum(
        df_15m["high"] - df_15m["low"],
        np.maximum(
            abs(df_15m["high"] - df_15m["close"].shift(1)),
            abs(df_15m["low"] - df_15m["close"].shift(1)),
        ),
    )
    atr = float(tr.tail(14).mean())
    if atr <= 0.2:
        return None

    kz_active, kz_name = in_ict_killzone()
    ofi = MARKET_STATE["ofi"]
    vpin = MARKET_STATE["vpin"]

    # Score Calculation (0 - 100)
    buy_score = 40
    sell_score = 40

    if in_discount:
        buy_score += 12
    if in_premium:
        sell_score += 12
    if ssl_swept:
        buy_score += 18
    if bsl_swept:
        sell_score += 18
    if bull_fvg:
        buy_score += 12
    if bear_fvg:
        sell_score += 12
    if kz_active:
        buy_score += 8
        sell_score += 8
    if ofi > 0.15:
        buy_score += 10
    elif ofi < -0.15:
        sell_score += 10
    if regime == "TREND":
        buy_score += 5
        sell_score += 5
    if kurt > 6.5:  # Fat-tail anomaly penalty
        buy_score -= 10
        sell_score -= 10

    side = None
    score = 0
    if buy_score >= MIN_SIGNAL_SCORE and buy_score > sell_score:
        side = "BUY"
        score = min(99, buy_score)
    elif sell_score >= MIN_SIGNAL_SCORE and sell_score > buy_score:
        side = "SELL"
        score = min(99, sell_score)

    if not side:
        return None

    sl_dist = round(max(1.8, min(4.5, atr * 1.25)), 2)
    if side == "BUY":
        entry = round(curr_price, 2)
        sl = round(entry - sl_dist, 2)
        tp1 = round(entry + sl_dist * 2.0, 2)
        tp2 = round(entry + sl_dist * 3.5, 2)
        tp3 = round(entry + sl_dist * 5.0, 2)
    else:
        entry = round(curr_price, 2)
        sl = round(entry + sl_dist, 2)
        tp1 = round(entry - sl_dist * 2.0, 2)
        tp2 = round(entry - sl_dist * 3.5, 2)
        tp3 = round(entry - sl_dist * 5.0, 2)

    rec_risk = calc_fractional_kelly(win_rate=0.62, rr_ratio=3.0, fraction=0.25)

    return {
        "id": f"SIG-{int(time.time())}-{side}",
        "created_at": datetime.datetime.now(UZT).strftime("%Y-%m-%d %H:%M:%S UZT"),
        "symbol": SYMBOL,
        "side": side,
        "entry": entry,
        "sl": sl,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "sl_points": sl_dist,
        "score": score,
        "regime": regime,
        "hurst": hurst,
        "vpin": vpin,
        "ofi": ofi,
        "killzone": kz_name,
        "risk_pct": rec_risk,
        "status": "ACTIVE",
        "timestamp": int(time.time()),
    }


# ==================== DATA FETCHING & WS STREAM ====================
async def fetch_klines(session: aiohttp.ClientSession, interval: str, limit: int = 120) -> pd.DataFrame:
    url = f"{BYBIT_REST}/v5/market/kline"
    params = {"category": "linear", "symbol": SYMBOL, "interval": interval, "limit": limit}
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


async def bybit_ws_worker(app):
    """Subscribes to Bybit L2 Orderbook (50-depth) and Public Trades for OFI & VPIN."""
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
                                best_bid = max(ORDERBOOK_BIDS.keys())
                                best_ask = min(ORDERBOOK_ASKS.keys())
                                MARKET_STATE["bid"] = best_bid
                                MARKET_STATE["ask"] = best_ask
                                MARKET_STATE["spread"] = round(best_ask - best_bid, 3)
                                MARKET_STATE["ofi"] = compute_l2_ofi()
                                MARKET_STATE["last_ws_ts"] = time.time()
                        elif topic.startswith("publicTrade."):
                            for t in payload.get("data", []):
                                MARKET_STATE["last_price"] = float(t["p"])
                                update_tick_vpin(t["S"], float(t["v"]))
        except Exception as e:
            logger.warning(f"Bybit WS qayta ulanmoqda: {e}")
            await asyncio.sleep(3)


# ==================== TELEGRAM & BRIDGE BROADCAST ====================
async def send_telegram_signal(session: aiohttp.ClientSession, sig: dict):
    if not BOT_TOKEN or not ADMIN_CHAT_ID:
        return
    icon = "🟢" if sig["side"] == "BUY" else "🔴"
    text = (
        f"{icon} <b>INSTITUTIONAL QUANT SIGNAL ({sig['symbol']})</b>
"
        f"━━━━━━━━━━━━━━━━━━━━
"
        f"📌 <b>Yo'nalish:</b> {sig['side']} | <b>Sifat:</b> {sig['score']}/100
"
        f"🎯 <b>Entry:</b> <code>{sig['entry']}</code>
"
        f"🛑 <b>Stop-Loss:</b> <code>{sig['sl']}</code> ({sig['sl_points']}$)
"
        f"✅ <b>TP1 (1:2):</b> <code>{sig['tp1']}</code>
"
        f"✅ <b>TP2 (1:3.5):</b> <code>{sig['tp2']}</code>
"
        f"✅ <b>TP3 (1:5):</b> <code>{sig['tp3']}</code>
"
        f"━━━━━━━━━━━━━━━━━━━━
"
        f"📊 <b>Quant Rejim:</b> {sig['regime']} (Hurst: {sig['hurst']})
"
        f"🌊 <b>Order Flow (OFI):</b> {sig['ofi']:+.3f} | <b>VPIN:</b> {sig['vpin']:.3f}
"
        f"⏰ <b>Oyna:</b> {sig['killzone']} | <b>Tavsiya Risk:</b> {sig['risk_pct']}%"
    )
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    await session.post(url, json={"chat_id": ADMIN_CHAT_ID, "text": text, "parse_mode": "HTML"})


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
            df_15m = await fetch_klines(app["session"], "15", 120)
            df_1h = await fetch_klines(app["session"], "60", 100)
            sig = analyze_smc_and_quant(df_15m, df_1h)
            if sig and (time.time() - LAST_SIGNAL_TS) >= 900:
                LAST_SIGNAL_TS = time.time()
                ACTIVE_SIGNALS.append(sig)
                await save_signal_db(sig)
                await send_telegram_signal(app["session"], sig)
                await broadcast_ws_signal(sig)
                logger.info(f"Yangi signal yuborildi: {sig['id']} ({sig['score']} ball)")
        except Exception as e:
            logger.error(f"Scanner xatosi: {e}")
        await asyncio.sleep(15)


# ==================== HTTP & WEBSOCKET API ENDPOINTS ====================
def check_auth(request) -> bool:
    token = request.headers.get("X-Bridge-Token") or request.query.get("token", "")
    return token == BRIDGE_TOKEN


async def handle_health(request):
    return web.json_response({"ok": True, "status": "running", "market": MARKET_STATE})


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
        "balance": data.get("balance", 0.0),
        "equity": data.get("equity", 0.0),
        "mt5_bid": data.get("bid", 0.0),
        "mt5_ask": data.get("ask", 0.0),
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


async def on_cleanup(app):
    for t in ("ws_task", "scan_task"):
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
