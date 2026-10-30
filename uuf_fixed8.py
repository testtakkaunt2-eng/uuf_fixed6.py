"""
GOLD (EXNESS / XM / XAUUSD) SMC + ICT Bot — v19 ULTIMATE (10/10 EDITION)
========================================================================
v16 TITANIUM FIXED va v18 TITANIUM ning to'liq birlashtirilgan, dublikatlardan
tozalangan yakuniy versiyasi:
  ✅ 1 ta foydalanuvchi uchun 3 tagacha Exness (Demo/Real) MT5 hisob ulash + Avto-server topish
  ✅ Native MetaTrader5 + Bepul Cloud Bridge (/api/mt5_bridge/poll) orqali LONG/SHORT avto-savdo
  ✅ TP1/TP2/Trailing va /sync bo'lganda Exness ochiq bitimlarining SL darajasini avto-yangilash
  ✅ Chatga yuborilgan Exness ID/Parol xabarini darhol o'chirish (deleteMessage himoyasi)
  ✅ BOT_TOKEN dan avtomatik Fernet shifrlash kaliti yaratish (ENCRYPTION_KEY shart emas)
  ✅ To'liq SMC + ICT motori: HTF 60s kesh, NY 17:00 PDH/PDL, DXY/US10Y Macro SMT,
     BTC/EUR vaqt bo'yicha SMT, Volume Profile (POC/VAH/VAL), Order Flow Delta, RSI Divergence
  ✅ /sync va /offset ochiq bitim darajalarini xavfsiz siljitadi, /close esa _monitor_lock bilan himoyalangan
  ✅ Telegram Mini App (WebApp) HMAC-SHA256 avtorizatsiyasi + barcha buyruqlarni 1 bosishda boshqarish
"""

import asyncio
import aiohttp
import aiosqlite
import base64
import datetime
import hashlib
import hmac
import html
import json
import logging
import math
import os
import re
import struct
import time
import traceback
import urllib.parse
import zlib

try:
    from cryptography.fernet import Fernet
    _HAS_CRYPTO = True
except ImportError:
    _HAS_CRYPTO = False

try:
    import ccxt.async_support as ccxt_async
    _HAS_CCXT = True
except ImportError:
    _HAS_CCXT = False

try:
    import MetaTrader5 as mt5
    _HAS_MT5 = True
except ImportError:
    mt5 = None
    _HAS_MT5 = False

try:
    from zoneinfo import ZoneInfo
    _HAS_ZONEINFO = True
except ImportError:
    _HAS_ZONEINFO = False

from aiohttp import web

# ==================== VAQT (UZT = UTC+5) ====================
UZT_OFFSET = datetime.timedelta(hours=5)

def uzt_now():
    return datetime.datetime.now(datetime.timezone.utc) + UZT_OFFSET

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.Formatter.converter = staticmethod(
    lambda ts: (datetime.datetime.fromtimestamp(ts, datetime.timezone.utc) + UZT_OFFSET).timetuple()
)
logger = logging.getLogger("gold_smc_v19")

# ==================== KONFIGURATSIYA ====================
SYMBOL = "XAUUSDT"
BTC_SYMBOL = os.environ.get("BTC_SYMBOL", "BTCUSDT")
EUR_SYMBOL = os.environ.get("EUR_SYMBOL", "EURUSDT")
BYBIT_REST_URL = os.environ.get("BYBIT_REST_URL", "https://api.bybit.com").rstrip("/")
BYBIT_WS_URL = os.environ.get("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/linear")

LIMIT = 240
HTF_LIMIT = 120

RR1, RR2, RR3 = 1.5, 3.0, 4.5
SWING_LEFT, SWING_RIGHT = 3, 3
CHECK_INTERVAL_SEC = 5
ZONE_MAX_DISTANCE_PCT = 0.05
ZONE_TOL_MAX_POINTS = 1.5
MIN_SIGNAL_SCORE = 65
TRADE_STALE_WARNING_HOURS = 6
FLIP_WARN_AFTER_SEC = 60
FLIP_WARN_REPEAT_SEC = 300
LIVE_PRICE_MAX_AGE_SEC = 10
ASIA_END_HOUR_UTC = 6
AI_COOLDOWN_SEC = 20
CHART_COOLDOWN_SEC = 10
XM_SPREAD_BUFFER = 0.30
SNIPER_SKIP_IF_SL_TOO_WIDE = True
FVG_REQUIRE_UNTESTED = True
NEWS_RETRY_SEC = 300

DEFAULT_BALANCE_USD = 20.0
DEFAULT_RISK_PER_TRADE_PCT = 10.0
MAX_RISK_PER_TRADE_PCT = 25.0
SNIPER_MIN_SL_POINTS = 1.20
SNIPER_MAX_SL_POINTS_SMALL = 2.65

XM_MIN_LOT = 0.01
XM_OZ_PER_MIN_LOT = 1.0

# Exness sozlamalari (1 ta foydalanuvchiga maksimal 3 ta hisob)
MAX_EXNESS_ACCOUNTS = 3
EXNESS_DEFAULT_SERVER = os.environ.get("EXNESS_DEFAULT_SERVER", "Exness-MT5Trial9")
EXNESS_GOLD_SYMBOLS = ("XAUUSDm", "XAUUSD", "XAUUSDc", "GOLDm", "GOLD")
EXNESS_FALLBACK_SERVERS = (
    "Exness-MT5Trial9", "Exness-MT5Trial8", "Exness-MT5Trial10",
    "Exness-MT5Trial7", "Exness-MT5Trial6", "Exness-MT5Trial11",
    "Exness-MT5Trial12", "Exness-MT5Trial5", "Exness-MT5Trial",
    "Exness-MT5Real", "Exness-MT5Real9", "Exness-MT5Real8",
)
MT5_TERMINAL_PATH = os.environ.get("MT5_TERMINAL_PATH", "").strip()

# Binance ixtiyoriy avto-trade
AUTO_TRADE_ENABLED_GLOBAL = os.environ.get("AUTO_TRADE_ENABLED", "1").strip() == "1"
BINANCE_SYMBOL = os.environ.get("BINANCE_SYMBOL", "PAXG/USDT")
BINANCE_TESTNET = os.environ.get("BINANCE_TESTNET", "1").strip() == "1"
AUTO_TRADE_MAX_RISK_PCT = min(MAX_RISK_PER_TRADE_PCT, float(os.environ.get("AUTO_TRADE_MAX_RISK_PCT", "15")))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", BASE_DIR)
try:
    os.makedirs(DATA_DIR, exist_ok=True)
except Exception:
    DATA_DIR = BASE_DIR
DB_FILE = os.path.join(DATA_DIR, "gold_bot.sqlite")

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

KILLZONES_ENABLED = True
KILLZONES = [
    {"name": "London Open", "start": datetime.time(6, 0), "end": datetime.time(12, 0)},
    {"name": "NY Open / Overlap", "start": datetime.time(12, 0), "end": datetime.time(17, 0)},
]
SESSION_TZ = {
    "asia": "Asia/Tokyo",
    "london": "Europe/London",
    "newyork": "America/New_York",
}

# ==================== SECRETS & ENCRYPTION ====================
def _get_secret(name):
    value = os.environ.get(name)
    if value:
        return value
    try:
        import config
        return getattr(config, name)
    except (ImportError, AttributeError):
        raise RuntimeError(f"'{name}' topilmadi. Environment variable sifatida o'rnating.")

BOT_TOKEN = _get_secret("BOT_TOKEN")
CHAT_ID = _get_secret("CHAT_ID")
ADMIN_CHAT_ID = str(CHAT_ID)
WEBAPP_URL = os.environ.get("WEBAPP_URL", "").strip()

if not re.fullmatch(r"-?\d+", ADMIN_CHAT_ID.strip()):
    logger.warning(f"CHAT_ID ('{ADMIN_CHAT_ID}') raqam ko'rinishida emas!")

WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET")
WEBHOOK_ENABLED = bool(WEBHOOK_SECRET)

ENCRYPTION_KEY = os.environ.get("ENCRYPTION_KEY", "").strip()
_fernet = None
if _HAS_CRYPTO:
    try:
        if ENCRYPTION_KEY:
            _fernet = Fernet(ENCRYPTION_KEY.encode("utf-8"))
        else:
            derived = base64.urlsafe_b64encode(hashlib.sha256(BOT_TOKEN.encode("utf-8")).digest())
            _fernet = Fernet(derived)
    except Exception:
        derived = base64.urlsafe_b64encode(hashlib.sha256(BOT_TOKEN.encode("utf-8")).digest())
        _fernet = Fernet(derived)

def encrypt_secret(plain_text):
    if _fernet:
        return _fernet.encrypt(plain_text.encode("utf-8")).decode("utf-8")
    key = hashlib.sha256(BOT_TOKEN.encode("utf-8")).digest()
    raw = plain_text.encode("utf-8")
    xored = bytes(b ^ key[i % len(key)] for i, b in enumerate(raw))
    return "xor:" + base64.urlsafe_b64encode(xored).decode("utf-8")

def decrypt_secret(token):
    if token.startswith("xor:"):
        key = hashlib.sha256(BOT_TOKEN.encode("utf-8")).digest()
        raw = base64.urlsafe_b64decode(token[4:].encode("utf-8"))
        return bytes(b ^ key[i % len(key)] for i, b in enumerate(raw)).decode("utf-8")
    if _fernet:
        return _fernet.decrypt(token.encode("utf-8")).decode("utf-8")
    raise RuntimeError("Shifrdan ochish imkonsiz (cryptography kutubxonasi kerak).")

AUTO_TRADE_AVAILABLE = _HAS_CCXT and AUTO_TRADE_ENABLED_GLOBAL

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash").strip() or "gemini-2.5-flash"
GEMINI_PRESETS = [
    ("gemini-2.5-flash", "Tez va arzon (standart)"),
    ("gemini-2.5-flash-lite", "Eng tez, eng yengil"),
    ("gemini-2.5-pro", "Eng aqlli, sekinroq (thinking)"),
]
_GEMINI_NAME_RE = re.compile(r"gemini-[a-z0-9][a-z0-9.\-]{1,50}")
_CURRENT_GEMINI_MODEL = GEMINI_MODEL

def get_gemini_model():
    return _CURRENT_GEMINI_MODEL

ALLOW_PUBLIC = os.environ.get("ALLOW_PUBLIC", "0").strip() == "1"
ALLOWED_ENV = {x.strip() for x in os.environ.get("ALLOWED_CHAT_IDS", "").split(",") if x.strip()}
DASHBOARD_TOKEN = os.environ.get("DASHBOARD_TOKEN", "").strip()

DEFAULT_PRICE_OFFSET = float(os.environ.get("PRICE_OFFSET", "-8.21"))
_CURRENT_OFFSET = DEFAULT_PRICE_OFFSET
_LIVE_BYBIT_RAW_PRICE = None
_LIVE_PRICE = None
_LIVE_PRICE_TS = 0.0
_WS_CONNECTED = False
_LAST_WS_MONITOR_TS = 0.0
_flip_since = None
_flip_last_warn = float("-inf")
_ai_last = {}
_chart_last = {}
_denied_notified = set()
_bg_tasks = set()
_smt_disabled_until = {}
_geoblock_warned = False
_mt5_lock = asyncio.Lock()
_monitor_lock = asyncio.Lock()
_BRIDGE_SIGNALS_QUEUE = []

# ==================== HELPERS ====================
def _safe_log_error(error):
    msg = str(error)
    for sec in (BOT_TOKEN, WEBHOOK_SECRET, GEMINI_API_KEY):
        if sec:
            msg = msg.replace(sec, "***")
    return msg

_UNBOUNDED_CAP = 2000

def _bounded_add(target_set, item, cap=_UNBOUNDED_CAP):
    if len(target_set) >= cap:
        target_set.clear()
    target_set.add(item)

def _bounded_set(target_dict, key, value, cap=_UNBOUNDED_CAP):
    if len(target_dict) >= cap:
        target_dict.clear()
    target_dict[key] = value

def T(seconds):
    return aiohttp.ClientTimeout(total=seconds)

WS_TIMEOUT = aiohttp.ClientWSTimeout(ws_close=15) if hasattr(aiohttp, "ClientWSTimeout") else 15

def live_price():
    if _WS_CONNECTED and _LIVE_PRICE and (time.monotonic() - _LIVE_PRICE_TS) <= LIVE_PRICE_MAX_AGE_SEC:
        return _LIVE_PRICE
    return None

def _spawn(coro):
    t = asyncio.create_task(coro)
    _bg_tasks.add(t)
    t.add_done_callback(_bg_tasks.discard)
    return t

def closed_only(candles):
    return candles[:-1] if len(candles) > 1 else candles

def market_open(now_utc=None):
    now = now_utc or datetime.datetime.now(datetime.timezone.utc)
    wd = now.weekday()
    if wd in (5, 6):
        return False
    if wd == 4 and now.hour >= 21:
        return False
    if wd == 0 and now.hour < 1:
        return False
    return True

def in_killzone():
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    if not market_open(now_utc):
        return False
    if not KILLZONES_ENABLED:
        return True
    now_time = now_utc.time()
    for kz in KILLZONES:
        if kz["start"] <= now_time < kz["end"]:
            return True
    return False

def current_session():
    if _HAS_ZONEINFO:
        try:
            now_utc = datetime.datetime.now(datetime.timezone.utc)
            lon_h = now_utc.astimezone(ZoneInfo(SESSION_TZ["london"])).hour
            ny_h = now_utc.astimezone(ZoneInfo(SESSION_TZ["newyork"])).hour
            tok_h = now_utc.astimezone(ZoneInfo(SESSION_TZ["asia"])).hour
            lon_act = 7 <= lon_h < 16
            ny_act = 8 <= ny_h < 17
            tok_act = 9 <= tok_h < 18
            if lon_act and ny_act:
                return "london_ny_overlap"
            if lon_act:
                return "london"
            if ny_act:
                return "newyork"
            if tok_act:
                return "asia"
            return "after_hours"
        except Exception:
            pass
    hour = datetime.datetime.now(datetime.timezone.utc).hour
    if 0 <= hour < 7:
        return "asia"
    if 7 <= hour < 12:
        return "london"
    if 12 <= hour < 16:
        return "london_ny_overlap"
    if 16 <= hour < 21:
        return "newyork"
    return "after_hours"

# ==================== DATABASE (aiosqlite) ====================
class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self._conn = None
        self._lock = asyncio.Lock()

    async def init(self):
        self._conn = await aiosqlite.connect(self.db_path)
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS state (
                key TEXT PRIMARY KEY, value TEXT
            )
        """)
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT, signal TEXT, entry REAL, sl REAL, tp1 REAL, tp2 REAL,
                result TEXT, close_price REAL, opened_at TEXT, closed_at TEXT,
                lot REAL, risk_usd REAL, confirmations TEXT
            )
        """)
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS broker_accounts (
                chat_id TEXT PRIMARY KEY,
                broker TEXT NOT NULL,
                api_key_enc TEXT NOT NULL,
                api_secret_enc TEXT NOT NULL,
                auto_trade INTEGER NOT NULL DEFAULT 0,
                risk_pct REAL NOT NULL DEFAULT 2.0,
                created_at TEXT,
                updated_at TEXT
            )
        """)
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS exness_accounts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT NOT NULL,
                slot INTEGER NOT NULL,
                login TEXT NOT NULL,
                password_enc TEXT NOT NULL,
                server TEXT NOT NULL,
                label TEXT DEFAULT 'Exness Demo',
                symbol TEXT DEFAULT 'XAUUSDm',
                lot REAL NOT NULL DEFAULT 0.01,
                auto_trade INTEGER NOT NULL DEFAULT 1,
                last_balance REAL DEFAULT NULL,
                last_status TEXT DEFAULT 'Ulangan',
                created_at TEXT,
                updated_at TEXT,
                UNIQUE(chat_id, slot)
            )
        """)
        await self._conn.commit()
        logger.info(f"DB tayyor: {self.db_path}")

    async def close(self):
        if self._conn:
            await self._conn.close()

    async def state_get(self, key, default=None):
        async with self._lock:
            async with self._conn.execute("SELECT value FROM state WHERE key=?", (key,)) as cur:
                row = await cur.fetchone()
                return json.loads(row[0]) if row else default

    async def state_set(self, key, value):
        async with self._lock:
            await self._conn.execute(
                "INSERT INTO state (key, value) VALUES (?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, json.dumps(value, ensure_ascii=False)),
            )
            await self._conn.commit()

    async def insert_trade(self, trade, result, close_price, lot, risk_usd, confs):
        async with self._lock:
            await self._conn.execute("""
                INSERT INTO trades (symbol, signal, entry, sl, tp1, tp2, result,
                                    close_price, opened_at, closed_at, lot, risk_usd, confirmations)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                SYMBOL, trade["signal"], trade["entry"], trade.get("initial_sl", trade["sl"]),
                trade["tp1"], trade["tp2"], result, close_price,
                trade.get("opened_at"),
                datetime.datetime.now(datetime.timezone.utc).isoformat(),
                lot, risk_usd, confs,
            ))
            await self._conn.commit()

    async def get_recent_trades(self, limit=30):
        async with self._lock:
            async with self._conn.execute(
                "SELECT signal, result, entry, close_price, closed_at "
                "FROM trades ORDER BY id DESC LIMIT ?", (limit,)
            ) as cur:
                rows = await cur.fetchall()
                return [
                    {"signal": r[0], "result": r[1], "entry": r[2],
                     "close_price": r[3], "closed_at": r[4]}
                    for r in rows
                ]

    async def get_all_results(self):
        async with self._lock:
            async with self._conn.execute("SELECT result FROM trades") as cur:
                rows = await cur.fetchall()
                return [r[0] for r in rows]

    # ---------- Exness Multi-Account (1-3 slots) ----------
    async def get_exness_accounts(self, chat_id):
        async with self._lock:
            async with self._conn.execute(
                "SELECT id, chat_id, slot, login, password_enc, server, label, symbol, "
                "lot, auto_trade, last_balance, last_status "
                "FROM exness_accounts WHERE chat_id=? ORDER BY slot ASC",
                (str(chat_id),),
            ) as cur:
                rows = await cur.fetchall()
                return [
                    {
                        "id": r[0], "chat_id": r[1], "slot": r[2], "login": r[3],
                        "password_enc": r[4], "server": r[5], "label": r[6],
                        "symbol": r[7], "lot": r[8], "auto_trade": bool(r[9]),
                        "last_balance": r[10], "last_status": r[11],
                    }
                    for r in rows
                ]

    async def get_all_active_exness_accounts(self):
        async with self._lock:
            async with self._conn.execute(
                "SELECT id, chat_id, slot, login, password_enc, server, label, symbol, "
                "lot, auto_trade, last_balance, last_status "
                "FROM exness_accounts WHERE auto_trade=1 ORDER BY chat_id, slot ASC"
            ) as cur:
                rows = await cur.fetchall()
                return [
                    {
                        "id": r[0], "chat_id": r[1], "slot": r[2], "login": r[3],
                        "password_enc": r[4], "server": r[5], "label": r[6],
                        "symbol": r[7], "lot": r[8], "auto_trade": bool(r[9]),
                        "last_balance": r[10], "last_status": r[11],
                    }
                    for r in rows
                ]

    async def add_or_update_exness_account(
        self, chat_id, login, password_enc, server, slot=None, label=None, lot=0.01, symbol="XAUUSDm"
    ):
        existing = await self.get_exness_accounts(chat_id)
        if slot is None:
            for acc in existing:
                if str(acc["login"]) == str(login):
                    slot = acc["slot"]
                    break
        if slot is None:
            used_slots = {acc["slot"] for acc in existing}
            for s_num in range(1, MAX_EXNESS_ACCOUNTS + 1):
                if s_num not in used_slots:
                    slot = s_num
                    break
        if slot is None or not (1 <= int(slot) <= MAX_EXNESS_ACCOUNTS):
            raise ValueError(f"Maksimal {MAX_EXNESS_ACCOUNTS} ta Exness hisob ulash mumkin! Avval birini o'chiring.")

        slot = int(slot)
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        lbl = label or f"Exness #{slot}"
        async with self._lock:
            await self._conn.execute("""
                INSERT INTO exness_accounts
                    (chat_id, slot, login, password_enc, server, label, symbol, lot, auto_trade, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                ON CONFLICT(chat_id, slot) DO UPDATE SET
                    login=excluded.login,
                    password_enc=excluded.password_enc,
                    server=excluded.server,
                    label=excluded.label,
                    symbol=excluded.symbol,
                    lot=excluded.lot,
                    updated_at=excluded.updated_at
            """, (str(chat_id), slot, str(login), password_enc, server, lbl, symbol, float(lot), now, now))
            await self._conn.commit()
        return slot

    async def update_exness_status(self, chat_id, slot, balance=None, status_text=None):
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        async with self._lock:
            if balance is not None and status_text is not None:
                await self._conn.execute(
                    "UPDATE exness_accounts SET last_balance=?, last_status=?, updated_at=? WHERE chat_id=? AND slot=?",
                    (float(balance), str(status_text), now, str(chat_id), int(slot)),
                )
            elif status_text is not None:
                await self._conn.execute(
                    "UPDATE exness_accounts SET last_status=?, updated_at=? WHERE chat_id=? AND slot=?",
                    (str(status_text), now, str(chat_id), int(slot)),
                )
            await self._conn.commit()

    async def toggle_exness_autotrade(self, chat_id, slot, enabled=None, lot=None):
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        async with self._lock:
            if enabled is not None:
                await self._conn.execute(
                    "UPDATE exness_accounts SET auto_trade=?, updated_at=? WHERE chat_id=? AND slot=?",
                    (1 if enabled else 0, now, str(chat_id), int(slot)),
                )
            if lot is not None:
                await self._conn.execute(
                    "UPDATE exness_accounts SET lot=?, updated_at=? WHERE chat_id=? AND slot=?",
                    (max(0.01, round(float(lot), 2)), now, str(chat_id), int(slot)),
                )
            await self._conn.commit()

    async def delete_exness_account(self, chat_id, slot):
        async with self._lock:
            await self._conn.execute(
                "DELETE FROM exness_accounts WHERE chat_id=? AND slot=?",
                (str(chat_id), int(slot)),
            )
            await self._conn.commit()

    # ---------- Binance broker accounts ----------
    async def upsert_broker_account(self, chat_id, broker, api_key_enc, api_secret_enc, risk_pct=2.0):
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        async with self._lock:
            await self._conn.execute("""
                INSERT INTO broker_accounts (chat_id, broker, api_key_enc, api_secret_enc,
                                              auto_trade, risk_pct, created_at, updated_at)
                VALUES (?, ?, ?, ?, 0, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    broker=excluded.broker, api_key_enc=excluded.api_key_enc,
                    api_secret_enc=excluded.api_secret_enc, risk_pct=excluded.risk_pct,
                    updated_at=excluded.updated_at
            """, (str(chat_id), broker, api_key_enc, api_secret_enc, risk_pct, now, now))
            await self._conn.commit()

    async def get_broker_account(self, chat_id):
        async with self._lock:
            async with self._conn.execute(
                "SELECT chat_id, broker, api_key_enc, api_secret_enc, auto_trade, risk_pct "
                "FROM broker_accounts WHERE chat_id=?", (str(chat_id),)
            ) as cur:
                row = await cur.fetchone()
                if not row:
                    return None
                return {
                    "chat_id": row[0], "broker": row[1], "api_key_enc": row[2],
                    "api_secret_enc": row[3], "auto_trade": bool(row[4]), "risk_pct": row[5],
                }

    async def set_auto_trade(self, chat_id, enabled):
        async with self._lock:
            await self._conn.execute(
                "UPDATE broker_accounts SET auto_trade=?, updated_at=? WHERE chat_id=?",
                (1 if enabled else 0, datetime.datetime.now(datetime.timezone.utc).isoformat(), str(chat_id)),
            )
            await self._conn.commit()

    async def remove_broker_account(self, chat_id):
        async with self._lock:
            await self._conn.execute("DELETE FROM broker_accounts WHERE chat_id=?", (str(chat_id),))
            await self._conn.commit()

    async def get_autotrade_accounts(self, broker=None):
        q = "SELECT chat_id, broker, api_key_enc, api_secret_enc, auto_trade, risk_pct FROM broker_accounts WHERE auto_trade=1"
        params = ()
        if broker:
            q += " AND broker=?"
            params = (broker,)
        async with self._lock:
            async with self._conn.execute(q, params) as cur:
                rows = await cur.fetchall()
                return [
                    {"chat_id": r[0], "broker": r[1], "api_key_enc": r[2],
                     "api_secret_enc": r[3], "auto_trade": bool(r[4]), "risk_pct": r[5]}
                    for r in rows
                ]

db = DatabaseManager(DB_FILE)

# ==================== EXNESS MT5 MULTI-ACCOUNT ENGINE (CLEANED) ====================
def _mt5_resolve_gold_symbol(preferred="XAUUSDm"):
    candidates = [preferred] + [s for s in EXNESS_GOLD_SYMBOLS if s != preferred]
    for sym in candidates:
        info = mt5.symbol_info(sym)
        if info is not None:
            if not info.visible:
                mt5.symbol_select(sym, True)
            return sym
    return preferred

def _mt5_test_login_sync(login_id, password, server):
    """Avto-server aniqlash bilan kuchaytirilgan Exness MT5 ulanish tekshiruvi."""
    if not _HAS_MT5:
        return {
            "ok": True,
            "mode": "cloud_bridge",
            "balance": None,
            "symbol": "XAUUSDm",
            "server": server if (server and server.upper() != "AUTO") else EXNESS_DEFAULT_SERVER,
            "msg": "Cloud Bridge rejimida saqlandi (Bepul Web/Bridge faol).",
        }

    servers_to_try = [server] if server and server.upper() != "AUTO" else []
    for s_fb in EXNESS_FALLBACK_SERVERS:
        if s_fb not in servers_to_try:
            servers_to_try.append(s_fb)

    last_err = None
    for srv in servers_to_try:
        init_kwargs = {"login": int(login_id), "password": str(password), "server": str(srv)}
        if MT5_TERMINAL_PATH:
            init_kwargs["path"] = MT5_TERMINAL_PATH
        if mt5.initialize(**init_kwargs) and mt5.login(login=int(login_id), password=str(password), server=str(srv)):
            acc_info = mt5.account_info()
            bal = round(float(acc_info.balance), 2) if acc_info else 0.0
            sym = _mt5_resolve_gold_symbol()
            return {
                "ok": True,
                "mode": "native_mt5",
                "balance": bal,
                "symbol": sym,
                "server": srv,
                "msg": f"MT5 ulandi ({srv})! Balans: ${bal:.2f} | Simbol: {sym}",
            }
        last_err = mt5.last_error()

    return {"ok": False, "msg": f"Login/Parol yoki Server xato: {last_err}"}

def _mt5_open_order_sync(login_id, password, server, side, lot, sl, tp, preferred_sym="XAUUSDm", entry_ref=None):
    """Exness haqiqiy Bid/Ask narxiga nisbatan SL/TP masofasini aniq saqlab order ochish."""
    if not _HAS_MT5:
        return {"ok": False, "mode": "cloud_bridge", "msg": "Cloud Bridge navbatiga qo'shildi"}

    init_kwargs = {"login": int(login_id), "password": str(password), "server": str(server)}
    if MT5_TERMINAL_PATH:
        init_kwargs["path"] = MT5_TERMINAL_PATH
    if not mt5.initialize(**init_kwargs) or not mt5.login(login=int(login_id), password=str(password), server=str(server)):
        return {"ok": False, "msg": f"MT5 login xato: {mt5.last_error()}"}

    sym = _mt5_resolve_gold_symbol(preferred_sym)
    tick = mt5.symbol_info_tick(sym)
    sym_info = mt5.symbol_info(sym)
    if not tick or not sym_info:
        return {"ok": False, "msg": f"Simbol ({sym}) kotirovkasi topilmadi"}

    digits = sym_info.digits
    is_buy = (side == "LONG")
    order_type = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL
    price = tick.ask if is_buy else tick.bid

    if entry_ref and abs(price - float(entry_ref)) <= 15.0:
        sl_dist = abs(float(entry_ref) - float(sl))
        tp_dist = abs(float(tp) - float(entry_ref))
        adj_sl = (price - sl_dist) if is_buy else (price + sl_dist)
        adj_tp = (price + tp_dist) if is_buy else (price - tp_dist)
    else:
        adj_sl, adj_tp = float(sl), float(tp)

    vol = max(sym_info.volume_min, min(sym_info.volume_max, round(float(lot), 2)))
    res = None
    for fill_mode in (mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_FOK, mt5.ORDER_FILLING_RETURN):
        request = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": sym,
            "volume": vol,
            "type": order_type,
            "price": round(price, digits),
            "sl": round(adj_sl, digits),
            "tp": round(adj_tp, digits),
            "deviation": 35,
            "magic": 202619,
            "comment": "SMC_v19_Sniper",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": fill_mode,
        }
        res = mt5.order_send(request)
        if res is not None and res.retcode == mt5.TRADE_RETCODE_DONE:
            acc_info = mt5.account_info()
            bal = round(float(acc_info.balance), 2) if acc_info else None
            return {
                "ok": True,
                "ticket": res.order,
                "price": res.price or price,
                "sl": round(adj_sl, digits),
                "tp": round(adj_tp, digits),
                "symbol": sym,
                "balance": bal,
            }

    code = res.retcode if res else "None"
    comment = res.comment if res else str(mt5.last_error())
    return {"ok": False, "msg": f"Order xato ({code}): {comment}"}

def _mt5_modify_or_close_sync(login_id, password, server, action="close", new_sl=None):
    if not _HAS_MT5:
        return {"ok": False, "msg": "MT5 yo'q"}
    init_kwargs = {"login": int(login_id), "password": str(password), "server": str(server)}
    if MT5_TERMINAL_PATH:
        init_kwargs["path"] = MT5_TERMINAL_PATH
    if not mt5.initialize(**init_kwargs) or not mt5.login(login=int(login_id), password=str(password), server=str(server)):
        return {"ok": False, "msg": "MT5 login xato"}

    positions = mt5.positions_get(magic=202619)
    if not positions:
        return {"ok": True, "count": 0}

    count = 0
    for pos in positions:
        sym = pos.symbol
        tick = mt5.symbol_info_tick(sym)
        sym_info = mt5.symbol_info(sym)
        if not tick or not sym_info:
            continue
        digits = sym_info.digits
        if action == "close":
            close_type = mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
            close_price = tick.bid if pos.type == mt5.ORDER_TYPE_BUY else tick.ask
            for fill_mode in (mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_FOK, mt5.ORDER_FILLING_RETURN):
                req = {
                    "action": mt5.TRADE_ACTION_DEAL,
                    "position": pos.ticket,
                    "symbol": sym,
                    "volume": pos.volume,
                    "type": close_type,
                    "price": round(close_price, digits),
                    "deviation": 35,
                    "magic": 202619,
                    "type_filling": fill_mode,
                }
                r = mt5.order_send(req)
                if r and r.retcode == mt5.TRADE_RETCODE_DONE:
                    count += 1
                    break
        elif action == "modify_sl" and new_sl is not None:
            req = {
                "action": mt5.TRADE_ACTION_SLTP,
                "position": pos.ticket,
                "symbol": sym,
                "sl": round(float(new_sl), digits),
                "tp": pos.tp,
            }
            r = mt5.order_send(req)
            if r and r.retcode == mt5.TRADE_RETCODE_DONE:
                count += 1
    return {"ok": True, "count": count}

async def test_exness_account(acc):
    try:
        pw = decrypt_secret(acc["password_enc"])
    except Exception as e:
        return {"ok": False, "msg": f"Parol shifr xatosi: {e}"}
    async with _mt5_lock:
        loop = asyncio.get_running_loop()
        res = await loop.run_in_executor(
            None, _mt5_test_login_sync, acc["login"], pw, acc["server"]
        )
    if res.get("ok"):
        await db.update_exness_status(
            acc["chat_id"], acc["slot"],
            balance=res.get("balance"),
            status_text=res.get("msg", "Ulangan"),
        )
    else:
        await db.update_exness_status(acc["chat_id"], acc["slot"], status_text=res.get("msg", "Xato"))
    return res

async def execute_exness_autotrades(session, trade):
    accounts = await db.get_all_active_exness_accounts()
    if not accounts:
        return

    tp_target = trade.get("tp2") or trade["tp1"]
    if len(_BRIDGE_SIGNALS_QUEUE) >= 100:
        _BRIDGE_SIGNALS_QUEUE.pop(0)
    _BRIDGE_SIGNALS_QUEUE.append({
        "ts": int(time.time()),
        "signal": trade["signal"],
        "entry": trade["entry"],
        "sl": trade["sl"],
        "tp1": trade["tp1"],
        "tp2": tp_target,
    })

    for acc in accounts:
        try:
            pw = decrypt_secret(acc["password_enc"])
        except Exception:
            continue
        lot = acc.get("lot") or XM_MIN_LOT
        if _HAS_MT5:
            async with _mt5_lock:
                loop = asyncio.get_running_loop()
                res = await loop.run_in_executor(
                    None,
                    _mt5_open_order_sync,
                    acc["login"], pw, acc["server"],
                    trade["signal"], lot, trade["sl"], tp_target,
                    acc.get("symbol", "XAUUSDm"), trade["entry"],
                )
            if res.get("ok"):
                await db.update_exness_status(
                    acc["chat_id"], acc["slot"],
                    balance=res.get("balance"),
                    status_text=f"Ochiq #{res.get('ticket')} ({trade['signal']})",
                )
                await send_telegram(
                    session,
                    f"✅ <b>Exness #{acc['slot']} ({html.escape(acc['login'])}) — Avto-Order ochildi!</b>\n"
                    f"Yo'nalish: <b>{trade['signal']}</b> | Lot: <code>{lot}</code> | Narx: <code>{res.get('price')}</code>\n"
                    f"SL: <code>{res.get('sl')}</code> | TP: <code>{res.get('tp')}</code>",
                    chat_id=acc["chat_id"],
                )
            else:
                await db.update_exness_status(acc["chat_id"], acc["slot"], status_text=res.get("msg", "Xato"))
                await send_telegram(
                    session,
                    f"⚠️ <b>Exness #{acc['slot']} ({html.escape(acc['login'])}) order xatosi:</b> "
                    f"{html.escape(str(res.get('msg')))}",
                    chat_id=acc["chat_id"],
                )
        else:
            await db.update_exness_status(
                acc["chat_id"], acc["slot"],
                status_text=f"Bridge Signal: {trade['signal']} @ {trade['entry']}",
            )

async def sync_exness_positions_action(action="close", new_sl=None):
    if not _HAS_MT5:
        return
    accounts = await db.get_all_active_exness_accounts()
    for acc in accounts:
        try:
            pw = decrypt_secret(acc["password_enc"])
            async with _mt5_lock:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(
                    None, _mt5_modify_or_close_sync, acc["login"], pw, acc["server"], action, new_sl
                )
        except Exception as e:
            logger.warning(f"Exness {action} xato ({acc['login']}): {_safe_log_error(e)}")

# ==================== MEMORY CACHE ====================
_LAST_CANDLES_15M = []
_LAST_STRUCT_15M = {}
_zones_cache = {}
_swings_cache = {}
_metrics = {
    "signals": 0, "closed": 0, "wins": 0, "losses": 0,
    "errors": 0, "tg_errors": 0, "ws_ticks": 0, "start": time.time(),
}

O, H, L, C, V = 1, 2, 3, 4, 5

def clear_analysis_caches():
    _zones_cache.clear()
    _swings_cache.clear()

# ==================== SUBSCRIBERS / ACCOUNT ====================
async def get_subs():
    return set(await db.state_get("subscribers", [ADMIN_CHAT_ID]))

async def add_sub(chat_id):
    s = await get_subs()
    s.add(str(chat_id))
    await db.state_set("subscribers", list(s))

async def remove_sub(chat_id):
    s = await get_subs()
    s.discard(str(chat_id))
    await db.state_set("subscribers", list(s))

async def get_account():
    acc = await db.state_get(
        "account",
        {"balance": DEFAULT_BALANCE_USD, "risk_pct": DEFAULT_RISK_PER_TRADE_PCT},
    )
    if not acc.get("balance"):
        acc["balance"] = DEFAULT_BALANCE_USD
    if not acc.get("risk_pct"):
        acc["risk_pct"] = DEFAULT_RISK_PER_TRADE_PCT
    return acc

async def update_account(balance=None, risk=None):
    acc = await get_account()
    if balance is not None:
        acc["balance"] = balance
    if risk is not None:
        acc["risk_pct"] = risk
    await db.state_set("account", acc)

def _is_win_result(r):
    return r in ("TP1", "TP2", "TP3", "TP1 + Trailing SL", "TP2 + Trailing SL")

async def win_rate_text():
    results = await db.get_all_results()
    total = len(results)
    if not total:
        return "Hali statistika yo'q"
    wins = sum(1 for r in results if _is_win_result(r))
    losses = sum(1 for r in results if r == "SL")
    be = total - wins - losses
    denom = wins + losses
    wr = (wins / denom * 100) if denom > 0 else 0.0
    return f"WR: {wr:.1f}% ({wins}✅ / {losses}❌ / {be}🛡 | Jami: {total})"

# ==================== GEMINI AI ENGINE ====================
async def ask_gemini_ai(session, prompt_text, system_instruction=None):
    if not GEMINI_API_KEY:
        return None
    model_name = get_gemini_model()
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent"
    headers = {"x-goog-api-key": GEMINI_API_KEY, "Content-Type": "application/json"}
    model_l = model_name.lower()
    can_disable_thinking = "2.5" in model_l and "flash" in model_l

    for attempt in (0, 1):
        gen_cfg = {"temperature": 0.3, "maxOutputTokens": 2048}
        if can_disable_thinking and attempt == 0:
            gen_cfg["thinkingConfig"] = {"thinkingBudget": 0}
        else:
            gen_cfg["maxOutputTokens"] = 8192
        payload = {
            "contents": [{"parts": [{"text": prompt_text}]}],
            "generationConfig": gen_cfg,
        }
        if system_instruction:
            payload["systemInstruction"] = {"parts": [{"text": system_instruction}]}
        try:
            async with session.post(url, json=payload, headers=headers, timeout=T(35)) as r:
                if r.status != 200:
                    body = (await r.text())[:300]
                    logger.warning(f"Gemini API status {r.status}: {_safe_log_error(body)}")
                    if r.status == 400 and attempt == 0 and can_disable_thinking:
                        continue
                    return None
                data = await r.json()
                candidates = data.get("candidates", [])
                if not candidates:
                    return None
                cand = candidates[0]
                parts = cand.get("content", {}).get("parts", [])
                text = "".join(p_.get("text", "") for p_ in parts).strip()
                return text or None
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"Gemini AI xato: {_safe_log_error(e)}")
            return None
    return None

async def generate_ai_signal_review(session, trade, status, confirmations):
    if not GEMINI_API_KEY:
        return None
    sys_prompt = (
        "Sen professional SMC va ICT oltin (XAUUSD) treyderisan. "
        "Foydalanuvchi Exness/XM brokerda kichik balans (0.01 lot) bilan savdo qiladi. "
        "Berilgan signalga 2-3 jumlada juda qisqa, aniq o'zbek tilida xulosa ber: "
        "kirish qanchalik xavfsiz, likvidlik tuzog'i (inducement) yo'qmi va TP1 da nima qilish kerak."
    )
    user_prompt = (
        f"Signal: {trade['signal']} | Grade: {trade.get('grade')} ({trade.get('score')}/100)\n"
        f"Entry: {trade['entry']} | SL: {trade['sl']} (Risk: {abs(trade['entry']-trade['sl']):.2f}$) | "
        f"TP1: {trade['tp1']} | TP2: {trade['tp2']}\n"
        f"MTF: 5m={status.get('b5')}, 15m={status.get('b15')}, 1H={status.get('b1h')}, 4H={status.get('b4h')}\n"
        f"EQ(50%): {status.get('eq15')} | PDH/PDL: {status.get('pdh')}/{status.get('pdl')}\n"
        f"Tasdiqlar: {', '.join(confirmations)}"
    )
    return await ask_gemini_ai(session, user_prompt, system_instruction=sys_prompt)

async def _send_ai_followup(session, trade, status, confirmations):
    try:
        txt = await generate_ai_signal_review(session, trade, status, confirmations)
        if txt:
            await send_telegram(
                session,
                f"🧠 <b>Gemini AI Xulosasi ({trade['signal']} @ {trade['entry']}):</b>\n<i>{html.escape(txt)}</i>",
            )
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning(f"AI follow-up xato: {_safe_log_error(e)}")

async def _ai_post_mortem(session, trade, sl_price):
    try:
        await asyncio.sleep(900)
        if not GEMINI_API_KEY or not _LAST_CANDLES_15M:
            return
        now_p = live_price() or _LAST_CANDLES_15M[-1][C]
        sys_prompt = (
            "Sen institutsional SMC/ICT mentorisan. Oltin (XAUUSD) bitimi Stop Loss bo'ldi. "
            "15 daqiqadan keyingi narxga qarab, SL sababini 2-3 jumlada aniq tushuntir: "
            "bu Inducement/Liquidity Sweep bo'ldimi yoki haqiqiy Trend o'zgarishimi?"
        )
        prompt = (
            f"Bitim: {trade['signal']} | Entry: {trade['entry']} | SL urildi: {sl_price}\n"
            f"TP1: {trade['tp1']} | Zona: 15m {trade.get('zone_kind')} ({trade.get('zone_bottom')}-{trade.get('zone_top')})\n"
            f"Tasdiqlar: {trade.get('confirmations', '-')}\n"
            f"SL dan 15 daqiqa keyingi joriy narx: {now_p} (15m bias: {_LAST_STRUCT_15M.get('bias')})"
        )
        txt = await ask_gemini_ai(session, prompt, system_instruction=sys_prompt)
        if txt:
            await send_telegram(
                session,
                f"🧬 <b>AI Post-Mortem (SL Ekspertizasi — {trade['signal']}):</b>\n"
                f"SL narx: <code>{sl_price}</code> ➡️ 15m keyin: <code>{now_p}</code>\n\n"
                f"<i>{html.escape(txt)}</i>",
            )
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.warning(f"Post-mortem xato: {_safe_log_error(e)}")

async def get_learned_adjustments(current_confs):
    async with db._lock:
        async with db._conn.execute(
            "SELECT result, confirmations FROM trades WHERE confirmations IS NOT NULL ORDER BY id DESC LIMIT 60"
        ) as cur:
            rows = await cur.fetchall()
    if len(rows) < 5:
        return 0, []

    stats = {}
    for res, conf_str in rows:
        is_win = _is_win_result(res)
        is_loss = (res == "SL")
        if not (is_win or is_loss):
            continue
        for tag in (conf_str or "").split(", "):
            tag = tag.strip()
            if not tag or "Xotira:" in tag:
                continue
            w, l = stats.get(tag, (0, 0))
            stats[tag] = (w + (1 if is_win else 0), l + (1 if is_loss else 0))

    adj, notes = 0, []
    for c in list(current_confs):
        w, l = stats.get(c, (0, 0))
        total = w + l
        if total >= 4:
            wr = w / total
            if wr <= 0.35:
                adj -= 6
                notes.append(f"🧠Xotira: {c} ko'p SL (-6)")
            elif wr >= 0.70:
                adj += 5
                notes.append(f"🧠Xotira: {c} yuqori WR (+5)")
    return max(-15, min(15, adj)), notes

# ==================== PURE-PYTHON PNG CHART RENDERER ====================
_FONT_5X7 = {
    " ": [0, 0, 0, 0, 0, 0, 0],
    "0": [14, 17, 19, 21, 25, 17, 14],
    "1": [4, 12, 4, 4, 4, 4, 14],
    "2": [14, 17, 1, 6, 8, 16, 31],
    "3": [31, 2, 4, 2, 1, 17, 14],
    "4": [2, 6, 10, 18, 31, 2, 2],
    "5": [31, 16, 30, 1, 1, 17, 14],
    "6": [6, 8, 16, 30, 17, 17, 14],
    "7": [31, 1, 2, 4, 8, 8, 8],
    "8": [14, 17, 17, 14, 17, 17, 14],
    "9": [14, 17, 17, 15, 1, 2, 12],
    "A": [14, 17, 17, 31, 17, 17, 17],
    "B": [30, 17, 17, 30, 17, 17, 30],
    "C": [14, 17, 16, 16, 16, 17, 14],
    "D": [28, 18, 17, 17, 17, 18, 28],
    "E": [31, 16, 16, 30, 16, 16, 31],
    "F": [31, 16, 16, 30, 16, 16, 16],
    "G": [14, 17, 16, 23, 17, 17, 15],
    "H": [17, 17, 17, 31, 17, 17, 17],
    "I": [14, 4, 4, 4, 4, 4, 14],
    "J": [7, 2, 2, 2, 2, 18, 12],
    "K": [17, 18, 20, 24, 20, 18, 17],
    "L": [16, 16, 16, 16, 16, 16, 31],
    "M": [17, 27, 21, 21, 17, 17, 17],
    "N": [17, 25, 21, 19, 17, 17, 17],
    "O": [14, 17, 17, 17, 17, 17, 14],
    "P": [30, 17, 17, 30, 16, 16, 16],
    "Q": [14, 17, 17, 17, 21, 18, 13],
    "R": [30, 17, 17, 30, 20, 18, 17],
    "S": [15, 16, 16, 14, 1, 1, 30],
    "T": [31, 4, 4, 4, 4, 4, 4],
    "U": [17, 17, 17, 17, 17, 17, 14],
    "V": [17, 17, 17, 17, 17, 10, 4],
    "W": [17, 17, 17, 21, 21, 27, 17],
    "X": [17, 17, 10, 4, 10, 17, 17],
    "Y": [17, 17, 10, 4, 4, 4, 4],
    "Z": [31, 1, 2, 4, 8, 16, 31],
    ".": [0, 0, 0, 0, 0, 12, 12],
    ":": [0, 12, 12, 0, 12, 12, 0],
    "-": [0, 0, 0, 31, 0, 0, 0],
    "+": [0, 4, 4, 31, 4, 4, 0],
    "/": [1, 2, 4, 4, 8, 16, 0],
    "(": [2, 4, 8, 8, 8, 4, 2],
    ")": [8, 4, 2, 2, 2, 4, 8],
    "%": [25, 26, 4, 4, 8, 19, 19],
    "$": [4, 15, 20, 14, 5, 30, 4],
    "=": [0, 0, 31, 0, 31, 0, 0],
}

def render_smc_chart_png(candles, zones, struct_info, trade=None, width=820, height=460):
    window = candles[-65:] if len(candles) >= 65 else candles
    if len(window) < 10:
        return None

    bg_r, bg_g, bg_b = 16, 19, 26
    row_bytes = width * 3
    buf = bytearray([bg_r, bg_g, bg_b] * (width * height))

    def set_px(x, y, c):
        if 0 <= x < width and 0 <= y < height:
            idx = (y * width + x) * 3
            buf[idx], buf[idx + 1], buf[idx + 2] = c[0], c[1], c[2]

    def fill_rect(x0, y0, x1, y1, c, alpha=1.0):
        xa, xb = max(0, min(x0, x1)), min(width - 1, max(x0, x1))
        ya, yb = max(0, min(y0, y1)), min(height - 1, max(y0, y1))
        if alpha >= 0.99:
            for y in range(ya, yb + 1):
                base = y * row_bytes
                for x in range(xa, xb + 1):
                    i = base + x * 3
                    buf[i], buf[i + 1], buf[i + 2] = c[0], c[1], c[2]
        else:
            inv = 1.0 - alpha
            for y in range(ya, yb + 1):
                base = y * row_bytes
                for x in range(xa, xb + 1):
                    i = base + x * 3
                    buf[i] = int(buf[i] * inv + c[0] * alpha)
                    buf[i + 1] = int(buf[i + 1] * inv + c[1] * alpha)
                    buf[i + 2] = int(buf[i + 2] * inv + c[2] * alpha)

    def draw_hline(y, x0, x1, c, dashed=False):
        if not (0 <= y < height):
            return
        xa, xb = max(0, min(x0, x1)), min(width - 1, max(x0, x1))
        for x in range(xa, xb + 1):
            if not dashed or (x // 5) % 2 == 0:
                set_px(x, y, c)

    def draw_text(x, y, text, c, scale=1):
        cx = x
        for ch in str(text).upper():
            glyph = _FONT_5X7.get(ch, _FONT_5X7[" "])
            for gy in range(7):
                row_bits = glyph[gy]
                for gx in range(5):
                    if (row_bits >> (4 - gx)) & 1:
                        for sy in range(scale):
                            for sx in range(scale):
                                set_px(cx + gx * scale + sx, y + gy * scale + sy, c)
            cx += 6 * scale

    p_highs = [c[H] for c in window]
    p_lows = [c[L] for c in window]
    if trade:
        p_highs.extend([trade["entry"], trade["sl"], trade["tp1"], trade["tp2"]])
        p_lows.extend([trade["entry"], trade["sl"], trade["tp1"], trade["tp2"]])
    p_max = max(p_highs)
    p_min = min(p_lows)
    pad = max((p_max - p_min) * 0.08, 1.0)
    p_max += pad
    p_min -= pad

    margin_left, margin_right = 16, 118
    margin_top, margin_bottom = 38, 24
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom

    def price_to_y(p):
        return int(margin_top + (p_max - p) / (p_max - p_min) * plot_h)

    for i in range(6):
        gp = p_min + (p_max - p_min) * (i / 5.0)
        gy = price_to_y(gp)
        draw_hline(gy, margin_left, width - margin_right, (32, 38, 50), dashed=True)
        draw_text(width - margin_right + 8, gy - 3, f"{gp:.2f}", (135, 145, 165), scale=1)

    start_idx = len(candles) - len(window)
    n_bars = len(window)
    step_x = plot_w / max(n_bars, 1)

    ref_price = window[-1][C]
    vis_zones = [z for z in zones if z["top"] >= p_min and z["bottom"] <= p_max]
    vis_zones.sort(key=lambda z: abs(ref_price - z["ce"]))
    for z in vis_zones[:8]:
        zy1 = price_to_y(z["top"])
        zy2 = price_to_y(z["bottom"])
        rel_idx = max(0, z["index"] - start_idx)
        zx1 = int(margin_left + rel_idx * step_x)
        zx2 = width - margin_right
        if z["type"] == "bullish":
            col = (22, 165, 115) if z["kind"] != "BB" else (55, 135, 225)
        else:
            col = (215, 60, 75) if z["kind"] != "BB" else (225, 145, 40)
        fill_rect(zx1, zy1, zx2, zy2, col, alpha=0.22)
        draw_hline(zy1, zx1, zx2, col)
        draw_hline(zy2, zx1, zx2, col)
        draw_text(zx2 - 42, min(zy1, zy2) + 3, z["kind"], col, scale=1)

    eq = struct_info.get("eq")
    if eq and p_min <= eq <= p_max:
        eq_y = price_to_y(eq)
        draw_hline(eq_y, margin_left, width - margin_right, (220, 185, 65), dashed=True)
        draw_text(margin_left + 4, eq_y - 9, f"EQ 50%: {eq:.2f}", (220, 185, 65), scale=1)

    candle_w = max(3, int(step_x * 0.64))
    for i, c in enumerate(window):
        cx = int(margin_left + (i + 0.5) * step_x)
        yh = price_to_y(c[H])
        yl = price_to_y(c[L])
        yo = price_to_y(c[O])
        yc = price_to_y(c[C])
        bull = c[C] >= c[O]
        col = (38, 198, 130) if bull else (234, 75, 90)
        fill_rect(cx, yh, cx, yl, col)
        b_top, b_bot = min(yo, yc), max(yo, yc)
        if b_bot == b_top:
            b_bot += 1
        fill_rect(cx - candle_w // 2, b_top, cx + candle_w // 2, b_bot, col)

    if trade:
        levels = [
            ("ENTRY", trade["entry"], (240, 240, 255)),
            ("SL", trade["sl"], (255, 75, 85)),
            ("TP1", trade["tp1"], (45, 210, 140)),
            ("TP2", trade["tp2"], (30, 235, 180)),
        ]
        for lbl, val, col in levels:
            if p_min <= val <= p_max:
                ly = price_to_y(val)
                draw_hline(ly, margin_left, width - margin_right, col, dashed=(lbl != "ENTRY"))
                draw_text(width - margin_right + 8, ly - 4, f"{lbl}:{val:.1f}", col, scale=1)

    last_p = live_price() or window[-1][C]
    bias_str = (struct_info.get("bias") or "NEUTRAL").upper()
    hdr = f"EXNESS/XM GOLD 15M | PRICE: {last_p:.2f} | BIAS: {bias_str}"
    draw_text(margin_left, 12, hdr, (235, 215, 155), scale=2)

    raw_data = bytearray()
    for y in range(height):
        raw_data.append(0)
        raw_data.extend(buf[y * row_bytes:(y + 1) * row_bytes])

    def png_chunk(c_type, c_data):
        crc = zlib.crc32(c_type + c_data) & 0xFFFFFFFF
        return struct.pack(">I", len(c_data)) + c_type + c_data + struct.pack(">I", crc)

    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    idat = zlib.compress(bytes(raw_data), level=6)
    return (
        b"\x89PNG\r\n\x1a\n"
        + png_chunk(b"IHDR", ihdr)
        + png_chunk(b"IDAT", idat)
        + png_chunk(b"IEND", b"")
    )

async def render_chart_async(candles, zones, struct_info, trade=None):
    loop = asyncio.get_running_loop()
    trade_copy = dict(trade) if trade else None
    return await loop.run_in_executor(
        None, lambda: render_smc_chart_png(list(candles), list(zones), dict(struct_info or {}), trade_copy)
    )

# ==================== BYBIT API ====================
def _warn_geoblock_once(status):
    global _geoblock_warned
    if not _geoblock_warned:
        _geoblock_warned = True
        logger.error(f"Bybit {status} qaytardi — BYBIT_REST_URL=https://api.bytick.com sinab ko'ring.")

async def fetch_raw_price(session):
    url = f"{BYBIT_REST_URL}/v5/market/tickers"
    async with session.get(url, params={"category": "linear", "symbol": SYMBOL},
                           headers=HEADERS, timeout=T(10)) as r:
        r.raise_for_status()
        data = await r.json()
        rows = data.get("result", {}).get("list", [])
        if not rows:
            raise RuntimeError("Ticker bo'sh")
        return float(rows[0]["lastPrice"])

async def fetch_ohlcv(session, timeframe, symbol=SYMBOL, limit=LIMIT):
    imap = {"1m": "1", "5m": "5", "15m": "15", "30m": "30",
            "1h": "60", "4h": "240", "1d": "D"}
    interval = imap.get(timeframe, timeframe)
    url = f"{BYBIT_REST_URL}/v5/market/kline"
    params = {"category": "linear", "symbol": symbol,
              "interval": interval, "limit": limit}

    offset = _CURRENT_OFFSET if symbol == SYMBOL else 0.0
    last_err = None
    for attempt in range(3):
        try:
            async with session.get(url, params=params, headers=HEADERS, timeout=T(10)) as r:
                if r.status in (403, 451):
                    _warn_geoblock_once(r.status)
                r.raise_for_status()
                data = await r.json()
                rows = data.get("result", {}).get("list", [])
                if not rows:
                    raise RuntimeError(f"Kline bo'sh: {symbol} {timeframe}")
                rows.sort(key=lambda x: int(x[0]))
                return [
                    [
                        int(x[0]),
                        round(float(x[1]) + offset, 2),
                        round(float(x[2]) + offset, 2),
                        round(float(x[3]) + offset, 2),
                        round(float(x[4]) + offset, 2),
                        float(x[5]),
                    ]
                    for x in rows
                ]
        except Exception as e:
            last_err = e
            await asyncio.sleep(0.4 * (attempt + 1))
    raise last_err

# ==================== REAL-TIME WEBSOCKET LOOP ====================
async def _ws_monitor(session, price):
    try:
        trade = await db.state_get("current_trade")
        if trade:
            await monitor_trade(session, price, _LAST_STRUCT_15M.get("bias"))
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.error(f"WS monitor xato: {_safe_log_error(e)}")

async def bybit_ws_loop(app):
    global _LIVE_BYBIT_RAW_PRICE, _LIVE_PRICE, _LIVE_PRICE_TS, _WS_CONNECTED, _LAST_WS_MONITOR_TS
    session = app["session"]
    monitor_task = None
    backoff = 3

    while True:
        try:
            async with session.ws_connect(BYBIT_WS_URL, heartbeat=20, timeout=WS_TIMEOUT) as ws:
                await ws.send_json({"op": "subscribe", "args": [f"tickers.{SYMBOL}"]})
                _WS_CONNECTED = True
                backoff = 3
                logger.info("⚡ Real-time Bybit WebSocket ulandi!")

                async for msg in ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        try:
                            payload = json.loads(msg.data)
                        except ValueError:
                            continue
                        data = payload.get("data") if isinstance(payload, dict) else None
                        if not isinstance(data, dict):
                            continue
                        last_p_str = data.get("lastPrice")
                        if last_p_str:
                            raw_p = float(last_p_str)
                            _LIVE_BYBIT_RAW_PRICE = raw_p
                            live_xm = round(raw_p + _CURRENT_OFFSET, 2)
                            _LIVE_PRICE = live_xm
                            _LIVE_PRICE_TS = time.monotonic()
                            _metrics["ws_ticks"] += 1

                            now_mono = time.monotonic()
                            if (now_mono - _LAST_WS_MONITOR_TS >= 0.35
                                    and (monitor_task is None or monitor_task.done())):
                                _LAST_WS_MONITOR_TS = now_mono
                                monitor_task = _spawn(_ws_monitor(session, live_xm))
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING,
                                      aiohttp.WSMsgType.ERROR):
                        break
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"WS xato: {_safe_log_error(e)}")
        finally:
            _WS_CONNECTED = False
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 30)

# ==================== ADVANCED SMC & ICT ENGINE ====================
def find_swings(candles, left=SWING_LEFT, right=SWING_RIGHT):
    highs, lows = [], []
    n = len(candles)
    if n < left + right + 1:
        return highs, lows
    for i in range(left, n - right):
        ch, cl = candles[i][H], candles[i][L]
        is_high, is_low = True, True
        for offset in range(-left, right + 1):
            if offset == 0:
                continue
            other = candles[i + offset]
            if other[H] > ch:
                is_high = False
            if other[L] < cl:
                is_low = False
            if not is_high and not is_low:
                break
        if is_high:
            highs.append((i, ch))
        if is_low:
            lows.append((i, cl))
    return highs, lows

def get_swings_cached(candles, cache_key="default", left=SWING_LEFT, right=SWING_RIGHT):
    if not candles:
        return [], []
    ts = candles[-1][0]
    full_key = (cache_key, left, right, _CURRENT_OFFSET)
    cached = _swings_cache.get(full_key)
    if cached and cached["ts"] == ts:
        return cached["highs"], cached["lows"]
    h, l = find_swings(candles, left, right)
    _swings_cache[full_key] = {"ts": ts, "highs": h, "lows": l}
    return h, l

def analyze_market_structure(candles, cache_key="default"):
    highs, lows = get_swings_cached(candles, cache_key=cache_key)
    if len(highs) < 2 or len(lows) < 2:
        return {
            "bias": None, "event": None, "event_bar": -1,
            "dealing_high": None, "dealing_low": None, "eq": None,
            "ote_top": None, "ote_bottom": None,
        }

    events = [(idx, "H", p) for idx, p in highs] + [(idx, "L", p) for idx, p in lows]
    events.sort(key=lambda x: x[0])

    last_h = highs[0][1]
    last_l = lows[0][1]
    bias = None
    last_event = None
    event_bar = -1

    for i in range(1, len(candles)):
        close_p = candles[i][C]
        for s_idx, s_type, s_price in events:
            if s_idx + SWING_RIGHT == i:
                if s_type == "H":
                    last_h = s_price
                else:
                    last_l = s_price

        if close_p > last_h:
            last_event = "CHoCH_BULL" if bias == "bearish" else "BOS_BULL"
            bias = "bullish"
            last_h = candles[i][H]
            event_bar = i
        elif close_p < last_l:
            last_event = "CHoCH_BEAR" if bias == "bullish" else "BOS_BEAR"
            bias = "bearish"
            last_l = candles[i][L]
            event_bar = i

    if bias is None:
        if highs[-1][1] > highs[-2][1] and lows[-1][1] >= lows[-2][1]:
            bias = "bullish"
        elif lows[-1][1] < lows[-2][1] and highs[-1][1] <= highs[-2][1]:
            bias = "bearish"

    recent_highs = [p for _, p in highs[-4:]]
    recent_lows = [p for _, p in lows[-4:]]
    d_high = max(recent_highs) if recent_highs else candles[-1][H]
    d_low = min(recent_lows) if recent_lows else candles[-1][L]
    rng = max(d_high - d_low, 0.01)
    eq = (d_high + d_low) / 2.0

    if bias == "bullish":
        ote_top = d_high - rng * 0.618
        ote_bottom = d_high - rng * 0.786
    else:
        ote_bottom = d_low + rng * 0.618
        ote_top = d_low + rng * 0.786

    return {
        "bias": bias,
        "event": last_event,
        "event_bar": event_bar,
        "dealing_high": round(d_high, 2),
        "dealing_low": round(d_low, 2),
        "eq": round(eq, 2),
        "ote_top": round(max(ote_top, ote_bottom), 2),
        "ote_bottom": round(min(ote_top, ote_bottom), 2),
    }

def confirmed_structure_bias(candles, cache_key="default"):
    return analyze_market_structure(candles, cache_key=cache_key)["bias"]

def detect_equal_highs_lows(candles, cache_key="XAU_15m", tol_pct=0.06):
    highs, lows = get_swings_cached(candles, cache_key=cache_key)
    eqh, eql = None, None
    if len(highs) >= 2:
        h1, h2 = highs[-2][1], highs[-1][1]
        if abs(h1 - h2) / h2 * 100 <= tol_pct:
            eqh = round((h1 + h2) / 2.0, 2)
    if len(lows) >= 2:
        l1, l2 = lows[-2][1], lows[-1][1]
        if abs(l1 - l2) / l2 * 100 <= tol_pct:
            eql = round((l1 + l2) / 2.0, 2)
    return eqh, eql

def detect_asia_and_pd_sweeps(candles_15m, pdh, pdl):
    asia_high, asia_low = None, None
    now = datetime.datetime.now(datetime.timezone.utc)
    now_date = now.date()
    asia_done = now.hour >= ASIA_END_HOUR_UTC
    asia_bars = []
    for c in candles_15m[-96:]:
        dt = datetime.datetime.fromtimestamp(c[0] / 1000.0, tz=datetime.timezone.utc)
        if dt.date() == now_date and 0 <= dt.hour < ASIA_END_HOUR_UTC:
            asia_bars.append(c)
    if len(asia_bars) >= 4:
        asia_high = round(max(c[H] for c in asia_bars), 2)
        asia_low = round(min(c[L] for c in asia_bars), 2)

    recent = candles_15m[-4:]
    bull_sweeps, bear_sweeps = [], []

    for c in recent:
        if pdl and c[L] < pdl and c[C] > pdl:
            bull_sweeps.append("PDL Sweep (Kecha Low likvidligi olindi)")
        if pdh and c[H] > pdh and c[C] < pdh:
            bear_sweeps.append("PDH Sweep (Kecha High likvidligi olindi)")
        if asia_done and asia_low and c[L] < asia_low and c[C] > asia_low:
            bull_sweeps.append("Asia Low Sweep (Judas Swing)")
        if asia_done and asia_high and c[H] > asia_high and c[C] < asia_high:
            bear_sweeps.append("Asia High Sweep (Judas Swing)")

    return {
        "asia_high": asia_high,
        "asia_low": asia_low,
        "bull_sweep": bull_sweeps[0] if bull_sweeps else None,
        "bear_sweep": bear_sweeps[0] if bear_sweeps else None,
    }

def detect_fvg(candles, min_size=0.0):
    fvgs = []
    for i in range(2, len(candles)):
        f, m, t = candles[i - 2], candles[i - 1], candles[i]
        mid_body = abs(m[C] - m[O])
        if t[L] > f[H] and (t[L] - f[H]) >= min_size and mid_body > 0:
            fvgs.append({
                "type": "bullish", "kind": "FVG",
                "top": t[L], "bottom": f[H],
                "ce": (t[L] + f[H]) / 2.0, "index": i,
            })
        elif t[H] < f[L] and (f[L] - t[H]) >= min_size and mid_body > 0:
            fvgs.append({
                "type": "bearish", "kind": "FVG",
                "top": f[L], "bottom": t[H],
                "ce": (f[L] + t[H]) / 2.0, "index": i,
            })
    return fvgs

def detect_ob_and_breakers(candles):
    bodies = [abs(c[C] - c[O]) for c in candles]
    vols = [c[V] if len(c) > 5 else 0 for c in candles]
    raw_obs = []
    for i in range(10, len(candles) - 2):
        ab = sum(bodies[i - 10:i]) / 10
        av = sum(vols[i - 10:i]) / 10
        if ab == 0 or av == 0:
            continue
        cur, fol = candles[i], candles[i + 1]
        ib = bodies[i + 1] > ab * 1.35
        iv = vols[i + 1] > av * 1.4
        bob = cur[C] < cur[O] and fol[C] > fol[O] and fol[C] > cur[H]
        beob = cur[C] > cur[O] and fol[C] < fol[O] and fol[C] < cur[L]
        if ib and iv and bob:
            raw_obs.append({
                "type": "bullish", "kind": "OB",
                "top": max(cur[O], cur[C]), "bottom": cur[L],
                "ce": (max(cur[O], cur[C]) + cur[L]) / 2.0, "index": i,
            })
        if ib and iv and beob:
            raw_obs.append({
                "type": "bearish", "kind": "OB",
                "top": cur[H], "bottom": min(cur[O], cur[C]),
                "ce": (cur[H] + min(cur[O], cur[C])) / 2.0, "index": i,
            })

    zones = []
    n = len(candles)
    for ob in raw_obs:
        fi = ob["index"]
        if n - 1 - fi > 60:
            continue
        broken_idx = None
        check_end = max(fi + 1, n - 2)
        for j in range(fi + 1, check_end):
            c = candles[j]
            if ob["type"] == "bullish" and c[C] < ob["bottom"]:
                broken_idx = j
                break
            if ob["type"] == "bearish" and c[C] > ob["top"]:
                broken_idx = j
                break

        if broken_idx is None:
            zones.append(ob)
        else:
            bb_type = "bearish" if ob["type"] == "bullish" else "bullish"
            bb_valid = True
            for k in range(broken_idx + 1, check_end):
                ck = candles[k]
                if bb_type == "bullish" and ck[C] < ob["bottom"]:
                    bb_valid = False
                    break
                if bb_type == "bearish" and ck[C] > ob["top"]:
                    bb_valid = False
                    break
            if bb_valid and (n - 1 - broken_idx <= 35):
                zones.append({
                    "type": bb_type, "kind": "BB",
                    "top": ob["top"], "bottom": ob["bottom"],
                    "ce": ob["ce"], "index": broken_idx,
                })
    return zones

def _fvg_is_fresh(zone, candles, max_age_bars=45, untested=False):
    fi = zone["index"]
    if len(candles) - 1 - fi > max_age_bars:
        return False
    check_end = max(fi + 1, len(candles) - 2)
    for i in range(fi + 1, check_end):
        c = candles[i]
        if zone["type"] == "bullish" and c[C] < zone["bottom"]:
            return False
        if zone["type"] == "bearish" and c[C] > zone["top"]:
            return False
        if untested and zone.get("kind") == "FVG":
            if zone["type"] == "bullish" and c[L] <= zone["ce"]:
                return False
            if zone["type"] == "bearish" and c[H] >= zone["ce"]:
                return False
    return True

def get_active_zones(candles, cache_key="XAU_15m"):
    if not candles:
        return []
    ts = candles[-1][0]
    full_key = (cache_key, _CURRENT_OFFSET)
    cached = _zones_cache.get(full_key)
    if cached and cached["ts"] == ts:
        return cached["zones"]

    atr = calculate_atr(candles) or 0.0
    min_size = max(0.30, atr * 0.10)
    untested = FVG_REQUIRE_UNTESTED and cache_key == "XAU_15m"
    fvgs = [z for z in detect_fvg(candles, min_size) if _fvg_is_fresh(z, candles, untested=untested)]
    obs_bbs = detect_ob_and_breakers(candles)
    all_fresh = fvgs + obs_bbs
    _zones_cache[full_key] = {"ts": ts, "zones": all_fresh}
    return all_fresh

def check_5m_ltf_trigger(cl5, struct5, target_bias, zone=None):
    triggers = []
    if struct5["bias"] == target_bias:
        ev = struct5.get("event") or ""
        ev_bar = struct5.get("event_bar", -1)
        if ev_bar >= 0 and (len(cl5) - 1 - ev_bar) <= 6:
            if "CHoCH" in ev:
                triggers.append("5m CHoCH (MSS)")
            elif "BOS" in ev:
                triggers.append("5m BOS")

    zones_5m = get_active_zones(cl5, cache_key="XAU_5m")
    recent_5m_zones = [
        z for z in zones_5m
        if z["type"] == target_bias and (len(cl5) - 1 - z["index"] <= 6)
    ]
    if recent_5m_zones:
        triggers.append(f"5m {recent_5m_zones[-1]['kind']} Displacement")

    if len(cl5) >= 2:
        last, prev = cl5[-1], cl5[-2]
        rng = max(last[H] - last[L], 0.01)
        upper_wick = last[H] - max(last[O], last[C])
        lower_wick = min(last[O], last[C]) - last[L]
        touches = True
        if zone is not None:
            touches = last[L] <= zone["top"] + 0.5 and last[H] >= zone["bottom"] - 0.5

        if touches:
            if target_bias == "bullish":
                if last[C] > last[O] and prev[C] < prev[O] and last[C] > prev[O]:
                    triggers.append("5m Bullish Engulfing")
                elif lower_wick / rng >= 0.50 and last[C] >= last[O]:
                    triggers.append("5m Bullish Pinbar")
            else:
                if last[C] < last[O] and prev[C] > prev[O] and last[C] < prev[O]:
                    triggers.append("5m Bearish Engulfing")
                elif upper_wick / rng >= 0.50 and last[C] <= last[O]:
                    triggers.append("5m Bearish Pinbar")

    return triggers

def detect_impulse(candles, lookback=20, threshold=2.2):
    if len(candles) < lookback + 1:
        return None
    ranges = [c[H] - c[L] for c in candles]
    avg_range = sum(ranges[-lookback - 1:-1]) / lookback
    if avg_range == 0:
        return None
    last = candles[-1]
    last_range = last[H] - last[L]
    ratio = last_range / avg_range
    if ratio >= threshold:
        return {
            "ts": last[0],
            "direction": "yuqoriga 🚀" if last[C] > last[O] else "pastga 🩸",
            "range": round(last_range, 2),
            "ratio": round(ratio, 1),
            "price": round(last[C], 2),
        }
    return None

def calculate_atr(candles, period=14):
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        trs.append(max(
            candles[i][H] - candles[i][L],
            abs(candles[i][H] - candles[i - 1][C]),
            abs(candles[i][L] - candles[i - 1][C]),
        ))
    return sum(trs[-period:]) / period

def calculate_vwap(candles, lookback=48):
    cpv, cv = 0.0, 0.0
    for c in candles[-lookback:]:
        tp = (c[H] + c[L] + c[C]) / 3.0
        v = c[V] if len(c) > 5 else 0.0
        cpv += tp * v
        cv += v
    return cpv / cv if cv > 0 else None

# ==================== HTF CACHE, NY PDH/PDL, INDICATORS & MACRO SMT ====================
def compute_pdh_pdl(c1h, c1d):
    """Kechagi High/Low New York 17:00 server kuni bo'yicha hisoblanadi."""
    if _HAS_ZONEINFO and c1h and len(c1h) >= 30:
        try:
            ny = ZoneInfo("America/New_York")
            days = {}
            for c in c1h:
                dt = datetime.datetime.fromtimestamp(c[0] / 1000.0, tz=datetime.timezone.utc).astimezone(ny)
                key = (dt + datetime.timedelta(hours=7)).date()
                d = days.get(key)
                if d is None:
                    days[key] = [c[H], c[L]]
                else:
                    d[0] = max(d[0], c[H])
                    d[1] = min(d[1], c[L])
            keys = sorted(days)
            if len(keys) >= 2:
                prev = days[keys[-2]]
                return round(prev[0], 2), round(prev[1], 2)
        except Exception:
            pass
    if c1d and len(c1d) >= 2:
        prev_day = c1d[-2]
        return round(prev_day[H], 2), round(prev_day[L], 2)
    return None, None

async def fetch_htf_context(session):
    now_ts = time.time()
    cache = await db.state_get(
        "htf_cache",
        {"b1h": None, "b4h": None, "b1d": None, "pdh": None, "pdl": None, "ts": 0, "offset": None},
    )
    age = now_ts - cache.get("ts", 0)
    if 0 <= age < 60 and cache.get("offset") == _CURRENT_OFFSET:
        return cache

    tasks = [
        fetch_ohlcv(session, "1h", limit=HTF_LIMIT),
        fetch_ohlcv(session, "4h", limit=HTF_LIMIT),
        fetch_ohlcv(session, "1d", limit=HTF_LIMIT),
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    def safe_bias(idx, tf_name, cache_field):
        r = results[idx]
        if isinstance(r, Exception):
            return cache.get(cache_field)
        return confirmed_structure_bias(closed_only(r), cache_key=f"XAU_{tf_name}")

    b1h = safe_bias(0, "1h", "b1h")
    b4h = safe_bias(1, "4h", "b4h")
    b1d = safe_bias(2, "1d", "b1d")

    c1h = results[0] if not isinstance(results[0], Exception) else None
    c1d = results[2] if not isinstance(results[2], Exception) else None
    pdh, pdl = compute_pdh_pdl(c1h, c1d)
    if pdh is None:
        pdh, pdl = cache.get("pdh"), cache.get("pdl")

    data = {
        "b1h": b1h, "b4h": b4h, "b1d": b1d,
        "pdh": pdh, "pdl": pdl, "ts": now_ts, "offset": _CURRENT_OFFSET,
    }
    await db.state_set("htf_cache", data)
    return data

def mtf_alignment_score(b5, b15, b1h, b4h):
    score = 0
    if b5 and b15 and b5 == b15:
        score += 1
    if b15 and b1h and b15 == b1h:
        score += 1
    if b1h and b4h and b1h == b4h:
        score += 1
    return score

def calculate_rsi(candles, period=14):
    closes = [c[C] for c in candles]
    count = len(closes)
    if count < period + 1:
        return [None] * count
    rsis = [None] * period
    gains, losses = [], []
    for i in range(1, period + 1):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    rsis.append(100 if avg_loss == 0 else 100 - (100 / (1 + avg_gain / avg_loss)))
    for i in range(period + 1, count):
        diff = closes[i] - closes[i - 1]
        gain = max(diff, 0)
        loss = max(-diff, 0)
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
        rsis.append(100 if avg_loss == 0 else 100 - (100 / (1 + avg_gain / avg_loss)))
    return rsis

def detect_rsi_divergence(candles, rsis, lookback=24):
    if len(candles) < lookback or lookback < 7:
        return False, False
    window = candles[-lookback:]
    offset = len(candles) - lookback
    highs, lows = find_swings(window)
    bull, bear = False, False
    if len(lows) >= 2:
        fi, fp = lows[-2]
        si, sp = lows[-1]
        fr, sr = rsis[offset + fi], rsis[offset + si]
        if fr is not None and sr is not None and sp < fp and sr > fr:
            bull = True
    if len(highs) >= 2:
        fi, fp = highs[-2]
        si, sp = highs[-1]
        fr, sr = rsis[offset + fi], rsis[offset + si]
        if fr is not None and sr is not None and sp > fp and sr < fr:
            bear = True
    return bull, bear

def detect_liquidity_sweep(candles, cache_key="XAU_15m"):
    highs, lows = get_swings_cached(candles, cache_key=cache_key)
    if not highs or not lows:
        return None
    last_high = highs[-1][1]
    last_low = lows[-1][1]
    for c in candles[-3:]:
        if c[H] > last_high and c[C] < last_high:
            return "bearish_sweep"
        if c[L] < last_low and c[C] > last_low:
            return "bullish_sweep"
    return None

def _swing_near(swings, candles, ts, max_gap_ms):
    best = None
    for idx, price in swings:
        gap = abs(candles[idx][0] - ts)
        if gap <= max_gap_ms and (best is None or gap < best[0]):
            best = (gap, price)
    return best[1] if best else None

def detect_smt_divergence(gold_candles, other_candles, other_key):
    if not gold_candles or not other_candles:
        return None
    gh, gl = get_swings_cached(gold_candles, cache_key="XAU_15m")
    oh, ol = get_swings_cached(other_candles, cache_key=other_key)
    max_gap = 3 * 15 * 60 * 1000
    if len(gl) >= 2 and ol:
        t1, t2 = gold_candles[gl[-2][0]][0], gold_candles[gl[-1][0]][0]
        o1 = _swing_near(ol, other_candles, t1, max_gap)
        o2 = _swing_near(ol, other_candles, t2, max_gap)
        if o1 is not None and o2 is not None and gl[-1][1] < gl[-2][1] and o2 > o1:
            return "bullish"
    if len(gh) >= 2 and oh:
        t1, t2 = gold_candles[gh[-2][0]][0], gold_candles[gh[-1][0]][0]
        o1 = _swing_near(oh, other_candles, t1, max_gap)
        o2 = _swing_near(oh, other_candles, t2, max_gap)
        if o1 is not None and o2 is not None and gh[-1][1] > gh[-2][1] and o2 < o1:
            return "bearish"
    return None

def volume_profile(candles, bins=20, lookback=100):
    window = candles[-lookback:] if len(candles) >= lookback else candles
    if len(window) < 2:
        return None
    highs = [c[H] for c in window]
    lows = [c[L] for c in window]
    pmin, pmax = min(lows), max(highs)
    if pmax == pmin:
        return None
    bins = min(bins, max(2, len(window) // 2))
    bsize = (pmax - pmin) / bins
    profile = [0.0] * bins
    for c in window:
        v = c[V] if len(c) > 5 else 0
        idx = int((c[C] - pmin) / bsize)
        idx = max(0, min(bins - 1, idx))
        profile[idx] += v
    poc_idx = profile.index(max(profile))
    poc = pmin + (poc_idx + 0.5) * bsize
    total = sum(profile)
    target = total * 0.7
    acc = profile[poc_idx]
    up, dn = poc_idx, poc_idx
    while acc < target and (up < bins - 1 or dn > 0):
        upv = profile[up + 1] if up < bins - 1 else -1
        dnv = profile[dn - 1] if dn > 0 else -1
        if upv >= dnv and up < bins - 1:
            up += 1
            acc += profile[up]
        elif dn > 0:
            dn -= 1
            acc += profile[dn]
        else:
            break
    vah = pmin + (up + 1) * bsize
    val = pmin + dn * bsize
    return {"poc": round(poc, 2), "vah": round(vah, 2), "val": round(val, 2)}

def order_flow_delta(candles, lookback=20):
    delta = 0.0
    for c in candles[-lookback:]:
        rng = c[H] - c[L]
        if rng == 0:
            continue
        v = c[V] if len(c) > 5 else 0.0
        delta += ((c[C] - c[O]) / rng) * v
    return round(delta, 2)

def evaluate_institutional_score(
    struct15, price, ltf_triggers, mtf_score, vwap, bull_div, bear_div,
    btc_bias, eur_bias, smt_eur, smt_btc, zone_confluence, sweep15,
    pd_asia_info, eqh, eql, delta, vp, session_name
):
    bias = struct15["bias"]
    eq = struct15.get("eq")
    ote_top = struct15.get("ote_top")
    ote_bot = struct15.get("ote_bottom")
    event = struct15.get("event")

    score = 0
    confs = []

    if ltf_triggers:
        score += 20
        confs.append(" + ".join(ltf_triggers[:2]))

    if mtf_score >= 3:
        score += 15
        confs.append("MTF 3/3 (Full)")
    elif mtf_score == 2:
        score += 10
        confs.append("MTF 2/3")

    if ote_bot and ote_top and ote_bot <= price <= ote_top:
        score += 15
        confs.append("ICT OTE (0.618-0.786)")
    elif bias == "bullish" and eq and price <= eq:
        score += 10
        confs.append("ICT Discount (<50% EQ)")
    elif bias == "bearish" and eq and price >= eq:
        score += 10
        confs.append("ICT Premium (>50% EQ)")
    elif eq and ((bias == "bullish" and price > eq) or (bias == "bearish" and price < eq)):
        score -= 15
        confs.append("⚠️ Premium/Discount noto'g'ri tomonda (-15)")

    if bias == "bullish":
        if pd_asia_info.get("bull_sweep"):
            score += 15
            confs.append(pd_asia_info["bull_sweep"])
        elif sweep15 == "bullish_sweep":
            score += 12
            confs.append("15m SSL Sweep")
        if eqh and eqh > price:
            score += 5
            confs.append(f"Target BSL (EQH @ {eqh})")
    else:
        if pd_asia_info.get("bear_sweep"):
            score += 15
            confs.append(pd_asia_info["bear_sweep"])
        elif sweep15 == "bearish_sweep":
            score += 12
            confs.append("15m BSL Sweep")
        if eql and eql < price:
            score += 5
            confs.append(f"Target SSL (EQL @ {eql})")

    if zone_confluence:
        score += 8
        confs.append("FVG + OB/BB Confluence")
    if event and ("CHoCH" in event or "BOS" in event):
        score += 5
        confs.append(f"15m {event}")

    if bias == "bullish":
        if smt_eur == "bullish" or smt_btc == "bullish":
            score += 10
            confs.append("ICT SMT Divergence (Bullish)")
        elif btc_bias == "bullish" and eur_bias == "bullish":
            score += 6
            confs.append("SMT: BTC+EUR Bullish")
    else:
        if smt_eur == "bearish" or smt_btc == "bearish":
            score += 10
            confs.append("ICT SMT Divergence (Bearish)")
        elif btc_bias == "bearish" and eur_bias == "bearish":
            score += 6
            confs.append("SMT: BTC+EUR Bearish")

    if bias == "bullish":
        if bull_div:
            score += 6
            confs.append("RSI Bullish Div")
        if vwap and price > vwap:
            score += 4
            confs.append("VWAP ustida")
        if delta and delta > 0:
            score += 4
            confs.append(f"Delta (+{delta})")
        if vp and price <= vp["val"]:
            score += 4
            confs.append("VP VAL Discount")
    else:
        if bear_div:
            score += 6
            confs.append("RSI Bearish Div")
        if vwap and price < vwap:
            score += 4
            confs.append("VWAP ostida")
        if delta and delta < 0:
            score += 4
            confs.append(f"Delta ({delta})")
        if vp and price >= vp["vah"]:
            score += 4
            confs.append("VP VAH Premium")

    if session_name == "london_ny_overlap":
        score += 5
        confs.append("Killzone (London/NY overlap)")
    elif session_name in ("london", "newyork"):
        score += 3
        confs.append(f"Killzone ({session_name})")

    score = max(0, min(score, 100))
    grade = "A+" if score >= 80 else ("A" if score >= 68 else "B+")
    return score, grade, confs

_dxy_cache = {"ts": 0.0, "dxy_bias": None, "us10y_bias": None, "dxy_smt": None}

async def _fetch_yahoo_15m(session, ticker):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    async with session.get(url, params={"interval": "15m", "range": "5d"}, headers=HEADERS, timeout=T(8)) as r:
        r.raise_for_status()
        res = (await r.json())["chart"]["result"][0]
        ts_list = res.get("timestamp", [])
        q = res["indicators"]["quote"][0]
        out = []
        for i, t_sec in enumerate(ts_list):
            o, h, l, c = q["open"][i], q["high"][i], q["low"][i], q["close"][i]
            if None not in (o, h, l, c):
                out.append([int(t_sec * 1000), float(o), float(h), float(l), float(c), 0.0])
        return closed_only(out)

def detect_inverse_dxy_smt(gold_candles, dxy_candles):
    if not gold_candles or not dxy_candles:
        return None
    gh, gl = get_swings_cached(gold_candles, cache_key="XAU_15m")
    dh, dl = get_swings_cached(dxy_candles, cache_key="DXY_15m")
    max_gap = 3 * 15 * 60 * 1000
    if len(gl) >= 2 and dh:
        t1, t2 = gold_candles[gl[-2][0]][0], gold_candles[gl[-1][0]][0]
        d1, d2 = _swing_near(dh, dxy_candles, t1, max_gap), _swing_near(dh, dxy_candles, t2, max_gap)
        if d1 is not None and d2 is not None and gl[-1][1] < gl[-2][1] and d2 < d1:
            return "bullish"
    if len(gh) >= 2 and dl:
        t1, t2 = gold_candles[gh[-2][0]][0], gold_candles[gh[-1][0]][0]
        d1, d2 = _swing_near(dl, dxy_candles, t1, max_gap), _swing_near(dl, dxy_candles, t2, max_gap)
        if d1 is not None and d2 is not None and gh[-1][1] > gh[-2][1] and d2 > d1:
            return "bearish"
    return None

async def get_macro_dxy_context(session, gold_cl15):
    now_m = time.monotonic()
    if now_m - _dxy_cache["ts"] < 120:
        return _dxy_cache
    dxy_c, tnx_c = await asyncio.gather(
        _fetch_yahoo_15m(session, "DX-Y.NYB"),
        _fetch_yahoo_15m(session, "^TNX"),
        return_exceptions=True,
    )
    dxy_ok = dxy_c if not isinstance(dxy_c, Exception) else None
    tnx_ok = tnx_c if not isinstance(tnx_c, Exception) else None
    _dxy_cache.update({
        "ts": now_m,
        "dxy_bias": confirmed_structure_bias(dxy_ok, cache_key="DXY_15m") if dxy_ok else _dxy_cache["dxy_bias"],
        "us10y_bias": confirmed_structure_bias(tnx_ok, cache_key="TNX_15m") if tnx_ok else _dxy_cache["us10y_bias"],
        "dxy_smt": detect_inverse_dxy_smt(gold_cl15, dxy_ok) if dxy_ok else None,
    })
    return _dxy_cache

async def _fetch_corr(session, symbol, key):
    if time.monotonic() < _smt_disabled_until.get(key, 0):
        return None
    try:
        return closed_only(await fetch_ohlcv(session, "15m", symbol=symbol))
    except Exception as e:
        _smt_disabled_until[key] = time.monotonic() + 600
        logger.warning(f"{symbol} olinmadi, SMT 10 daqiqaga o'chirildi: {_safe_log_error(e)}")
        return None

# ==================== 20$ SNIPER TRADE BUILDER ====================
def build_sniper_trade(cl15, cl5, struct15, price, atr15, balance):
    bias = struct15["bias"]
    zones_bias = [z for z in get_active_zones(cl15, "XAU_15m") if z["type"] == bias]
    if not zones_bias:
        return None

    tol = min(price * (ZONE_MAX_DISTANCE_PCT / 100.0), ZONE_TOL_MAX_POINTS)
    candidates = []
    for z in zones_bias:
        lo, hi = min(z["bottom"], z["top"]), max(z["bottom"], z["top"])
        if lo - tol <= price <= hi + tol:
            candidates.append(z)
    if not candidates:
        return None

    candidates.sort(key=lambda z: abs(price - z["ce"]))
    zone = candidates[0]
    zone_lo = min(zone["bottom"], zone["top"])
    zone_hi = max(zone["bottom"], zone["top"])

    confluence = any(
        other is not zone
        and other["kind"] != zone["kind"]
        and other["bottom"] <= zone["top"]
        and other["top"] >= zone["bottom"]
        for other in zones_bias
    )

    is_small_acc = (balance or DEFAULT_BALANCE_USD) <= 60.0
    max_sl_pts = SNIPER_MAX_SL_POINTS_SMALL if is_small_acc else (atr15 * 2.5 if atr15 else 6.0)
    min_sl_pts = SNIPER_MIN_SL_POINTS

    recent_5m_low = min(c[L] for c in cl5[-4:]) if len(cl5) >= 4 else zone_lo
    recent_5m_high = max(c[H] for c in cl5[-4:]) if len(cl5) >= 4 else zone_hi
    entry = round(price, 2)

    if bias == "bullish":
        structural_sl = min(zone_lo - 0.30, recent_5m_low - 0.25) - XM_SPREAD_BUFFER
        raw_risk = entry - structural_sl
    else:
        structural_sl = max(zone_hi + 0.30, recent_5m_high + 0.25) + XM_SPREAD_BUFFER
        raw_risk = structural_sl - entry

    if raw_risk <= 0:
        return None
    if raw_risk > max_sl_pts and SNIPER_SKIP_IF_SL_TOO_WIDE:
        return None

    risk = min(max(raw_risk, min_sl_pts), max_sl_pts)
    direction = 1 if bias == "bullish" else -1
    sl = round(entry - direction * risk, 2)
    return {
        "signal": "LONG" if bias == "bullish" else "SHORT",
        "bias": bias,
        "zone_kind": zone["kind"],
        "zone_top": round(zone_hi, 2),
        "zone_bottom": round(zone_lo, 2),
        "entry": entry, "sl": sl, "initial_sl": sl,
        "tp1": round(entry + direction * risk * RR1, 2),
        "tp2": round(entry + direction * risk * RR2, 2),
        "tp3": round(entry + direction * risk * RR3, 2),
        "confluence": confluence,
        "sniper_mode": is_small_acc,
        "opened_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "breakeven": False, "tp1_notified": False, "tp2_notified": False, "stale_warned": False,
    }

def calc_xm_lot_and_risk(entry, sl, balance, risk_pct):
    bal = balance or DEFAULT_BALANCE_USD
    sl_dist = abs(entry - sl)
    if sl_dist <= 0:
        return XM_MIN_LOT, 0.0

    target_risk_usd = bal * (risk_pct / 100.0)
    raw_lots = (target_risk_usd / sl_dist) * XM_MIN_LOT
    lot = max(XM_MIN_LOT, math.floor(raw_lots / XM_MIN_LOT + 1e-9) * XM_MIN_LOT)
    lot = round(lot, 2)
    actual_risk_usd = round((lot / XM_MIN_LOT) * sl_dist * XM_OZ_PER_MIN_LOT, 2)
    return lot, actual_risk_usd

# ==================== BINANCE AVTO-TRADE ====================
def _make_binance_client(api_key, api_secret):
    client = ccxt_async.binance({
        "apiKey": api_key,
        "secret": api_secret,
        "enableRateLimit": True,
        "options": {"defaultType": "spot"},
    })
    if BINANCE_TESTNET:
        client.set_sandbox_mode(True)
    return client

async def _binance_autotrade_one(session, acc, trade):
    chat_id = acc["chat_id"]
    try:
        api_key = decrypt_secret(acc["api_key_enc"])
        api_secret = decrypt_secret(acc["api_secret_enc"])
    except Exception:
        return

    client = _make_binance_client(api_key, api_secret)
    try:
        balance_info = await client.fetch_balance()
        usdt_free = float(((balance_info.get("USDT") or {}).get("free")) or 0.0)
        if usdt_free <= 1.0:
            return
        risk_pct = min(acc["risk_pct"] or 2.0, AUTO_TRADE_MAX_RISK_PCT)
        risk_usd = usdt_free * (risk_pct / 100.0)
        ticker = await client.fetch_ticker(BINANCE_SYMBOL)
        px = float(ticker["last"])
        sl_dist_pct = abs(trade["entry"] - trade["sl"]) / trade["entry"]
        if sl_dist_pct <= 0:
            return
        quote_amount = min(risk_usd / sl_dist_pct, usdt_free * 0.95)
        amount = float(client.amount_to_precision(BINANCE_SYMBOL, quote_amount / px))
        buy_order = await client.create_order(BINANCE_SYMBOL, "market", "buy", amount)
        filled_px = float(buy_order.get("average") or buy_order.get("price") or px)
        sl_px = round(filled_px * (1 - sl_dist_pct), 6)
        tp_px = round(filled_px * (1 + sl_dist_pct * RR1), 6)
        await send_telegram(
            session,
            f"🤖 <b>Binance Auto-Trade</b> — LONG {html.escape(BINANCE_SYMBOL)}\n"
            f"Hajm: <code>{amount}</code> @ ~<code>{filled_px}</code> | SL: <code>{sl_px}</code> | TP1: <code>{tp_px}</code>",
            chat_id=chat_id,
        )
    except Exception as e:
        logger.error(f"Binance auto-trade xato ({chat_id}): {_safe_log_error(e)}")
    finally:
        try:
            await client.close()
        except Exception:
            pass

async def execute_binance_autotrades(session, trade):
    if not AUTO_TRADE_AVAILABLE or trade.get("signal") != "LONG":
        return
    accs = await db.get_autotrade_accounts(broker="binance")
    for acc in accs:
        _spawn(_binance_autotrade_one(session, acc, dict(trade)))

# ==================== TELEGRAM (Text + Photo + Delete) ====================
def _split_text(text, limit=3900):
    if len(text) <= limit:
        return [text]
    chunks, cur = [], ""
    for line in text.split("\n"):
        while len(line) > limit:
            if cur:
                chunks.append(cur)
                cur = ""
            chunks.append(line[:limit])
            line = line[limit:]
        if len(cur) + len(line) + 1 > limit:
            chunks.append(cur)
            cur = line
        else:
            cur = f"{cur}\n{line}" if cur else line
    if cur:
        chunks.append(cur)
    return chunks

def _strip_html(text):
    return html.unescape(re.sub(r"<[^>]+>", "", text))

async def _allowed_ids():
    return set(await db.state_get("allowed", [])) | ALLOWED_ENV | {ADMIN_CHAT_ID}

async def is_allowed(cid):
    return ALLOW_PUBLIC or str(cid) in await _allowed_ids()

async def delete_telegram_message(session, chat_id, message_id):
    if not chat_id or not message_id:
        return
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/deleteMessage"
    try:
        async with session.post(url, data={"chat_id": chat_id, "message_id": message_id}, timeout=T(5)):
            pass
    except Exception:
        pass

async def send_telegram(session, text, with_keyboard=False, chat_id=None, parse_mode="HTML"):
    if chat_id:
        targets = [chat_id]
    else:
        targets = list(await get_subs())
        if not ALLOW_PUBLIC:
            allowed = await _allowed_ids()
            targets = [t for t in targets if str(t) in allowed]
    if not targets:
        return

    kb_rows = [
        ["📊 Signal", "🖼 Grafik", "🧠 AI Tahlil"],
        ["🏦 Exness Hisoblar", "🎯 Zonalar", "📈 Statistika"],
    ]
    if WEBAPP_URL:
        kb_rows.insert(0, [{"text": "📱 Mini App Boshqaruv Paneli", "web_app": {"url": WEBAPP_URL}}])

    markup = json.dumps({
        "keyboard": kb_rows,
        "resize_keyboard": True,
    }) if with_keyboard else None
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    chunks = _split_text(text)

    for target in targets:
        for ci, chunk in enumerate(chunks):
            use_mode = parse_mode
            for attempt in range(3):
                payload = {"chat_id": target, "text": chunk if use_mode else _strip_html(chunk)}
                if use_mode:
                    payload["parse_mode"] = use_mode
                if markup and ci == len(chunks) - 1:
                    payload["reply_markup"] = markup
                try:
                    async with session.post(url, data=payload, timeout=T(10)) as r:
                        if r.status == 429:
                            data = await r.json()
                            await asyncio.sleep(data.get("parameters", {}).get("retry_after", 1))
                            continue
                        if r.status == 200:
                            break
                        _metrics["tg_errors"] += 1
                        if r.status == 400 and use_mode:
                            use_mode = None
                            continue
                        break
                except Exception as e:
                    logger.error(f"TG exception ({target}): {_safe_log_error(e)}")
                    _metrics["tg_errors"] += 1
                    break
        await asyncio.sleep(0.05)

async def send_telegram_photo(session, png_bytes, caption="", chat_id=None):
    if not png_bytes:
        return
    if chat_id:
        targets = [chat_id]
    else:
        targets = list(await get_subs())
        if not ALLOW_PUBLIC:
            allowed = await _allowed_ids()
            targets = [t for t in targets if str(t) in allowed]
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendPhoto"

    for target in targets:
        use_html = True
        for attempt in range(3):
            try:
                form = aiohttp.FormData()
                form.add_field("chat_id", str(target))
                form.add_field("caption", (caption if use_html else _strip_html(caption))[:1024])
                if use_html:
                    form.add_field("parse_mode", "HTML")
                form.add_field("photo", png_bytes, filename="xm_gold_smc.png", content_type="image/png")
                async with session.post(url, data=form, timeout=T(20)) as r:
                    if r.status == 200:
                        break
                    if r.status == 429:
                        data = await r.json()
                        await asyncio.sleep(data.get("parameters", {}).get("retry_after", 1))
                        continue
                    if r.status == 400 and use_html:
                        use_html = False
                        continue
                    break
            except Exception as e:
                logger.error(f"TG photo xato ({target}): {_safe_log_error(e)}")
                break
        await asyncio.sleep(0.05)

# ==================== TRADING MANAGEMENT & SAFE OFFSET SHIFT ====================
async def apply_offset_change(new_off):
    """Offset o'zgarganda ochiq bitim darajalari ham siljitiladi va Exness SL yangilanadi."""
    global _CURRENT_OFFSET, _LIVE_PRICE
    new_sl_to_sync = None
    async with _monitor_lock:
        delta = round(new_off - _CURRENT_OFFSET, 2)
        _CURRENT_OFFSET = new_off
        if _LIVE_BYBIT_RAW_PRICE:
            _LIVE_PRICE = round(_LIVE_BYBIT_RAW_PRICE + _CURRENT_OFFSET, 2)
        await db.state_set("price_offset", _CURRENT_OFFSET)
        t = await db.state_get("current_trade")
        if t and delta:
            for k in ("entry", "sl", "initial_sl", "tp1", "tp2", "tp3",
                      "zone_top", "zone_bottom", "last_trail_note_sl"):
                if t.get(k) is not None:
                    t[k] = round(t[k] + delta, 2)
            new_sl_to_sync = t.get("sl")
            await db.state_set("current_trade", t)
    clear_analysis_caches()
    if new_sl_to_sync is not None:
        _spawn(sync_exness_positions_action(action="modify_sl", new_sl=new_sl_to_sync))
    return delta

async def close_trade(session, trade, result, price):
    """Chaqiruvchi _monitor_lock ni ushlab turishi kerak."""
    global _flip_since, _flip_last_warn
    acc = await get_account()
    init_sl = trade.get("initial_sl", trade["sl"])
    lot = trade.get("lot")
    usd_risk = trade.get("risk_usd")
    if not lot or usd_risk is None:
        lot, usd_risk = calc_xm_lot_and_risk(trade["entry"], init_sl, acc["balance"], acc["risk_pct"])

    try:
        await db.insert_trade(trade, result, price, lot, usd_risk, trade.get("confirmations", ""))
    except Exception as e:
        logger.error(f"Savdoni bazaga yozishda xato: {_safe_log_error(e)}")
    await db.state_set("current_trade", None)
    _flip_since, _flip_last_warn = None, float("-inf")

    _spawn(sync_exness_positions_action(action="close"))

    _metrics["closed"] += 1
    if _is_win_result(result):
        _metrics["wins"] += 1
    elif result == "SL":
        _metrics["losses"] += 1

    pnl_pts = (price - trade["entry"]) if trade["signal"] == "LONG" else (trade["entry"] - price)
    pnl_usd = round((lot / XM_MIN_LOT) * pnl_pts * XM_OZ_PER_MIN_LOT, 2)
    r_mult = pnl_pts / max(abs(trade["entry"] - init_sl), 0.01)

    emoji = {
        "TP2": "✅🏆", "TP3": "🚀👑", "TP1 + Trailing SL": "✅🛡", "TP2 + Trailing SL": "✅🏆",
        "BE+": "➖✅", "SL": "❌", "BE": "➖", "MANUAL": "🛑",
    }.get(result, "ℹ️")
    stat = await win_rate_text()
    await send_telegram(
        session,
        f"<b>GOLD {trade['signal']} YOPILDI</b> {emoji}\n"
        f"Natija: <b>{html.escape(result)}</b> | Narx: <code>{price}</code>\n"
        f"💰 <b>Natija ({lot} lot):</b> <code>{pnl_usd:+.2f}$</code> ({r_mult:+.2f}R)\n"
        f"📊 {html.escape(stat)}",
    )
    if result == "SL" and GEMINI_API_KEY:
        _spawn(_ai_post_mortem(session, dict(trade), price))

async def monitor_trade(session, price, bias15):
    global _flip_since, _flip_last_warn
    if _monitor_lock.locked():
        return
    async with _monitor_lock:
        trade = await db.state_get("current_trade")
        if not trade:
            return
        before = json.dumps(trade, sort_keys=True)
        old_sl = trade["sl"]

        side = trade["signal"]
        is_long = side == "LONG"
        entry = trade["entry"]
        tp1, tp2, tp3 = trade["tp1"], trade["tp2"], trade.get("tp3")
        init_sl = trade.get("initial_sl", trade["sl"])
        risk_pts = max(abs(entry - init_sl), 0.01)
        notes = []
        to_close = None

        def reached(level):
            return price >= level if is_long else price <= level

        def better_sl(new, old):
            return new > old if is_long else new < old

        if not trade.get("stale_warned") and trade.get("opened_at"):
            try:
                opened = datetime.datetime.fromisoformat(trade["opened_at"])
                age_h = (datetime.datetime.now(datetime.timezone.utc) - opened).total_seconds() / 3600.0
                if age_h >= TRADE_STALE_WARNING_HOURS:
                    trade["stale_warned"] = True
                    notes.append(f"⏳ <b>{side}</b> bitim {age_h:.1f} soatdan beri ochiq.")
            except Exception:
                trade["stale_warned"] = True

        if reached(tp1) and not trade.get("tp1_notified"):
            trade["tp1_notified"] = True
            trade["breakeven"] = True
            be_lock = max(abs(tp1 - entry) * 0.25, 0.30)
            new_sl = round(entry + be_lock, 2) if is_long else round(entry - be_lock, 2)
            if better_sl(new_sl, trade["sl"]):
                trade["sl"] = new_sl
            notes.append(
                f"🎯 <b>TP1 OLINDI ({tp1})!</b>\n"
                f"🛡 SL foydali darajaga (BE+) ko'chirildi: <code>{trade['sl']}</code>"
            )

        if reached(tp2) and not trade.get("tp2_notified"):
            trade["tp2_notified"] = True
            if better_sl(tp1, trade["sl"]):
                trade["sl"] = round(tp1, 2)
            notes.append(
                f"🏆 <b>TP2 OLINDI ({tp2})!</b>\n"
                f"🔒 SL TP1 ga ko'chirildi: <code>{trade['sl']}</code> | Maqsad TP3: <code>{tp3}</code>"
            )

        if trade.get("tp1_notified") and ((is_long and price > tp1) or (not is_long and price < tp1)):
            trail_sl = round(price - risk_pts, 2) if is_long else round(price + risk_pts, 2)
            if better_sl(trail_sl, trade["sl"]) and abs(trail_sl - trade["sl"]) > 0.35:
                trade["sl"] = trail_sl
                last_noted = trade.get("last_trail_note_sl")
                if last_noted is None or abs(trail_sl - last_noted) >= 1.0:
                    trade["last_trail_note_sl"] = trail_sl
                    notes.append(f"🔥 <b>{side} Trailing SL:</b> <code>{trade['sl']}</code>")

        sl_hit = (is_long and price <= trade["sl"]) or ((not is_long) and price >= trade["sl"])
        tp3_hit = bool(tp3) and reached(tp3)

        if sl_hit:
            sl_price = trade["sl"]
            pnl_pts = (sl_price - entry) if is_long else (entry - sl_price)
            if trade.get("tp2_notified"):
                label = "TP2 + Trailing SL"
            elif trade.get("tp1_notified"):
                label = "TP1 + Trailing SL" if pnl_pts >= 0.5 * risk_pts else "BE+"
            else:
                label = "SL"
            to_close = (label, sl_price)
        elif tp3_hit:
            to_close = ("TP3", tp3)

        if not to_close:
            if bias15 is not None and bias15 != trade["bias"]:
                now_m = time.monotonic()
                if _flip_since is None:
                    _flip_since = now_m
                if (now_m - _flip_since >= FLIP_WARN_AFTER_SEC
                        and now_m - _flip_last_warn >= FLIP_WARN_REPEAT_SEC):
                    _flip_last_warn = now_m
                    notes.append(
                        f"⚠️ <b>Diqqat:</b> {side} ochiq, lekin 15m struktura <b>{bias15.upper()}</b> ga o'zgardi!"
                    )
            else:
                _flip_since = None

        if json.dumps(trade, sort_keys=True) != before:
            await db.state_set("current_trade", trade)
            if trade["sl"] != old_sl and not to_close:
                _spawn(sync_exness_positions_action(action="modify_sl", new_sl=trade["sl"]))
        for n in notes:
            await send_telegram(session, n)
        if to_close:
            await close_trade(session, trade, *to_close)

# ==================== NEWS ====================
_news_cache = {"events": None, "fetched_at": None, "failed": False}
NEWS_CACHE_TTL_SEC = 3600
NEWS_CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
NEWS_BLOCK_MINUTES_BEFORE = 30
NEWS_BLOCK_MINUTES_AFTER = 30

async def fetch_high_impact_news(session):
    now = datetime.datetime.now(datetime.timezone.utc)
    cached = _news_cache.get("events")
    fetched_at = _news_cache.get("fetched_at")
    if cached is not None and fetched_at and (now - fetched_at).total_seconds() < NEWS_CACHE_TTL_SEC:
        return cached
    events = []
    try:
        async with session.get(NEWS_CALENDAR_URL, headers=HEADERS, timeout=T(10)) as r:
            r.raise_for_status()
            data = await r.json(content_type=None)
            for item in data:
                if item.get("country") != "USD" or item.get("impact") != "High":
                    continue
                date_str = item.get("date")
                if not date_str:
                    continue
                try:
                    dt = datetime.datetime.fromisoformat(date_str.replace("Z", "+00:00")).astimezone(datetime.timezone.utc)
                    events.append({"title": item.get("title", "USD News"), "time": dt})
                except Exception:
                    continue
        _news_cache["events"] = events
        _news_cache["fetched_at"] = now
        _news_cache["failed"] = False
    except Exception as e:
        logger.warning(f"News fetch xato: {_safe_log_error(e)}")
        _news_cache["fetched_at"] = now - datetime.timedelta(seconds=NEWS_CACHE_TTL_SEC - NEWS_RETRY_SEC)
        _news_cache["failed"] = True
        if cached is not None:
            return cached
    return events

async def check_news_window(session):
    events = await fetch_high_impact_news(session)
    if not events:
        return False, None
    now = datetime.datetime.now(datetime.timezone.utc)
    for ev in events:
        diff_min = (ev["time"] - now).total_seconds() / 60.0
        if -NEWS_BLOCK_MINUTES_AFTER <= diff_min <= NEWS_BLOCK_MINUTES_BEFORE:
            return True, f"{ev['title']} ({int(abs(diff_min))} daq)"
    return False, None

# ==================== UNIFIED INSTITUTIONAL MARKET ANALYSIS ====================
async def analyze_gold_market(session, run_deep_scoring=True):
    global _LAST_CANDLES_15M, _LAST_STRUCT_15M, _LIVE_BYBIT_RAW_PRICE
    c5, c15 = await asyncio.gather(
        fetch_ohlcv(session, "5m", limit=LIMIT),
        fetch_ohlcv(session, "15m", limit=LIMIT),
    )
    htf = await fetch_htf_context(session)
    b1h, b4h, b1d = htf["b1h"], htf["b4h"], htf["b1d"]
    pdh, pdl = htf["pdh"], htf["pdl"]

    cl5 = closed_only(c5)
    cl15 = closed_only(c15)
    _LAST_CANDLES_15M = cl15

    rest_xm_price = round(c15[-1][C], 2)
    if _LIVE_BYBIT_RAW_PRICE is None:
        _LIVE_BYBIT_RAW_PRICE = round(rest_xm_price - _CURRENT_OFFSET, 2)
    price = live_price() or rest_xm_price

    struct5 = analyze_market_structure(cl5, cache_key="XAU_5m")
    struct15 = analyze_market_structure(cl15, cache_key="XAU_15m")
    _LAST_STRUCT_15M = struct15

    b5 = struct5["bias"]
    b15 = struct15["bias"]
    sweeps = detect_asia_and_pd_sweeps(cl15, pdh, pdl)
    eqh, eql = detect_equal_highs_lows(cl15, cache_key="XAU_15m")
    atr15 = calculate_atr(cl15)
    vwap15 = calculate_vwap(cl15)
    zones15 = get_active_zones(cl15, cache_key="XAU_15m")
    news_blocked, news_title = await check_news_window(session)
    session_name = current_session()

    acc = await get_account()
    candidate = None
    confirmations = []
    score = 0

    if run_deep_scoring and b15 in ("bullish", "bearish") and not (b1h and b4h and b1h != b15 and b4h != b15):
        candidate = build_sniper_trade(cl15, cl5, struct15, price, atr15, acc["balance"])
        if candidate:
            ltf_triggers = check_5m_ltf_trigger(
                cl5, struct5, b15,
                zone={"top": candidate["zone_top"], "bottom": candidate["zone_bottom"]},
            )
            if not ltf_triggers:
                candidate = None
            else:
                rsis15 = calculate_rsi(cl15)
                bull_div, bear_div = detect_rsi_divergence(cl15, rsis15)
                sweep15 = detect_liquidity_sweep(cl15, cache_key="XAU_15m")
                delta15 = order_flow_delta(cl15)
                vp15 = volume_profile(cl15)

                btc_cl, eur_cl = await asyncio.gather(
                    _fetch_corr(session, BTC_SYMBOL, "BTC"),
                    _fetch_corr(session, EUR_SYMBOL, "EUR"),
                )
                btc_bias = confirmed_structure_bias(btc_cl, cache_key="BTC_15m") if btc_cl else None
                eur_bias = confirmed_structure_bias(eur_cl, cache_key="EUR_15m") if eur_cl else None
                smt_btc = detect_smt_divergence(cl15, btc_cl, "BTC_15m") if btc_cl else None
                smt_eur = detect_smt_divergence(cl15, eur_cl, "EUR_15m") if eur_cl else None

                mtf = mtf_alignment_score(b5, b15, b1h, b4h)
                score, grade, confirmations = evaluate_institutional_score(
                    struct15, price, ltf_triggers, mtf, vwap15, bull_div, bear_div,
                    btc_bias, eur_bias, smt_eur, smt_btc, candidate["confluence"],
                    sweep15, sweeps, eqh, eql, delta15, vp15, session_name
                )

                macro = await get_macro_dxy_context(session, cl15)
                if b15 == "bullish":
                    if macro.get("dxy_smt") == "bullish":
                        score += 8
                        confirmations.append("Macro SMT: DXY Inverse Bullish (+8)")
                    elif macro.get("dxy_bias") == "bearish":
                        score += 4
                        confirmations.append("DXY Bearish Bias (+4)")
                    if macro.get("us10y_bias") == "bearish":
                        score += 3
                        confirmations.append("US10Y Yield Bearish (+3)")
                else:
                    if macro.get("dxy_smt") == "bearish":
                        score += 8
                        confirmations.append("Macro SMT: DXY Inverse Bearish (+8)")
                    elif macro.get("dxy_bias") == "bullish":
                        score += 4
                        confirmations.append("DXY Bullish Bias (+4)")
                    if macro.get("us10y_bias") == "bullish":
                        score += 3
                        confirmations.append("US10Y Yield Bullish (+3)")

                adj, mem_notes = await get_learned_adjustments(confirmations)
                if adj != 0:
                    score += adj
                    confirmations.extend(mem_notes)

                score = max(0, min(100, score))
                grade = "A+" if score >= 80 else ("A" if score >= 68 else "B+")

                lot, usd_risk = calc_xm_lot_and_risk(
                    candidate["entry"], candidate["sl"], acc["balance"], acc["risk_pct"]
                )
                candidate["score"] = score
                candidate["grade"] = grade
                candidate["lot"] = lot
                candidate["risk_usd"] = usd_risk
                candidate["confirmations"] = ", ".join(confirmations)

    return {
        "price": price,
        "raw_bybit": _LIVE_BYBIT_RAW_PRICE,
        "offset": _CURRENT_OFFSET,
        "b5": b5, "b15": b15, "b1h": b1h, "b4h": b4h, "b1d": b1d,
        "pdh": pdh, "pdl": pdl,
        "asia_high": sweeps["asia_high"], "asia_low": sweeps["asia_low"],
        "bull_sweep": sweeps["bull_sweep"], "bear_sweep": sweeps["bear_sweep"],
        "eq15": struct15.get("eq"),
        "ote_top": struct15.get("ote_top"), "ote_bottom": struct15.get("ote_bottom"),
        "dealing_high": struct15.get("dealing_high"), "dealing_low": struct15.get("dealing_low"),
        "event15": struct15.get("event"),
        "eqh": eqh, "eql": eql,
        "atr15": round(atr15, 2) if atr15 else None,
        "vwap15": round(vwap15, 2) if vwap15 else None,
        "zones15": zones15,
        "session": session_name,
        "in_killzone": in_killzone(),
        "news_blocked": news_blocked,
        "news_title": news_title,
        "candidate": candidate,
        "score": score,
        "confirmations": confirmations,
        "cl15": cl15,
        "struct15": struct15,
    }

# ==================== MESSAGE FORMATTERS ====================
def format_signal_message(trade, status, acc):
    side_emoji = "🟢 <b>LONG (BUY)</b>" if trade["signal"] == "LONG" else "🔴 <b>SHORT (SELL)</b>"
    sl_pts = abs(trade["entry"] - trade["sl"])
    tp1_pts = abs(trade["tp1"] - trade["entry"])
    tp2_pts = abs(trade["tp2"] - trade["entry"])
    tp3_pts = abs(trade["tp3"] - trade["entry"])

    lines = [
        f"🎯 <b>GOLD SNIPER SIGNAL — v19 ULTIMATE</b>",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"Yo'nalish: {side_emoji} | Daraja: <b>{trade.get('grade', 'A')} ({trade.get('score', 0)}/100)</b>",
        f"Zona: <b>15m {html.escape(trade['zone_kind'])}</b> (<code>{trade['zone_bottom']} — {trade['zone_top']}</code>)",
        f"",
        f"📍 <b>Kirish (Entry):</b> <code>{trade['entry']}</code>",
        f"🛑 <b>Stop Loss:</b> <code>{trade['sl']}</code> (<i>-{sl_pts:.2f}$ / -{trade['risk_usd']:.2f}$</i>)",
        f"✅ <b>TP1 ({RR1}R):</b> <code>{trade['tp1']}</code> (<i>+{tp1_pts:.2f}$</i>)",
        f"🏆 <b>TP2 ({RR2}R):</b> <code>{trade['tp2']}</code> (<i>+{tp2_pts:.2f}$</i>)",
        f"🚀 <b>TP3 ({RR3}R):</b> <code>{trade['tp3']}</code> (<i>+{tp3_pts:.2f}$</i>)",
        f"",
        f"💼 <b>Lot (Balans ${acc['balance']:.0f}):</b> <code>{trade['lot']}</code> lot (Risk: {acc['risk_pct']:.1f}%)",
        f"🧭 <b>MTF:</b> 5m: <code>{status['b5']}</code> | 15m: <code>{status['b15']}</code> | 1H: <code>{status['b1h']}</code> | 4H: <code>{status['b4h']}</code>",
        f"⚖️ <b>EQ (50%):</b> <code>{status['eq15']}</code> | <b>PDH/PDL:</b> <code>{status['pdh']}</code> / <code>{status['pdl']}</code>",
        f"",
        f"🔥 <b>Tasdiqlar:</b> <i>{html.escape(trade.get('confirmations', '-'))}</i>",
    ]
    return "\n".join(lines)

async def format_status_message(status):
    acc = await get_account()
    trade = await db.state_get("current_trade")
    ex_accs = await db.get_exness_accounts(ADMIN_CHAT_ID)
    ws_badge = "🟢 Live WS" if _WS_CONNECTED else "🟡 REST"
    kz_badge = "🔥 Faol" if status["in_killzone"] else "💤 Tinch"
    news_txt = f"⛔ {html.escape(status['news_title'])}" if status["news_blocked"] else "✅ Tinch"

    lines = [
        f"📊 <b>XAUUSD (EXNESS/XM) BOZOR HOLATI</b> ({ws_badge})",
        f"━━━━━━━━━━━━━━━━━━━━",
        f"💰 <b>Joriy narx:</b> <code>{status['price']}</code> (Offset: <code>{status['offset']:+.2f}</code>)",
        f"🕒 <b>Sessiya:</b> <code>{status['session'].upper()}</code> ({kz_badge}) | <b>Yangilik:</b> {news_txt}",
        f"",
        f"📐 <b>Struktura (MTF):</b>",
        f" • 5m: <b>{status['b5'] or '-'}</b> | 15m: <b>{status['b15'] or '-'}</b> ({status['event15'] or '-'})",
        f" • 1H: <b>{status['b1h'] or '-'}</b> | 4H: <b>{status['b4h'] or '-'}</b> | 1D: <b>{status['b1d'] or '-'}</b>",
        f"",
        f"🎯 <b>ICT Darajalar:</b>",
        f" • Dealing Range: <code>{status['dealing_low']}</code> — <code>{status['dealing_high']}</code>",
        f" • Equilibrium (50%): <code>{status['eq15']}</code> | VWAP: <code>{status['vwap15']}</code>",
        f" • OTE (61.8-78.6%): <code>{status['ote_bottom']}</code> — <code>{status['ote_top']}</code>",
        f" • Kecha (NY PDH / PDL): <code>{status['pdh']}</code> / <code>{status['pdl']}</code>",
        f" • Asia High / Low: <code>{status['asia_high']}</code> / <code>{status['asia_low']}</code>",
        f" • Likvidlik (BSL / SSL): EQH: <code>{status['eqh'] or '-'}</code> | EQL: <code>{status['eql'] or '-'}</code>",
    ]

    if trade:
        pnl_pts = (status["price"] - trade["entry"]) if trade["signal"] == "LONG" else (trade["entry"] - status["price"])
        lot = trade.get("lot", XM_MIN_LOT)
        pnl_usd = round((lot / XM_MIN_LOT) * pnl_pts * XM_OZ_PER_MIN_LOT, 2)
        lines.extend([
            f"",
            f"⚡ <b>OCHIQ BITIM ({trade['signal']}):</b>",
            f" • Entry: <code>{trade['entry']}</code> | Joriy PnL: <b>{pnl_pts:+.2f}$ ({pnl_usd:+.2f}$)</b>",
            f" • SL: <code>{trade['sl']}</code> | TP1: <code>{trade['tp1']}</code> | TP2: <code>{trade['tp2']}</code>",
        ])
    else:
        lines.append("\n📭 <b>Ochiq bitim:</b> Yo'q (Sniper zona kutilmoqda)")

    lines.append(f"\n🏦 <b>Exness hisoblar:</b> {len(ex_accs)}/{MAX_EXNESS_ACCOUNTS} ulangan | Balans: <code>${acc['balance']:.2f}</code>")
    return "\n".join(lines)

# ==================== MAIN SCANNER LOOP ====================
async def scanner_loop(app):
    session = app["session"]
    last_impulse_ts = 0
    last_signal_bar_ts = 0

    while True:
        try:
            if not market_open():
                await asyncio.sleep(30)
                continue

            status = await analyze_gold_market(session, run_deep_scoring=True)
            price = status["price"]
            cl15 = status["cl15"]

            # 1) Impulse / keskin harakat haqida ogohlantirish
            imp = detect_impulse(cl15)
            if imp and imp["ts"] != last_impulse_ts:
                last_impulse_ts = imp["ts"]
                await send_telegram(
                    session,
                    f"⚡ <b>IMPULS HARAKAT ANIQLANDI!</b>\n"
                    f"Yo'nalish: <b>{imp['direction']}</b> | Sham hajmi: <code>{imp['range']}$</code> (o'rtachadan {imp['ratio']}x katta)\n"
                    f"Joriy narx: <code>{imp['price']}</code>",
                )

            # 2) Ochiq bitimni tekshirish
            trade = await db.state_get("current_trade")
            if trade:
                await monitor_trade(session, price, status["b15"])
            else:
                # 3) Yangi Sniper signal tekshirish
                cand = status.get("candidate")
                cur_bar_ts = cl15[-1][0] if cl15 else 0
                if (
                    cand
                    and cand.get("score", 0) >= MIN_SIGNAL_SCORE
                    and status["in_killzone"]
                    and not status["news_blocked"]
                    and cur_bar_ts != last_signal_bar_ts
                ):
                    async with _monitor_lock:
                        existing = await db.state_get("current_trade")
                        if not existing:
                            await db.state_set("current_trade", cand)
                            last_signal_bar_ts = cur_bar_ts
                            _metrics["signals"] += 1
                            acc = await get_account()
                            msg = format_signal_message(cand, status, acc)
                            await send_telegram(session, msg, with_keyboard=True)

                            # Grafik yuborish
                            png = await render_chart_async(cl15, status["zones15"], status["struct15"], trade=cand)
                            if png:
                                await send_telegram_photo(
                                    session, png,
                                    caption=f"🎯 <b>{cand['signal']} @ {cand['entry']}</b> | SL: {cand['sl']} | TP1: {cand['tp1']} | TP2: {cand['tp2']}",
                                )

                            # Exness & Binance avto-savdo
                            _spawn(execute_exness_autotrades(session, dict(cand)))
                            _spawn(execute_binance_autotrades(session, dict(cand)))

                            # Gemini AI sharhi
                            if GEMINI_API_KEY:
                                _spawn(_send_ai_followup(session, dict(cand), dict(status), list(status["confirmations"])))

        except asyncio.CancelledError:
            raise
        except Exception as e:
            _metrics["errors"] += 1
            logger.error(f"Scanner loop xato: {_safe_log_error(e)}")

        await asyncio.sleep(CHECK_INTERVAL_SEC)

# ==================== EXNESS HISOBLARNI FORMATLASH ====================
async def format_exness_menu(chat_id):
    accs = await db.get_exness_accounts(chat_id)
    mode_str = "🟢 Native MT5 Terminal" if _HAS_MT5 else "☁️ Cloud Bridge (/api/mt5_bridge/poll)"
    lines = [
        f"🏦 <b>EXNESS MT5 MULTI-ACCOUNT ({len(accs)}/{MAX_EXNESS_ACCOUNTS})</b>",
        f"Rejim: <b>{mode_str}</b>",
        f"━━━━━━━━━━━━━━━━━━━━",
    ]
    if not accs:
        lines.append("📭 Hali Exness hisob ulanmagan.\n")
    else:
        for a in accs:
            st_icon = "🟢 ON" if a["auto_trade"] else "🔴 OFF"
            bal_str = f"${a['last_balance']:.2f}" if a["last_balance"] is not None else "Noma'lum"
            lines.append(
                f"<b>#{a['slot']} — {html.escape(a['label'])}</b> [{st_icon}]\n"
                f" • Login: <code>{html.escape(a['login'])}</code> | Server: <code>{html.escape(a['server'])}</code>\n"
                f" • Simbol: <code>{html.escape(a['symbol'])}</code> | Lot: <code>{a['lot']}</code> | Balans: <b>{bal_str}</b>\n"
                f" • Holat: <i>{html.escape(str(a['last_status']))}</i>\n"
            )

    lines.extend([
        "📌 <b>Buyruqlar (xavfsiz — parol xabari darhol o'chiriladi):</b>",
        "• Ulash: <code>/exness_add LOGIN PAROL [SERVER|AUTO] [LOT]</code>",
        "  Misol: <code>/exness_add 12345678 MyPass123 AUTO 0.01</code>",
        "• Lot o'zgartirish: <code>/exness_lot 1 0.02</code>",
        "• Avto-savdo Yoqish/O'chirish: <code>/exness_toggle 1</code>",
        "• Ulanishni tekshirish: <code>/exness_test 1</code>",
        "• O'chirish: <code>/exness_del 1</code>",
    ])
    return "\n".join(lines)

# ==================== TELEGRAM BUYRUQLAR HANDLERI ====================
async def handle_command(session, chat_id, text, message_id=None):
    global _CURRENT_GEMINI_MODEL
    cid = str(chat_id)
    raw_text = (text or "").strip()
    if not raw_text:
        return

    # Parol yuborilgan xabarni darhol o'chirish himoyasi
    if raw_text.lower().startswith(("/exness_add", "/binance_set")) and message_id:
        await delete_telegram_message(session, cid, message_id)

    if not await is_allowed(cid):
        if cid not in _denied_notified:
            _bounded_add(_denied_notified, cid)
            await send_telegram(
                session,
                f"⛔ Sizga ruxsat berilmagan. Chat ID: <code>{cid}</code>",
                chat_id=cid,
            )
        return

    await add_sub(cid)
    parts = raw_text.split()
    cmd = parts[0].lower().split("@")[0]

    # Tugmalar mapping
    if raw_text == "📊 Signal":
        cmd = "/signal"
    elif raw_text == "🖼 Grafik":
        cmd = "/chart"
    elif raw_text == "🧠 AI Tahlil":
        cmd = "/ai"
    elif raw_text == "🏦 Exness Hisoblar":
        cmd = "/exness"
    elif raw_text == "🎯 Zonalar":
        cmd = "/zones"
    elif raw_text == "📈 Statistika":
        cmd = "/stats"

    if cmd in ("/start", "/help"):
        msg = (
            "👑 <b>GOLD (EXNESS / XM) SMC + ICT BOT — v19 ULTIMATE</b>\n"
            "━━━━━━━━━━━━━━━━━━━━\n"
            "• <b>15m + 5m LTF Sniper</b> kirishlar (FVG, OB, Breaker, OTE, Sweep)\n"
            "• <b>Macro SMT:</b> DXY, US10Y, BTC va EUR divergensiya filtri\n"
            "• <b>Exness MT5:</b> 3 tagacha hisobda avto-order va avto-SL (Trailing)\n\n"
            "🔹 <b>Asosiy buyruqlar:</b>\n"
            "• /signal — Bozor tahlili va joriy holat\n"
            "• /chart — 15m SMC + Zona + Darajalar grafigi (PNG)\n"
            "• /zones — Faol FVG, Order Block va Breaker zonalar\n"
            "• /exness — Exness MT5 hisoblarni ulash va boshqarish (1-3 slot)\n"
            "• /sync <code>2915.40</code> — Broker narxi bilan 1-bosishda sinxronlash\n"
            "• /offset <code>-8.20</code> — Bybit va Broker farqini sozlash\n"
            "• /balance <code>20</code> | /risk <code>10</code> — Balans va risk sozlash\n"
            "• /close — Ochiq bitimni qo'lda yopish\n"
            "• /ai <code>[savol]</code> — Gemini AI bozor tahlili | /model — AI model tanlash\n"
            "• /stats — Win Rate va oxirgi bitimlar tarixi"
        )
        await send_telegram(session, msg, with_keyboard=True, chat_id=cid)

    elif cmd in ("/signal", "/status"):
        status = await analyze_gold_market(session, run_deep_scoring=False)
        msg = await format_status_message(status)
        await send_telegram(session, msg, with_keyboard=True, chat_id=cid)

    elif cmd == "/chart":
        now_m = time.monotonic()
        if now_m - _chart_last.get(cid, 0) < CHART_COOLDOWN_SEC:
            await send_telegram(session, "⏳ Grafik uchun biroz kuting (10s cooldown).", chat_id=cid)
            return
        _bounded_set(_chart_last, cid, now_m)
        status = await analyze_gold_market(session, run_deep_scoring=False)
        trade = await db.state_get("current_trade")
        png = await render_chart_async(status["cl15"], status["zones15"], status["struct15"], trade=trade)
        if png:
            cap = (
                f"🖼 <b>XAUUSD 15m SMC Grafik</b> | Narx: <code>{status['price']}</code>\n"
                f"15m Bias: <b>{status['b15']}</b> | EQ(50%): <code>{status['eq15']}</code> | "
                f"PDH/PDL: <code>{status['pdh']}</code> / <code>{status['pdl']}</code>"
            )
            await send_telegram_photo(session, png, caption=cap, chat_id=cid)
        else:
            await send_telegram(session, "⚠️ Grafik chizish uchun yetarli shamlar yo'q.", chat_id=cid)

    elif cmd == "/zones":
        status = await analyze_gold_market(session, run_deep_scoring=False)
        zones = status["zones15"]
        p = status["price"]
        bull_z = [z for z in zones if z["type"] == "bullish"]
        bear_z = [z for z in zones if z["type"] == "bearish"]
        bull_z.sort(key=lambda z: abs(p - z["ce"]))
        bear_z.sort(key=lambda z: abs(p - z["ce"]))

        lines = [
            f"🎯 <b>FAOL 15M SMC ZONALAR (Joriy: {p})</b>",
            f"━━━━━━━━━━━━━━━━━━━━",
            f"🟢 <b>Bullish (Support / Demand):</b>",
        ]
        for z in bull_z[:5]:
            dist = p - z["ce"]
            lines.append(f" • <b>{z['kind']}</b>: <code>{z['bottom']:.2f} — {z['top']:.2f}</code> (CE: {z['ce']:.2f} | {dist:+.1f}$)")
        if not bull_z:
            lines.append(" • Faol bullish zona yo'q")

        lines.append("\n🔴 <b>Bearish (Resistance / Supply):</b>")
        for z in bear_z[:5]:
            dist = z["ce"] - p
            lines.append(f" • <b>{z['kind']}</b>: <code>{z['bottom']:.2f} — {z['top']:.2f}</code> (CE: {z['ce']:.2f} | +{dist:.1f}$)")
        if not bear_z:
            lines.append(" • Faol bearish zona yo'q")

        await send_telegram(session, "\n".join(lines), chat_id=cid)

    elif cmd == "/exness":
        msg = await format_exness_menu(cid)
        await send_telegram(session, msg, chat_id=cid)

    elif cmd == "/exness_add":
        if len(parts) < 3:
            await send_telegram(
                session,
                "⚠️ Format: <code>/exness_add LOGIN PAROL [SERVER|AUTO] [LOT] [SYMBOL]</code>\n"
                "Misol: <code>/exness_add 12345678 Parol123 AUTO 0.01 XAUUSDm</code>",
                chat_id=cid,
            )
            return
        login_id = parts[1].strip()
        pw_plain = parts[2].strip()
        srv = parts[3].strip() if len(parts) >= 4 else "AUTO"
        lot_val = float(parts[4]) if len(parts) >= 5 else 0.01
        sym_val = parts[5].strip() if len(parts) >= 6 else "XAUUSDm"

        await send_telegram(session, f"🔄 Exness (<code>{html.escape(login_id)}</code>) tekshirilmoqda...", chat_id=cid)
        async with _mt5_lock:
            loop = asyncio.get_running_loop()
            test_res = await loop.run_in_executor(None, _mt5_test_login_sync, login_id, pw_plain, srv)

        if not test_res.get("ok"):
            await send_telegram(
                session,
                f"❌ <b>Exness ulanish xatosi:</b> {html.escape(str(test_res.get('msg')))}",
                chat_id=cid,
            )
            return

        real_srv = test_res.get("server") or (EXNESS_DEFAULT_SERVER if srv.upper() == "AUTO" else srv)
        real_sym = test_res.get("symbol") or sym_val
        pw_enc = encrypt_secret(pw_plain)
        try:
            slot = await db.add_or_update_exness_account(
                cid, login_id, pw_enc, real_srv, lot=max(0.01, round(lot_val, 2)), symbol=real_sym
            )
            await db.update_exness_status(
                cid, slot, balance=test_res.get("balance"), status_text=test_res.get("msg", "Ulangan")
            )
            await send_telegram(
                session,
                f"✅ <b>Exness #{slot}-slot muvaffaqiyatli ulandi!</b>\n"
                f"Login: <code>{html.escape(login_id)}</code> | Server: <code>{html.escape(real_srv)}</code>\n"
                f"Simbol: <code>{html.escape(real_sym)}</code> | Lot: <code>{lot_val:.2f}</code>\n"
                f"Holat: <i>{html.escape(str(test_res.get('msg')))}</i>\n"
                f"🔒 Xavfsizlik uchun parol xabaringiz chatdan o'chirildi.",
                chat_id=cid,
            )
        except Exception as e:
            await send_telegram(session, f"❌ Xato: {html.escape(str(e))}", chat_id=cid)

    elif cmd == "/exness_lot" and len(parts) >= 3:
        try:
            slot, lot_v = int(parts[1]), float(parts[2])
            await db.toggle_exness_autotrade(cid, slot, lot=lot_v)
            await send_telegram(session, f"✅ Exness #{slot}-slot lot hajmi <code>{max(0.01, round(lot_v, 2))}</code> ga o'zgartirildi.", chat_id=cid)
        except Exception as e:
            await send_telegram(session, f"⚠️ Xato: {html.escape(str(e))}", chat_id=cid)

    elif cmd == "/exness_toggle" and len(parts) >= 2:
        try:
            slot = int(parts[1])
            accs = await db.get_exness_accounts(cid)
            target = next((a for a in accs if a["slot"] == slot), None)
            if not target:
                await send_telegram(session, f"⚠️ #{slot}-slot topilmadi.", chat_id=cid)
                return
            new_state = not target["auto_trade"]
            await db.toggle_exness_autotrade(cid, slot, enabled=new_state)
            st_txt = "🟢 YOQILDI" if new_state else "🔴 O'CHIRILDI"
            await send_telegram(session, f"🏦 Exness #{slot}-slot avto-savdo: <b>{st_txt}</b>", chat_id=cid)
        except Exception as e:
            await send_telegram(session, f"⚠️ Xato: {html.escape(str(e))}", chat_id=cid)

    elif cmd == "/exness_test" and len(parts) >= 2:
        try:
            slot = int(parts[1])
            accs = await db.get_exness_accounts(cid)
            target = next((a for a in accs if a["slot"] == slot), None)
            if not target:
                await send_telegram(session, f"⚠️ #{slot}-slot topilmadi.", chat_id=cid)
                return
            res = await test_exness_account(target)
            await send_telegram(session, f"🔍 <b>Exness #{slot} natija:</b> {html.escape(str(res.get('msg')))}", chat_id=cid)
        except Exception as e:
            await send_telegram(session, f"⚠️ Xato: {html.escape(str(e))}", chat_id=cid)

    elif cmd == "/exness_del" and len(parts) >= 2:
        try:
            slot = int(parts[1])
            await db.delete_exness_account(cid, slot)
            await send_telegram(session, f"🗑 Exness #{slot}-slot o'chirildi.", chat_id=cid)
        except Exception as e:
            await send_telegram(session, f"⚠️ Xato: {html.escape(str(e))}", chat_id=cid)

    elif cmd == "/sync" and len(parts) >= 2:
        try:
            broker_p = float(parts[1].replace(",", "."))
            raw_bybit = _LIVE_BYBIT_RAW_PRICE or await fetch_raw_price(session)
            new_off = round(broker_p - raw_bybit, 2)
            delta = await apply_offset_change(new_off)
            await send_telegram(
                session,
                f"🔄 <b>Narx sinxronlandi!</b>\n"
                f"Broker narxi: <code>{broker_p:.2f}</code> | Bybit: <code>{raw_bybit:.2f}</code>\n"
                f"Yangi Offset: <code>{_CURRENT_OFFSET:+.2f}</code> (Siljish: <code>{delta:+.2f}$</code>)",
                chat_id=cid,
            )
        except Exception as e:
            await send_telegram(session, f"⚠️ Format: <code>/sync 2915.50</code> ({html.escape(str(e))})", chat_id=cid)

    elif cmd == "/offset" and len(parts) >= 2:
        try:
            new_off = round(float(parts[1].replace(",", ".")), 2)
            delta = await apply_offset_change(new_off)
            await send_telegram(
                session,
                f"⚙️ <b>Offset yangilandi:</b> <code>{_CURRENT_OFFSET:+.2f}</code> (Siljish: <code>{delta:+.2f}$</code>)",
                chat_id=cid,
            )
        except Exception:
            await send_telegram(session, "⚠️ Format: <code>/offset -8.21</code>", chat_id=cid)

    elif cmd == "/balance" and len(parts) >= 2:
        try:
            val = max(5.0, float(parts[1].replace(",", ".")))
            await update_account(balance=val)
            await send_telegram(session, f"💰 Balans <code>${val:.2f}</code> qilib saqlandi.", chat_id=cid)
        except Exception:
            await send_telegram(session, "⚠️ Format: <code>/balance 20</code>", chat_id=cid)

    elif cmd == "/risk" and len(parts) >= 2:
        try:
            val = max(0.5, min(MAX_RISK_PER_TRADE_PCT, float(parts[1].replace(",", "."))))
            await update_account(risk=val)
            await send_telegram(session, f"⚖️ Risk <code>{val:.1f}%</code> qilib saqlandi.", chat_id=cid)
        except Exception:
            await send_telegram(session, f"⚠️ Format: <code>/risk 10</code> (Max: {MAX_RISK_PER_TRADE_PCT}%)", chat_id=cid)

    elif cmd == "/close":
        async with _monitor_lock:
            trade = await db.state_get("current_trade")
            if not trade:
                await send_telegram(session, "📭 Hozirda ochiq bitim yo'q.", chat_id=cid)
                return
            cur_p = live_price() or (_LAST_CANDLES_15M[-1][C] if _LAST_CANDLES_15M else trade["entry"])
            await close_trade(session, trade, "MANUAL", round(cur_p, 2))

    elif cmd == "/stats":
        wr_txt = await win_rate_text()
        recent = await db.get_recent_trades(limit=10)
        uptime_h = (time.time() - _metrics["start"]) / 3600.0
        lines = [
            f"📈 <b>SAVDO STATISTIKASI VA METRIKALAR</b>",
            f"━━━━━━━━━━━━━━━━━━━━",
            f"🏆 <b>{html.escape(wr_txt)}</b>",
            f"⏱ Uptime: <code>{uptime_h:.1f} soat</code> | WS Ticks: <code>{_metrics['ws_ticks']}</code>",
            f"",
            f"📜 <b>Oxirgi 10 ta bitim:</b>",
        ]
        if not recent:
            lines.append(" • Hali yopilgan bitimlar yo'q")
        else:
            for t in recent:
                lines.append(
                    f" • <b>{t['signal']}</b> | {html.escape(str(t['result']))} | "
                    f"<code>{t['entry']} ➡️ {t['close_price']}</code>"
                )
        await send_telegram(session, "\n".join(lines), chat_id=cid)

    elif cmd == "/model":
        if len(parts) >= 2:
            m_Input = parts[1].strip()
            if _GEMINI_NAME_RE.fullmatch(m_Input):
                _CURRENT_GEMINI_MODEL = m_Input
                await db.state_set("gemini_model", _CURRENT_GEMINI_MODEL)
                await send_telegram(session, f"🧠 Gemini model o'zgardi: <code>{html.escape(_CURRENT_GEMINI_MODEL)}</code>", chat_id=cid)
                return
        presets_txt = "\n".join([f"• <code>/model {m}</code> — {desc}" for m, desc in GEMINI_PRESETS])
        await send_telegram(
            session,
            f"🧠 <b>Joriy Gemini Model:</b> <code>{html.escape(_CURRENT_GEMINI_MODEL)}</code>\n\n{presets_txt}",
            chat_id=cid,
        )

    elif cmd == "/ai":
        if not GEMINI_API_KEY:
            await send_telegram(session, "⚠️ GEMINI_API_KEY o'rnatilmagan.", chat_id=cid)
            return
        now_m = time.monotonic()
        if now_m - _ai_last.get(cid, 0) < AI_COOLDOWN_SEC:
            await send_telegram(session, "⏳ AI tahlil uchun 20 soniya kuting.", chat_id=cid)
            return
        _bounded_set(_ai_last, cid, now_m)

        user_q = raw_text[len(parts[0]):].strip()
        status = await analyze_gold_market(session, run_deep_scoring=False)
        sys_p = (
            "Sen institutional SMC va ICT XAUUSD (Oltin) tahlilchisisisan. "
            "O'zbek tilida aniq, qisqa (maksimum 6-8 qator), suvsiz va raqamlarga tayanib javob ber."
        )
        ctx = (
            f"Joriy XAUUSD narxi: {status['price']} | Sessiya: {status['session']}\n"
            f"MTF: 5m={status['b5']}, 15m={status['b15']} ({status['event15']}), 1H={status['b1h']}, 4H={status['b4h']}\n"
            f"EQ(50%): {status['eq15']} | OTE: {status['ote_bottom']}-{status['ote_top']} | "
            f"PDH/PDL: {status['pdh']}/{status['pdl']} | EQH/EQL: {status['eqh']}/{status['eql']}\n"
            f"Savol: {user_q or 'Hozirgi bozor holati, likvidlik qayerda va qaysi zonadan kirish xavfsiz?'}"
        )
        await send_telegram(session, "🧠 <i>Gemini AI tahlil qilmoqda...</i>", chat_id=cid)
        ans = await ask_gemini_ai(session, ctx, system_instruction=sys_p)
        await send_telegram(
            session,
            f"🧠 <b>Gemini AI ({html.escape(_CURRENT_GEMINI_MODEL)}):</b>\n\n{html.escape(ans or 'Javob olinmadi.')}",
            chat_id=cid,
        )

    elif cmd == "/allow" and cid == ADMIN_CHAT_ID and len(parts) >= 2:
        allowed = set(await db.state_get("allowed", []))
        allowed.add(parts[1].strip())
        await db.state_set("allowed", list(allowed))
        await send_telegram(session, f"✅ <code>{html.escape(parts[1])}</code> ruxsat ro'yxatiga qo'shildi.", chat_id=cid)

    elif cmd == "/disallow" and cid == ADMIN_CHAT_ID and len(parts) >= 2:
        allowed = set(await db.state_get("allowed", []))
        allowed.discard(parts[1].strip())
        await db.state_set("allowed", list(allowed))
        await send_telegram(session, f"🗑 <code>{html.escape(parts[1])}</code> ruxsat ro'yxatidan chiqarildi.", chat_id=cid)

# ==================== TELEGRAM MINI APP (WEBAPP) HMAC-SHA256 AUTH ====================
def verify_webapp_init_data(init_data_raw):
    if not init_data_raw:
        return None
    try:
        parsed = dict(urllib.parse.parse_qsl(init_data_raw, keep_blank_values=True))
        recv_hash = parsed.pop("hash", None)
        if not recv_hash:
            return None
        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(parsed.items()))
        secret_key = hmac.new(b"WebAppData", BOT_TOKEN.encode("utf-8"), hashlib.sha256).digest()
        calc_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(calc_hash, recv_hash):
            return None
        user_json = parsed.get("user")
        return json.loads(user_json) if user_json else {"id": ADMIN_CHAT_ID}
    except Exception:
        return None

def _check_api_auth(request):
    init_data = request.headers.get("X-Telegram-Init-Data") or request.query.get("initData", "")
    user = verify_webapp_init_data(init_data)
    if user:
        return str(user.get("id", ADMIN_CHAT_ID))
    token = request.headers.get("X-Dashboard-Token") or request.query.get("token", "")
    if DASHBOARD_TOKEN and token and hmac.compare_digest(token, DASHBOARD_TOKEN):
        return ADMIN_CHAT_ID
    if ALLOW_PUBLIC:
        return ADMIN_CHAT_ID
    return None

# ==================== MINI APP HTML UI ====================
MINI_APP_HTML = """<!DOCTYPE html>
<html lang="uz">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<title>GOLD SMC v19 ULTIMATE</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
  :root { --bg:#0d1117; --card:#161b22; --gold:#f0b90b; --green:#2ebd85; --red:#f6465d; --txt:#e6edf3; --muted:#8b949e; }
  * { box-sizing:border-box; margin:0; padding:0; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif; }
  body { background:var(--bg); color:var(--txt); padding:12px; font-size:14px; }
  .hdr { display:flex; justify-content:space-between; align-items:center; padding:12px; background:var(--card); border-radius:12px; border:1px solid #30363d; margin-bottom:12px; }
  .price { font-size:24px; font-weight:800; color:var(--gold); }
  .badge { padding:4px 8px; border-radius:6px; font-size:12px; font-weight:600; background:#21262d; }
  .grid { display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-bottom:12px; }
  .card { background:var(--card); padding:12px; border-radius:12px; border:1px solid #30363d; margin-bottom:12px; }
  .card h3 { font-size:13px; color:var(--muted); margin-bottom:8px; text-transform:uppercase; }
  .btn { width:100%; padding:10px; border:none; border-radius:8px; font-weight:700; cursor:pointer; color:#fff; background:#238636; transition:0.15s; }
  .btn.gold { background:var(--gold); color:#000; }
  .btn.red { background:var(--red); }
  .btn.sec { background:#21262d; color:var(--txt); border:1px solid #30363d; }
  input { width:100%; padding:9px; border-radius:8px; border:1px solid #30363d; background:#0d1117; color:var(--txt); margin-bottom:8px; }
  .row { display:flex; justify-content:space-between; padding:4px 0; border-bottom:1px solid #21262d; }
</style>
</head>
<body>
  <div class="hdr">
    <div>
      <div style="font-size:12px;color:var(--muted)">XAUUSD (EXNESS/XM)</div>
      <div class="price" id="price">...</div>
    </div>
    <div style="text-align:right">
      <div class="badge" id="bias">15m: ...</div>
      <div style="font-size:11px;color:var(--muted);margin-top:4px" id="offset">Offset: ...</div>
    </div>
  </div>

  <div class="grid">
    <button class="btn gold" onclick="act('signal')">📊 Telegram Signal</button>
    <button class="btn sec" onclick="act('chart')">🖼 15m Grafik</button>
    <button class="btn sec" onclick="act('ai')">🧠 Gemini AI</button>
    <button class="btn red" onclick="act('close')">🛑 Bitimni Yopish</button>
  </div>

  <div class="card">
    <h3>⚡ Ochiq Bitim & ICT Darajalar</h3>
    <div id="trade_box">Yuklanmoqda...</div>
  </div>

  <div class="card">
    <h3>🔄 Broker Narxini Sinxronlash (/sync)</h3>
    <div style="display:flex;gap:8px">
      <input type="number" step="0.01" id="sync_p" placeholder="Exness/XM joriy narxi (masalan 2915.40)" style="margin:0">
      <button class="btn gold" style="width:110px" onclick="syncPrice()">Sinxron</button>
    </div>
  </div>

  <div class="card">
    <h3>🏦 Exness MT5 Hisoblar (Max 3)</h3>
    <div id="exness_list" style="margin-bottom:10px">...</div>
    <input id="ex_login" placeholder="MT5 Login ID">
    <input id="ex_pass" type="password" placeholder="MT5 Parol">
    <div style="display:flex;gap:8px">
      <input id="ex_srv" placeholder="Server (AUTO)" value="AUTO">
      <input id="ex_lot" type="number" step="0.01" placeholder="Lot" value="0.01">
    </div>
    <button class="btn" onclick="addExness()">➕ Exness Hisob Ulash</button>
  </div>

<script>
const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
if (tg) { tg.expand(); }
const initData = tg ? tg.initData : "";
const urlToken = new URLSearchParams(location.search).get("token") || "";

async function api(path, body=null) {
  const opts = { headers: { "Content-Type":"application/json", "X-Telegram-Init-Data": initData, "X-Dashboard-Token": urlToken } };
  if (body) { opts.method = "POST"; opts.body = JSON.stringify(body); }
  const r = await fetch(path, opts);
  return await r.json();
}

async function loadState() {
  try {
    const d = await api("/api/state");
    if (!d.ok) return;
    document.getElementById("price").innerText = d.price ? d.price.toFixed(2) : "---";
    document.getElementById("bias").innerText = "15m: " + (d.b15 || "NEUTRAL").toUpperCase();
    document.getElementById("offset").innerText = "Offset: " + (d.offset >= 0 ? "+" : "") + d.offset;
    const tb = document.getElementById("trade_box");
    if (d.trade) {
      tb.innerHTML = `<div class="row"><span>Yo'nalish:</span><b>${d.trade.signal} @ ${d.trade.entry}</b></div>
                      <div class="row"><span>SL / TP1 / TP2:</span><b>${d.trade.sl} / ${d.trade.tp1} / ${d.trade.tp2}</b></div>`;
    } else {
      tb.innerHTML = `<div class="row"><span>Holat:</span><span>Sniper zona kutilmoqda</span></div>
                      <div class="row"><span>EQ (50%) /WR:</span><b>${d.eq15 || '-'} | ${d.wr}</b></div>`;
    }
    const el = document.getElementById("exness_list");
    el.innerHTML = (d.exness || []).map(a =>
      `<div class="row"><span>#${a.slot} <b>${a.login}</b> (${a.server})</span>
       <span>${a.lot} lot | <a href="#" style="color:var(--gold)" onclick="delEx(${a.slot})">O'chirish</a></span></div>`
    ).join("") || "Hali hisob ulanmagan";
  } catch (e) {}
}

async function act(cmd) {
  await api("/api/action", { action: cmd });
  if (tg) tg.showAlert("Buyruq bajarildi: " + cmd);
  loadState();
}

async function syncPrice() {
  const v = parseFloat(document.getElementById("sync_p").value);
  if (!v) return;
  await api("/api/action", { action: "sync", price: v });
  loadState();
}

async function addExness() {
  const login = document.getElementById("ex_login").value.trim();
  const password = document.getElementById("ex_pass").value.trim();
  const server = document.getElementById("ex_srv").value.trim() || "AUTO";
  const lot = parseFloat(document.getElementById("ex_lot").value) || 0.01;
  if (!login || !password) return;
  const r = await api("/api/action", { action: "exness_add", login, password, server, lot });
  alert(r.msg || "Bajarildi");
  document.getElementById("ex_pass").value = "";
  loadState();
}

async function delEx(slot) {
  await api("/api/action", { action: "exness_del", slot });
  loadState();
}

loadState();
setInterval(loadState, 3000);
</script>
</body>
</html>
"""

# ==================== WEB SERVER & CLOUD BRIDGE ENDPOINTS ====================
async def handle_index(request):
    return web.Response(text=MINI_APP_HTML, content_type="text/html")

async def handle_health(request):
    return web.json_response({
        "ok": True,
        "version": "v19_ULTIMATE_10_10",
        "ws_connected": _WS_CONNECTED,
        "live_price": live_price(),
        "offset": _CURRENT_OFFSET,
        "metrics": _metrics,
    })

async def handle_api_state(request):
    cid = _check_api_auth(request)
    if not cid:
        return web.json_response({"ok": False, "error": "Unauthorized"}, status=401)
    trade = await db.state_get("current_trade")
    ex_accs = await db.get_exness_accounts(cid)
    wr = await win_rate_text()
    p = live_price() or (_LAST_CANDLES_15M[-1][C] if _LAST_CANDLES_15M else None)
    return web.json_response({
        "ok": True,
        "price": p,
        "offset": _CURRENT_OFFSET,
        "b15": _LAST_STRUCT_15M.get("bias"),
        "eq15": _LAST_STRUCT_15M.get("eq"),
        "trade": trade,
        "wr": wr,
        "exness": [
            {
                "slot": a["slot"], "login": a["login"], "server": a["server"],
                "lot": a["lot"], "auto_trade": a["auto_trade"],
                "balance": a["last_balance"], "status": a["last_status"],
            }
            for a in ex_accs
        ],
    })

async def handle_api_action(request):
    cid = _check_api_auth(request)
    if not cid:
        return web.json_response({"ok": False, "error": "Unauthorized"}, status=401)
    session = request.app["session"]
    data = await request.json()
    action = data.get("action")

    if action in ("signal", "chart", "ai", "close"):
        await handle_command(session, cid, f"/{action}")
        return web.json_response({"ok": True, "msg": f"/{action} yuborildi"})

    if action == "sync":
        broker_p = float(data["price"])
        raw_bybit = _LIVE_BYBIT_RAW_PRICE or await fetch_raw_price(session)
        new_off = round(broker_p - raw_bybit, 2)
        await apply_offset_change(new_off)
        return web.json_response({"ok": True, "offset": _CURRENT_OFFSET})

    if action == "exness_add":
        login_id = str(data.get("login", "")).strip()
        pw = str(data.get("password", "")).strip()
        srv = str(data.get("server", "AUTO")).strip()
        lot = float(data.get("lot", 0.01))
        await handle_command(session, cid, f"/exness_add {login_id} {pw} {srv} {lot}")
        return web.json_response({"ok": True, "msg": "Exness hisob tekshirildi va saqlandi!"})

    if action == "exness_del":
        slot = int(data.get("slot", 1))
        await db.delete_exness_account(cid, slot)
        return web.json_response({"ok": True})

    return web.json_response({"ok": False, "msg": "Noma'lum amal"}, status=400)

async def handle_mt5_bridge_poll(request):
    """Bepul Cloud Bridge: Tashqi MT5 terminal yoki EA ushbu endpoint orqali signallarni oladi."""
    token = request.headers.get("X-Bridge-Token") or request.query.get("token", "")
    if DASHBOARD_TOKEN and token != DASHBOARD_TOKEN:
        return web.json_response({"ok": False, "error": "Invalid token"}, status=403)
    trade = await db.state_get("current_trade")
    return web.json_response({
        "ok": True,
        "active_trade": trade,
        "recent_signals": _BRIDGE_SIGNALS_QUEUE[-10:],
        "server_time": int(time.time()),
    })

async def handle_webhook(request):
    if WEBHOOK_SECRET:
        hdr = request.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if not hmac.compare_digest(hdr, WEBHOOK_SECRET):
            return web.Response(status=403)
    try:
        upd = await request.json()
        msg = upd.get("message") or upd.get("edited_message")
        if msg and "text" in msg:
            chat_id = msg["chat"]["id"]
            msg_id = msg.get("message_id")
            _spawn(handle_command(request.app["session"], chat_id, msg["text"], message_id=msg_id))
    except Exception as e:
        logger.error(f"Webhook xato: {_safe_log_error(e)}")
    return web.json_response({"ok": True})

# ==================== TELEGRAM POLLING LOOP ====================
async def telegram_polling_loop(app):
    session = app["session"]
    offset = 0
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"

    # Agar avval webhook qolib ketgan bo'lsa tozalash
    try:
        async with session.post(f"https://api.telegram.org/bot{BOT_TOKEN}/deleteWebhook", timeout=T(10)):
            pass
    except Exception:
        pass

    logger.info("🤖 Telegram Polling ishga tushdi!")
    while True:
        try:
            params = {"offset": offset, "timeout": 25, "allowed_updates": ["message"]}
            async with session.get(url, params=params, timeout=T(35)) as r:
                if r.status != 200:
                    await asyncio.sleep(3)
                    continue
                data = await r.json()
                for upd in data.get("result", []):
                    offset = upd["update_id"] + 1
                    msg = upd.get("message")
                    if msg and "text" in msg:
                        cid = msg["chat"]["id"]
                        mid = msg.get("message_id")
                        _spawn(handle_command(session, cid, msg["text"], message_id=mid))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"Polling xato: {_safe_log_error(e)}")
            await asyncio.sleep(3)

# ==================== APP LIFECYCLE & MAIN ====================
async def on_startup(app):
    global _CURRENT_OFFSET, _CURRENT_GEMINI_MODEL
    await db.init()
    saved_off = await db.state_get("price_offset")
    if saved_off is not None:
        _CURRENT_OFFSET = float(saved_off)
    saved_mod = await db.state_get("gemini_model")
    if saved_mod and _GEMINI_NAME_RE.fullmatch(str(saved_mod)):
        _CURRENT_GEMINI_MODEL = str(saved_mod)

    app["session"] = aiohttp.ClientSession()
    app["ws_task"] = asyncio.create_task(bybit_ws_loop(app))
    app["scanner_task"] = asyncio.create_task(scanner_loop(app))
    if not WEBHOOK_ENABLED:
        app["poll_task"] = asyncio.create_task(telegram_polling_loop(app))

    logger.info(f"🚀 GOLD SMC v19 ULTIMATE tayyor! Offset: {_CURRENT_OFFSET:+.2f} | Model: {_CURRENT_GEMINI_MODEL}")

async def on_cleanup(app):
    for task_name in ("ws_task", "scanner_task", "poll_task"):
        t = app.get(task_name)
        if t:
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
    for t in list(_bg_tasks):
        t.cancel()
    if "session" in app:
        await app["session"].close()
    await db.close()
    if _HAS_MT5:
        try:
            mt5.shutdown()
        except Exception:
            pass

def create_app():
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/webapp", handle_index)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/api/state", handle_api_state)
    app.router.add_post("/api/action", handle_api_action)
    app.router.add_get("/api/mt5_bridge/poll", handle_mt5_bridge_poll)
    app.router.add_post("/webhook", handle_webhook)
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app

app = create_app()   # <-- QO'SHILDI: modul darajasida app obyekti

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    web.run_app(app, host="0.0.0.0", port=port)
