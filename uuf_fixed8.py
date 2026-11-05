"""
GOLD (EXNESS / XM / XAUUSD) SMC + ICT Bot — v20 DUAL ENGINE (Ake Signals 7 [1m] + Day Trading [15m] ALOHIDA)
========================================================================
v16 TITANIUM FIXED va v18 TITANIUM ning to'liq birlashtirilgan, dublikatlardan
tozalangan yakuniy versiyasi:
  ✅ 1 ta foydalanuvchi uchun 3 tagacha Exness (Demo/Real) MT5 hisob ulash + Avto-server topish
  ✅ Native MetaTrader5 + Bepul Cloud Bridge (/api/mt5_bridge/poll) orqali BUY/SELL avto-savdo
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
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

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
HTF_LIMIT = 720          # 1H da 720 ta sham = to'liq 1 oylik (30 kun) tarix
HTF_4H_LIMIT = 180       # 4H da 180 ta sham = to'liq 1 oylik (30 kun) tarix
HTF_1D_LIMIT = 120       # 1D da 120 ta sham = 4 oylik tarix
HTF_1W_LIMIT = 24        # 1W da 24 ta sham = 6 oylik haftalik tarix
HTF_1M_LIMIT = 12        # 1M da 12 ta sham = 1 yillik oylik tarix

RR1, RR2, RR3 = 3.5, 3.5, 3.5
SWING_LEFT, SWING_RIGHT = 3, 3
CHECK_INTERVAL_SEC = 5
ZONE_MAX_DISTANCE_PCT = 0.05   # Faqat aniq zona ichidan kirish
ZONE_TOL_MAX_POINTS = 0.70     # 0.70$ aniqlikda Sniper kirish (erta kirmaydi)
MIN_SIGNAL_SCORE = 76          # Real hisob uchun faqat A/A+ (Sweep + CHoCH + Zona + SMT)
DAILY_TARGET_SIGNALS_MIN = 999 # Kunlik son cheklovi olib tashlandi (faqat sifat 76+ ball tekshiriladi)
DAILY_TARGET_SIGNALS_MAX = 999
SIGNAL_COOLDOWN_SEC = 900      # 15 daqiqa (1 ta 15m sham) — sifatli signal chiqsa kuniga limitsiz beraveradi
INTRADAY_MIN_HOLD_SEC = 7200
TRADE_STALE_WARNING_HOURS = 8
FLIP_WARN_AFTER_SEC = 180
FLIP_WARN_REPEAT_SEC = 600
LIVE_PRICE_MAX_AGE_SEC = 10
ASIA_END_HOUR_UTC = 6
AI_COOLDOWN_SEC = 20
CHART_COOLDOWN_SEC = 10
XM_SPREAD_BUFFER = 0.25        # Exness/XM spred buferi
SNIPER_SKIP_IF_SL_TOO_WIDE = True
FVG_REQUIRE_UNTESTED = True
NEWS_RETRY_SEC = 300

DEFAULT_BALANCE_USD = 20.0
MIN_AUTO_BALANCE_USD = 1.0
SEND_AI_EXTRA_MESSAGES = False
DEFAULT_RISK_PER_TRADE_PCT = 15.0  # 20$ ning 15% = 3.0$ (0.01 lotga mos)
MAX_RISK_PER_TRADE_PCT = 22.0
SNIPER_MIN_SL_POINTS = 3.00        # Minimal Sniper SL: 3.0$ (spred shovqinidan himoyalangan)
SNIPER_MAX_SL_POINTS_SMALL = 4.20  # Maksimal Sniper SL: 4.2$ (20$ hisobda 5 ta imkoniyat)

# ===== TRUE SNIPER DAY-TRADING SOZLAMALARI =====
RANGE_SNIPER_MODE = False
RANGE_LOOKBACK_15M = 96
RANGE_MIN_WIDTH = 18.0
SNIPER_BUY_MAX_POS = 0.35          # 24 soatlik diapazonning pastki 35% qismidan BUY
SNIPER_SELL_MIN_POS = 0.65         # 24 soatlik diapazonning yuqori 65% qismidan SELL
RANGE_ENTRY_ZONE_PCT = 0.85
INTRADAY_POS_MIN_PCT = 0.10
INTRADAY_SL_ATR_MULT = 1.20
INTRADAY_TP1_ATR_MULT = 3.50
INTRADAY_TP2_ATR_MULT = 3.50
RANGE_SL_BUFFER = 0.25
RANGE_SL_MIN = 3.0
RANGE_SL_MAX = 4.2
RANGE_TP_MIN = 10.0
RANGE_TP_MAX = 30.0
RANGE_MIN_RR = 3.5

# ===== AKE SIGNALS 7 (1M SCALPING 1R-5R) ALOHIDA SOZLAMALARI =====
AKE_ENABLED_DEFAULT = True
DAY_ENABLED_DEFAULT = True
AKE_TF = "1m"
AKE_LIMIT = 240
AKE_MIN_SL_POINTS = 1.60           # 1m scalp minimal SL (1R) masofasi ($)
AKE_MAX_SL_POINTS = 2.85           # 1m scalp optimal maksimal SL (1R) masofasi ($), 3.80$ cho'zilib ketmasligi uchun
AKE_SKIP_IF_SL_OVER = 3.20         # Agar 1m sham juda katta bo'lib SL > 3.20$ bo'lsa, xavfli sham o'tkazib yuboriladi
AKE_ATR_SL_MULT = 1.20             # 1m ATR ko'paytmasi (1R hisoblash uchun)
AKE_TP_R_LEVELS = (1.0, 2.0, 3.0, 4.0, 5.0)  # TP1=1R, TP2=2R, TP3=3R, TP4=4R, TP5=5R
AKE_COOLDOWN_SEC = 600             # 1m Ake signallar orasida minimal kutish (10 daqiqa, shovqinni kesish uchun)
AKE_FLIP_AFTER_TP1_MIN_SEC = 600   # Faqat TP1 olingandan keyin va kamida 10 daq o'tgach teskari impulsga ruxsat
AKE_ASIA_START_HOUR_UTC = 18       # AsiaSessionHigh/Low 18:00 - 02:00
AKE_ASIA_END_HOUR_UTC = 2
EXNESS_AUTOTRADE_DEFAULT_MODE = "day"  # "day" | "ake" | "both"


XM_MIN_LOT = 0.01
XM_OZ_PER_MIN_LOT = 1.0

# Exness sozlamalari (1 ta foydalanuvchiga maksimal 3 ta hisob)
MAX_EXNESS_ACCOUNTS = 3
EXNESS_DEFAULT_SERVER = os.environ.get("EXNESS_DEFAULT_SERVER", "Exness-MT5Real")
EXNESS_GOLD_SYMBOLS = ("XAUUSDm", "XAUUSD", "XAUUSDc", "GOLDm", "GOLD")
EXNESS_FALLBACK_SERVERS = (
    "Exness-MT5Real", "Exness-MT5Real9", "Exness-MT5Real8",
    "Exness-MT5Real7", "Exness-MT5Real6", "Exness-MT5Real10",
    "Exness-MT5Real11", "Exness-MT5Real12", "Exness-MT5Real5",
    "Exness-MT5Real4", "Exness-MT5Real3", "Exness-MT5Real2",
    "Exness-MT5Trial9", "Exness-MT5Trial8", "Exness-MT5Trial10",
    "Exness-MT5Trial7", "Exness-MT5Trial6", "Exness-MT5Trial",
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
    {"name": "Asia Impulse / Pre-London", "start": datetime.time(1, 0), "end": datetime.time(6, 0)},
    {"name": "London Open / Silver Bullet", "start": datetime.time(6, 0), "end": datetime.time(12, 0)},
    {"name": "NY Open / Overlap / PM Session", "start": datetime.time(12, 0), "end": datetime.time(21, 0)},
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
_ACTIVE_BROKER = os.environ.get("ACTIVE_BROKER", "exness").strip().lower()  # "exness" | "xm"
_AUTO_SYNC_ENABLED = os.environ.get("AUTO_SYNC_ENABLED", "1").strip() != "0"
# Exness (XAUUSDm) va XM (GOLD) ning Spot XAU/USD ga nisbatan spred/kotirovka korreksiyasi ($)
_BROKER_CALIBRATION = {"exness": 0.00, "xm": 0.25}
_LAST_SPOT_SYNC_INFO = {
    "spot_raw": None,
    "exness_price": None,
    "xm_price": None,
    "bybit_raw": None,
    "source": "init",
    "ts": 0.0,
}
METAAPI_TOKEN_ENV = os.environ.get("METAAPI_TOKEN", "").strip()
METAAPI_ACCOUNT_ID_ENV = os.environ.get("METAAPI_ACCOUNT_ID", "").strip()
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
_ake_monitor_lock = asyncio.Lock()
_BRIDGE_SIGNALS_QUEUE = []
_AKE_BRIDGE_SIGNALS_QUEUE = []
_TERMUX_BRIDGE_STATE = {
    "online": False,
    "last_seen": 0.0,
    "login": None,
    "server": None,
    "symbol": None,
    "bid": None,
    "ask": None,
    "balance": None,
    "driver": None,
}

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
    if _LIVE_PRICE is not None and (time.monotonic() - _LIVE_PRICE_TS) <= max(LIVE_PRICE_MAX_AGE_SEC, 30):
        return _LIVE_PRICE
    return None

def get_unified_price(fallback_candles=None):
    """Ake Signals 7 (1m) va Day Trading (15m) hamda Grafiklar 100% bir xil narxda yurishi uchun yagona narx funksiyasi."""
    global _LIVE_PRICE, _LIVE_PRICE_TS
    lp = live_price()
    if lp is not None:
        return round(float(lp), 2)
    if _LIVE_BYBIT_RAW_PRICE is not None:
        val = round(float(_LIVE_BYBIT_RAW_PRICE) + _CURRENT_OFFSET, 2)
        _LIVE_PRICE = val
        _LIVE_PRICE_TS = time.monotonic()
        return val
    if fallback_candles and len(fallback_candles) > 0:
        val = round(float(fallback_candles[-1][C]), 2)
        _LIVE_PRICE = val
        _LIVE_PRICE_TS = time.monotonic()
        return val
    return _LIVE_PRICE

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
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS ake_trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                signal TEXT,
                entry REAL,
                sl REAL,
                initial_sl REAL,
                risk_pts REAL,
                tp1 REAL,
                tp2 REAL,
                tp3 REAL,
                tp4 REAL,
                tp5 REAL,
                max_r_hit INTEGER DEFAULT 0,
                peak_r REAL DEFAULT -1.0,
                rule_r REAL DEFAULT -1.0,
                result TEXT,
                close_price REAL,
                opened_at TEXT,
                closed_at TEXT,
                lot REAL,
                risk_usd REAL,
                reason TEXT
            )
        """)
        await self._conn.commit()
        logger.info(f"DB tayyor (Day Trading + Ake Signals 7 alohida jadvallar): {self.db_path}")

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

    # ---------- Ake Signals 7 (1m Scalping) Alohida DB Metodlari ----------
    async def insert_ake_trade(
        self, trade, result, close_price, max_r_hit, peak_r, rule_r, lot, risk_usd, reason=""
    ):
        async with self._lock:
            await self._conn.execute("""
                INSERT INTO ake_trades (
                    symbol, signal, entry, sl, initial_sl, risk_pts,
                    tp1, tp2, tp3, tp4, tp5,
                    max_r_hit, peak_r, rule_r, result, close_price,
                    opened_at, closed_at, lot, risk_usd, reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                SYMBOL,
                trade["signal"],
                trade["entry"],
                trade["sl"],
                trade.get("initial_sl", trade["sl"]),
                trade.get("risk_pts", abs(trade["entry"] - trade.get("initial_sl", trade["sl"]))),
                trade["tp1"],
                trade["tp2"],
                trade["tp3"],
                trade["tp4"],
                trade["tp5"],
                int(max_r_hit),
                float(peak_r),
                float(rule_r),
                str(result),
                float(close_price),
                trade.get("opened_at"),
                datetime.datetime.now(datetime.timezone.utc).isoformat(),
                float(lot),
                float(risk_usd),
                str(reason),
            ))
            await self._conn.commit()

    async def get_ake_trades_since(self, since_iso):
        async with self._lock:
            async with self._conn.execute(
                "SELECT signal, entry, initial_sl, max_r_hit, peak_r, rule_r, result, close_price, closed_at "
                "FROM ake_trades WHERE closed_at >= ? ORDER BY id DESC",
                (since_iso,),
            ) as cur:
                rows = await cur.fetchall()
                return [
                    {
                        "signal": r[0], "entry": r[1], "initial_sl": r[2],
                        "max_r_hit": int(r[3] or 0), "peak_r": float(r[4] or 0.0),
                        "rule_r": float(r[5] or 0.0), "result": r[6],
                        "close_price": r[7], "closed_at": r[8],
                    }
                    for r in rows
                ]

    async def clear_ake_trades(self):
        async with self._lock:
            await self._conn.execute("DELETE FROM ake_trades")
            await self._conn.commit()

    async def clear_day_trades(self):
        async with self._lock:
            await self._conn.execute("DELETE FROM trades")
            await self._conn.commit()

    async def get_recent_ake_trades(self, limit=15):
        async with self._lock:
            async with self._conn.execute(
                "SELECT signal, entry, close_price, max_r_hit, peak_r, rule_r, result, closed_at "
                "FROM ake_trades ORDER BY id DESC LIMIT ?",
                (limit,),
            ) as cur:
                rows = await cur.fetchall()
                return [
                    {
                        "signal": r[0], "entry": r[1], "close_price": r[2],
                        "max_r_hit": int(r[3] or 0), "peak_r": float(r[4] or 0.0),
                        "rule_r": float(r[5] or 0.0), "result": r[6], "closed_at": r[7],
                    }
                    for r in rows
                ]

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

# ---------- MetaApi Cloud REST Engine (Railway Linux & Android Termux uchun bepul/to'g'ridan-to'g'ri MT5 savdo) ----------
METAAPI_REGIONS = (
    "https://mt-client-api-v1.agiliumtrade.agiliumtrade.ai",
    "https://mt-client-api-v1.london.agiliumtrade.ai",
    "https://mt-client-api-v1.new-york.agiliumtrade.ai",
    "https://mt-client-api-v1.singapore.agiliumtrade.ai",
)

async def _metaapi_request(session, token, account_id, method, subpath, payload=None):
    headers = {"auth-token": token, "Content-Type": "application/json"}
    last_err = None
    for base_url in METAAPI_REGIONS:
        url = f"{base_url}/users/current/accounts/{account_id}{subpath}"
        try:
            async with session.request(method, url, json=payload, headers=headers, timeout=T(12)) as r:
                if r.status in (200, 201, 204):
                    if r.status == 204:
                        return {"ok": True}
                    data = await r.json(content_type=None)
                    return {"ok": True, "data": data, "base_url": base_url}
                txt = (await r.text())[:250]
                last_err = f"HTTP {r.status}: {txt}"
                if r.status in (401, 403):
                    break
        except Exception as e:
            last_err = str(e)
    return {"ok": False, "msg": last_err or "MetaApi ulanish xatosi"}

async def metaapi_open_order(session, token, account_id, side, lot, sl, tp, symbol="XAUUSDm"):
    action_type = "ORDER_TYPE_BUY" if side == "LONG" else "ORDER_TYPE_SELL"
    payload = {
        "actionType": action_type,
        "symbol": symbol,
        "volume": round(float(lot), 2),
        "stopLoss": round(float(sl), 2),
        "takeProfit": round(float(tp), 2),
        "comment": "SMC_v21_Auto",
    }
    res = await _metaapi_request(session, token, account_id, "POST", "/trade", payload)
    if res.get("ok"):
        d = res.get("data") or {}
        retcode = d.get("numericCode") or d.get("stringCode") or "DONE"
        if d.get("stringCode") in ("TRADE_RETCODE_DONE", "ERR_NO_ERROR", None) or d.get("numericCode") in (10009, 0, None):
            return {
                "ok": True,
                "ticket": d.get("orderId") or d.get("positionId") or "MetaApi",
                "price": d.get("openPrice") or sl,
                "sl": round(float(sl), 2),
                "tp": round(float(tp), 2),
                "symbol": symbol,
            }
        return {"ok": False, "msg": f"MetaApi rad etdi ({retcode}): {d.get('message', '')}"}
    return res

async def metaapi_modify_or_close(session, token, account_id, action="close", new_sl=None):
    pos_res = await _metaapi_request(session, token, account_id, "GET", "/positions")
    if not pos_res.get("ok"):
        return pos_res
    positions = pos_res.get("data") or []
    count = 0
    for pos in positions:
        pos_id = pos.get("id")
        sym = pos.get("symbol")
        if not pos_id:
            continue
        if action == "close":
            r = await _metaapi_request(session, token, account_id, "POST", "/trade", {
                "actionType": "POSITION_CLOSE_ID",
                "positionId": str(pos_id),
            })
            if r.get("ok"):
                count += 1
        elif action == "modify_sl" and new_sl is not None:
            r = await _metaapi_request(session, token, account_id, "POST", "/trade", {
                "actionType": "POSITION_MODIFY",
                "positionId": str(pos_id),
                "stopLoss": round(float(new_sl), 2),
                "takeProfit": pos.get("takeProfit"),
            })
            if r.get("ok"):
                count += 1
    return {"ok": True, "count": count}

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
    tp_target = trade.get("tp5") or trade.get("tp2") or trade["tp1"]
    if len(_BRIDGE_SIGNALS_QUEUE) >= 100:
        _BRIDGE_SIGNALS_QUEUE.pop(0)
    _BRIDGE_SIGNALS_QUEUE.append({
        "id": f"sig_{int(time.time()*1000)}",
        "ts": int(time.time()),
        "status": "pending",
        "action": "OPEN",
        "engine": trade.get("engine", "DAY_TRADING"),
        "signal": trade["signal"],
        "entry": trade["entry"],
        "sl": trade["sl"],
        "tp1": trade["tp1"],
        "tp2": tp_target,
        "lot": trade.get("lot", XM_MIN_LOT),
    })

    # Global MetaApi env bo'lsa va bazada hisob bo'lmasa ham ishlatish
    meta_cfg = await db.state_get("metaapi_config")
    if not meta_cfg and METAAPI_TOKEN_ENV and METAAPI_ACCOUNT_ID_ENV:
        meta_cfg = {"token": METAAPI_TOKEN_ENV, "account_id": METAAPI_ACCOUNT_ID_ENV, "symbol": "XAUUSDm", "lot": XM_MIN_LOT}

    if meta_cfg and meta_cfg.get("token") and meta_cfg.get("account_id"):
        m_lot = meta_cfg.get("lot") or trade.get("lot") or XM_MIN_LOT
        m_sym = meta_cfg.get("symbol") or ("GOLD" if _ACTIVE_BROKER == "xm" else "XAUUSDm")
        m_res = await metaapi_open_order(
            session, meta_cfg["token"], meta_cfg["account_id"],
            trade["signal"], m_lot, trade["sl"], tp_target, symbol=m_sym
        )
        if m_res.get("ok"):
            await send_telegram(
                session,
                f"✅ <b>[Cloud MT5 Auto-Trade] {side_txt(trade['signal'])} ochildi!</b>\n"
                f"Simbol: <code>{html.escape(str(m_sym))}</code> | Lot: <code>{m_lot}</code> | "
                f"SL: <code>{m_res.get('sl')}</code> | TP: <code>{m_res.get('tp')}</code>",
            )
        else:
            await send_telegram(
                session,
                f"⚠️ <b>[Cloud MT5 Auto-Trade] xato:</b> {html.escape(str(m_res.get('msg')))}",
            )

    if not accounts:
        return

    for acc in accounts:
        try:
            pw = decrypt_secret(acc["password_enc"])
        except Exception:
            continue
        lot = acc.get("lot") or trade.get("lot") or XM_MIN_LOT
        srv_str = str(acc.get("server") or "")
        # Agar hisob MetaApi orqali ulangan bo'lsa (server == 'METAAPI' va login == account_id, pw == metaapi_token)
        if srv_str.upper().startswith("METAAPI"):
            m_res = await metaapi_open_order(
                session, pw, acc["login"],
                trade["signal"], lot, trade["sl"], tp_target,
                symbol=acc.get("symbol", "XAUUSDm"),
            )
            if m_res.get("ok"):
                await db.update_exness_status(
                    acc["chat_id"], acc["slot"],
                    status_text=f"MetaApi Ochiq #{m_res.get('ticket')} ({side_txt(trade['signal'])})",
                )
                await send_telegram(
                    session,
                    f"✅ <b>Exness/XM #{acc['slot']} (Cloud): {side_txt(trade['signal'])} ochildi</b>\n"
                    f"Lot: <code>{lot}</code> | SL: <code>{m_res.get('sl')}</code> | TP: <code>{m_res.get('tp')}</code>",
                    chat_id=acc["chat_id"],
                )
            else:
                await db.update_exness_status(acc["chat_id"], acc["slot"], status_text=m_res.get("msg", "Xato"))
                await send_telegram(
                    session,
                    f"⚠️ <b>Exness/XM #{acc['slot']} Cloud xato:</b> {html.escape(str(m_res.get('msg')))}",
                    chat_id=acc["chat_id"],
                )
        elif _HAS_MT5:
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
                    status_text=f"Ochiq #{res.get('ticket')} ({side_txt(trade['signal'])})",
                )
                await send_telegram(
                    session,
                    f"✅ <b>Exness #{acc['slot']}: {side_txt(trade['signal'])} ochildi</b>\n"
                    f"Lot: <code>{lot}</code> | Narx: <code>{res.get('price')}</code> | "
                    f"SL: <code>{res.get('sl')}</code> | TP: <code>{res.get('tp')}</code>",
                    chat_id=acc["chat_id"],
                )
            else:
                await db.update_exness_status(acc["chat_id"], acc["slot"], status_text=res.get("msg", "Xato"))
                await send_telegram(
                    session,
                    f"⚠️ <b>Exness #{acc['slot']} xato:</b> {html.escape(str(res.get('msg')))}",
                    chat_id=acc["chat_id"],
                )
        else:
            await db.update_exness_status(
                acc["chat_id"], acc["slot"],
                status_text=f"Bridge navbatida: {side_txt(trade['signal'])} @ {trade['entry']}",
            )

async def sync_exness_positions_action(action="close", new_sl=None):
    # Bridge queue ga ham SL o'zgarishi yoki CLOSE buyrug'ini qo'shamiz (Termux / EA bridge uchun)
    if len(_BRIDGE_SIGNALS_QUEUE) >= 100:
        _BRIDGE_SIGNALS_QUEUE.pop(0)
    _BRIDGE_SIGNALS_QUEUE.append({
        "id": f"act_{int(time.time()*1000)}",
        "ts": int(time.time()),
        "status": "pending",
        "action": "CLOSE" if action == "close" else "MODIFY_SL",
        "new_sl": new_sl,
    })

    meta_cfg = await db.state_get("metaapi_config")
    if not meta_cfg and METAAPI_TOKEN_ENV and METAAPI_ACCOUNT_ID_ENV:
        meta_cfg = {"token": METAAPI_TOKEN_ENV, "account_id": METAAPI_ACCOUNT_ID_ENV}

    accounts = await db.get_all_active_exness_accounts()
    if not _HAS_MT5 and not meta_cfg and not any(str(a.get("server", "")).upper().startswith("METAAPI") for a in accounts):
        return

    async with aiohttp.ClientSession() as tmp_sess:
        if meta_cfg and meta_cfg.get("token") and meta_cfg.get("account_id"):
            try:
                await metaapi_modify_or_close(tmp_sess, meta_cfg["token"], meta_cfg["account_id"], action=action, new_sl=new_sl)
            except Exception as e:
                logger.warning(f"MetaApi {action} xato: {_safe_log_error(e)}")

        for acc in accounts:
            try:
                pw = decrypt_secret(acc["password_enc"])
                if str(acc.get("server", "")).upper().startswith("METAAPI"):
                    await metaapi_modify_or_close(tmp_sess, pw, acc["login"], action=action, new_sl=new_sl)
                elif _HAS_MT5:
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
_LAST_CANDLES_1M = []
_LAST_AKE_INFO = {}
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


# ==================== TUSHUNARLI MATN YORDAMCHILARI ====================
def side_txt(signal):
    """Ichkarida LONG/SHORT qoladi, foydalanuvchiga BUY/SELL ko'rsatiladi."""
    return "BUY" if str(signal).upper() == "LONG" else "SELL"

def side_icon(signal):
    return "🟢" if str(signal).upper() == "LONG" else "🔴"

def trend_txt(bias):
    if bias == "bullish":
        return "yuqoriga ↑"
    if bias == "bearish":
        return "pastga ↓"
    return "yo'q"

_RESULT_TXT = {
    "TP (2.0R)": "TP ✅ (foyda)",
    "TP1": "TP ✅ (foyda)",
    "TP2": "TP ✅ (foyda)",
    "TP3": "TP ✅ (foyda)",
    "TP1 + Trailing SL": "Foyda bilan yopildi ✅",
    "TP2 + Trailing SL": "Foyda bilan yopildi ✅",
    "BE+": "Zararsiz yopildi ➖",
    "BE": "Zararsiz yopildi ➖",
    "SL": "SL ❌ (zarar)",
    "MANUAL": "Qo'lda yopildi 🛑",
    "REVERSAL_FLIP": "Teskari signal 🔄",
}

def result_txt(result):
    return _RESULT_TXT.get(result, str(result))

def calc_trade_pnl(trade, price, lot):
    """0.01 lot uchun yagona TP bo'yicha sof foyda/zarar (USD) va R."""
    init_sl = trade.get("initial_sl", trade["sl"])
    entry = trade["entry"]
    is_long = trade["signal"] == "LONG"
    pnl_pts = (price - entry) if is_long else (entry - price)
    pnl_usd = round((lot / XM_MIN_LOT) * pnl_pts * XM_OZ_PER_MIN_LOT, 2)
    r_mult = pnl_pts / max(abs(entry - init_sl), 0.01)
    return pnl_usd, r_mult

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

async def apply_pnl_to_balance(pnl_usd):
    """Bitim yopilganda foyda/zararni balansga qo'shadi. Yangi balansni qaytaradi."""
    acc = await get_account()
    new_bal = round(max(MIN_AUTO_BALANCE_USD, float(acc["balance"]) + float(pnl_usd)), 2)
    await update_account(balance=new_bal)
    return new_bal

def _is_win_result(r):
    return r in ("TP (2.0R)", "TP1", "TP2", "TP3")

async def win_rate_text():
    results = await db.get_all_results()
    total = len(results)
    if not total:
        return "Hali statistika yo'q (Kazino bloki #1: 0/20)"
    wins = sum(1 for r in results if _is_win_result(r))
    losses = sum(1 for r in results if r == "SL")
    be = total - wins - losses
    denom = wins + losses
    wr = (wins / denom * 100) if denom > 0 else 0.0
    block_num = (total - 1) // 20 + 1
    in_block = ((total - 1) % 20) + 1
    return (
        f"WR: {wr:.1f}% ({wins}✅ / {losses}❌ / {be}🛡 | Jami: {total}) | "
        f"🎲 Blok #{block_num}: {in_block}/20"
    )

async def check_psychology_guard():
    """Trading in the Zone: Anti-Revenge (Tilt) va Anti-Euphoria nazorati."""
    recent = await db.get_recent_trades(limit=5)
    if not recent:
        return {"score_add": 0, "risk_mult": 1.0, "note": None}
    # Ketma-ket SL (Revenge trading himoyasi)
    consec_sl = 0
    for t in recent:
        if t["result"] == "SL":
            consec_sl += 1
        else:
            break
    if consec_sl >= 2:
        return {
            "score_add": 6,
            "risk_mult": 0.75,
            "note": f"🛑 Anti-Tilt ({consec_sl}x SL): Faqat A/A+ signal va -25% risk",
        }
    # Ketma-ket Win (Eyforiya himoyasi)
    consec_win = 0
    for t in recent:
        if _is_win_result(t["result"]):
            consec_win += 1
        else:
            break
    if consec_win >= 3:
        return {
            "score_add": 0,
            "risk_mult": 1.0,
            "note": f"🛡 Anti-Eyforiya ({consec_win}x Win): Standart risk qat'iy saqlandi",
        }
    return {"score_add": 0, "risk_mult": 1.0, "note": None}

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
        "Berilgan signalga 1-2 jumlada juda qisqa, oddiy o'zbek tilida xulosa ber. "
        "Murakkab terminlarsiz: kirish xavfsizmi yoki yo'qmi, va nimaga e'tibor berish kerak. "
        "LONG o'rniga BUY, SHORT o'rniga SELL deb yoz."
    )
    user_prompt = (
        f"Signal: {side_txt(trade['signal'])} | Grade: {trade.get('grade')} ({trade.get('score')}/100)\n"
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
                f"🧠 <b>AI ({side_txt(trade['signal'])} @ {trade['entry']}):</b> <i>{html.escape(txt)}</i>",
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
            "Sen SMC/ICT mentorisan. Oltin (XAUUSD) bitimi Stop Loss bo'ldi. "
            "15 daqiqadan keyingi narxga qarab, SL sababini 1-2 jumlada oddiy o'zbek tilida tushuntir. "
            "LONG o'rniga BUY, SHORT o'rniga SELL deb yoz."
        )
        prompt = (
            f"Bitim: {side_txt(trade['signal'])} | Entry: {trade['entry']} | SL urildi: {sl_price}\n"
            f"TP1: {trade['tp1']} | Zona: 15m {trade.get('zone_kind')} ({trade.get('zone_bottom')}-{trade.get('zone_top')})\n"
            f"Tasdiqlar: {trade.get('confirmations', '-')}\n"
            f"SL dan 15 daqiqa keyingi joriy narx: {now_p} (15m bias: {_LAST_STRUCT_15M.get('bias')})"
        )
        txt = await ask_gemini_ai(session, prompt, system_instruction=sys_prompt)
        if txt:
            await send_telegram(
                session,
                f"🧬 <b>SL sababi ({side_txt(trade['signal'])}):</b> <i>{html.escape(txt)}</i>",
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
    if len(rows) < 1:
        return 0, []
    stats = {}
    for res, conf_str in rows:
        is_tp = _is_win_result(res)
        is_be = res in ("BE+", "BE", "TP1 + Trailing SL", "TP2 + Trailing SL")
        is_loss = (res == "SL")
        for tag in (conf_str or "").split(", "):
            tag = tag.strip()
            if not tag or "Xotira:" in tag:
                continue
            w, l, b = stats.get(tag, (0, 0, 0))
            stats[tag] = (
                w + (1 if is_tp else 0),
                l + (1 if is_loss else 0),
                b + (1 if is_be else 0),
            )
    adj, notes = 0, []
    # 1) Oxirgi bitim natijasiga qarab darhol moslashish (1-bitimdanoq ishlaydi)
    last_res, _ = rows[0]
    if last_res == "SL":
        adj -= 4
        notes.append("🧠Moslashuv: Oxirgi bitim SL (-4 filtr)")
    elif last_res in ("BE+", "BE", "TP1 + Trailing SL"):
        adj -= 2
        notes.append("🧠Moslashuv: Oxirgi bitim TP ga yetmay BE bo'ldi (aniqroq kirish -2)")
    elif _is_win_result(last_res):
        adj += 3
        notes.append("🧠Moslashuv: Oxirgi bitim to'liq TP (+3)")
    # 2) Har bir tasdiq (confirmation) bo'yicha 1-bitimdan boshlab xotira tahlili
    for c in list(current_confs):
        w, l, b = stats.get(c, (0, 0, 0))
        total = w + l + b
        if total >= 1:
            if l > w and l >= 1:
                adj -= 4
                notes.append(f"🧠Xotira: {c} SL bergan (-4)")
            elif b > w and w == 0:
                adj -= 2
                notes.append(f"🧠Xotira: {c} faqat BE bergan (-2)")
            elif w > l:
                adj += 4
                notes.append(f"🧠Xotira: {c} TP bergan (+4)")
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
    window = [list(c) for c in (candles[-65:] if len(candles) >= 65 else candles)]
    if len(window) < 10:
        return None
    uni_p = get_unified_price(window)
    if uni_p is not None:
        window[-1][C] = uni_p
        if uni_p > window[-1][H]:
            window[-1][H] = uni_p
        if uni_p < window[-1][L]:
            window[-1][L] = uni_p

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
            ("TP", trade["tp1"], (30, 235, 180)),
        ]
        for lbl, val, col in levels:
            if p_min <= val <= p_max:
                ly = price_to_y(val)
                draw_hline(ly, margin_left, width - margin_right, col, dashed=(lbl != "ENTRY"))
                draw_text(width - margin_right + 8, ly - 4, f"{lbl}:{val:.1f}", col, scale=1)

    last_p = get_unified_price(window) or window[-1][C]
    brk_lbl = _ACTIVE_BROKER.upper()
    bias_str = (struct_info.get("bias") or "NEUTRAL").upper()
    hdr = f"{brk_lbl} GOLD 15M | PRICE: {last_p:.2f} | BIAS: {bias_str}"
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

def render_ake_chart_png(candles_1m, ake_trade=None, ake_stats=None, scalp_band=None, zones_1m=None, width=820, height=520):
    """TradingView 'Ake signals 7' 1m uslubidagi maxsus grafik:
    - 1m shamlar + Scalp kanal chizig'i
    - Yashil TP1..TP5 (1R..5R) va Qizil SL (-1R) qutilari + Long/Short yorlig'i
    - Pastki chap burchakda 'Ake signals 7 · 1' (Den / Nedelya / Mesyats) R-statistika jadvali
    """
    window = [list(c) for c in (candles_1m[-60:] if len(candles_1m) >= 60 else candles_1m)]
    if len(window) < 10:
        return None
    uni_p = get_unified_price(window)
    if uni_p is not None:
        window[-1][C] = uni_p
        if uni_p > window[-1][H]:
            window[-1][H] = uni_p
        if uni_p < window[-1][L]:
            window[-1][L] = uni_p

    bg_r, bg_g, bg_b = 245, 247, 250  # TradingView yorug' (light) foni (skrinshotdagi kabi)
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
            if not dashed or (x // 4) % 2 == 0:
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
    if ake_trade:
        for k in ("entry", "sl", "tp1", "tp2", "tp3", "tp4", "tp5"):
            if ake_trade.get(k) is not None:
                p_highs.append(ake_trade[k])
                p_lows.append(ake_trade[k])
    p_max = max(p_highs)
    p_min = min(p_lows)
    pad = max((p_max - p_min) * 0.10, 1.2)
    p_max += pad
    p_min -= pad

    margin_left, margin_right = 16, 118
    margin_top, margin_bottom = 42, 24
    plot_w = width - margin_left - margin_right
    plot_h = height - margin_top - margin_bottom

    def price_to_y(p):
        return int(margin_top + (p_max - p) / max(p_max - p_min, 0.01) * plot_h)

    # Grid lines
    for i in range(7):
        gp = p_min + (p_max - p_min) * (i / 6.0)
        gy = price_to_y(gp)
        draw_hline(gy, margin_left, width - margin_right, (220, 225, 232), dashed=True)
        draw_text(width - margin_right + 8, gy - 3, f"{gp:.2f}", (70, 78, 92), scale=1)

    n_bars = len(window)
    proj_bars = 16 if ake_trade else 2
    total_slots = n_bars + proj_bars
    step_x = plot_w / max(total_slots, 1)

    # 1m SMC/ICT Zonalarni (OB, FVG, IFVG, FU, BB) chizish
    if zones_1m:
        start_idx_1m = max(0, len(candles_1m) - len(window))
        ref_p = window[-1][C]
        vis_z = [z for z in zones_1m if z["top"] >= p_min and z["bottom"] <= p_max]
        vis_z.sort(key=lambda z: abs(ref_p - z["ce"]))
        for z in vis_z[:6]:
            zy1 = price_to_y(z["top"])
            zy2 = price_to_y(z["bottom"])
            rel_i = max(0, z.get("index", 0) - start_idx_1m)
            zx_start = int(margin_left + rel_i * step_x)
            zx_end = int(margin_left + n_bars * step_x)
            z_col = (22, 165, 115) if z["type"] == "bullish" else (215, 60, 75)
            fill_rect(zx_start, zy1, zx_end, zy2, z_col, alpha=0.14)
            draw_hline(zy1, zx_start, zx_end, z_col, dashed=True)
            draw_hline(zy2, zx_start, zx_end, z_col, dashed=True)
            draw_text(zx_start + 3, min(zy1, zy2) + 2, z["kind"], z_col, scale=1)

    # Draw Ake Signals 7 TP1..TP5 (green) & SL -1R (red) box starting from exact entry candle and projecting right
    if ake_trade:
        entry_bar_ts = ake_trade.get("bar_ts")
        entry_idx = n_bars - 1
        if entry_bar_ts:
            for idx_b, cb in enumerate(window):
                if cb[0] >= entry_bar_ts:
                    entry_idx = idx_b
                    break
        zx1 = int(margin_left + (entry_idx + 0.5) * step_x)
        zx2 = width - margin_right - 4
        ey = price_to_y(ake_trade["entry"])
        sly = price_to_y(ake_trade.get("initial_sl", ake_trade["sl"]))
        tp5y = price_to_y(ake_trade["tp5"])
        fill_rect(zx1, min(ey, tp5y), zx2, max(ey, tp5y), (46, 189, 133), alpha=0.18)
        fill_rect(zx1, min(ey, sly), zx2, max(ey, sly), (246, 70, 93), alpha=0.18)
        draw_hline(ey, zx1, zx2, (40, 55, 75))
        draw_text(zx1 + 4, ey - 9, f"E {ake_trade['entry']:.2f}", (25, 35, 50), scale=1)
        draw_hline(sly, zx1, zx2, (220, 50, 70), dashed=True)
        draw_text(zx1 + 4, sly - 9, f"SL {ake_trade.get('initial_sl', ake_trade['sl']):.2f} -1R", (200, 40, 60), scale=1)

        max_hit = int(ake_trade.get("max_r_hit", 0))
        for idx_tp in range(1, 6):
            tp_val = ake_trade.get(f"tp{idx_tp}")
            if tp_val is None:
                continue
            tpy = price_to_y(tp_val)
            draw_hline(tpy, zx1, zx2, (22, 150, 110), dashed=True)
            mark = " V" if max_hit >= idx_tp else ""
            draw_text(zx1 + 4, tpy - 9, f"TP{idx_tp} {tp_val:.2f} {idx_tp}R{mark}", (16, 125, 90), scale=1)

        # Long / Short badge
        is_long = ake_trade["signal"] == "LONG"
        badge_col = (22, 150, 110) if is_long else (225, 55, 70)
        badge_txt = "BUY" if is_long else "SELL"
        by = min(height - 30, max(margin_top + 4, sly + 6 if is_long else sly - 20))
        fill_rect(zx1 + 6, by, zx1 + 48, by + 15, badge_col, alpha=0.95)
        draw_text(zx1 + 11, by + 4, badge_txt, (255, 255, 255), scale=1)

    # Candlesticks
    candle_w = max(3, int(step_x * 0.64))
    for i, c in enumerate(window):
        cx = int(margin_left + (i + 0.5) * step_x)
        yh = price_to_y(c[H])
        yl = price_to_y(c[L])
        yo = price_to_y(c[O])
        yc = price_to_y(c[C])
        bull = c[C] >= c[O]
        col = (14, 159, 110) if bull else (224, 52, 68)
        fill_rect(cx, yh, cx, yl, col)
        b_top, b_bot = min(yo, yc), max(yo, yc)
        if b_bot == b_top:
            b_bot += 1
        fill_rect(cx - candle_w // 2, b_top, cx + candle_w // 2, b_bot, col)

    # Header (Day Trading bilan 100% bir xil joriy narx)
    last_p = get_unified_price(window) or window[-1][C]
    brk_lbl = _ACTIVE_BROKER.upper()
    hdr = f"{brk_lbl} XAUUSD 1M | AKE SIGNALS 7 | PRICE: {last_p:.2f}"
    draw_text(margin_left, 10, hdr, (25, 32, 45), scale=2)
    if scalp_band:
        draw_text(margin_left, 28, f"SCALP BAND: {scalp_band[0]:.2f} - {scalp_band[1]:.2f}", (180, 115, 20), scale=1)

    # Pastki chap burchakdagi O'zbekcha Ake signals 7 statistika jadvali
    if ake_stats:
        tx0, ty0 = margin_left + 4, height - 148
        tx1, ty1 = margin_left + 430, height - 18
        fill_rect(tx0, ty0, tx1, ty1, (22, 27, 38), alpha=0.94)
        fill_rect(tx0, ty0 + 70, tx1, ty0 + 92, (20, 75, 68), alpha=0.85)
        draw_text(tx0 + 8, ty0 + 8, "AKE SIGNALS 7", (210, 220, 235), scale=1)
        draw_text(tx0 + 135, ty0 + 8, "KUN", (160, 175, 195), scale=1)
        draw_text(tx0 + 235, ty0 + 8, "HAFTA", (160, 175, 195), scale=1)
        draw_text(tx0 + 335, ty0 + 8, "OY", (160, 175, 195), scale=1)

        d_st = ake_stats.get("day", {})
        w_st = ake_stats.get("week", {})
        m_st = ake_stats.get("month", {})

        rows_draw = [
            ("BITIMLAR", f"{d_st.get('total', 0)}", f"{w_st.get('total', 0)}", f"{m_st.get('total', 0)}", (230, 235, 245)),
            ("TP OLINDI", f"{d_st.get('wins', 0)} ({d_st.get('wr', 0)}%)", f"{w_st.get('wins', 0)} ({w_st.get('wr', 0)}%)", f"{m_st.get('wins', 0)} ({m_st.get('wr', 0)}%)", (46, 204, 145)),
            ("STOP -1R", f"{d_st.get('losses', 0)}", f"{w_st.get('losses', 0)}", f"{m_st.get('losses', 0)}", (246, 85, 100)),
            ("JAMI R", f"{d_st.get('total_r', 0.0):+.1f}R", f"{w_st.get('total_r', 0.0):+.1f}R", f"{m_st.get('total_r', 0.0):+.1f}R", (46, 225, 160)),
            ("ORTACHA R", f"{d_st.get('avg_r', 0.0):+.1f}R", f"{w_st.get('avg_r', 0.0):+.1f}R", f"{m_st.get('avg_r', 0.0):+.1f}R", (46, 204, 145)),
            ("QOIDA R", f"{d_st.get('rule_r', 0.0):+.1f}R", f"{w_st.get('rule_r', 0.0):+.1f}R", f"{m_st.get('rule_r', 0.0):+.1f}R", (175, 185, 205)),
        ]
        for r_i, (r_lbl, c1_v, c2_v, c3_v, r_col) in enumerate(rows_draw):
            ry = ty0 + 25 + r_i * 17
            draw_text(tx0 + 8, ry, r_lbl, (170, 180, 200), scale=1)
            draw_text(tx0 + 135, ry, c1_v, r_col, scale=1)
            draw_text(tx0 + 235, ry, c2_v, r_col, scale=1)
            draw_text(tx0 + 335, ry, c3_v, r_col, scale=1)

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

async def render_ake_chart_async(candles_1m, ake_trade=None, ake_stats=None, scalp_band=None, zones_1m=None):
    loop = asyncio.get_running_loop()
    t_copy = dict(ake_trade) if ake_trade else None
    s_copy = dict(ake_stats) if ake_stats else None
    z_copy = list(zones_1m) if zones_1m else []
    return await loop.run_in_executor(
        None, lambda: render_ake_chart_png(list(candles_1m), t_copy, s_copy, scalp_band, z_copy)
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
            "1h": "60", "4h": "240", "1d": "D", "1w": "W", "1M": "M"}
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


# ==================== AVTO-SYNC: EXNESS VA XM REAL SPOT XAUUSD NARX SINXRONIZATORI ====================
SWISSQUOTE_XAU_URL = "https://forex-data-feed.swissquote.com/public-quotes/bboquotes/instrument/XAU/USD"
YAHOO_XAU_SPOT_URL = "https://query1.finance.yahoo.com/v8/finance/chart/XAUUSD=X"

async def fetch_spot_xauusd_price(session):
    """Exness (XAUUSDm) va XM (GOLD) foydalanadigan haqiqiy Spot XAU/USD interbank kotirovkasini olish."""
    # 0-manba: Agar Termux Bridge ulangan bo'lsa, bevosita foydalanuvchining Exness/XM MT5 dagi jonli narxini oladi!
    if (time.time() - _TERMUX_BRIDGE_STATE.get("last_seen", 0)) <= 30:
        t_bid = _TERMUX_BRIDGE_STATE.get("bid")
        t_ask = _TERMUX_BRIDGE_STATE.get("ask")
        if t_bid and t_ask and float(t_bid) > 1000:
            return round((float(t_bid) + float(t_ask)) / 2.0, 2), "Termux Live Exness/XM MT5" 
    # 1-manba: Swissquote Interbank BBO Spot XAU/USD (Exness/XM LP kotirovkasi bilan 1:1)
    try:
        async with session.get(SWISSQUOTE_XAU_URL, headers=HEADERS, timeout=T(7)) as r:
            if r.status == 200:
                data = await r.json(content_type=None)
                if isinstance(data, list) and data:
                    prices = data[0].get("spreadProfilePrices") or []
                    if prices:
                        bid = float(prices[0].get("bid", 0))
                        ask = float(prices[0].get("ask", 0))
                        if bid > 1000 and ask > 1000:
                            return round((bid + ask) / 2.0, 2), "Swissquote Spot XAU/USD"
    except Exception:
        pass

    # 2-manba: Yahoo Finance XAUUSD=X Spot
    try:
        async with session.get(YAHOO_XAU_SPOT_URL, params={"interval": "1m", "range": "1d"}, headers=HEADERS, timeout=T(7)) as r:
            if r.status == 200:
                d = await r.json(content_type=None)
                meta = d["chart"]["result"][0]["meta"]
                reg_p = float(meta.get("regularMarketPrice") or 0)
                if reg_p > 1000:
                    return round(reg_p, 2), "Yahoo Spot XAUUSD"
    except Exception:
        pass

    return None, None

async def run_auto_sync_once(session, force=False):
    """Bybit XAUUSDT va haqiqiy Exness/XM Spot XAUUSD orasidagi farqni avtomatik hisoblab sinxronlaydi."""
    global _LAST_SPOT_SYNC_INFO
    if not _AUTO_SYNC_ENABLED and not force:
        return None
    raw_bybit = _LIVE_BYBIT_RAW_PRICE
    if raw_bybit is None:
        try:
            raw_bybit = await fetch_raw_price(session)
        except Exception:
            return None

    spot_p, src = await fetch_spot_xauusd_price(session)
    if spot_p is None:
        return None

    ex_cal = float(_BROKER_CALIBRATION.get("exness", 0.0))
    xm_cal = float(_BROKER_CALIBRATION.get("xm", 0.25))
    exness_p = round(spot_p + ex_cal, 2)
    xm_p = round(spot_p + xm_cal, 2)

    target_p = exness_p if _ACTIVE_BROKER == "exness" else xm_p
    new_off = round(target_p - raw_bybit, 2)

    _LAST_SPOT_SYNC_INFO = {
        "spot_raw": spot_p,
        "exness_price": exness_p,
        "xm_price": xm_p,
        "bybit_raw": round(raw_bybit, 2),
        "source": src,
        "ts": time.time(),
    }

    if force or abs(new_off - _CURRENT_OFFSET) >= 0.08:
        await apply_offset_change(new_off)
    return _LAST_SPOT_SYNC_INFO

async def auto_sync_loop(app):
    session = app["session"]
    await asyncio.sleep(2)
    while True:
        try:
            if _AUTO_SYNC_ENABLED and market_open():
                await run_auto_sync_once(session)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(f"Auto-sync xato: {_safe_log_error(e)}")
        await asyncio.sleep(25)

# ==================== REAL-TIME WEBSOCKET LOOP ====================
async def _ws_monitor(session, price):
    try:
        trade = await db.state_get("current_trade")
        if trade:
            await monitor_trade(session, price, _LAST_STRUCT_15M.get("bias"))
        ake_trade = await db.state_get("current_trade_ake")
        if ake_trade:
            await monitor_ake_trade(session, price)
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

def detect_asia_and_pd_sweeps(candles_15m, pdh, pdl, pwh=None, pwl=None, pmh=None, pml=None):
    asia_high, asia_low = None, None
    ff_high, ff_low = None, None
    daily_open_ny = None
    now = datetime.datetime.now(datetime.timezone.utc)
    now_date = now.date()
    asia_done = now.hour >= ASIA_END_HOUR_UTC
    asia_bars, ff_bars = [], []
    for c in candles_15m[-96:]:
        dt = datetime.datetime.fromtimestamp(c[0] / 1000.0, tz=datetime.timezone.utc)
        if dt.date() == now_date:
            if 0 <= dt.hour < ASIA_END_HOUR_UTC:
                asia_bars.append(c)
            if dt.hour == 6:  # Frankfurt Open (06:00 - 07:00 UTC)
                ff_bars.append(c)
            if dt.hour == 4 and dt.minute == 0 and daily_open_ny is None:
                daily_open_ny = round(c[O], 2)  # NY Midnight Open (00:00 EST/EDT ~ 04:00/05:00 UTC)
    if len(asia_bars) >= 4:
        asia_high = round(max(c[H] for c in asia_bars), 2)
        asia_low = round(min(c[L] for c in asia_bars), 2)
    if ff_bars:
        ff_high = round(max(c[H] for c in ff_bars), 2)
        ff_low = round(min(c[L] for c in ff_bars), 2)

    # 90-Minute ICT Cycle (NY 00:00 dan boshlab har 90 daqiqalik faza)
    mins_since_utc0 = now.hour * 60 + now.minute
    cycle_pos = mins_since_utc0 % 90
    if cycle_pos < 30:
        cycle_90m = "Accumulation (0-30m)"
    elif cycle_pos < 60:
        cycle_90m = "Manipulation / Judas (30-60m)"
    else:
        cycle_90m = "Distribution / Real Move (60-90m)"

    recent = candles_15m[-4:]
    bull_sweeps, bear_sweeps = [], []

    for c in recent:
        if pml and c[L] < pml and c[C] > pml:
            bull_sweeps.append("PML Sweep (O'tgan oy Low likvidligi olindi)")
        if pmh and c[H] > pmh and c[C] < pmh:
            bear_sweeps.append("PMH Sweep (O'tgan oy High likvidligi olindi)")
        if pwl and c[L] < pwl and c[C] > pwl:
            bull_sweeps.append("PWL Sweep (O'tgan hafta Low likvidligi olindi)")
        if pwh and c[H] > pwh and c[C] < pwh:
            bear_sweeps.append("PWH Sweep (O'tgan hafta High likvidligi olindi)")
        if pdl and c[L] < pdl and c[C] > pdl:
            bull_sweeps.append("PDL Sweep (Kecha Low likvidligi olindi)")
        if pdh and c[H] > pdh and c[C] < pdh:
            bear_sweeps.append("PDH Sweep (Kecha High likvidligi olindi)")
        if asia_done and asia_low and c[L] < asia_low and c[C] > asia_low:
            bull_sweeps.append("Asia Low Sweep (Judas Swing)")
        if asia_done and asia_high and c[H] > asia_high and c[C] < asia_high:
            bear_sweeps.append("Asia High Sweep (Judas Swing)")
        if now.hour >= 7 and ff_low and c[L] < ff_low and c[C] > ff_low:
            bull_sweeps.append("Frankfurt Low (FFL) Trap Sweep")
        if now.hour >= 7 and ff_high and c[H] > ff_high and c[C] < ff_high:
            bear_sweeps.append("Frankfurt High (FFH) Trap Sweep")

    return {
        "asia_high": asia_high,
        "asia_low": asia_low,
        "ff_high": ff_high,
        "ff_low": ff_low,
        "daily_open_ny": daily_open_ny,
        "cycle_90m": cycle_90m,
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

def detect_fu_candles(candles):
    """Bozorni zabt etish (4-dars): Likvidlikni olib strukturani buzuvchi FU Candle va uning 50% Wick zonasi."""
    fu_zones = []
    n = len(candles)
    if n < 12:
        return fu_zones
    for i in range(5, n - 1):
        cur = candles[i]
        prev_high = max(c[H] for c in candles[i - 5:i])
        prev_low = min(c[L] for c in candles[i - 5:i])
        prev_struct_low = min(c[L] for c in candles[i - 3:i])
        prev_struct_high = max(c[H] for c in candles[i - 3:i])
        rng = max(cur[H] - cur[L], 0.01)
        upper_wick = cur[H] - max(cur[O], cur[C])
        lower_wick = min(cur[O], cur[C]) - cur[L]
        # Bullish FU: pastdagi likvidlikni (stop-loss) olib, kuchli yuqoriga yopilgan sham
        if cur[L] < prev_low and cur[C] > prev_low and lower_wick / rng >= 0.38 and cur[C] >= prev_struct_high:
            wick_top = min(cur[O], cur[C])
            wick_bot = cur[L]
            wick_50 = (wick_top + wick_bot) / 2.0
            fu_zones.append({
                "type": "bullish", "kind": "FU",
                "top": round(wick_top, 2), "bottom": round(wick_bot, 2),
                "ce": round(wick_50, 2), "index": i, "algo_candle": True,
            })
        # Bearish FU: tepadagi likvidlikni olib, kuchli pastga yopilgan sham
        elif cur[H] > prev_high and cur[C] < prev_high and upper_wick / rng >= 0.38 and cur[C] <= prev_struct_low:
            wick_top = cur[H]
            wick_bot = max(cur[O], cur[C])
            wick_50 = (wick_top + wick_bot) / 2.0
            fu_zones.append({
                "type": "bearish", "kind": "FU",
                "top": round(wick_top, 2), "bottom": round(wick_bot, 2),
                "ce": round(wick_50, 2), "index": i, "algo_candle": True,
            })
    return fu_zones

def detect_ob_and_breakers(candles, max_age_bars=60):
    """3-dars Body-Only Orderblock + ICT Algo Candle (FVG + Liquidity Grab) + Rejection Block (RB)."""
    bodies = [abs(c[C] - c[O]) for c in candles]
    vols = [c[V] if len(c) > 5 else 0 for c in candles]
    raw_obs = []
    n = len(candles)
    for i in range(10, n - 2):
        ab = sum(bodies[i - 10:i]) / 10
        av = sum(vols[i - 10:i]) / 10
        if ab == 0 or av == 0:
            continue
        cur, fol = candles[i], candles[i + 1]
        nxt = candles[i + 2]
        ib = bodies[i + 1] > ab * 1.35
        iv = vols[i + 1] > av * 1.4
        bob = cur[C] < cur[O] and fol[C] > fol[O] and fol[C] > cur[H]
        beob = cur[C] > cur[O] and fol[C] < fol[O] and fol[C] < cur[L]

        # ICT Algo Candle: likvidlik olgan va ketidan FVG qoldirgan OB
        grabbed_low = cur[L] <= min(c[L] for c in candles[max(0, i - 5):i])
        grabbed_high = cur[H] >= max(c[H] for c in candles[max(0, i - 5):i])
        has_bull_fvg = nxt[L] > cur[H]
        has_bear_fvg = nxt[H] < cur[L]

        # Body-Only qoidasi: faqat sham tanasi (Open - Close) olinadi
        body_top = max(cur[O], cur[C])
        body_bot = min(cur[O], cur[C])
        if body_top - body_bot < 0.15:
            body_top = cur[H] if beob else body_top
            body_bot = cur[L] if bob else body_bot

        if ib and iv and bob:
            raw_obs.append({
                "type": "bullish", "kind": "AC_OB" if (grabbed_low and has_bull_fvg) else "OB",
                "top": body_top, "bottom": body_bot,
                "ce": (body_top + body_bot) / 2.0, "index": i,
                "algo_candle": bool(grabbed_low and has_bull_fvg),
            })
        if ib and iv and beob:
            raw_obs.append({
                "type": "bearish", "kind": "AC_OB" if (grabbed_high and has_bear_fvg) else "OB",
                "top": body_top, "bottom": body_bot,
                "ce": (body_top + body_bot) / 2.0, "index": i,
                "algo_candle": bool(grabbed_high and has_bear_fvg),
            })

    zones = []
    for ob in raw_obs:
        fi = ob["index"]
        if n - 1 - fi > max_age_bars:
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
            # Inducement (IDM) tekshiruvi: OB dan keyin va joriy shamdan oldin kichik pullback bo'lganmi
            has_idm = False
            for m_idx in range(fi + 2, n - 1):
                if ob["type"] == "bullish" and candles[m_idx][L] < candles[m_idx - 1][L]:
                    has_idm = True
                    break
                if ob["type"] == "bearish" and candles[m_idx][H] > candles[m_idx - 1][H]:
                    has_idm = True
                    break
            ob["has_idm"] = has_idm
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
                    "algo_candle": ob.get("algo_candle", False),
                    "has_idm": True,
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

def detect_ifvg(candles, min_size=0.20):
    """ICT Inversion Fair Value Gap (IFVG): Impuls bilan buzilgan FVG teskari zonaga aylanadi."""
    raw_fvgs = detect_fvg(candles, min_size=min_size)
    ifvgs = []
    n = len(candles)
    for z in raw_fvgs:
        fi = z["index"]
        if n - 1 - fi > 45:
            continue
        inv_idx = None
        inv_type = None
        check_end = max(fi + 1, n - 1)
        for j in range(fi + 1, check_end):
            c = candles[j]
            if z["type"] == "bearish" and c[C] > z["top"]:
                inv_idx = j
                inv_type = "bullish"
                break
            if z["type"] == "bullish" and c[C] < z["bottom"]:
                inv_idx = j
                inv_type = "bearish"
                break
        if inv_idx is not None and (n - 1 - inv_idx <= 28):
            still_valid = True
            for k in range(inv_idx + 1, max(inv_idx + 1, n - 2)):
                ck = candles[k]
                if inv_type == "bullish" and ck[C] < z["bottom"]:
                    still_valid = False
                    break
                if inv_type == "bearish" and ck[C] > z["top"]:
                    still_valid = False
                    break
            if still_valid:
                ifvgs.append({
                    "type": inv_type, "kind": "IFVG",
                    "top": z["top"], "bottom": z["bottom"],
                    "ce": z["ce"], "index": inv_idx,
                    "algo_candle": True, "has_idm": True,
                })
    return ifvgs

def check_ict_silver_bullet_and_macro():
    """ICT Silver Bullet (03-04, 10-11, 14-15 NY) va 20-daq Macro (xx:50-xx:10) oynalari."""
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    if _HAS_ZONEINFO:
        try:
            ny_dt = now_utc.astimezone(ZoneInfo("America/New_York"))
        except Exception:
            ny_dt = now_utc - datetime.timedelta(hours=4)
    else:
        ny_dt = now_utc - datetime.timedelta(hours=4)

    ny_h, ny_m = ny_dt.hour, ny_dt.minute
    sb_name = None
    if ny_h == 3:
        sb_name = "ICT London Silver Bullet (03:00-04:00 NY)"
    elif ny_h == 10:
        sb_name = "ICT NY AM Silver Bullet (10:00-11:00 NY)"
    elif ny_h == 14:
        sb_name = "ICT NY PM Silver Bullet (14:00-15:00 NY)"

    is_macro = (ny_m >= 50 or ny_m <= 10)
    macro_name = f"ICT 20m Macro ({ny_h:02d}:{ny_m:02d} NY)" if is_macro else None
    return {"silver_bullet": sb_name, "macro": macro_name}

def get_active_zones(candles, cache_key="XAU_15m"):
    if not candles:
        return []
    ts = candles[-1][0]
    full_key = (cache_key, _CURRENT_OFFSET)
    cached = _zones_cache.get(full_key)
    if cached and cached["ts"] == ts:
        return cached["zones"]

    atr = calculate_atr(candles) or 0.0
    min_size = max(0.22, atr * 0.08)
    untested = FVG_REQUIRE_UNTESTED and cache_key == "XAU_15m"
    is_htf = cache_key in ("XAU_1h", "XAU_4h", "XAU_1d")
    fvg_age = 180 if is_htf else 45
    ob_age = 180 if is_htf else 60
    fvgs = [z for z in detect_fvg(candles, min_size) if _fvg_is_fresh(z, candles, max_age_bars=fvg_age, untested=untested)]
    ifvgs = detect_ifvg(candles, min_size=min_size)
    obs_bbs = detect_ob_and_breakers(candles, max_age_bars=ob_age)
    fus = [z for z in detect_fu_candles(candles) if _fvg_is_fresh(z, candles, max_age_bars=fvg_age if is_htf else 35)]
    all_fresh = fvgs + ifvgs + obs_bbs + fus
    _zones_cache[full_key] = {"ts": ts, "zones": all_fresh}
    return all_fresh

def check_5m_ltf_trigger(cl5, struct5, target_bias, zone=None, cl1=None):
    triggers = []
    if struct5["bias"] == target_bias:
        ev = struct5.get("event") or ""
        ev_bar = struct5.get("event_bar", -1)
        if ev_bar >= 0 and (len(cl5) - 1 - ev_bar) <= 14:
            if "CHoCH" in ev:
                triggers.append("5m CHoCH (MSS)")
            elif "BOS" in ev:
                triggers.append("5m BOS")

    zones_5m = get_active_zones(cl5, cache_key="XAU_5m")
    recent_5m_zones = [
        z for z in zones_5m
        if z["type"] == target_bias and (len(cl5) - 1 - z["index"] <= 8)
    ]
    if recent_5m_zones:
        triggers.append(f"5m {recent_5m_zones[-1]['kind']} Displacement")

    if len(cl5) >= 3:
        last, prev, prev2 = cl5[-1], cl5[-2], cl5[-3]
        rng = max(last[H] - last[L], 0.01)
        body = abs(last[C] - last[O])
        upper_wick = last[H] - max(last[O], last[C])
        lower_wick = min(last[O], last[C]) - last[L]
        touches = True
        if zone is not None:
            touches = (
                min(last[L], prev[L]) <= zone["top"] + 1.8
                and max(last[H], prev[H]) >= zone["bottom"] - 1.8
            )

        if touches:
            if target_bias == "bullish":
                if last[C] > last[O] and prev[C] < prev[O] and last[C] >= prev[O]:
                    triggers.append("5m Bullish Engulfing")
                elif lower_wick / rng >= 0.42 and last[C] >= last[O]:
                    triggers.append("5m Bullish Rejection Pin")
                elif last[C] > last[O] and body / rng >= 0.65 and last[C] > prev[H]:
                    triggers.append("5m Bullish Displacement Close")
                elif last[L] < prev[L] and last[C] > prev[C] and last[C] > last[O]:
                    triggers.append("5m Micro Liquidity Grab + Close")
            else:
                if last[C] < last[O] and prev[C] > prev[O] and last[C] <= prev[O]:
                    triggers.append("5m Bearish Engulfing")
                elif upper_wick / rng >= 0.42 and last[C] <= last[O]:
                    triggers.append("5m Bearish Rejection Pin")
                elif last[C] < last[O] and body / rng >= 0.65 and last[C] < prev[L]:
                    triggers.append("5m Bearish Displacement Close")
                elif last[H] > prev[H] and last[C] < prev[C] and last[C] < last[O]:
                    triggers.append("5m Micro Liquidity Grab + Close")

    if cl1 and len(cl1) >= 20:
        struct1 = analyze_market_structure(cl1, cache_key="XAU_1m")
        if struct1.get("bias") == target_bias:
            ev1 = struct1.get("event") or ""
            if "CHoCH" in ev1 or "BOS" in ev1:
                triggers.append(f"1m Sniper {ev1}")

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
        {
            "b1h": None, "b4h": None, "b1d": None, "b1w": None, "b1m": None,
            "pdh": None, "pdl": None, "pwh": None, "pwl": None, "pmh": None, "pml": None,
            "month_high": None, "month_low": None, "month_eq": None,
            "htf_zones": [], "ts": 0, "offset": None,
        },
    )
    age = now_ts - cache.get("ts", 0)
    if 0 <= age < 60 and cache.get("offset") == _CURRENT_OFFSET:
        return cache

    tasks = [
        fetch_ohlcv(session, "1h", limit=HTF_LIMIT),       # 720 ta sham = 30 kun (1 oy)
        fetch_ohlcv(session, "4h", limit=HTF_4H_LIMIT),    # 180 ta sham = 30 kun (1 oy)
        fetch_ohlcv(session, "1d", limit=HTF_1D_LIMIT),    # 120 ta sham = 4 oy
        fetch_ohlcv(session, "1w", limit=HTF_1W_LIMIT),    # 24 hafta
        fetch_ohlcv(session, "1M", limit=HTF_1M_LIMIT),    # 12 oy
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
    b1w = safe_bias(3, "1w", "b1w")
    b1m = safe_bias(4, "1M", "b1m")

    c1h = results[0] if not isinstance(results[0], Exception) else None
    c4h = results[1] if not isinstance(results[1], Exception) else None
    c1d = results[2] if not isinstance(results[2], Exception) else None
    c1w = results[3] if not isinstance(results[3], Exception) else None
    c1m = results[4] if not isinstance(results[4], Exception) else None

    pdh, pdl = compute_pdh_pdl(c1h, c1d)
    if pdh is None:
        pdh, pdl = cache.get("pdh"), cache.get("pdl")

    # O'tgan hafta High/Low (PWH / PWL)
    pwh, pwl = cache.get("pwh"), cache.get("pwl")
    if c1w and len(c1w) >= 2:
        pwh, pwl = round(c1w[-2][H], 2), round(c1w[-2][L], 2)
    elif c1d and len(c1d) >= 8:
        prev_7d = c1d[-8:-1]
        pwh, pwl = round(max(c[H] for c in prev_7d), 2), round(min(c[L] for c in prev_7d), 2)

    # O'tgan oy High/Low (PMH / PML)
    pmh, pml = cache.get("pmh"), cache.get("pml")
    if c1m and len(c1m) >= 2:
        pmh, pml = round(c1m[-2][H], 2), round(c1m[-2][L], 2)

    # Oxirgi 30 kunlik (1 oylik) Dealing Range (High, Low, 50% Equilibrium)
    month_high, month_low, month_eq = cache.get("month_high"), cache.get("month_low"), cache.get("month_eq")
    if c1h and len(c1h) >= 48:
        cl1h_m = closed_only(c1h)[-720:]
        month_high = round(max(c[H] for c in cl1h_m), 2)
        month_low = round(min(c[L] for c in cl1h_m), 2)
        month_eq = round((month_high + month_low) / 2.0, 2)
    elif c1d and len(c1d) >= 5:
        cl1d_m = closed_only(c1d)[-30:]
        month_high = round(max(c[H] for c in cl1d_m), 2)
        month_low = round(min(c[L] for c in cl1d_m), 2)
        month_eq = round((month_high + month_low) / 2.0, 2)

    # 1 oylik 4H va 1D aktiv Order Block / FVG / FU zonalari
    htf_zones = []
    if c4h and len(c4h) >= 20:
        for z in get_active_zones(closed_only(c4h), cache_key="XAU_4h")[-12:]:
            zc = dict(z)
            zc["tf"] = "4H"
            htf_zones.append(zc)
    if c1d and len(c1d) >= 15:
        for z in get_active_zones(closed_only(c1d)[-45:], cache_key="XAU_1d")[-8:]:
            zc = dict(z)
            zc["tf"] = "1D"
            htf_zones.append(zc)

    data = {
        "b1h": b1h, "b4h": b4h, "b1d": b1d, "b1w": b1w, "b1m": b1m,
        "pdh": pdh, "pdl": pdl, "pwh": pwh, "pwl": pwl, "pmh": pmh, "pml": pml,
        "month_high": month_high, "month_low": month_low, "month_eq": month_eq,
        "htf_zones": htf_zones, "ts": now_ts, "offset": _CURRENT_OFFSET,
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
        score -= 5
        confs.append("⚠️ Trend Continuation (Premium/Discount -5)")

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
def _range_sniper_plan(cl15, bias, entry):
    """
    Intraday Sniper v20 standarti (75-80% Win Rate, 5-9 soatlik Day-Trading):
    - Cho'qqida BUY (>85%) yoki tubda SELL (<15%) kirish taqiqlanadi.
    - SL = max(6.0$, 2.30 x ATR) — kunlik shovqinda urib ketmasligi uchun strukturali masofa.
    - TP1 = max(4.5$, 0.90 x ATR) — birinchi intraday impulsda 50% foyda olinadi va SL BE+ ga ko'chadi.
    - TP2 = max(12.0$, 3.60 x ATR) — asosiy kunlik sessiya maqsadi (London/NY to'lqini).
    (sl, tp2, tp1) qaytaradi yoki None.
    """
    if not cl15 or len(cl15) < 40:
        return None
    win = cl15[-RANGE_LOOKBACK_15M:]
    ll = min(c[L] for c in win)
    hh = max(c[H] for c in win)
    width = hh - ll
    if width < RANGE_MIN_WIDTH:
        return None
    pos = (entry - ll) / max(width, 0.01)
    atr = calculate_atr(cl15, 14) or 4.0

    sl_dist = round(max(RANGE_SL_MIN, min(RANGE_SL_MAX, atr * INTRADAY_SL_ATR_MULT)), 2)
    tp1_dist = round(max(4.5, atr * INTRADAY_TP1_ATR_MULT), 2)
    tp2_dist = round(max(RANGE_TP_MIN, min(RANGE_TP_MAX, atr * INTRADAY_TP2_ATR_MULT)), 2)

    if bias == "bullish":
        if not (INTRADAY_POS_MIN_PCT <= pos <= RANGE_ENTRY_ZONE_PCT):
            return None
        return round(entry - sl_dist, 2), round(entry + tp2_dist, 2), round(entry + tp1_dist, 2)
    elif bias == "bearish":
        if not ((1.0 - RANGE_ENTRY_ZONE_PCT) <= pos <= (1.0 - INTRADAY_POS_MIN_PCT)):
            return None
        return round(entry + sl_dist, 2), round(entry - tp2_dist, 2), round(entry - tp1_dist, 2)
    return None

def build_sniper_trade(cl15, cl5, struct15, price, atr15, balance, target_bias=None, setup_model="Day-Trading Sniper"):
    bias = target_bias or struct15["bias"]
    if bias not in ("bullish", "bearish"):
        return None

    # 1) QAT'IY PREMIUM / DISCOUNT (ENG UCHIDAN KIRISH) FILTRI:
    eq15 = struct15.get("eq")
    if eq15:
        if bias == "bullish" and price > eq15:
            return None  # O'rtadan yoki tepadan BUY ochish taqiqlanadi!
        if bias == "bearish" and price < eq15:
            return None  # O'rtadan yoki pastdan (3906 kabi) SELL ochish taqiqlanadi!

    zones_15m = [z for z in get_active_zones(cl15, "XAU_15m") if z["type"] == bias]
    zones_5m = [z for z in get_active_zones(cl5, "XAU_5m") if z["type"] == bias]
    zones_bias = zones_15m + zones_5m
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

    # Ustuvorlik: FU / AC_OB / IFVG va narxga eng yaqin CE
    kind_priority = {"FU": 0, "AC_OB": 0, "IFVG": 0, "OB": 1, "BB": 1, "FVG": 2}
    candidates.sort(key=lambda z: (kind_priority.get(z["kind"], 3), abs(price - z["ce"])))
    zone = candidates[0]
    zone_lo = min(zone["bottom"], zone["top"])
    zone_hi = max(zone["bottom"], zone["top"])

    # 2) ZERO-DRAWDOWN REJECTION SHARTI: Oxirgi 5m sham zonadan o'z yo'nalishiga qarab yopilgan bo'lishi shart
    if len(cl5) >= 2:
        last5 = cl5[-1]
        if bias == "bullish" and last5[C] <= last5[O]:
            return None  # BUY uchun 5m sham yashil (o'suvchi) bo'lib yopilishi shart
        if bias == "bearish" and last5[C] >= last5[O]:
            return None  # SELL uchun 5m sham qizil (tushuvchi) bo'lib yopilishi shart

    confluence = any(
        other is not zone
        and other["kind"] != zone["kind"]
        and other["bottom"] <= zone["top"] + 0.5
        and other["top"] >= zone["bottom"] - 0.5
        for other in zones_bias
    )

    max_sl_pts = SNIPER_MAX_SL_POINTS_SMALL  # Maksimum 6.0$ SL
    min_sl_pts = SNIPER_MIN_SL_POINTS        # Minimum 3.5$ SL
    entry = round(price, 2)

    recent_5m_low = min(c[L] for c in cl5[-3:]) if len(cl5) >= 3 else zone_lo
    recent_5m_high = max(c[H] for c in cl5[-3:]) if len(cl5) >= 3 else zone_hi

    if bias == "bullish":
        structural_sl = min(zone_lo - 0.20, recent_5m_low - 0.15) - XM_SPREAD_BUFFER
        raw_risk = entry - structural_sl
        if raw_risk > max_sl_pts:
            tight_sl = min(zone["ce"] - 0.50, recent_5m_low - 0.20) - XM_SPREAD_BUFFER
            if min_sl_pts <= (entry - tight_sl) <= max_sl_pts:
                raw_risk = entry - tight_sl
    else:
        structural_sl = max(zone_hi + 0.20, recent_5m_high + 0.15) + XM_SPREAD_BUFFER
        raw_risk = structural_sl - entry
        if raw_risk > max_sl_pts:
            tight_sl = max(zone["ce"] + 0.50, recent_5m_high + 0.20) + XM_SPREAD_BUFFER
            if min_sl_pts <= (tight_sl - entry) <= max_sl_pts:
                raw_risk = tight_sl - entry

    if raw_risk <= 0 or (raw_risk > max_sl_pts and SNIPER_SKIP_IF_SL_TOO_WIDE):
        return None

    risk = round(min(max(raw_risk, min_sl_pts), max_sl_pts), 2)
    direction = 1 if bias == "bullish" else -1
    sl = round(entry - direction * risk, 2)

    # 3) YAGONA KATTA DAY-TRADING TP (Kamida 1:3.5 RR yoki kunlik cho'qqi/tubgacha)
    win_96 = cl15[-RANGE_LOOKBACK_15M:] if len(cl15) >= 30 else cl15
    hh_96 = max(c[H] for c in win_96)
    ll_96 = min(c[L] for c in win_96)
    min_tp_dist = round(risk * RR1, 2)  # Kamida 3.5R (masalan 5$ SL bo'lsa, 17.5$ TP)

    if bias == "bullish":
        struct_target = max(hh_96 - entry, min_tp_dist)
        tp_dist = min(max(struct_target, min_tp_dist), risk * 5.0)
    else:
        struct_target = max(entry - ll_96, min_tp_dist)
        tp_dist = min(max(struct_target, min_tp_dist), risk * 5.0)

    tp_final = round(entry + direction * tp_dist, 2)

    return {
        "signal": "LONG" if bias == "bullish" else "SHORT",
        "bias": bias,
        "setup_model": setup_model,
        "zone_kind": zone["kind"],
        "zone_top": round(zone_hi, 2),
        "zone_bottom": round(zone_lo, 2),
        "entry": entry, "sl": sl, "initial_sl": sl,
        "tp1": tp_final, "tp2": tp_final, "tp3": tp_final,  # Yagona TP!
        "rr": round(abs(tp_final - entry) / max(risk, 0.01), 2),
        "confluence": confluence,
        "algo_candle": zone.get("algo_candle", False),
        "has_idm": zone.get("has_idm", False),
        "sniper_mode": True,
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
            f"🤖 <b>Binance: BUY {html.escape(BINANCE_SYMBOL)}</b>\n"
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
        ["📊 Day Signal", "🖼 Day Grafik", "📈 Day Stat"],
        ["⚡ Ake Signal", "📉 Ake Grafik", "📋 Ake Stat"],
        ["🎯 Zonalar", "🧠 AI Tahlil", "🏦 Exness Hisoblar"],
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
        t_ake = await db.state_get("current_trade_ake")
        if t_ake and delta:
            for k in ("entry", "sl", "initial_sl", "tp1", "tp2", "tp3", "tp4", "tp5"):
                if t_ake.get(k) is not None:
                    t_ake[k] = round(t_ake[k] + delta, 2)
            await db.state_set("current_trade_ake", t_ake)
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

    pnl_usd, r_mult = calc_trade_pnl(trade, price, lot)
    new_balance = await apply_pnl_to_balance(pnl_usd)

    sign_icon = "✅" if pnl_usd > 0 else ("❌" if pnl_usd < 0 else "➖")
    await send_telegram(
        session,
        f"{sign_icon} <b>{side_txt(trade['signal'])} yopildi</b> — {html.escape(result_txt(result))}\n"
        f"Narx: <code>{price}</code> | Natija: <b>{pnl_usd:+.2f}$</b> ({r_mult:+.2f}R)\n"
        f"💰 Balans: <b>${new_balance:.2f}</b>",
    )
    if result == "SL" and GEMINI_API_KEY and SEND_AI_EXTRA_MESSAGES:
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
        tp_lvl = trade.get("tp1", entry)  # Yagona asosiy TP
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
                    notes.append(f"⏳ {side_txt(side)} Day-Trading bitim {age_h:.0f} soatdan beri ochiq.")
            except Exception:
                trade["stale_warned"] = True

        # 1) +1.5R da haqiqiy Bezubitok (+0.30$ foyda bilan 20$ real hisobni himoyalash)
        be_trigger = round(entry + risk_pts * 1.5, 2) if is_long else round(entry - risk_pts * 1.5, 2)
        if reached(be_trigger) and not trade.get("breakeven"):
            trade["breakeven"] = True
            new_sl = round(entry + 0.30, 2) if is_long else round(entry - 0.30, 2)
            if better_sl(new_sl, trade["sl"]):
                trade["sl"] = new_sl
                notes.append(
                    f"🛡 {side_txt(side)} +1.5R foydada! SL bezubitokka (+0.30$) olindi → SL: <code>{trade['sl']}</code>"
                )

        # 2) +2.5R da foydaning +1.5R qismini qulflash (Trailing SL)
        trail_trigger = round(entry + risk_pts * 2.5, 2) if is_long else round(entry - risk_pts * 2.5, 2)
        if reached(trail_trigger) and not trade.get("tp1_notified"):
            trade["tp1_notified"] = True
            trail_sl = round(entry + risk_pts * 1.5, 2) if is_long else round(entry - risk_pts * 1.5, 2)
            if better_sl(trail_sl, trade["sl"]):
                trade["sl"] = trail_sl
                notes.append(f"🔥 {side_txt(side)} +2.5R! +1.5R foyda qulflandi → SL: <code>{trade['sl']}</code> (TP: <code>{tp_lvl}</code>)")

        # 3) Asosiy TP yoki SL tekshiruvi
        sl_hit = (is_long and price <= trade["sl"]) or ((not is_long) and price >= trade["sl"])
        tp_hit = reached(tp_lvl)

        if tp_hit:
            to_close = ("TP2", tp_lvl)
        elif sl_hit:
            sl_price = trade["sl"]
            if trade.get("tp1_notified"):
                label = "TP1 + Trailing SL"
            elif trade.get("breakeven"):
                label = "BE+"
            else:
                label = "SL"
            to_close = (label, sl_price)

        if not to_close:
            if bias15 is not None and bias15 != trade["bias"]:
                now_m = time.monotonic()
                if _flip_since is None:
                    _flip_since = now_m
                if (now_m - _flip_since >= FLIP_WARN_AFTER_SEC
                        and now_m - _flip_last_warn >= FLIP_WARN_REPEAT_SEC):
                    _flip_last_warn = now_m
                    notes.append(
                        f"⚠️ {side_txt(side)} ochiq, lekin 15m trend {trend_txt(bias15)} o'zgardi."
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
async def get_daily_signal_info():
    """Bugungi (UZT) yuborilgan signallar sonini kuzatish (7-10 ta sifatli signal balansi uchun)."""
    today_str = uzt_now().strftime("%Y-%m-%d")
    info = await db.state_get("daily_signals", {"date": today_str, "count": 0, "last_ts": 0})
    if info.get("date") != today_str:
        info = {"date": today_str, "count": 0, "last_ts": info.get("last_ts", 0)}
        await db.state_set("daily_signals", info)
    return info

async def increment_daily_signal_count():
    info = await get_daily_signal_info()
    info["count"] = int(info.get("count", 0)) + 1
    info["last_ts"] = int(time.time())
    await db.state_set("daily_signals", info)
    return info

async def analyze_gold_market(session, run_deep_scoring=True):
    global _LAST_CANDLES_15M, _LAST_STRUCT_15M, _LIVE_BYBIT_RAW_PRICE
    c1, c5, c15 = await asyncio.gather(
        fetch_ohlcv(session, "1m", limit=90),
        fetch_ohlcv(session, "5m", limit=LIMIT),
        fetch_ohlcv(session, "15m", limit=LIMIT),
        return_exceptions=True,
    )
    if isinstance(c5, Exception) or isinstance(c15, Exception):
        raise RuntimeError(f"OHLCV xato: {c5 if isinstance(c5, Exception) else c15}")
    cl1 = closed_only(c1) if not isinstance(c1, Exception) else []
    htf = await fetch_htf_context(session)
    b1h, b4h, b1d = htf["b1h"], htf["b4h"], htf["b1d"]
    b1w, b1m = htf.get("b1w"), htf.get("b1m")
    pdh, pdl = htf["pdh"], htf["pdl"]
    pwh, pwl = htf.get("pwh"), htf.get("pwl")
    pmh, pml = htf.get("pmh"), htf.get("pml")
    month_high, month_low, month_eq = htf.get("month_high"), htf.get("month_low"), htf.get("month_eq")
    htf_zones = htf.get("htf_zones") or []

    cl5 = closed_only(c5)
    cl15 = closed_only(c15)
    _LAST_CANDLES_15M = cl15

    global _LIVE_PRICE, _LIVE_PRICE_TS
    rest_xm_price = round(c15[-1][C], 2)
    if _LIVE_BYBIT_RAW_PRICE is None:
        _LIVE_BYBIT_RAW_PRICE = round(rest_xm_price - _CURRENT_OFFSET, 2)
    if live_price() is None:
        _LIVE_PRICE = rest_xm_price
        _LIVE_PRICE_TS = time.monotonic()
    price = get_unified_price(c15)

    struct5 = analyze_market_structure(cl5, cache_key="XAU_5m")
    struct15 = analyze_market_structure(cl15, cache_key="XAU_15m")
    _LAST_STRUCT_15M = struct15

    b5 = struct5["bias"]
    b15 = struct15["bias"]
    sweeps = detect_asia_and_pd_sweeps(cl15, pdh, pdl, pwh=pwh, pwl=pwl, pmh=pmh, pml=pml)
    sweep15 = detect_liquidity_sweep(cl15, cache_key="XAU_15m")
    sweep5 = detect_liquidity_sweep(cl5, cache_key="XAU_5m")
    eqh, eql = detect_equal_highs_lows(cl15, cache_key="XAU_15m")
    atr15 = calculate_atr(cl15)
    vwap15 = calculate_vwap(cl15)
    zones15 = get_active_zones(cl15, cache_key="XAU_15m")
    news_blocked, news_title = await check_news_window(session)
    session_name = current_session()
    ict_windows = check_ict_silver_bullet_and_macro()

    acc = await get_account()
    candidate = None
    confirmations = []
    score = 0

    if run_deep_scoring:
        # 3-MODEL TOP-TRADER SETUP SELECTION:
        # 1) Model 1: MTF Trend Sniper (15m bias yo'nalishida)
        # 2) Model 2: ICT Turtle Soup / Liquidity Sweep Reversal (PDH/PDL, Asia H/L yoki 15m Sweep + 5m CHoCH/MSS)
        # 3) Model 3: ICT Silver Bullet / 5m IFVG Intraday Scalp (5m bias + 1H/OrderFlow uyg'unligi)
        setup_trials = []
        # 24 soatlik (96 sham) VA 6 soatlik (24 sham) diapazonga nisbatan narx qayerdaligi:
        # Bu 4145 dan 4117 ga qulaganda 6 soatlik tubda (4117.86 da) kechikib SELL ochishni 100% bloklaydi!
        win_96 = cl15[-RANGE_LOOKBACK_15M:] if len(cl15) >= 30 else cl15
        ll_96 = min(c[L] for c in win_96)
        hh_96 = max(c[H] for c in win_96)
        pos_96 = (price - ll_96) / max(hh_96 - ll_96, 1.0)

        win_24 = cl15[-24:] if len(cl15) >= 24 else cl15
        ll_24 = min(c[L] for c in win_24)
        hh_24 = max(c[H] for c in win_24)
        pos_24 = (price - ll_24) / max(hh_24 - ll_24, 1.0)

        has_bull_liq_raid = bool(sweeps.get("bull_sweep") or sweep15 == "bullish_sweep" or sweep5 == "bullish_sweep")
        has_bear_liq_raid = bool(sweeps.get("bear_sweep") or sweep15 == "bearish_sweep" or sweep5 == "bearish_sweep")

        delta15_pre = order_flow_delta(cl15, 12)
        delta5_pre = order_flow_delta(cl5, 6)
        eq15_val = struct15.get("eq")

        # 1) ENG PASTIDAN (TUBIDAN) SNIPER BUY:
        # Narx so'nggi 6 soatlik diapazonning pastki 35% qismida (pos_24 <= 0.35) va EQ dan pastda bo'lishi shart!
        if pos_24 <= 0.35 and pos_96 <= 0.55 and (not eq15_val or price <= eq15_val):
            if has_bull_liq_raid and (b5 == "bullish" or delta5_pre > 0):
                setup_trials.append(("bullish", "Sniper Bottom Entry (BUY)"))

        # 2) ENG UCHIDAN (CHO'QQISIDAN) SNIPER SELL:
        # Narx so'nggi 6 soatlik diapazonning yuqori 65% qismida (pos_24 >= 0.65) va EQ dan tepada bo'lishi shart!
        if pos_24 >= 0.65 and pos_96 >= 0.45 and (not eq15_val or price >= eq15_val):
            if has_bear_liq_raid and (b5 == "bearish" or delta5_pre < 0):
                setup_trials.append(("bearish", "Sniper Top Entry (SELL)"))

        for trial_bias, model_name in setup_trials:
            cand_try = build_sniper_trade(
                cl15, cl5, struct15, price, atr15, acc["balance"],
                target_bias=trial_bias, setup_model=model_name,
            )
            if not cand_try:
                continue
            ltf_triggers = check_5m_ltf_trigger(
                cl5, struct5, trial_bias,
                zone={"top": cand_try["zone_top"], "bottom": cand_try["zone_bottom"]},
                cl1=cl1,
            )
            if ltf_triggers:
                candidate = cand_try
                break

        if candidate:
            active_bias = candidate["bias"]
            eval_struct = dict(struct15)
            eval_struct["bias"] = active_bias

            rsis15 = calculate_rsi(cl15)
            bull_div, bear_div = detect_rsi_divergence(cl15, rsis15)
            delta15 = order_flow_delta(cl15)
            delta5 = order_flow_delta(cl5)
            vp15 = volume_profile(cl15)

            btc_cl, eur_cl = await asyncio.gather(
                _fetch_corr(session, BTC_SYMBOL, "BTC"),
                _fetch_corr(session, EUR_SYMBOL, "EUR"),
            )
            btc_bias = confirmed_structure_bias(btc_cl, cache_key="BTC_15m") if btc_cl else None
            eur_bias = confirmed_structure_bias(eur_cl, cache_key="EUR_15m") if eur_cl else None
            smt_btc = detect_smt_divergence(cl15, btc_cl, "BTC_15m") if btc_cl else None
            smt_eur = detect_smt_divergence(cl15, eur_cl, "EUR_15m") if eur_cl else None

            mtf = mtf_alignment_score(b5, active_bias, b1h, b4h)
            score, grade, confirmations = evaluate_institutional_score(
                eval_struct, price, ltf_triggers, mtf, vwap15, bull_div, bear_div,
                btc_bias, eur_bias, smt_eur, smt_btc, candidate["confluence"],
                sweep15 or sweep5, sweeps, eqh, eql, delta15, vp15, session_name
            )

            confirmations.insert(0, candidate.get("setup_model", "SMC Sniper"))

            # ICT Silver Bullet & 20m Macro Window bonuslari
            if ict_windows["silver_bullet"]:
                score += 8
                confirmations.append(f"{ict_windows['silver_bullet']} (+8)")
            elif ict_windows["macro"]:
                score += 5
                confirmations.append(f"{ict_windows['macro']} (+5)")

            # 5m Order Flow Delta tasdig'i
            if (active_bias == "bullish" and delta5 > 0) or (active_bias == "bearish" and delta5 < 0):
                score += 4
                confirmations.append(f"5m OrderFlow Impulse ({delta5:+.1f} +4)")

            macro = await get_macro_dxy_context(session, cl15)
            if active_bias == "bullish":
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

            # IFVG, FU Candle, Algo Candle va IDM uchun qo'shimcha ball
            if candidate.get("zone_kind") in ("IFVG", "FU", "AC_OB") or candidate.get("algo_candle"):
                score += 8
                confirmations.append(f"ICT {candidate['zone_kind']} (Algo/Inversion +8)")
            if candidate.get("has_idm"):
                score += 6
                confirmations.append("IDM (Inducement Swept +6)")

            # NY Midnight Open (Judas Swing) filtri
            d_open = sweeps.get("daily_open_ny")
            if d_open:
                if active_bias == "bullish" and price < d_open:
                    score += 5
                    confirmations.append(f"Below NY 00:00 Open ({d_open} +5)")
                elif active_bias == "bearish" and price > d_open:
                    score += 5
                    confirmations.append(f"Above NY 00:00 Open ({d_open} +5)")

            # 1 Oylik (30 kunlik) Dealing Range & 1W/1M Trend tahlili
            if month_eq:
                if active_bias == "bullish" and price <= month_eq:
                    score += 5
                    confirmations.append(f"1M Discount (<{month_eq} +5)")
                elif active_bias == "bearish" and price >= month_eq:
                    score += 5
                    confirmations.append(f"1M Premium (>{month_eq} +5)")
            if b1d == active_bias and (b1w == active_bias or b1m == active_bias):
                score += 6
                confirmations.append("1D+1W/1M Macro Trend Aligned (+6)")
            for hz in htf_zones:
                if hz.get("type") == active_bias and (hz["bottom"] - 2.5 <= price <= hz["top"] + 2.5):
                    score += 8
                    confirmations.append(f"HTF {hz.get('tf', '4H')} {hz['kind']} Zona (+8)")
                    break

            # Trading in the Zone: Anti-Tilt & Anti-Euphoria + Kunlik 7-10 Signal Pacing
            psych = await check_psychology_guard()
            if psych["note"]:
                confirmations.append(psych["note"])
            effective_risk = round(acc["risk_pct"] * psych["risk_mult"], 2)

            daily_info = await get_daily_signal_info()
            daily_cnt = daily_info.get("count", 0)
            # Kunlik limit olib tashlandi: nechta sifatli A/A+ (76+ ball) signal chiqsa hammasini beradi
            pacing_add = 0

            score = max(0, min(100, score))
            grade = "A+" if score >= 80 else ("A" if score >= 68 else "B+")

            lot, usd_risk = calc_xm_lot_and_risk(
                candidate["entry"], candidate["sl"], acc["balance"], effective_risk
            )
            candidate["min_req_score"] = MIN_SIGNAL_SCORE + psych["score_add"] + pacing_add
            candidate["daily_count"] = daily_cnt + 1
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
        "b1w": b1w, "b1m": b1m,
        "pdh": pdh, "pdl": pdl, "pwh": pwh, "pwl": pwl, "pmh": pmh, "pml": pml,
        "month_high": month_high, "month_low": month_low, "month_eq": month_eq,
        "htf_zones": htf_zones,
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
    sl_pts = abs(trade["entry"] - trade["sl"])
    tp_pts = abs(trade["tp1"] - trade["entry"])
    tp_usd = round((trade["lot"] / XM_MIN_LOT) * tp_pts * XM_OZ_PER_MIN_LOT, 2)
    lines = [
        f"{side_icon(trade['signal'])} <b>[DAY TRADING 15m] GOLD {side_txt(trade['signal'])}</b> (#{trade.get('daily_count', 1)})",
        f"Kirish: <code>{trade['entry']}</code>",
        f"SL: <code>{trade['sl']}</code> (-{trade['risk_usd']:.2f}$)",
        f"TP: <code>{trade['tp1']}</code> (+{tp_usd:.2f}$) | RR 1:{abs(trade['tp1'] - trade['entry']) / max(abs(trade['entry'] - trade['sl']), 0.01):.1f}",
        f"Lot: <code>{trade['lot']}</code> | Balans: ${acc['balance']:.2f}",
    ]
    return "\n".join(lines)

async def format_status_message(status):
    acc = await get_account()
    trade = await db.state_get("current_trade")
    ake_trade = await db.state_get("current_trade_ake")
    kz_txt = "faol" if status["in_killzone"] else "tinch"
    news_txt = f"⛔ {html.escape(status['news_title'])}" if status["news_blocked"] else "yo'q"
    brk = _ACTIVE_BROKER.upper()
    sync_st = "🟢 Avto-Sync" if _AUTO_SYNC_ENABLED else "⚪ Qo'lda"
    ex_p = _LAST_SPOT_SYNC_INFO.get("exness_price") or status["price"]
    xm_p = _LAST_SPOT_SYNC_INFO.get("xm_price") or round(status["price"] + 0.25, 2)

    lines = [
        f"📊 <b>GOLD ({brk}): <code>{status['price']:.2f}</code></b> ({sync_st} | Offset: <code>{_CURRENT_OFFSET:+.2f}</code>)",
        f"🏦 Exness: <code>{ex_p:.2f}</code> | 🏛 XM: <code>{xm_p:.2f}</code> | 📈 Bybit: <code>{(status.get('raw_bybit') or 0):.2f}</code>",
        f"Trend (LTF/MTF): 5m {trend_txt(status['b5'])} | 15m {trend_txt(status['b15'])} | 1H {trend_txt(status['b1h'])} | 4H {trend_txt(status['b4h'])}",
        f"Macro (1 Oy): 1D {trend_txt(status.get('b1d'))} | 1W {trend_txt(status.get('b1w'))} | 1M {trend_txt(status.get('b1m'))}",
        f"1 Oylik Diapazon: <code>{status.get('month_low')} — {status.get('month_high')}</code> (EQ 50%: <code>{status.get('month_eq')}</code>)",
        f"Likvidlik: PDH/L <code>{status.get('pdh')}/{status.get('pdl')}</code> | PWH/L <code>{status.get('pwh')}/{status.get('pwl')}</code>",
        f"Sessiya: {status['session'].upper()} ({kz_txt}) | Yangilik: {news_txt}",
        f"💰 <b>Umumiy Balans (Day + Ake): ${acc['balance']:.2f}</b>",
    ]

    if trade:
        lot = trade.get("lot", XM_MIN_LOT)
        pnl_usd, _ = calc_trade_pnl(trade, status["price"], lot)
        lines.extend([
            "",
            f"{side_icon(trade['signal'])} <b>[Day Trading 15m] Ochiq: {side_txt(trade['signal'])}</b> @ <code>{trade['entry']}</code>",
            f"SL: <code>{trade['sl']}</code> | TP: <code>{trade['tp1']}</code> | Hozir: <b>{pnl_usd:+.2f}$</b>",
        ])
    else:
        lines.append("📭 Day Trading (15m): ochiq bitim yo'q")

    if ake_trade:
        lines.extend([
            "",
            f"⚡ <b>[Ake Signals 7 · 1m] Ochiq: {side_txt(ake_trade['signal'])}</b> @ <code>{ake_trade['entry']}</code> (Max: {ake_trade.get('max_r_hit', 0)}R)",
            f"SL: <code>{ake_trade['sl']}</code> | TP1: <code>{ake_trade['tp1']}</code> .. TP5: <code>{ake_trade['tp5']}</code>",
        ])
    return "\n".join(lines)


# ==================== AKE SIGNALS 7 (1M SCALPING 1R–5R) ENGINE ====================
def _calc_ema_series(closes, period):
    if not closes:
        return []
    k = 2.0 / (period + 1.0)
    ema = [closes[0]]
    for p in closes[1:]:
        ema.append(p * k + ema[-1] * (1.0 - k))
    return ema

def compute_ake_Asia_1800_0200(cl1):
    """Skrinshotdagi 'AsiaSessionHi... 1800-0200' darajalarini hisoblash."""
    if not cl1:
        return None, None
    asia_bars = []
    for c in cl1[-240:]:
        dt = datetime.datetime.fromtimestamp(c[0] / 1000.0, tz=datetime.timezone.utc)
        if dt.hour >= AKE_ASIA_START_HOUR_UTC or dt.hour < AKE_ASIA_END_HOUR_UTC:
            asia_bars.append(c)
    if len(asia_bars) >= 5:
        return round(max(c[H] for c in asia_bars), 2), round(min(c[L] for c in asia_bars), 2)
    return None, None

def _compute_supertrend_1m(cl1, period=10, mult=2.2):
    """TradingView Ake signals 7 uchun 1m ATR Supertrend (Scalp Trend) liniyasi va trend almashishi."""
    n = len(cl1)
    if n < period + 5:
        return None, None, False, False

    trs = [cl1[0][H] - cl1[0][L]]
    for i in range(1, n):
        trs.append(max(
            cl1[i][H] - cl1[i][L],
            abs(cl1[i][H] - cl1[i - 1][C]),
            abs(cl1[i][L] - cl1[i - 1][C]),
        ))

    upper_band = [0.0] * n
    lower_band = [0.0] * n
    trend = [1] * n

    for i in range(period, n):
        atr = sum(trs[i - period + 1:i + 1]) / period
        hl2 = (cl1[i][H] + cl1[i][L]) / 2.0
        basic_upper = hl2 + mult * atr
        basic_lower = hl2 - mult * atr

        if i == period:
            upper_band[i] = basic_upper
            lower_band[i] = basic_lower
            trend[i] = 1 if cl1[i][C] >= hl2 else -1
            continue

        if basic_upper < upper_band[i - 1] or cl1[i - 1][C] > upper_band[i - 1]:
            upper_band[i] = basic_upper
        else:
            upper_band[i] = upper_band[i - 1]

        if basic_lower > lower_band[i - 1] or cl1[i - 1][C] < lower_band[i - 1]:
            lower_band[i] = basic_lower
        else:
            lower_band[i] = lower_band[i - 1]

        if trend[i - 1] == -1 and cl1[i][C] > upper_band[i - 1]:
            trend[i] = 1
        elif trend[i - 1] == 1 and cl1[i][C] < lower_band[i - 1]:
            trend[i] = -1
        else:
            trend[i] = trend[i - 1]

    flipped_bull = (trend[-1] == 1 and trend[-2] == -1)
    flipped_bear = (trend[-1] == -1 and trend[-2] == 1)
    return trend[-1], (round(lower_band[-1], 2), round(upper_band[-1], 2)), flipped_bull, flipped_bear

def analyze_ake_1m_scalp(cl1, cl5, price, last_closed_ake=None):
    """Ake signals 7 (1m XAUUSD Scalping) — Yuqori aniqlikdagi 2-bosqichli tasdiq (Two-Step Sniper) motori:
    1) Pichoqni havoda ushlamaslik (Anti-Knife-Catch):
       - BUY uchun: avval 1m da pastki ekstremum (Liquidity Sweep / Pivot Low) shakllanib, so'ngra sham oldingi
         shamning High va EMA9 darajasini yorib yuqoriga yopilishi (1m Micro-CHoCH) talab qilinadi.
       - SELL uchun: avval 1m da yuqori ekstremum (Liquidity Sweep / Pivot High, masalan 4128.78) shakllanib,
         keyin sham oldingi shamning Low va EMA9 darajasini buzib pastga yopilishi (masalan 4126.68) talab qilinadi.
    2) Bir to'lqinda 2 marta yomon narxda kirmaslik (Anti-Exhaustion):
       - SELL foydada yopilgach (masalan 4126.68 -> 4122.88), narx tepaga (kamida EMA21 ustiga) qaytmasdan turib,
         pastdan (4122.09) yana SELL ochish qat'iyan bloklanadi.
    3) SL (-1R) = 1.60$..2.85$ (agar sham juda cho'zilgan va SL > 3.20$ bo'lsa, signal o'tkazib yuboriladi).
    """
    if not cl1 or len(cl1) < 55:
        return {"candidate": None, "scalp_band": None, "trend_1m": None}

    closes = [c[C] for c in cl1]
    ema9_list = _calc_ema_series(closes, 9)
    ema21_list = _calc_ema_series(closes, 21)
    ema50_list = _calc_ema_series(closes, 50)
    ema9, ema21, ema50 = ema9_list[-1], ema21_list[-1], ema50_list[-1]
    atr1 = calculate_atr(cl1, 14) or 1.80
    rsis1 = calculate_rsi(cl1, 14)
    cur_rsi = rsis1[-1] if rsis1 and rsis1[-1] is not None else 50.0
    prev_rsi = rsis1[-2] if len(rsis1) >= 2 and rsis1[-2] is not None else 50.0

    st_dir, st_bands, st_flip_bull, st_flip_bear = _compute_supertrend_1m(cl1, period=10, mult=2.2)
    scalp_low = st_bands[0] if st_bands else round(min(ema9, ema21) - atr1 * 0.5, 2)
    scalp_high = st_bands[1] if st_bands else round(max(ema9, ema21) + atr1 * 0.5, 2)
    trend_1m = "bullish" if (st_dir == 1 or ema9 > ema21) else "bearish"

    b_htf = confirmed_structure_bias(cl5, cache_key="XAU_15m_ref") if (cl5 and len(cl5) >= 20) else None

    # Oxirgi 15-20 ta sham ekstremumlari (joriy va oldingi 2 ta shamdan tashqari)
    lookback_18 = cl1[-20:-2]
    swing_high_18 = max(c[H] for c in lookback_18)
    swing_low_18 = min(c[L] for c in lookback_18)

    # 30 daqiqalik diapazon pozitsiyasi (cho'qqida BUY / tubda SELL ochmaslik uchun)
    win_30 = cl1[-30:]
    hh_30 = max(c[H] for c in win_30)
    ll_30 = min(c[L] for c in win_30)
    pos_30 = (price - ll_30) / max(hh_30 - ll_30, 0.5)
    asia_h, asia_l = compute_ake_Asia_1800_0200(cl1)

    last = cl1[-1]
    prev = cl1[-2]
    prev2 = cl1[-3]
    prev3 = cl1[-4]
    rng = max(last[H] - last[L], 0.01)
    body = abs(last[C] - last[O])
    avg_body_10 = sum(abs(c[C] - c[O]) for c in cl1[-12:-2]) / 10.0

    is_strong_bull_candle = (
        last[C] > last[O]
        and body >= max(avg_body_10 * 1.15, atr1 * 0.40, 0.40)
        and (last[H] - last[C]) <= body * 0.55
    )
    is_strong_bear_candle = (
        last[C] < last[O]
        and body >= max(avg_body_10 * 1.15, atr1 * 0.40, 0.40)
        and (last[C] - last[L]) <= body * 0.55
    )

    bull_fvg_1m = (last[L] > prev2[H]) and (prev[C] > prev[O])
    bear_fvg_1m = (last[H] < prev2[L]) and (prev[C] < prev[O])

    # 1m va 15m SMC/ICT Zonalarni (OB, AC_OB, FVG, IFVG, FU, BB) aniqlash (/zones bilan bir xil mantiq)
    zones_1m = get_active_zones(cl1, cache_key="XAU_1m")
    zones_htf = get_active_zones(cl5, cache_key="XAU_15m") if (cl5 and len(cl5) >= 20) else []
    all_ake_zones = []
    for z in zones_1m:
        zc = dict(z)
        zc["tf"] = "1m"
        all_ake_zones.append(zc)
    for z in zones_htf:
        zc = dict(z)
        zc["tf"] = "15m"
        all_ake_zones.append(zc)

    bull_zones = [z for z in all_ake_zones if z["type"] == "bullish"]
    bear_zones = [z for z in all_ake_zones if z["type"] == "bearish"]
    bull_zones.sort(key=lambda z: abs(price - z["ce"]))
    bear_zones.sort(key=lambda z: abs(price - z["ce"]))

    # Narx joriy shamda yoki oldingi shamda BUY yoki SELL zonasiga (OB/FVG/IFVG/FU/BB) tegdimi?
    zone_tol_1m = max(atr1 * 0.55, 0.85)
    active_bull_zone = next(
        (z for z in bull_zones if (z["bottom"] - zone_tol_1m) <= min(last[L], prev[L], price) <= (z["top"] + zone_tol_1m)),
        bull_zones[0] if bull_zones else None,
    )
    active_bear_zone = next(
        (z for z in bear_zones if (z["bottom"] - zone_tol_1m) <= max(last[H], prev[H], price) <= (z["top"] + zone_tol_1m)),
        bear_zones[0] if bear_zones else None,
    )
    touched_bull_zone = bool(
        active_bull_zone and (active_bull_zone["bottom"] - zone_tol_1m) <= min(last[L], prev[L], price) <= (active_bull_zone["top"] + zone_tol_1m)
    )
    touched_bear_zone = bool(
        active_bear_zone and (active_bear_zone["bottom"] - zone_tol_1m) <= max(last[H], prev[H], price) <= (active_bear_zone["top"] + zone_tol_1m)
    )

    # 2-BOSQICHLI REVERSAL TASDIG'I (Pichoqni havoda ushlamaslik uchun!):
    # BUY uchun: oxirgi 3 ta sham ichida likvidlik supurilgan (sweep_low < swing_low_18) VA
    # joriy sham yangi Low qilmasdan (yoki pin-bar bilan) oldingi shamning High hamda EMA9 ustida yopilishi shart!
    recent_3_low = min(last[L], prev[L], prev2[L])
    recent_3_high = max(last[H], prev[H], prev2[H])

    swept_low_recently = (
        recent_3_low < swing_low_18
        or (asia_l is not None and recent_3_low < asia_l)
    )
    swept_high_recently = (
        recent_3_high > swing_high_18
        or (asia_h is not None and recent_3_high > asia_h)
    )

    # 1) LONG (Buy) Ake Signal:
    # A) Confirmed Liquidity Grab + Micro-CHoCH (pastki likvidlik olingach, 2-sham tasdig'i yoki kuchli Pin/Engulfing prev High/Body va EMA9 ustida yopilsa)
    bull_sweep_reversal = (
        is_strong_bull_candle
        and swept_low_recently
        and last[C] > swing_low_18
        and last[C] > max(prev[O], prev[C])
        and (
            (prev[L] < swing_low_18 and last[L] >= prev[L] - 0.10)
            or (last[L] < swing_low_18 and (min(last[O], last[C]) - last[L]) >= body * 0.45)
        )
        and last[C] >= ema9
        and ((min(last[O], prev[C]) - ll_30) / max(hh_30 - ll_30, 1.0)) <= 0.55
        and prev_rsi <= 58.0 and cur_rsi <= 68.0
    )
    # B) Confirmed Supertrend Flip + Pullback/Breakout (faqat o'rta/pastki diapazondan, cho'qqida >72% kirmaydi!)
    bull_trend_breakout = (
        is_strong_bull_candle
        and st_dir == 1
        and last[C] > ema9 >= ema21
        and pos_30 <= 0.72
        and b_htf != "bearish"
        and (st_flip_bull or (last[C] > swing_high_18 and prev[C] <= swing_high_18 and bull_fvg_1m))
        and 48.0 <= cur_rsi <= 65.0
    )

    # 2) SHORT (Sell) Ake Signal:
    # A) Confirmed Liquidity Grab + Micro-CHoCH (yuqori likvidlik olingach, masalan 4128.78 dan keyin 4126.68 da pastga yorib yopilsa)
    bear_sweep_reversal = (
        is_strong_bear_candle
        and swept_high_recently
        and last[C] < swing_high_18
        and last[C] < min(prev[O], prev[C])
        and (
            (prev[H] > swing_high_18 and last[H] <= prev[H] + 0.10)
            or (last[H] > swing_high_18 and (last[H] - max(last[O], last[C])) >= body * 0.45)
        )
        and last[C] <= ema9
        and ((max(last[O], prev[C]) - ll_30) / max(hh_30 - ll_30, 1.0)) >= 0.45
        and prev_rsi >= 42.0 and cur_rsi >= 32.0
    )
    # B) Confirmed Supertrend Flip + Breakdown (faqat o'rta/yuqori diapazondan, tubda <28% SELL kirmaydi!)
    bear_trend_breakout = (
        is_strong_bear_candle
        and st_dir == -1
        and last[C] < ema9 <= ema21
        and pos_30 >= 0.28
        and b_htf != "bullish"
        and (st_flip_bear or (last[C] < swing_low_18 and prev[C] >= swing_low_18 and bear_fvg_1m))
        and 35.0 <= cur_rsi <= 52.0
    )

    # C) SMC/ICT Zona (OB / FVG / IFVG / FU / BB) dan aniq impuls bilan qaytish
    bull_zone_bounce = (
        touched_bull_zone
        and is_strong_bull_candle
        and last[C] > max(prev[O], prev[C])
        and last[C] >= ema9
        and pos_30 <= 0.60
        and prev_rsi <= 56.0 and cur_rsi <= 66.0
    )
    bear_zone_bounce = (
        touched_bear_zone
        and is_strong_bear_candle
        and last[C] < min(prev[O], prev[C])
        and last[C] <= ema9
        and pos_30 >= 0.40
        and prev_rsi >= 44.0 and cur_rsi >= 34.0
    )

    sig_side = None
    sig_reason = []
    matched_zone = None
    if bull_sweep_reversal or bull_trend_breakout or bull_zone_bounce:
        sig_side = "LONG"
        matched_zone = active_bull_zone
        if bull_sweep_reversal:
            sig_reason.append("Likvidlik supurildi + Qayrilish")
        if bull_trend_breakout:
            sig_reason.append("Supertrend BOS impuls")
        if touched_bull_zone and active_bull_zone:
            sig_reason.append(f"{active_bull_zone.get('tf', '1m')} {active_bull_zone['kind']} zonadan")
    elif bear_sweep_reversal or bear_trend_breakout or bear_zone_bounce:
        sig_side = "SHORT"
        matched_zone = active_bear_zone
        if bear_sweep_reversal:
            sig_reason.append("Likvidlik supurildi + Qayrilish")
        if bear_trend_breakout:
            sig_reason.append("Supertrend BOS impuls")
        if touched_bear_zone and active_bear_zone:
            sig_reason.append(f"{active_bear_zone.get('tf', '1m')} {active_bear_zone['kind']} zonadan")

    # 3) ANTI-EXHAUSTION VA TAKRORIY KIRISH FILTRI:
    # Agar oxirgi yopilgan Ake bitimi shu yo'nalishda bo'lsa va narx hali pullback qilmasdan
    # undan yomonroq (cho'zilgan) narxda turgan bo'lsa (masalan SELL @ 4126.68 yopilgach SELL @ 4122.09), BLOKLANADI!
    if sig_side and last_closed_ake:
        last_side = last_closed_ake.get("signal")
        last_entry = float(last_closed_ake.get("entry") or 0.0)
        last_max_r = int(last_closed_ake.get("max_r_hit") or 0)
        if last_side == sig_side and last_entry > 0:
            if sig_side == "SHORT" and last_max_r >= 1 and price < last_entry - 1.20 and pos_30 < 0.55:
                sig_side = None
            elif sig_side == "LONG" and last_max_r >= 1 and price > last_entry + 1.20 and pos_30 > 0.45:
                sig_side = None

    candidate = None
    if sig_side:
        entry = round(price, 2)
        recent_low_4 = min(c[L] for c in cl1[-4:])
        recent_high_4 = max(c[H] for c in cl1[-4:])
        atr_risk = round(atr1 * AKE_ATR_SL_MULT, 2)

        if sig_side == "LONG":
            structural_dist = entry - (recent_low_4 - 0.18)
            raw_sl_dist = max(structural_dist, atr_risk)
        else:
            structural_dist = (recent_high_4 + 0.18) - entry
            raw_sl_dist = max(structural_dist, atr_risk)

        # Agar 1m strukturaviy SL juda katta (> 3.20$) bo'lsa, xavfli cho'zilgan shamga kirmaymiz!
        if raw_sl_dist <= AKE_SKIP_IF_SL_OVER:
            risk_1r = round(min(max(raw_sl_dist, AKE_MIN_SL_POINTS), AKE_MAX_SL_POINTS), 2)
            if sig_side == "LONG":
                sl = round(entry - risk_1r, 2)
                tp1 = round(entry + risk_1r * 1.0, 2)
                tp2 = round(entry + risk_1r * 2.0, 2)
                tp3 = round(entry + risk_1r * 3.0, 2)
                tp4 = round(entry + risk_1r * 4.0, 2)
                tp5 = round(entry + risk_1r * 5.0, 2)
            else:
                sl = round(entry + risk_1r, 2)
                tp1 = round(entry - risk_1r * 1.0, 2)
                tp2 = round(entry - risk_1r * 2.0, 2)
                tp3 = round(entry - risk_1r * 3.0, 2)
                tp4 = round(entry - risk_1r * 4.0, 2)
                tp5 = round(entry - risk_1r * 5.0, 2)

            candidate = {
                "engine": "AKE_SIGNALS_7",
                "signal": sig_side,
                "bias": "bullish" if sig_side == "LONG" else "bearish",
                "entry": entry,
                "sl": sl,
                "initial_sl": sl,
                "risk_pts": risk_1r,
                "tp1": tp1,
                "tp2": tp2,
                "tp3": tp3,
                "tp4": tp4,
                "tp5": tp5,
                "max_r_hit": 0,
                "zone_tf": matched_zone.get("tf", "1m") if matched_zone else "1m",
                "zone_kind": matched_zone.get("kind", "Scalp") if matched_zone else "Scalp",
                "zone_bottom": round(matched_zone["bottom"], 2) if matched_zone else round(scalp_low, 2),
                "zone_top": round(matched_zone["top"], 2) if matched_zone else round(scalp_high, 2),
                "zone_ce": round(matched_zone["ce"], 2) if matched_zone else round((scalp_low + scalp_high) / 2.0, 2),
                "lot": XM_MIN_LOT,
                "risk_usd": round(risk_1r * XM_OZ_PER_MIN_LOT, 2),
                "reason": " + ".join(sig_reason),
                "bar_ts": last[0],
                "opened_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            }

    return {
        "candidate": candidate,
        "scalp_band": (scalp_low, scalp_high),
        "trend_1m": trend_1m,
        "ema9": round(ema9, 2),
        "ema21": round(ema21, 2),
        "atr1": round(atr1, 2),
        "asia_high": asia_h,
        "asia_low": asia_l,
        "zones_1m": zones_1m,
        "bull_zones": bull_zones[:5],
        "bear_zones": bear_zones[:5],
    }

def _compute_ake_window_metrics(rows):
    total = len(rows)
    if total == 0:
        return {"total": 0, "wins": 0, "losses": 0, "wr": 0, "total_r": 0.0, "avg_r": 0.0, "rule_r": 0.0, "rule_avg_r": 0.0}
    wins = sum(1 for r in rows if int(r.get("max_r_hit", 0)) >= 1)
    losses = total - wins
    wr = int(round((wins / total) * 100))
    total_r = round(sum(float(r.get("peak_r", 0.0)) for r in rows), 1)
    avg_r = round(total_r / total, 1)
    rule_r = round(sum(float(r.get("rule_r", 0.0)) for r in rows), 1)
    rule_avg_r = round(rule_r / total, 1)
    return {
        "total": total,
        "wins": wins,
        "losses": losses,
        "wr": wr,
        "total_r": total_r,
        "avg_r": avg_r,
        "rule_r": rule_r,
        "rule_avg_r": rule_avg_r,
    }

async def get_ake_stats_summary():
    """TradingView 'Ake signals 7 · 1' jadvali uchun День / Неделя / Месяц ko'rsatkichlari."""
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    uzt_cur = now_utc + UZT_OFFSET
    start_of_day_uzt = datetime.datetime(uzt_cur.year, uzt_cur.month, uzt_cur.day, tzinfo=datetime.timezone.utc) - UZT_OFFSET
    day_iso = start_of_day_uzt.isoformat()
    week_iso = (now_utc - datetime.timedelta(days=7)).isoformat()
    month_iso = (now_utc - datetime.timedelta(days=30)).isoformat()

    day_rows = await db.get_ake_trades_since(day_iso)
    week_rows = await db.get_ake_trades_since(week_iso)
    month_rows = await db.get_ake_trades_since(month_iso)

    return {
        "day": _compute_ake_window_metrics(day_rows),
        "week": _compute_ake_window_metrics(week_rows),
        "month": _compute_ake_window_metrics(month_rows),
    }

async def format_ake_stats_table_message():
    st = await get_ake_stats_summary()
    d, w, m = st["day"], st["week"], st["month"]
    ake_open = await db.state_get("current_trade_ake")
    in_work = 1 if ake_open else 0
    d_tp = f"{d['wins']} ({d['wr']}%)"
    w_tp = f"{w['wins']} ({w['wr']}%)"
    m_tp = f"{m['wins']} ({m['wr']}%)"
    d_tot = f"{d['total_r']:+.1f}R"
    w_tot = f"{w['total_r']:+.1f}R"
    m_tot = f"{m['total_r']:+.1f}R"
    d_avg = f"{d['avg_r']:+.1f}R"
    w_avg = f"{w['avg_r']:+.1f}R"
    m_avg = f"{m['avg_r']:+.1f}R"
    d_rul = f"{d['rule_r']:+.1f}R ({d['rule_avg_r']:+.1f})"
    w_rul = f"{w['rule_r']:+.1f}R ({w['rule_avg_r']:+.1f})"
    m_rul = f"{m['rule_r']:+.1f}R ({m['rule_avg_r']:+.1f})"
    lines = [
        "⚡ <b>Ake signals 7 · 1m (O'zbekcha Statistika Jadvali)</b>",
        "<pre>",
        f"{'Ko`rsatkich':<15} | {'Kun':<13} | {'Hafta':<13} | {'Oy':<13}",
        "-" * 61,
        f"{'Bitimlar':<15} | {d['total']:<13} | {w['total']:<13} | {m['total']:<13}",
        f"{'TP olindi':<15} | {d_tp:<13} | {w_tp:<13} | {m_tp:<13}",
        f"{'Stop -1R':<15} | {d['losses']:<13} | {w['losses']:<13} | {m['losses']:<13}",
        f"{'Jami R':<15} | {d_tot:<13} | {w_tot:<13} | {m_tot:<13}",
        f"{'O`rtacha R':<15} | {d_avg:<13} | {w_avg:<13} | {m_avg:<13}",
        f"{'Qoida bo`yicha':<15} | {d_rul:<13} | {w_rul:<13} | {m_rul:<13}",
        "</pre>",
        f"<i>Yopilgan bitimlar · TP = erishilgan eng yuqori TP · TP1 gacha SL = -1R · faol bitim: {in_work}</i>",
    ]
    return "\n".join(lines)

def format_ake_zones_block(ake_info, p_now):
    bull_z = ake_info.get("bull_zones") or []
    bear_z = ake_info.get("bear_zones") or []
    sb = ake_info.get("scalp_band") or (0.0, 0.0)
    lines = [
        f"🎯 <b>Ake signals 7 (1m/15m) Zonalar (Narx: <code>{p_now:.2f}</code>):</b>",
        f"📏 Skalp Kanal: <code>{sb[0]:.2f} — {sb[1]:.2f}</code> | Osiyo H/L: <code>{ake_info.get('asia_high')} / {ake_info.get('asia_low')}</code>",
        "🟢 <b>Ake BUY zonalari (pastda):</b>",
    ]
    if bull_z:
        for z in bull_z[:4]:
            dist = p_now - z["ce"]
            lines.append(
                f" • <b>{z.get('tf', '1m')} {z['kind']}</b>: <code>{z['bottom']:.2f} — {z['top']:.2f}</code> "
                f"(CE: <code>{z['ce']:.2f}</code> | {dist:+.1f}$)"
            )
    else:
        lines.append(" • Hozircha yaqin 1m BUY zona yo'q")

    lines.append("🔴 <b>Ake SELL zonalari (tepada):</b>")
    if bear_z:
        for z in bear_z[:4]:
            dist = z["ce"] - p_now
            lines.append(
                f" • <b>{z.get('tf', '1m')} {z['kind']}</b>: <code>{z['bottom']:.2f} — {z['top']:.2f}</code> "
                f"(CE: <code>{z['ce']:.2f}</code> | +{dist:.1f}$)"
            )
    else:
        lines.append(" • Hozircha yaqin 1m SELL zona yo'q")
    return "\n".join(lines)

def format_ake_signal_message(trade, balance=None):
    is_long = trade["signal"] == "LONG"
    tag = "🟢 BUY" if is_long else "🔴 SELL"
    max_hit = int(trade.get("max_r_hit", 0))
    lot = float(trade.get("lot") or XM_MIN_LOT)
    r_usd = round((lot / XM_MIN_LOT) * float(trade["risk_pts"]) * XM_OZ_PER_MIN_LOT, 2)
    brk = _ACTIVE_BROKER.upper()
    bal_txt = f" | 💰 Balans: <b>${float(balance):.2f}</b>" if balance is not None else ""
    z_kind = trade.get("zone_kind", "OB/FVG")
    z_tf = trade.get("zone_tf", "1m")
    z_bot = trade.get("zone_bottom")
    z_top = trade.get("zone_top")
    z_ce = trade.get("zone_ce")
    zone_line = (
        f"🎯 Zona ({z_tf} <b>{html.escape(str(z_kind))}</b>): <code>{z_bot:.2f} — {z_top:.2f}</code> (CE: <code>{z_ce:.2f}</code>)"
        if (z_bot is not None and z_top is not None)
        else f"🎯 Zona: <b>{html.escape(str(z_kind))}</b>"
    )
    lines = [
        f"⚡ <b>Ake signals 7 ({brk}) — {tag}</b> [1m Skalp]",
        f"Kirish (E): <code>{trade['entry']:.2f}</code> | Lot: <code>{lot:.2f}</code>{bal_txt}",
        zone_line,
        f"SL (-1R): <code>{trade.get('initial_sl', trade['sl']):.2f}</code> (Xavf: <b>-{r_usd:.2f}$</b>)",
        f"  • TP1 (1R): <code>{trade['tp1']:.2f}</code> (+{r_usd*1:.2f}$) {'✓' if max_hit >= 1 else ''}",
        f"  • TP2 (2R): <code>{trade['tp2']:.2f}</code> (+{r_usd*2:.2f}$) {'✓' if max_hit >= 2 else ''}",
        f"  • TP3 (3R): <code>{trade['tp3']:.2f}</code> (+{r_usd*3:.2f}$) {'✓' if max_hit >= 3 else ''}",
        f"  • TP4 (4R): <code>{trade['tp4']:.2f}</code> (+{r_usd*4:.2f}$) {'✓' if max_hit >= 4 else ''}",
        f"  • TP5 (5R): <code>{trade['tp5']:.2f}</code> (+{r_usd*5:.2f}$) {'✓' if max_hit >= 5 else ''}",
        f"Tasdiq: <i>{html.escape(trade.get('reason', '1m Impuls'))}</i>",
    ]
    return "\n".join(lines)

async def close_ake_trade(session, trade, price, reason_label="SL"):
    """Ake Signals 7 bitimini yopish va Kun/Hafta/Oy R-jadvaliga yozish (_ake_monitor_lock ichida chaqiriladi)."""
    max_hit = int(trade.get("max_r_hit", 0))
    entry = float(trade["entry"])
    risk_pts = max(float(trade.get("risk_pts", abs(entry - trade.get("initial_sl", trade["sl"])))), 0.01)
    is_long = trade["signal"] == "LONG"
    raw_r = ((price - entry) if is_long else (entry - price)) / risk_pts

    if max_hit >= 1:
        peak_r = float(max_hit)
        if reason_label == "TP5":
            rule_r = 5.0
        elif reason_label == "TRAIL_SL":
            rule_r = float(max_hit - 1)
        else:
            rule_r = round(raw_r, 2)
        res_code = f"TP{max_hit} (+{peak_r:.1f}R)"
        icon = "✅"
    else:
        if reason_label in ("SL", "STOP_-1R"):
            peak_r = -1.0
            rule_r = -1.0
            res_code = "Stop -1R"
            icon = "❌"
        else:
            peak_r = -1.0 if raw_r < 0 else round(raw_r, 1)
            rule_r = round(raw_r, 2)
            res_code = f"{reason_label} ({rule_r:+.1f}R)"
            icon = "🔄" if raw_r >= 0 else "❌"

    lot = trade.get("lot", XM_MIN_LOT)
    risk_usd = trade.get("risk_usd", round(risk_pts, 2))
    try:
        await db.insert_ake_trade(
            trade, res_code, round(price, 2), max_hit, peak_r, rule_r, lot, risk_usd, trade.get("reason", "")
        )
    except Exception as e:
        logger.error(f"Ake trade DB yozish xato: {_safe_log_error(e)}")

    await db.state_set("current_trade_ake", None)
    ex_mode = await db.state_get("exness_mode", EXNESS_AUTOTRADE_DEFAULT_MODE)
    if ex_mode in ("ake", "both"):
        _spawn(sync_exness_positions_action(action="close"))

    # Yagona umumiy balansni (Day Trading + Ake Signals 7) avto-yangilash!
    # Agar TP1+ olingan bo'lsa, foyda = rekord TP R bo'yicha yoki trailing SL bo'yicha hisoblanadi
    effective_r_for_bal = peak_r if max_hit >= 1 else rule_r
    pnl_usd = round((float(lot) / XM_MIN_LOT) * (effective_r_for_bal * risk_pts) * XM_OZ_PER_MIN_LOT, 2)
    new_balance = await apply_pnl_to_balance(pnl_usd)

    st = await get_ake_stats_summary()
    d = st["day"]
    await send_telegram(
        session,
        f"{icon} <b>[Ake signals 7 · 1m] {side_txt(trade['signal'])} yopildi</b> — <b>{html.escape(res_code)}</b>\n"
        f"E: <code>{entry:.2f}</code> ➡️ Yopildi: <code>{price:.2f}</code> | Rekord TP: <b>{peak_r:+.1f}R</b>\n"
        f"💵 Natija: <b>{pnl_usd:+.2f}$</b> | 💰 Umumiy Balans: <b>${new_balance:.2f}</b>\n"
        f"📊 Bugun (Kun): <b>{d['total']}</b> bitim | TP olindi: <b>{d['wins']} ({d['wr']}%)</b> | "
        f"Stop -1R: <b>{d['losses']}</b> | Jami: <b>{d['total_r']:+.1f}R</b>",
    )

async def monitor_ake_trade(session, price):
    """Ake Signals 7 (1m) ochiq bitimini TP1..TP5 (1R..5R) va SL (-1R) bo'yicha real-vaqtda kuzatish."""
    if _ake_monitor_lock.locked():
        return
    async with _ake_monitor_lock:
        trade = await db.state_get("current_trade_ake")
        if not trade:
            return

        is_long = trade["signal"] == "LONG"
        entry = float(trade["entry"])
        max_hit = int(trade.get("max_r_hit", 0))
        changed = False
        notes = []

        def reached(lvl):
            return price >= lvl if is_long else price <= lvl

        # TP1..TP5 bosqichlarini tekshirish
        for k in range(1, 6):
            tp_lvl = float(trade[f"tp{k}"])
            if reached(tp_lvl) and max_hit < k:
                max_hit = k
                trade["max_r_hit"] = k
                changed = True
                if k == 1:
                    # TP1 (1R) olindi -> SL Entry (0.0R Breakeven) ga ko'chadi
                    trade["sl"] = entry
                    notes.append(
                        f"⚡ <b>[Ake signals 7 · 1m] {side_txt(trade['signal'])} TP1 (+1.0R) olindi ✓</b> "
                        f"(<code>{tp_lvl:.2f}</code>) | SL ➡️ E (<code>{entry:.2f}</code> 0.0R)"
                    )
                elif k < 5:
                    prev_tp = float(trade[f"tp{k-1}"])
                    trade["sl"] = prev_tp
                    notes.append(
                        f"🔥 <b>[Ake signals 7 · 1m] {side_txt(trade['signal'])} TP{k} (+{k}.0R) olindi ✓</b> "
                        f"(<code>{tp_lvl:.2f}</code>) | SL ➡️ TP{k-1} (<code>{prev_tp:.2f}</code> +{k-1}.0R)"
                    )

        if max_hit >= 5 and reached(float(trade["tp5"])):
            await close_ake_trade(session, trade, float(trade["tp5"]), reason_label="TP5")
            return

        # SL tekshiruvi
        sl_lvl = float(trade["sl"])
        sl_hit = (is_long and price <= sl_lvl) or ((not is_long) and price >= sl_lvl)
        if sl_hit:
            lbl = "TRAIL_SL" if max_hit >= 1 else "STOP_-1R"
            await close_ake_trade(session, trade, sl_lvl, reason_label=lbl)
            return

        if changed:
            await db.state_set("current_trade_ake", trade)
            ex_mode = await db.state_get("exness_mode", EXNESS_AUTOTRADE_DEFAULT_MODE)
            if ex_mode in ("ake", "both"):
                _spawn(sync_exness_positions_action(action="modify_sl", new_sl=trade["sl"]))
        for n in notes:
            await send_telegram(session, n)

# ==================== MAIN SCANNER LOOP ====================
async def scanner_loop(app):
    global _LAST_CANDLES_1M, _LAST_AKE_INFO
    session = app["session"]
    last_impulse_ts = 0
    last_signal_bar_ts = 0
    last_ake_bar_ts = 0
    last_ake_signal_time = 0.0

    while True:
        try:
            if not market_open():
                await asyncio.sleep(30)
                continue

            status = await analyze_gold_market(session, run_deep_scoring=True)
            price = status["price"]
            cl15 = status["cl15"]

            # ==================== A) AKE SIGNALS 7 (1M SCALPING) ALOHIDA MOTORI ====================
            ake_enabled = await db.state_get("ake_enabled", AKE_ENABLED_DEFAULT)
            if ake_enabled:
                try:
                    c1_raw = await fetch_ohlcv(session, "1m", limit=AKE_LIMIT)
                    cl1_ake = closed_only(c1_raw)
                    _LAST_CANDLES_1M = cl1_ake
                    recent_ake_list = await db.get_recent_ake_trades(limit=1)
                    last_closed_ake = recent_ake_list[0] if recent_ake_list else None
                    ake_info = analyze_ake_1m_scalp(cl1_ake, status.get('cl15', []), price, last_closed_ake=last_closed_ake)
                    _LAST_AKE_INFO = ake_info

                    ake_trade = await db.state_get("current_trade_ake")
                    if ake_trade:
                        await monitor_ake_trade(session, price)
                        ake_trade = await db.state_get("current_trade_ake")

                    ake_cand = ake_info.get("candidate")
                    if ake_cand and not status["news_blocked"]:
                        ake_bar_key = f"{ake_cand['bar_ts']}_{ake_cand['signal']}"
                        elapsed_since_ake = time.time() - last_ake_signal_time
                        ake_cd_ok = elapsed_since_ake >= AKE_COOLDOWN_SEC

                        # MUHIM QOIDA: Ochiq Ake bitimi hali TP1 (+1R) ga yetmagan bo'lsa,
                        # uni teskari signal (FLIP) bilan erta yopish QAT'IYAN TAQIQLANADI!
                        # Bitim to'liq SL (-1R) yoki TP1..TP5 gacha ishlashi shart.
                        can_flip_after_tp1 = (
                            ake_trade is not None
                            and ake_cand["signal"] != ake_trade["signal"]
                            and int(ake_trade.get("max_r_hit", 0)) >= 1
                            and elapsed_since_ake >= AKE_FLIP_AFTER_TP1_MIN_SEC
                        )

                        if ake_bar_key != last_ake_bar_ts and (
                            (ake_trade is None and ake_cd_ok) or can_flip_after_tp1
                        ):
                            async with _ake_monitor_lock:
                                cur_ake = await db.state_get("current_trade_ake")
                                if cur_ake and cur_ake["signal"] != ake_cand["signal"]:
                                    await close_ake_trade(session, cur_ake, price, reason_label="TRAIL_SL")
                                await db.state_set("current_trade_ake", ake_cand)
                                last_ake_bar_ts = ake_bar_key
                                last_ake_signal_time = time.time()

                            acc_ake = await get_account()
                            ake_msg = format_ake_signal_message(ake_cand, balance=acc_ake['balance'])
                            await send_telegram(session, ake_msg, with_keyboard=True)

                            ake_stats_now = await get_ake_stats_summary()
                            ake_png = await render_ake_chart_async(
                                cl1_ake, ake_trade=ake_cand, ake_stats=ake_stats_now,
                                scalp_band=ake_info.get("scalp_band"), zones_1m=ake_info.get("zones_1m")
                            )
                            if ake_png:
                                await send_telegram_photo(
                                    session, ake_png,
                                    caption=(
                                        f"⚡ <b>Ake signals 7 [1m] {side_txt(ake_cand['signal'])}</b> | "
                                        f"E: {ake_cand['entry']} | SL(-1R): {ake_cand['sl']} | "
                                        f"TP1: {ake_cand['tp1']} .. TP5: {ake_cand['tp5']}"
                                    ),
                                )

                            ex_mode = await db.state_get("exness_mode", EXNESS_AUTOTRADE_DEFAULT_MODE)
                            if ex_mode in ("ake", "both"):
                                _spawn(execute_exness_autotrades(session, dict(ake_cand)))
                except asyncio.CancelledError:
                    raise
                except Exception as e_ake:
                    logger.warning(f"Ake 1m scanner xato: {_safe_log_error(e_ake)}")

            # ==================== B) DAY TRADING (15M/5M SMC+ICT) ALOHIDA MOTORI ====================
            day_enabled = await db.state_get("day_enabled", DAY_ENABLED_DEFAULT)
            if not day_enabled:
                await asyncio.sleep(CHECK_INTERVAL_SEC)
                continue

            # 1) Impulse / keskin harakat haqida ogohlantirish
            imp = detect_impulse(cl15)
            if imp and imp["ts"] != last_impulse_ts:
                last_impulse_ts = imp["ts"]
                await send_telegram(
                    session,
                    f"⚡ Keskin harakat {imp['direction']} | {imp['range']}$ | narx: <code>{imp['price']}</code>",
                )

            # 2) Ochiq bitimni tekshirish
            trade = await db.state_get("current_trade")
            if trade:
                await monitor_trade(session, price, status["b15"])
                trade = await db.state_get("current_trade")

            # 3) Yangi Sniper signal tekshirish (Kuniga 7-10 ta sifatli signal uchun Multi-Wave + Reversal Flip)
            cand = status.get("candidate")
            daily_info = await get_daily_signal_info()
            cooldown_ok = (time.time() - daily_info.get("last_ts", 0)) >= SIGNAL_COOLDOWN_SEC
            is_reversal_flip = (
                trade is not None
                and cand is not None
                and cand.get("signal") != trade.get("signal")
                and cand.get("score", 0) >= 78
                and cooldown_ok
            )
            can_open_new = (
                (trade is None and cooldown_ok)
                or is_reversal_flip
            )
            if can_open_new:
                cur_bar_ts = f"{cl15[-1][0]}_{cand.get('signal')}_{cand.get('zone_kind')}_{cand.get('zone_bottom')}" if (cl15 and cand) else 0
                if (
                    cand
                    and cand.get("score", 0) >= cand.get("min_req_score", MIN_SIGNAL_SCORE)
                    and status["in_killzone"]
                    and not status["news_blocked"]
                    and cur_bar_ts != last_signal_bar_ts
                ):
                    async with _monitor_lock:
                        if trade:
                            close_lbl = "BE+" if trade.get("tp1_notified") else "REVERSAL_FLIP"
                            old_lot = trade.get("lot", XM_MIN_LOT)
                            await db.insert_trade(
                                trade, close_lbl, price,
                                old_lot,
                                trade.get("risk_usd", 0.0),
                                trade.get("confirmations", ""),
                            )
                            _metrics["closed"] += 1
                            _spawn(sync_exness_positions_action(action="close"))
                            # Balansni avto-yangilash + qisqa xabar
                            flip_pnl, flip_r = calc_trade_pnl(trade, price, old_lot)
                            flip_bal = await apply_pnl_to_balance(flip_pnl)
                            await send_telegram(
                                session,
                                f"{'✅' if flip_pnl > 0 else ('❌' if flip_pnl < 0 else '➖')} "
                                f"<b>{side_txt(trade['signal'])} yopildi</b> — {html.escape(result_txt(close_lbl))}\n"
                                f"Narx: <code>{price}</code> | Natija: <b>{flip_pnl:+.2f}$</b> ({flip_r:+.2f}R)\n"
                                f"💰 Balans: <b>${flip_bal:.2f}</b>",
                            )
                        upd_daily = await increment_daily_signal_count()
                        cand["daily_count"] = upd_daily["count"]
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
                                caption=f"{side_icon(cand['signal'])} <b>{side_txt(cand['signal'])} @ {cand['entry']}</b> | SL: {cand['sl']} | TP: {cand['tp1']}",
                            )

                        # Exness & Binance avto-savdo (Day Trading rejimi yoqilgan bo'lsa)
                        ex_mode = await db.state_get("exness_mode", EXNESS_AUTOTRADE_DEFAULT_MODE)
                        if ex_mode in ("day", "both"):
                            _spawn(execute_exness_autotrades(session, dict(cand)))
                        _spawn(execute_binance_autotrades(session, dict(cand)))

                        # Gemini AI sharhi
                        if GEMINI_API_KEY and SEND_AI_EXTRA_MESSAGES:
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
    lines = [f"🏦 <b>Exness hisoblar ({len(accs)}/{MAX_EXNESS_ACCOUNTS})</b>"]
    if not accs:
        lines.append("Hali hisob ulanmagan.")
    else:
        for a in accs:
            st_icon = "🟢" if a["auto_trade"] else "🔴"
            bal_str = f"${a['last_balance']:.2f}" if a["last_balance"] is not None else "?"
            lines.append(
                f"{st_icon} #{a['slot']} <code>{html.escape(a['login'])}</code> | "
                f"lot {a['lot']} | balans {bal_str}"
            )
    lines.extend([
        "",
        "• Ulash: <code>/exness_add LOGIN PAROL AUTO 0.01</code>",
        "• Lot: <code>/exness_lot 1 0.02</code>",
        "• Avto-savdo yoq/o'ch: <code>/exness_toggle 1</code>",
        "• Tekshirish: <code>/exness_test 1</code>",
        "• O'chirish: <code>/exness_del 1</code>",
    ])
    return "\n".join(lines)

# ==================== TELEGRAM BUYRUQLAR HANDLERI ====================
async def handle_command(session, chat_id, text, message_id=None):
    global _CURRENT_GEMINI_MODEL, _ACTIVE_BROKER, _AUTO_SYNC_ENABLED, _BROKER_CALIBRATION
    cid = str(chat_id)
    raw_text = (text or "").strip()
    if not raw_text:
        return

    # Parol yuborilgan xabarni darhol o'chirish himoyasi
    if raw_text.lower().startswith(("/exness_add", "/binance_set", "/metaapi_set")) and message_id:
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

    # Tugmalar mapping (Day Trading va Ake Signals 7 alohida)
    if raw_text in ("📊 Signal", "📊 Day Signal"):
        cmd = "/signal"
    elif raw_text in ("🖼 Grafik", "🖼 Day Grafik"):
        cmd = "/chart"
    elif raw_text in ("📈 Statistika", "📈 Day Stat"):
        cmd = "/stats"
    elif raw_text == "⚡ Ake Signal":
        cmd = "/ake"
    elif raw_text == "📉 Ake Grafik":
        cmd = "/ake_chart"
    elif raw_text == "📋 Ake Stat":
        cmd = "/ake_stats"
    elif raw_text == "🧠 AI Tahlil":
        cmd = "/ai"
    elif raw_text == "🏦 Exness Hisoblar":
        cmd = "/exness"
    elif raw_text == "🎯 Zonalar":
        cmd = "/zones"

    if cmd in ("/start", "/help"):
        ake_en = await db.state_get("ake_enabled", AKE_ENABLED_DEFAULT)
        day_en = await db.state_get("day_enabled", DAY_ENABLED_DEFAULT)
        ex_m = await db.state_get("exness_mode", EXNESS_AUTOTRADE_DEFAULT_MODE)
        msg = (
            "👑 <b>GOLD BOT v20 — 2 TA ALOHIDA TIZIM</b>\n\n"
            f"📊 <b>1) DAY TRADING (15m/5m SMC+ICT Sniper)</b> [{'🟢 Yoqilgan' if day_en else '🔴 O`chirilgan'}]\n"
            "• /signal yoki /day — Day Trading holati va ochiq bitimi\n"
            "• /chart — 15m SMC Day Trading grafigi\n"
            "• /stats — Day Trading statistikasi va Win Rate\n"
            "• /close — Day Trading ochiq bitimini yopish\n"
            "• /day_toggle — Day Trading avto-signalni yoqish/o'chirish\n\n"
            f"⚡ <b>2) AKE SIGNALS 7 (1m Scalping 1R–5R)</b> [{'🟢 Yoqilgan' if ake_en else '🔴 O`chirilgan'}]\n"
            "• /ake — Ake Signals 7 (1m) joriy holati va TP1–TP5 bitimi\n"
            "• /ake_chart — 1m Ake Signals grafigi (TP1–TP5 + R-jadval)\n"
            "• /ake_stats — День / Неделя / Месяц R-statistika jadvali\n"
            "• /ake_close — Ake 1m ochiq bitimini qo'lda yopish\n"
            "• /ake_toggle — Ake Signals 7 avto-signalni yoqish/o'chirish\n\n"
            "⚙️ <b>UMUMIY VA EXNESS SOZLAMALAR:</b>\n"
            f"• /exness_mode <code>day|ake|both</code> — Exness avto-savdo rejimi (hozir: <b>{ex_m.upper()}</b>)\n"
            "• /zones — 15m/4H/1D kirish zonalari\n"
            "• /balance <code>20</code> | /risk <code>15</code>\n"
            "• /broker <code>exness|xm</code> — Exness yoki XM rejimini tanlash\n"
            "• /autosync <code>on|off|now</code> — Avto-Sync (Spot XAU/USD)\n"
            "• /sync_exness <code>4135.20</code> | /sync_xm <code>4135.45</code>\n"
            "• /metaapi_set — Railway/Termux Cloud MT5 avto-savdo ulash\n"
            "• /termux — Railway va Termux avto-savdo qo'llanmasi\n"
            "• /ake_reset | /day_reset — Statistikani 0 ga tozalash\n"
            "• /ai <code>[savol]</code> — Gemini AI tahlil"
        )
        await send_telegram(session, msg, with_keyboard=True, chat_id=cid)

    elif cmd in ("/ake", "/ake_signal", "/ake_zones"):
        c1_raw, c15_raw = await asyncio.gather(
            fetch_ohlcv(session, "1m", limit=AKE_LIMIT),
            fetch_ohlcv(session, "15m", limit=LIMIT),
        )
        cl1_ake = closed_only(c1_raw)
        cl15_ake = closed_only(c15_raw)
        p_now = get_unified_price(c1_raw)
        ake_info = analyze_ake_1m_scalp(cl1_ake, cl15_ake, p_now)
        if cmd == "/ake_zones":
            await send_telegram(session, format_ake_zones_block(ake_info, p_now), with_keyboard=True, chat_id=cid)
            return
        ake_trade = await db.state_get("current_trade_ake")
        ake_en = await db.state_get("ake_enabled", AKE_ENABLED_DEFAULT)
        sb = ake_info.get("scalp_band") or (0.0, 0.0)
        lines = [
            f"⚡ <b>Ake signals 7 (1m Skalp): <code>{p_now:.2f}</code></b>",
            f"Holat: <b>{'🟢 FAOL' if ake_en else '🔴 O`CHIRILGAN'}</b> | 1m Trend: <b>{trend_txt(ake_info.get('trend_1m'))}</b>",
            f"Skalp Kanal: <code>{sb[0]:.2f} — {sb[1]:.2f}</code> | 1m ATR: <code>{ake_info.get('atr1')}</code>",
            f"Osiyo (18:00-02:00): Yuqori <code>{ake_info.get('asia_high')}</code> / Pastki <code>{ake_info.get('asia_low')}</code>",
        ]
        if ake_trade:
            lines.append("")
            acc_ake = await get_account()
            lines.append(format_ake_signal_message(ake_trade, balance=acc_ake['balance']))
        else:
            lines.append("📭 Hozir ochiq 1m Ake bitim yo'q (yangi zona + impuls kutilmoqda)")
        lines.append("")
        lines.append(format_ake_zones_block(ake_info, p_now))
        lines.append("")
        lines.append(await format_ake_stats_table_message())
        await send_telegram(session, "\n".join(lines), with_keyboard=True, chat_id=cid)

    elif cmd == "/ake_chart":
        c1_raw = await fetch_ohlcv(session, "1m", limit=AKE_LIMIT)
        cl1_ake = closed_only(c1_raw)
        p_now = get_unified_price(c1_raw)
        ake_info = analyze_ake_1m_scalp(cl1_ake, [], p_now)
        ake_trade = await db.state_get("current_trade_ake") or ake_info.get("candidate")
        ake_stats = await get_ake_stats_summary()
        png = await render_ake_chart_async(
            cl1_ake, ake_trade=ake_trade, ake_stats=ake_stats, scalp_band=ake_info.get("scalp_band"), zones_1m=ake_info.get("zones_1m")
        )
        if png:
            d_st = ake_stats["day"]
            cap = (
                f"⚡ <b>Ake signals 7 · 1m XAUUSD</b> | Narx: <code>{p_now:.2f}</code>\n"
                f"Kun: {d_st['total']} bitim | TP olindi: {d_st['wins']} ({d_st['wr']}%) | "
                f"Stop -1R: {d_st['losses']} | Jami: <b>{d_st['total_r']:+.1f}R</b>"
            )
            await send_telegram_photo(session, png, caption=cap, chat_id=cid)
        else:
            await send_telegram(session, "⚠️ 1m Ake grafik chizish uchun shamlar yetarli emas.", chat_id=cid)

    elif cmd in ("/ake_stats", "/ake_stat"):
        msg = await format_ake_stats_table_message()
        recent_ake = await db.get_recent_ake_trades(limit=8)
        if recent_ake:
            msg += "\n\n📜 <b>Oxirgi Ake 1m bitimlar:</b>"
            for r in recent_ake:
                msg += (
                    f"\n • <b>{side_txt(r['signal'])}</b> | {html.escape(str(r['result']))} | "
                    f"<code>{r['entry']:.2f} ➡️ {r['close_price']:.2f}</code> (Rekord: <b>{r['peak_r']:+.1f}R</b>)"
                )
        await send_telegram(session, msg, with_keyboard=True, chat_id=cid)

    elif cmd == "/ake_reset":
        async with _ake_monitor_lock:
            await db.clear_ake_trades()
            await db.state_set("current_trade_ake", None)
        await send_telegram(
            session,
            "🧹 <b>Ake signals 7 (1m) tarixi va R-jadvali 0 ga tozalandi!</b>",
            chat_id=cid,
        )

    elif cmd == "/day_reset":
        async with _monitor_lock:
            await db.clear_day_trades()
            await db.state_set("current_trade", None)
        await send_telegram(
            session,
            "🧹 <b>Day Trading (15m) tarixi va Win Rate statistikasi 0 ga tozalandi!</b>",
            chat_id=cid,
        )

    elif cmd == "/ake_close":
        async with _ake_monitor_lock:
            ake_trade = await db.state_get("current_trade_ake")
            if not ake_trade:
                await send_telegram(session, "📭 Ochiq Ake 1m bitim yo'q.", chat_id=cid)
                return
            cur_p = live_price() or (_LAST_CANDLES_1M[-1][C] if _LAST_CANDLES_1M else ake_trade["entry"])
            await close_ake_trade(session, ake_trade, round(cur_p, 2), reason_label="MANUAL")

    elif cmd in ("/ake_toggle", "/ake_on", "/ake_off"):
        cur_en = await db.state_get("ake_enabled", AKE_ENABLED_DEFAULT)
        new_en = True if cmd == "/ake_on" else (False if cmd == "/ake_off" else (not cur_en))
        await db.state_set("ake_enabled", new_en)
        await send_telegram(
            session,
            f"⚡ <b>Ake Signals 7 (1m Scalping):</b> {'🟢 YOQILDI' if new_en else '🔴 O`CHIRILDI'}",
            chat_id=cid,
        )

    elif cmd in ("/day_toggle", "/day_on", "/day_off"):
        cur_en = await db.state_get("day_enabled", DAY_ENABLED_DEFAULT)
        new_en = True if cmd == "/day_on" else (False if cmd == "/day_off" else (not cur_en))
        await db.state_set("day_enabled", new_en)
        await send_telegram(
            session,
            f"📊 <b>Day Trading (15m/5m SMC+ICT):</b> {'🟢 YOQILDI' if new_en else '🔴 O`CHIRILGAN'}",
            chat_id=cid,
        )

    elif cmd == "/exness_mode":
        if len(parts) >= 2 and parts[1].lower() in ("day", "ake", "both"):
            m_val = parts[1].lower()
            await db.state_set("exness_mode", m_val)
            await send_telegram(
                session,
                f"🏦 <b>Exness avto-savdo rejimi o'zgartirildi:</b> <code>{m_val.upper()}</code>\n"
                "• <code>day</code> — faqat Day Trading (15m) signallarini ochadi\n"
                "• <code>ake</code> — faqat Ake Signals 7 (1m) signallarini ochadi\n"
                "• <code>both</code> — ikkala tizim signallarini ham ochadi",
                chat_id=cid,
            )
        else:
            cur_m = await db.state_get("exness_mode", EXNESS_AUTOTRADE_DEFAULT_MODE)
            await send_telegram(
                session,
                f"🏦 <b>Joriy Exness avto-savdo rejimi:</b> <code>{cur_m.upper()}</code>\n"
                "O'zgartirish: <code>/exness_mode day</code> | <code>/exness_mode ake</code> | <code>/exness_mode both</code>",
                chat_id=cid,
            )

    elif cmd in ("/signal", "/status", "/day"):
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

    elif cmd in ("/zones", "/zonalar"):
        status, c1_raw = await asyncio.gather(
            analyze_gold_market(session, run_deep_scoring=False),
            fetch_ohlcv(session, "1m", limit=AKE_LIMIT),
        )
        zones = status["zones15"]
        p = status["price"]
        cl1_z = closed_only(c1_raw)
        ake_z_info = analyze_ake_1m_scalp(cl1_z, status["cl15"], p)
        bull_z = [z for z in zones if z["type"] == "bullish"]
        bear_z = [z for z in zones if z["type"] == "bearish"]
        bull_z.sort(key=lambda z: abs(p - z["ce"]))
        bear_z.sort(key=lambda z: abs(p - z["ce"]))

        lines = [
            format_ake_zones_block(ake_z_info, p),
            "",
            f"📊 <b>Day Trading (15m) Zonalar (Narx: <code>{p:.2f}</code>):</b>",
            f"🟢 <b>15m BUY zonalari (pastda):</b>",
        ]
        for z in bull_z[:5]:
            dist = p - z["ce"]
            lines.append(f" • <b>{z['kind']}</b>: <code>{z['bottom']:.2f} — {z['top']:.2f}</code> (CE: {z['ce']:.2f} | {dist:+.1f}$)")
        if not bull_z:
            lines.append(" • yo'q")

        lines.append("\n🔴 <b>SELL zonalari (tepada):</b>")
        for z in bear_z[:5]:
            dist = z["ce"] - p
            lines.append(f" • <b>{z['kind']}</b>: <code>{z['bottom']:.2f} — {z['top']:.2f}</code> (CE: {z['ce']:.2f} | +{dist:.1f}$)")
        if not bear_z:
            lines.append(" • yo'q")

        htf_z = status.get("htf_zones") or []
        if htf_z:
            lines.append("\n🏛 <b>1 Oylik HTF (4H/1D) Zonalar:</b>")
            htf_sorted = sorted(htf_z, key=lambda z: abs(p - z["ce"]))[:6]
            for z in htf_sorted:
                icon = "🟢" if z["type"] == "bullish" else "🔴"
                lines.append(
                    f" • {icon} <b>{z.get('tf', '4H')} {z['kind']}</b>: "
                    f"<code>{z['bottom']:.2f} — {z['top']:.2f}</code> (CE: {z['ce']:.2f})"
                )

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

    elif cmd in ("/sync", "/sync_exness", "/sync_xm") and len(parts) >= 2:
        try:
            target_brk = "xm" if cmd == "/sync_xm" else ("exness" if cmd == "/sync_exness" else _ACTIVE_BROKER)
            _ACTIVE_BROKER = target_brk
            await db.state_set("active_broker", _ACTIVE_BROKER)

            broker_p = float(parts[1].replace(",", "."))
            raw_bybit = _LIVE_BYBIT_RAW_PRICE or await fetch_raw_price(session)
            spot_p, _ = await fetch_spot_xauusd_price(session)
            if spot_p:
                _BROKER_CALIBRATION[target_brk] = round(broker_p - spot_p, 2)
                await db.state_set("broker_calibration", _BROKER_CALIBRATION)

            new_off = round(broker_p - raw_bybit, 2)
            delta = await apply_offset_change(new_off)
            await send_telegram(
                session,
                f"🔄 <b>{target_brk.upper()} narxi sinxronlandi va Avto-Sync kalibrlandi!</b>\n"
                f"{target_brk.upper()} narxi: <code>{broker_p:.2f}</code> | Bybit: <code>{raw_bybit:.2f}</code>\n"
                f"Yangi Offset: <code>{_CURRENT_OFFSET:+.2f}</code> (Siljish: <code>{delta:+.2f}$</code>)",
                chat_id=cid,
            )
        except Exception as e:
            await send_telegram(session, f"⚠️ Format: <code>/sync 4135.20</code> yoki <code>/sync_exness 4135.20</code> | <code>/sync_xm 4135.45</code> ({html.escape(str(e))})", chat_id=cid)

    elif cmd == "/broker":
        if len(parts) >= 2 and parts[1].lower() in ("exness", "xm"):
            _ACTIVE_BROKER = parts[1].lower()
            await db.state_set("active_broker", _ACTIVE_BROKER)
            info = await run_auto_sync_once(session, force=True)
            cur_p = get_unified_price()
            await send_telegram(
                session,
                f"🏦 <b>Faol broker o'zgartirildi: {_ACTIVE_BROKER.upper()}</b>\n"
                f"Joriy {_ACTIVE_BROKER.upper()} narxi: <code>{cur_p}</code> (Offset: <code>{_CURRENT_OFFSET:+.2f}</code>)\n"
                f"Exness va XM narxlari avtomatik Spot XAU/USD ga bog'langan.",
                chat_id=cid,
            )
        else:
            ex_p = _LAST_SPOT_SYNC_INFO.get("exness_price")
            xm_p = _LAST_SPOT_SYNC_INFO.get("xm_price")
            await send_telegram(
                session,
                f"🏦 <b>Joriy faol broker: {_ACTIVE_BROKER.upper()}</b>\n"
                f"• Exness narxi: <code>{ex_p}</code> (kalibrlash: <code>/sync_exness NARX</code>)\n"
                f"• XM narxi: <code>{xm_p}</code> (kalibrlash: <code>/sync_xm NARX</code>)\n"
                "Almashtirish: <code>/broker exness</code> yoki <code>/broker xm</code>",
                chat_id=cid,
            )

    elif cmd == "/autosync":
        arg = parts[1].lower() if len(parts) >= 2 else "now"
        if arg in ("on", "1", "true"):
            _AUTO_SYNC_ENABLED = True
            await db.state_set("auto_sync_enabled", True)
            info = await run_auto_sync_once(session, force=True)
            await send_telegram(
                session,
                f"🟢 <b>Avto-Sync YOQILDI!</b> ({_ACTIVE_BROKER.upper()})\n"
                f"Joriy narx: <code>{get_unified_price()}</code> | Offset: <code>{_CURRENT_OFFSET:+.2f}</code>",
                chat_id=cid,
            )
        elif arg in ("off", "0", "false"):
            _AUTO_SYNC_ENABLED = False
            await db.state_set("auto_sync_enabled", False)
            await send_telegram(session, "⚪ <b>Avto-Sync O'CHIRILDI.</b> (Qo'lda /sync ishlaydi)", chat_id=cid)
        else:
            info = await run_auto_sync_once(session, force=True)
            if info:
                await send_telegram(
                    session,
                    f"🔄 <b>Avto-Sync bajarildi ({html.escape(str(info.get('source')))}):</b>\n"
                    f"🏦 Exness: <code>{info.get('exness_price')}</code> | 🏛 XM: <code>{info.get('xm_price')}</code>\n"
                    f"📈 Bybit raw: <code>{info.get('bybit_raw')}</code> | Offset ({_ACTIVE_BROKER.upper()}): <code>{_CURRENT_OFFSET:+.2f}</code>",
                    chat_id=cid,
                )
            else:
                await send_telegram(session, "⚠️ Spot XAU/USD manbasiga ulanib bo'lmadi, joriy offset saqlandi.", chat_id=cid)

    elif cmd == "/metaapi_set":
        if len(parts) < 3:
            await send_telegram(
                session,
                "☁️ <b>Railway & Termux uchun Cloud MT5 Avto-Savdo (MetaApi) ulash:</b>\n"
                "Format: <code>/metaapi_set METAAPI_TOKEN ACCOUNT_ID [SYMBOL] [LOT]</code>\n"
                "Misol (Exness): <code>/metaapi_set eyJhbG... 1a2b3c4d-5678 XAUUSDm 0.01</code>\n"
                "Misol (XM): <code>/metaapi_set eyJhbG... 1a2b3c4d-5678 GOLD 0.01</code>\n\n"
                "💡 <i>MetaApi.cloud da bepul ro'yxatdan o'tib, Exness/XM MT5 login-parolingizni ulaysiz va Token hamda Account ID ni shu yerga yuborasiz — shunda Windows kompyutersiz to'g'ridan-to'g'ri Railway yoki Termux'da 24/7 avto-savdo ishlaydi!</i>",
                chat_id=cid,
            )
            return
        m_tok = parts[1].strip()
        m_acc = parts[2].strip()
        m_sym = parts[3].strip() if len(parts) >= 4 else ("GOLD" if _ACTIVE_BROKER == "xm" else "XAUUSDm")
        m_lot = float(parts[4]) if len(parts) >= 5 else 0.01
        await db.state_set("metaapi_config", {"token": m_tok, "account_id": m_acc, "symbol": m_sym, "lot": m_lot})
        pw_enc = encrypt_secret(m_tok)
        slot = await db.add_or_update_exness_account(cid, m_acc, pw_enc, "METAAPI-CLOUD", label="Cloud MT5", lot=m_lot, symbol=m_sym)
        await send_telegram(
            session,
            f"✅ <b>Cloud MT5 Avto-Savdo (#{slot}-slot) ulandi!</b>\n"
            f"Account ID: <code>{html.escape(m_acc[:12])}...</code> | Simbol: <code>{html.escape(m_sym)}</code> | Lot: <code>{m_lot:.2f}</code>\n"
            f"🔒 Token xabaringiz xavfsizlik uchun chatdan o'chirildi.",
            chat_id=cid,
        )

    elif cmd == "/test_order":
        side_arg = parts[1].upper() if len(parts) >= 2 else "BUY"
        sig_type = "LONG" if side_arg in ("BUY", "LONG") else "SHORT"
        cur_p = get_unified_price(_LAST_CANDLES_1M or _LAST_CANDLES_15M) or 4135.00
        sl_p = round(cur_p - 2.50, 2) if sig_type == "LONG" else round(cur_p + 2.50, 2)
        tp_p = round(cur_p + 5.00, 2) if sig_type == "LONG" else round(cur_p - 5.00, 2)
        test_trade = {
            "engine": "TEST_ORDER",
            "signal": sig_type,
            "entry": cur_p,
            "sl": sl_p,
            "tp1": tp_p,
            "tp2": tp_p,
            "lot": XM_MIN_LOT,
        }
        await execute_exness_autotrades(session, test_trade)
        t_online = (time.time() - _TERMUX_BRIDGE_STATE.get("last_seen", 0)) <= 20
        st_line = "🟢 Termux ulangan — buyruq Termux'ga yuborildi!" if t_online else "⏳ Buyruq navbatga qo'shildi (Termux'da termux_autotrader.py ni yoqing)!"
        await send_telegram(
            session,
            f"🧪 <b>Test Order ({side_txt(sig_type)} 0.01 lot) yuborildi!</b>\n"
            f"Kirish: <code>{cur_p}</code> | SL: <code>{sl_p}</code> | TP: <code>{tp_p}</code>\n"
            f"{st_line}",
            chat_id=cid,
        )

    elif cmd == "/termux":
        t_age = time.time() - _TERMUX_BRIDGE_STATE.get("last_seen", 0)
        t_on = t_age <= 20
        t_status = (
            f"🟢 <b>ULANGAN</b> (Hisob: <code>{_TERMUX_BRIDGE_STATE.get('login')}</code> | "
            f"Server: <code>{_TERMUX_BRIDGE_STATE.get('server')}</code> | "
            f"Narx: <code>{_TERMUX_BRIDGE_STATE.get('bid')}</code> | "
            f"Balans: <b>${_TERMUX_BRIDGE_STATE.get('balance')}</b>)"
            if t_on else "⚪ <b>Hozircha ulanmagan</b> (Termux'da <code>python termux_autotrader.py</code> ni ishga tushiring)"
        )
        await send_telegram(
            session,
            f"📱 <b>Termux Avto-Trade Holati:</b> {t_status}\n\n"
            "🚀 <b>MetaApi.cloud'siz to'g'ridan-to'g'ri Termux'da yoqish:</b>\n"
            "1. Termux'ga <code>termux_autotrader.py</code> faylini yuklang.\n"
            "2. Termux ichida 1 marta o'rnating:\n"
            "<pre>pkg update -y && pkg install x11-repo tur-repo -y\n"
            "pkg install python chromium -y\n"
            "pip install aiohttp selenium</pre>\n"
            "3. Ishga tushiring:\n"
            "<pre>python termux_autotrader.py</pre>\n"
            "4. Birinchi marta ishga tushganda Railway ssilkangizni va Exness/XM Login, Parol, Serveringizni so'raydi va saqlab oladi.\n"
            "5. Tekshirish uchun Telegram'da <code>/test_order buy</code> yuborsangiz, Termux darhol Exness/XM da 0.01 lot ochadi!",
            chat_id=cid,
        )
        return
    elif cmd in ("/ake", "/ake_signal"):
        c1_raw = await fetch_ohlcv(session, "1m", limit=AKE_LIMIT)
        cl1_ake = closed_only(c1_raw)
        p_now = get_unified_price(c1_raw)
        ake_info = analyze_ake_1m_scalp(cl1_ake, [], p_now)
        ake_trade = await db.state_get("current_trade_ake")
        ake_en = await db.state_get("ake_enabled", AKE_ENABLED_DEFAULT)
        sb = ake_info.get("scalp_band") or (0.0, 0.0)
        lines = [
            f"⚡ <b>Ake signals 7 (1m Scalping): {p_now:.2f}</b>",
            f"Holat: <b>{'🟢 FAOL' if ake_en else '🔴 O`CHIRILGAN'}</b> | 1m Trend: <b>{trend_txt(ake_info.get('trend_1m'))}</b>",
            f"Scalp Band: <code>{sb[0]:.2f} — {sb[1]:.2f}</code> | 1m ATR: <code>{ake_info.get('atr1')}</code>",
            f"Asia (18:00-02:00): High <code>{ake_info.get('asia_high')}</code> / Low <code>{ake_info.get('asia_low')}</code>",
        ]
        if ake_trade:
            lines.append("")
            acc_ake = await get_account()
            lines.append(format_ake_signal_message(ake_trade, balance=acc_ake['balance']))
        else:
            lines.append("📭 Hozir ochiq 1m Ake bitim yo'q (yangi 1m impuls kutilmoqda)")
        lines.append("")
        lines.append(await format_ake_stats_table_message())
        await send_telegram(session, "\n".join(lines), with_keyboard=True, chat_id=cid)

    elif cmd == "/ake_chart":
        c1_raw = await fetch_ohlcv(session, "1m", limit=AKE_LIMIT)
        cl1_ake = closed_only(c1_raw)
        p_now = get_unified_price(c1_raw)
        ake_info = analyze_ake_1m_scalp(cl1_ake, [], p_now)
        ake_trade = await db.state_get("current_trade_ake") or ake_info.get("candidate")
        ake_stats = await get_ake_stats_summary()
        png = await render_ake_chart_async(
            cl1_ake, ake_trade=ake_trade, ake_stats=ake_stats, scalp_band=ake_info.get("scalp_band")
        )
        if png:
            d_st = ake_stats["day"]
            cap = (
                f"⚡ <b>Ake signals 7 · 1m XAUUSD</b> | Narx: <code>{p_now:.2f}</code>\n"
                f"День: {d_st['total']} bitim | Тейк: {d_st['wins']} ({d_st['wr']}%) | "
                f"Стоп -1R: {d_st['losses']} | Итого: <b>{d_st['total_r']:+.1f}R</b>"
            )
            await send_telegram_photo(session, png, caption=cap, chat_id=cid)
        else:
            await send_telegram(session, "⚠️ 1m Ake grafik chizish uchun shamlar yetarli emas.", chat_id=cid)

    elif cmd in ("/ake_stats", "/ake_stat"):
        msg = await format_ake_stats_table_message()
        recent_ake = await db.get_recent_ake_trades(limit=8)
        if recent_ake:
            msg += "\n\n📜 <b>Oxirgi Ake 1m bitimlar:</b>"
            for r in recent_ake:
                msg += (
                    f"\n • <b>{side_txt(r['signal'])}</b> | {html.escape(str(r['result']))} | "
                    f"<code>{r['entry']:.2f} ➡️ {r['close_price']:.2f}</code> (Rekord: <b>{r['peak_r']:+.1f}R</b>)"
                )
        await send_telegram(session, msg, with_keyboard=True, chat_id=cid)

    elif cmd == "/ake_reset":
        async with _ake_monitor_lock:
            await db.clear_ake_trades()
            await db.state_set("current_trade_ake", None)
        await send_telegram(
            session,
            "🧹 <b>Ake signals 7 (1m) tarixi va R-jadvali 0 ga tozalandi!</b>",
            chat_id=cid,
        )

    elif cmd == "/day_reset":
        async with _monitor_lock:
            await db.clear_day_trades()
            await db.state_set("current_trade", None)
        await send_telegram(
            session,
            "🧹 <b>Day Trading (15m) tarixi va Win Rate statistikasi 0 ga tozalandi!</b>",
            chat_id=cid,
        )

    elif cmd == "/ake_close":
        async with _ake_monitor_lock:
            ake_trade = await db.state_get("current_trade_ake")
            if not ake_trade:
                await send_telegram(session, "📭 Ochiq Ake 1m bitim yo'q.", chat_id=cid)
                return
            cur_p = live_price() or (_LAST_CANDLES_1M[-1][C] if _LAST_CANDLES_1M else ake_trade["entry"])
            await close_ake_trade(session, ake_trade, round(cur_p, 2), reason_label="MANUAL")

    elif cmd in ("/ake_toggle", "/ake_on", "/ake_off"):
        cur_en = await db.state_get("ake_enabled", AKE_ENABLED_DEFAULT)
        new_en = True if cmd == "/ake_on" else (False if cmd == "/ake_off" else (not cur_en))
        await db.state_set("ake_enabled", new_en)
        await send_telegram(
            session,
            f"⚡ <b>Ake Signals 7 (1m Scalping):</b> {'🟢 YOQILDI' if new_en else '🔴 O`CHIRILDI'}",
            chat_id=cid,
        )

    elif cmd in ("/day_toggle", "/day_on", "/day_off"):
        cur_en = await db.state_get("day_enabled", DAY_ENABLED_DEFAULT)
        new_en = True if cmd == "/day_on" else (False if cmd == "/day_off" else (not cur_en))
        await db.state_set("day_enabled", new_en)
        await send_telegram(
            session,
            f"📊 <b>Day Trading (15m/5m SMC+ICT):</b> {'🟢 YOQILDI' if new_en else '🔴 O`CHIRILGAN'}",
            chat_id=cid,
        )

    elif cmd == "/exness_mode":
        if len(parts) >= 2 and parts[1].lower() in ("day", "ake", "both"):
            m_val = parts[1].lower()
            await db.state_set("exness_mode", m_val)
            await send_telegram(
                session,
                f"🏦 <b>Exness avto-savdo rejimi o'zgartirildi:</b> <code>{m_val.upper()}</code>\n"
                "• <code>day</code> — faqat Day Trading (15m) signallarini ochadi\n"
                "• <code>ake</code> — faqat Ake Signals 7 (1m) signallarini ochadi\n"
                "• <code>both</code> — ikkala tizim signallarini ham ochadi",
                chat_id=cid,
            )
        else:
            cur_m = await db.state_get("exness_mode", EXNESS_AUTOTRADE_DEFAULT_MODE)
            await send_telegram(
                session,
                f"🏦 <b>Joriy Exness avto-savdo rejimi:</b> <code>{cur_m.upper()}</code>\n"
                "O'zgartirish: <code>/exness_mode day</code> | <code>/exness_mode ake</code> | <code>/exness_mode both</code>",
                chat_id=cid,
            )

    elif cmd in ("/signal", "/status", "/day"):
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
            f"🎯 <b>Zonalar (narx: {p})</b>",
            f"🟢 <b>BUY zonalari (pastda):</b>",
        ]
        for z in bull_z[:5]:
            dist = p - z["ce"]
            lines.append(f" • <b>{z['kind']}</b>: <code>{z['bottom']:.2f} — {z['top']:.2f}</code> (CE: {z['ce']:.2f} | {dist:+.1f}$)")
        if not bull_z:
            lines.append(" • yo'q")

        lines.append("\n🔴 <b>SELL zonalari (tepada):</b>")
        for z in bear_z[:5]:
            dist = z["ce"] - p
            lines.append(f" • <b>{z['kind']}</b>: <code>{z['bottom']:.2f} — {z['top']:.2f}</code> (CE: {z['ce']:.2f} | +{dist:.1f}$)")
        if not bear_z:
            lines.append(" • yo'q")

        htf_z = status.get("htf_zones") or []
        if htf_z:
            lines.append("\n🏛 <b>1 Oylik HTF (4H/1D) Zonalar:</b>")
            htf_sorted = sorted(htf_z, key=lambda z: abs(p - z["ce"]))[:6]
            for z in htf_sorted:
                icon = "🟢" if z["type"] == "bullish" else "🔴"
                lines.append(
                    f" • {icon} <b>{z.get('tf', '4H')} {z['kind']}</b>: "
                    f"<code>{z['bottom']:.2f} — {z['top']:.2f}</code> (CE: {z['ce']:.2f})"
                )

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

    elif cmd in ("/sync", "/sync_exness", "/sync_xm") and len(parts) >= 2:
        try:
            target_brk = "xm" if cmd == "/sync_xm" else ("exness" if cmd == "/sync_exness" else _ACTIVE_BROKER)
            _ACTIVE_BROKER = target_brk
            await db.state_set("active_broker", _ACTIVE_BROKER)

            broker_p = float(parts[1].replace(",", "."))
            raw_bybit = _LIVE_BYBIT_RAW_PRICE or await fetch_raw_price(session)
            spot_p, _ = await fetch_spot_xauusd_price(session)
            if spot_p:
                _BROKER_CALIBRATION[target_brk] = round(broker_p - spot_p, 2)
                await db.state_set("broker_calibration", _BROKER_CALIBRATION)

            new_off = round(broker_p - raw_bybit, 2)
            delta = await apply_offset_change(new_off)
            await send_telegram(
                session,
                f"🔄 <b>{target_brk.upper()} narxi sinxronlandi va Avto-Sync kalibrlandi!</b>\n"
                f"{target_brk.upper()} narxi: <code>{broker_p:.2f}</code> | Bybit: <code>{raw_bybit:.2f}</code>\n"
                f"Yangi Offset: <code>{_CURRENT_OFFSET:+.2f}</code> (Siljish: <code>{delta:+.2f}$</code>)",
                chat_id=cid,
            )
        except Exception as e:
            await send_telegram(session, f"⚠️ Format: <code>/sync 4135.20</code> yoki <code>/sync_exness 4135.20</code> | <code>/sync_xm 4135.45</code> ({html.escape(str(e))})", chat_id=cid)

    elif cmd == "/broker":
        if len(parts) >= 2 and parts[1].lower() in ("exness", "xm"):
            _ACTIVE_BROKER = parts[1].lower()
            await db.state_set("active_broker", _ACTIVE_BROKER)
            info = await run_auto_sync_once(session, force=True)
            cur_p = get_unified_price()
            await send_telegram(
                session,
                f"🏦 <b>Faol broker o'zgartirildi: {_ACTIVE_BROKER.upper()}</b>\n"
                f"Joriy {_ACTIVE_BROKER.upper()} narxi: <code>{cur_p}</code> (Offset: <code>{_CURRENT_OFFSET:+.2f}</code>)\n"
                f"Exness va XM narxlari avtomatik Spot XAU/USD ga bog'langan.",
                chat_id=cid,
            )
        else:
            ex_p = _LAST_SPOT_SYNC_INFO.get("exness_price")
            xm_p = _LAST_SPOT_SYNC_INFO.get("xm_price")
            await send_telegram(
                session,
                f"🏦 <b>Joriy faol broker: {_ACTIVE_BROKER.upper()}</b>\n"
                f"• Exness narxi: <code>{ex_p}</code> (kalibrlash: <code>/sync_exness NARX</code>)\n"
                f"• XM narxi: <code>{xm_p}</code> (kalibrlash: <code>/sync_xm NARX</code>)\n"
                "Almashtirish: <code>/broker exness</code> yoki <code>/broker xm</code>",
                chat_id=cid,
            )

    elif cmd == "/autosync":
        arg = parts[1].lower() if len(parts) >= 2 else "now"
        if arg in ("on", "1", "true"):
            _AUTO_SYNC_ENABLED = True
            await db.state_set("auto_sync_enabled", True)
            info = await run_auto_sync_once(session, force=True)
            await send_telegram(
                session,
                f"🟢 <b>Avto-Sync YOQILDI!</b> ({_ACTIVE_BROKER.upper()})\n"
                f"Joriy narx: <code>{get_unified_price()}</code> | Offset: <code>{_CURRENT_OFFSET:+.2f}</code>",
                chat_id=cid,
            )
        elif arg in ("off", "0", "false"):
            _AUTO_SYNC_ENABLED = False
            await db.state_set("auto_sync_enabled", False)
            await send_telegram(session, "⚪ <b>Avto-Sync O'CHIRILDI.</b> (Qo'lda /sync ishlaydi)", chat_id=cid)
        else:
            info = await run_auto_sync_once(session, force=True)
            if info:
                await send_telegram(
                    session,
                    f"🔄 <b>Avto-Sync bajarildi ({html.escape(str(info.get('source')))}):</b>\n"
                    f"🏦 Exness: <code>{info.get('exness_price')}</code> | 🏛 XM: <code>{info.get('xm_price')}</code>\n"
                    f"📈 Bybit raw: <code>{info.get('bybit_raw')}</code> | Offset ({_ACTIVE_BROKER.upper()}): <code>{_CURRENT_OFFSET:+.2f}</code>",
                    chat_id=cid,
                )
            else:
                await send_telegram(session, "⚠️ Spot XAU/USD manbasiga ulanib bo'lmadi, joriy offset saqlandi.", chat_id=cid)

    elif cmd == "/metaapi_set":
        if len(parts) < 3:
            await send_telegram(
                session,
                "☁️ <b>Railway & Termux uchun Cloud MT5 Avto-Savdo (MetaApi) ulash:</b>\n"
                "Format: <code>/metaapi_set METAAPI_TOKEN ACCOUNT_ID [SYMBOL] [LOT]</code>\n"
                "Misol (Exness): <code>/metaapi_set eyJhbG... 1a2b3c4d-5678 XAUUSDm 0.01</code>\n"
                "Misol (XM): <code>/metaapi_set eyJhbG... 1a2b3c4d-5678 GOLD 0.01</code>\n\n"
                "💡 <i>MetaApi.cloud da bepul ro'yxatdan o'tib, Exness/XM MT5 login-parolingizni ulaysiz va Token hamda Account ID ni shu yerga yuborasiz — shunda Windows kompyutersiz to'g'ridan-to'g'ri Railway yoki Termux'da 24/7 avto-savdo ishlaydi!</i>",
                chat_id=cid,
            )
            return
        m_tok = parts[1].strip()
        m_acc = parts[2].strip()
        m_sym = parts[3].strip() if len(parts) >= 4 else ("GOLD" if _ACTIVE_BROKER == "xm" else "XAUUSDm")
        m_lot = float(parts[4]) if len(parts) >= 5 else 0.01
        await db.state_set("metaapi_config", {"token": m_tok, "account_id": m_acc, "symbol": m_sym, "lot": m_lot})
        pw_enc = encrypt_secret(m_tok)
        slot = await db.add_or_update_exness_account(cid, m_acc, pw_enc, "METAAPI-CLOUD", label="Cloud MT5", lot=m_lot, symbol=m_sym)
        await send_telegram(
            session,
            f"✅ <b>Cloud MT5 Avto-Savdo (#{slot}-slot) ulandi!</b>\n"
            f"Account ID: <code>{html.escape(m_acc[:12])}...</code> | Simbol: <code>{html.escape(m_sym)}</code> | Lot: <code>{m_lot:.2f}</code>\n"
            f"🔒 Token xabaringiz xavfsizlik uchun chatdan o'chirildi.",
            chat_id=cid,
        )

    elif cmd == "/test_order":
        side_arg = parts[1].upper() if len(parts) >= 2 else "BUY"
        sig_type = "LONG" if side_arg in ("BUY", "LONG") else "SHORT"
        cur_p = get_unified_price(_LAST_CANDLES_1M or _LAST_CANDLES_15M) or 4135.00
        sl_p = round(cur_p - 2.50, 2) if sig_type == "LONG" else round(cur_p + 2.50, 2)
        tp_p = round(cur_p + 5.00, 2) if sig_type == "LONG" else round(cur_p - 5.00, 2)
        test_trade = {
            "engine": "TEST_ORDER",
            "signal": sig_type,
            "entry": cur_p,
            "sl": sl_p,
            "tp1": tp_p,
            "tp2": tp_p,
            "lot": XM_MIN_LOT,
        }
        await execute_exness_autotrades(session, test_trade)
        t_online = (time.time() - _TERMUX_BRIDGE_STATE.get("last_seen", 0)) <= 20
        st_line = "🟢 Termux ulangan — buyruq Termux'ga yuborildi!" if t_online else "⏳ Buyruq navbatga qo'shildi (Termux'da termux_autotrader.py ni yoqing)!"
        await send_telegram(
            session,
            f"🧪 <b>Test Order ({side_txt(sig_type)} 0.01 lot) yuborildi!</b>\n"
            f"Kirish: <code>{cur_p}</code> | SL: <code>{sl_p}</code> | TP: <code>{tp_p}</code>\n"
            f"{st_line}",
            chat_id=cid,
        )

    elif cmd == "/termux":
        t_age = time.time() - _TERMUX_BRIDGE_STATE.get("last_seen", 0)
        t_on = t_age <= 20
        t_status = (
            f"🟢 <b>ULANGAN</b> (Hisob: <code>{_TERMUX_BRIDGE_STATE.get('login')}</code> | "
            f"Server: <code>{_TERMUX_BRIDGE_STATE.get('server')}</code> | "
            f"Narx: <code>{_TERMUX_BRIDGE_STATE.get('bid')}</code> | "
            f"Balans: <b>${_TERMUX_BRIDGE_STATE.get('balance')}</b>)"
            if t_on else "⚪ <b>Hozircha ulanmagan</b> (Termux'da <code>python termux_autotrader.py</code> ni ishga tushiring)"
        )
        await send_telegram(
            session,
            f"📱 <b>Termux Avto-Trade Holati:</b> {t_status}\n\n"
            "🚀 <b>MetaApi.cloud'siz to'g'ridan-to'g'ri Termux'da yoqish:</b>\n"
            "1. Termux'ga <code>termux_autotrader.py</code> faylini yuklang.\n"
            "2. Termux ichida 1 marta o'rnating:\n"
            "<pre>pkg update -y && pkg install x11-repo tur-repo -y\n"
            "pkg install python chromium -y\n"
            "pip install aiohttp selenium</pre>\n"
            "3. Ishga tushiring:\n"
            "<pre>python termux_autotrader.py</pre>\n"
            "4. Birinchi marta ishga tushganda Railway ssilkangizni va Exness/XM Login, Parol, Serveringizni so'raydi va saqlab oladi.\n"
            "5. Tekshirish uchun Telegram'da <code>/test_order buy</code> yuborsangiz, Termux darhol Exness/XM da 0.01 lot ochadi!",
            chat_id=cid,
        )
        return
        msg = (
            "📱 <b>Railway va Android Termux'da Exness/XM Avto-Savdoni Yoqish Qo'llanmasi:</b>\n\n"
            "❓ <b>Nima uchun oddiy MT5 Railway'da ishlamagan?</b>\n"
            "Standard <code>MetaTrader5</code> kutubxonasi faqat Windows (.exe) kompyuterda ishlaydi. Railway (Linux) va Android Termux'da ishlashi uchun botga <b>Cloud MT5 REST API (MetaApi)</b> qo'shildi!\n\n"
            "🚀 <b>1-USUL: To'g'ridan-to'g'ri Railway'da (Telefon ham shart emas, 24/7 ishlaydi):</b>\n"
            "1. <code>app.metaapi.cloud</code> saytida bepul hisob oching.\n"
            "2. Exness yoki XM MT5 hisobingizni (Login, Parol, Server) qo'shing.\n"
            "3. U yerdan <b>API Token</b> va <b>Account ID</b> ni nusxalab, botga yuboring:\n"
            "   <code>/metaapi_set TOKEN ACCOUNT_ID XAUUSDm 0.01</code>\n"
            "4. Avto-savdo rejimini tanlang: <code>/exness_mode both</code> (yoki <code>day</code> / <code>ake</code>).\n\n"
            "📲 <b>2-USUL: Telefon Termux'da botni yoki Bridge'ni ishga tushirish:</b>\n"
            "Termux ichida quyidagi buyruqlarni yozasiz:\n"
            "<pre>pkg update && pkg install python git -y\n"
            "pip install aiohttp aiosqlite cryptography python-dotenv\n"
            "export BOT_TOKEN='TOKENINGIZ'\n"
            "export CHAT_ID='ID_RAQAMINGIZ'\n"
            "python uuf_fixed8.py</pre>"
        )
        await send_telegram(session, msg, chat_id=cid)

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
            await send_telegram(session, f"💰 Balans: <b>${val:.2f}</b>", chat_id=cid)
        except Exception:
            await send_telegram(session, "⚠️ Format: <code>/balance 20</code>", chat_id=cid)

    elif cmd == "/risk" and len(parts) >= 2:
        try:
            val = max(0.5, min(MAX_RISK_PER_TRADE_PCT, float(parts[1].replace(",", "."))))
            await update_account(risk=val)
            await send_telegram(session, f"⚖️ Risk: <b>{val:.1f}%</b>", chat_id=cid)
        except Exception:
            await send_telegram(session, f"⚠️ Format: <code>/risk 10</code> (Max: {MAX_RISK_PER_TRADE_PCT}%)", chat_id=cid)

    elif cmd == "/close":
        async with _monitor_lock:
            trade = await db.state_get("current_trade")
            if not trade:
                await send_telegram(session, "📭 Ochiq bitim yo'q.", chat_id=cid)
                return
            cur_p = live_price() or (_LAST_CANDLES_15M[-1][C] if _LAST_CANDLES_15M else trade["entry"])
            await close_trade(session, trade, "MANUAL", round(cur_p, 2))

    elif cmd == "/stats":
        wr_txt = await win_rate_text()
        recent = await db.get_recent_trades(limit=10)
        uptime_h = (time.time() - _metrics["start"]) / 3600.0
        lines = [
            f"📈 <b>Natijalar</b>",
            f"🏆 {html.escape(wr_txt)}",
            f"💰 Balans: <b>${(await get_account())['balance']:.2f}</b>",
            f"",
            f"📜 <b>Oxirgi 10 ta bitim:</b>",
        ]
        if not recent:
            lines.append(" • Hali yopilgan bitimlar yo'q")
        else:
            for t in recent:
                lines.append(
                    f" • <b>{side_txt(t['signal'])}</b> | {html.escape(result_txt(t['result']))} | "
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
            f"MTF: 5m={status['b5']}, 15m={status['b15']} ({status['event15']}), 1H={status['b1h']}, 4H={status['b4h']}, "
            f"1D={status.get('b1d')}, 1W={status.get('b1w')}, 1M={status.get('b1m')}\n"
            f"15m EQ(50%): {status['eq15']} | OTE: {status['ote_bottom']}-{status['ote_top']}\n"
            f"1 Oylik (30 kun) Diapazon: {status.get('month_low')} - {status.get('month_high')} (1M EQ 50%: {status.get('month_eq')})\n"
            f"Likvidlik: PDH/PDL={status['pdh']}/{status['pdl']} | PWH/PWL={status.get('pwh')}/{status.get('pwl')} | "
            f"PMH/PML={status.get('pmh')}/{status.get('pml')} | EQH/EQL={status['eqh']}/{status['eql']}\n"
            f"Savol: {user_q or 'Hozirgi 1 oylik va kunlik bozor holati, likvidlik qayerda va qaysi zonadan kirish xavfsiz?'}"
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
<title>GOLD SMC v21 LIVE</title>
<script src="https://telegram.org/js/telegram-web-app.js"></script>
<style>
  :root { --bg:#0b0e14; --card:#151922; --gold:#f0b90b; --green:#10b981; --red:#f43f5e; --txt:#e6edf3; --muted:#8b949e; }
  * { box-sizing:border-box; margin:0; padding:0; font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif; }
  body { background:var(--bg); color:var(--txt); padding:10px; font-size:13px; }
  .hdr { display:flex; justify-content:space-between; align-items:center; padding:12px; background:var(--card); border-radius:12px; border:1px solid #262c3a; margin-bottom:10px; }
  .price { font-size:24px; font-weight:800; color:var(--gold); letter-spacing:0.3px; }
  .badge { padding:4px 8px; border-radius:6px; font-size:11px; font-weight:700; background:#21262d; display:inline-block; }
  .tabs { display:flex; gap:6px; margin-bottom:8px; }
  .tab { flex:1; padding:8px 6px; text-align:center; border-radius:8px; background:#1c2230; color:var(--muted); font-weight:700; font-size:12px; cursor:pointer; border:1px solid #262c3a; }
  .tab.active { background:var(--gold); color:#000; border-color:var(--gold); }
  .grid { display:grid; grid-template-columns:1fr 1fr; gap:7px; margin-bottom:10px; }
  .card { background:var(--card); padding:11px; border-radius:12px; border:1px solid #262c3a; margin-bottom:10px; }
  .card h3 { font-size:12px; color:var(--muted); margin-bottom:8px; text-transform:uppercase; display:flex; justify-content:space-between; }
  .btn { width:100%; padding:9px; border:none; border-radius:8px; font-weight:700; cursor:pointer; color:#fff; background:#10b981; font-size:12px; }
  .btn.gold { background:var(--gold); color:#000; }
  .btn.red { background:var(--red); }
  .btn.sec { background:#1f2637; color:var(--txt); border:1px solid #2e374f; }
  input { width:100%; padding:8px; border-radius:8px; border:1px solid #2e374f; background:#0b0e14; color:var(--txt); margin-bottom:7px; font-size:13px; }
  .row { display:flex; justify-content:space-between; padding:4px 0; border-bottom:1px solid #1f2430; }
  canvas { width:100%; height:240px; display:block; border-radius:8px; background:#0f131c; border:1px solid #232a3b; }
</style>
</head>
<body>
  <div class="hdr">
    <div>
      <div style="font-size:11px;color:var(--muted)" id="brk_label">XAUUSD (EXNESS AVTO-SYNC)</div>
      <div class="price" id="price">...</div>
      <div style="font-size:11px;color:var(--muted);margin-top:2px" id="sub_prices">Exness: ... | XM: ...</div>
    </div>
    <div style="text-align:right">
      <div class="badge" style="background:#10b98122;color:#10b981;border:1px solid #10b98155" id="bal_badge">💰 $20.00</div>
      <div style="font-size:11px;color:var(--muted);margin-top:5px" id="bias">15m: ...</div>
      <div style="font-size:11px;color:var(--muted);margin-top:2px" id="offset">Offset: ...</div>
    </div>
  </div>

  <div class="tabs">
    <div class="tab active" id="tab_1m" onclick="setChartMode('1m')">⚡ 1m Ake Live</div>
    <div class="tab" id="tab_15m" onclick="setChartMode('15m')">📊 15m Day Live</div>
    <div class="tab" id="tab_tv" onclick="setChartMode('tv')">🌐 TradingView</div>
  </div>

  <div class="card" style="padding:8px">
    <canvas id="live_canvas" width="720" height="360"></canvas>
    <div id="tv_box" style="display:none;height:260px;border-radius:8px;overflow:hidden">
      <iframe src="https://s.tradingview.com/widgetembed/?symbol=OANDA%3AXAUUSD&interval=1&theme=dark&style=1&timezone=Asia%2FTashkent"
              style="width:100%;height:260px;border:0" allowfullscreen></iframe>
    </div>
    <div style="display:flex;gap:6px;margin-top:7px">
      <button class="btn sec" id="btn_ex" onclick="setBroker('exness')">🏦 Exness</button>
      <button class="btn sec" id="btn_xm" onclick="setBroker('xm')">🏛 XM</button>
      <button class="btn gold" onclick="act('autosync')">🔄 Avto-Sync</button>
    </div>
  </div>

  <div class="card">
    <h3><span>⚡ AKE SIGNALS 7 (1m Scalping 1R–5R)</span><span id="ake_work"></span></h3>
    <div id="ake_box" style="margin-bottom:8px">Yuklanmoqda...</div>
    <div id="ake_stats_box" style="font-size:12px;color:var(--muted);margin-bottom:8px"></div>
    <div class="grid" style="margin-bottom:0">
      <button class="btn gold" onclick="act('ake')">⚡ Ake Signal</button>
      <button class="btn sec" onclick="act('ake_chart')">📉 1m Grafik</button>
      <button class="btn sec" onclick="act('ake_stats')">📋 R-Jadval</button>
      <button class="btn red" onclick="act('ake_close')">🛑 Ake Yopish</button>
    </div>
  </div>

  <div class="card">
    <h3><span>📊 DAY TRADING (15m/5m SMC+ICT)</span><span>1:3.5 RR</span></h3>
    <div id="trade_box" style="margin-bottom:8px">Yuklanmoqda...</div>
    <div class="grid" style="margin-bottom:0">
      <button class="btn gold" onclick="act('signal')">📊 Day Signal</button>
      <button class="btn sec" onclick="act('chart')">🖼 15m Grafik</button>
      <button class="btn sec" onclick="act('ai')">🧠 Gemini AI</button>
      <button class="btn red" onclick="act('close')">🛑 Day Yopish</button>
    </div>
  </div>

  <div class="card">
    <h3>🔄 Broker Narxini Aniq Kalibrlash (/sync)</h3>
    <div style="display:flex;gap:8px">
      <input type="number" step="0.01" id="sync_p" placeholder="Exness/XM narxi (masalan 4135.20)" style="margin:0">
      <button class="btn gold" style="width:110px" onclick="syncPrice()">Saqlash</button>
    </div>
  </div>

  <div class="card">
    <h3>🏦 Exness / XM Cloud MT5 Hisoblar</h3>
    <div id="exness_list" style="margin-bottom:8px">...</div>
    <input id="ex_login" placeholder="MT5 Login yoki MetaApi Account ID">
    <input id="ex_pass" type="password" placeholder="MT5 Parol yoki MetaApi Token">
    <div style="display:flex;gap:8px">
      <input id="ex_srv" placeholder="Server (AUTO yoki METAAPI)" value="AUTO">
      <input id="ex_lot" type="number" step="0.01" placeholder="Lot" value="0.01">
    </div>
    <button class="btn" onclick="addExness()">➕ MT5 Hisob Ulash</button>
  </div>

<script>
const tg = window.Telegram && window.Telegram.WebApp ? window.Telegram.WebApp : null;
if (tg) { tg.expand(); }
const initData = tg ? tg.initData : "";
const urlToken = new URLSearchParams(location.search).get("token") || "";
let chartMode = "1m";
let lastData = null;

function setChartMode(m) {
  chartMode = m;
  document.getElementById("tab_1m").className = "tab" + (m === "1m" ? " active" : "");
  document.getElementById("tab_15m").className = "tab" + (m === "15m" ? " active" : "");
  document.getElementById("tab_tv").className = "tab" + (m === "tv" ? " active" : "");
  document.getElementById("live_canvas").style.display = (m === "tv") ? "none" : "block";
  document.getElementById("tv_box").style.display = (m === "tv") ? "block" : "none";
  if (lastData && m !== "tv") drawLiveChart(lastData);
}

async function api(path, body=null) {
  const opts = { headers: { "Content-Type":"application/json", "X-Telegram-Init-Data": initData, "X-Dashboard-Token": urlToken } };
  if (body) { opts.method = "POST"; opts.body = JSON.stringify(body); }
  const r = await fetch(path, opts);
  return await r.json();
}

function drawLiveChart(d) {
  const cvs = document.getElementById("live_canvas");
  if (!cvs) return;
  const ctx = cvs.getContext("2d");
  const W = cvs.width, H = cvs.height;
  ctx.fillStyle = "#0f131c";
  ctx.fillRect(0, 0, W, H);

  const candles = (chartMode === "1m" ? d.candles_1m : d.candles_15m) || [];
  if (candles.length < 5) {
    ctx.fillStyle = "#8b949e";
    ctx.font = "16px sans-serif";
    ctx.fillText("Shamlar yuklanmoqda...", 260, 180);
    return;
  }

  const activeTrade = (chartMode === "1m") ? d.ake_trade : d.trade;
  let pMax = -1e9, pMin = 1e9;
  for (const c of candles) {
    if (c[2] > pMax) pMax = c[2];
    if (c[3] < pMin) pMin = c[3];
  }
  if (activeTrade) {
    const lvls = [activeTrade.entry, activeTrade.sl, activeTrade.tp1, activeTrade.tp2, activeTrade.tp3, activeTrade.tp4, activeTrade.tp5];
    for (const v of lvls) {
      if (v) {
        if (v > pMax) pMax = v;
        if (v < pMin) pMin = v;
      }
    }
  }
  const pad = Math.max((pMax - pMin) * 0.10, 0.8);
  pMax += pad; pMin -= pad;

  const ml = 12, mr = 86, mt = 28, mb = 18;
  const pw = W - ml - mr, ph = H - mt - mb;
  const toY = (p) => mt + ((pMax - p) / Math.max(pMax - pMin, 0.01)) * ph;

  // Grid
  ctx.strokeStyle = "#1c2333";
  ctx.lineWidth = 1;
  ctx.fillStyle = "#8b949e";
  ctx.font = "11px monospace";
  for (let i = 0; i <= 5; i++) {
    const gp = pMin + (pMax - pMin) * (i / 5);
    const gy = toY(gp);
    ctx.beginPath(); ctx.moveTo(ml, gy); ctx.lineTo(W - mr, gy); ctx.stroke();
    ctx.fillText(gp.toFixed(2), W - mr + 6, gy + 4);
  }

  const totalSlots = candles.length + (activeTrade ? 8 : 2);
  const stepX = pw / totalSlots;
  const cw = Math.max(3, stepX * 0.65);

  // Candlesticks
  candles.forEach((c, i) => {
    const cx = ml + (i + 0.5) * stepX;
    const yo = toY(c[1]), yh = toY(c[2]), yl = toY(c[3]), yc = toY(c[4]);
    const bull = c[4] >= c[1];
    ctx.strokeStyle = bull ? "#10b981" : "#f43f5e";
    ctx.fillStyle = bull ? "#10b981" : "#f43f5e";
    ctx.beginPath(); ctx.moveTo(cx, yh); ctx.lineTo(cx, yl); ctx.stroke();
    const top = Math.min(yo, yc), bot = Math.max(yo, yc);
    ctx.fillRect(cx - cw / 2, top, cw, Math.max(1.5, bot - top));
  });

  // Draw Trade Levels (Entry, SL, TP1..TP5)
  if (activeTrade) {
    const drawLvl = (val, lbl, col) => {
      if (!val || val < pMin || val > pMax) return;
      const y = toY(val);
      ctx.strokeStyle = col;
      ctx.setLineDash([4, 3]);
      ctx.beginPath(); ctx.moveTo(ml, y); ctx.lineTo(W - mr, y); ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = col;
      ctx.fillText(lbl + " " + val.toFixed(2), ml + 6, y - 4);
    };
    drawLvl(activeTrade.entry, "E", "#f0b90b");
    drawLvl(activeTrade.sl, "SL", "#f43f5e");
    drawLvl(activeTrade.tp1, "TP1", "#10b981");
    if (chartMode === "1m") {
      drawLvl(activeTrade.tp2, "TP2", "#10b981");
      drawLvl(activeTrade.tp3, "TP3", "#10b981");
      drawLvl(activeTrade.tp4, "TP4", "#10b981");
      drawLvl(activeTrade.tp5, "TP5", "#10b981");
    }
  }

  // Live price horizontal line
  if (d.price) {
    const py = toY(d.price);
    ctx.strokeStyle = "#f0b90b";
    ctx.lineWidth = 1.2;
    ctx.beginPath(); ctx.moveTo(ml, py); ctx.lineTo(W - mr, py); ctx.stroke();
    ctx.fillStyle = "#f0b90b";
    ctx.fillRect(W - mr + 2, py - 9, 80, 18);
    ctx.fillStyle = "#000";
    ctx.font = "bold 11px monospace";
    ctx.fillText(d.price.toFixed(2), W - mr + 8, py + 4);
  }

  ctx.fillStyle = "#e6edf3";
  ctx.font = "bold 12px sans-serif";
  ctx.fillText((d.broker || "EXNESS").toUpperCase() + " XAUUSD " + chartMode.toUpperCase() + " LIVE", ml + 4, 18);
}

async function loadState() {
  try {
    const d = await api("/api/state");
    if (!d.ok) return;
    lastData = d;
    document.getElementById("price").innerText = d.price ? d.price.toFixed(2) : "---";
    document.getElementById("brk_label").innerText = `XAUUSD (${(d.broker || 'exness').toUpperCase()} ${d.auto_sync ? '🟢 AVTO-SYNC' : '⚪ MANUAL'})`;
    document.getElementById("sub_prices").innerText = `Exness: ${d.exness_price ? d.exness_price.toFixed(2) : '-'} | XM: ${d.xm_price ? d.xm_price.toFixed(2) : '-'}`;
    document.getElementById("bal_badge").innerText = `💰 $${(d.balance || 20).toFixed(2)}`;
    document.getElementById("bias").innerText = "15m: " + (d.b15 || "NEUTRAL").toUpperCase();
    document.getElementById("offset").innerText = "Offset: " + (d.offset >= 0 ? "+" : "") + d.offset;

    document.getElementById("btn_ex").style.borderColor = (d.broker === "exness") ? "#f0b90b" : "#2e374f";
    document.getElementById("btn_xm").style.borderColor = (d.broker === "xm") ? "#f0b90b" : "#2e374f";

    if (chartMode !== "tv") drawLiveChart(d);

    const tb = document.getElementById("trade_box");
    if (d.trade) {
      tb.innerHTML = `<div class="row"><span>Day Bitim:</span><b>${d.trade.signal === "LONG" ? "🟢 BUY" : "🔴 SELL"} @ ${d.trade.entry}</b></div>
                      <div class="row"><span>SL / TP:</span><b>${d.trade.sl} / ${d.trade.tp1}</b></div>`;
    } else {
      tb.innerHTML = `<div class="row"><span>Day Holat:</span><span>15m Sniper zona kutilmoqda</span></div>
                      <div class="row"><span>EQ (50%) / WR:</span><b>${d.eq15 || '-'} | ${d.wr}</b></div>`;
    }
    const ab = document.getElementById("ake_box");
    if (d.ake_trade) {
      document.getElementById("ake_work").innerText = "🟢 OCHIQ";
      ab.innerHTML = `<div class="row"><span>Ake 1m Bitim:</span><b>${d.ake_trade.signal === "LONG" ? "🟢 LONG" : "🔴 SHORT"} @ ${d.ake_trade.entry} (Max: ${d.ake_trade.max_r_hit || 0}R)</b></div>
                      <div class="row"><span>SL(-1R) / TP1..TP5:</span><b>${d.ake_trade.sl} / ${d.ake_trade.tp1} .. ${d.ake_trade.tp5}</b></div>`;
    } else {
      document.getElementById("ake_work").innerText = "⏳ KUTILMOQDA";
      ab.innerHTML = `<div class="row"><span>Ake 1m Holat:</span><span>1m 2-bosqichli tasdiq kutilmoqda</span></div>`;
    }
    if (d.ake_stats && d.ake_stats.day) {
      const ds = d.ake_stats.day, ws = d.ake_stats.week, ms = d.ake_stats.month;
      document.getElementById("ake_stats_box").innerHTML =
        `<div class="row"><span>Kun (${ds.total}):</span><b>TP ${ds.wins} (${ds.wr}%) | Stop ${ds.losses} | Jami ${ds.total_r >= 0 ? '+' : ''}${ds.total_r}R</b></div>` +
        `<div class="row"><span>Hafta (${ws.total}):</span><b>TP ${ws.wins} (${ws.wr}%) | Jami ${ws.total_r >= 0 ? '+' : ''}${ws.total_r}R</b></div>` +
        `<div class="row"><span>Oy (${ms.total}):</span><b>TP ${ms.wins} (${ms.wr}%) | Jami ${ms.total_r >= 0 ? '+' : ''}${ms.total_r}R</b></div>`;
    }
    const el = document.getElementById("exness_list");
    el.innerHTML = (d.exness || []).map(a =>
      `<div class="row"><span>#${a.slot} <b>${a.login}</b> (${a.server})</span>
       <span>${a.lot} lot | <a href="#" style="color:var(--gold)" onclick="delEx(${a.slot})">O'chirish</a></span></div>`
    ).join("") || "Hali hisob ulanmagan (/metaapi_set yoki pastdan ulang)";
  } catch (e) {}
}

async function act(cmd) {
  await api("/api/action", { action: cmd });
  if (tg) tg.showAlert("Buyruq bajarildi: " + cmd);
  loadState();
}

async function setBroker(brk) {
  await api("/api/action", { action: "set_broker", broker: brk });
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
setInterval(loadState, 2500);
</script>
</body>
</html>
"""

# ==================== WEB SERVER & CLOUD BRIDGE ENDPOINTS ====================
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
    ake_trade = await db.state_get("current_trade_ake")
    ake_stats = await get_ake_stats_summary()
    acc = await get_account()
    ex_accs = await db.get_exness_accounts(cid)
    wr = await win_rate_text()
    p = get_unified_price(_LAST_CANDLES_1M or _LAST_CANDLES_15M)

    c1_out = [list(c[:5]) for c in (_LAST_CANDLES_1M[-48:] if _LAST_CANDLES_1M else [])]
    if c1_out and p is not None:
        c1_out[-1][4] = p
        if p > c1_out[-1][2]:
            c1_out[-1][2] = p
        if p < c1_out[-1][3]:
            c1_out[-1][3] = p

    c15_out = [list(c[:5]) for c in (_LAST_CANDLES_15M[-48:] if _LAST_CANDLES_15M else [])]
    if c15_out and p is not None:
        c15_out[-1][4] = p
        if p > c15_out[-1][2]:
            c15_out[-1][2] = p
        if p < c15_out[-1][3]:
            c15_out[-1][3] = p

    return web.json_response({
        "ok": True,
        "price": p,
        "broker": _ACTIVE_BROKER,
        "auto_sync": _AUTO_SYNC_ENABLED,
        "exness_price": _LAST_SPOT_SYNC_INFO.get("exness_price") or p,
        "xm_price": _LAST_SPOT_SYNC_INFO.get("xm_price") or (round(p + 0.25, 2) if p else None),
        "bybit_raw": _LIVE_BYBIT_RAW_PRICE,
        "offset": _CURRENT_OFFSET,
        "balance": acc.get("balance", DEFAULT_BALANCE_USD),
        "b15": _LAST_STRUCT_15M.get("bias"),
        "eq15": _LAST_STRUCT_15M.get("eq"),
        "scalp_band": _LAST_AKE_INFO.get("scalp_band"),
        "candles_1m": c1_out,
        "candles_15m": c15_out,
        "trade": trade,
        "ake_trade": ake_trade,
        "ake_stats": ake_stats,
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

    if action in ("signal", "chart", "ai", "close", "ake", "ake_chart", "ake_stats", "ake_close", "autosync"):
        await handle_command(session, cid, f"/{action}")
        return web.json_response({"ok": True, "msg": f"/{action} yuborildi"})

    if action == "set_broker":
        brk = str(data.get("broker", "exness")).lower()
        await handle_command(session, cid, f"/broker {brk}")
        return web.json_response({"ok": True, "broker": _ACTIVE_BROKER})

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
    """Termux Auto-Trader (termux_autotrader.py) ushbu endpoint orqali yangi signallar va SL/Close buyruqlarini oladi."""
    token = request.headers.get("X-Bridge-Token") or request.query.get("token", "")
    if DASHBOARD_TOKEN and token != DASHBOARD_TOKEN:
        return web.json_response({"ok": False, "error": "Invalid token"}, status=403)

    now_s = int(time.time())
    # Faqat oxirgi 10 daqiqa ichidagi va hali bajarilmagan (pending) buyruqlarni yuboramiz
    pending = [
        item for item in _BRIDGE_SIGNALS_QUEUE
        if item.get("status") == "pending" and (now_s - int(item.get("ts", 0))) <= 600
    ]
    trade_day = await db.state_get("current_trade")
    trade_ake = await db.state_get("current_trade_ake")
    ex_mode = await db.state_get("exness_mode", EXNESS_AUTOTRADE_DEFAULT_MODE)

    # Agar foydalanuvchi Telegram'da /exness_add orqali hisob qo'shgan bo'lsa, Termux avtomatik olishi uchun:
    saved_accs = []
    for a in await db.get_all_active_exness_accounts():
        try:
            pw_plain = decrypt_secret(a["password_enc"])
        except Exception:
            pw_plain = ""
        saved_accs.append({
            "slot": a["slot"],
            "login": a["login"],
            "password": pw_plain,
            "server": a["server"],
            "symbol": a["symbol"],
            "lot": a["lot"],
        })

    return web.json_response({
        "ok": True,
        "exness_mode": ex_mode,
        "active_broker": _ACTIVE_BROKER,
        "active_trade_day": trade_day,
        "active_trade_ake": trade_ake,
        "pending_commands": pending,
        "recent_signals": _BRIDGE_SIGNALS_QUEUE[-10:],
        "saved_accounts": saved_accs,
        "server_time": now_s,
    })

async def handle_mt5_bridge_heartbeat(request):
    """Termux Auto-Trader har 2 soniyada Exness/XM dagi jonli narx va balansni shu yerga yuboradi."""
    global _LIVE_PRICE, _LIVE_PRICE_TS, _LAST_SPOT_SYNC_INFO
    token = request.headers.get("X-Bridge-Token") or request.query.get("token", "")
    if DASHBOARD_TOKEN and token != DASHBOARD_TOKEN:
        return web.json_response({"ok": False, "error": "Invalid token"}, status=403)

    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "Bad JSON"}, status=400)

    was_offline = (time.time() - _TERMUX_BRIDGE_STATE.get("last_seen", 0)) > 60
    _TERMUX_BRIDGE_STATE["online"] = True
    _TERMUX_BRIDGE_STATE["last_seen"] = time.time()
    for k in ("login", "server", "symbol", "driver"):
        if data.get(k) is not None:
            _TERMUX_BRIDGE_STATE[k] = data.get(k)

    bid = data.get("bid")
    ask = data.get("ask")
    bal = data.get("balance")

    if bid is not None and ask is not None:
        try:
            b_val = float(bid)
            a_val = float(ask)
            if b_val > 1000 and a_val > 1000:
                _TERMUX_BRIDGE_STATE["bid"] = round(b_val, 2)
                _TERMUX_BRIDGE_STATE["ask"] = round(a_val, 2)
                mid_p = round((b_val + a_val) / 2.0, 2)
                _LIVE_PRICE = mid_p
                _LIVE_PRICE_TS = time.monotonic()
                if _LIVE_BYBIT_RAW_PRICE:
                    new_off = round(mid_p - _LIVE_BYBIT_RAW_PRICE, 2)
                    if abs(new_off - _CURRENT_OFFSET) >= 0.08:
                        await apply_offset_change(new_off)
        except Exception:
            pass

    if bal is not None:
        try:
            bal_f = round(float(bal), 2)
            if bal_f >= 0.5:
                _TERMUX_BRIDGE_STATE["balance"] = bal_f
                await update_account(balance=bal_f)
        except Exception:
            pass

    if was_offline and data.get("login"):
        session = request.app["session"]
        _spawn(send_telegram(
            session,
            f"📱 <b>Termux Avto-Trade ulandi!</b>\n"
            f"Hisob: <code>{html.escape(str(data.get('login')))}</code> ({html.escape(str(data.get('server', 'MT5')))})\n"
            f"Simbol: <code>{html.escape(str(data.get('symbol', 'XAUUSDm')))}</code> | "
            f"Jonli narx: <code>{_TERMUX_BRIDGE_STATE.get('bid') or '-'}</code> | "
            f"Balans: <b>${_TERMUX_BRIDGE_STATE.get('balance') or '-'}</b>",
        ))

    return web.json_response({"ok": True, "unified_price": get_unified_price()})

async def handle_mt5_bridge_report(request):
    """Termux Auto-Trader order ochganda, SL o'zgartirganda yoki yopganda natijani shu yerga yuboradi."""
    token = request.headers.get("X-Bridge-Token") or request.query.get("token", "")
    if DASHBOARD_TOKEN and token != DASHBOARD_TOKEN:
        return web.json_response({"ok": False, "error": "Invalid token"}, status=403)

    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "Bad JSON"}, status=400)

    cmd_id = data.get("id")
    for item in _BRIDGE_SIGNALS_QUEUE:
        if item.get("id") == cmd_id:
            item["status"] = "done" if data.get("ok") else "error"
            break

    session = request.app["session"]
    act = data.get("action", "OPEN")
    ok = data.get("ok", False)
    login = data.get("login", "MT5")
    msg_txt = data.get("msg", "")
    price = data.get("price")
    sl = data.get("sl")
    tp = data.get("tp")
    lot = data.get("lot", 0.01)
    sig = data.get("signal", "LONG")
    bal = data.get("balance")

    if bal is not None:
        try:
            if float(bal) >= 0.5:
                await update_account(balance=round(float(bal), 2))
        except Exception:
            pass

    if ok:
        if act == "OPEN":
            await send_telegram(
                session,
                f"✅ <b>[Termux MT5 Auto-Trade] {side_txt(sig)} ochildi!</b>\n"
                f"Hisob: <code>{html.escape(str(login))}</code> | Lot: <code>{lot}</code>\n"
                f"Narx: <code>{price}</code> | SL: <code>{sl}</code> | TP: <code>{tp}</code>",
            )
        elif act == "MODIFY_SL":
            await send_telegram(
                session,
                f"🛡 <b>[Termux MT5] SL muvaffaqiyatli ko'chirildi ➡️ <code>{sl}</code></b>",
            )
        elif act == "CLOSE":
            await send_telegram(
                session,
                f"🏁 <b>[Termux MT5] Ochiq bitim yopildi!</b>" + (f" | 💰 Balans: <b>${float(bal):.2f}</b>" if bal else ""),
            )
    else:
        await send_telegram(
            session,
            f"⚠️ <b>[Termux MT5 Xato ({html.escape(str(act))})]:</b> {html.escape(str(msg_txt))}",
        )

    return web.json_response({"ok": True})

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
    global _CURRENT_OFFSET, _CURRENT_GEMINI_MODEL, _ACTIVE_BROKER, _AUTO_SYNC_ENABLED, _BROKER_CALIBRATION
    await db.init()
    saved_off = await db.state_get("price_offset")
    if saved_off is not None:
        _CURRENT_OFFSET = float(saved_off)
    saved_brk = await db.state_get("active_broker")
    if saved_brk in ("exness", "xm"):
        _ACTIVE_BROKER = saved_brk
    saved_as = await db.state_get("auto_sync_enabled")
    if saved_as is not None:
        _AUTO_SYNC_ENABLED = bool(saved_as)
    saved_cal = await db.state_get("broker_calibration")
    if isinstance(saved_cal, dict):
        _BROKER_CALIBRATION.update(saved_cal)
    saved_mod = await db.state_get("gemini_model")
    if saved_mod and _GEMINI_NAME_RE.fullmatch(str(saved_mod)):
        _CURRENT_GEMINI_MODEL = str(saved_mod)

    app["session"] = aiohttp.ClientSession()
    app["ws_task"] = asyncio.create_task(bybit_ws_loop(app))
    app["autosync_task"] = asyncio.create_task(auto_sync_loop(app))
    app["scanner_task"] = asyncio.create_task(scanner_loop(app))
    if not WEBHOOK_ENABLED:
        app["poll_task"] = asyncio.create_task(telegram_polling_loop(app))

    logger.info(f"🚀 GOLD SMC v19 ULTIMATE tayyor! Offset: {_CURRENT_OFFSET:+.2f} | Model: {_CURRENT_GEMINI_MODEL}")

async def on_cleanup(app):
    for task_name in ("ws_task", "autosync_task", "scanner_task", "poll_task"):
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

async def handle_index(request):
    html_path = os.path.join(BASE_DIR, "webapp", "index.html")
    if os.path.exists(html_path):
        return web.FileResponse(html_path)
    return web.Response(text=MINI_APP_HTML, content_type="text/html")

def create_app():
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_get("/webapp", handle_index)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/api/state", handle_api_state)
    app.router.add_post("/api/action", handle_api_action)
    app.router.add_get("/api/mt5_bridge/poll", handle_mt5_bridge_poll)
    app.router.add_post("/api/mt5_bridge/heartbeat", handle_mt5_bridge_heartbeat)
    app.router.add_post("/api/mt5_bridge/report", handle_mt5_bridge_report)
    app.router.add_post("/webhook", handle_webhook)

    webapp_dir = os.path.join(BASE_DIR, "webapp")
    if os.path.exists(webapp_dir):
        app.router.add_static("/", webapp_dir)

    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    return app

app = create_app()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    web.run_app(app, host="0.0.0.0", port=port)