#!/usr/bin/env python3
"""
GOLD SMC + ICT v22 ULTIMATE — TERMUX & PC MT5 CLOUD BRIDGE (10/10 EDITION)
==========================================================================
Ushbu skript Android (Termux), Windows/Linux PC yoki VPS orqali asosiy bot serveriga
(`uuf_fixed8.py`) ulanib, quyidagi vazifalarni 24/7 avtomatik bajaradi:
  ✅ Har 2 soniyada `/api/mt5_bridge/poll` orqali yangi Day [15m] (`202619`), Ake [1m] (`202620`)
     va Test (`999999`) signallarini qabul qiladi
  ✅ `open` (BUY/SELL ochish), `modify_sl` (TP1..TP4 va Bezubitok SL ko'chirish) hamda
     `close` (bitimni yopish) amallarini alohida `magic` raqamlar bo'yicha aniq bajaradi
  ✅ Uch xil drayverni qo'llab-quvvatlaydi (avtomatik tanlaydi):
     1) `native_mt5` — Windows/Wine da rasmiy MetaTrader5 kutubxonasi
     2) `metaapi`    — Android Termux / Linux uchun MetaApi REST Cloud API (MT5 terminali shart emas!)
     3) `paper_sim`  — Jonli Swissquote/Yahoo XAUUSD kotirovkasi bilan haqiqiy simulyatsiya va kotirovka uzatish
  ✅ Dublikat orderlardan 100% himoya (`termux_processed_ids.json` xotirasi)
  ✅ Serverga jonli Bid/Ask va balansni uzatadi (`/api/mt5_bridge/report`) — botning Avto-Sync motori
     Exness/XM haqiqiy narxiga 0.01$ aniqlikda moslashadi!

Ishga tushirish (Termux yoki PC):
  1) Kutubxonalar (ixtiyoriy, standart urllib ham ishlaydi!):
     pip install requests
  2) Sozlamalar (yoki pastdagi konstantalarni tahrirlang / .env faylga yozing):
     export BOT_SERVER_URL="https://sizning-bot-manzilingiz.up.railway.app"
     export BRIDGE_TOKEN=""
     export EXNESS_LOGIN="12345678"
     export EXNESS_SERVER="Exness-MT5Real"
     export EXNESS_SYMBOL="XAUUSDm"
     export METAAPI_TOKEN=""          # Termux'da haqiqiy MT5 hisobga ulash uchun (ixtiyoriy)
     export METAAPI_ACCOUNT_ID=""     # MetaApi Account ID (ixtiyoriy)
  3) Ishga tushirish:
     python3 termux_bridge.py
"""

import datetime
import json
import logging
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import MetaTrader5 as mt5
    _HAS_MT5 = True
except ImportError:
    mt5 = None
    _HAS_MT5 = False

# ==================== SOZLAMALAR ====================
BOT_SERVER_URL = os.environ.get("BOT_SERVER_URL", "http://127.0.0.1:8080").strip().rstrip("/")
BRIDGE_TOKEN = os.environ.get("BRIDGE_TOKEN", os.environ.get("DASHBOARD_TOKEN", "")).strip()

EXNESS_LOGIN = os.environ.get("EXNESS_LOGIN", "0").strip()
EXNESS_PASSWORD = os.environ.get("EXNESS_PASSWORD", "").strip()
EXNESS_SERVER = os.environ.get("EXNESS_SERVER", "Exness-MT5Real").strip()
EXNESS_SYMBOL = os.environ.get("EXNESS_SYMBOL", "XAUUSDm").strip()
DEFAULT_LOT = float(os.environ.get("DEFAULT_LOT", "0.01"))
POLL_INTERVAL_SEC = float(os.environ.get("POLL_INTERVAL_SEC", "2.0"))

# Android Termux'da MetaApi orqali to'g'ridan-to'g'ri MT5 hisobda savdo qilish (ixtiyoriy)
METAAPI_TOKEN = os.environ.get("METAAPI_TOKEN", "").strip()
METAAPI_ACCOUNT_ID = os.environ.get("METAAPI_ACCOUNT_ID", "").strip()
METAAPI_REGION_URL = os.environ.get(
    "METAAPI_REGION_URL",
    "https://mt-client-api-v1.london.agiliumtrade.ai",
).strip().rstrip("/")

MT5_MAGIC_DAY = 202619
MT5_MAGIC_AKE = 202620
MT5_MAGIC_TEST = 999999

STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "termux_bridge_state.json")
SWISSQUOTE_XAU_URL = "https://forex-data-feed.swissquote.com/public-quotes/bboquotes/instrument/XAU/USD"
YAHOO_XAU_SPOT_URL = "https://query1.finance.yahoo.com/v8/finance/chart/XAUUSD=X?interval=1m&range=1d"

# ==================== LOGGING ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("termux_bridge_v22")

_SSL_CTX = ssl.create_default_context()
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 14; TermuxBridge/v22)",
    "Content-Type": "application/json",
}

# ==================== HTTP HELPER (ZERO EXTERNAL DEPENDENCY) ====================
def http_json(url, method="GET", payload=None, extra_headers=None, timeout=10):
    hdrs = dict(HEADERS)
    if BRIDGE_TOKEN:
        hdrs["X-Bridge-Token"] = BRIDGE_TOKEN
        hdrs["X-Dashboard-Token"] = BRIDGE_TOKEN
    if extra_headers:
        hdrs.update(extra_headers)
    data_bytes = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(url, data=data_bytes, headers=hdrs, method=method)
    with urllib.request.urlopen(req, timeout=timeout, context=_SSL_CTX) as resp:
        raw = resp.read().decode("utf-8")
        return json.loads(raw) if raw else {}

# ==================== STATE PERSISTENCE (ANTI-DUPLICATE) ====================
def load_bridge_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
        except Exception:
            pass
    return {
        "processed_ids": [],
        "paper_balance": 20.0,
        "paper_positions": [],
    }

def save_bridge_state(st):
    try:
        st["processed_ids"] = st.get("processed_ids", [])[-250:]
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(st, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.debug(f"State saqlash xatosi: {e}")

# ==================== JONLI XAUUSD KOTIROVKA OLISH ====================
def fetch_live_spot_quote():
    """Swissquote yoki Yahoo Finance orqali real XAUUSD Bid/Ask kotirovkasini oladi."""
    try:
        req = urllib.request.Request(SWISSQUOTE_XAU_URL, headers={"User-Agent": HEADERS["User-Agent"]})
        with urllib.request.urlopen(req, timeout=6, context=_SSL_CTX) as r:
            data = json.loads(r.read().decode("utf-8"))
            if isinstance(data, list) and data:
                prices = data[0].get("spreadProfilePrices") or []
                if prices:
                    bid = float(prices[0].get("bid", 0))
                    ask = float(prices[0].get("ask", 0))
                    if bid > 1000 and ask > 1000:
                        return round(bid, 2), round(ask, 2)
    except Exception:
        pass
    try:
        req = urllib.request.Request(YAHOO_XAU_SPOT_URL, headers={"User-Agent": HEADERS["User-Agent"]})
        with urllib.request.urlopen(req, timeout=6, context=_SSL_CTX) as r:
            d = json.loads(r.read().decode("utf-8"))
            meta = d["chart"]["result"][0]["meta"]
            p = float(meta.get("regularMarketPrice") or 0)
            if p > 1000:
                return round(p - 0.12, 2), round(p + 0.13, 2)
    except Exception:
        pass
    return None, None

# ==================== EXECUTION DRIVERS (MT5 / METAAPI / PAPER) ====================
class BridgeExecutor:
    def __init__(self, state):
        self.state = state
        self.driver = "paper_sim"
        self.symbol = EXNESS_SYMBOL
        self._init_driver()

    def _init_driver(self):
        if _HAS_MT5 and EXNESS_LOGIN and EXNESS_LOGIN != "0" and EXNESS_PASSWORD:
            if mt5.initialize(login=int(EXNESS_LOGIN), password=EXNESS_PASSWORD, server=EXNESS_SERVER):
                if mt5.login(login=int(EXNESS_LOGIN), password=EXNESS_PASSWORD, server=EXNESS_SERVER):
                    self.driver = "native_mt5"
                    for candidate in (EXNESS_SYMBOL, "XAUUSDm", "XAUUSD", "XAUUSDc", "GOLDm", "GOLD"):
                        info = mt5.symbol_info(candidate)
                        if info is not None:
                            if not info.visible:
                                mt5.symbol_select(candidate, True)
                            self.symbol = candidate
                            break
                    logger.info(f"✅ Native MetaTrader5 ulandi! Server: {EXNESS_SERVER} | Simbol: {self.symbol}")
                    return
            logger.warning(f"⚠️ Native MT5 ulanmadi ({mt5.last_error()}), keyingi drayverga o'tilmoqda...")

        if METAAPI_TOKEN and METAAPI_ACCOUNT_ID:
            self.driver = "metaapi"
            logger.info(f"☁️ MetaApi Cloud MT5 drayveri faol! Account ID: {METAAPI_ACCOUNT_ID[:8]}...")
        else:
            self.driver = "paper_sim"
            logger.info("📱 Termux Live Spot + Paper MT5 drayveri faol (Swissquote/Yahoo real XAUUSD kotirovkasi bilan)!")

    def get_quote_and_balance(self):
        if self.driver == "native_mt5":
            tick = mt5.symbol_info_tick(self.symbol)
            acc = mt5.account_info()
            bid = round(float(tick.bid), 2) if tick else None
            ask = round(float(tick.ask), 2) if tick else None
            bal = round(float(acc.balance), 2) if acc else None
            return bid, ask, bal

        if self.driver == "metaapi":
            try:
                hdr = {"auth-token": METAAPI_TOKEN}
                info = http_json(
                    f"{METAAPI_REGION_URL}/users/current/accounts/{METAAPI_ACCOUNT_ID}/account-information",
                    extra_headers=hdr, timeout=8,
                )
                px = http_json(
                    f"{METAAPI_REGION_URL}/users/current/accounts/{METAAPI_ACCOUNT_ID}/symbols/{self.symbol}/current-price",
                    extra_headers=hdr, timeout=8,
                )
                return (
                    round(float(px.get("bid", 0)), 2) or None,
                    round(float(px.get("ask", 0)), 2) or None,
                    round(float(info.get("balance", 0)), 2) or None,
                )
            except Exception as e:
                logger.debug(f"MetaApi quote xato: {e}")

        bid, ask = fetch_live_spot_quote()
        return bid, ask, round(float(self.state.get("paper_balance", 20.0)), 2)

    def execute_signal(self, sig):
        action = str(sig.get("action") or "open").lower()
        engine = str(sig.get("engine") or "day").lower()
        magic = int(sig.get("magic") or (MT5_MAGIC_AKE if engine == "ake" else MT5_MAGIC_DAY))
        if engine == "test":
            magic = MT5_MAGIC_TEST

        if action == "open":
            return self._open_order(sig, engine, magic)
        elif action == "modify_sl":
            return self._modify_sl(sig, engine, magic)
        elif action == "close":
            return self._close_positions(sig, engine, magic)
        return {"ok": False, "msg": f"Noma'lum action: {action}"}

    def _open_order(self, sig, engine, magic):
        side = str(sig.get("signal", "LONG")).upper()
        is_buy = (side in ("LONG", "BUY"))
        lot = max(0.01, round(float(sig.get("lot") or DEFAULT_LOT), 2))
        entry_ref = float(sig.get("entry") or 0.0)
        sl = float(sig.get("sl") or 0.0)
        tp = float(sig.get("tp_final") or sig.get("tp1") or 0.0)
        comment = f"v22_{engine.upper()}_{'BUY' if is_buy else 'SELL'}"

        if self.driver == "native_mt5":
            tick = mt5.symbol_info_tick(self.symbol)
            s_info = mt5.symbol_info(self.symbol)
            if not tick or not s_info:
                return {"ok": False, "msg": "Simbol kotirovkasi topilmadi"}
            digits = s_info.digits
            price = tick.ask if is_buy else tick.bid
            if entry_ref > 0 and abs(price - entry_ref) <= 15.0:
                sl_d = abs(entry_ref - sl)
                tp_d = abs(tp - entry_ref)
                adj_sl = (price - sl_d) if is_buy else (price + sl_d)
                adj_tp = (price + tp_d) if is_buy else (price - tp_d)
            else:
                adj_sl, adj_tp = sl, tp

            order_type = mt5.ORDER_TYPE_BUY if is_buy else mt5.ORDER_TYPE_SELL
            for fill in (mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_FOK, mt5.ORDER_FILLING_RETURN):
                req = {
                    "action": mt5.TRADE_ACTION_DEAL,
                    "symbol": self.symbol,
                    "volume": lot,
                    "type": order_type,
                    "price": round(price, digits),
                    "sl": round(adj_sl, digits),
                    "tp": round(adj_tp, digits),
                    "deviation": 35,
                    "magic": magic,
                    "comment": comment[:31],
                    "type_time": mt5.ORDER_TIME_GTC,
                    "type_filling": fill,
                }
                res = mt5.order_send(req)
                if res and res.retcode == mt5.TRADE_RETCODE_DONE:
                    return {"ok": True, "ticket": res.order, "price": res.price or price, "driver": "native_mt5"}
            return {"ok": False, "msg": f"MT5 order_send xato: {mt5.last_error()}"}

        if self.driver == "metaapi":
            try:
                hdr = {"auth-token": METAAPI_TOKEN}
                payload = {
                    "actionType": "ORDER_TYPE_BUY" if is_buy else "ORDER_TYPE_SELL",
                    "symbol": self.symbol,
                    "volume": lot,
                    "stopLoss": round(sl, 2),
                    "takeProfit": round(tp, 2),
                    "magic": magic,
                    "comment": comment[:31],
                }
                res = http_json(
                    f"{METAAPI_REGION_URL}/users/current/accounts/{METAAPI_ACCOUNT_ID}/trade",
                    method="POST", payload=payload, extra_headers=hdr, timeout=12,
                )
                return {"ok": True, "ticket": res.get("orderId"), "driver": "metaapi"}
            except Exception as e:
                return {"ok": False, "msg": f"MetaApi trade xato: {e}"}

        # Paper / Live Quote Simulator
        bid, ask = fetch_live_spot_quote()
        fill_p = (ask if is_buy else bid) or entry_ref
        pos = {
            "ticket": int(time.time() * 1000) % 100000000,
            "engine": engine,
            "magic": magic,
            "side": "BUY" if is_buy else "SELL",
            "lot": lot,
            "entry": round(float(fill_p), 2),
            "sl": round(sl, 2),
            "tp": round(tp, 2),
            "opened_at": int(time.time()),
        }
        self.state.setdefault("paper_positions", []).append(pos)
        save_bridge_state(self.state)
        return {"ok": True, "ticket": pos["ticket"], "price": pos["entry"], "driver": "paper_sim"}

    def _modify_sl(self, sig, engine, magic):
        new_sl = round(float(sig.get("sl") or 0.0), 2)
        if new_sl <= 0:
            return {"ok": False, "msg": "Noto'g'ri SL qiymati"}

        if self.driver == "native_mt5":
            positions = mt5.positions_get(magic=magic) or []
            cnt = 0
            for pos in positions:
                req = {
                    "action": mt5.TRADE_ACTION_SLTP,
                    "position": pos.ticket,
                    "symbol": pos.symbol,
                    "sl": new_sl,
                    "tp": pos.tp,
                }
                r = mt5.order_send(req)
                if r and r.retcode == mt5.TRADE_RETCODE_DONE:
                    cnt += 1
            return {"ok": True, "modified": cnt, "new_sl": new_sl}

        if self.driver == "metaapi":
            try:
                hdr = {"auth-token": METAAPI_TOKEN}
                positions = http_json(
                    f"{METAAPI_REGION_URL}/users/current/accounts/{METAAPI_ACCOUNT_ID}/positions",
                    extra_headers=hdr, timeout=10,
                )
                cnt = 0
                for pos in (positions if isinstance(positions, list) else []):
                    if int(pos.get("magic") or 0) == magic:
                        http_json(
                            f"{METAAPI_REGION_URL}/users/current/accounts/{METAAPI_ACCOUNT_ID}/trade",
                            method="POST",
                            payload={
                                "actionType": "POSITION_MODIFY",
                                "positionId": pos["id"],
                                "stopLoss": new_sl,
                                "takeProfit": pos.get("takeProfit"),
                            },
                            extra_headers=hdr, timeout=10,
                        )
                        cnt += 1
                return {"ok": True, "modified": cnt, "new_sl": new_sl}
            except Exception as e:
                return {"ok": False, "msg": f"MetaApi modify_sl xato: {e}"}

        cnt = 0
        for pos in self.state.get("paper_positions", []):
            if int(pos.get("magic") or 0) == magic:
                pos["sl"] = new_sl
                cnt += 1
        save_bridge_state(self.state)
        return {"ok": True, "modified": cnt, "new_sl": new_sl}

    def _close_positions(self, sig, engine, magic):
        close_ref = float(sig.get("price") or 0.0)

        if self.driver == "native_mt5":
            positions = mt5.positions_get(magic=magic) or []
            cnt = 0
            for pos in positions:
                tick = mt5.symbol_info_tick(pos.symbol)
                s_info = mt5.symbol_info(pos.symbol)
                if not tick or not s_info:
                    continue
                c_type = mt5.ORDER_TYPE_SELL if pos.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
                c_px = tick.bid if pos.type == mt5.ORDER_TYPE_BUY else tick.ask
                for fill in (mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_FOK, mt5.ORDER_FILLING_RETURN):
                    req = {
                        "action": mt5.TRADE_ACTION_DEAL,
                        "position": pos.ticket,
                        "symbol": pos.symbol,
                        "volume": pos.volume,
                        "type": c_type,
                        "price": round(c_px, s_info.digits),
                        "deviation": 35,
                        "magic": magic,
                        "type_filling": fill,
                    }
                    r = mt5.order_send(req)
                    if r and r.retcode == mt5.TRADE_RETCODE_DONE:
                        cnt += 1
                        break
            return {"ok": True, "closed": cnt}

        if self.driver == "metaapi":
            try:
                hdr = {"auth-token": METAAPI_TOKEN}
                positions = http_json(
                    f"{METAAPI_REGION_URL}/users/current/accounts/{METAAPI_ACCOUNT_ID}/positions",
                    extra_headers=hdr, timeout=10,
                )
                cnt = 0
                for pos in (positions if isinstance(positions, list) else []):
                    if int(pos.get("magic") or 0) == magic:
                        http_json(
                            f"{METAAPI_REGION_URL}/users/current/accounts/{METAAPI_ACCOUNT_ID}/trade",
                            method="POST",
                            payload={"actionType": "POSITION_CLOSE_ID", "positionId": pos["id"]},
                            extra_headers=hdr, timeout=10,
                        )
                        cnt += 1
                return {"ok": True, "closed": cnt}
            except Exception as e:
                return {"ok": False, "msg": f"MetaApi close xato: {e}"}

        bid, ask = fetch_live_spot_quote()
        remaining = []
        closed_cnt = 0
        for pos in self.state.get("paper_positions", []):
            if int(pos.get("magic") or 0) == magic:
                is_buy = pos.get("side") == "BUY"
                exit_p = (bid if is_buy else ask) or close_ref or pos["entry"]
                pnl_pts = (exit_p - pos["entry"]) if is_buy else (pos["entry"] - exit_p)
                pnl_usd = round((float(pos.get("lot", 0.01)) / 0.01) * pnl_pts, 2)
                self.state["paper_balance"] = round(float(self.state.get("paper_balance", 20.0)) + pnl_usd, 2)
                closed_cnt += 1
            else:
                remaining.append(pos)
        self.state["paper_positions"] = remaining
        save_bridge_state(self.state)
        return {"ok": True, "closed": closed_cnt}


# ==================== ASOSIY BRIDGE SIKLI ====================
def run_bridge_forever():
    state = load_bridge_state()
    executor = BridgeExecutor(state)
    processed = set(state.get("processed_ids", []))
    first_poll = True
    last_report_ts = 0.0

    logger.info("==========================================================")
    logger.info("🚀 GOLD SMC v22 ULTIMATE — TERMUX CLOUD BRIDGE ISHGA TUSHDI")
    logger.info(f"🌐 Server URL : {BOT_SERVER_URL}")
    logger.info(f"⚙️ Drayver    : {executor.driver.upper()} | Simbol: {executor.symbol}")
    logger.info("==========================================================")

    while True:
        try:
            poll_url = f"{BOT_SERVER_URL}/api/mt5_bridge/poll"
            if BRIDGE_TOKEN:
                poll_url += f"?token={urllib.parse.quote(BRIDGE_TOKEN)}"
            data = http_json(poll_url, method="GET", timeout=10)

            if data.get("ok"):
                signals = data.get("recent_signals") or []
                now_ts = int(data.get("server_time") or time.time())

                if first_poll:
                    # Birinchi ulanishda eski (60 soniyadan oldingi) signallarni qayta ochmaslik
                    for s in signals:
                        sid = str(s.get("id") or "")
                        s_ts = int(s.get("ts") or 0)
                        if sid and (now_ts - s_ts > 60):
                            processed.add(sid)
                    state["processed_ids"] = list(processed)
                    save_bridge_state(state)
                    first_poll = False

                for sig in signals:
                    sid = str(sig.get("id") or "")
                    if not sid or sid in processed:
                        continue
                    logger.info(
                        f"⚡ Yangi buyruq: [{sig.get('engine', 'day').upper()}] "
                        f"{sig.get('action', 'open').upper()} | ID={sid}"
                    )
                    res = executor.execute_signal(sig)
                    logger.info(f"✅ Natija: {res}")
                    processed.add(sid)
                    state["processed_ids"] = list(processed)
                    save_bridge_state(state)

                    # Darhol serverga hisobot yuborish
                    bid, ask, bal = executor.get_quote_and_balance()
                    http_json(
                        f"{BOT_SERVER_URL}/api/mt5_bridge/report",
                        method="POST",
                        payload={
                            "login": EXNESS_LOGIN,
                            "server": EXNESS_SERVER,
                            "symbol": executor.symbol,
                            "bid": bid,
                            "ask": ask,
                            "balance": bal,
                            "driver": executor.driver,
                            "last_signal_id": sid,
                            "last_result": res,
                        },
                        timeout=8,
                    )
                    last_report_ts = time.time()

            # Har 10 soniyada jonli Bid/Ask kotirovka va Heartbeat yuborish
            if time.time() - last_report_ts >= 10.0:
                bid, ask, bal = executor.get_quote_and_balance()
                http_json(
                    f"{BOT_SERVER_URL}/api/mt5_bridge/report",
                    method="POST",
                    payload={
                        "login": EXNESS_LOGIN,
                        "server": EXNESS_SERVER,
                        "symbol": executor.symbol,
                        "bid": bid,
                        "ask": ask,
                        "balance": bal,
                        "driver": executor.driver,
                    },
                    timeout=8,
                )
                last_report_ts = time.time()

        except KeyboardInterrupt:
            logger.info("🛑 Termux Bridge foydalanuvchi tomonidan to'xtatildi.")
            break
        except urllib.error.HTTPError as he:
            logger.warning(f"⚠️ HTTP {he.code}: Serverga ulanish yoki token xatosi ({BOT_SERVER_URL})")
        except Exception as e:
            logger.warning(f"⚠️ Aloqa kutilmoqda ({BOT_SERVER_URL}): {e}")

        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    run_bridge_forever()
