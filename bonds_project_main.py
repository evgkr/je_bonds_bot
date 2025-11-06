from tinkoff.invest import MarketDataRequest,AsyncClient, SubscriptionAction, LastPriceInstrument, SubscribeLastPriceRequest, Client, CandleInstrument, CandleInterval, SubscribeCandlesRequest, SubscriptionInterval, GetMySubscriptions
import tokenAPI
from datetime import date, datetime, timedelta
from bs4 import BeautifulSoup as bs
from tokenAPI import bottoken
from tqdm import tqdm
import requests
import math
import pyxirr as px
import asyncio
import time

token = tokenAPI.token


#######
#######
# ====================== TELEGRAM SCREENER BOT v9.4 ======================
import os
import re
import html
import time
import asyncio
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Any, Tuple
from datetime import date
from queue import Queue, Empty
from urllib.parse import quote, unquote

from aiogram import Bot, Dispatcher, F, BaseMiddleware
from aiogram.filters import Command
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton, LinkPreviewOptions
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest

BOT_TOKEN = bottoken

MAX_TELEGRAM_MESSAGE = 11999
SAFE_LIMIT = 11900
EDIT_THROTTLE_SECONDS = 3
LP_DISABLED = LinkPreviewOptions(is_disabled=True)
IDLE_TIMEOUT_SECONDS = 30


# ===== очередь событий (вызывайте из стримов) =====
DATA_CHANGE_QUEUE: "Queue[str]" = Queue()
def notify_data_changed(uid: str, kind: str = ""):
    try: DATA_CHANGE_QUEUE.put_nowait(uid)
    except Exception: pass

# ====================== Состояние ======================
def _default_fields():
    # дефолтный набор полей (ваш пункт ранее)
    return ["Name", "YTW", "Price"]

def _default_filters():
    # дефолтный фильтр по рейтингам
    return FilterState(ratings={"AAA","AA","A","BBB","BB"})

@dataclass
class Range:
    lo: Optional[float] = None
    hi: Optional[float] = None
    def match(self, x: Optional[float]) -> bool:
        if x is None: return False
        if self.lo is not None and x < self.lo: return False
        if self.hi is not None and x > self.hi: return False
        return True
    def as_text(self) -> str:
        if self.lo is None and self.hi is None: return "—"
        if self.lo is None: return f"<= {self.hi}"
        if self.hi is None: return f">= {self.lo}"
        return f"{self.lo}..{self.hi}"

def parse_range(text: str) -> Optional[Range]:
    t = text.strip().replace(',', '.')
    t = re.sub(r'\s*%\s*', '', t)
    # поддерживаем &gt;= &lt;= тоже
    t = t.replace('&gt;=', '>=').replace('&lt;=', '<=')
    m = re.match(r'^\s*(-?\d+(?:\.\d+)?)\s*-\s*(-?\d+(?:\.\d+)?)\s*$', t)
    if m:
        a, b = float(m.group(1)), float(m.group(2))
        return Range(min(a,b), max(a,b))
    m = re.match(r'^\s*(?:>=|≥)\s*(-?\d+(?:\.\d+)?)\s*$', t)
    if m: return Range(float(m.group(1)), None)
    m = re.match(r'^\s*(?:<=|≤)\s*(-?\d+(?:\.\d+)?)\s*$', t)
    if m: return Range(None, float(m.group(1)))
    m = re.match(r'^\s*(-?\d+(?:\.\d+)?)\s*$', t)
    if m: return Range(float(m.group(1)), None)
    return None

@dataclass
class FilterState:
    # доходности
    ytm: Optional[Range] = None
    ytc: Optional[Range] = None
    ytw: Optional[Range] = None
    ytm_w: Optional[Range] = None
    ytc_w: Optional[Range] = None
    ytw_w: Optional[Range] = None
    # дюрации (базовые)
    d_mac: Optional[Range] = None
    d_mod: Optional[Range] = None
    d_mac_w: Optional[Range] = None
    d_mod_w: Optional[Range] = None
    # дюрации по сценариям
    d_mac_ytm: Optional[Range] = None
    d_mac_ytc: Optional[Range] = None
    d_mac_ytw: Optional[Range] = None
    d_mod_ytm: Optional[Range] = None
    d_mod_ytc: Optional[Range] = None
    d_mod_ytw: Optional[Range] = None
    d_mac_ytm_w: Optional[Range] = None
    d_mac_ytc_w: Optional[Range] = None
    d_mac_ytw_w: Optional[Range] = None
    d_mod_ytm_w: Optional[Range] = None
    d_mod_ytc_w: Optional[Range] = None
    d_mod_ytw_w: Optional[Range] = None
    # ∆P при -1% по кривой (базовые)
    dpp_down_1pct: Optional[Range] = None
    dpp_down_1pct_w: Optional[Range] = None
    # ∆P при -1% по сценариям
    dpp_ytm: Optional[Range] = None
    dpp_ytc: Optional[Range] = None
    dpp_ytw: Optional[Range] = None
    dpp_ytm_w: Optional[Range] = None
    dpp_ytc_w: Optional[Range] = None
    dpp_ytw_w: Optional[Range] = None
    # прочее
    term_days: Optional[Range] = None
    price: Optional[Range] = None
    volume: Optional[Range] = None  # в денежном выражении (лоты*цена)
    nominal: Optional[Range] = None
    issue_size: Optional[Range] = None
    vol_issue_pct: Optional[Range] = None  # процент объёма к выпуску
    coupons_per_year: Optional[Set[int]] = None
    ratings: Optional[Set[str]] = None
    sectors: Optional[Set[str]] = None
    countries: Optional[Set[str]] = None
    has_offer: Optional[bool] = None
    dtm_days: Optional[Range] = None
    offer_days: Optional[Range] = None

@dataclass
class ScreenerState:
    # дефолтные настройки
    sort_field: str = "YTW"
    sort_weighted: bool = False
    sort_desc: bool = True

    page_size: int = 7
    display_fields: List[str] = field(default_factory=_default_fields)
    use_watchlist: bool = False
    watchlist: Set[str] = field(default_factory=set)
    blacklist: Set[str] = field(default_factory=set)
    filters: FilterState = field(default_factory=_default_filters)

    # дефолтная группировка
    group: str = "sector"  # none|rating|sector|country|coupons|has_offer|term_bucket|issue_size_bucket

    # служебное
    active: bool = False
    chat_id: Optional[int] = None
    message_id: Optional[int] = None
    last_render_hash: Optional[int] = None
    last_edit_ts: float = 0.0
    awaiting_input: Optional[str] = None
    prompt_msg_id: Optional[int] = None
    list_view_msg_id: Optional[int] = None
    list_view_kind: Optional[str] = None
    presets: Dict[str, Any] = field(default_factory=dict)
    ui_mode: str = "main"
    page_idx: int = 0
    pages_count: int = 1
    last_interaction_ts: float = 0.0
    idle_task: Optional[asyncio.Task] = None

USER_STATES: Dict[int, ScreenerState] = {}

async def _idle_return_worker(bot: Bot, uid: int):
    state = USER_STATES.get(uid)
    if not state:
        return
    start_ts = state.last_interaction_ts
    try:
        await asyncio.sleep(IDLE_TIMEOUT_SECONDS)
        # Если с момента запуска ничего не нажимали и мы не в главном меню — вернёмся
        state = USER_STATES.get(uid)
        if not state:
            return
        if state.ui_mode != "main" and abs(state.last_interaction_ts - start_ts) < 1e-6:
            if state.chat_id and state.message_id:
                state.ui_mode = "main"
                try:
                    await bot.edit_message_reply_markup(
                        chat_id=state.chat_id,
                        message_id=state.message_id,
                        reply_markup=main_keyboard(state)
                    )
                except TelegramBadRequest:
                    # Сообщение могли удалить/поменять — молча игнорируем
                    pass
    except asyncio.CancelledError:
        # Таймер сброшен — просто выходим
        return

def _cancel_idle_timer(state: ScreenerState):
    if state.idle_task and not state.idle_task.done():
        state.idle_task.cancel()
    state.idle_task = None

def touch_idle_timer(bot: Bot, uid: int):
    """Обновить метку активности и перезапустить таймер возврата в меню."""
    state = USER_STATES.get(uid)
    if not state:
        return
    state.last_interaction_ts = time.time()
    # Перезапускаем только если не в главном меню; в главном меню возвращать нечего
    _cancel_idle_timer(state)
    if state.ui_mode != "main":
        state.idle_task = asyncio.create_task(_idle_return_worker(bot, uid))

class IdleTouchMiddleware(BaseMiddleware):
    def __init__(self, bot: Bot):
        super().__init__()
        self.bot = bot

    async def __call__(self, handler, event, data):
        q = event  # CallbackQuery
        st = USER_STATES.get(q.from_user.id)
        if st:
            if not st.chat_id and q.message:
                st.chat_id = q.message.chat.id
            if not st.message_id and q.message:
                st.message_id = q.message.message_id
            touch_idle_timer(self.bot, q.from_user.id)
        return await handler(event, data)

# ====================== Ваши данные (из глобалей) ======================
def _g(name: str, default):
    return globals().get(name, default)

def snapshot_data() -> Dict[str, Any]:
    return {
        "uids": list(_g("uid_bonds", [])),
        "ticker": _g("ticker_bonds", {}),
        "name": _g("name_bonds", {}),
        "maturity": _g("maturity_date_bonds", {}),
        "nominal": _g("nominal_bonds", {}),
        "country": _g("country_of_risk_bonds", {}),   # страны из country_of_risk_bonds
        "sector": _g("sector_bonds", {}),
        "issue_size": _g("issue_size_bonds", {}),
        "issue_size_money": _g("issue_size_money_bonds", {}),
        "rating": _g("credit_rating_bonds", {}),
        "offer_date": _g("offer_or_call_option_date_bonds", {}),
        "price": _g("last_price_bonds", {}),
        "volume": _g("volume_bonds", {}),
        # не взвешенные
        "YTM": _g("YTM_xirr_bonds", {}),
        "YTC": _g("YTC_xirr_bonds", {}),
        "YTW": _g("YTW_xirr_bonds", {}),
        "D_MAC_YTW": _g("macaulay_YTW_bonds", {}),
        "D_MOD_YTW": _g("modified_YTW_bonds", {}),
        # добавлено: duration по YTM/YTC
        "D_MAC_YTM": _g("macaulay_YTM_bonds", {}),
        "D_MOD_YTM": _g("modified_YTM_bonds", {}),
        "D_MAC_YTC": _g("macaulay_YTC_bonds", {}),
        "D_MOD_YTC": _g("modified_YTC_bonds", {}),
        "DPP_DOWN_YTW": _g("dpp_1pct_down_YTW_bonds", {}),
        # добавлено: ∆P по сценариям
        "DPP_DOWN_YTM": _g("dpp_1pct_down_YTM_bonds", {}),
        "DPP_DOWN_YTC": _g("dpp_1pct_down_YTC_bonds", {}),
        # взвешенные
        "wYTM": _g("weighted_YTM_xirr_bonds", {}),
        "wYTC": _g("weighted_YTC_xirr_bonds", {}),
        "wYTW": _g("weighted_YTW_xirr_bonds", {}),
        "wD_MAC_YTW": _g("weighted_macaulay_YTW_bonds", {}),
        "wD_MOD_YTW": _g("weighted_modified_YTW_bonds", {}),
        # добавлено: weighted duration по YTM/YTC
        "wD_MAC_YTM": _g("weighted_macaulay_YTM_bonds", {}),
        "wD_MOD_YTM": _g("weighted_modified_YTM_bonds", {}),
        "wD_MAC_YTC": _g("weighted_macaulay_YTC_bonds", {}),
        "wD_MOD_YTC": _g("weighted_modified_YTC_bonds", {}),
        "wDPP_DOWN_YTW": _g("weighted_dpp_1pct_down_YTW_bonds", {}),
        # добавлено: weighted ∆P по YTM/YTC
        "wDPP_DOWN_YTM": _g("weighted_dpp_1pct_down_YTM_bonds", {}),
        "wDPP_DOWN_YTC": _g("weighted_dpp_1pct_down_YTC_bonds", {}),
        "DAYS_YTW": _g("days_YTW_bonds", {}),
        "wDAYS_YTW": _g("days_weighted_YTW_bonds", {}),
        "coupons_per_year": _g("coupon_quantity_per_year_bonds", {}),
    }

# ====================== Нормализации/бакеты/форматы ======================
def normalize_rating(r: Optional[str]) -> Optional[str]:
    if not r or r == "-": return None
    r = r.strip().upper()
    for bucket in ["AAA","AA","A","BBB","BB","B","CCC","CC","C","D"]:
        if r.startswith(bucket): return bucket
    return r

RATING_ORDER = ["AAA","AA","A","BBB","BB","B","CCC","CC","C","D"]
RATING_EMOJI = {"AAA":"🟩","AA":"🟩","A":"🟨","BBB":"🟨","BB":"🟧","B":"🟧","CCC":"🟥","CC":"🟥","C":"🟥","D":"🟥"}

def sector_display_with_emoji(sec: Optional[str]) -> str:
    if not sec: return "Other 👁"
    s = str(sec).strip().replace("_"," ").title()
    mapping = {
        "Materials":"Materials 🧱","Health Care":"Health Care 💊","Telecom":"Telecom 📡","Other":"Other 👁",
        "Financial":"Financial 💵","Industrials":"Industrials 🏗","Municipal":"Municipal 🌆","Government":"Government 🏛",
        "Consumer":"Consumer 🛒","Energy":"Energy 🔋","Utilities":"Utilities 💡","It":"IT 💻",
        "Information Technology":"IT 💻","Real Estate":"Real Estate 🏠",
    }
    return mapping.get(s, f"{s} 👁")

def bucket_issue_size_money(x: Optional[float]) -> str:
    if x is None: return "—"
    M = 1_000_000
    B = 1_000_000_000
    if x <= 100*M:    return "<=100m"
    if x <= 1*B:      return "100m-1b"
    if x <= 10*B:     return "1b-10b"
    if x <= 100*B:    return "10b-100b"
    return ">100b"

def fmt_pct(x: Optional[float], digits=2) -> str:
    return "—" if x is None else f"{x*100:.{digits}f}%"

def fmt_num(x: Optional[float], digits=2) -> str:
    if x is None: return "—"
    if isinstance(x, int): return f"{x:,}".replace(",", " ")
    try: return f"{x:.{digits}f}"
    except: return str(x)

def fmt_days(d: Optional[int]) -> str:
    return "—" if d is None else f"{int(d)}d"

def short_name(s: Optional[str], n=9) -> str:
    if not s: return "—"
    s = str(s)
    return s if len(s) <= n else (s[:n - 1] + "…")

# ====================== Строки таблицы ======================
def make_rows(state: ScreenerState, data: Dict[str, Any]) -> List[Dict[str, Any]]:
    uids = data["uids"]
    rows = []
    today_d = date.today()

    for uid in uids:
        tkr = data["ticker"].get(uid)
        if not tkr:
            continue
        # Watchlist overrides blacklist:
        if state.use_watchlist:
            if state.watchlist and (tkr not in state.watchlist):
                continue
        else:
            if tkr in state.blacklist:
                continue

        r = {
            "uid": uid,
            "Ticker": tkr,
            "Name": data["name"].get(uid),
            "Price": data["price"].get(uid),
            "Vol": data["volume"].get(uid),  # лоты (из стрима)
            "Mat": data["maturity"].get(uid),
            "IssueSize": data["issue_size"].get(uid),
            "IssueSizeMoney": data["issue_size_money"].get(uid),
            "Nominal": data["nominal"].get(uid),
            "CouponsPerYear": data["coupons_per_year"].get(uid),
            "Country": data["country"].get(uid),
            "Sector": data["sector"].get(uid),
            "OfferDate": data["offer_date"].get(uid),
            "RatingOrig": data["rating"].get(uid),
            "RatingBucket": normalize_rating(data["rating"].get(uid)),
            # доходности
            "YTM": data["YTM"].get(uid),
            "YTC": data["YTC"].get(uid),
            "YTW": data["YTW"].get(uid),
            "YTM_w": data["wYTM"].get(uid),
            "YTC_w": data["wYTC"].get(uid),
            "YTW_w": data["wYTW"].get(uid),
            # базовые длительности/ΔP (по YTW)
            "D_mod": data["D_MOD_YTW"].get(uid),
            "D_mac": data["D_MAC_YTW"].get(uid),
            "D_mod_w": data["wD_MOD_YTW"].get(uid),
            "D_mac_w": data["wD_MAC_YTW"].get(uid),
            "DPP": data["DPP_DOWN_YTW"].get(uid),
            "DPP_w": data["wDPP_DOWN_YTW"].get(uid),
            # срок до YTW
            "TermDays": data["DAYS_YTW"].get(uid),
            "TermDays_w": data["wDAYS_YTW"].get(uid),
            # длительности по сценариям
            "DMacYTM": data.get("D_MAC_YTM", {}).get(uid),
            "DMacYTC": data.get("D_MAC_YTC", {}).get(uid),
            "DMacYTW": data.get("D_MAC_YTW", {}).get(uid),
            "DMacYTM_w": data.get("wD_MAC_YTM", {}).get(uid),
            "DMacYTC_w": data.get("wD_MAC_YTC", {}).get(uid),
            "DMacYTW_w": data.get("wD_MAC_YTW", {}).get(uid),
            "DModYTM": data.get("D_MOD_YTM", {}).get(uid),
            "DModYTC": data.get("D_MOD_YTC", {}).get(uid),
            "DModYTW": data.get("D_MOD_YTW", {}).get(uid),
            "DModYTM_w": data.get("wD_MOD_YTM", {}).get(uid),
            "DModYTC_w": data.get("wD_MOD_YTC", {}).get(uid),
            "DModYTW_w": data.get("wD_MOD_YTW", {}).get(uid),
            # % change по сценариям
            "DPP_YTM": data.get("DPP_DOWN_YTM", {}).get(uid),
            "DPP_YTC": data.get("DPP_DOWN_YTC", {}).get(uid),
            "DPP_YTW": data.get("DPP_DOWN_YTW", {}).get(uid),
            "DPP_YTM_w": data.get("wDPP_DOWN_YTM", {}).get(uid),
            "DPP_YTC_w": data.get("wDPP_DOWN_YTC", {}).get(uid),
            "DPP_YTW_w": data.get("wDPP_DOWN_YTW", {}).get(uid),
        }

        # денежный объём (лоты*последняя цена)
        vol_lots = r["Vol"]; px = r["Price"]
        r["VolMoney"] = (vol_lots * px) if (isinstance(vol_lots, (int,float)) and isinstance(px,(int,float))) else None
        # доля объёма в выпуске (в %)
        iss = r.get("IssueSize"); vol = r.get("Vol")
        r["VolIssuePct"] = (
            (vol * 100.0 / iss)
            if isinstance(vol, (int, float)) and isinstance(iss, (int, float)) and iss > 0
            else None
        )

        # DtA значения
        r["DtM"] = (r["Mat"] - today_d).days if r["Mat"] else None
        r["DtC"] = ((r["OfferDate"] - today_d).days if r["OfferDate"] else 0)
        r["DtW"] = r["TermDays_w"] if state.sort_weighted else r["TermDays"]

        rows.append(r)

    # --- фильтры ---
    f = state.filters
    def rngok(rng: Optional[Range], x: Optional[float]) -> bool:
        return True if rng is None else rng.match(x)

    filtered = []
    for r in rows:
        # доходности (в %)
        ytm_val = (r["YTM"] * 100.0) if r["YTM"] is not None else None
        ytc_val = (r["YTC"] * 100.0) if r["YTC"] is not None else None
        ytw_val = (r["YTW"] * 100.0) if r["YTW"] is not None else None
        ytm_w_val = (r["YTM_w"] * 100.0) if r["YTM_w"] is not None else None
        ytc_w_val = (r["YTC_w"] * 100.0) if r["YTC_w"] is not None else None
        ytw_w_val = (r["YTW_w"] * 100.0) if r["YTW_w"] is not None else None

        if f.ytm and not rngok(f.ytm, ytm_val):   continue
        if f.ytc and not rngok(f.ytc, ytc_val):   continue
        if f.ytw and not rngok(f.ytw, ytw_val):   continue
        if f.ytm_w and not rngok(f.ytm_w, ytm_w_val): continue
        if f.ytc_w and not rngok(f.ytc_w, ytc_w_val): continue
        if f.ytw_w and not rngok(f.ytw_w, ytw_w_val): continue

        # базовые длительности
        if f.d_mac and not rngok(f.d_mac, r["D_mac"]): continue
        if f.d_mod and not rngok(f.d_mod, r["D_mod"]): continue
        if f.d_mac_w and not rngok(f.d_mac_w, r["D_mac_w"]): continue
        if f.d_mod_w and not rngok(f.d_mod_w, r["D_mod_w"]): continue

        # длительности по сценариям
        if getattr(f, "d_mac_ytm", None) and not rngok(f.d_mac_ytm, r["DMacYTM"]): continue
        if getattr(f, "d_mac_ytc", None) and not rngok(f.d_mac_ytc, r["DMacYTC"]): continue
        if getattr(f, "d_mac_ytw", None) and not rngok(f.d_mac_ytw, r["DMacYTW"]): continue
        if getattr(f, "d_mod_ytm", None) and not rngok(f.d_mod_ytm, r["DModYTM"]): continue
        if getattr(f, "d_mod_ytc", None) and not rngok(f.d_mod_ytc, r["DModYTC"]): continue
        if getattr(f, "d_mod_ytw", None) and not rngok(f.d_mod_ytw, r["DModYTW"]): continue
        if getattr(f, "d_mac_ytm_w", None) and not rngok(f.d_mac_ytm_w, r["DMacYTM_w"]): continue
        if getattr(f, "d_mac_ytc_w", None) and not rngok(f.d_mac_ytc_w, r["DMacYTC_w"]): continue
        if getattr(f, "d_mac_ytw_w", None) and not rngok(f.d_mac_ytw_w, r["DMacYTW_w"]): continue
        if getattr(f, "d_mod_ytm_w", None) and not rngok(f.d_mod_ytm_w, r["DModYTM_w"]): continue
        if getattr(f, "d_mod_ytc_w", None) and not rngok(f.d_mod_ytc_w, r["DModYTC_w"]): continue
        if getattr(f, "d_mod_ytw_w", None) and not rngok(f.d_mod_ytw_w, r["DModYTW_w"]): continue

        # % change (в %)
        def pct(v): return (v*100.0 if v is not None else None)
        if getattr(f, "dpp_ytm", None) and not rngok(f.dpp_ytm, pct(r["DPP_YTM"])): continue
        if getattr(f, "dpp_ytc", None) and not rngok(f.dpp_ytc, pct(r["DPP_YTC"])): continue
        if getattr(f, "dpp_ytw", None) and not rngok(f.dpp_ytw, pct(r["DPP_YTW"])): continue
        if getattr(f, "dpp_ytm_w", None) and not rngok(f.dpp_ytm_w, pct(r["DPP_YTM_w"])): continue
        if getattr(f, "dpp_ytc_w", None) and not rngok(f.dpp_ytc_w, pct(r["DPP_YTC_w"])): continue
        if getattr(f, "dpp_ytw_w", None) and not rngok(f.dpp_ytw_w, pct(r["DPP_YTW_w"])): continue

        # dpp базовые
        if f.dpp_down_1pct and not rngok(f.dpp_down_1pct, pct(r["DPP"])): continue
        if f.dpp_down_1pct_w and not rngok(f.dpp_down_1pct_w, pct(r["DPP_w"])): continue

        # DtA диапазоны
        if getattr(f, "dtm_days", None) and not rngok(f.dtm_days, r["DtM"]): continue
        if getattr(f, "offer_days", None) and not rngok(f.offer_days, r["DtC"]): continue
        if f.term_days and not rngok(f.term_days, r["DtW"]): continue

        # прочее
        if f.price and not rngok(f.price, r["Price"]): continue
        if f.volume and not rngok(f.volume, r.get("VolMoney")): continue
        if f.nominal and not rngok(f.nominal, r["Nominal"]): continue
        if f.issue_size and not rngok(f.issue_size, r.get("IssueSizeMoney")): continue
        if getattr(f, "vol_issue_pct", None) and not rngok(f.vol_issue_pct, r.get("VolIssuePct")): continue
        if f.has_offer is not None:
            if (r["OfferDate"] is not None) != f.has_offer: continue
        if f.ratings and r["RatingBucket"] not in f.ratings: continue
        if f.sectors and (r["Sector"] not in f.sectors): continue
        if f.countries:
            country_val = r["Country"] if r["Country"] else "Unknown"
            if country_val not in f.countries:
                continue
        if f.coupons_per_year and r["CouponsPerYear"] not in f.coupons_per_year: continue

        rows_ok = r
        rows_ok["TermDays"] = r["DtW"]
        filtered.append(rows_ok)

    # --- сортировка ---
    key = state.sort_field
    reverse = state.sort_desc
    w = state.sort_weighted
    def kfunc(x):
        if key == "YTM": return (x["YTM_w"] if w else x["YTM"]) or -1e12
        if key == "YTC": return (x["YTC_w"] if w else x["YTC"]) or -1e12
        if key == "YTW": return (x["YTW_w"] if w else x["YTW"]) or -1e12

        if key == "D_MAC_YTM": return (x["DMacYTM_w"] if w else x["DMacYTM"]) or -1e12
        if key == "D_MAC_YTC": return (x["DMacYTC_w"] if w else x["DMacYTC"]) or -1e12
        if key == "D_MAC_YTW": return (x["DMacYTW_w"] if w else x["DMacYTW"]) or -1e12
        if key == "D_MOD_YTM": return (x["DModYTM_w"] if w else x["DModYTM"]) or -1e12
        if key == "D_MOD_YTC": return (x["DModYTC_w"] if w else x["DModYTC"]) or -1e12
        if key == "D_MOD_YTW": return (x["DModYTW_w"] if w else x["DModYTW"]) or -1e12

        if key == "DPP_YTM": return (x["DPP_YTM_w"] if w else x["DPP_YTM"]) or -1e12
        if key == "DPP_YTC": return (x["DPP_YTC_w"] if w else x["DPP_YTC"]) or -1e12
        if key == "DPP_YTW": return (x["DPP_YTW_w"] if w else x["DPP_YTW"]) or -1e12

        if key == "D_MAC": return (x["D_mac_w"] if w else x["D_mac"]) or -1e12
        if key == "D_MOD": return (x["D_mod_w"] if w else x["D_mod"]) or -1e12
        if key == "DPP":   return (x["DPP_w"]   if w else x["DPP"])   or -1e12

        if key == "DTA_DTM": return x.get("DtM") or -1e12
        if key == "DTA_DTC": return x.get("DtC") or -1e12
        if key == "DTA_DTW": return (x.get("TermDays_w") if w else x.get("TermDays")) or -1e12

        if key == "TERM": return (x["TermDays_w"] if w else x["TermDays"]) or -1e12  # legacy
        if key == "PRICE": return x["Price"] or -1e12
        if key == "VOLUME": return x.get("VolMoney") or -1e12
        if key == "ISSUE_MONEY": return x.get("IssueSizeMoney") or -1e12  # ← ДОБАВЛЕНО
        if key == "VOLISSUE": return x.get("VolIssuePct") or -1e12
        return -1e12

    filtered.sort(key=kfunc, reverse=reverse)

    # --- группировка (как было) ---
    group = state.group
    if group == "none":
        return filtered

    def bucket_term_days(d: Optional[int]) -> str:
        if d is None: return "—"
        if d <= 90:        return "<=3m"
        if d <= 180:       return "3-6m"
        if d <= 365:       return "6m-1y"
        if d <= 3 * 365:     return "1-3y"
        if d <= 5 * 365:     return "3-5y"
        if d <= 10 * 365:    return "5-10y"
        return ">=10y"

    buckets: Dict[str, List[Dict[str, Any]]] = {}
    for r in filtered:
        if group == "rating":
            g = r["RatingBucket"] or "N/A"
        elif group == "sector":
            g = r["Sector"] or "N/A"
        elif group == "country":
            g = r["Country"] or "Неизвестная страна"
        elif group == "coupons":
            g = str(r["CouponsPerYear"]) if r["CouponsPerYear"] is not None else "N/A"
        elif group == "has_offer":
            g = "Оферта" if r["OfferDate"] else "Без оферты"
        elif group == "term_bucket":
            g = bucket_term_days(r["DtM"])
        elif group == "issue_size_bucket":
            g = bucket_issue_size_money(r.get("IssueSizeMoney"))
        else:
            g = "Все"
        buckets.setdefault(g, []).append(r)

    def group_sort_key(gname: str):
        if group == "rating":
            try: return (RATING_ORDER.index(gname), gname)
            except: return (len(RATING_ORDER)+1, gname)
        if group == "coupons":
            try:
                n = int(gname) if gname.isdigit() else -999
                return (-n, gname)
            except:
                return (999, gname)
        if group == "term_bucket":
            order = {"<=3m": 0, "3-6m": 1, "6m-1y": 2, "1-3y": 3, "3-5y": 4, "5-10y": 5, ">=10y": 6}
            return (order.get(gname, 999), gname)
        if group == "issue_size_bucket":
            order = {"<=100m": 0, "100m-1b": 1, "1b-10b": 2, "10b-100b": 3, ">100b": 4}
            return (order.get(gname, 999), gname)
        return (0, str(gname))

    out = []
    for gname in sorted(buckets.keys(), key=group_sort_key):
        if group == "rating":
            em = RATING_EMOJI.get(gname, "")
            out.append({"_group_header": f"{em} {gname}"})
        elif group == "sector":
            out.append({"_group_header": sector_display_with_emoji(gname)})
        elif group == "coupons":
            out.append({"_group_header": f"Coupons/year: {gname}"})
        else:
            out.append({"_group_header": str(gname)})
        out.extend(buckets[gname][:state.page_size])
    return out

# ====================== Рендер строки + (w) ======================
def sort_display_field_name(state: ScreenerState) -> str:
    base_map = {
        "YTM": "YTM", "YTC": "YTC", "YTW": "YTW",
        "D_MAC": "D_mac", "D_MOD": "D_mod", "DPP": "ΔP@-1%",
        "D_MAC_YTM": "D_mac@YTM", "D_MAC_YTC": "D_mac@YTC", "D_MAC_YTW": "D_mac@YTW",
        "D_MOD_YTM": "D_mod@YTM", "D_MOD_YTC": "D_mod@YTC", "D_MOD_YTW": "D_mod@YTW",
        "DPP_YTM": "% change if yield ↓ 1% @YTM",
        "DPP_YTC": "% change if yield ↓ 1% @YTC",
        "DPP_YTW": "% change if yield ↓ 1% @YTW",
        "TERM": "Mat", "PRICE": "Price", "VOLUME": "Vol₽", "VOLISSUE": "Vol/Issue%",
        "ISSUE_MONEY": "Issue (₽)",                               # ← ДОБАВЛЕНО
        "DTA_DTM": "DtM", "DTA_DTC": "DtC", "DTA_DTW": "DtW",
    }
    base = base_map.get(state.sort_field, "YTW")

    weighted_fields = {
        "YTM","YTC","YTW",
        "D_MAC","D_MOD","DPP",
        "D_MAC_YTM","D_MAC_YTC","D_MAC_YTW",
        "D_MOD_YTM","D_MOD_YTC","D_MOD_YTW",
        "DPP_YTM","DPP_YTC","DPP_YTW",
    }
    needs_w_suffix = state.sort_weighted and state.sort_field in weighted_fields
    return base + (" (w)" if needs_w_suffix else "")

def ensure_sort_field_in_display_fields(state: ScreenerState):
    needed = sort_display_field_name(state)
    if needed not in state.display_fields:
        state.display_fields.append(needed)

ALLOWED_FIELDS = [
    "Name","Ticker","Price",
    "YTM","YTC","YTW","YTM (w)","YTC (w)","YTW (w)",
    "D_mod","D_mac","D_mod (w)","D_mac (w)",
    "D_mac@YTM","D_mac@YTM (w)","D_mac@YTC","D_mac@YTC (w)","D_mac@YTW","D_mac@YTW (w)",
    "D_mod@YTM","D_mod@YTM (w)","D_mod@YTC","D_mod@YTC (w)","D_mod@YTW","D_mod@YTW (w)",
    "ΔP@-1%","ΔP@-1% (w)",
    "% change if yield ↓ 1% @YTM","% change if yield ↓ 1% @YTM (w)",
    "% change if yield ↓ 1% @YTC","% change if yield ↓ 1% @YTC (w)",
    "% change if yield ↓ 1% @YTW","% change if yield ↓ 1% @YTW (w)",
    "Vol","Vol₽","Issue (₽)","Mat","Mat (w)", "Vol/Issue%",
    "DtM","DtC","DtW",
    "Rating","Sector","Country","Coupons/year"
]

BUILTIN_PRESETS: Dict[str, Dict[str, Any]] = {
    # поля + сортировка + группировка
    "default": {"fields": ["Name", "YTW", "Price"], "sort": "YTW", "w": False, "desc": True, "group": "sector"},

    # Desktop — «богатые» наборы, оставим group=sector
    "desk1":  {"fields": ["Name","YTW","Price","Rating","DtW","D_mac@YTW","Vol₽","Vol/Issue%"],
               "sort":"YTW", "w":False, "desc":True, "group":"sector"},
    "desk2":  {"fields": ["Name","YTW","YTW (w)","Price","Vol₽","Issue (₽)","Vol/Issue%","DtW"],
               "sort":"YTW", "w":False, "desc":True, "group":"sector"},
    "desk3":  {"fields": ["Name","YTW","Sector","D_mac@YTW","% change if yield ↓ 1% @YTW","Vol₽","Coupons/year"],
               "sort":"YTW", "w":False, "desc":True, "group":"rating"},

    # Phone — «компактные», без группировки
    "phone1": {"fields": ["Name","YTW","Price","Rating"],
               "sort":"YTW", "w":False, "desc":True, "group":"sector"},
    "phone2": {"fields": ["Name","YTW","Price","DtW"],
               "sort":"YTW", "w":False, "desc":True, "group":"sector"},
    "phone3": {"fields": ["Name","YTW","Price","Vol₽"],
               "sort":"YTW", "w":False, "desc":True, "group":"rating"},
}

# Кейс-инсенситив сопоставление разрешённых полей
ALLOWED_LOOKUP: Dict[str, str] = {s.lower(): s for s in ALLOWED_FIELDS}


def _line_from_row(state: ScreenerState, r: Dict[str, Any], cols: List[str], sort_disp_field: str) -> str:
    def anchor_tinkoff(ticker: str, inner_html: str, frag: str | None = None) -> str:
        url = f"https://www.tinkoff.ru/invest/bonds/{html.escape(ticker)}"
        if frag:
            url = f"{url}#{frag}"
        return f'<a href="{url}">{inner_html}</a>'

    def _note_fragment() -> str | None:
        """Строим текст подсказки для 'Open Link' (фрагмент после #, URL-encoded)."""
        from datetime import date, datetime

        def fmt_date(d):
            if isinstance(d, (date, datetime)):
                return d.strftime("%d.%m.%Y")
            return "—"

        def fmt_bool(x):
            return "Yes" if x else "No"

        def pct(x, digits=2):
            return "—" if x is None else f"{x * 100:.{digits}f}%"

        def num(x, digits=2):
            return "—" if x is None else f"{x:.{digits}f}"

        def num_int(x):
            if x is None:
                return "—"
            try:
                return f"{int(x):,}".replace(",", " ")
            except Exception:
                return str(x)

        def days(x):
            return "—" if x is None else f"{int(x)}d"

        full_name = r.get("Name")
        ticker = r.get("Ticker")
        maturity = r.get("Mat")
        dtm = r.get("DtM")

        offer_date = r.get("OfferDate")
        has_offer = (offer_date is not None)

        sector = r.get("Sector")
        rating = r.get("RatingOrig")
        cpn_year = r.get("CouponsPerYear")
        country = r.get("Country")

        # доходности
        ytm, ytm_w = r.get("YTM"), r.get("YTM_w")
        ytc, ytc_w = r.get("YTC"), r.get("YTC_w")
        ytw, ytw_w = r.get("YTW"), r.get("YTW_w")

        # DtA
        dtc = r.get("DtC") if has_offer else None
        dtw = r.get("DtW")

        # Дюрации @YTW
        dmac_ytw, dmac_ytw_w = r.get("DMacYTW"), r.get("DMacYTW_w")
        dmod_ytw, dmod_ytw_w = r.get("DModYTW"), r.get("DModYTW_w")

        # Объёмы
        vol_lots = r.get("Vol")
        issue_size_lots = r.get("IssueSize")
        vol_issue_pct = r.get("VolIssuePct")

        # ΔP при -1% для YTW
        dpp_ytw, dpp_ytw_w = r.get("DPP_YTW"), r.get("DPP_YTW_w")

        # Has offer: Yes (DD.MM.YYYY) / No
        has_offer_str = fmt_bool(has_offer)
        if has_offer:
            has_offer_str += f" ({fmt_date(offer_date)})"

        lines = [
            f"🏷️ Name: {full_name or '—'}",
            f"🆔 Ticker: {ticker or '—'}",
            f"📆 Maturity: {fmt_date(maturity)} ({days(dtm)})",
            f"📝 Has offer: {has_offer_str}",
            f"🏭 Sector: {(sector or '—')}",
            f"⭐ Credit rating: {rating or '—'}",
            f"💸 Coupons: {cpn_year if cpn_year is not None else '—'} per year"
        ]

        # Ведущий перенос строки делает диалог Telegram аккуратнее
        return quote("\n" + "\n" + "\n".join(lines), safe="")

    parts = []
    for c in cols:
        is_sort_col = (c == sort_disp_field)
        if c == "Ticker":
            core = f"{html.escape(r['Ticker'])}"
            core = f"<b>{core}</b>" if is_sort_col else core
            frag = _note_fragment()
            parts.append(anchor_tinkoff(r["Ticker"], core, frag))
        elif c == "Name":
            nm = short_name(r["Name"], 9)
            core = html.escape(nm)
            core = f"<b>{core}</b>" if is_sort_col else core
            frag = _note_fragment()
            parts.append(anchor_tinkoff(r["Ticker"], core, frag))
        elif c == "Price":
            val = "—" if r["Price"] is None else f"{r['Price']:.2f}₽"
            parts.append(f"<b>{val}</b>" if is_sort_col else val)

        # доходности
        elif c in ("YTW","YTM","YTC"):
            core = fmt_pct(r[c], 2)
            parts.append(f"<b>{core}</b>" if is_sort_col else core)
        elif c in ("YTW (w)","YTM (w)","YTC (w)"):
            base = c.split()[0] + "_w"
            core = fmt_pct(r.get(base), 2)
            parts.append(f"<b>{core}</b>" if is_sort_col else core)

        # дюрации
        elif c == "D_mod":
            core = fmt_num(r["D_mod"], 2); parts.append(f"<b>{core}</b>" if is_sort_col else core)
        elif c == "D_mac":
            core = fmt_num(r["D_mac"], 2); parts.append(f"<b>{core}</b>" if is_sort_col else core)
        elif c == "D_mod (w)":
            core = fmt_num(r["D_mod_w"], 2)
            parts.append(f"<b>{core}</b>" if is_sort_col else core)
        elif c == "D_mac (w)":
            core = fmt_num(r["D_mac_w"], 2)
            parts.append(f"<b>{core}</b>" if is_sort_col else core)

        # дюрации по сценариям
        elif c in ("D_mac@YTM","D_mac@YTC","D_mac@YTW","D_mac@YTM (w)","D_mac@YTC (w)","D_mac@YTW (w)"):
            key = {"D_mac@YTM":"DMacYTM","D_mac@YTC":"DMacYTC","D_mac@YTW":"DMacYTW",
                   "D_mac@YTM (w)":"DMacYTM_w","D_mac@YTC (w)":"DMacYTC_w","D_mac@YTW (w)":"DMacYTW_w"}[c]
            core = fmt_num(r.get(key), 2)
            parts.append(f"<b>{core}</b>" if is_sort_col else core)
        elif c in ("D_mod@YTM","D_mod@YTC","D_mod@YTW","D_mod@YTM (w)","D_mod@YTC (w)","D_mod@YTW (w)"):
            key = {"D_mod@YTM":"DModYTM","D_mod@YTC":"DModYTC","D_mod@YTW":"DModYTW",
                   "D_mod@YTM (w)":"DModYTM_w","D_mod@YTC (w)":"DModYTC_w","D_mod@YTW (w)":"DModYTW_w"}[c]
            core = fmt_num(r.get(key), 2)
            parts.append(f"<b>{core}</b>" if is_sort_col else core)

        # % change по сценариям
        elif c.startswith("% change if yield ↓ 1% @"):
            mapping = {
                "% change if yield ↓ 1% @YTM": "DPP_YTM",
                "% change if yield ↓ 1% @YTM (w)": "DPP_YTM_w",
                "% change if yield ↓ 1% @YTC": "DPP_YTC",
                "% change if yield ↓ 1% @YTC (w)": "DPP_YTC_w",
                "% change if yield ↓ 1% @YTW": "DPP_YTW",
                "% change if yield ↓ 1% @YTW (w)": "DPP_YTW_w",
            }
            key = mapping[c]
            val = r.get(key)
            core = ("—" if val is None else f"{fmt_num((val*100.0), 2)}%")
            parts.append(f"<b>{core}</b>" if is_sort_col else core)

        # базовый ΔP@-1%
        elif c == "ΔP@-1%":
            core = (fmt_num(r["DPP"]*100 if r["DPP"] is not None else None, 2) + "%" if r["DPP"] is not None else "—")
            parts.append(f"<b>{core}</b>" if is_sort_col else core)
        elif c == "ΔP@-1% (w)":
            val = r["DPP_w"]
            core = (fmt_num(val*100 if val is not None else None, 2) + "%" if val is not None else "—")
            parts.append(f"<b>{core}</b>" if is_sort_col else core)

        # объёмы/выпуск
        elif c == "Vol":
            core = fmt_num(r["Vol"], 0); parts.append(f"<b>{core}</b>" if is_sort_col else core)
        elif c == "Vol₽":
            val = r.get("VolMoney")
            core = "—" if val is None else f"{int(val):,}₽".replace(",", " ")
            parts.append(f"<b>{core}</b>" if is_sort_col else core)
        elif c == "Issue (₽)":
            val = r.get("IssueSizeMoney")
            core = "—" if val is None else f"{int(val):,}₽".replace(",", " ")
            parts.append(f"<b>{core}</b>" if is_sort_col else core)
        elif c == "Vol/Issue%":
            core = fmt_num(r.get("VolIssuePct"), 3)
            core = ("—" if core == "—" else f"{core}%")
            parts.append(f"<b>{core}</b>" if is_sort_col else core)

        # DtA
        elif c == "Mat":
            core = fmt_days(r["TermDays"]); parts.append(f"<b>{core}</b>" if is_sort_col else core)
        elif c == "Mat (w)":
            core = fmt_days(r["TermDays_w"])
            parts.append(f"<b>{core}</b>" if is_sort_col else core)
        elif c == "DtM":
            parts.append(fmt_days(r.get("DtM")))
        elif c == "DtC":
            parts.append(fmt_days(r.get("DtC")))
        elif c == "DtW":
            use_w = state.sort_weighted
            val = r.get("TermDays_w") if use_w else r.get("TermDays")
            parts.append(fmt_days(val))

        # прочее
        elif c == "Rating":
            parts.append(r.get("RatingOrig") or "—")
        elif c == "Sector":
            s = (r["Sector"] or "Other")
            parts.append(str(s).title())
        elif c == "Country":
            parts.append(r["Country"] or "—")
        elif c == "Coupons/year":
            parts.append(str(r["CouponsPerYear"]) if r["CouponsPerYear"] is not None else "—")
    return " | ".join(parts)

# ====================== Шапка + блоки/страницы (экранируем диапазоны) ======================
def rating_sorted(items: Set[str]) -> List[str]:
    idx = {r:i for i,r in enumerate(RATING_ORDER)}
    return sorted(items, key=lambda x: idx.get(x, 999))

def _format_ticker_list_grouped(title: str, tickers: "Collection[str]") -> str:
    from collections import defaultdict
    data = snapshot_data()

    # Собираем тикер → (name, sector)
    by_ticker = {}
    for uid in data["uids"]:
        t = data["ticker"].get(uid)
        if not t:
            continue
        if t in tickers:
            by_ticker[t] = (data["name"].get(uid), data["sector"].get(uid))

    # Закрываем «дыры»: тикеры, которых нет в снапшоте
    for t in tickers:
        by_ticker.setdefault(t, (None, None))

    # Группируем по сектору
    grouped = defaultdict(list)
    for t, (nm, sec) in by_ticker.items():
        grouped[sec].append((t, nm))

    # Сортируем секции и тикеры, печатаем "TICKER | Name"
    lines = [f"<b>{title}</b> ({len(tickers)}):"]
    for sec in sorted(grouped.keys(), key=lambda s: ("" if s is None else s)):
        sec_title = sector_display_with_emoji(sec)  # уже есть в коде
        items = sorted(grouped[sec], key=lambda x: x[0])  # по тикеру
        lines.append(f"\n<b>{sec_title}</b> — {len(items)}")
        for t, nm in items:
            shown_name = short_name(nm, 80) if nm else "—"
            lines.append(f"{_esc(t)} | {_esc(shown_name)}")

    return "\n".join(lines)

_TICKER_RX = re.compile(r"[A-Za-z0-9._-]+")
def _parse_tickers(text: str) -> List[str]:
    return [t.upper() for t in _TICKER_RX.findall(text or "")]

def _esc(s: str) -> str:
    return html.escape(s, quote=False)

def header_lines(state: ScreenerState) -> List[str]:
    def _esc(s: str) -> str:
        return html.escape(s, quote=False)

    sort_txt = sort_display_field_name(state)
    ord_txt = "Desc" if state.sort_desc else "Asc"
    group_label_map = {
        "none":"No group",
        "rating":"Credit rating",
        "sector":"Sector",
        "country":"Country",
        "coupons":"Coupons per year",
        "has_offer":"Put/Call option",
        "term_bucket":"Time to maturity (buckets)",
        "issue_size_bucket":"Issue size (buckets)",
    }
    group_txt = group_label_map.get(state.group, state.group)

    f = state.filters
    lines = []
    lines.append("📊 <b>Bond screener</b>")
    lines.append(f"↕️ Sort: <b>{_esc(sort_txt)}</b> | Order: <b>{ord_txt}</b>")
    lines.append(f"📦 Group: <b>{_esc(group_txt)}</b> | Group size: <b>{state.page_size}</b>")
    lines.append("🔍 Filters:")

    filters_lines: List[str] = []

    # доходности
    if f.ytm:   filters_lines.append("YTM " + _esc(f.ytm.as_text()) + "%")
    if f.ytc:   filters_lines.append("YTC " + _esc(f.ytc.as_text()) + "%")
    if f.ytw:   filters_lines.append("YTW " + _esc(f.ytw.as_text()) + "%")
    if f.ytm_w: filters_lines.append("YTM (w) " + _esc(f.ytm_w.as_text()) + "%")
    if f.ytc_w: filters_lines.append("YTC (w) " + _esc(f.ytc_w.as_text()) + "%")
    if f.ytw_w: filters_lines.append("YTW (w) " + _esc(f.ytw_w.as_text()) + "%")

    # базовые длительности
    if f.d_mac:   filters_lines.append("Dmac " + _esc(f.d_mac.as_text()))
    if f.d_mod:   filters_lines.append("Dmod " + _esc(f.d_mod.as_text()))
    if f.d_mac_w: filters_lines.append("Dmac (w) " + _esc(f.d_mac_w.as_text()))
    if f.d_mod_w: filters_lines.append("Dmod (w) " + _esc(f.d_mod_w.as_text()))

    # длительности по сценариям (включая (w))
    if f.d_mac_ytm:   filters_lines.append("Dmac@YTM " + _esc(f.d_mac_ytm.as_text()))
    if f.d_mac_ytc:   filters_lines.append("Dmac@YTC " + _esc(f.d_mac_ytc.as_text()))
    if f.d_mac_ytw:   filters_lines.append("Dmac@YTW " + _esc(f.d_mac_ytw.as_text()))
    if f.d_mod_ytm:   filters_lines.append("Dmod@YTM " + _esc(f.d_mod_ytm.as_text()))
    if f.d_mod_ytc:   filters_lines.append("Dmod@YTC " + _esc(f.d_mod_ytc.as_text()))
    if f.d_mod_ytw:   filters_lines.append("Dmod@YTW " + _esc(f.d_mod_ytw.as_text()))
    if f.d_mac_ytm_w: filters_lines.append("Dmac@YTM (w) " + _esc(f.d_mac_ytm_w.as_text()))
    if f.d_mac_ytc_w: filters_lines.append("Dmac@YTC (w) " + _esc(f.d_mac_ytc_w.as_text()))
    if f.d_mac_ytw_w: filters_lines.append("Dmac@YTW (w) " + _esc(f.d_mac_ytw_w.as_text()))
    if f.d_mod_ytm_w: filters_lines.append("Dmod@YTM (w) " + _esc(f.d_mod_ytm_w.as_text()))
    if f.d_mod_ytc_w: filters_lines.append("Dmod@YTC (w) " + _esc(f.d_mod_ytc_w.as_text()))
    if f.d_mod_ytw_w: filters_lines.append("Dmod@YTW (w) " + _esc(f.d_mod_ytw_w.as_text()))

    # ΔP@-1%
    if f.dpp_down_1pct:   filters_lines.append("ΔP@-1% " + _esc(f.dpp_down_1pct.as_text()) + "%")
    if f.dpp_down_1pct_w: filters_lines.append("ΔP@-1% (w) " + _esc(f.dpp_down_1pct_w.as_text()) + "%")

    # % change при -1% по сценариям
    if f.dpp_ytm:   filters_lines.append("%↓1%@YTM " + _esc(f.dpp_ytm.as_text()) + "%")
    if f.dpp_ytc:   filters_lines.append("%↓1%@YTC " + _esc(f.dpp_ytc.as_text()) + "%")
    if f.dpp_ytw:   filters_lines.append("%↓1%@YTW " + _esc(f.dpp_ytw.as_text()) + "%")
    if f.dpp_ytm_w: filters_lines.append("%↓1%@YTM(w) " + _esc(f.dpp_ytm_w.as_text()) + "%")
    if f.dpp_ytc_w: filters_lines.append("%↓1%@YTC(w) " + _esc(f.dpp_ytc_w.as_text()) + "%")
    if f.dpp_ytw_w: filters_lines.append("%↓1%@YTW(w) " + _esc(f.dpp_ytw_w.as_text()) + "%")

    # DtA
    if f.dtm_days:  filters_lines.append("DtM " + _esc(f.dtm_days.as_text()) + "d")
    if f.offer_days:filters_lines.append("DtC " + _esc(f.offer_days.as_text()) + "d")
    if f.term_days: filters_lines.append("DtW " + _esc(f.term_days.as_text()) + "d")

    # списки/прочее
    if f.ratings:  filters_lines.append("Ratings [" + ",".join(rating_sorted(set(f.ratings))) + "]")
    if f.sectors:  filters_lines.append("Sectors [" + ",".join(sorted(set(f.sectors))) + "]")
    if f.countries:filters_lines.append("Countries [" + ",".join(sorted(set(f.countries))) + "]")
    if f.coupons_per_year:
        filters_lines.append("Coupons/year [" + ",".join(sorted(str(x) for x in f.coupons_per_year)) + "]")

    if f.price:     filters_lines.append("Price " + _esc(f.price.as_text()))
    if f.volume:    filters_lines.append("Vol₽ " + _esc(f.volume.as_text()))
    if f.nominal:   filters_lines.append("Par " + _esc(f.nominal.as_text()))
    if f.issue_size:filters_lines.append("Issue (₽) " + _esc(f.issue_size.as_text()))
    if f.vol_issue_pct:
        filters_lines.append("Vol/Issue% " + _esc(f.vol_issue_pct.as_text()) + "%")
    if f.has_offer is not None:
        filters_lines.append("Put/Call " + ("only" if f.has_offer else "excluding"))

    if not filters_lines:
        lines.append("—")
    else:
        lines.extend(filters_lines)

    # список отображаемых полей (в порядке показа)
    sort_disp = sort_display_field_name(state)
    cols_line_parts = []
    for col in state.display_fields:
        item = _esc(col)
        if col == sort_disp:
            item = f"<b>{item}</b>"
        cols_line_parts.append(item)

    # один пустой отступ и список полей
    lines.append("")
    lines.append("🔤 Fields: " + " | ".join(cols_line_parts))

    lines.append("")

    return lines

def build_blocks(state: ScreenerState, data: Dict[str, Any]) -> List[List[str]]:
    cols = state.display_fields
    sort_disp_field = sort_display_field_name(state)
    rows = make_rows(state, data)
    blocks: List[List[str]] = []
    cur_block: List[str] = []

    if not rows:
        return [["📉 Данные загружаются или фильтры ничего не нашли. Попробуйте изменить Filter/Settings."]]

    grouped = any(("_group_header" in r) for r in rows)
    if not grouped:
        for r in rows:
            line = _line_from_row(state, r, cols, sort_disp_field)   # ← передаём state
            blocks.append([line])
        return blocks

    for r in rows:
        if "_group_header" in r:
            if cur_block: blocks.append(cur_block)
            cur_block = [f"<b>{_esc(str(r['_group_header']))}</b>"]
        else:
            cur_block.append(_line_from_row(state, r, cols, sort_disp_field))  # ← передаём state
    if cur_block:
        blocks.append(cur_block)
    return blocks

def render_page(state: ScreenerState, data: Dict[str, Any]) -> Tuple[str, int]:
    head = header_lines(state)
    head_txt = "\n".join(head)
    head_len = len(head_txt)

    blocks = build_blocks(state, data)

    grouped_mode = any(len(b) > 1 or (b and b[0].startswith("<b>")) for b in blocks)
    sep_between_blocks = "\n\n" if grouped_mode else "\n"

    pages: List[str] = []
    acc = head_txt
    acc_len = head_len
    first_block = True

    for block in blocks:
        block_txt = ("\n" if first_block else sep_between_blocks) + "\n".join(block)
        blen = len(block_txt)
        if acc_len + blen <= SAFE_LIMIT:
            acc += block_txt; acc_len += blen
        else:
            if acc == head_txt:
                if len(block) == 1:
                    pages.append(acc + block_txt[:(SAFE_LIMIT - acc_len)])
                    acc = head_txt; acc_len = head_len
                else:
                    piece = [block[0]]
                    ptxt = ("\n" if first_block else sep_between_blocks) + "\n".join(piece)
                    plen = len(ptxt)
                    i = 1
                    while i < len(block):
                        nxt = "\n" + block[i]
                        if plen + len(nxt) + acc_len <= SAFE_LIMIT:
                            piece.append(block[i]); plen += len(nxt); i += 1
                        else:
                            break
                    pages.append(acc + (("\n" if first_block else sep_between_blocks) + "\n".join(piece)))
                    rest = block[i:]
                    acc = head_txt + (("\n" + "\n".join(rest)) if rest else "")
                    acc_len = len(acc)
            else:
                pages.append(acc)
                acc = head_txt + block_txt
                acc_len = len(acc)
        first_block = False

    if acc.strip():
        pages.append(acc)

    if not pages:
        pages = ["(empty)"]
    total = len(pages)
    if state.page_idx >= total: state.page_idx = total - 1
    if state.page_idx < 0: state.page_idx = 0
    text = pages[state.page_idx]
    footer = f"\n\n— Page {state.page_idx+1}/{total} —"
    if len(text) + len(footer) <= SAFE_LIMIT:
        text = text + footer
    state.pages_count = total
    return text, total

# ====================== Списки стран/секторов/купонов ======================
def collect_all_sectors(data: Dict[str, Any]) -> List[str]:
    # Берём только те сектора, что реально присутствуют в данных
    return sorted({v for v in data["sector"].values() if v})

def collect_all_countries(data: Dict[str, Any]) -> List[str]:
    vals = [v for v in data["country"].values()]
    out = sorted({v for v in vals if v})
    if any(v in (None, "", "-") for v in vals):
        out.append("Unknown")  # добавляем кнопку неизвестной страны
    return out

def collect_all_coupons(data: Dict[str, Any]) -> List[int]:
    vals = sorted({v for v in data["coupons_per_year"].values() if isinstance(v, int)})
    return vals

# ====================== Клавиатуры (навигация всегда последним рядом!) ======================
def nav_row(state: ScreenerState) -> List[InlineKeyboardButton]:
    return [
        InlineKeyboardButton(text="⏮", callback_data="nav:first"),
        InlineKeyboardButton(text="◀", callback_data="nav:prev"),
        InlineKeyboardButton(text=f"{state.page_idx+1}/{max(1,state.pages_count)}", callback_data="nav:noop"),
        InlineKeyboardButton(text="▶", callback_data="nav:next"),
        InlineKeyboardButton(text="⏭", callback_data="nav:last"),
    ]

def main_keyboard(state: ScreenerState) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="Sort", callback_data="menu:sort")
    kb.button(text="Group", callback_data="menu:group")
    kb.button(text="Filter", callback_data="menu:filter")
    kb.button(text="Settings", callback_data="menu:settings")
    kb.button(text="Presets", callback_data="menu:presets")
    kb.adjust(3, 2)
    kb.row(*nav_row(state))
    return kb.as_markup()

def list_edit_keyboard(which: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    if which == "wl":
        kb.button(text="➕ Add", callback_data="wl:add")
        kb.button(text="➖ Remove", callback_data="wl:del")
    else:
        kb.button(text="➕ Add", callback_data="bl:add")
        kb.button(text="➖ Remove", callback_data="bl:del")
    kb.adjust(2)
    return kb.as_markup()

# --- Sort ---
def sort_keyboard(state: ScreenerState) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="Yield ▸", callback_data="sortcat:yield")
    kb.button(text="Duration ▸", callback_data="sortcat:duration")
    kb.button(text="% change if yield ↓ 1% ▸", callback_data="sortcat:dpp")
    kb.button(text="DtA ▸", callback_data="sortcat:dta")
    kb.button(text="Last price", callback_data="sort:set:PRICE")
    kb.button(text="Volume (₽)", callback_data="sort:set:VOLUME")
    kb.button(text="Issue size (₽)", callback_data="sort:set:ISSUE_MONEY")  # ← ДОБАВЛЕНО
    kb.button(text="Volume/Issue size", callback_data="sort:set:VOLISSUE")
    kb.button(text="← Back", callback_data="menu:main")
    kb.adjust(2)
    kb.row(*nav_row(state))
    return kb.as_markup()

def sort_dta_keyboard(state: ScreenerState) -> InlineKeyboardMarkup:
    """
    DtA: DtM, DtC, DtW. Для DtW учитываем флаг стиля сортировки (w) только на этапе расчёта,
    отдельной (w)-кнопки не даём — как вы и просили.
    """
    kb = InlineKeyboardBuilder()
    kb.button(text="DtM", callback_data="sort:set:DTA_DTM:n")
    kb.button(text="DtC", callback_data="sort:set:DTA_DTC:n")
    kb.button(text="DtW", callback_data="sort:set:DTA_DTW:n")
    kb.button(text="← Back", callback_data="menu:sort")
    kb.adjust(3)
    kb.row(*nav_row(state))
    return kb.as_markup()

def sort_yield_keyboard(state: ScreenerState) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for key,label,w in [
        ("YTM","YTM",False),("YTC","YTC",False),("YTW","YTW",False),
        ("YTM","YTM (w)",True),("YTC","YTC (w)",True),("YTW","YTW (w)",True),
    ]:
        kb.button(text=label, callback_data=f"sort:set:{key}:{'w' if w else 'n'}")
    kb.button(text="← Back", callback_data="menu:sort")
    kb.adjust(3)
    kb.row(*nav_row(state))
    return kb.as_markup()

def sort_duration_keyboard(state: ScreenerState) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    # 12 кнопок: Mac & Mod × (YTM/YTC/YTW) × (n/w)
    items = [
        ("D Macaulay — YTM", "D_MAC_YTM", "n"), ("D Macaulay — YTM (w)", "D_MAC_YTM", "w"),
        ("D Macaulay — YTC", "D_MAC_YTC", "n"), ("D Macaulay — YTC (w)", "D_MAC_YTC", "w"),
        ("D Macaulay — YTW", "D_MAC_YTW", "n"), ("D Macaulay — YTW (w)", "D_MAC_YTW", "w"),
        ("D Modified — YTM", "D_MOD_YTM", "n"), ("D Modified — YTM (w)", "D_MOD_YTM", "w"),
        ("D Modified — YTC", "D_MOD_YTC", "n"), ("D Modified — YTC (w)", "D_MOD_YTC", "w"),
        ("D Modified — YTW", "D_MOD_YTW", "n"), ("D Modified — YTW (w)", "D_MOD_YTW", "w"),
    ]
    for label, key, w in items:
        kb.button(text=label, callback_data=f"sort:set:{key}:{w}")
    kb.button(text="← Back", callback_data="menu:sort")
    kb.adjust(2)
    kb.row(*nav_row(state))
    return kb.as_markup()

def presets_keyboard(state: ScreenerState) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    # builtin
    labels = [
        ("Default", "default"),
        ("Desktop 1", "desk1"),
        ("Desktop 2", "desk2"),
        ("Desktop 3", "desk3"),
        ("Phone 1", "phone1"),
        ("Phone 2", "phone2"),
        ("Phone 3", "phone3"),
    ]
    for text, key in labels:
        kb.button(text=text, callback_data=f"preset:apply:{key}")
    # user
    if state.presets:
        for name in sorted(state.presets.keys()):
            kb.button(text=name, callback_data=f"preset:apply:user:{quote(name)}")
    kb.button(text="💾 Save current", callback_data="preset:save")
    kb.button(text="← Back", callback_data="menu:main")
    kb.adjust(1)
    kb.row(*nav_row(state))
    return kb.as_markup()

def sort_dpp_keyboard(state: ScreenerState) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    labels = [
        ("% change if yield ↓ 1% — YTM", "DPP_YTM", "n"),
        ("% change if yield ↓ 1% — YTM (w)", "DPP_YTM", "w"),
        ("% change if yield ↓ 1% — YTC", "DPP_YTC", "n"),
        ("% change if yield ↓ 1% — YTC (w)", "DPP_YTC", "w"),
        ("% change if yield ↓ 1% — YTW", "DPP_YTW", "n"),
        ("% change if yield ↓ 1% — YTW (w)", "DPP_YTW", "w"),
    ]
    for label, key, w in labels:
        kb.button(text=label, callback_data=f"sort:set:{key}:{w}")
    kb.button(text="← Back", callback_data="menu:sort")
    kb.adjust(1)
    kb.row(*nav_row(state))
    return kb.as_markup()

# --- Group ---
def group_keyboard(state: ScreenerState) -> InlineKeyboardMarkup:
    opts = [
        ("No group", "none"),
        ("Credit rating", "rating"),
        ("Sector", "sector"),
        ("Country", "country"),
        ("Coupons per year", "coupons"),
        ("Put/Call option", "has_offer"),
        ("Time to maturity (buckets)", "term_bucket"),
        ("Issue size (buckets)", "issue_size_bucket"),
    ]
    kb = InlineKeyboardBuilder()
    for t, k in opts:
        label = f"✓ {t}" if state.group == k else t
        kb.button(text=label, callback_data=f"group:{k}")
    kb.button(text="← Back", callback_data="menu:main")
    kb.adjust(1)
    kb.row(*nav_row(state))
    return kb.as_markup()

# --- Filter (главное меню) ---
def filter_keyboard(state: ScreenerState) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="Yield ▸", callback_data="filtercat:yield")
    kb.button(text="Duration ▸", callback_data="filtercat:duration")
    kb.button(text="% change if yield ↓ 1% ▸", callback_data="filtercat:dpp")
    kb.button(text="Credit ratings ▸", callback_data="filter:ratings")
    kb.button(text="Sectors ▸", callback_data="filter:sectors")
    kb.button(text="Countries ▸", callback_data="filter:countries")
    kb.button(text="Coupons per year ▸", callback_data="filter:coupons")
    kb.button(text="DtA ▸", callback_data="filtercat:dta")  # ← вместо старого Days to maturity
    kb.button(text="Last price", callback_data="filter:range:price")
    kb.button(text="Volume (₽)", callback_data="filter:range:volume_money")
    kb.button(text="Par value", callback_data="filter:range:nominal")
    kb.button(text="Issue size", callback_data="filter:range:issue")
    kb.button(text="Put/Call option", callback_data="filter:has_offer_cycle")
    kb.button(text="Clear filters", callback_data="filter:clear")
    kb.button(text="← Back", callback_data="menu:main")
    kb.adjust(2)
    kb.row(*nav_row(state))
    return kb.as_markup()

def filter_dta_keyboard(state: ScreenerState) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="DtM", callback_data="filter:range:dtm")   # до погашения (константа)
    kb.button(text="DtC", callback_data="filter:range:dtc")   # до оферты (0 если нет)
    kb.button(text="DtW", callback_data="filter:range:dtw")   # до YTW / weighted YTW
    kb.button(text="← Back", callback_data="menu:filter")
    kb.adjust(3)
    kb.row(*nav_row(state))
    return kb.as_markup()

def filter_yield_keyboard(state: ScreenerState) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="YTM", callback_data="filter:range:ytm")
    kb.button(text="YTC", callback_data="filter:range:ytc")
    kb.button(text="YTW", callback_data="filter:range:ytw")
    kb.button(text="YTM (w)", callback_data="filter:range:ytm_w")
    kb.button(text="YTC (w)", callback_data="filter:range:ytc_w")
    kb.button(text="YTW (w)", callback_data="filter:range:ytw_w")
    kb.button(text="← Back", callback_data="menu:filter")
    kb.adjust(3)
    kb.row(*nav_row(state))
    return kb.as_markup()

def filter_duration_keyboard(state: ScreenerState) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    items = [
        ("D Macaulay — YTM", "dmac_ytm"), ("D Macaulay — YTM (w)", "dmac_ytm_w"),
        ("D Macaulay — YTC", "dmac_ytc"), ("D Macaulay — YTC (w)", "dmac_ytc_w"),
        ("D Macaulay — YTW", "dmac_ytw"), ("D Macaulay — YTW (w)", "dmac_ytw_w"),
        ("D Modified — YTM", "dmod_ytm"), ("D Modified — YTM (w)", "dmod_ytm_w"),
        ("D Modified — YTC", "dmod_ytc"), ("D Modified — YTC (w)", "dmod_ytc_w"),
        ("D Modified — YTW", "dmod_ytw"), ("D Modified — YTW (w)", "dmod_ytw_w"),
    ]
    for label, key in items:
        kb.button(text=label, callback_data=f"filter:range:{key}")
    kb.button(text="← Back", callback_data="menu:filter")
    kb.adjust(2)
    kb.row(*nav_row(state))
    return kb.as_markup()

def filter_dpp_keyboard(state: ScreenerState) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    items = [
        ("% change if yield ↓ 1% — YTM", "dpp_ytm"),
        ("% change if yield ↓ 1% — YTM (w)", "dpp_ytm_w"),
        ("% change if yield ↓ 1% — YTC", "dpp_ytc"),
        ("% change if yield ↓ 1% — YTC (w)", "dpp_ytc_w"),
        ("% change if yield ↓ 1% — YTW", "dpp_ytw"),
        ("% change if yield ↓ 1% — YTW (w)", "dpp_ytw_w"),
    ]
    for label, key in items:
        kb.button(text=label, callback_data=f"filter:range:{key}")
    kb.button(text="← Back", callback_data="menu:filter")
    kb.adjust(1)  # по одной кнопке в ряд — максимум ширины
    kb.row(*nav_row(state))
    return kb.as_markup()

# --- Фильтр: рейтинги/сектора/страны/купоны ---
def ratings_keyboard(selected: Optional[Set[str]], state: ScreenerState) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for b in RATING_ORDER:
        mark = "✓ " if (selected and b in selected) else ""
        kb.button(text=f"{mark}{b}", callback_data=f"filter:rating_toggle:{b}")
    kb.button(text="Готово", callback_data="menu:filter")
    kb.adjust(5)
    kb.row(*nav_row(state))
    return kb.as_markup()

def sectors_keyboard(options: List[str], selected: Optional[Set[str]], state: ScreenerState) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for s in options:
        mark = "✓ " if (selected and s in selected) else ""
        disp = str(s).replace("_", " ").title() if s else "Other"
        kb.button(text=f"{mark}{disp}", callback_data=f"filter:sector_toggle:{quote(s)}")
    kb.button(text="Готово", callback_data="menu:filter")
    kb.adjust(3)
    kb.row(*nav_row(state))
    return kb.as_markup()

def countries_keyboard(options: List[str], selected: Optional[Set[str]], state: ScreenerState) -> InlineKeyboardMarkup:
    """
    Используем индекс опции вместо полного названия страны в callback_data,
    чтобы не превышать лимит 64 байта у Telegram.
    """
    kb = InlineKeyboardBuilder()
    for i, c in enumerate(options):
        val = c if (c is not None and c != "") else "Unknown"
        mark = "✓ " if (selected and val in selected) else ""
        # короткий callback по индексу, НЕ по названию
        kb.button(text=f"{mark}{val}", callback_data=f"filter:country_toggle_i:{i}")
    kb.button(text="Готово", callback_data="menu:filter")
    kb.adjust(3)
    kb.row(*nav_row(state))
    return kb.as_markup()

def coupons_keyboard(options: List[int], selected: Optional[Set[int]], state: ScreenerState) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for n in options:
        mark = "✓ " if (selected and n in selected) else ""
        kb.button(text=f"{mark}{n}", callback_data=f"filter:coupon_toggle:{n}")
    kb.button(text="Готово", callback_data="menu:filter")
    kb.adjust(5)
    kb.row(*nav_row(state))
    return kb.as_markup()

def settings_keyboard(state: ScreenerState) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text=f"Order: {'Desc' if state.sort_desc else 'Asc'}", callback_data="settings:toggle_order")
    kb.button(text=f"Page size: {state.page_size}", callback_data="settings:pagesize")
    kb.button(text="Fields (display)", callback_data="settings:fields")
    kb.button(text=f"{'Watchlist' if state.use_watchlist else 'All list'}", callback_data="settings:watch_toggle")
    kb.button(text="Bond to blacklist", callback_data="settings:blacklist")
    kb.button(text="← Back", callback_data="menu:main")
    kb.adjust(2)
    kb.row(*nav_row(state))
    return kb.as_markup()

# Новое: корневое меню полей с разделами
def settings_fields_root(state: ScreenerState) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="Yield ▸", callback_data="fields_cat:yield")
    kb.button(text="Duration ▸", callback_data="fields_cat:duration")
    kb.button(text="% change if yield ↓ 1% ▸", callback_data="fields_cat:dpp")
    kb.button(text="DtA ▸", callback_data="fields_cat:dta")
    kb.button(text="Others ▸", callback_data="fields_cat:others")
    kb.button(text="← Back", callback_data="menu:settings")
    kb.adjust(2)
    kb.row(*nav_row(state))
    return kb.as_markup()

def fields_cat_keyboard(state: ScreenerState, cat: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    groups = {
        "yield": ["YTM","YTC","YTW","YTM (w)","YTC (w)","YTW (w)"],
        "duration": [
            "D_mac@YTM","D_mac@YTM (w)","D_mac@YTC","D_mac@YTC (w)","D_mac@YTW","D_mac@YTW (w)",
            "D_mod@YTM","D_mod@YTM (w)","D_mod@YTC","D_mod@YTC (w)","D_mod@YTW","D_mod@YTW (w)",
        ],
        "dpp": [
            "% change if yield ↓ 1% @YTM","% change if yield ↓ 1% @YTM (w)",
            "% change if yield ↓ 1% @YTC","% change if yield ↓ 1% @YTC (w)",
            "% change if yield ↓ 1% @YTW","% change if yield ↓ 1% @YTW (w)",
        ],
        "dta": ["DtM","DtC","DtW"],
        "others": ["Name","Ticker","Price","Vol","Vol₽","Issue (₽)","Vol/Issue%","Rating","Sector","Country","Coupons/year"],  # ← ДОБАВЛЕНО Issue (₽)
    }
    for f in groups[cat]:
        if f in state.display_fields:
            idx = state.display_fields.index(f) + 1
            label = f"✓ {idx}. {f}"
        else:
            label = f
        kb.button(text=label, callback_data=f"fields:toggle:{f}")
    kb.button(text="Готово", callback_data="fields:done")
    kb.button(text="← Back", callback_data="settings:fields")
    kb.adjust(2)
    kb.row(*nav_row(state))
    return kb.as_markup()

def fields_keyboard(state: ScreenerState) -> InlineKeyboardMarkup:
    # Старое меню (не используется новым роутингом, оставлено для совместимости)
    kb = InlineKeyboardBuilder()
    for f in ALLOWED_FIELDS:
        if f in state.display_fields:
            idx = state.display_fields.index(f) + 1
            kb.button(text=f"✓ {idx}. {f}", callback_data=f"fields:toggle:{f}")
        else:
            kb.button(text=f"{f}", callback_data=f"fields:toggle:{f}")
    kb.button(text="Готово", callback_data="fields:done")
    kb.adjust(2)
    kb.row(*nav_row(state))
    return kb.as_markup()

def current_markup(state: ScreenerState) -> InlineKeyboardMarkup:
    if state.ui_mode == "sort": return sort_keyboard(state)
    if state.ui_mode == "sort_yield": return sort_yield_keyboard(state)
    if state.ui_mode == "sort_duration": return sort_duration_keyboard(state)
    if state.ui_mode == "sort_dpp": return sort_dpp_keyboard(state)
    if state.ui_mode == "sort_dta": return sort_dta_keyboard(state)

    if state.ui_mode == "group": return group_keyboard(state)

    if state.ui_mode == "filter": return filter_keyboard(state)
    if state.ui_mode == "filter_yield": return filter_yield_keyboard(state)
    if state.ui_mode == "filter_duration": return filter_duration_keyboard(state)
    if state.ui_mode == "filter_dpp": return filter_dpp_keyboard(state)
    if state.ui_mode == "filter_dta": return filter_dta_keyboard(state)

    # ➜ ДОБАВИТЬ:
    if state.ui_mode == "filter_ratings":
        return ratings_keyboard(state.filters.ratings or set(), state)
    if state.ui_mode == "filter_sectors":
        opts = collect_all_sectors(snapshot_data())
        return sectors_keyboard(opts, state.filters.sectors or set(), state)
    if state.ui_mode == "filter_countries":
        opts = collect_all_countries(snapshot_data())
        return countries_keyboard(opts, state.filters.countries or set(), state)
    if state.ui_mode == "filter_coupons":
        opts = collect_all_coupons(snapshot_data())
        return coupons_keyboard(opts, state.filters.coupons_per_year or set(), state)
    # ⬅︎ ДОБАВИТЬ
    if state.ui_mode == "presets": return presets_keyboard(state)

    if state.ui_mode == "settings": return settings_keyboard(state)
    if state.ui_mode == "settings_fields_root": return settings_fields_root(state)
    if state.ui_mode.startswith("settings_fields_cat:"):
        cat = state.ui_mode.split(":",1)[1]
        return fields_cat_keyboard(state, cat)

    if state.ui_mode == "settings_fields": return fields_keyboard(state)
    return main_keyboard(state)

# ====================== Редактирование одного сообщения ======================
bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
dp.callback_query.middleware(IdleTouchMiddleware(bot))

async def _schedule_edit(st: ScreenerState, text: str, delay: float):
    await asyncio.sleep(max(0.0, delay))
    try:
        await bot.edit_message_text(
            chat_id=st.chat_id,
            message_id=st.message_id,
            text=text,
            reply_markup=current_markup(st),
            link_preview_options=LP_DISABLED
        )
        st.last_render_hash = hash(f"{st.page_idx}|{st.ui_mode}|{text}")
        st.last_edit_ts = time.time()
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            st.last_render_hash = hash(f"{st.page_idx}|{st.ui_mode}|{text}")
            st.last_edit_ts = time.time()
    except Exception:
        pass

async def safe_edit(st: ScreenerState, text: str):
    new_hash = hash(f"{st.page_idx}|{st.ui_mode}|{text}")
    now = time.time()
    if st.last_render_hash is not None and new_hash == st.last_render_hash:
        return
    delta = now - st.last_edit_ts
    if delta <= EDIT_THROTTLE_SECONDS:
        # мягкая отложенная правка, чтобы второй клик не «терялся»
        delay = (EDIT_THROTTLE_SECONDS - delta) + 0.05
        asyncio.create_task(_schedule_edit(st, text, delay))
        return
    try:
        await bot.edit_message_text(
            chat_id=st.chat_id,
            message_id=st.message_id,
            text=text,
            reply_markup=current_markup(st),
            link_preview_options=LP_DISABLED
        )
        st.last_render_hash = new_hash
        st.last_edit_ts = now
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            st.last_render_hash = new_hash
            st.last_edit_ts = now
        else:
            if "text is too long" in str(e).lower():
                global SAFE_LIMIT
                SAFE_LIMIT = max(1200, int(SAFE_LIMIT * 0.9))
            pass
    except Exception:
        pass

async def rerender_one(user_id: int):
    st = USER_STATES.get(user_id)
    if not st or not st.active or not st.chat_id or not st.message_id:
        return
    text, _ = render_page(st, snapshot_data())
    await safe_edit(st, text)

async def rerender_all_active():
    for uid in list(USER_STATES.keys()):
        await rerender_one(uid)

def _awaiting_list_edit(message: Message) -> bool:
    st = USER_STATES.get(message.from_user.id)
    return bool(st and st.awaiting_input in {"wl:add","wl:del","bl:add","bl:del"})

def _awaiting_preset_save(m: Message) -> bool:
    st = USER_STATES.get(m.from_user.id)
    return bool(st and st.awaiting_input == "preset_save")

# ====================== Хэндлеры ======================
@dp.message(Command("start"))
async def on_start(m: Message):
    USER_STATES.setdefault(m.from_user.id, ScreenerState())
    await m.answer("Привет! Наберите /screener чтобы запустить живой облигационный скринер.", link_preview_options=LP_DISABLED)

@dp.message(Command("watchlist", "wl"))
async def cmd_watchlist(m: Message):
    st = USER_STATES.get(m.from_user.id)
    tickers = st.watchlist if st else set()
    text = _format_ticker_list_grouped("👀 Watchlist", tickers)
    msg = await m.answer(text, reply_markup=list_edit_keyboard("wl"))
    if st:
        st.list_view_msg_id = msg.message_id
        st.list_view_kind = "wl"
        st.awaiting_input = None
        st.prompt_msg_id = None

@dp.message(Command("blacklist", "bl"))
async def cmd_blacklist(m: Message):
    st = USER_STATES.get(m.from_user.id)
    tickers = st.blacklist if st else set()
    text = _format_ticker_list_grouped("⛔ Blacklist", tickers)
    msg = await m.answer(text, reply_markup=list_edit_keyboard("bl"))
    if st:
        st.list_view_msg_id = msg.message_id
        st.list_view_kind = "bl"
        st.awaiting_input = None
        st.prompt_msg_id = None

@dp.message(Command("screener", "sc"))
async def on_screener(m: Message):
    st = USER_STATES.setdefault(m.from_user.id, ScreenerState())
    st.active = True
    st.chat_id = m.chat.id
    st.message_id = None
    st.awaiting_input = None
    st.prompt_msg_id = None
    st.ui_mode = "main"
    _cancel_idle_timer(st)
    st.page_idx = 0

    text, _ = render_page(st, snapshot_data())
    msg = await m.answer(text, reply_markup=current_markup(st), link_preview_options=LP_DISABLED)
    st.message_id = msg.message_id
    st.last_render_hash = hash(f"{st.page_idx}|{st.ui_mode}|{text}")

@dp.message(F.text, ~F.text.startswith("/"), _awaiting_list_edit)
async def on_list_edit_input(m: Message, bot: Bot):
    st = USER_STATES.get(m.from_user.id)
    tag = st.awaiting_input if st else None
    if tag not in {"wl:add","wl:del","bl:add","bl:del"}:
        return

    tickers = _parse_tickers(m.text)
    added, removed = [], []

    if tag.startswith("wl:"):
        for t in tickers:
            if tag.endswith("add"):
                if t not in st.watchlist:
                    st.watchlist.add(t); added.append(t)
                st.blacklist.discard(t)  # не даём конфликтовать
            else:
                if t in st.watchlist:
                    st.watchlist.discard(t); removed.append(t)
        which = "wl"
    else:
        for t in tickers:
            if tag.endswith("add"):
                if t not in st.blacklist:
                    st.blacklist.add(t); added.append(t)
                st.watchlist.discard(t)  # не даём конфликтовать
            else:
                if t in st.blacklist:
                    st.blacklist.discard(t); removed.append(t)
        which = "bl"

    # Сбрасываем ожидание и удаляем подсказку
    st.awaiting_input = None
    try:
        if st.prompt_msg_id:
            await bot.delete_message(chat_id=m.chat.id, message_id=st.prompt_msg_id)
    except TelegramBadRequest:
        pass
    st.prompt_msg_id = None

    # Перерисовываем исходное сообщение со списком
    if which == "wl":
        txt = _format_ticker_list_grouped("👀 Watchlist", st.watchlist)
    else:
        txt = _format_ticker_list_grouped("⛔ Blacklist", st.blacklist)

    kb = list_edit_keyboard(which)
    try:
        if st.list_view_msg_id:
            await bot.edit_message_text(
                chat_id=m.chat.id,
                message_id=st.list_view_msg_id,
                text=txt,
                reply_markup=kb
            )
        else:
            msg = await m.answer(txt, reply_markup=kb)
            st.list_view_msg_id = msg.message_id
    except TelegramBadRequest:
        msg = await m.answer(txt, reply_markup=kb)
        st.list_view_msg_id = msg.message_id

    # Скрываем сообщение пользователя с тикерами
    try:
        await bot.delete_message(m.chat.id, m.message_id)
    except Exception:
        pass

    # Если открыт скринер — сразу обновим его
    await rerender_one(m.from_user.id)

@dp.callback_query(F.data.startswith("menu:"))
async def on_menu(q: CallbackQuery):
    st = USER_STATES.setdefault(q.from_user.id, ScreenerState())
    action = q.data.split(":", 1)[1]
    st.awaiting_input = None
    st.ui_mode = {
        "main": "main",
        "sort": "sort",
        "group": "group",
        "filter": "filter",
        "settings": "settings",
        "presets": "presets",  # ← добавили
    }[action]
    if action == "main":
        _cancel_idle_timer(st)
    st.page_idx = 0
    await rerender_one(q.from_user.id)
    await q.answer()

@dp.callback_query(F.data.in_({"wl:add","wl:del","bl:add","bl:del"}))
async def on_list_edit_start(q: CallbackQuery, bot: Bot):
    st = USER_STATES.get(q.from_user.id)
    if not st:
        await q.answer()
        return
    st.awaiting_input = q.data                 # 'wl:add' | 'wl:del' | 'bl:add' | 'bl:del'
    if q.message:
        st.list_view_msg_id = q.message.message_id  # сообщение списка, которое будем редактировать
    prompt = await q.message.answer("Введите тикер(ы) через пробел или запятую. Пример: SU26238RMFS6 RU000A105ABC")
    st.prompt_msg_id = prompt.message_id
    await q.answer("Жду тикер(ы)…", show_alert=False)

@dp.callback_query(F.data == "action:refresh")
async def on_refresh(q: CallbackQuery):
    await rerender_one(q.from_user.id)
    await q.answer("Обновлено")

# --- навигация ---
@dp.callback_query(F.data.startswith("nav:"))
async def on_nav(q: CallbackQuery):
    st = USER_STATES.setdefault(q.from_user.id, ScreenerState())
    cmd = q.data.split(":",1)[1]
    _ = render_page(st, snapshot_data())  # пересчёт страниц
    if cmd == "noop":
        await q.answer(); return
    if cmd == "first": st.page_idx = 0
    elif cmd == "prev": st.page_idx = max(0, st.page_idx - 1)
    elif cmd == "next": st.page_idx = min(max(0, st.pages_count - 1), st.page_idx + 1)
    elif cmd == "last": st.page_idx = max(0, st.pages_count - 1)
    await rerender_one(q.from_user.id)
    await q.answer()

# --- сортировка (категории) ---
@dp.callback_query(F.data.startswith("sortcat:"))
async def on_sortcat(q: CallbackQuery):
    st = USER_STATES.setdefault(q.from_user.id, ScreenerState())
    cat = q.data.split(":",1)[1]
    st.ui_mode = {
        "yield": "sort_yield",
        "duration": "sort_duration",
        "dpp": "sort_dpp",
        "dta": "sort_dta",              # ← добавлено
    }.get(cat, "sort")
    await rerender_one(q.from_user.id)
    await q.answer()

@dp.callback_query(F.data.startswith("sort:set:"))
async def on_sort_set(q: CallbackQuery):
    st = USER_STATES.setdefault(q.from_user.id, ScreenerState())
    # запоминаем старую метку сортировки ДО изменения
    old_label = sort_display_field_name(st)

    parts = q.data.split(":")
    if len(parts) == 3:
        st.sort_field = parts[2]
        st.sort_weighted = False
    else:
        st.sort_field = parts[2]
        st.sort_weighted = (parts[3] == "w")

    # формируем новую метку сортировки ПОСЛЕ изменения
    new_label = sort_display_field_name(st)

    # заменяем старую метку в списке полей на новую (без сдвига позиции)
    if old_label in st.display_fields:
        idx = st.display_fields.index(old_label)
        st.display_fields[idx] = new_label
        # устраняем возможные дубликаты new_label
        while st.display_fields.count(new_label) > 1:
            st.display_fields.remove(new_label)
    else:
        if new_label not in st.display_fields:
            st.display_fields.append(new_label)

    st.page_idx = 0
    st.ui_mode = "main"
    _cancel_idle_timer(st)
    await rerender_one(q.from_user.id)
    await q.answer("Сортировка применена")

# --- группировка ---
@dp.callback_query(F.data.startswith("group:"))
async def on_group(q: CallbackQuery):
    st = USER_STATES.setdefault(q.from_user.id, ScreenerState())
    new_group = q.data.split(":",1)[1]
    st.group = "none" if st.group == new_group else new_group
    st.page_idx = 0
    st.ui_mode = "main"
    _cancel_idle_timer(st)
    await rerender_one(q.from_user.id)
    await q.answer("Группировка применена")

# --- фильтры: категории ---
@dp.callback_query(F.data.startswith("filtercat:"))
async def on_filtercat(q: CallbackQuery):
    st = USER_STATES.setdefault(q.from_user.id, ScreenerState())
    cat = q.data.split(":",1)[1]
    st.ui_mode = {
        "yield": "filter_yield",
        "duration": "filter_duration",
        "dpp": "filter_dpp",
        "dta": "filter_dta",        # ← добавлено
    }.get(cat, "filter")
    await rerender_one(q.from_user.id)
    await q.answer()

@dp.callback_query(F.data == "menu:presets")
async def on_menu_presets(q: CallbackQuery, bot: Bot):
    st = USER_STATES.get(q.from_user.id)
    if not st:
        return await q.answer()
    st.ui_mode = "presets"
    # фиксируем chat/message, если не заполнены
    if q.message:
        st.chat_id = st.chat_id or q.message.chat.id
        st.message_id = st.message_id or q.message.message_id
    try:
        await bot.edit_message_reply_markup(
            chat_id=st.chat_id, message_id=st.message_id,
            reply_markup=presets_keyboard(st)
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            raise
    await q.answer()

@dp.callback_query(F.data.startswith("preset:apply:"))
async def on_preset_apply(q: CallbackQuery, bot: Bot):
    st = USER_STATES.get(q.from_user.id)
    if not st:
        return await q.answer()
    key = q.data.split("preset:apply:", 1)[1]

    # достаём пресет (встроенный или пользовательский)
    preset: Dict[str, Any] = {}
    if key.startswith("user:"):
        name = unquote(key.split("user:", 1)[1])
        preset = st.presets.get(name) or {}
        # обратная совместимость: раньше хранили только список полей
        if isinstance(preset, list):
            preset = {"fields": preset}
    else:
        preset = BUILTIN_PRESETS.get(key) or {}
        if isinstance(preset, list):
            preset = {"fields": preset}

    # поля
    fields_in = list(preset.get("fields", []))
    canon: List[str] = []
    for f in fields_in:
        v = ALLOWED_LOOKUP.get(f.lower())
        if v:
            canon.append(v)
    st.display_fields = canon or _default_fields()

    # сортировка/веса/порядок — если заданы в пресете
    if "sort" in preset:  st.sort_field = preset["sort"]
    if "w" in preset:     st.sort_weighted = bool(preset["w"])
    if "desc" in preset:  st.sort_desc = bool(preset["desc"])
    if "group" in preset: st.group = preset["group"]

    ensure_sort_field_in_display_fields(st)
    st.ui_mode = "main"

    await rerender_one(q.from_user.id)
    await q.answer("Preset applied")

@dp.callback_query(F.data == "preset:save")
async def on_preset_save(q: CallbackQuery):
    st = USER_STATES.get(q.from_user.id)
    if not st:
        return await q.answer()
    st.awaiting_input = "preset_save"
    prompt = await q.message.answer(
        "Введите <b>название</b> пресета.\n\n"
        "Будут сохранены <i>текущие</i>: поля, сортировка (вкл. weighted/asc/desc) и группировка.",
        link_preview_options=LP_DISABLED
    )
    st.prompt_msg_id = prompt.message_id
    await q.answer("Waiting for name…")

@dp.message(F.text, ~F.text.startswith("/"), _awaiting_preset_save)
async def on_preset_save_input(m: Message, bot: Bot):
    st = USER_STATES.get(m.from_user.id)
    name = (m.text or "").strip()
    name = name[:50] if name else "Custom"

    st.presets[name] = {
        "fields": list(st.display_fields),
        "sort": st.sort_field,
        "w": st.sort_weighted,
        "desc": st.sort_desc,
        "group": st.group,
    }

    # очистка ожидания + удаляем подсказку и сообщение пользователя
    st.awaiting_input = None
    try:
        if st.prompt_msg_id:
            await bot.delete_message(m.chat.id, st.prompt_msg_id)
    except TelegramBadRequest:
        pass
    st.prompt_msg_id = None
    try:
        await bot.delete_message(m.chat.id, m.message_id)
    except TelegramBadRequest:
        pass

    # вернём клавиатуру пресетов (она изменится — добавится новый пункт)
    try:
        await bot.edit_message_reply_markup(
            chat_id=st.chat_id or m.chat.id,
            message_id=st.message_id,
            reply_markup=presets_keyboard(st)
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            raise

# --- фильтры: диапазоны (персонализированные приглашения, всё экранируем) ---
def pretty_key_and_unit(key: str) -> Tuple[str, str]:
    m = {
        "ytw": ("YTW", ""), "ytm": ("YTM", ""), "ytc": ("YTC", ""),
        "ytw_w": ("YTW (w)", ""), "ytm_w": ("YTM (w)", ""), "ytc_w": ("YTC (w)", ""),

        "dmac": ("D Macaulay", ""), "dmod": ("D Modified", ""),
        "dmac_w": ("D Macaulay (w)", ""), "dmod_w": ("D Modified (w)", ""),

        "dmac_ytm": ("D Macaulay @YTM", ""), "dmac_ytm_w": ("D Macaulay @YTM (w)", ""),
        "dmac_ytc": ("D Macaulay @YTC", ""), "dmac_ytc_w": ("D Macaulay @YTC (w)", ""),
        "dmac_ytw": ("D Macaulay @YTW", ""), "dmac_ytw_w": ("D Macaulay @YTW (w)", ""),
        "dmod_ytm": ("D Modified @YTM", ""), "dmod_ytm_w": ("D Modified @YTM (w)", ""),
        "dmod_ytc": ("D Modified @YTC", ""), "dmod_ytc_w": ("D Modified @YTC (w)", ""),
        "dmod_ytw": ("D Modified @YTW", ""), "dmod_ytw_w": ("D Modified @YTW (w)", ""),

        "dpp": ("ΔP@-1%", "%"), "dpp_w": ("ΔP@-1% (w)", "%"),
        "dpp_ytm": ("% change if yield ↓ 1% — YTM", "%"),
        "dpp_ytc": ("% change if yield ↓ 1% — YTC", "%"),
        "dpp_ytw": ("% change if yield ↓ 1% — YTW", "%"),
        "dpp_ytm_w": ("% change if yield ↓ 1% — YTM (w)", "%"),
        "dpp_ytc_w": ("% change if yield ↓ 1% — YTC (w)", "%"),
        "dpp_ytw_w": ("% change if yield ↓ 1% — YTW (w)", "%"),

        "dtm": ("DtM", "d"),
        "dtc": ("DtC", "d"),
        "dtw": ("DtW", "d"),

        "term": ("Days to maturity (legacy)", "d"),   # на случай старых кнопок
        "price": ("Last price", "₽"),
        "volume_money": ("Volume (₽)", "₽"),
        "volume": ("Volume (lots)", ""),              # бэкап, если вдруг
        "nominal": ("Par value", "₽"),
        "issue": ("Issue size (in millions)", ""),
    }
    return m.get(key, (key, ""))

@dp.callback_query(F.data.startswith("filter:range:"))
async def on_filter_range(q: CallbackQuery):
    st = USER_STATES.setdefault(q.from_user.id, ScreenerState())
    key = q.data.split(":",2)[2]
    st.awaiting_input = f"range:{key}"
    title, unit = pretty_key_and_unit(key)
    examples_raw = "10-15" if unit != "%" else "10-15%"
    ge_raw = ">=12.3" if unit != "%" else ">=12.3%"
    le_raw = "<=9.5" if unit != "%" else "<=9.5%"
    examples = html.escape(examples_raw, quote=False)
    ge = html.escape(ge_raw, quote=False)
    le = html.escape(le_raw, quote=False)
    text = (
        f"Введите диапазон для <b>{html.escape(title)}</b>\n"
        f"Форматы: <code>min-max</code>, <code>&gt;=x</code>, <code>&lt;=y</code>\n"
        f"Примеры: <code>{examples}</code>, <code>{ge}</code>, <code>{le}</code>\n"
        f"Отмена: /cancel"
    )
    msg = await q.message.answer(text, link_preview_options=LP_DISABLED)
    st.prompt_msg_id = msg.message_id
    await q.answer()

@dp.message(F.text.regexp(r'^/cancel$'))
async def on_cancel(m: Message):
    st = USER_STATES.setdefault(m.from_user.id, ScreenerState())
    try:
        if st.prompt_msg_id: await bot.delete_message(m.chat.id, st.prompt_msg_id)
        await bot.delete_message(m.chat.id, m.message_id)
    except Exception:
        pass
    st.awaiting_input = None
    st.prompt_msg_id = None
    await rerender_one(m.from_user.id)

def _assign_range(st: ScreenerState, key: str, rng: Optional[Range]):
    f = st.filters
    if key == "ytw": f.ytw = rng
    elif key == "ytm": f.ytm = rng
    elif key == "ytc": f.ytc = rng
    elif key == "ytw_w": f.ytw_w = rng
    elif key == "ytm_w": f.ytm_w = rng
    elif key == "ytc_w": f.ytc_w = rng
    elif key == "dmac": f.d_mac = rng
    elif key == "dmod": f.d_mod = rng
    elif key == "dmac_w": f.d_mac_w = rng
    elif key == "dmod_w": f.d_mod_w = rng
    elif key == "dpp": f.dpp_down_1pct = rng
    elif key == "dpp_w": f.dpp_down_1pct_w = rng

    elif key == "dmac_ytm": f.d_mac_ytm = rng
    elif key == "dmac_ytm_w": f.d_mac_ytm_w = rng
    elif key == "dmac_ytc": f.d_mac_ytc = rng
    elif key == "dmac_ytc_w": f.d_mac_ytc_w = rng
    elif key == "dmac_ytw": f.d_mac_ytw = rng
    elif key == "dmac_ytw_w": f.d_mac_ytw_w = rng
    elif key == "dmod_ytm": f.d_mod_ytm = rng
    elif key == "dmod_ytm_w": f.d_mod_ytm_w = rng
    elif key == "dmod_ytc": f.d_mod_ytc = rng
    elif key == "dmod_ytc_w": f.d_mod_ytc_w = rng
    elif key == "dmod_ytw": f.d_mod_ytw = rng
    elif key == "dmod_ytw_w": f.d_mod_ytw_w = rng

    elif key == "dpp_ytm": f.dpp_ytm = rng
    elif key == "dpp_ytc": f.dpp_ytc = rng
    elif key == "dpp_ytw": f.dpp_ytw = rng
    elif key == "dpp_ytm_w": f.dpp_ytm_w = rng
    elif key == "dpp_ytc_w": f.dpp_ytc_w = rng
    elif key == "dpp_ytw_w": f.dpp_ytw_w = rng

    elif key == "dtm":
        if rng:
            rng = Range(lo=int(rng.lo) if rng.lo is not None else None,
                        hi=int(rng.hi) if rng.hi is not None else None)
        f.dtm_days = rng                         # ← диапазон по DtM
    elif key == "dtc":
        if rng:
            rng = Range(lo=int(rng.lo) if rng.lo is not None else None,
                        hi=int(rng.hi) if rng.hi is not None else None)
        f.offer_days = rng                       # ← используем уже существующее поле для DtC
    elif key == "dtw" or key == "term":
        if rng:
            rng = Range(lo=int(rng.lo) if rng.lo is not None else None,
                        hi=int(rng.hi) if rng.hi is not None else None)
        f.term_days = rng                        # ← DtW

    elif key == "price": f.price = rng
    elif key == "volume_money": f.volume = rng   # ← денежный объём
    elif key == "volume": f.volume = rng         # бэкап
    elif key == "nominal": f.nominal = rng
    elif key == "issue":
        if rng:
            rng = Range(
                lo = (rng.lo * 1_000_000) if rng.lo is not None else None,
                hi = (rng.hi * 1_000_000) if rng.hi is not None else None,
            )
        f.issue_size = rng

@dp.message()
async def on_text(m: Message):
    st = USER_STATES.setdefault(m.from_user.id, ScreenerState())
    if st.awaiting_input and st.awaiting_input.startswith("range:"):
        key = st.awaiting_input.split(":",1)[1]
        rng = parse_range(m.text)
        if rng is None:
            title, unit = pretty_key_and_unit(key)
            hint_raw = "Например: 10-15, >=12.3, <=9.5" if unit != "%" else "Например: 10-15%, >=12.3%, <=9.5%"
            hint = html.escape(hint_raw, quote=False)
            reply = await m.reply(f"Не понял диапазон для <b>{html.escape(title)}</b>. {hint}", link_preview_options=LP_DISABLED)
            await asyncio.sleep(2.5)
            try:
                await bot.delete_message(m.chat.id, reply.message_id)
            except Exception:
                pass
            return
        _assign_range(st, key, rng); st.awaiting_input = None
        st.page_idx = 0
        try:
            if st.prompt_msg_id: await bot.delete_message(m.chat.id, st.prompt_msg_id)
            await bot.delete_message(m.chat.id, m.message_id)
        except Exception:
            pass
        st.prompt_msg_id = None
        await rerender_one(m.from_user.id)
        return
    elif st.awaiting_input == "pagesize":
        try:
            n = int(m.text.strip());
            if n < 5 or n > 100: raise ValueError()
            st.page_size = n; st.awaiting_input = None
            st.page_idx = 0
            try:
                if st.prompt_msg_id: await bot.delete_message(m.chat.id, st.prompt_msg_id)
                await bot.delete_message(m.chat.id, m.message_id)
            except Exception:
                pass
            st.prompt_msg_id = None
            await rerender_one(m.from_user.id)
        except:
            reply = await m.reply("Введите целое 5..100.", link_preview_options=LP_DISABLED)
            await asyncio.sleep(2.5)
            try:
                await bot.delete_message(m.chat.id, reply.message_id)
            except Exception:
                pass
        return
    elif st.awaiting_input == "blacklist":
        tickers = [t.strip().upper() for t in re.split(r'[,\s]+', m.text.strip()) if t.strip()]
        st.blacklist.update(tickers); st.awaiting_input = None
        st.page_idx = 0
        try:
            if st.prompt_msg_id: await bot.delete_message(m.chat.id, st.prompt_msg_id)
            await bot.delete_message(m.chat.id, m.message_id)
        except Exception:
            pass
        st.prompt_msg_id = None
        await rerender_one(m.from_user.id)
        # Если сейчас открыто сообщение с blacklist — обновим его тоже
        if getattr(st, "list_view_msg_id", None) and getattr(st, "list_view_kind", None) == "bl":
            try:
                await bot.edit_message_text(
                    chat_id=m.chat.id,
                    message_id=st.list_view_msg_id,
                    text=_format_ticker_list_grouped("⛔ Blacklist", st.blacklist),
                    reply_markup=list_edit_keyboard("bl")
                )
            except TelegramBadRequest:
                pass
        return

# --- фильтры: рейтинги/сектора/страны/купоны/оферта ---
@dp.callback_query(F.data == "filter:ratings")
async def on_ratings(q: CallbackQuery):
    st = USER_STATES.setdefault(q.from_user.id, ScreenerState())
    if st.filters.ratings is None:
        st.filters.ratings = set()
    st.ui_mode = "filter_ratings"
    await rerender_one(q.from_user.id)
    await q.answer()

@dp.callback_query(F.data.startswith("filter:rating_toggle:"))
async def on_rating_toggle(q: CallbackQuery):
    st = USER_STATES.setdefault(q.from_user.id, ScreenerState())
    r = q.data.split(":",2)[2]
    st.filters.ratings = st.filters.ratings or set()
    if r in st.filters.ratings: st.filters.ratings.remove(r)
    else: st.filters.ratings.add(r)
    st.ui_mode = "filter_ratings"
    st.page_idx = 0
    await rerender_one(q.from_user.id)
    await q.answer()

@dp.callback_query(F.data == "filter:sectors")
async def on_sectors(q: CallbackQuery):
    st = USER_STATES.setdefault(q.from_user.id, ScreenerState())
    if st.filters.sectors is None:
        st.filters.sectors = set()
    st.ui_mode = "filter_sectors"
    st.page_idx = 0
    await rerender_one(q.from_user.id)
    await q.answer()

@dp.callback_query(F.data.startswith("filter:sector_toggle:"))
async def on_sector_toggle(q: CallbackQuery):
    st = USER_STATES.setdefault(q.from_user.id, ScreenerState())
    val = unquote(q.data.split(":",2)[2])
    st.filters.sectors = st.filters.sectors or set()
    if val in st.filters.sectors: st.filters.sectors.remove(val)
    else: st.filters.sectors.add(val)
    st.ui_mode = "filter_sectors"
    st.page_idx = 0
    await rerender_one(q.from_user.id)
    await q.answer()

@dp.callback_query(F.data == "filter:countries")
async def on_countries(q: CallbackQuery):
    st = USER_STATES.setdefault(q.from_user.id, ScreenerState())
    if st.filters.countries is None:
        st.filters.countries = set()
    st.ui_mode = "filter_countries"
    st.page_idx = 0
    await rerender_one(q.from_user.id)
    await q.answer()

@dp.callback_query(F.data.startswith("filter:country_toggle"))
async def on_country_toggle(q: CallbackQuery):
    """
    Поддерживаем новый формат 'filter:country_toggle_i:<idx>' (короткий)
    и старый 'filter:country_toggle:<urlencoded>' на всякий случай.
    """
    st = USER_STATES.setdefault(q.from_user.id, ScreenerState())
    st.filters.countries = st.filters.countries or set()

    if q.data.startswith("filter:country_toggle_i:"):
        # по индексу из текущего списка
        try:
            idx = int(q.data.split(":", 2)[2])
        except Exception:
            await q.answer(); return
        opts = collect_all_countries(snapshot_data())
        if idx < 0 or idx >= len(opts):
            await q.answer(); return
        val = opts[idx] if (opts[idx] is not None and opts[idx] != "") else "Unknown"
    else:
        # бэкап: старый формат с URL-кодированием
        val = unquote(q.data.split(":", 2)[2])

    if val in st.filters.countries:
        st.filters.countries.remove(val)
    else:
        st.filters.countries.add(val)

    st.ui_mode = "filter_countries"
    st.page_idx = 0
    await rerender_one(q.from_user.id)
    await q.answer()

@dp.callback_query(F.data == "filter:coupons")
async def on_coupons(q: CallbackQuery):
    st = USER_STATES.setdefault(q.from_user.id, ScreenerState())
    if st.filters.coupons_per_year is None:
        st.filters.coupons_per_year = set()
    st.ui_mode = "filter_coupons"
    st.page_idx = 0
    await rerender_one(q.from_user.id)
    await q.answer()

@dp.callback_query(F.data.startswith("filter:coupon_toggle:"))
async def on_coupon_toggle(q: CallbackQuery):
    st = USER_STATES.setdefault(q.from_user.id, ScreenerState())
    val = q.data.split(":",2)[2]
    try:
        n = int(val)
    except:
        await q.answer(); return
    st.filters.coupons_per_year = st.filters.coupons_per_year or set()
    if n in st.filters.coupons_per_year: st.filters.coupons_per_year.remove(n)
    else: st.filters.coupons_per_year.add(n)
    st.ui_mode = "filter_coupons"
    st.page_idx = 0
    await rerender_one(q.from_user.id)
    await q.answer()

@dp.callback_query(F.data == "filter:has_offer_cycle")
async def on_has_offer_cycle(q: CallbackQuery):
    st = USER_STATES.setdefault(q.from_user.id, ScreenerState())
    cur = st.filters.has_offer
    nxt = {None: False, False: True, True: None}[cur]
    st.filters.has_offer = nxt
    st.ui_mode = "filter"
    st.page_idx = 0
    label = {None: "including", False: "excluding", True: "only"}[nxt]
    await rerender_one(q.from_user.id)
    await q.answer(f"Put/Call: {label}")

@dp.callback_query(F.data == "filter:clear")
async def on_filter_clear(q: CallbackQuery):
    st = USER_STATES.setdefault(q.from_user.id, ScreenerState())
    st.filters = FilterState()
    st.ui_mode = "filter"
    st.page_idx = 0
    await rerender_one(q.from_user.id)
    await q.answer("Фильтры сброшены к дефолтным")

# --- settings / fields ---
@dp.callback_query(F.data.startswith("settings:"))
async def on_settings(q: CallbackQuery):
    st = USER_STATES.setdefault(q.from_user.id, ScreenerState())
    cmd = q.data.split(":",1)[1]
    st.ui_mode = "settings"
    if cmd == "toggle_order":
        st.sort_desc = not st.sort_desc
        st.page_idx = 0
        await rerender_one(q.from_user.id)
    elif cmd == "pagesize":
        st.awaiting_input = "pagesize"
        msg = await q.message.answer("Введите новое значение Page size (5..100):", link_preview_options=LP_DISABLED)
        st.prompt_msg_id = msg.message_id
    elif cmd == "fields":
        st.ui_mode = "settings_fields_root"   # ← БЫЛО "settings_fields"
        st.page_idx = 0
        await rerender_one(q.from_user.id)
    elif cmd == "watch_toggle":
        st.use_watchlist = not st.use_watchlist
        st.page_idx = 0
        await rerender_one(q.from_user.id)
    elif cmd == "blacklist":
        st.awaiting_input = "blacklist"
        msg = await q.message.answer("Введите тикеры для чёрного списка через пробел/запятую.", link_preview_options=LP_DISABLED)
        st.prompt_msg_id = msg.message_id
    await q.answer()

@dp.callback_query(F.data.startswith("fields_cat:"))
async def on_fields_cat(q: CallbackQuery):
    st = USER_STATES.setdefault(q.from_user.id, ScreenerState())
    cat = q.data.split(":",1)[1]
    st.ui_mode = f"settings_fields_cat:{cat}"
    st.page_idx = 0
    await rerender_one(q.from_user.id)
    await q.answer()

@dp.callback_query(F.data.startswith("fields:toggle:"))
async def on_fields_toggle(q: CallbackQuery):
    st = USER_STATES.setdefault(q.from_user.id, ScreenerState())
    f = q.data.split(":",2)[2]
    if f in st.display_fields:
        st.display_fields.remove(f)
    else:
        st.display_fields.append(f)
    if "Name" not in st.display_fields and "Ticker" not in st.display_fields:
        st.display_fields.append("Ticker")
    ensure_sort_field_in_display_fields(st)
    # остаёмся в текущем меню (root/cat)
    if st.ui_mode.startswith("settings_fields_cat:"):
        pass
    else:
        st.ui_mode = "settings_fields"
    st.page_idx = 0
    await rerender_one(q.from_user.id)
    await q.answer()

@dp.callback_query(F.data == "fields:done")
async def on_fields_done(q: CallbackQuery):
    st = USER_STATES.setdefault(q.from_user.id, ScreenerState())
    if "Name" not in st.display_fields and "Ticker" not in st.display_fields:
        st.display_fields.insert(0, "Ticker")
    ensure_sort_field_in_display_fields(st)
    st.ui_mode = "settings"
    st.page_idx = 0
    await rerender_one(q.from_user.id)
    await q.answer("Поля сохранены")

# --- watchlist команды ---
@dp.message(Command("watch_add"))
async def on_watch_add(m: Message):
    st = USER_STATES.setdefault(m.from_user.id, ScreenerState())
    parts = m.text.split()[1:]
    for t in parts: st.watchlist.add(t.upper())
    st.page_idx = 0
    await rerender_one(m.from_user.id)
    await m.reply("Добавлено в watchlist: " + (", ".join([t.upper() for t in parts]) if parts else "ничего не указано."), link_preview_options=LP_DISABLED)

@dp.message(Command("watch_remove"))
async def on_watch_remove(m: Message):
    st = USER_STATES.setdefault(m.from_user.id, ScreenerState())
    parts = m.text.split()[1:]
    for t in parts: st.watchlist.discard(t.upper())
    st.page_idx = 0
    await rerender_one(m.from_user.id)
    await m.reply("Удалено из watchlist: " + (", ".join([t.upper() for t in parts]) if parts else "ничего не указано."), link_preview_options=LP_DISABLED)

@dp.message(Command("watch_clear"))
async def on_watch_clear(m: Message):
    st = USER_STATES.setdefault(m.from_user.id, ScreenerState())
    st.watchlist.clear()
    st.page_idx = 0
    await rerender_one(m.from_user.id)
    await m.reply("Watchlist очищен.", link_preview_options=LP_DISABLED)

# ====================== Луп событий (стримы) ======================
async def change_watcher_loop():
    from time import monotonic
    while True:
        try:
            loop = asyncio.get_running_loop()
            _ = await loop.run_in_executor(None, DATA_CHANGE_QUEUE.get)
            t0 = monotonic()
            while (monotonic() - t0) < 1:
                try: DATA_CHANGE_QUEUE.get_nowait()
                except Empty: break
                await asyncio.sleep(1)
            await rerender_all_active()
        except Exception:
            await asyncio.sleep(0.5)

async def bot_main():
    asyncio.create_task(change_watcher_loop())
    await dp.start_polling(bot)

def start_bot_thread():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(bot_main())

if BOT_TOKEN and BOT_TOKEN != "PASTE_YOUR_TELEGRAM_BOT_TOKEN_HERE":
    threading.Thread(target=start_bot_thread, daemon=True).start()
else:
    print("⚠️ TELEGRAM_BOT_TOKEN не задан. Укажите BOT_TOKEN сверху, чтобы включить бота.")
# ====================== END TELEGRAM SCREENER BOT v9.4 ======================


#######


#######



def initial_list():
    with Client(token) as client:
        r = client.instruments.bonds(
            instrument_status=1
        )

    name_bonds, ticker_bonds, maturity_date_bonds, nominal_bonds, aci_bonds, sector_bonds, coupon_quantity_per_year_bonds, country_of_risk_bonds, issue_size_bonds, issue_size_money_bonds = {}, {}, {}, {}, {}, {}, {}, {}, {}, {}

    print('Получение списка облигаций и данных по ним по критериям…')
    for bond in r.instruments:
        if bond.nominal.currency == 'rub' \
                and bond.currency == 'rub' \
                and bond.lot == 1 \
                and not bond.floating_coupon_flag \
                and not bond.amortization_flag \
                and not bond.perpetual_flag \
                and bond.nominal.units + (bond.nominal.nano * 10 ** (-9)) != 0 \
                and bond.issue_size != 0:
            instrument_id = bond.uid

            ticker_bonds[instrument_id] = bond.ticker
            name_bonds[instrument_id] = bond.name
            coupon_quantity_per_year_bonds[instrument_id] = bond.coupon_quantity_per_year
            maturity_date_bonds[instrument_id] = bond.maturity_date.date()
            nominal_bonds[instrument_id] = bond.nominal.units + (bond.nominal.nano * 10 ** (-9))
            aci_bonds[instrument_id] = bond.aci_value.units + (bond.aci_value.nano * 10 ** (-9))
            country_of_risk_bonds[instrument_id] = bond.country_of_risk_name
            if bond.sector != "":
                sector_bonds[instrument_id] = bond.sector
            else:
                sector_bonds[instrument_id] = "other"
            issue_size_bonds[instrument_id] = bond.issue_size
            issue_size_money_bonds[instrument_id] = bond.issue_size * (bond.initial_nominal.units + (bond.initial_nominal.nano * 10 ** (-9)))
    print('После первичной фильтрации осталось', len(ticker_bonds), 'облигаций(и, я).')
    return ticker_bonds, name_bonds, coupon_quantity_per_year_bonds, maturity_date_bonds, nominal_bonds, aci_bonds, country_of_risk_bonds, sector_bonds, issue_size_bonds, issue_size_money_bonds


def list_after_smartlab():
    print('Парсинг Smart-lab для отфильтровки списка облигаций по кредитному рейтингу и получения информации по офертам/кол-опционам…')
    credit_rating_count = 0
    uid_bonds = []
    credit_rating_bonds, offer_or_call_option_date_bonds = {}, {}
    for bond_ticker in tqdm(ticker_bonds):
        try:
            url = f'https://smart-lab.ru/q/bonds/{ticker_bonds[bond_ticker]}/'
            r = requests.get(url)
            soup = bs(r.text, 'lxml')
            if soup.find(string=lambda text: text and 'Сектор' in text).find_next('div',
                                                                                  class_='quotes-simple-table__item').text.strip() in ('Субфедеральные', 'Государственные'):
                credit_rating = 'AAA+'
            elif soup.find('div', class_='linear-progress-bar__text') is None:
                credit_rating = '-'
            else:
                credit_rating = soup.find('div', class_='linear-progress-bar__text').text.strip()
            offer_date = soup.find('a', class_='blue-link').find_next('div',
                                                                      class_='quotes-simple-table__item').text.strip()
            if (offer_date == '—' or datetime.strptime(offer_date, '%d-%m-%Y').date() < date.today()) and credit_rating != '-':
                uid_bonds.append(bond_ticker)
                credit_rating_bonds[bond_ticker] = credit_rating
            elif (offer_date != '—' and datetime.strptime(offer_date, '%d-%m-%Y').date() > date.today()) and credit_rating != '-':
                uid_bonds.append(bond_ticker)
                credit_rating_bonds[bond_ticker] = credit_rating
                offer_or_call_option_date_bonds[bond_ticker] = datetime.strptime(offer_date, '%d-%m-%Y').date()
            else:
                credit_rating_count += 1
        except Exception:
            pass
    print(f'Из списка было удалено {credit_rating_count} облигаций(я), у которых отсутствовал кредитный рейтинг.')
    print('После вторичной фильтрации по кредитному рейтингу осталось', len(uid_bonds), 'облигаций, из которых', len(offer_or_call_option_date_bonds), 'облигаций(и, я) имеют предстоящую оферту/кол-опцион.')
    return uid_bonds, credit_rating_bonds, offer_or_call_option_date_bonds

def confirming_list_for_analysis():
    print('Подтверждение списка облигаций…')
    dicts = [ticker_bonds, name_bonds, coupon_quantity_per_year_bonds, maturity_date_bonds, nominal_bonds, aci_bonds, country_of_risk_bonds, sector_bonds, issue_size_bonds, credit_rating_bonds, offer_or_call_option_date_bonds]
    for d in dicts:
        for key in list(d.keys()):
            if key not in uid_bonds:
                del d[key]
    return uid_bonds, ticker_bonds, name_bonds, coupon_quantity_per_year_bonds, maturity_date_bonds, nominal_bonds, aci_bonds, country_of_risk_bonds, sector_bonds, issue_size_bonds, credit_rating_bonds, offer_or_call_option_date_bonds

def setting_timeframes():
    print('Установка временных интервалов…')
    if datetime.today().isoweekday() == 5:
        day = datetime.today() + timedelta(days=3)
    elif datetime.today().isoweekday() == 6:
        day = datetime.today() + timedelta(days=2)
    else:
        day = datetime.today() + timedelta(days=1)
    return day

def building_lambdas():
    pd_table = {'AAA+': (0.00143, 0.00562, 0.00989), 'AAA': (0.00143, 0.00562, 0.00989), 'AAA-': (0.00143, 0.00562, 0.00989),
                'AA+': (0.00218, 0.00796, 0.01374), 'AA': (0.00284, 0.00994, 0.01693), 'AA-': (0.00371, 0.0124, 0.02083),
                'A+': (0.00484, 0.01545, 0.02561), 'A': (0.00631, 0.01924, 0.03145), 'A-': (0.00823, 0.02395, 0.03858),
                'BBB+': (0.01072, 0.02976, 0.04724), 'BBB': (0.01396, 0.03694, 0.05772), 'BBB-': (0.01815, 0.04577, 0.07037),
                'BB+': (0.02357, 0.05658, 0.08553), 'BB': (0.03056, 0.06975, 0.10359), 'BB-': (0.03954, 0.08572, 0.12494),
                'B+': (0.05102, 0.10493, 0.14996), 'B': (0.06561, 0.12785, 0.17897), 'B-': (0.08399, 0.1549, 0.21218),
                'CCC': (0.13524, 0.22273, 0.29137), 'CC': (0.25834, 0.35905, 0.43681), 'C': (0.51756, 0.58956, 0.65416),
                'D': (0.999, 0.999, 0.999)}
    out = {}
    for r, (pd1, pd2, pd3) in pd_table.items():
        s1, s2, s3 = 1 - pd1, 1 - pd2, 1 - pd3
        l1 = -math.log(s1)
        l2 = math.log(s1 / s2)
        l3 = math.log(s2 / s3)
        out[r] = (l1, l2, l3)
    return out

def survival_on_date(rating, target_date, lambdas_by_rating):
    t = (target_date - date.today()).days / 365.0
    l1, l2, l3 = lambdas_by_rating[rating]
    if t <= 1.0:
        expo = -l1 * t
    elif t <= 2.0:
        expo = -l1 - l2 * (t - 1.0)
    else:
        expo = -l1 - l2 - l3 * (t - 2.0)
    s = math.exp(expo)
    return s

def coupon_cashflows():
    print('Формирование денежных потоков по купонам…')
    coupons_to_maturity_bonds, weighted_coupons_to_maturity_bonds, coupons_to_offer_bonds, weighted_coupons_to_offer_bonds = {}, {}, {}, {}
    count = 0
    for instrument_id in tqdm(uid_bonds[:]):
        time.sleep(0.3)
        with Client(token) as client:
            r = client.instruments.get_bond_coupons(
                instrument_id=instrument_id,
                from_=day,
                to=datetime.today() + timedelta(weeks=49999)
            )

        if len(r.events) != 0:
            coupons, weighted_coupons, coupons_to_offer, weighted_coupons_to_offer = {}, {}, {}, {}
            for event in r.events:
                coupon = event.pay_one_bond.units + (event.pay_one_bond.nano * 10 ** (-9))
                weighted_coupon = (event.pay_one_bond.units + (event.pay_one_bond.nano * 10 ** (-9))) * survival_on_date(credit_rating_bonds[instrument_id], event.coupon_date.date(), lambdas_by_rating)
                coupons[event.coupon_date.date()] = coupon
                weighted_coupons[event.coupon_date.date()] = weighted_coupon
                if instrument_id in offer_or_call_option_date_bonds.keys():
                    if event.coupon_date.date() <= offer_or_call_option_date_bonds[instrument_id]:
                        coupons_to_offer[event.coupon_date.date()] = coupon
                        weighted_coupons_to_offer[event.coupon_date.date()] = weighted_coupon
            coupons_to_maturity_bonds[instrument_id] = coupons
            weighted_coupons_to_maturity_bonds[instrument_id] = weighted_coupons
            coupons_to_offer_bonds[instrument_id] = coupons_to_offer
            weighted_coupons_to_offer_bonds[instrument_id] = weighted_coupons_to_offer
        else:
            count += 1
            uid_bonds.remove(instrument_id)
    confirming_list_for_analysis()
    print('Из-за отсутсвия информации по купонам, было удалено', count, 'облигаций(и, я).')
    return coupons_to_maturity_bonds, weighted_coupons_to_maturity_bonds, coupons_to_offer_bonds, weighted_coupons_to_offer_bonds

def historic_volume_request():
    print('Запрос исторических данных по объёмам за сегодня…')
    for uid in tqdm(uid_bonds):
        with Client(token) as client:
            r = client.market_data.get_candles(
                interval=CandleInterval.CANDLE_INTERVAL_DAY,
                instrument_id=uid,
                from_=datetime.today(),
                to=datetime.today() + timedelta(days=1)
            )
            for candle in r.candles:
                volume = candle.volume
                volume_bonds[uid] = volume
        time.sleep(0.05)
    return volume_bonds

def historic_prices_request():
    print('Запрос исторических последних цен…')
    last_price_bonds = {}
    count = 0
    with Client(token) as client:
        r = client.market_data.get_last_prices(
            instrument_id=uid_bonds
        )

    for last_price in r.last_prices:
        instrument_id = last_price.instrument_uid
        price = (last_price.price.units + (last_price.price.nano * 10 ** (-9))) / 100 * nominal_bonds[instrument_id]

        if price != 0:
            last_price_bonds[instrument_id] = price

            profitability_and_duration_calculation(price, instrument_id)

        else:
            count += 1
            uid_bonds.remove(instrument_id)
    confirming_list_for_analysis()
    print('Из-за отсутсвия информации по последним ценам, было удалено', count, 'облигаций(и, я).')
    return last_price_bonds

async def volumes_and_last_prices_stream(uids_chunk):
    print('Запуск стрима объёмов…')
    async def request_iterator():
        yield MarketDataRequest(
            subscribe_candles_request=SubscribeCandlesRequest(
                subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
                waiting_close=False,
                instruments=[
                    CandleInstrument(
                        instrument_id=uid,
                        interval=SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_DAY
                    ) for uid in uids_chunk
                ]
            )
        )
        while True:
            await asyncio.sleep(0)

    async with AsyncClient(token) as client:
        async for r in client.market_data_stream.market_data_stream(
                request_iterator()
        ):
            if r.candle != None:
                instrument_id = r.candle.instrument_uid
                volume = r.candle.volume
                volume_bonds[instrument_id] = volume

                if r.candle.close is not None and r.candle.close != 0:
                    price = (r.candle.close.units + (r.candle.close.nano * 10 ** (-9))) / 100 * nominal_bonds[
                        instrument_id]

                    if price != last_price_bonds[instrument_id]:
                        last_price_bonds[instrument_id] = price
                        asyncio.create_task(asyncio.to_thread(profitability_and_duration_calculation, price, instrument_id))

                print(ticker_bonds[instrument_id], volume_bonds[instrument_id], last_price_bonds[instrument_id])

def profitability_and_duration_calculation(price, instrument_id):
    pv = price + aci_bonds[instrument_id]
    dy1 = 0.01
    today_ = date.today()

    def mac_dur_conv(dates, cfs, y, pv_):
        iy = 1.0 + y
        s_mac = 0.0
        s_conv = 0.0
        for d, cf in zip(dates, cfs):
            t = (d - today_).days / 365.0
            s_mac += t * cf * (iy ** (-t))
            s_conv += t * (t + 1.0) * cf * (iy ** (-t - 2.0))
        d_mac = s_mac / pv_
        d_mod = d_mac / iy
        c_mod = s_conv / pv_
        return d_mac, d_mod, c_mod

    YTM_xirr = px.xirr(
        [today_, *coupons_to_maturity_bonds[instrument_id].keys(), maturity_date_bonds[instrument_id]],
        [-pv, *coupons_to_maturity_bonds[instrument_id].values(),
         nominal_bonds[instrument_id]]
    )

    weighted_YTM_xirr = px.xirr(
        [today_, *weighted_coupons_to_maturity_bonds[instrument_id].keys(), maturity_date_bonds[instrument_id]],
        [-pv, *weighted_coupons_to_maturity_bonds[instrument_id].values(),
         nominal_bonds[instrument_id] * survival_on_date(credit_rating_bonds[instrument_id],
                                                         maturity_date_bonds[instrument_id], lambdas_by_rating)]
    )

    dates_m = [*coupons_to_maturity_bonds[instrument_id].keys(), maturity_date_bonds[instrument_id]]
    cfs_m = [*coupons_to_maturity_bonds[instrument_id].values(), nominal_bonds[instrument_id]]
    mac_ytm, dmod_ytm, cmod_ytm = mac_dur_conv(dates_m, cfs_m, YTM_xirr, pv)

    w_dates_m = [*weighted_coupons_to_maturity_bonds[instrument_id].keys(), maturity_date_bonds[instrument_id]]
    w_cfs_m = [*weighted_coupons_to_maturity_bonds[instrument_id].values(), nominal_bonds[instrument_id] * survival_on_date(credit_rating_bonds[instrument_id], maturity_date_bonds[instrument_id], lambdas_by_rating)]
    w_mac_ytm, w_dmod_ytm, w_cmod_ytm = mac_dur_conv(w_dates_m, w_cfs_m, YTM_xirr, pv)

    if instrument_id in offer_or_call_option_date_bonds.keys():
        YTC_xirr = px.xirr(
            [today_, *coupons_to_offer_bonds[instrument_id].keys(),
             offer_or_call_option_date_bonds[instrument_id]],
            [-pv, *coupons_to_offer_bonds[instrument_id].values(),
             nominal_bonds[instrument_id]]
        )

        weighted_YTC_xirr = px.xirr(
            [today_, *weighted_coupons_to_offer_bonds[instrument_id].keys(),
             offer_or_call_option_date_bonds[instrument_id]],
            [-pv, *weighted_coupons_to_offer_bonds[instrument_id].values(),
             nominal_bonds[instrument_id] * survival_on_date(credit_rating_bonds[instrument_id],
                                                             offer_or_call_option_date_bonds[instrument_id],
                                                             lambdas_by_rating)]
        )

        dates_c = [*coupons_to_offer_bonds[instrument_id].keys(), offer_or_call_option_date_bonds[instrument_id]]
        cfs_c = [*coupons_to_offer_bonds[instrument_id].values(), nominal_bonds[instrument_id]]

        w_dates_c = [*weighted_coupons_to_offer_bonds[instrument_id].keys(), offer_or_call_option_date_bonds[instrument_id]]
        w_cfs_c = [*weighted_coupons_to_offer_bonds[instrument_id].values(), nominal_bonds[instrument_id] * survival_on_date(credit_rating_bonds[instrument_id], offer_or_call_option_date_bonds[instrument_id], lambdas_by_rating)]
    else:
        YTC_xirr = YTM_xirr
        weighted_YTC_xirr = weighted_YTM_xirr
        dates_c, cfs_c, w_dates_c, w_cfs_c = dates_m, cfs_m, w_dates_m, w_cfs_m

    mac_ytc, dmod_ytc, cmod_ytc = mac_dur_conv(dates_c, cfs_c, YTC_xirr, pv)
    w_mac_ytc, w_dmod_ytc, w_cmod_ytc = mac_dur_conv(w_dates_c, w_cfs_c, YTC_xirr, pv)

    if YTC_xirr < YTM_xirr:
        YTW_xirr = YTC_xirr
        days_YTW = (offer_or_call_option_date_bonds[instrument_id] - today_).days
        mac_ytw, dmod_ytw, cmod_ytw = mac_ytc, dmod_ytc, cmod_ytc
    else:
        YTW_xirr = YTM_xirr
        days_YTW = (maturity_date_bonds[instrument_id] - today_).days
        mac_ytw, dmod_ytw, cmod_ytw = mac_ytm, dmod_ytm, cmod_ytm

    if weighted_YTC_xirr < weighted_YTM_xirr:
        weighted_YTW_xirr = weighted_YTC_xirr
        days_weighted_YTW = (offer_or_call_option_date_bonds[instrument_id] - today_).days
        w_mac_ytw, w_dmod_ytw, w_cmod_ytw = w_mac_ytc, w_dmod_ytc, w_cmod_ytc
    else:
        weighted_YTW_xirr = weighted_YTM_xirr
        days_weighted_YTW = (maturity_date_bonds[instrument_id] - today_).days
        w_mac_ytw, w_dmod_ytw, w_cmod_ytw = w_mac_ytm, w_dmod_ytm, w_cmod_ytm

    YTM_xirr_bonds[instrument_id], weighted_YTM_xirr_bonds[instrument_id], YTC_xirr_bonds[instrument_id], weighted_YTC_xirr_bonds[instrument_id], YTW_xirr_bonds[instrument_id], weighted_YTW_xirr_bonds[instrument_id], days_YTW_bonds[instrument_id], days_weighted_YTW_bonds[instrument_id] = YTM_xirr, weighted_YTM_xirr, YTC_xirr, weighted_YTC_xirr, YTW_xirr, weighted_YTW_xirr, days_YTW, days_weighted_YTW

    macaulay_YTM_bonds[instrument_id] = mac_ytm
    modified_YTM_bonds[instrument_id] = dmod_ytm
    convexity_YTM_bonds[instrument_id] = cmod_ytm
    macaulay_YTC_bonds[instrument_id] = mac_ytc
    modified_YTC_bonds[instrument_id] = dmod_ytc
    convexity_YTC_bonds[instrument_id] = cmod_ytc
    macaulay_YTW_bonds[instrument_id] = mac_ytw
    modified_YTW_bonds[instrument_id] = dmod_ytw
    convexity_YTW_bonds[instrument_id] = cmod_ytw

    dpp_1pct_up_YTM_bonds[instrument_id] = -dmod_ytm * dy1 + 0.5 * cmod_ytm * dy1 * dy1
    dpp_1pct_down_YTM_bonds[instrument_id] = dmod_ytm * dy1 + 0.5 * cmod_ytm * dy1 * dy1
    dpp_1pct_up_YTC_bonds[instrument_id] = -dmod_ytc * dy1 + 0.5 * cmod_ytc * dy1 * dy1
    dpp_1pct_down_YTC_bonds[instrument_id] = dmod_ytc * dy1 + 0.5 * cmod_ytc * dy1 * dy1
    dpp_1pct_up_YTW_bonds[instrument_id] = -dmod_ytw * dy1 + 0.5 * cmod_ytw * dy1 * dy1
    dpp_1pct_down_YTW_bonds[instrument_id] = dmod_ytw * dy1 + 0.5 * cmod_ytw * dy1 * dy1

    weighted_macaulay_YTM_bonds[instrument_id] = w_mac_ytm
    weighted_modified_YTM_bonds[instrument_id] = w_dmod_ytm
    weighted_convexity_YTM_bonds[instrument_id] = w_cmod_ytm
    weighted_macaulay_YTC_bonds[instrument_id] = w_mac_ytc
    weighted_modified_YTC_bonds[instrument_id] = w_dmod_ytc
    weighted_convexity_YTC_bonds[instrument_id] = w_cmod_ytc
    weighted_macaulay_YTW_bonds[instrument_id] = w_mac_ytw
    weighted_modified_YTW_bonds[instrument_id] = w_dmod_ytw
    weighted_convexity_YTW_bonds[instrument_id] = w_cmod_ytw

    weighted_dpp_1pct_up_YTM_bonds[instrument_id] = -w_dmod_ytm * dy1 + 0.5 * w_cmod_ytm * dy1 * dy1
    weighted_dpp_1pct_down_YTM_bonds[instrument_id] = w_dmod_ytm * dy1 + 0.5 * w_cmod_ytm * dy1 * dy1
    weighted_dpp_1pct_up_YTC_bonds[instrument_id] = -w_dmod_ytc * dy1 + 0.5 * w_cmod_ytc * dy1 * dy1
    weighted_dpp_1pct_down_YTC_bonds[instrument_id] = w_dmod_ytc * dy1 + 0.5 * w_cmod_ytc * dy1 * dy1
    weighted_dpp_1pct_up_YTW_bonds[instrument_id] = -w_dmod_ytw * dy1 + 0.5 * w_cmod_ytw * dy1 * dy1
    weighted_dpp_1pct_down_YTW_bonds[instrument_id] = w_dmod_ytw * dy1 + 0.5 * w_cmod_ytw * dy1 * dy1

    notify_data_changed(instrument_id)

ticker_bonds, name_bonds, coupon_quantity_per_year_bonds, maturity_date_bonds, nominal_bonds, aci_bonds, country_of_risk_bonds, sector_bonds, issue_size_bonds, issue_size_money_bonds = initial_list()
uid_bonds, credit_rating_bonds, offer_or_call_option_date_bonds = list_after_smartlab()
uid_bonds, ticker_bonds, name_bonds, coupon_quantity_per_year_bonds, maturity_date_bonds, nominal_bonds, aci_bonds, country_of_risk_bonds, sector_bonds, issue_size_bonds, credit_rating_bonds, offer_or_call_option_date_bonds = confirming_list_for_analysis()
day = setting_timeframes()
lambdas_by_rating = building_lambdas()
coupons_to_maturity_bonds, weighted_coupons_to_maturity_bonds, coupons_to_offer_bonds, weighted_coupons_to_offer_bonds = coupon_cashflows()

volume_bonds = {}

YTM_xirr_bonds, weighted_YTM_xirr_bonds, YTC_xirr_bonds, weighted_YTC_xirr_bonds, YTW_xirr_bonds, weighted_YTW_xirr_bonds, days_YTW_bonds, days_weighted_YTW_bonds = {}, {}, {}, {}, {}, {}, {}, {}

macaulay_YTM_bonds, modified_YTM_bonds, convexity_YTM_bonds = {}, {}, {}
macaulay_YTC_bonds, modified_YTC_bonds, convexity_YTC_bonds = {}, {}, {}
macaulay_YTW_bonds, modified_YTW_bonds, convexity_YTW_bonds = {}, {}, {}

weighted_macaulay_YTM_bonds, weighted_modified_YTM_bonds, weighted_convexity_YTM_bonds = {}, {}, {}
weighted_macaulay_YTC_bonds, weighted_modified_YTC_bonds, weighted_convexity_YTC_bonds = {}, {}, {}
weighted_macaulay_YTW_bonds, weighted_modified_YTW_bonds, weighted_convexity_YTW_bonds = {}, {}, {}

dpp_1pct_up_YTM_bonds, dpp_1pct_down_YTM_bonds = {}, {}
dpp_1pct_up_YTC_bonds, dpp_1pct_down_YTC_bonds = {}, {}
dpp_1pct_up_YTW_bonds, dpp_1pct_down_YTW_bonds = {}, {}

weighted_dpp_1pct_up_YTM_bonds, weighted_dpp_1pct_down_YTM_bonds = {}, {}
weighted_dpp_1pct_up_YTC_bonds, weighted_dpp_1pct_down_YTC_bonds = {}, {}
weighted_dpp_1pct_up_YTW_bonds, weighted_dpp_1pct_down_YTW_bonds = {}, {}

volume_bonds = historic_volume_request()

last_price_bonds = historic_prices_request()

async def main():
    tasks = [
        volumes_and_last_prices_stream(uid_bonds[i:i+295])  # каждая корутина — своё соединение
        for i in range(0, len(uid_bonds), 295)
    ]
    # Исключение в одной не прибьёт остальные:
    await asyncio.gather(*tasks, return_exceptions=True)

asyncio.run(main())

