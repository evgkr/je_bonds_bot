from tinkoff.invest import MarketDataRequest,AsyncClient, SubscriptionAction, Client, CandleInstrument, CandleInterval, SubscribeCandlesRequest, SubscriptionInterval, GetMySubscriptions, SubscriptionStatus
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
from collections import defaultdict
import random
from grpc.aio import AioRpcError
import re
import html
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Any, Tuple
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
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
import copy

token = tokenAPI.token
BOT_TOKEN = bottoken

def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S %d-%m-%Y")

EDIT_THROTTLE_SECONDS = 3
LP_DISABLED = LinkPreviewOptions(is_disabled=True)
IDLE_TIMEOUT_SECONDS = 30
MAX_LINES_PER_PAGE = 23
HEARTBEAT_SEC = 120   # сколько ждём ping/данные прежде чем перезапускаться
BACKOFF_MAX   = 30

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
    return FilterState(ratings={"AAA","AA","A","BBB"})

@dataclass
class Range:
    lo: float | None
    hi: float | None
    inc_lo: bool = True   # включать нижнюю границу
    inc_hi: bool = True   # включать верхнюю границу

    def match(self, x: float | None) -> bool:
        if x is None:
            return False
        if (self.lo is not None) and (x < self.lo or (not self.inc_lo and x == self.lo)):
            return False
        if (self.hi is not None) and (x > self.hi or (not self.inc_hi and x == self.hi)):
            return False
        return True

    def as_text(self) -> str:
        if self.lo is not None and self.hi is not None:
            # для симметрии оставим A-B (оба включительно визуально),
            # но если одна из границ строгая — покажем знаками
            if self.inc_lo and self.inc_hi:
                return f"{self.lo:g}-{self.hi:g}"
            lo_sign = ">" if not self.inc_lo else ">="
            hi_sign = "<" if not self.inc_hi else "<="
            return f"{lo_sign}{self.lo:g} & {hi_sign}{self.hi:g}"
        if self.lo is not None:
            sign = ">" if not self.inc_lo else ">="
            return f"{sign}{self.lo:g}"
        if self.hi is not None:
            sign = "<" if not self.inc_hi else "<="
            return f"{sign}{self.hi:g}"
        return "—"

def parse_range(s: str | None) -> Range | None:
    if not s:
        return None
    s = s.strip().replace(",", ".")   # запятая как десятичный

    pct = False
    if s.endswith("%"):
        pct = True
        s = s[:-1].strip()

    # A-B
    m = re.match(r"^\s*(-?\d+(?:\.\d+)?)\s*-\s*(-?\d+(?:\.\d+)?)\s*$", s)
    if m:
        lo = float(m.group(1))
        hi = float(m.group(2))
        if pct:
            lo /= 100.0; hi /= 100.0
        if lo > hi:
            lo, hi = hi, lo
        return Range(lo=lo, hi=hi, inc_lo=True, inc_hi=True)

    # >=X / <=Y / >X / <Y  (включая HTML-escaped варианты)
    m = re.match(r"^(>=|&gt;=|≤=|<=|&lt;=|>|&gt;|<|&lt;)\s*(-?\d+(?:\.\d+)?)\s*$", s)
    if m:
        op = m.group(1)
        val = float(m.group(2))
        if pct:
            val /= 100.0

        # нормализуем оп
        if op in (">=", "&gt;=", "≥", "≤="):  # "≤=" сюда случайно не попадёт; на всякий
            return Range(lo=val, hi=None, inc_lo=True, inc_hi=True)
        if op in ("<=", "&lt;="):
            return Range(lo=None, hi=val, inc_lo=True, inc_hi=True)
        if op in (">", "&gt;"):
            return Range(lo=val, hi=None, inc_lo=False, inc_hi=True)
        if op in ("<", "&lt;"):
            return Range(lo=None, hi=val, inc_lo=True, inc_hi=False)

    # просто X  → точка (включительно)
    m = re.match(r"^\s*(-?\d+(?:\.\d+)?)\s*$", s)
    if m:
        val = float(m.group(1))
        if pct:
            val /= 100.0
        return Range(lo=val, hi=val, inc_lo=True, inc_hi=True)

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
    list_page_idx: int = 0
    list_pages_count: int = 1
    presets: Dict[str, Any] = field(default_factory=dict)
    ui_mode: str = "main"
    page_idx: int = 0
    pages_count: int = 1
    last_interaction_ts: float = 0.0
    idle_task: Optional[asyncio.Task] = None
    tx_rows_cap: Optional[int] = None
    wl_cmd_msg_id: Optional[int] = None
    bl_cmd_msg_id: Optional[int] = None


    # --- отдельные view для watchlist/blacklist ---
    wl_view_msg_id: Optional[int] = None
    bl_view_msg_id: Optional[int] = None
    wl_page_idx: int = 0
    bl_page_idx: int = 0
    wl_pages_count: int = 1
    bl_pages_count: int = 1

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

def note_fragment_for_uid(uid: str | None) -> str | None:
    """Строит 'Open Link' #fragment 1-в-1 как в скринере, но по uid (без row)."""
    if not uid:
        return None
    d = snapshot_data()

    def fmt_date(x):
        from datetime import date, datetime
        if isinstance(x, (date, datetime)): return x.strftime("%d.%m.%Y")
        return "—"

    def fmt_bool(x): return "Yes" if x else "No"
    def num(x, digits=2): return "—" if x is None else f"{x:.{digits}f}"
    def num_int(x):
        if x is None: return "—"
        try: return f"{int(x):,}".replace(",", " ")
        except: return str(x)
    def days(x): return "—" if x is None else f"{int(x)}d"

    t = d["ticker"].get(uid)
    name = d["name"].get(uid)
    mat = d["maturity"].get(uid)
    today = date.today()
    dtm = (mat - today).days if mat else None

    offer = d["offer_date"].get(uid)
    has_offer = offer is not None
    dtc = (offer - today).days if offer else None

    rating = d["rating"].get(uid)
    cpn = d["coupons_per_year"].get(uid)
    sector = d["sector"].get(uid)
    vol_lots = d["volume"].get(uid)
    iss = d["issue_size"].get(uid)
    vol_issue_pct = (vol_lots * 100.0 / iss) if isinstance(vol_lots,(int,float)) and isinstance(iss,(int,float)) and iss>0 else None

    has_offer_str = fmt_bool(has_offer)
    if has_offer:
        has_offer_str += f" ({fmt_date(offer)} - {dtc}d)"

    lines = [
        f"🏷️ Name: {name or '—'}",
        f"📆 Mat.: {fmt_date(mat)} ({days(dtm)})",
        f"📝 Offer: {has_offer_str}",
        f"⭐ CR: {rating or '—'}",
        f"💸 Cpn/yr: {cpn if cpn is not None else '—'}",
        f"📊 Vol.: {num_int(vol_lots)} ({'—' if vol_issue_pct is None else f'{num(vol_issue_pct, 4)}%'})"
    ]
    return quote("\n" + "\n" + "\n".join(lines), safe="")

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
RATING_SCORE = {b: float(i) for i, b in enumerate(RATING_ORDER)}

def _ratings_sorted_desc(vals) -> list[str]:
    """Сортировка рейтингов строго по RATING_ORDER (AAA, AA, A, ...)."""
    if not vals:
        return []
    s = [str(x).upper() for x in vals]
    set_s = set(s)
    out = [r for r in RATING_ORDER if r in set_s]
    # если вдруг попадутся неизвестные значения — в конец
    tail = [r for r in s if r not in set(RATING_ORDER)]
    for t in tail:
        if t not in out:
            out.append(t)
    return out

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

    scope_uids = getattr(state, "scope_uids", None)  # set[str] | None

    for uid in uids:
        if scope_uids is not None and uid not in scope_uids:
            continue

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
        return filtered[:state.page_size]

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

    frag = note_fragment_for_uid(r.get("uid"))

    parts = []
    link_name = ('Name' in cols) and (('Ticker' not in cols) or cols.index('Name') < cols.index('Ticker'))
    link_ticker = ('Ticker' in cols) and (('Name' not in cols) or cols.index('Ticker') < cols.index('Name'))
    for c in cols:
        is_sort_col = (c == sort_disp_field)
        if c == "Ticker":
            core = html.escape(r["Ticker"])
            core = f"<b>{core}</b>" if is_sort_col else core
            parts.append(anchor_tinkoff(r["Ticker"], core, frag) if link_ticker else core)
        elif c == "Name":
            nm = short_name(r["Name"], 9)
            core = html.escape(nm)
            core = f"<b>{core}</b>" if is_sort_col else core
            parts.append(anchor_tinkoff(r["Ticker"], core, frag) if link_name else core)

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

def _format_ticker_list_grouped(title: str, tickers: Set[str], page_idx: int, cap: int) -> tuple[str, int]:
    """
    Группируем ТОЛЬКО по сектору (как просил), оформляем как в скринере:
    - Заголовок группы через sector_display_with_emoji(...),
    - Пустая строка между группами,
    - На странице ровно cap облигаций (заголовки не считаем),
    - Внизу футер: — Page X/Y —.
    """
    d = snapshot_data()

    def _header_with_part(h: str, idx: int, total: int) -> str:
        if total <= 1:
            return h
        return h.replace("</b>", f" ({idx}/{total})</b>")

    # uid <-> ticker
    t_by_uid = d["ticker"]           # uid -> TICKER
    uid_by_t = {t_by_uid[uid].upper(): uid for uid in d["uids"]}
    sector_by_uid = d["sector"]

    # только известные тикеры
    tickers = [t.upper() for t in tickers if t.upper() in uid_by_t]

    # группировка по сектору
    groups: Dict[str, List[str]] = defaultdict(list)   # sector -> [строки]
    for t in sorted(set(tickers)):
        uid = uid_by_t[t]
        sec = sector_by_uid.get(uid) or "Other"
        # ссылка «как в скринере» (тот же frag)
        frag = note_fragment_for_uid(uid) or ""
        url = f"https://www.tinkoff.ru/invest/bonds/{html.escape(t)}"
        if frag:
            url += f"#{frag}"
        name = d["name"].get(uid) or ""
        # Кликом делаем ТИКЕР (Name — plain), чтобы совпадало с поведением скринера
        row = f'<a href="{html.escape(url, quote=True)}">{html.escape(t)}</a> — {html.escape(name)}'
        groups[sec].append(row)

    # порядок групп — по названию (как раньше), «Other» в конце
    ordered = sorted([g for g in groups.keys() if g != "Other"], key=lambda s: str(s).lower())
    if "Other" in groups:
        ordered.append("Other")

    # соберём блоки вида: [<b>Header</b> (N), row...]
    blocks: List[List[str]] = []
    for sec in ordered:
        rows = groups[sec]
        n = len(rows)
        header_txt = sector_display_with_emoji(sec)       # <-- ЭМОДЗИ и нормализация, как в скринере
        header = f"<b>{html.escape(header_txt)}</b> ({n})"
        blocks.append([header] + rows)

    # если пусто
    if not blocks:
        return f"{title}\n\n(empty)\n\n— Page 1/1 —", 1

    cap = max(1, int(cap))
    pages: List[str] = []
    acc: List[str] = []          # копим контент без title, title добавим в flush
    rows_on_page = 0             # считаем ТОЛЬКО облигации
    first_group_on_page = True   # чтобы ставить пустую строку между группами

    def flush_page():
        nonlocal acc, rows_on_page, first_group_on_page
        page_body = "\n".join([title, ""] + acc) if acc else f"{title}\n\n(empty)"
        pages.append(page_body)
        acc = []
        rows_on_page = 0
        first_group_on_page = True

    for block in blocks:
        header = block[0]
        rows = block[1:]
        need = len(rows)

        parts_total = 1
        part_idx = 0

        while True:
            remaining = cap - rows_on_page
            if need <= remaining:
                # помещается целиком
                if not first_group_on_page:
                    acc.append("")  # пустая строка МЕЖДУ группами на той же странице
                final_header = header if (parts_total == 1 and part_idx == 0) else _header_with_part(header, part_idx + 1, parts_total)
                acc.append(final_header)
                acc.extend(rows)
                rows_on_page += need
                first_group_on_page = False
                break
            else:
                if rows_on_page > 0:
                    # переносим всю группу на след. страницу
                    flush_page()
                    continue
                else:
                    # единственная группа на странице — режем
                    parts_total = (need + cap - 1) // cap if parts_total == 1 else parts_total
                    take = min(cap, need)
                    chunk = rows[:take]
                    rows = rows[take:]
                    need -= take
                    # первая часть — без пустой строки (страница пустая)
                    acc.append(_header_with_part(header, part_idx + 1, parts_total))
                    acc.extend(chunk)
                    rows_on_page += take
                    part_idx += 1
                    first_group_on_page = False
                    if need > 0:
                        flush_page()
                        continue
                    else:
                        break

    if acc:
        flush_page()

    total = max(1, len(pages))
    page_idx = max(0, min(page_idx, total - 1))
    # добавим футер «как в скринере»
    page_text = pages[page_idx] + f"\n\n— Page {page_idx+1}/{total} —"
    return page_text, total

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
    size_label = "Group size"
    lines.append(f"📦 Group: <b>{_esc(group_txt)}</b> | {size_label}: <b>{state.page_size}</b>")
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
        return [["📉 The data is loading or the filters didn't find anything. Try changing Filter/Settings."]]

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
    head_lines = header_lines(state)
    head_txt = "\n".join(head_lines)

    blocks = build_blocks(state, data)
    grouped_mode = any(len(b) > 1 or (b and b[0].startswith("<b>")) for b in blocks)
    sep_between_blocks = "\n\n" if grouped_mode else "\n"

    # текущий «жёсткий» лимит по числу облигационных строк на страницу
    cap = max(1, min(MAX_LINES_PER_PAGE, int(state.tx_rows_cap or MAX_LINES_PER_PAGE)))

    pages: List[str] = []
    acc = head_txt
    rows_on_page = 0
    first_block_on_page = True

    def flush_page():
        nonlocal acc, rows_on_page, first_block_on_page
        pages.append(acc)
        acc = head_txt
        rows_on_page = 0
        first_block_on_page = True

    def push_block(lines: List[str]):
        nonlocal acc, first_block_on_page
        prefix = "\n" if first_block_on_page else sep_between_blocks
        acc += prefix + "\n".join(lines)
        first_block_on_page = False

    def _header_with_part(h: str, idx: int, total: int) -> str:
        if total <= 1:
            return h
        # Вставляем " (i/n)" перед закрывающим </b>
        return h.replace("</b>", f" ({idx}/{total})</b>")

    for block in blocks:
        # определяем: это группа (с заголовком) или одиночная строка
        has_header = grouped_mode and len(block) >= 2 and block[0].startswith("<b>")
        header = block[0] if has_header else None
        rows = block[1:] if has_header else block[:]  # только облигационные строки
        need = len(rows)

        if not has_header:
            # Некомпактный режим: по 1 строке = 1 облигация
            if rows_on_page >= cap:
                flush_page()
            push_block(rows)         # rows тут длиной 1
            rows_on_page += 1
            continue

        parts_total = 1
        part_idx = 0

        # Групповой режим
        while True:
            remaining = cap - rows_on_page
            if need <= remaining:
                # вся группа влезает на текущую страницу (с заголовком)
                final_header = _header_with_part(header, part_idx + 1, parts_total) \
                    if (parts_total > 1 or part_idx > 0) else header
                push_block([final_header] + rows)
                rows_on_page += need
                break
            else:
                # группа не помещается на текущую страницу
                if rows_on_page > 0:
                    # на странице уже есть другие элементы → переносим ГОТОВУЮ группу целиком на следующую
                    flush_page()
                    # и на пустой странице попробуем снова
                    continue
                else:
                    # группа — единственная на странице → режем по cap
                    if parts_total == 1:
                        parts_total = (need + cap - 1) // cap  # ceil(need/cap) без math

                    take = max(1, min(cap, need))
                    chunk = rows[:take]
                    hdr = _header_with_part(header, part_idx + 1, parts_total)

                    push_block([hdr] + chunk)
                    rows = rows[take:]
                    need -= take
                    rows_on_page += len(chunk)
                    part_idx += 1

                    if need > 0:
                        flush_page()
                        continue
                    else:
                        break

    if acc.strip():
        pages.append(acc)

    if not pages:
        pages = ["(empty)"]

    total = len(pages)
    if state.page_idx >= total:
        state.page_idx = total - 1
    if state.page_idx < 0:
        state.page_idx = 0

    text = pages[state.page_idx]
    footer = f"\n\n— Page {state.page_idx+1}/{total} —"
    text = text + footer  # всегда добавляем; если Telegram не примет — уменьшим cap и перерисуем

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

def list_edit_keyboard(which: str, st: ScreenerState) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    if which == "wl":
        kb.button(text="➕ Add", callback_data="wl:add")
        kb.button(text="➖ Remove", callback_data="wl:del")
        cur = st.wl_page_idx + 1
        tot = max(1, st.wl_pages_count)
    else:
        kb.button(text="➕ Add", callback_data="bl:add")
        kb.button(text="➖ Remove", callback_data="bl:del")
        cur = st.bl_page_idx + 1
        tot = max(1, st.bl_pages_count)
    kb.adjust(2)
    kb.row(
        InlineKeyboardButton(text="⏮", callback_data=f"listnav:{which}:first"),
        InlineKeyboardButton(text="◀",  callback_data=f"listnav:{which}:prev"),
        InlineKeyboardButton(text=f"{cur}/{tot}", callback_data="listnav:noop"),
        InlineKeyboardButton(text="▶",  callback_data=f"listnav:{which}:next"),
        InlineKeyboardButton(text="⏭", callback_data=f"listnav:{which}:last"),
    )
    kb.row(InlineKeyboardButton(text="Close", callback_data=f"listnav:{which}:close"))
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
    kb.button(text=f"Group size: {state.page_size}", callback_data="settings:pagesize")
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
    def _calc_hash(s: str) -> int:
        return hash(f"{st.page_idx}|{st.ui_mode}|{s}")

    new_hash = _calc_hash(text)
    now = time.time()

    if st.last_render_hash is not None and new_hash == st.last_render_hash:
        return

    delta = now - st.last_edit_ts
    if delta <= EDIT_THROTTLE_SECONDS:
        delay = (EDIT_THROTTLE_SECONDS - delta) + 0.05
        asyncio.create_task(_schedule_edit(st, text, delay))
        return

    attempts = 0
    while True:
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
            return
        except TelegramBadRequest as e:
            emsg = str(e).lower()
            if "message is not modified" in emsg:
                st.last_render_hash = hash(f"{st.page_idx}|{st.ui_mode}|{text}")
                st.last_edit_ts = time.time()
                return

            too_long = (
                    "text is too long" in emsg
                    or "message is too long" in emsg
                    or "entities" in emsg
                    or "entity" in emsg
            )
            if not too_long:
                # чужая ошибка — выходим
                return

            # уменьшаем cap на 1, перерендериваем и повторяем
            cur = int(st.tx_rows_cap or MAX_LINES_PER_PAGE)
            st.tx_rows_cap = max(1, cur - 1)
            text, _ = render_page(st, snapshot_data())
            attempts += 1
            if attempts > (MAX_LINES_PER_PAGE + 5):  # предохранитель
                return

async def safe_send_initial(m: Message, st: ScreenerState):
    # стартуем с потолка
    st.tx_rows_cap = MAX_LINES_PER_PAGE
    attempts = 0
    while True:
        text, _ = render_page(st, snapshot_data())
        try:
            msg = await m.answer(text, reply_markup=current_markup(st), link_preview_options=LP_DISABLED)
            st.message_id = msg.message_id
            st.last_render_hash = hash(f"{st.page_idx}|{st.ui_mode}|{text}")
            st.last_edit_ts = time.time()
            return
        except TelegramBadRequest as e:
            emsg = str(e).lower()
            too_long = (
                "text is too long" in emsg
                or "message is too long" in emsg
                or "entities" in emsg
                or "entity" in emsg
            )
            if not too_long:
                # не наша ошибка — выходим
                raise

            # уменьшаем на 1 и пробуем дальше
            cur = int(st.tx_rows_cap or MAX_LINES_PER_PAGE)
            st.tx_rows_cap = max(1, cur - 1)
            attempts += 1
            if attempts > (MAX_LINES_PER_PAGE + 5):
                # предохранитель на случай «не влезает даже 1»
                head = "\n".join(header_lines(st))
                fallback = head + "\n\n⚠️ Page is too dense for Telegram. Narrow filters or reduce fields."
                msg = await m.answer(fallback, reply_markup=current_markup(st), link_preview_options=LP_DISABLED)
                st.message_id = msg.message_id
                st.last_render_hash = hash(f"{st.page_idx}|{st.ui_mode}|{fallback}")
                st.last_edit_ts = time.time()
                return

async def rerender_one(user_id: int):
    st = USER_STATES.get(user_id)
    if not st or not st.active or not st.chat_id or not st.message_id:
        return
    text, _ = render_page(st, snapshot_data())
    await safe_edit(st, text)

async def rerender_all_active():
    for uid in list(USER_STATES.keys()):
        await rerender_one(uid)

def _awaiting_list_edit(m: Message) -> bool:
    st = USER_STATES.setdefault(m.from_user.id, ScreenerState())
    return (st.awaiting_input or "").startswith(("wl:", "bl:"))

def _awaiting_preset_save(m: Message) -> bool:
    st = USER_STATES.get(m.from_user.id)
    return bool(st and st.awaiting_input == "preset_save")

import time as _time

@dataclass
class AlertRule:
    alert_id: int
    label: str
    enabled: bool = True
    mode: str = "repeat5"  # once | repeat5 | digest_day | digest_week
    config: Dict[str, Any] = field(default_factory=dict)  # scope/filters/conditions/trigger
    created_at: float = field(default_factory=time.time)

ALERT_NEXT_ID: Dict[int, int] = {}

def _alerts_next_id(uid: int) -> int:
    ALERT_NEXT_ID[uid] = ALERT_NEXT_ID.get(uid, 0) + 1
    return ALERT_NEXT_ID[uid]

@dataclass
class AlertsUIState:
    chat_id: Optional[int] = None
    message_id: Optional[int] = None
    screen: str = "root"         # root | manage | select_num | mute | rename
    selected_idx: int = 0
    mute_until: Optional[float] = None
    awaiting: Optional[str] = None   # None | "rename"
    wizard_edit_idx: Optional[int] = None  # None = создаём, иначе редактируем rules[idx]

    # --- safe edit like screener ---
    last_render_hash: Optional[int] = None
    last_edit_ts: float = 0.0

    # во время активного взаимодействия пользователя с UI
    busy_until: float = 0.0

    # отложенный flush, чтобы схлопывать частые апдейты
    pending_text: Optional[str] = None
    pending_markup: Optional[InlineKeyboardMarkup] = None
    pending_due_ts: float = 0.0
    pending_task: Optional[asyncio.Task] = field(default=None, repr=False, compare=False)

ALERT_RULES: Dict[int, List[AlertRule]] = {}
ALERT_UI: Dict[int, AlertsUIState] = {}

def _alerts_rules(uid: int) -> List[AlertRule]:
    return ALERT_RULES.setdefault(uid, [])

def _alerts_ui(uid: int) -> AlertsUIState:
    return ALERT_UI.setdefault(uid, AlertsUIState())

def _alerts_is_muted(uid: int) -> bool:
    ui = _alerts_ui(uid)
    if ui.mute_until is None:
        return False
    return _time.time() < ui.mute_until

def _fmt_hhmm(t) -> str | None:
    if t is None:
        return None
    if isinstance(t, int):
        return f"{t:02d}:00"
    s = str(t).strip()
    if re.match(r"^\d{1,2}$", s):
        return f"{int(s):02d}:00"
    m = re.match(r"^(\d{1,2}):(\d{1,2})$", s)
    if m:
        h = int(m.group(1))
        mm = int(m.group(2))
        if 0 <= h <= 23 and 0 <= mm <= 59:
            return f"{h:02d}:{mm:02d}"
    return s


def _alerts_mode_title(r: AlertRule) -> str:
    if r.mode == "once":
        return "Once (on crossover)"
    if r.mode == "repeat5":
        return "Perpetually"
    if r.mode == "digest_day":
        t = _fmt_hhmm((r.config.get("trigger") or {}).get("time"))
        return f"Digest (once a day{f' at {t}' if t else ''})"
    if r.mode == "digest_week":
        tr = r.config.get("trigger") or {}
        t = _fmt_hhmm(tr.get("time"))
        wd = tr.get("weekday")
        wd_map = ["Mon","Tue","Wen","Thu","Fri","Sat","Sun"]
        wd_s = wd_map[wd] if isinstance(wd, int) and 0 <= wd <= 6 else None
        suffix = ""
        if wd_s and t:
            suffix = f", {wd_s} {t}"
        elif wd_s:
            suffix = f", {wd_s}"
        elif t:
            suffix = f", {t}"
        return f"Digest (once a week{suffix})"
    return r.mode

def _alerts_root_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ New alert", callback_data="alerts:new")
    kb.button(text="📋 My alerts", callback_data="alerts:manage")
    kb.button(text="🔕 Mute all", callback_data="alerts:mute")
    kb.adjust(1)
    return kb.as_markup()

def _alerts_manage_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="✏️ Edit", callback_data="alerts:edit")
    kb.button(text="🗑 Delete", callback_data="alerts:delete")
    kb.button(text="⏸️ Pause / ▶️ Turn on", callback_data="alerts:toggle")
    kb.button(text="◀️ Prev", callback_data="alerts:prev")
    kb.button(text="Choose №", callback_data="alerts:select")
    kb.button(text="▶️ Next", callback_data="alerts:next")
    kb.button(text="⬅️ Back", callback_data="alerts:root")
    kb.button(text="➕ New alert", callback_data="alerts:new")
    kb.adjust(2, 1, 3, 2)
    return kb.as_markup()

def _alerts_close_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="✖️ Close", callback_data="alerts:close")
    kb.adjust(1)
    return kb.as_markup()

def _alerts_select_num_kb(uid: int) -> InlineKeyboardMarkup:
    rules = _alerts_rules(uid)
    kb = InlineKeyboardBuilder()
    # числа кнопками
    for i in range(len(rules)):
        kb.button(text=str(i + 1), callback_data=f"alerts:sel:{i}")
    # раскладка: до 5 в ряд
    if len(rules) <= 5:
        kb.adjust(len(rules) if rules else 1)
    else:
        rows = []
        cur = min(5, len(rules))
        while len(rows) * 5 < len(rules):
            rows.append(min(5, len(rules) - len(rows) * 5))
        kb.adjust(*rows)
    kb.row(InlineKeyboardButton(text="⬅️ Back", callback_data="alerts:manage"))
    return kb.as_markup()

def _alerts_mute_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🕐 Snooze for 1 hour", callback_data="alerts:mute:1h")
    kb.button(text="🌙 Snooze until end of day", callback_data="alerts:mute:eod")
    kb.button(text="📅 Snooze for 1 week", callback_data="alerts:mute:1w")
    kb.button(text="⏸ Mute until manually enabled", callback_data="alerts:mute:forever")
    kb.button(text="🔔 Unmute", callback_data="alerts:mute:off")
    kb.button(text="⬅️ Back", callback_data="alerts:root")
    kb.adjust(1)
    return kb.as_markup()

def _alerts_rename_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Back", callback_data="alerts:rename_cancel")
    kb.adjust(1)
    return kb.as_markup()

@dataclass
class AlertDraft:
    scope_kind: str = ""                  # papers | watchlist | market | screener | grouping
    papers: List[str] = field(default_factory=list)
    grouping_kind: Optional[str] = None   # rating/sector/country/has_offer/...
    grouping_values: Set[str] = field(default_factory=set)

    # фильтры (совместимы со скринером)
    filters: FilterState = field(default_factory=FilterState)
    inherited_from_screener: bool = False

ALERT_DRAFTS: Dict[int, AlertDraft] = {}

def _aw_draft(uid: int) -> AlertDraft:
    return ALERT_DRAFTS.setdefault(uid, AlertDraft())

def _aw_scope_summary(d: AlertDraft) -> str:
    if d.scope_kind == "papers":
        head = ", ".join(d.papers[:4])
        if len(d.papers) > 4:
            head += "…"
        return f"🎯 Бумаги: <code>{html.escape(head)}</code>"
    if d.scope_kind == "watchlist":
        return "📜 Watchlist"
    if d.scope_kind == "market":
        return "🌐 Весь рынок (минус blacklist)"
    if d.scope_kind == "screener":
        return "📊 Текущий скринер (текущие фильтры)"
    if d.scope_kind == "grouping":
        vals = ", ".join(list(d.grouping_values)[:6])
        if len(d.grouping_values) > 6:
            vals += "…"
        return f"🧩 Группировка: <b>{html.escape(str(d.grouping_kind))}</b> = <code>{html.escape(vals)}</code>"
    return "—"

def _aw_range_str(r: Range) -> str:
    if r.lo is not None and r.hi is not None:
        return f"{r.lo:g}-{r.hi:g}"
    if r.lo is not None:
        return f">={r.lo:g}"
    if r.hi is not None:
        return f"<={r.hi:g}"
    return "—"

def _aw_filters_summary(fs: FilterState) -> List[str]:
    out = []
    # несколько ключевых для отображения, остальное добавим позже
    if fs.ytw: out.append(f"YTW {_aw_range_str(fs.ytw)}")
    if fs.ytm: out.append(f"YTM {_aw_range_str(fs.ytm)}")
    if fs.ytc: out.append(f"YTC {_aw_range_str(fs.ytc)}")
    if fs.price: out.append(f"Price {_aw_range_str(fs.price)}")
    if fs.volume: out.append(f"Volume(₽) {_aw_range_str(fs.volume)}")
    if fs.dtm_days: out.append(f"DtM(d) {_aw_range_str(fs.dtm_days)}")
    if fs.offer_days: out.append(f"Offer(d) {_aw_range_str(fs.offer_days)}")
    if fs.has_offer is True: out.append("Has offer = True")
    if fs.has_offer is False: out.append("Has offer = False")
    if fs.ratings: out.append("Ratings: " + ",".join(sorted(fs.ratings)))
    if fs.coupons_per_year: out.append("Coupons/yr: " + ",".join(map(str, sorted(fs.coupons_per_year))))
    if fs.sectors: out.append(f"Sectors: {len(fs.sectors)}")
    if fs.countries: out.append(f"Countries: {len(fs.countries)}")
    return out

def _aw_kb_step1() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🎯 По конкретным бумагам", callback_data="alerts:w:scope:papers")
    kb.button(text="📜 По watchlist", callback_data="alerts:w:scope:watchlist")
    kb.button(text="🌐 По всему рынку", callback_data="alerts:w:scope:market")
    kb.button(text="📊 По текущему скринеру", callback_data="alerts:w:scope:screener")
    kb.button(text="🧩 По группировке (значения)", callback_data="alerts:w:scope:grouping")
    kb.button(text="❌ Отмена", callback_data="alerts:w:cancel")
    kb.adjust(1)
    return kb.as_markup()

def _aw_kb_papers_prompt() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Back", callback_data="alerts:w:step1")
    kb.button(text="❌ Cancel", callback_data="alerts:w:cancel")
    kb.adjust(2)
    return kb.as_markup()

def _aw_kb_step2_main() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="➡️ Continue without filters", callback_data="alerts:w:step3")

    kb.button(text="Yield ▸", callback_data="alerts:w:fcat:yield")
    kb.button(text="DtA ▸", callback_data="alerts:w:fcat:dta")
    kb.button(text="Credit ratings ▸", callback_data="alerts:w:ratings")

    kb.button(text="Last price", callback_data="alerts:w:range:price")
    kb.button(text="Volume (₽)", callback_data="alerts:w:range:volume_money")
    kb.button(text="Par value", callback_data="alerts:w:range:nominal")
    kb.button(text="Issue size", callback_data="alerts:w:range:issue")

    kb.button(text="Put/Call option (toggle)", callback_data="alerts:w:toggle:has_offer")
    kb.button(text="Clear filters", callback_data="alerts:w:filters_clear")

    kb.button(text="✅ Done", callback_data="alerts:w:step3")
    kb.button(text="⬅️ Back", callback_data="alerts:w:step1")
    kb.button(text="❌ Cancel", callback_data="alerts:w:cancel")
    kb.adjust(1, 3, 4, 2, 3)
    return kb.as_markup()


def _aw_kb_yield() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="YTM", callback_data="alerts:w:range:ytm")
    kb.button(text="YTC", callback_data="alerts:w:range:ytc")
    kb.button(text="YTW", callback_data="alerts:w:range:ytw")
    kb.button(text="YTM (w)", callback_data="alerts:w:range:ytm_w")
    kb.button(text="YTC (w)", callback_data="alerts:w:range:ytc_w")
    kb.button(text="YTW (w)", callback_data="alerts:w:range:ytw_w")
    kb.button(text="⬅️ Back", callback_data="alerts:w:step2")
    kb.adjust(3)
    return kb.as_markup()


def _aw_kb_dta() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="DtM", callback_data="alerts:w:range:dtm")
    kb.button(text="DtC", callback_data="alerts:w:range:dtc")
    kb.button(text="DtW", callback_data="alerts:w:range:dtw")
    kb.button(text="⬅️ Back", callback_data="alerts:w:step2")
    kb.adjust(3)
    return kb.as_markup()


def _aw_kb_ratings(uid: int) -> InlineKeyboardMarkup:
    d = _aw_draft(uid)
    selected = d.filters.ratings or set()
    kb = InlineKeyboardBuilder()
    for r in RATING_ORDER:
        mark = "✓ " if r in selected else ""
        kb.button(text=f"{mark}{r}", callback_data=f"alerts:w:rt:{r}")
    kb.button(text="✅ Done", callback_data="alerts:w:step2")
    kb.button(text="⬅️ Back", callback_data="alerts:w:step2")
    kb.adjust(5)
    return kb.as_markup()


def _aw_kb_range_prompt() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Back", callback_data="alerts:w:step2")
    kb.button(text="❌ Cancel", callback_data="alerts:w:cancel")
    kb.adjust(2)
    return kb.as_markup()


def _aw_kb_step3_stub() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Сохранить черновик", callback_data="alerts:w:save_draft")
    kb.button(text="⬅️ Назад", callback_data="alerts:w:step2")
    kb.button(text="❌ Отмена", callback_data="alerts:w:cancel")
    kb.adjust(1)
    return kb.as_markup()

def _aw_render(uid: int) -> Tuple[str, InlineKeyboardMarkup]:
    ui = _alerts_ui(uid)
    d = _aw_draft(uid)

    if ui.screen == "w_step1":
        txt = "<b>Step 1 of 5. What to track?</b>\n"
        return txt, _aw_kb_step1()

    if ui.screen == "w_papers":
        txt = (
            "<b>Step 1 of 5. Securities</b>\n\n"
            "Enter tickers/ISIN/UID as a list (separated by spaces or new lines).\n"
            "Example:\n<code>RU000A105KD1 RU000A105LQ3</code>"
        )
        return txt, _aw_kb_papers_prompt()

    if ui.screen == "w_step2":
        lines = [
            "<b>Step 2 of 5. Add filters?</b>",
            "",
            f"<b>Scope:</b> {_aw_scope_summary(d)}",
        ]
        cur = _aw_filters_summary(d.filters)
        if cur:
            lines.append("")
            lines.append("<b>Current filters:</b>")
            for x in cur[:10]:
                lines.append(f"🔹 {html.escape(x)}")
        return "\n".join(lines), _aw_kb_step2_main()

    if ui.screen == "w_f_yield":
        txt = "<b>Step 2 of 5. Filters → Yield</b>\n\nSelect a parameter:"
        return txt, _aw_kb_yield()

    if ui.screen == "w_f_dta":
        txt = "<b>Step 2 of 5. Filters → DtA</b>\n\nSelect a parameter:"
        return txt, _aw_kb_dta()

    if ui.screen == "w_f_ratings":
        txt = "<b>Step 2 of 5. Filters → Credit ratings</b>\n\nSelect a parameter:"
        return txt, _aw_kb_ratings(uid)

    if ui.screen == "w_range":
        # ui.awaiting = "w_range:<key>"
        key = (ui.awaiting or "").split(":", 1)[1] if (ui.awaiting or "").startswith("w_range:") else ""
        title, unit = pretty_key_and_unit(key)
        examples_raw = "10-15" if unit != "%" else "10-15%"
        ge_raw = ">=12.3" if unit != "%" else ">=12.3%"
        le_raw = "<=9.5" if unit != "%" else "<=9.5%"
        txt = (
            f"<b>Step 2 of 5. Range</b>\n\n"
            f"Enter condition for <b>{html.escape(title)}</b>\n"
            f"Format: <code>min-max</code>, <code>&gt;=x</code>, <code>&lt;=y</code>, <code>&gt;x</code>, <code>&lt;y</code>\n"
            f"Examples: <code>{html.escape(examples_raw)}</code>, <code>{html.escape(ge_raw)}</code>, <code>{html.escape(le_raw)}</code>\n"
        )
        return txt, _aw_kb_range_prompt()

    if ui.screen == "w_step3":
        txt = (
            "<b>Шаг 3 из 5</b>\n\n"
            "Дальше будет настройка условий (Простой/Продвинутый) — сделаем на этапе 4.\n\n"
            "Сейчас можно <b>сохранить черновик</b> (scope + фильтры)."
        )
        return txt, _aw_kb_step3_stub()

        # fallback
    ui.screen = "w_step1"
    return _aw_render(uid)

@dataclass
class AlertCond:
    key: str
    rng: Optional[Range] = None
    vals: Optional[list[str]] = None

@dataclass
class AlertWizardDraft:
    # step1 scope
    scope_kind: str = ""  # papers | watchlist | market | screener | grouping
    papers: list[str] = field(default_factory=list)

    # grouping (минимальная версия: Rating)
    grouping_kind: str | None = None
    grouping_values: Set[str] = field(default_factory=set)

    # step2 filters (subset как в фильтрах)
    filters: FilterState = field(default_factory=FilterState)

    inherited_from_screener: bool = False

    exclude_papers: list[str] = field(default_factory=list)  # фильтр: убрать бумаги (тикеры/ISIN/UID)

    # step3 conditions
    # step3 conditions (конструктор выражения)
    expr_tokens: list[str] = field(default_factory=list)  # например: ["(", "C0", "OR", "C1", ")", "AND", "C2"]
    # step3 conditions (совместимость + новый редактор выражения)
    cond_kind: str = ""  # legacy: simple | adv (может ещё использоваться старым кодом)
    logic: str = "and"   # legacy: and | or (чтобы не падал рендер/сейв)

    expr_tokens: list[str] = field(default_factory=list)  # например: ["(", "C0", "OR", "C1", ")", "AND", "C2"]
    expr_cursor: int = 0  # позиция курсора (между токенами)

    conditions: list[AlertCond] = field(default_factory=list)
    cr_pick: Set[str] = field(default_factory=set)


    cr_pick: Set[str] = field(default_factory=set)  # временно для экрана выбора кредитных рейтингов

    # step4 trigger
    trigger_kind: str = ""      # once | repeat5 | digest_day | digest_week
    digest_time: str | None = None   # HH:MM
    digest_weekday: int | None = None

    # step5 name
    name: str | None = None

ALERT_WIZARD: dict[int, AlertWizardDraft] = {}

def _awz_draft(uid: int) -> AlertWizardDraft:
    return ALERT_WIZARD.setdefault(uid, AlertWizardDraft())

def _awz_clear(uid: int):
    ALERT_WIZARD.pop(uid, None)

def _awz_range_str(r: Range | None) -> str:
    if r is None:
        return "—"

    lo = getattr(r, "lo", None)
    hi = getattr(r, "hi", None)
    inc_lo = getattr(r, "inc_lo", True)
    inc_hi = getattr(r, "inc_hi", True)

    if lo is not None and hi is not None:
        # диапазон всегда показываем как lo-hi
        return f"{lo:g}-{hi:g}"

    if lo is not None:
        sign = "≥" if inc_lo else ">"
        return f"{sign}{lo:g}"

    if hi is not None:
        sign = "≤" if inc_hi else "<"
        return f"{sign}{hi:g}"

    return "—"

def _awz_expr_open_cnt(tokens: list[str]) -> int:
    return sum(1 for t in tokens if t == "(") - sum(1 for t in tokens if t == ")")

def _awz_expr_expect_cond(tokens: list[str]) -> bool:
    if not tokens:
        return True
    return tokens[-1] in ("AND", "OR", "(")

def _awz_expr_remove_condition(d, idx: int):
    """Удаляет условие idx из d.conditions и переиндексирует expr_tokens (Ck -> remove, Cj>k -> C(j-1)).
    ВАЖНО: НЕ чистим "соседние" токены (AND/OR/скобки) — удаляем только то, что попросил пользователь.
    """
    if idx < 0 or idx >= len(d.conditions):
        return

    # удалить условие
    del d.conditions[idx]

    # пересобрать токены
    new_tokens: list[str] = []
    for t in (d.expr_tokens or []):
        t = str(t)
        if t.startswith("C"):
            try:
                j = int(t[1:])
            except Exception:
                new_tokens.append(t)
                continue
            if j == idx:
                continue
            if j > idx:
                new_tokens.append(f"C{j-1}")
            else:
                new_tokens.append(t)
        else:
            new_tokens.append(t)

    d.expr_tokens = new_tokens

    # курсор поджать
    if hasattr(d, "expr_cursor"):
        d.expr_cursor = max(0, min(int(d.expr_cursor or 0), len(d.expr_tokens)))

def _awz_scope_line(uid: int, d: AlertWizardDraft, exclude_uids: Optional[Set[str]] = None) -> str:
    uids = _awz_matching_uids(uid, d)
    if uids is None:
        n_txt = "…"
        uids = []
    else:
        if exclude_uids:
            ex = set(exclude_uids)
            uids = [x for x in uids if x not in ex]
        n_txt = str(len(uids))

    if d.scope_kind == "papers":
        nm = _awz_uids_display_names(uids, limit=4)
        return f"Specific bonds ({n_txt}): <code>{html.escape(nm or '—')}</code>"

    if d.scope_kind == "watchlist":
        return f"Watchlist ({n_txt})"

    if d.scope_kind == "market":
        return f"Whole market ({n_txt})"

    if d.scope_kind == "grouping":
        vals = ", ".join(list(d.grouping_values)[:6])
        if len(d.grouping_values) > 6:
            vals += "…"
        return f"Группа {d.grouping_kind}: <code>{html.escape(vals)}</code> ({n_txt})"

    if d.scope_kind == "screener":
        return f"Текущий скринер ({n_txt})"

    return f"— ({n_txt})"

def _awz_matching_uids(uid: int, d: AlertWizardDraft) -> Optional[list[str]]:
    """UID'ы бумаг, которые реально попадают под scope + exclude + filters (как будет в движке)."""
    data = snapshot_data() or {}
    if not data.get("uids") or not data.get("ticker"):
        return None

    cfg = {
        "scope_kind": d.scope_kind or "market",
        "papers": d.papers or [],
        "grouping_kind": d.grouping_kind,
        "grouping_values": sorted(list(d.grouping_values or set())),
        "exclude_papers": getattr(d, "exclude_papers", []) or [],
    }

    scope_uids = list(dict.fromkeys(_alerts_scope_uids(uid, cfg, data) or []))
    if not scope_uids:
        return []

    st = ScreenerState()
    st.filters = d.filters
    st.use_watchlist = False
    st.watchlist = set()
    st.blacklist = set()
    st.group = "none"
    st.page_size = 10**9

    d2 = dict(data)
    d2["uids"] = scope_uids

    rows = make_rows(st, d2) or []
    return [r["uid"] for r in rows if isinstance(r, dict) and "uid" in r]


def _awz_uids_display_names(uids: list[str], limit: int = 4) -> str:
    data = snapshot_data() or {}
    nmap = data.get("name", {}) or {}
    tmap = data.get("ticker", {}) or {}

    names = []
    for u in uids:
        nm = nmap.get(u)
        if nm:
            names.append(str(nm).strip())
        else:
            t = tmap.get(u)
            names.append(str(t).strip() if t else str(u))

    names = [x for x in names if x]
    head = names[:limit]
    tail = "…" if len(names) > limit else ""
    return ", ".join(head) + tail

def _awz_default_name(uid: int) -> str:
    """
    Базовое имя с порядковым номером.
    Для нового алерта: ALERT_NEXT_ID.get(uid,0)+1
    Для редактирования: используем alert_id текущего правила.
    """
    ui = _alerts_ui(uid)
    rules = _alerts_rules(uid)
    if ui.wizard_edit_idx is not None and rules:
        idx = min(max(ui.wizard_edit_idx, 0), len(rules) - 1)
        rid = getattr(rules[idx], "alert_id", None)
        if rid:
            return f"Alert {rid}"
    nxt = ALERT_NEXT_ID.get(uid, 0) + 1
    return f"Alert {nxt}"

def _uniq_keep_order(seq: list[str]) -> list[str]:
    # сохраняем порядок, убираем дубли
    return list(dict.fromkeys(seq))

def _awz_token_to_uid(tok: str, data: Dict[str, Any]) -> Optional[str]:
    tok = (tok or "").strip()
    if not tok:
        return None

    uids = set(data.get("uids", []) or [])
    if tok in uids:
        return tok

    tmap = data.get("ticker", {}) or {}
    uid_by_tkr = {str(t).upper(): u for u, t in tmap.items() if t}
    return uid_by_tkr.get(tok.upper())


def _awz_token_to_name(tok: str, data: Dict[str, Any]) -> str:
    """Пытаемся показать название бумаги; если нет — аккуратный fallback."""
    tok = (tok or "").strip()
    if not tok:
        return ""

    uid = _awz_token_to_uid(tok, data)
    if not uid:
        return tok  # не сопоставилось

    nmap = data.get("name", {}) or {}
    tmap = data.get("ticker", {}) or {}
    name = nmap.get(uid)
    if name:
        return str(name).strip()

    # если имени нет, показываем тикер по uid (а не исходный токен)
    tkr = tmap.get(uid)
    return str(tkr).strip() if tkr else tok


def _awz_tokens_display_names(tokens: List[str], limit: int = 10) -> str:
    data = snapshot_data() or {}
    out = []
    for tok in (tokens or []):
        s = _awz_token_to_name(str(tok), data)
        if s:
            out.append(s)
    out = list(dict.fromkeys(out))
    head = out[:limit]
    tail = "…" if len(out) > limit else ""
    return ", ".join(head) + tail

def _awz_conditions_line(d: AlertWizardDraft) -> str:
    if not d.conditions:
        return "—"
    parts = []
    for c in d.conditions[:4]:
        title, unit = pretty_key_and_unit(c.key)
        if c.vals:
            parts.append(f"{title}: {','.join(c.vals)}")
        else:
            parts.append(f"{title} {_awz_range_str(c.rng)}{unit}")

    s = (" & " if d.logic == "and" else " | ").join(parts)
    if len(d.conditions) > 4:
        s += "…"
    return s

def _awz_trigger_line(d: AlertWizardDraft) -> str:
    if d.trigger_kind == "once":
        return "🔔 Once (on crossover)"
    if d.trigger_kind == "repeat5":
        return "🔁 Perpetually"
    if d.trigger_kind == "digest_day":
        return f"📰 Digest (once a day {d.digest_time or '??:??'})"
    if d.trigger_kind == "digest_week":
        wd_map = ["Mon","Tue","Wen","Thu","Fri","Sat","Sun"]
        wd = wd_map[d.digest_weekday] if isinstance(d.digest_weekday, int) and 0 <= d.digest_weekday <= 6 else "??"
        return f"📰 Digest (once a week {wd} {d.digest_time or '??:??'})"
    return "—"

def _awz_set_filter_range(d: AlertWizardDraft, key: str, rng: Range | None):
    tmp = ScreenerState()
    tmp.filters = d.filters
    _assign_range(tmp, key, rng)
    d.filters = tmp.filters

def _awz_range_to_dict(r: Range | None) -> dict | None:
    if r is None:
        return None
    return {
        "lo": getattr(r, "lo", None),
        "hi": getattr(r, "hi", None),
        "inc_lo": getattr(r, "inc_lo", True),
        "inc_hi": getattr(r, "inc_hi", True),
    }

def _awz_range_from_dict(x: dict | None) -> Range | None:
    if not x:
        return None
    return Range(
        lo=x.get("lo", None),
        hi=x.get("hi", None),
        inc_lo=x.get("inc_lo", True),
        inc_hi=x.get("inc_hi", True),
    )

def _awz_filters_to_dict(fs: FilterState) -> dict:
    """Сериализация FilterState в dict.

    В текущем коде FilterState — не dataclass и не имеет .dict(),
    поэтому собираем поля из аннотаций + из __dict__ (для динамических фильтров).
    """
    out: dict = {}
    keys = set(getattr(FilterState, "__annotations__", {}).keys()) | set(getattr(fs, "__dict__", {}).keys())
    for k in sorted(keys):
        v = getattr(fs, k, None)
        if v is None:
            continue
        if isinstance(v, Range):
            out[k] = _awz_range_to_dict(v)
        elif isinstance(v, set):
            if v:
                out[k] = sorted(list(v))
        else:
            out[k] = v
    return out

def _awz_filters_from_dict(dct: dict) -> FilterState:
    fs = FilterState()
    for k, v in (dct or {}).items():
        if isinstance(v, dict) and ("lo" in v or "hi" in v):
            setattr(fs, k, _awz_range_from_dict(v))
        elif isinstance(v, list):
            setattr(fs, k, set(v))
        else:
            setattr(fs, k, v)
    return fs

def _awz_draft_from_rule(uid: int, r: AlertRule) -> AlertWizardDraft:
    d = AlertWizardDraft()
    cfg = r.config or {}
    d.scope_kind = cfg.get("scope_kind", "") or ""
    d.papers = cfg.get("papers", []) or []
    d.exclude_papers = cfg.get("exclude_papers", []) or []
    d.grouping_kind = cfg.get("grouping_kind", None)
    d.grouping_values = set(cfg.get("grouping_values", []) or [])

    d.filters = _awz_filters_from_dict(cfg.get("filters", {}) or {})

    d.cond_kind = cfg.get("cond_kind", "") or ""
    d.logic = (cfg.get("logic", "and") or "and").lower()
    d.conditions = []
    d.expr_tokens = list(cfg.get("expr") or [])
    for c in cfg.get("conditions", []) or []:
        if not isinstance(c, dict):
            continue
        key = c.get("key")
        vals = c.get("vals")
        if key and isinstance(vals, list) and vals:
            d.conditions.append(AlertCond(key=str(key), rng=None, vals=[str(x).upper() for x in vals]))
            continue

        rng = _awz_range_from_dict(c.get("rng"))
        if key and rng:
            d.conditions.append(AlertCond(key=str(key), rng=rng))

    if not d.expr_tokens and d.conditions:
        d.expr_tokens = []
        for i in range(len(d.conditions)):
            if i:
                d.expr_tokens.append("AND")
            d.expr_tokens.append(f"C{i}")

    tr = cfg.get("trigger") or {}
    d.trigger_kind = tr.get("kind", "") or r.mode or ""
    d.digest_time = tr.get("time")
    d.digest_weekday = tr.get("weekday")

    d.name = cfg.get("name") or r.label
    return d

def _awz_write_to_rule(uid: int, d: AlertWizardDraft, r: AlertRule):
    user_name = (d.name or "").strip()
    if user_name:
        name = user_name
    else:
        rid = getattr(r, "alert_id", None)
        name = f"Alert {rid}" if rid else _awz_default_name(uid)
        d.name = name  # фиксируем в черновике, чтобы и в config сохранилось

    r.label = name

    # mode по trigger_kind
    if d.trigger_kind in ("once", "repeat5", "digest_day", "digest_week"):
        r.mode = d.trigger_kind
    else:
        r.mode = "repeat5"

    conds_out = []
    for c in d.conditions:
        item = {"key": c.key}
        if c.vals:
            item["vals"] = list(c.vals)
        elif c.rng:
            item["rng"] = _awz_range_to_dict(c.rng)
        conds_out.append(item)

    r.config = {
        "name": name,
        "scope_kind": d.scope_kind,
        "papers": d.papers,
        "exclude_papers": d.exclude_papers,
        "grouping_kind": d.grouping_kind,
        "grouping_values": sorted(list(d.grouping_values)),
        "filters": _awz_filters_to_dict(d.filters),
        "cond_kind": d.cond_kind,
        "logic": (d.logic or "and").lower(),
        "conditions": conds_out,
        "expr": d.expr_tokens,
        "trigger": {"kind": d.trigger_kind, "time": d.digest_time, "weekday": d.digest_weekday},
    }


def _awz_kb_step1() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="📄 Specific bonds", callback_data="alerts:w:scope:papers")
    kb.button(text="⭐️ Watchlist", callback_data="alerts:w:scope:watchlist")
    kb.button(text="🌐 Whole market", callback_data="alerts:w:scope:market")
    kb.button(text="❌ Cancel", callback_data="alerts:w:cancel")
    kb.adjust(1)
    return kb.as_markup()

def _awz_kb_cond_cr(uid: int) -> InlineKeyboardMarkup:
    d = _awz_draft(uid)
    sel = d.cr_pick or set()

    kb = InlineKeyboardBuilder()
    for b in RATING_ORDER:
        mark = "✓ " if b in sel else ""
        kb.button(text=f"{mark}{RATING_EMOJI.get(b,'')} {b}", callback_data=f"alerts:w:cr_t:{b}")

    kb.button(text="✅ Done", callback_data="alerts:w:cr_done")
    kb.button(text="🧹 Clear", callback_data="alerts:w:cr_clear")
    kb.button(text="⬅️ Back", callback_data="alerts:w:step3")
    kb.adjust(3, 3, 3, 2, 1)
    return kb.as_markup()

def _awz_kb_back_cancel(back_cb: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="⬅️ Back", callback_data=back_cb)
    kb.button(text="❌ Cancel", callback_data="alerts:w:cancel")
    kb.adjust(2)
    return kb.as_markup()


def _awz_kb_group_rating(uid: int) -> InlineKeyboardMarkup:
    d = _awz_draft(uid)
    sel = d.grouping_values
    kb = InlineKeyboardBuilder()
    for r in RATING_ORDER:
        mark = "✓ " if r in sel else ""
        kb.button(text=f"{mark}{r}", callback_data=f"alerts:w:gval:{r}")
    kb.button(text="✅ Done", callback_data="alerts:w:step2")
    kb.button(text="⬅️ Back", callback_data="alerts:w:step1")
    kb.adjust(5)
    return kb.as_markup()

def _awz_kb_step2(uid: Optional[int] = None) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="➡️ Continue", callback_data="alerts:w:step3")

    kb.button(text="Issue size", callback_data="alerts:w:fr:issue")
    kb.button(text="Sectors ▸", callback_data="alerts:w:fcat:sectors")
    kb.button(text="Countries ▸", callback_data="alerts:w:fcat:countries")
    kb.button(text="Coupon per year ▸", callback_data="alerts:w:fcat:coupons")
    kb.button(text="Par value", callback_data="alerts:w:fr:nominal")

    kb.button(text="Put/Call option (toggle)", callback_data="alerts:w:toggle:has_offer")
    kb.button(text="Remove bonds", callback_data="alerts:w:excl:start")

    kb.button(text="🧹 Clear", callback_data="alerts:w:filters_clear")
    kb.button(text="⬅️ Back", callback_data="alerts:w:step1")
    kb.button(text="❌ Cancel", callback_data="alerts:w:cancel")

    kb.adjust(1, 2, 2, 1, 2, 2, 1, 2)
    return kb.as_markup()

def _awz_kb_f_sectors(uid: int) -> InlineKeyboardMarkup:
    d = _awz_draft(uid)
    selected = d.filters.sectors or set()
    opts = collect_all_sectors(snapshot_data())

    kb = InlineKeyboardBuilder()
    for s in opts:
        mark = "✓ " if s in selected else ""
        kb.button(text=f"{mark}{sector_display_with_emoji(s)}", callback_data=f"alerts:w:sect:{quote(s)}")

    kb.button(text="✅ Done", callback_data="alerts:w:step2")
    kb.button(text="⬅️ Back", callback_data="alerts:w:step2")
    kb.adjust(2)
    return kb.as_markup()


def _awz_kb_f_countries(uid: int) -> InlineKeyboardMarkup:
    d = _awz_draft(uid)
    selected = d.filters.countries or set()
    opts = collect_all_countries(snapshot_data())

    kb = InlineKeyboardBuilder()
    for i, c in enumerate(opts):
        mark = "✓ " if c in selected else ""
        kb.button(text=f"{mark}{c}", callback_data=f"alerts:w:ctry_i:{i}")
    kb.button(text="✅ Done", callback_data="alerts:w:step2")
    kb.button(text="⬅️ Back", callback_data="alerts:w:step2")
    kb.adjust(3)
    return kb.as_markup()


def _awz_kb_f_coupons(uid: int) -> InlineKeyboardMarkup:
    d = _awz_draft(uid)
    selected = d.filters.coupons_per_year or set()
    opts = collect_all_coupons(snapshot_data())

    kb = InlineKeyboardBuilder()
    for n in opts:
        mark = "✓ " if n in selected else ""
        kb.button(text=f"{mark}{n}", callback_data=f"alerts:w:cpn:{n}")
    kb.button(text="✅ Done", callback_data="alerts:w:step2")
    kb.button(text="⬅️ Back", callback_data="alerts:w:step2")
    kb.adjust(4)
    return kb.as_markup()

def _awz_kb_yield() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for key, label in [("ytm", "YTM"), ("ytc", "YTC"), ("ytw", "YTW"), ("ytm_w", "YTM (w)"), ("ytc_w", "YTC (w)"),
                       ("ytw_w", "YTW (w)")]:
        kb.button(text=label, callback_data=f"alerts:w:fr:{key}")
    kb.button(text="⬅️ Back", callback_data="alerts:w:step2")
    kb.adjust(3)
    return kb.as_markup()


def _awz_kb_dta() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for key, label in [("dtm", "DtM"), ("dtc", "DtC"), ("dtw", "DtW")]:
        kb.button(text=label, callback_data=f"alerts:w:fr:{key}")
    kb.button(text="⬅️ Back", callback_data="alerts:w:step2")
    kb.adjust(3)
    return kb.as_markup()


def _awz_kb_ratings_filter(uid: int) -> InlineKeyboardMarkup:
    d = _awz_draft(uid)
    sel = d.filters.ratings or set()
    kb = InlineKeyboardBuilder()
    for r in RATING_ORDER:
        mark = "✓ " if r in sel else ""
        kb.button(text=f"{mark}{r}", callback_data=f"alerts:w:rtf:{r}")
    kb.button(text="✅ Done", callback_data="alerts:w:step2")
    kb.button(text="⬅️ Back", callback_data="alerts:w:step2")
    kb.adjust(5)
    return kb.as_markup()

def _awz_kb_param_pick(_prefix: str) -> InlineKeyboardMarkup:
    """
    Совместимость: _awz_render() вызывает _awz_kb_param_pick(),
    но фактически используем новое меню параметров с подкатегориями.
    """
    return _awz_kb_param_root(0)


_AWZ_PARAM_GROUPS: dict[str, list[str]] = {
    "yield": ["ytm", "ytc", "ytw", "ytm_w", "ytc_w", "ytw_w"],
    "duration": [
        "dmac_ytm", "dmac_ytc", "dmac_ytw",
        "dmod_ytm", "dmod_ytc", "dmod_ytw",
        "dmac_ytm_w", "dmac_ytc_w", "dmac_ytw_w",
        "dmod_ytm_w", "dmod_ytc_w", "dmod_ytw_w",
    ],
    "chg_yield": ["dpp_ytm", "dpp_ytc", "dpp_ytw", "dpp_ytm_w", "dpp_ytc_w", "dpp_ytw_w"],
    "dta": ["dtm", "dtc", "dtw"],
}

def _awz_kb_param_root(uid: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="Yield ▸", callback_data="alerts:w:pcat:yield")
    kb.button(text="Duration ▸", callback_data="alerts:w:pcat:duration")
    kb.button(text="Change in yield ▸", callback_data="alerts:w:pcat:chg_yield")
    kb.button(text="DtA ▸", callback_data="alerts:w:pcat:dta")

    kb.button(text="Credit ratings", callback_data="alerts:w:range:cr")
    kb.button(text="Last price", callback_data="alerts:w:range:price")
    kb.button(text="Volume", callback_data="alerts:w:range:volume")
    kb.button(text="Volume / Issue size", callback_data="alerts:w:range:vol_issue_pct")

    kb.button(text="⬅️ Back", callback_data="alerts:w:pback")
    kb.button(text="❌ Cancel", callback_data="alerts:w:cancel")
    kb.adjust(2, 2, 2, 2, 2)
    return kb.as_markup()

def _awz_kb_param_group(uid: int, group: str) -> InlineKeyboardMarkup:
    keys = _AWZ_PARAM_GROUPS.get(group, [])
    kb = InlineKeyboardBuilder()
    for key in keys:
        title, _ = pretty_key_and_unit(key)
        kb.button(text=title, callback_data=f"alerts:w:range:{key}")
    kb.button(text="⬅️ Back", callback_data="alerts:w:param_root")
    kb.button(text="❌ Cancel", callback_data="alerts:w:cancel")
    kb.adjust(2, 2)
    return kb.as_markup()

def _awz_kb_adv_logic() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Все (И)", callback_data="alerts:w:logic:and")
    kb.button(text="🔀 Любое (ИЛИ)", callback_data="alerts:w:logic:or")
    kb.button(text="⬅️ Назад", callback_data="alerts:w:step3")
    kb.adjust(2, 1)
    return kb.as_markup()

def _awz_kb_adv_list() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить ещё условие", callback_data="alerts:w:addcond")
    kb.button(text="➡️ Дальше", callback_data="alerts:w:step4")
    kb.button(text="⬅️ Назад", callback_data="alerts:w:step3")
    kb.button(text="❌ Отмена", callback_data="alerts:w:cancel")
    kb.adjust(1, 2, 1)
    return kb.as_markup()

def _awz_kb_step4_trigger() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🔔 Once (on crossover)", callback_data="alerts:w:trig:once")
    kb.button(text="🔁 Perpetually", callback_data="alerts:w:trig:repeat5")
    kb.button(text="📰 Digest", callback_data="alerts:w:trig:digest")
    kb.button(text="⬅️ Back", callback_data="alerts:w:step3")
    kb.button(text="❌ Cancel", callback_data="alerts:w:cancel")
    kb.adjust(1)
    return kb.as_markup()

def _awz_kb_digest_kind() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="Once a day", callback_data="alerts:w:digest:day")
    kb.button(text="Once a week", callback_data="alerts:w:digest:week")
    kb.button(text="⬅️ Back", callback_data="alerts:w:step4")
    kb.adjust(2,1)
    return kb.as_markup()

def _awz_kb_weekdays() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for i,wd in enumerate(["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]):
        kb.button(text=wd, callback_data=f"alerts:w:wd:{i}")
    kb.button(text="⬅️ Back", callback_data="alerts:w:digest_kind")
    kb.adjust(7,1)
    return kb.as_markup()

def _awz_kb_time_presets() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for t in ["09:00","12:00","18:00","21:00"]:
        kb.button(text=t, callback_data=f"alerts:w:time:{t}")
    kb.button(text="Another time", callback_data="alerts:w:time:other")
    kb.button(text="⬅️ Back", callback_data="alerts:w:time_back")
    kb.adjust(4,1,1)
    return kb.as_markup()

def _awz_kb_step5_name() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="✏️ Enter name", callback_data="alerts:w:name:enter")
    kb.button(text="➡️ Use default name", callback_data="alerts:w:name:auto")
    kb.button(text="⬅️ Back", callback_data="alerts:w:step4")
    kb.button(text="❌ Cancel", callback_data="alerts:w:cancel")
    kb.adjust(1)
    return kb.as_markup()

def _awz_kb_summary(editing: bool) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Save" if editing else "✅ Create", callback_data="alerts:w:save")
    kb.button(text="⬅️ Back", callback_data="alerts:w:step5")
    kb.button(text="❌ Cancel", callback_data="alerts:w:cancel")
    kb.adjust(1)
    return kb.as_markup()

def _awz_expr_build_default(d: AlertWizardDraft):
    # если условия уже есть (старый алерт), а выражения нет — соберём C0 AND C1 AND ...
    if d.conditions and not (d.expr_tokens or []):
        d.expr_tokens = []
        for i in range(len(d.conditions)):
            if i:
                d.expr_tokens.append("AND")
            d.expr_tokens.append(f"C{i}")
        d.expr_cursor = len(d.expr_tokens)

def _awz_expr_validate(tokens: list[str]) -> tuple[bool, str]:
    if not tokens:
        return False, "Add at least one condition."
    if _awz_expr_open_cnt(tokens) != 0:
        return False, "Brackets are not closed."

    # грубая валидация порядка: нельзя начинать/заканчивать оператором, нельзя два оператора подряд
    ops = {"AND", "OR"}
    def is_cond(t: str) -> bool:
        return t.startswith("C")
    prev = None
    for t in tokens:
        if t in ops:
            if prev is None or prev in ops or prev == "(":
                return False, "Invalid position for AND/OR."
        if t == ")":
            if prev in ops or prev == "(" or prev is None:
                return False, "Invalid ')'."
        if is_cond(t):
            if prev and (prev.startswith("C") or prev == ")"):
                return False, "AND/OR must be between conditions."
        if t == "(":
            if prev and (prev.startswith("C") or prev == ")"):
                return False, "AND/OR is required before '('."
        prev = t

    if prev in ops or prev == "(":
        return False, "An expression can’t end with AND/OR or '('."
    return True, ""


def _awz_expr_pretty(d: AlertWizardDraft, show_cursor: bool = True) -> str:
    tokens = list(d.expr_tokens or [])
    cur = max(0, min(int(getattr(d, "expr_cursor", 0) or 0), len(tokens)))

    def show(t: str) -> str:
        if t == "AND":
            return "AND"
        if t == "OR":
            return "OR"
        if t.startswith("C"):
            try:
                return f"#{int(t[1:]) + 1}"
            except Exception:
                return t
        return t

    out = []
    if show_cursor:
        for i, t in enumerate(tokens):
            if i == cur:
                out.append("▮")
            out.append(show(str(t)))
        if cur == len(tokens):
            out.append("▮")
        return " ".join(out) if out else "▮"

    for t in tokens:
        out.append(show(str(t)))
    return " ".join(out) if out else "—"


def _awz_kb_expr(uid: int) -> InlineKeyboardMarkup:
    d = _awz_draft(uid)
    _awz_expr_build_default(d)

    kb = InlineKeyboardBuilder()

    kb.button(text="◀", callback_data="alerts:w:expr_left")
    kb.button(text="▶", callback_data="alerts:w:expr_right")
    kb.button(text="⌫", callback_data="alerts:w:expr_del")
    kb.adjust(3)

    kb.button(text="( ", callback_data="alerts:w:expr_ins:( ")
    kb.button(text=" )", callback_data="alerts:w:expr_ins:)")
    kb.button(text="AND", callback_data="alerts:w:expr_ins:AND")
    kb.button(text="OR", callback_data="alerts:w:expr_ins:OR")
    kb.adjust(4)

    kb.button(text="➕ Condition", callback_data="alerts:w:expr_add")
    kb.button(text="🧹 Clear", callback_data="alerts:w:expr_clear")
    kb.button(text="✅ Continue", callback_data="alerts:w:expr_done")
    kb.adjust(2, 1)

    kb.button(text="⬅️ Back", callback_data="alerts:w:step2")
    kb.button(text="❌ Cancel", callback_data="alerts:w:cancel")
    kb.adjust(2)

    return kb.as_markup()

def _awz_filters_lines(d: AlertWizardDraft) -> list[str]:
    fs = d.filters
    out: list[str] = []

    def rng_line(title: str, r: Optional[Range], unit: str = ""):
        if r is None:
            return
        out.append(f"🔹 {title}: {_awz_range_str(r)}{unit}")

    # Issue size (в Range хранится абсолют, у тебя в парсере это обычно *1e6)
    if getattr(fs, "issue_size", None) is not None:
        r = fs.issue_size
        lo = (r.lo / 1_000_000) if r.lo is not None else None
        hi = (r.hi / 1_000_000) if r.hi is not None else None
        if lo is not None and hi is not None:
            out.append(f"🔹 Issue size: {lo:g}-{hi:g} mln")
        elif lo is not None:
            out.append(f"🔹 Issue size: ≥{lo:g} mln")
        elif hi is not None:
            out.append(f"🔹 Issue size: ≤{hi:g} mln")

    if getattr(fs, "sectors", None):
        pretty = [sector_display_with_emoji(s) for s in sorted(fs.sectors)]
        out.append("🔹 Sectors: " + ", ".join(pretty))

    if getattr(fs, "countries", None):
        out.append("🔹 Countries: " + ", ".join(sorted(fs.countries)))

    if getattr(fs, "coupons_per_year", None):
        out.append("🔹 Coupon per year: " + ", ".join(map(str, sorted(fs.coupons_per_year))))

    rng_line("Par value", getattr(fs, "nominal", None))
    rng_line("Last price", getattr(fs, "price", None))
    rng_line("Volume (₽)", getattr(fs, "volume", None))
    rng_line("Volume / Issue size", getattr(fs, "vol_issue_pct", None), "%")

    if getattr(fs, "ratings", None):
        out.append("🔹 Ratings: " + ", ".join(_ratings_sorted_desc(list(fs.ratings))))

    ho = getattr(fs, "has_offer", None)
    if ho is True:
        out.append("🔹 Put/Call option: only")
    elif ho is False:
        out.append("🔹 Put/Call option: excluded")

    if getattr(d, "exclude_papers", None):
        ex = d.exclude_papers or []
        head = _awz_tokens_display_names(ex, limit=10)
        if head:
            out.append("🔹 Remove bonds: " + head)

    return out

# ---------- render ----------
def _awz_render(uid: int) -> tuple[str, InlineKeyboardMarkup]:
    ui = _alerts_ui(uid)
    d = _awz_draft(uid)

    if ui.screen == "aw_step1":
        return "<b>Step 1 of 5. What to track?</b>", _awz_kb_step1()

    if ui.screen == "aw_papers":
        txt = (
            "<b>Step 1 of 5. Securities</b>\n\n"
            "Enter tickers/ISIN/UID as a list (separated by spaces or new lines).\n"
            "Example:\n<code>RU000A105KD1 RU000A105LQ3</code>\n\n"
        )
        return txt, _awz_kb_back_cancel("alerts:w:step1")

    if ui.screen == "aw_excl":
        txt = (
            "<b>Step 2 of 5. Remove bonds</b>\n\n"
            "Enter tickers/UID as a list (separated by spaces or new lines).\n"
            "These securities will be excluded from the alert scope.\n\n"
            "Example:\n<code>RU000A105KD1 RU000A105LQ3</code>\n\n"
        )
        return txt, _awz_kb_back_cancel("alerts:w:step2")

    if ui.screen == "aw_group_rating":
        txt = "<b>Step 1 of 5. Select ratings (grouping)</b>\n\nYou can select multiple:"
        return txt, _awz_kb_group_rating(uid)

    if ui.screen == "aw_step2":
        lines = [
            "<b>Step 2 of 5. Add filters?</b>",
            "",
            f"<b>Scope:</b> {_awz_scope_line(uid, d)}",
            "",
            "<b>Current filters:</b>",
        ]
        fl = _awz_filters_lines(d)
        if fl:
            lines.extend([html.escape(x) for x in fl])
        else:
            lines.append("🔹 no filters")

        return "\n".join(lines), _awz_kb_step2(uid)

    if ui.screen == "aw_f_sectors":
        return "<b>Step 2 of 5. Filters → Sectors</b>\n\nYou can select multiple:", _awz_kb_f_sectors(uid)

    if ui.screen == "aw_f_countries":
        return "<b>Step 2 of 5. Filters → Countries</b>\n\nYou can select multiple:", _awz_kb_f_countries(uid)

    if ui.screen == "aw_f_coupons":
        return "<b>Step 2 of 5. Filters → Coupon per year</b>\n\nYou can select multiple:", _awz_kb_f_coupons(uid)

    if ui.screen == "aw_f_yield":
        return "<b>Step 2 of 5. Filters → Yield</b>\n\nSelect a parameter:", _awz_kb_yield()

    if ui.screen == "aw_f_dta":
        return "<b>Step 2 of 5. Filters → DtA</b>\n\nSelect a parameter:", _awz_kb_dta()

    if ui.screen == "aw_f_ratings":
        return "<b>Step 2 of 5. Filters → Рейтинг</b>\n\nSelect a parameter:", _awz_kb_ratings_filter(uid)

    if ui.screen == "aw_filter_range":
        key = (ui.awaiting or "").split(":", 1)[1] if (ui.awaiting or "").startswith("aw_filter_range:") else ""
        title, unit = pretty_key_and_unit(key)
        txt = (
            "<b>Step 2 of 5. Range</b>\n\n"
            f"Enter condition for <b>{html.escape(title)}</b>\n"
            "Format: <code>min-max</code>, <code>&gt;=x</code>, <code>&lt;=y</code>, <code>&gt;x</code>, <code>&lt;y</code>"
        )
        return txt, _awz_kb_back_cancel("alerts:w:step2")

    if ui.screen == "aw_step3":
        _awz_expr_build_default(d)
        ok, err = _awz_expr_validate(list(d.expr_tokens or []))

        lines = [
            "<b>Step 3 of 5. Conditions</b>",
            "",
            f"<b>Scope:</b> {_awz_scope_line(uid, d)}",
            "<b>Filters:</b>",
        ]

        fl = _awz_filters_lines(d)
        if fl:
            lines.extend([html.escape(x) for x in fl])
        else:
            lines.append("🔹 no filters")

        lines += [
            "",
            "<b>Expression:</b>",
            f"<code>{html.escape(_awz_expr_pretty(d))}</code>",
        ]

        if not ok:
            lines.append(f"\n⚠️ {html.escape(err)}")

        lines.append("\n<b>Conditions:</b>")
        if not d.conditions:
            lines.append("🔹 yet empty")
        else:
            for i, c in enumerate(d.conditions, 1):
                title, unit = pretty_key_and_unit(c.key)
                if c.vals:
                    vtxt = ", ".join(map(str, c.vals))
                    lines.append(f"🔹 #{i}: {html.escape(title)}: <code>{html.escape(vtxt)}</code>")
                else:
                    lines.append(
                        f"🔹 #{i}: {html.escape(title)} {html.escape(_awz_range_str(c.rng))}{html.escape(unit)}"
                    )

        return "\n".join(lines), _awz_kb_expr(uid)

    if ui.screen == "aw_simple_param":
        return "<b>Простой алерт. Выбери параметр:</b>", _awz_kb_param_pick("alerts:w:skey")

    if ui.screen == "aw_adv_logic":
        return "<b>Продвинутый алерт. Как связать условия?</b>", _awz_kb_adv_logic()

    if ui.screen == "aw_param_group":
        group = ""
        if (ui.awaiting or "").startswith("aw_param_group:"):
            group = ui.awaiting.split(":", 1)[1]
        return "<b>Select a parameter:</b>", _awz_kb_param_group(uid, group)

    if ui.screen == "aw_adv_list":
        lines = ["<b>Продвинутый алерт. Условия</b>", ""]
        if not d.conditions:
            lines.append("Пока нет условий. Нажми <b>➕ Добавить ещё условие</b>.")
        else:
            for i, c in enumerate(d.conditions, start=1):
                title, unit = pretty_key_and_unit(c.key)
                if getattr(c, "vals", None):
                    vtxt = ", ".join([str(x) for x in c.vals])
                    lines.append(
                        f"Условие {i}: <b>{html.escape(title)}</b>: <code>{html.escape(vtxt)}</code>"
                    )
                else:
                    lines.append(
                        f"Условие {i}: <b>{html.escape(title)}</b> {html.escape(_awz_range_str(c.rng))}{html.escape(unit)}"
                    )
            lines.append("")
            lines.append(f"Связка: <b>{'И' if d.logic == 'and' else 'ИЛИ'}</b>")
        return "\n".join(lines), _awz_kb_adv_list()

    if ui.screen == "aw_adv_param":
        return "<b>Condition: select a parameter</b>", _awz_kb_param_pick("alerts:w:akey")

    if ui.screen == "aw_cond_range":
        # awaiting: aw_cond_range:<key>
        key = (ui.awaiting or "").split(":")[-1] if (ui.awaiting or "").startswith("aw_cond_range:") else ""
        title, unit = pretty_key_and_unit(key)
        txt = (
            "<b>Step 3 of 5. Range</b>\n\n"
            f"Set a condition for <b>{html.escape(title)}</b>\n"
            "Format: <code>min-max</code>, <code>&gt;=x</code>, <code>&lt;=y</code>, <code>&gt;x</code>, <code>&lt;y</code>"
        )
        return txt, _awz_kb_back_cancel("alerts:w:step3")

    if ui.screen == "aw_cond_cr":
        sel = _ratings_sorted_desc(list(d.cr_pick or set()))
        sel_txt = ", ".join(sel) if sel else "—"
        return f"<b>Step 3 of 5. Credit ratings</b>\n\nSelected: <code>{html.escape(sel_txt)}</code>", _awz_kb_cond_cr(
            uid)

    if ui.screen == "aw_step4":
        return "<b>Step 4 of 5. Alert mode</b>", _awz_kb_step4_trigger()

    if ui.screen == "aw_digest_kind":
        return "<b>Digest</b>\n\nHow often?", _awz_kb_digest_kind()

    if ui.screen == "aw_weekday":
        return "<b>Digest: weekday</b>", _awz_kb_weekdays()

    if ui.screen == "aw_time_pick":
        return "<b>What time?</b>", _awz_kb_time_presets()

    if ui.screen == "aw_time_input":
        txt = "<b>Enter time</b> (HH:MM). Example: <code>21:00</code>"
        return txt, _awz_kb_back_cancel("alerts:w:time_pick")

    if ui.screen == "aw_step5":
        return "<b>Step 5 of 5. Alert name</b>\n\nGive it a name so it’s easier to recognize later.", _awz_kb_step5_name()

    if ui.screen == "aw_name_input":
        txt = "<b>Alert name</b>\n\nEnter the name."
        return txt, _awz_kb_back_cancel("alerts:w:step5")

    if ui.screen == "aw_summary":
        editing = _alerts_ui(uid).wizard_edit_idx is not None
        user_name = (d.name or "").strip()
        name = user_name if user_name else _awz_default_name(uid)

        lines = [
            "<b>Review & confirm</b>",
            "",
            f"<b>Alert name:</b> <code>{html.escape(name)}</code>",
            "",
            f"<b>Scope:</b> {_awz_scope_line(uid, d)}",
            "",
        ]

        fl = _awz_filters_lines(d)
        lines.append("<b>Filters:</b>")
        if fl:
            lines.extend([html.escape(x) for x in fl])
        else:
            lines.append("🔹 no filters")

        lines.append("")
        if hasattr(d, "expr_tokens") and d.expr_tokens:
            # выражение + список
            try:
                expr_txt = _awz_expr_pretty(d, show_cursor=False)
            except Exception:
                expr_txt = "—"
            lines.append("<b>Expression:</b>")
            lines.append(f"<code>{html.escape(expr_txt)}</code>")
            lines.append("")
            lines.append("<b>Conditions:</b>")
            if not d.conditions:
                lines.append("🔹 yet empty")
            else:
                for i, c in enumerate(d.conditions, 1):
                    title, unit = pretty_key_and_unit(c.key)
                    if getattr(c, "vals", None):
                        vtxt = ", ".join(map(str, c.vals))
                        lines.append(f"🔹 #{i}: {html.escape(title)}: <code>{html.escape(vtxt)}</code>")
                    else:
                        lines.append(
                            f"🔹 #{i}: {html.escape(title)} {html.escape(_awz_range_str(c.rng))}{html.escape(unit)}"
                        )

        else:
            # fallback на старую строку
            lines.append(f"<b>Conditions:</b> {html.escape(_awz_conditions_line(d))}")

        lines.append("")

        lines.append(f"<b>Trigger:</b> {_awz_trigger_line(d)}")

        return "\n".join(lines), _awz_kb_summary(editing)

        # fallback
    return "<b>Step 1 of 5. What to track?</b>", _awz_kb_step1()


def _alerts_render_root(uid: int) -> tuple[str, InlineKeyboardMarkup]:
    rules = _alerts_rules(uid)
    active = [r for r in rules if r.enabled]
    lines = []
    lines.append("<b>🔔 Alerts</b>")
    lines.append(f"Active: {len(active)}")
    lines.append("")
    if active:
        for i, r in enumerate(active[:20], start=1):
            lines.append(f"{i}️⃣ <code>{html.escape(r.label)}</code>")
    else:
        lines.append("No active alerts yet.")
    if _alerts_is_muted(uid):
        lines.append("")
        lines.append("🔕 <b>Muted</b> (notifications are paused).")
    return "\n".join(lines), _alerts_root_kb()

def _alerts_render_manage(uid: int) -> tuple[str, InlineKeyboardMarkup]:
    rules = _alerts_rules(uid)
    ui = _alerts_ui(uid)
    n = len(rules)
    if n == 0:
        txt = "<b>My alerts (0)</b>\n\nNo alerts yet. Tap <b>➕ New</b>."
        return txt, _alerts_manage_kb()

    # clamp selected
    if ui.selected_idx < 0:
        ui.selected_idx = 0
    if ui.selected_idx >= n:
        ui.selected_idx = n - 1

    lines = [f"<b>My alerts ({n})</b>", ""]
    for i, r in enumerate(rules, start=1):
        prefix = "▶️ " if (i - 1) == ui.selected_idx else ""
        status = "🟢" if r.enabled else "⏸️"
        lines.append(f"{prefix}{i}️⃣ <code>{html.escape(r.label)}</code> — {status} {_alerts_mode_title(r)}")

    # детали выбранного
    sel = rules[ui.selected_idx]
    try:
        d = _awz_draft_from_rule(uid, sel)
    except Exception:
        d = None

    if d:
        lines.append("")
        lines.append("────────────")
        lines.append(f"<b>Selected:</b> <code>{html.escape(sel.label)}</code>")
        lines.append("")
        exclude = None
        if sel and (getattr(sel, "mode", "") or "").strip() == "once":
            try:
                aid = int(getattr(sel, "alert_id", 0) or 0)
                exclude = _ALERTS_ONCE_FIRED.get(uid, {}).get(aid, set())
            except Exception:
                exclude = None

        lines.append(f"<b>Scope:</b> {_awz_scope_line(uid, d, exclude_uids=exclude)}")
        lines.append("")
        fl = _awz_filters_lines(d)
        lines.append("<b>Filters:</b>")
        if fl:
            lines.extend([html.escape(x) for x in fl])
        else:
            lines.append("🔹 no filters")

        if hasattr(d, "expr_tokens") and d.expr_tokens:
            lines.append("")
            lines.append("<b>Expression:</b>")
            try:
                lines.append(f"<code>{html.escape(_awz_expr_pretty(d, show_cursor=False))}</code>")
            except Exception:
                lines.append("<code>—</code>")

            lines.append("")
            lines.append("<b>Conditions:</b>")
            if not d.conditions:
                lines.append("🔹 yet empty")
            else:
                for i, c in enumerate(d.conditions, 1):
                    title, unit = pretty_key_and_unit(c.key)
                    if getattr(c, "vals", None):
                        vtxt = ", ".join(map(str, c.vals))
                        lines.append(f"🔹 #{i}: {html.escape(title)}: <code>{html.escape(vtxt)}</code>")
                    else:
                        lines.append(
                            f"🔹 #{i}: {html.escape(title)} {html.escape(_awz_range_str(c.rng))}{html.escape(unit)}"
                        )

        else:
            lines.append("")
            lines.append(f"<b>Conditions:</b> {html.escape(_awz_conditions_line(d))}")

    return "\n".join(lines), _alerts_manage_kb()

def _alerts_render_select_num(uid: int) -> tuple[str, InlineKeyboardMarkup]:
    rules = _alerts_rules(uid)
    if not rules:
        return "<b>Select №</b>\n\nNo alerts yet.", _alerts_manage_kb()
    return "<b>Select an alert number:</b>", _alerts_select_num_kb(uid)

def _alerts_render_rename(uid: int) -> tuple[str, InlineKeyboardMarkup]:
    rules = _alerts_rules(uid)
    ui = _alerts_ui(uid)
    if not rules:
        ui.awaiting = None
        ui.screen = "manage"
        return _alerts_render_manage(uid)

    idx = max(0, min(ui.selected_idx, len(rules) - 1))
    cur = rules[idx]
    ui.awaiting = "rename"
    ui.screen = "rename"
    txt = (
        "<b>Rename alert</b>\n\n"
        f"Current name:\n<code>{html.escape(cur.label)}</code>\n\n"
        "Enter new name.\n"
        "Cancel: /cancel"
    )
    return txt, _alerts_rename_kb()

def _alerts_kb_sig(markup: Optional[InlineKeyboardMarkup]) -> Any:
    if not markup:
        return ()
    try:
        rows = []
        for row in (markup.inline_keyboard or []):
            rows.append(tuple((b.text, getattr(b, "callback_data", None), getattr(b, "url", None)) for b in row))
        return tuple(rows)
    except Exception:
        return str(markup)


def _alerts_render_hash(text: str, markup: Optional[InlineKeyboardMarkup]) -> int:
    return hash((text, _alerts_kb_sig(markup)))


def _alerts_extract_retry_after(e: Exception) -> Optional[int]:
    # 1) TelegramRetryAfter has retry_after
    ra = getattr(e, "retry_after", None)
    if isinstance(ra, (int, float)) and ra > 0:
        return int(ra)

    # 2) parse from message: "retry after 185"
    s = str(e).lower()
    m = re.search(r"retry after (\d+)", s)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None


async def _alerts_flush_pending(uid: int):
    ui = _alerts_ui(uid)
    try:
        delay = max(0.0, ui.pending_due_ts - time.time())
        if delay > 0:
            await asyncio.sleep(delay)

        if not ui.chat_id or not ui.message_id:
            return

        text = ui.pending_text
        markup = ui.pending_markup
        if text is None:
            return

        h = _alerts_render_hash(text, markup)
        if ui.last_render_hash is not None and h == ui.last_render_hash:
            return

        try:
            await bot.edit_message_text(
                chat_id=ui.chat_id,
                message_id=ui.message_id,
                text=text,
                reply_markup=markup,
                link_preview_options=LP_DISABLED,
            )
            ui.last_render_hash = h
            ui.last_edit_ts = time.time()
            return

        except TelegramBadRequest as e:
            emsg = str(e).lower()
            if "message is not modified" in emsg:
                ui.last_render_hash = h
                ui.last_edit_ts = time.time()
                return
            if "message to edit not found" in emsg:
                ui.message_id = None
                return

            ra = _alerts_extract_retry_after(e)
            if ra:
                ui.pending_due_ts = time.time() + ra + 0.2
                ui.pending_task = asyncio.create_task(_alerts_flush_pending(uid))
                return

            raise

        except Exception as e:
            ra = _alerts_extract_retry_after(e)
            if ra:
                ui.pending_due_ts = time.time() + ra + 0.2
                ui.pending_task = asyncio.create_task(_alerts_flush_pending(uid))
                return

    finally:
        # задача отработала
        ui.pending_task = None


async def _alerts_edit(uid: int, text: str, markup: InlineKeyboardMarkup):
    ui = _alerts_ui(uid)
    if not ui.chat_id or not ui.message_id:
        return

    # запоминаем последнее, что хотим видеть
    ui.pending_text = text
    ui.pending_markup = markup

    new_hash = _alerts_render_hash(text, markup)
    if ui.last_render_hash is not None and new_hash == ui.last_render_hash:
        return

    now = time.time()
    delta = now - (ui.last_edit_ts or 0.0)

    # throttling — как в скринере
    if delta <= EDIT_THROTTLE_SECONDS:
        ui.pending_due_ts = now + (EDIT_THROTTLE_SECONDS - delta) + 0.05
        # отменяем предыдущий pending и планируем новый flush (схлопываем апдейты)
        if ui.pending_task and not ui.pending_task.done():
            ui.pending_task.cancel()
        ui.pending_task = asyncio.create_task(_alerts_flush_pending(uid))
        return

    # пробуем отредактировать сразу
    try:
        await bot.edit_message_text(
            chat_id=ui.chat_id,
            message_id=ui.message_id,
            text=text,
            reply_markup=markup,
            link_preview_options=LP_DISABLED,
        )
        ui.last_render_hash = new_hash
        ui.last_edit_ts = time.time()
        return

    except TelegramRetryAfter as e:
        ra = int(getattr(e, "retry_after", 0) or 0)
        await asyncio.sleep(ra + 0.25)
        try:
            await bot.edit_message_text(
                chat_id=ui.chat_id,
                message_id=ui.message_id,
                text=text,
                reply_markup=markup,
                link_preview_options=LP_DISABLED,
            )
        except TelegramBadRequest as e2:
            emsg = str(e2).lower()
            if "message is not modified" in emsg:
                return
            if "message to edit not found" in emsg:
                ui.message_id = None
                return
            print(f"[{_ts()}] ⚠️ alerts edit retry failed after {ra}s: {e2}")
        except Exception as e2:
            print(f"[{_ts()}] ⚠️ alerts edit retry failed after {ra}s: {e2}")
        return

    except TelegramBadRequest as e:
        emsg = str(e).lower()
        if "message is not modified" in emsg:
            ui.last_render_hash = new_hash
            ui.last_edit_ts = time.time()
            return
        if "message to edit not found" in emsg:
            ui.message_id = None
            return

        ra = _alerts_extract_retry_after(e)
        if ra:
            ui.pending_due_ts = time.time() + ra + 0.2
            if ui.pending_task and not ui.pending_task.done():
                ui.pending_task.cancel()
            ui.pending_task = asyncio.create_task(_alerts_flush_pending(uid))
            return

        raise

    except Exception as e:
        ra = _alerts_extract_retry_after(e)
        if ra:
            ui.pending_due_ts = time.time() + ra + 0.2
            if ui.pending_task and not ui.pending_task.done():
                ui.pending_task.cancel()
            ui.pending_task = asyncio.create_task(_alerts_flush_pending(uid))
            return

async def _alerts_send_message(uid: int, text: str, kb: Optional[InlineKeyboardMarkup] = None):
    """Глобальный sender для engine (не вложенный!). Возвращает Message или None."""
    try:
        if "LP_DISABLED" in globals():
            return await bot.send_message(uid, text, reply_markup=kb, link_preview_options=LP_DISABLED, parse_mode="HTML")
        return await bot.send_message(uid, text, reply_markup=kb, parse_mode="HTML")
    except Exception:
        try:
            # fallback без parse_mode/preview
            return await bot.send_message(uid, text, reply_markup=kb)
        except Exception:
            return None

# ====================== ALERTS (stage 5: engine + notifications) ======================

ALERTS_BUCKET_SEC = 300          # проверки "в конце 5-минутки"
ALERTS_LOOP_SLEEP_SEC = 3.0

_ALERTS_LAST_BUCKET: Optional[int] = None

# user -> alert_id -> uid -> bool
_ALERTS_LAST_TRUTH: Dict[int, Dict[int, Dict[str, bool]]] = defaultdict(lambda: defaultdict(dict))
# user -> alert_id -> set(uid)  (для mode=once)
_ALERTS_ONCE_FIRED: Dict[int, Dict[int, Set[str]]] = defaultdict(lambda: defaultdict(set))
# user -> alert_id -> uid -> {"uid":..., "ticker":..., "vals":{key: str}}
_ALERTS_DIGEST_BUF: Dict[int, Dict[int, Dict[str, Dict[str, Any]]]] = defaultdict(lambda: defaultdict(dict))
# user -> alert_id -> token (day/week) чтобы не слать повторно
_ALERTS_DIGEST_SENT: Dict[int, Dict[int, str]] = defaultdict(dict)
ALERT_REPEAT_COOLDOWN_SEC = 300  # 5 минут
# uid -> alert_id -> bond_uid -> ts_until
_ALERTS_REPEAT_COOLDOWN: Dict[int, Dict[int, Dict[str, float]]] = defaultdict(lambda: defaultdict(dict))

# user -> message_id -> {"pages": [str], "page_idx": int}
_ALERTS_DIGEST_VIEW: Dict[int, Dict[int, Dict[str, Any]]] = defaultdict(dict)

ALERTS_DIGEST_PAGE_CAP = 22  # максимум облигаций на страницу

# user -> alert_id -> set(bond_uid)  (бумаги, которые уже показывали пользователю в уведомлениях)
_ALERTS_SHOWN: Dict[int, Dict[int, Set[str]]] = defaultdict(lambda: defaultdict(set))

def _alerts_mark_shown(uid: int, alert_id: int, bond_uid: str):
    try:
        _ALERTS_SHOWN[uid][int(alert_id)].add(str(bond_uid))
    except Exception:
        pass

def _alerts_digest_nav_kb(page_idx: int, total: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()

    if total <= 1:
        kb.row(InlineKeyboardButton(text="✖️ Close", callback_data="adg:close"))
        return kb.as_markup()

    kb.row(
        InlineKeyboardButton(text="⏮", callback_data="adg:first"),
        InlineKeyboardButton(text="◀️", callback_data="adg:prev"),
        InlineKeyboardButton(text=f"{page_idx+1}/{total}", callback_data="noop"),
        InlineKeyboardButton(text="▶️", callback_data="adg:next"),
        InlineKeyboardButton(text="⏭", callback_data="adg:last"),
    )
    kb.row(InlineKeyboardButton(text="✖️ Close", callback_data="adg:close"))
    return kb.as_markup()


def _alerts_digest_item_line(it: Dict[str, Any]) -> str:
    # ссылка на тинькофф как в обычных алертах
    tkr = (it.get("ticker") or "").strip()
    name = (it.get("name") or tkr or "Бумага").strip()
    uid = it.get("uid")

    frag = note_fragment_for_uid(uid) if uid else ""
    url = f"https://www.tinkoff.ru/invest/bonds/{html.escape(tkr, quote=True)}"
    if frag:
        url += f"#{html.escape(frag, quote=True)}"

    link = f'<a href="{url}">{html.escape(name, quote=False)}</a>'

    checks = it.get("checks") or []
    tail = "; ".join([html.escape(str(x), quote=False) for x in checks])

    if tail:
        return f"🔹 {link} — {tail}"
    return f"🔹 {link}"

def _alerts_build_paged_list_pages(title_lines: List[str], items: List[Dict[str, Any]]) -> List[str]:
    lines = [_alerts_digest_item_line(it) for it in items]

    pages: List[str] = []
    for i in range(0, len(lines), ALERTS_DIGEST_PAGE_CAP):
        chunk = lines[i:i + ALERTS_DIGEST_PAGE_CAP]
        page_no = (i // ALERTS_DIGEST_PAGE_CAP) + 1
        total = (len(lines) + ALERTS_DIGEST_PAGE_CAP - 1) // ALERTS_DIGEST_PAGE_CAP

        txt_lines: List[str] = []
        txt_lines.extend(title_lines)
        txt_lines.append("")
        txt_lines.extend(chunk)
        txt_lines.append("")
        txt_lines.append(f"— Page {page_no}/{total} —")

        pages.append("\n".join(txt_lines))

    if pages:
        return pages

    # пустой список
    return ["\n".join(title_lines + ["", "(нет бумаг)"])]


def _alerts_build_primary_check_pages(label_html: str, now_msk: datetime, items: List[Dict[str, Any]]) -> List[str]:
    stamp = now_msk.strftime("%H:%M %d.%m.%Y")
    title_lines = [
        "⚠️ <b>Currently matches the condition (initial check)</b>",
        f"<b>{label_html}</b>",
        f"Snapshot: <b>{html.escape(stamp, quote=False)} MSK</b>",
    ]
    return _alerts_build_paged_list_pages(title_lines, items)


def _alerts_build_digest_snapshot_pages(label_html: str, now_msk: datetime, items: List[Dict[str, Any]]) -> List[str]:
    stamp = now_msk.strftime("%H:%M %d.%m.%Y")
    title_lines = [
        f"📰 <b>Alert Digest: {label_html}</b>",
        f"Matches: <b>{len(items)} bond(s)</b>",
        f"Snapshot: <b>{html.escape(stamp, quote=False)} MSK</b>",
    ]

    return _alerts_build_paged_list_pages(title_lines, items)

def _alerts_cfg(rule: Any) -> Dict[str, Any]:
    return getattr(rule, "config", None) or {}


def _range_from_obj(obj: Any) -> Optional[Range]:
    if obj is None:
        return None
    if isinstance(obj, Range):
        return obj
    if isinstance(obj, dict):
        return Range(
            lo=obj.get("lo"),
            hi=obj.get("hi"),
            inc_lo=obj.get("inc_lo", True),
            inc_hi=obj.get("inc_hi", True),
        )
    return None


def _filterstate_from_dict(d: Optional[Dict[str, Any]]) -> FilterState:
    f = FilterState()
    if not d:
        return f

    for k, v in d.items():
        if v is None:
            continue

        # диапазоны
        if isinstance(v, dict) and ("lo" in v or "hi" in v):
            setattr(f, k, _range_from_obj(v))
            continue

        # множества
        if k in ("ratings", "sectors", "countries", "coupons_per_year") and isinstance(v, (list, set, tuple)):
            setattr(f, k, set(v))
            continue

        setattr(f, k, v)

    return f


def _alerts_conditions_from_cfg(cfg: Dict[str, Any]) -> Tuple[str, List[dict]]:
    logic = (cfg.get("logic") or cfg.get("cond_logic") or "and").lower()
    raw = cfg.get("conditions") or []

    out: List[dict] = []
    for c in raw:
        if not isinstance(c, dict):
            continue
        key = str(c.get("key") or c.get("field") or "").strip()
        if not key:
            continue

        vals = c.get("vals")
        if isinstance(vals, list) and vals:
            out.append({"key": key, "vals": [str(x).upper() for x in vals]})
            continue

        rng = _range_from_obj(c.get("rng") or c.get("range"))
        if rng:
            out.append({"key": key, "rng": rng})

    # поддержка "простого" формата
    if not out:
        sk = cfg.get("simple_key")
        sr = _range_from_obj(cfg.get("simple_rng") or cfg.get("simple_range"))
        if sk and sr:
            out.append({"key": str(sk), "rng": sr})
            logic = "and"

    expr = cfg.get("expr")
    if isinstance(expr, list) and expr:
        return "expr", [{"expr": expr, "conds": out}]

    return logic, out


def _alerts_metric_value(key: str, row: Dict[str, Any]) -> Optional[float]:
    if key in ("cr", "CR", "rating", "RatingBucket"):
        b = row.get("RatingBucket")
        if not b:
            return None
        return RATING_SCORE.get(b)
    # доходности (в % как в фильтрах)
    if key in ("ytw", "YTW"):
        return (row.get("YTW") * 100.0) if row.get("YTW") is not None else None
    if key in ("ytm", "YTM"):
        return (row.get("YTM") * 100.0) if row.get("YTM") is not None else None
    if key in ("ytc", "YTC"):
        return (row.get("YTC") * 100.0) if row.get("YTC") is not None else None
    if key in ("ytw_w", "YTW_w"):
        return (row.get("YTW_w") * 100.0) if row.get("YTW_w") is not None else None
    if key in ("ytm_w", "YTM_w"):
        return (row.get("YTM_w") * 100.0) if row.get("YTM_w") is not None else None
    if key in ("ytc_w", "YTC_w"):
        return (row.get("YTC_w") * 100.0) if row.get("YTC_w") is not None else None

    # price / объём / сроки
    if key == "price":
        return row.get("Price")
    if key == "volume_money":
        return row.get("VolMoney")
    if key == "volume":
        return row.get("Vol")
    if key == "dtm":
        return row.get("DtM")
    if key == "dtc":
        return row.get("DtC")
    if key == "dtw":
        return row.get("DtW")

    # дюрации
    if key == "dmac":
        return row.get("D_mac")
    if key == "dmod":
        return row.get("D_mod")
    if key == "dmac_w":
        return row.get("D_mac_w")
    if key == "dmod_w":
        return row.get("D_mod_w")

    # ΔP и %change (в % как в фильтрах)
    if key in ("dpp", "dpp_w"):
        v = row.get("DPP_w") if key.endswith("_w") else row.get("DPP")
        return (v * 100.0) if v is not None else None

    if key.startswith("dpp_"):
        v = row.get({
                        "dpp_ytm": "DPP_YTM",
                        "dpp_ytc": "DPP_YTC",
                        "dpp_ytw": "DPP_YTW",
                        "dpp_ytm_w": "DPP_YTM_w",
                        "dpp_ytc_w": "DPP_YTC_w",
                        "dpp_ytw_w": "DPP_YTW_w",
                    }.get(key, ""), None)
        return (v * 100.0) if v is not None else None

    # fallback: ничего
    return None

def _alerts_truth_row(row: Dict[str, Any], logic: str, conds) -> bool:
    # поддержка старого вызова: _alerts_truth_row(row, conds, logic)
    if isinstance(logic, list) and isinstance(conds, str):
        logic, conds = conds, logic

    if not conds:
        return False

    if (logic or "").lower() == "expr":
        pack = conds[0] if isinstance(conds, list) and conds and isinstance(conds[0], dict) else {}
        expr = pack.get("expr") or []
        cdefs = pack.get("conds") or []

        # truth для каждой Ck
        truths: list[bool] = []
        for c in cdefs:
            key = c.get("key")
            if not key:
                truths.append(False)
                continue
            if "vals" in c:
                rb = normalize_rating(row.get("RatingBucket"))
                truths.append(rb in set(c.get("vals") or []))
            else:
                rng = c.get("rng")
                if rng is None:
                    truths.append(False)
                else:
                    val = _alerts_metric_value(key, row)
                    truths.append(rng.match(val))

        # shunting-yard -> RPN
        prec = {"OR": 1, "AND": 2}
        out_rpn = []
        st = []

        for t in expr:
            t = str(t)
            if t.startswith("C"):
                out_rpn.append(t)
            elif t in ("AND", "OR"):
                while st and st[-1] in ("AND", "OR") and prec[st[-1]] >= prec[t]:
                    out_rpn.append(st.pop())
                st.append(t)
            elif t == "(":
                st.append(t)
            elif t == ")":
                while st and st[-1] != "(":
                    out_rpn.append(st.pop())
                if st and st[-1] == "(":
                    st.pop()

        while st:
            out_rpn.append(st.pop())

        # eval RPN
        bs = []
        for t in out_rpn:
            if t.startswith("C"):
                try:
                    idx = int(t[1:])
                    bs.append(truths[idx] if 0 <= idx < len(truths) else False)
                except Exception:
                    bs.append(False)
            elif t in ("AND", "OR"):
                if len(bs) < 2:
                    return False
                b = bs.pop()
                a = bs.pop()
                bs.append(a and b if t == "AND" else a or b)

        return bool(bs[-1]) if bs else False


    checks = []
    for c in conds:
        if isinstance(c, tuple) and len(c) == 2:
            key, rng = c
            val = _alerts_metric_value(key, row)
            checks.append(rng.match(val))
            continue

        if not isinstance(c, dict):
            continue

        key = c.get("key")
        if not key:
            continue

        if "vals" in c:
            # credit ratings: OR внутри выбранного списка
            rb = normalize_rating(row.get("RatingBucket"))
            checks.append(rb in set(c.get("vals") or []))
        else:
            rng = c.get("rng")
            if rng is None:
                checks.append(False)
            else:
                val = _alerts_metric_value(key, row)
                checks.append(rng.match(val))

    if not checks:
        return False

    if (logic or "").lower() == "or":
        return any(checks)
    return all(checks)

class _ExprNode:
    __slots__ = ("op", "a", "b", "idx")
    def __init__(self, op: str, a=None, b=None, idx: int = -1):
        self.op = op
        self.a = a
        self.b = b
        self.idx = idx


def _alerts_cond_label(c) -> str:
    # tuple legacy
    if isinstance(c, tuple) and len(c) == 2:
        key, rng = c
        title, unit = pretty_key_and_unit(str(key))
        return f"{title} {_awz_range_str(rng)}{unit}"

    if not isinstance(c, dict):
        return "—"

    key = str(c.get("key") or "")
    title, unit = pretty_key_and_unit(key)

    if "vals" in c:
        vals = [str(x).upper() for x in (c.get("vals") or [])]
        vals_sorted = [r for r in RATING_ORDER if r in set(vals)]
        return f"{title}: {', '.join(vals_sorted)}"

    rng = c.get("rng")
    return f"{title} {_awz_range_str(rng)}{unit}"


def _alerts_cond_truth(c, row: dict) -> bool:
    if isinstance(c, tuple) and len(c) == 2:
        key, rng = c
        val = _alerts_metric_value(str(key), row)
        return bool(rng and rng.match(val))

    if not isinstance(c, dict):
        return False

    key = c.get("key")
    if not key:
        return False

    if "vals" in c:
        rb = normalize_rating(row.get("RatingBucket"))
        return rb in set(c.get("vals") or [])

    rng = c.get("rng")
    val = _alerts_metric_value(str(key), row)
    return bool(rng and rng.match(val))


def _expr_to_ast(expr_tokens: list[str]) -> _ExprNode | None:
    # shunting-yard -> RPN
    prec = {"AND": 2, "OR": 1}
    ops = []
    out = []

    for t in expr_tokens:
        t = str(t).strip()
        if not t:
            continue
        if t.startswith("C"):
            out.append(t)
        elif t in ("AND", "OR"):
            while ops and ops[-1] in ("AND", "OR") and prec[ops[-1]] >= prec[t]:
                out.append(ops.pop())
            ops.append(t)
        elif t == "(":
            ops.append(t)
        elif t == ")":
            while ops and ops[-1] != "(":
                out.append(ops.pop())
            if ops and ops[-1] == "(":
                ops.pop()

    while ops:
        out.append(ops.pop())

    # RPN -> AST
    st: list[_ExprNode] = []
    for t in out:
        if t.startswith("C"):
            try:
                idx = int(t[1:])
            except Exception:
                return None
            st.append(_ExprNode("C", idx=idx))
        elif t in ("AND", "OR"):
            if len(st) < 2:
                return None
            b = st.pop()
            a = st.pop()
            st.append(_ExprNode(t, a=a, b=b))
        else:
            return None

    return st[-1] if st else None


def _expr_eval(node: _ExprNode | None, truths: list[bool], memo: dict[int, bool]) -> bool:
    if node is None:
        return False
    nid = id(node)
    if nid in memo:
        return memo[nid]
    if node.op == "C":
        v = bool(truths[node.idx]) if 0 <= node.idx < len(truths) else False
    elif node.op == "AND":
        v = _expr_eval(node.a, truths, memo) and _expr_eval(node.b, truths, memo)
    elif node.op == "OR":
        v = _expr_eval(node.a, truths, memo) or _expr_eval(node.b, truths, memo)
    else:
        v = False
    memo[nid] = v
    return v


def _expr_pick_proof(node: _ExprNode | None, truths: list[bool], memo: dict[int, bool]) -> list[int]:
    """Возвращает индексы условий, которые объясняют срабатывание (одна ветка для OR)."""
    if node is None:
        return []
    if node.op == "C":
        return [node.idx] if (0 <= node.idx < len(truths) and truths[node.idx]) else []

    if node.op == "AND":
        # для AND нужны обе стороны
        left = _expr_pick_proof(node.a, truths, memo)
        right = _expr_pick_proof(node.b, truths, memo)
        return left + right

    if node.op == "OR":
        # выбираем ту ветку, которая истинна (левая приоритетнее)
        if _expr_eval(node.a, truths, memo):
            return _expr_pick_proof(node.a, truths, memo)
        if _expr_eval(node.b, truths, memo):
            return _expr_pick_proof(node.b, truths, memo)
        return []

    return []

def _alerts_fact_for_cond(c, row: dict) -> str:
    """Фактическое значение по условию, формат как в скринере."""
    # Credit rating
    if isinstance(c, dict) and "vals" in c:
        ro = row.get("RatingOrig") or row.get("RatingBucket")
        ro = (str(ro).strip() if ro is not None else "")
        return ro or "—"

    # legacy tuple
    key = None
    if isinstance(c, tuple) and len(c) == 2:
        key = str(c[0])
    elif isinstance(c, dict):
        key = str(c.get("key") or "")
    if not key:
        return "—"

    v = _alerts_metric_value(key, row)
    if v is None:
        return "—"

    # правила форматирования как в _alerts_fmt_keyval / скринере
    if key in ("ytw", "ytm", "ytc", "ytw_w", "ytm_w", "ytc_w"):
        return f"{v:.2f}%"
    if key in ("dpp", "dpp_w") or str(key).startswith("dpp_"):
        return f"{v:.2f}%"
    if key in ("dtm", "dtc", "dtw"):
        return f"{int(v)}d"
    if key == "price":
        return f"{v:.2f}₽"
    if key in ("volume_money",):
        return f"{fmt_num(v, 0)}₽"

    _, unit = pretty_key_and_unit(key)
    return f"{fmt_num(v, 2)}{unit}"


def _alerts_explain_row_with_values(logic: str, conds, row: dict) -> list[str]:
    """
    Как _alerts_explain_row, но добавляет факт-значение:
    '... (факт ...)'.
    """
    if not conds:
        return []

    used_conds = []

    if (logic or "").lower() == "expr":
        pack = conds[0] if isinstance(conds, list) and conds and isinstance(conds[0], dict) else {}
        expr = pack.get("expr") or []
        cdefs = pack.get("conds") or []

        truths = [_alerts_cond_truth(c, row) for c in cdefs]
        node = _expr_to_ast(expr)
        memo: dict[int, bool] = {}
        used = _expr_pick_proof(node, truths, memo)

        if not used:
            used = [i for i, t in enumerate(truths) if t]

        for i in used:
            if 0 <= i < len(cdefs):
                used_conds.append(cdefs[i])

    else:
        checks = [_alerts_cond_truth(c, row) for c in conds]
        if (logic or "").lower() == "or":
            for i, ok in enumerate(checks):
                if ok:
                    used_conds.append(conds[i])
                    break
        else:
            for i, ok in enumerate(checks):
                if ok:
                    used_conds.append(conds[i])

    out = []
    for c in used_conds:
        lbl = _alerts_cond_label(c)
        fact = _alerts_fact_for_cond(c, row)
        out.append(f"{lbl} (fact {fact})")
    return out

def _alerts_scope_uids(uid: int, cfg: Dict[str, Any], data: Dict[str, Any]) -> List[str]:
    """Scope для алертов: blacklist (/bl) НЕ влияет на область. Влияет только exclude_papers + выбранный scope."""
    st = USER_STATES.get(uid) or ScreenerState()
    wl = set(getattr(st, "watchlist", set()) or set())

    # TICKER->uid
    tmap = data.get("ticker", {}) or {}
    uid_by_tkr = {str(t).upper(): u for u, t in tmap.items() if t}
    all_uids = set(data.get("uids", []) or [])

    # exclude tokens -> uid
    ex_uids: set[str] = set()
    for tok in (cfg.get("exclude_papers") or []):
        if tok is None:
            continue
        s = str(tok).strip()
        if not s:
            continue
        if s in all_uids:
            ex_uids.add(s)
            continue
        u = uid_by_tkr.get(s.upper())
        if u:
            ex_uids.add(u)

    def _apply_exclude(res: list[str]) -> list[str]:
        res = list(dict.fromkeys(res))
        if not ex_uids:
            return res
        return [u for u in res if u not in ex_uids]

    kind = (cfg.get("scope_kind") or "market").lower()

    if kind == "papers":
        res = []
        for tok in (cfg.get("papers") or []):
            t = str(tok).strip().upper()
            u = uid_by_tkr.get(t)
            if u:
                res.append(u)
        return _apply_exclude(res)

    if kind == "watchlist":
        res = []
        for t in wl:
            u = uid_by_tkr.get(str(t).upper())
            if u:
                res.append(u)
        return _apply_exclude(res)

    if kind == "grouping":
        gk = (cfg.get("grouping_kind") or "").lower()
        vals = set(cfg.get("grouping_values") or [])
        if gk in ("rating", "ratings") and vals:
            res = []
            rmap = data.get("rating", {}) or {}
            for u in (data.get("uids", []) or []):
                rb = normalize_rating(rmap.get(u))
                if rb in vals:
                    res.append(u)
            return _apply_exclude(res)
        return []

    if kind == "screener":
        base = USER_STATES.get(uid)
        if not base:
            return []
        st2 = copy.deepcopy(base)
        st2.group = "none"
        st2.page_size = 10**9
        st2.tx_rows_cap = None
        st2.blacklist = set()  # важно: blacklist не влияет
        rows = make_rows(st2, data)
        res = [r.get("uid") for r in rows if isinstance(r, dict) and r.get("uid")]
        return _apply_exclude(res)

    # market
    res = [u for u in (data.get("uids", []) or []) if (tmap.get(u) is not None)]
    return _apply_exclude(res)

def _alerts_tinkoff_link(row: Dict[str, Any]) -> str:
    tkr = (row.get("Ticker") or "—")
    uid = row.get("uid")
    frag = note_fragment_for_uid(uid)

    # ссылка строится по тикеру (как было), но текст — название бумаги
    name = (
        row.get("Name")
        or row.get("ShortName")
        or row.get("Title")
        or row.get("SecurityName")
        or tkr
    )

    tkr_esc = html.escape(str(tkr), quote=True)
    name_esc = html.escape(str(name), quote=False)

    url = f"https://www.tinkoff.ru/invest/bonds/{tkr_esc}"
    if frag:
        url += f"#{frag}"

    return f'<a href="{url}">{name_esc}</a>'

def _parse_hhmm(s: Optional[str], default: str = "21:00"):
    from datetime import time as dtime
    raw = (s or default).strip()
    try:
        hh, mm = raw.split(":", 1)
        return dtime(int(hh), int(mm))
    except Exception:
        hh, mm = default.split(":")
        return dtime(int(hh), int(mm))

async def _alerts_primary_check_after_save(uid: int, rule: Any, now_msk: datetime):
    mode = (getattr(rule, "mode", "") or "").strip()
    if mode not in ("once", "repeat5"):
        return

    cfg = _alerts_cfg(rule)
    logic, conds = _alerts_conditions_from_cfg(cfg)
    if not conds:
        return

    data = snapshot_data()
    scope_uids = _alerts_scope_uids(uid, cfg, data)
    if not scope_uids:
        return

    scoped_data = dict(data)
    scoped_data["uids"] = scope_uids

    f = _filterstate_from_dict(cfg.get("filters") or {})
    st = ScreenerState()
    st.filters = f
    st.use_watchlist = False
    st.watchlist = set()
    st.blacklist = set()
    st.group = "none"
    st.page_size = 10**9

    rows = make_rows(st, scoped_data)

    alert_id = int(getattr(rule, "alert_id", 0) or 0)
    if alert_id <= 0:
        return

    prev_map = _ALERTS_LAST_TRUTH[uid][alert_id]
    items: List[Dict[str, Any]] = []

    for r in rows:
        if not isinstance(r, dict):
            continue
        buid = r.get("uid")
        if not buid:
            continue

        truth = _alerts_truth_row(r, logic, conds)

        if truth:
            checks = _alerts_explain_row_with_values(logic, conds, r)
            items.append({
                "uid": buid,
                "ticker": r.get("Ticker"),
                "name": r.get("Name"),
                "checks": checks,
                "ts": time.time(),
            })

            # ✅ важно: чтобы повторяющиеся ждали следующего False->True
            prev_map[buid] = True

            # ✅ для once: эти бумаги больше не отслеживаем
            if mode == "once":
                _ALERTS_ONCE_FIRED[uid][alert_id].add(buid)

    if not items:
        return

    items.sort(key=lambda x: (str(x.get("ticker") or ""), str(x.get("uid") or "")))

    label_html = html.escape(getattr(rule, "label", f"Alert {alert_id}"), quote=False)
    pages = _alerts_build_primary_check_pages(label_html, now_msk, items)

    msg = await _alerts_send_message(uid, pages[0], _alerts_digest_nav_kb(0, len(pages)))
    if msg:
        _ALERTS_DIGEST_VIEW[uid][msg.message_id] = {"pages": pages, "page_idx": 0}
        for it in items:
            _alerts_mark_shown(uid, alert_id, it["uid"])

async def _alerts_process_bucket(now_msk: datetime, force: bool = False):
    data = snapshot_data()

    for uid, rules in list(ALERT_RULES.items()):
        if not rules:
            continue

        # mute
        if "_alerts_is_muted" in globals():
            try:
                if _alerts_is_muted(uid):
                    continue
            except Exception:
                pass

        for idx, rule in enumerate(list(rules)):
            if not getattr(rule, "enabled", True):
                continue

            alert_id = getattr(rule, "alert_id", None) or (idx + 1)
            mode = getattr(rule, "mode", "repeat5")

            cfg = _alerts_cfg(rule)
            logic, conds = _alerts_conditions_from_cfg(cfg)
            if not conds:
                continue
            # ✅ digest теперь не живёт в bucket-цикле: отправляется только по расписанию как “срез”
            if mode in ("digest_day", "digest_week"):
                continue

            # scope + filters
            scope_uids = _alerts_scope_uids(uid, cfg, data)
            if not scope_uids:
                continue

            scoped_data = dict(data)
            scoped_data["uids"] = scope_uids

            f = _filterstate_from_dict(cfg.get("filters") or {})
            st = ScreenerState()
            st.filters = f
            st.use_watchlist = False
            st.watchlist = set()
            st.blacklist = set()   # тут blacklist выключен
            st.group = "none"
            st.page_size = 10**9

            rows = make_rows(st, scoped_data)

            prev_map = _ALERTS_LAST_TRUTH[uid][alert_id]

            for r in rows:
                if not isinstance(r, dict):
                    continue
                buid = r.get("uid")
                if not buid:
                    continue

                # once: если уже срабатывало по этой бумаге — пропускаем
                if mode == "once" and buid in _ALERTS_ONCE_FIRED[uid][alert_id]:
                    continue

                truth = _alerts_truth_row(r, logic, conds)
                prev = prev_map.get(buid, False)
                prev_map[buid] = truth

                if truth and not prev:
                    # repeat5 cooldown per bond
                    if mode == "repeat5":
                        now = time.time()
                        until = _ALERTS_REPEAT_COOLDOWN[uid][alert_id].get(buid, 0)
                        if now < until:
                            continue
                        _ALERTS_REPEAT_COOLDOWN[uid][alert_id][buid] = now + ALERT_REPEAT_COOLDOWN_SEC

                    if mode == "once":
                        _ALERTS_ONCE_FIRED[uid][alert_id].add(buid)

                    # вычислим какие условия true
                    # какие условия реально "доказали" истинность выражения (учитывает скобки/И/ИЛИ)
                    checks = _alerts_explain_row_with_values(logic, conds, r)

                    label = html.escape(getattr(rule, "label", f"Alert {alert_id}"), quote=False)
                    # ✅ DIGEST: не шлём сразу, копим до времени отправки
                    if mode in ("digest_day", "digest_week"):
                        _ALERTS_DIGEST_BUF[uid][alert_id][buid] = {
                            "uid": buid,
                            "ticker": r.get("Ticker"),
                            "name": r.get("Name"),
                            "checks": checks,
                            "ts": time.time(),
                        }
                        continue

                    # ✅ отдельное сообщение на каждую бумагу
                    msg_lines = []
                    msg_lines.append(f"🔔 <b>Alert: {label}</b>")
                    msg_lines.append("")
                    msg_lines.append(f"<b>Bond:</b> {_alerts_tinkoff_link(r)}")
                    msg_lines.append("")
                    if checks:
                        msg_lines.append("<b>Matched:</b>")
                        for label in checks:
                            msg_lines.append(f"🔹 {html.escape(label, quote=False)}")
                    else:
                        msg_lines.append("")
                        msg_lines.append("<b>Matched:</b>")
                        msg_lines.append("🔹 (couldn't determine)")

                    txt = "\n".join(msg_lines)
                    msg = await _alerts_send_message(uid, "\n".join(msg_lines), _alerts_close_kb())
                    if msg:
                        _alerts_mark_shown(uid, alert_id, buid)

async def _alerts_process_digests(now_msk: datetime):
    data = snapshot_data()

    for uid, rules in list(ALERT_RULES.items()):
        if not rules:
            continue

        if "_alerts_is_muted" in globals():
            try:
                if _alerts_is_muted(uid):
                    continue
            except Exception:
                pass

        for rule in rules:
            if not getattr(rule, "enabled", True):
                continue

            cfg = _alerts_cfg(rule)
            alert_id = int(getattr(rule, "alert_id", 0) or 0)
            mode = (getattr(rule, "mode", "") or "").strip()
            if mode not in ("digest_day", "digest_week"):
                continue

            trig = (cfg.get("trigger") or cfg.get("digest") or {}) or {}
            t_raw = trig.get("time") or cfg.get("digest_time") or "21:00"
            t = _parse_hhmm(t_raw, "21:00")

            # защита от повторной отправки в тот же период
            if mode == "digest_day":
                token = now_msk.strftime("%Y-%m-%d")
                if _ALERTS_DIGEST_SENT[uid].get(alert_id) == token:
                    continue
                if now_msk.time() < t:
                    continue
            else:
                wd = trig.get("weekday")
                if wd is None:
                    wd = cfg.get("digest_weekday")
                try:
                    wd = int(wd)
                except Exception:
                    wd = 0

                if now_msk.weekday() != wd:
                    continue

                iso = now_msk.isocalendar()
                token = f"{iso.year}-W{iso.week}"
                if _ALERTS_DIGEST_SENT[uid].get(alert_id) == token:
                    continue
                if now_msk.time() < t:
                    continue

            logic, conds = _alerts_conditions_from_cfg(cfg)
            if not conds:
                _ALERTS_DIGEST_SENT[uid][alert_id] = token
                continue

            scope_uids = _alerts_scope_uids(uid, cfg, data)
            if not scope_uids:
                _ALERTS_DIGEST_SENT[uid][alert_id] = token
                continue

            scoped_data = dict(data)
            scoped_data["uids"] = scope_uids

            f = _filterstate_from_dict(cfg.get("filters") or {})
            st = ScreenerState()
            st.filters = f
            st.use_watchlist = False
            st.watchlist = set()
            st.blacklist = set()
            st.group = "none"
            st.page_size = 10**9

            rows = make_rows(st, scoped_data)

            items: List[Dict[str, Any]] = []
            for r in rows:
                if not isinstance(r, dict):
                    continue
                buid = r.get("uid")
                if not buid:
                    continue

                truth = _alerts_truth_row(r, logic, conds)
                if truth:
                    checks = _alerts_explain_row_with_values(logic, conds, r)
                    items.append({
                        "uid": buid,
                        "ticker": r.get("Ticker"),
                        "name": r.get("Name"),
                        "checks": checks,
                        "ts": time.time(),
                    })

            if not items:
                _ALERTS_DIGEST_SENT[uid][alert_id] = token
                continue

            items.sort(key=lambda x: (str(x.get("ticker") or ""), str(x.get("uid") or "")))

            label_html = html.escape(getattr(rule, "label", f"Alert {alert_id}"), quote=False)
            pages = _alerts_build_digest_snapshot_pages(label_html, now_msk, items)

            msg = await _alerts_send_message(uid, pages[0], _alerts_digest_nav_kb(0, len(pages)))
            if msg:
                _ALERTS_DIGEST_VIEW[uid][msg.message_id] = {"pages": pages, "page_idx": 0}
                for it in items:
                    _alerts_mark_shown(uid, alert_id, it["uid"])
                    
            _ALERTS_DIGEST_SENT[uid][alert_id] = token

async def alerts_engine_loop():
    global _ALERTS_LAST_BUCKET
    while True:
        try:
            now_msk = _msk_now() if "_msk_now" in globals() else datetime.now()

            # дайджесты проверяем всегда
            await _alerts_process_digests(now_msk)

            # ночной простой: не считаем кроссы, чтобы не было "догонялок"
            if "_ENGINE_RUNNING" in globals() and not _ENGINE_RUNNING:
                await asyncio.sleep(ALERTS_LOOP_SLEEP_SEC)
                continue

            bucket = int(time.time() // ALERTS_BUCKET_SEC)
            if bucket != _ALERTS_LAST_BUCKET:
                _ALERTS_LAST_BUCKET = bucket
                await _alerts_process_bucket(now_msk)

        except Exception as e:
            print(f"[{_ts()}] ⚠️ ALERTS engine error: {e}")

        await asyncio.sleep(ALERTS_LOOP_SLEEP_SEC)

@dp.callback_query(F.data.startswith("alntf:"))
async def on_alert_notification_cb(q: CallbackQuery):
    uid = q.from_user.id
    parts = (q.data or "").split(":")
    # alntf:open:ID / alntf:toggle:ID / alntf:manage:ID
    if len(parts) != 3:
        await q.answer()
        return

    action = parts[1]
    try:
        alert_id = int(parts[2])
    except Exception:
        await q.answer()
        return

    rules = ALERT_RULES.get(uid) or []
    rule = next((r for r in rules if getattr(r, "alert_id", None) == alert_id), None)
    if not rule:
        await q.answer("Alert not found", show_alert=False)
        return

    if action == "toggle":
        rule.enabled = not getattr(rule, "enabled", True)
        await q.answer("Done")
        return

    if action == "manage":
        # переносим пользователя в /alerts -> manage (в том же сообщении /alerts)
        ui = _alerts_ui(uid)  # твой UI-стейт алертов
        ui.chat_id = q.message.chat.id if q.message else ui.chat_id
        ui.screen = "manage"
        ui.selected_idx = max(0, next((i for i, r in enumerate(rules) if getattr(r, "alert_id", None) == alert_id), 0))
        text, kb = _alerts_render_manage(uid)
        if ui.message_id and ui.chat_id:
            await _alerts_edit(uid, text, kb)
        else:
            msg = await q.message.answer(text, reply_markup=kb, link_preview_options=LP_DISABLED)
            ui.chat_id = msg.chat.id
            ui.message_id = msg.message_id
        await q.answer()
        return

    if action == "open":
        # открываем в скринере текущие "TRUE" бумаги этого алерта
        cfg = _alerts_cfg(rule)
        data = snapshot_data()
        logic, conds = _alerts_conditions_from_cfg(cfg)
        if not conds:
            await q.answer("Нет условий", show_alert=False)
            return

        scope_uids = _alerts_scope_uids(uid, cfg, data)
        scoped_data = dict(data)
        scoped_data["uids"] = scope_uids

        st_eval = ScreenerState()
        st_eval.filters = _filterstate_from_dict(cfg.get("filters") or {})
        st_eval.group = "none"
        st_eval.page_size = 10 ** 9

        rows = make_rows(st_eval, scoped_data)
        matched = [r for r in rows if "uid" in r and _alerts_truth_row(r, logic, conds)]
        uids_to_show = [r["uid"] for r in matched]

        if not uids_to_show:
            await q.answer("No securities matching this alert right now.", show_alert=False)
            return

        st = USER_STATES.setdefault(uid, ScreenerState())

        # мягко остановим старый скринер (как в /screener)
        if getattr(st, "active", False) and getattr(st, "chat_id", None) and getattr(st, "message_id", None):
            try:
                await bot.edit_message_reply_markup(chat_id=st.chat_id, message_id=st.message_id, reply_markup=None)
            except Exception:
                pass
            _cancel_idle_timer(st)
            st.message_id = None

        st.active = True
        st.chat_id = q.message.chat.id
        st.message_id = None
        st.awaiting_input = None
        st.prompt_msg_id = None
        st.ui_mode = "main"
        st.page_idx = 0
        st.tx_rows_cap = 40
        st.scope_uids = set(uids_to_show)
        st.filters = FilterState()  # чтобы показать все matched без ограничений

        await safe_send_initial(q.message, st)
        await q.answer()
        return

    await q.answer()

def _alerts_apply_screen(uid: int) -> tuple[str, InlineKeyboardMarkup]:
    ui = _alerts_ui(uid)

    if ui.screen.startswith("aw_"):
        return _awz_render(uid)

    if ui.screen.startswith("w_"):
        return _aw_render(uid)

    if ui.screen == "manage":
        return _alerts_render_manage(uid)
    if ui.screen == "select_num":
        return _alerts_render_select_num(uid)
    if ui.screen == "mute":
        return ("🔕 <b>Mute all alerts:</b>\n", _alerts_mute_kb())
    if ui.screen == "rename":
        return _alerts_render_rename(uid)
    # default root
    ui.screen = "root"
    return _alerts_render_root(uid)

async def _alerts_try_consume_text(m: Message) -> bool:
    """
    ВАЖНО: вызываем из общего on_text() в самом начале,
    чтобы rename работал независимо от порядка хэндлеров.
    """
    uid = m.from_user.id
    ui = ALERT_UI.get(uid)
    if ui:
        ui.busy_until = time.time() + 2.0

    # ===== alerts wizard input consume =====
    if ui and ui.screen.startswith("aw_"):
        txt = (m.text or "").strip()

        # /cancel для любого ввода мастера
        if txt.lower() == "/cancel":
            ui.awaiting = None
            # откат по контексту
            if ui.screen in ("aw_papers",):
                ui.screen = "aw_step1"
            elif ui.screen in ("aw_filter_range", "aw_f_yield", "aw_f_dta", "aw_f_ratings", "aw_excl",
                                   "aw_f_sectors", "aw_f_countries", "aw_f_coupons"):
                ui.screen = "aw_step2"
            elif ui.screen in ("aw_cond_range", "aw_simple_param", "aw_adv_logic", "aw_adv_list", "aw_adv_param"):
                ui.screen = "aw_step3"
            elif ui.screen in ("aw_time_pick", "aw_time_input", "aw_weekday", "aw_digest_kind"):
                ui.screen = "aw_step4"
            elif ui.screen in ("aw_name_input",):
                ui.screen = "aw_step5"
            else:
                ui.screen = "aw_step1"

            text, kb = _alerts_apply_screen(uid)
            await _alerts_edit(uid, text, kb)
            try:
                await bot.delete_message(m.chat.id, m.message_id)
            except Exception:
                pass
            return True

        d = _awz_draft(uid)

        # ввод списка бумаг
        if ui.awaiting == "aw_papers":
            tokens = [t.strip() for t in re.split(r"[\s,;]+", txt) if t.strip()]
            tokens = _uniq_keep_order(tokens)
            if not tokens:
                text, kb = _alerts_apply_screen(uid)
                await _alerts_edit(uid, text + "\n\n⚠️ No tickers found. Please try again.", kb)
                return True

            data0 = snapshot_data() or {}
            all_uids = set(data0.get("uids", []) or [])
            tmap = data0.get("ticker", {}) or {}
            uid_by_tkr = {str(t).upper(): u for u, t in tmap.items() if t}

            valid_tickers: list[str] = []
            bad: list[str] = []

            for tok in tokens:
                s = str(tok).strip()
                if not s:
                    continue

                # uid напрямую
                if s in all_uids:
                    tkr = tmap.get(s)
                    if not tkr:
                        bad.append(tok)
                        continue
                    valid_tickers.append(str(tkr).upper())
                    continue

                # ticker/isin
                t = s.upper()
                u = uid_by_tkr.get(t)
                if not u:
                    bad.append(tok)
                    continue
                valid_tickers.append(t)

            valid_tickers = _uniq_keep_order(valid_tickers)
            if not valid_tickers:
                text, kb = _alerts_apply_screen(uid)
                msg = text + "\n\n⚠️ Couldn’t match any securities."
                if bad:
                    msg += "\nNot found: <code>" + html.escape(", ".join(bad[:20])) + "</code>"
                await _alerts_edit(uid, msg, kb)
                return True

            d.scope_kind = "papers"
            d.papers = valid_tickers
            ui.awaiting = None
            ui.screen = "aw_step2"

            text, kb = _alerts_apply_screen(uid)
            extra = ""
            if bad:
                extra += "\n\n⚠️ Not found and removed: <code>" + html.escape(", ".join(bad[:20])) + "</code>"

            await _alerts_edit(uid, text + extra, kb)

            try:
                await bot.delete_message(m.chat.id, m.message_id)
            except Exception:
                pass
            return True


        # ввод списка исключаемых бумаг
        # ввод списка бумаг для исключения
        if ui.awaiting == "aw_excl":
            tokens = [t.strip() for t in re.split(r"[\s,;]+", txt) if t.strip()]
            seen = set()
            ex = []
            for t in tokens:
                tt = t.strip()
                if tt and tt not in seen:
                    seen.add(tt)
                    ex.append(tt)

            if not ex:
                text0, kb0 = _alerts_apply_screen(uid)
                await _alerts_edit(uid, text0 + "\n\n⚠️ No tickers found. Please try again.", kb0)
                return True

            # всегда сохраняем как фильтр (ничего не удаляем из d.papers)
            d.exclude_papers = _uniq_keep_order((d.exclude_papers or []) + ex)

            ui.awaiting = None
            ui.screen = "aw_step2"

            text1, kb1 = _alerts_apply_screen(uid)
            await _alerts_edit(uid, text1, kb1)

            try:
                await bot.delete_message(m.chat.id, m.message_id)
            except Exception:
                pass
            return True

        # ввод диапазона фильтра
        if (ui.awaiting or "").startswith("aw_filter_range:"):
            key = ui.awaiting.split(":", 1)[1]
            rng = parse_range(txt)
            if rng is None:
                text, kb = _alerts_apply_screen(uid)
                await _alerts_edit(uid,
                                   text + "\n\n⚠️ Invalid range. Examples: <code>10-15</code>, <code>>=12</code>, <code>>12</code>, <code><9</code>",
                                   kb)
                return True

            _awz_set_filter_range(d, key, rng)
            ui.awaiting = None
            ui.screen = "aw_step2"

            text, kb = _alerts_apply_screen(uid)
            await _alerts_edit(uid, text, kb)
            try:
                await bot.delete_message(m.chat.id, m.message_id)
            except Exception:
                pass
            return True

        # ввод диапазона условия (simple / adv)
        if (ui.awaiting or "").startswith("aw_cond_range:"):
            key = ui.awaiting.split(":", 1)[1]
            rng = parse_range(txt)
            if rng is None:
                text, kb = _alerts_apply_screen(uid)
                await _alerts_edit(uid,
                                   text + "\n\n⚠️ Invalid range. Examples: <code>10-15</code>, <code>>=12</code>, <code>>12</code>, <code><9</code>",
                                   kb)
                try:
                    await bot.delete_message(m.chat.id, m.message_id)
                except Exception:
                    pass

                return True

            d.conditions.append(AlertCond(key=key, rng=rng))
            cid = len(d.conditions) - 1

            # НЕ вызываем _awz_expr_build_default здесь, иначе будет дублирование
            d.expr_tokens = list(getattr(d, "expr_tokens", []) or [])
            if getattr(d, "expr_cursor", None) is None:
                d.expr_cursor = len(d.expr_tokens)

            cur = max(0, min(int(d.expr_cursor or 0), len(d.expr_tokens)))
            d.expr_tokens.insert(cur, f"C{cid}")
            d.expr_cursor = cur + 1

            ui.awaiting = None
            ui.screen = "aw_step3"

            # удалить введённое сообщение пользователя (чтобы не засорять чат)
            try:
                await m.delete()
            except Exception:
                pass

            screen_text, kb = _alerts_apply_screen(uid)
            await _alerts_edit(uid, screen_text, kb)
            return True


        if ui.awaiting == "aw_time_input":
            raw = txt.strip()
            if not re.match(r"^\d{1,2}:\d{2}$", raw):
                await m.answer("❌ Time format must be HH:MM (e.g., 21:00).")
                return True

            h, mm = raw.split(":")
            h = int(h)
            mm = int(mm)
            if not (0 <= h <= 23 and 0 <= mm <= 59):
                await m.answer("❌ Invalid time.")
                return True

            d.digest_time = f"{h:02d}:{mm:02d}"
            ui.awaiting = None
            ui.screen = "aw_step5"

            # важное: перерисовать мастер
            screen_text, kb = _alerts_apply_screen(uid)
            await _alerts_edit(uid, screen_text, kb)

            # удалить введённое сообщение пользователя
            try:
                await m.delete()
            except Exception:
                pass
            return True

        # ввод имени
        if ui.awaiting == "aw_name_input":
            # txt уже равен (m.text or "").strip() выше по коду
            d.name = txt[:120].strip()
            ui.awaiting = None
            ui.screen = "aw_summary"

            screen_text, kb = _alerts_apply_screen(uid)
            await _alerts_edit(uid, screen_text, kb)

            try:
                await bot.delete_message(m.chat.id, m.message_id)
            except Exception:
                pass
            return True

    # ===== alerts wizard (stage3): papers + range input =====
    if ui and (ui.awaiting or "").startswith("w_"):
        txt = (m.text or "").strip()
        if txt.lower() == "/cancel":
            # отмена текущего ввода
            if ui.awaiting == "w_papers":
                ui.awaiting = None
                ui.screen = "w_step1"
            elif (ui.awaiting or "").startswith("w_range:"):
                ui.awaiting = None
                ui.screen = "w_step2"
            text, kb = _alerts_apply_screen(uid)
            await _alerts_edit(uid, text, kb)
            try:
                await bot.delete_message(m.chat.id, m.message_id)
            except Exception:
                pass
            return True

        if ui.awaiting == "w_papers":
            tokens = [t.strip() for t in re.split(r"[\s,;]+", txt) if t.strip()]
            # уникализируем, сохраняя порядок
            seen = set()
            papers = []
            for t in tokens:
                if t not in seen:
                    seen.add(t)
                    papers.append(t)

            if not papers:
                # просто оставим тот же экран
                text, kb = _alerts_apply_screen(uid)
                await _alerts_edit(uid, text + "\n\n⚠️ Не вижу тикеров. Введи ещё раз.", kb)
                return True

            d = _aw_draft(uid)
            d.scope_kind = "papers"
            d.papers = papers
            ui.awaiting = None
            ui.screen = "w_step2"

            text, kb = _alerts_apply_screen(uid)
            await _alerts_edit(uid, text, kb)

            try:
                await bot.delete_message(m.chat.id, m.message_id)
            except Exception:
                pass
            return True

        if (ui.awaiting or "").startswith("w_range:"):
            key = ui.awaiting.split(":", 1)[1]
            rng = parse_range(txt)
            if rng is None:
                # перерисуем тот же prompt с предупреждением
                ui.screen = "w_range"
                text, kb = _alerts_apply_screen(uid)
                await _alerts_edit(uid,
                                   text + "\n\n⚠️ Не понял диапазон. Пример: <code>10-15</code> or <code>>=12</code>",
                                   kb)
                return True

            d = _aw_draft(uid)
            tmp = ScreenerState()
            tmp.filters = d.filters
            _assign_range(tmp, key, rng)
            d.filters = tmp.filters

            ui.awaiting = None
            ui.screen = "w_step2"

            text, kb = _alerts_apply_screen(uid)
            await _alerts_edit(uid, text, kb)

            try:
                await bot.delete_message(m.chat.id, m.message_id)
            except Exception:
                pass
            return True

    if not ui or ui.awaiting != "rename":
        return False

    txt = (m.text or "").strip()
    if not txt:
        return True

    if txt.lower() == "/cancel":
        ui.awaiting = None
        ui.screen = "manage"
        text, kb = _alerts_render_manage(uid)
        await _alerts_edit(uid, text, kb)
        try:
            await bot.delete_message(m.chat.id, m.message_id)
        except Exception:
            pass
        return True

    rules = _alerts_rules(uid)
    if not rules:
        ui.awaiting = None
        ui.screen = "manage"
        return True

    idx = max(0, min(ui.selected_idx, len(rules) - 1))
    # ограничим длину имени
    new_name = txt[:120]
    rules[idx].label = new_name

    ui.awaiting = None
    ui.screen = "manage"
    text, kb = _alerts_render_manage(uid)
    await _alerts_edit(uid, text, kb)

    # по возможности убираем сообщение пользователя (не критично)
    try:
        await bot.delete_message(m.chat.id, m.message_id)
    except Exception:
        pass
    return True

# ====================== Хэндлеры ======================
@dp.message(Command("start"))
async def on_start(m: Message):
    USER_STATES.setdefault(m.from_user.id, ScreenerState())
    await m.answer("Привет! Наберите /screener чтобы запустить живой облигационный скринер.", link_preview_options=LP_DISABLED)

@dp.message(Command("cutoff_test"))
async def cmd_cutoff_test(m: Message):
    parts = (m.text or "").split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await m.answer("Использование: /cutoff_test <тикер|ISIN|uid>")
        return

    q = parts[1].strip()
    q_up = q.upper()

    # 1) пробуем тикер среди уже загруженных облигаций
    uid = None
    for u, t in ticker_bonds.items():
        if (t or "").upper() == q_up:
            uid = u
            break

    # 2) если передали uid напрямую
    if uid is None and q in uid_bonds:
        uid = q

    # 3) иначе — через FindInstrument (для ISIN тоже)
    if uid is None:
        try:
            with Client(token) as client:
                fn = getattr(client.instruments, "find_instrument", None)
                if fn is None:
                    raise RuntimeError("find_instrument not found in SDK")
                r = fn(query=q)
                insts = getattr(r, "instruments", []) or []
                # пытаемся выбрать bond
                pick = None
                for inst in insts:
                    kind = getattr(inst, "instrument_kind", None) or getattr(inst, "kind", None)
                    if kind and "BOND" in str(kind):
                        pick = inst
                        break
                if pick is None and insts:
                    pick = insts[0]
                uid = getattr(pick, "uid", None) if pick else None
        except Exception:
            uid = None

    if not uid:
        await m.answer("Не смог определить облигацию по этому вводу. Попробуй тикер из списка бота или instrument_uid.")
        return

    settle = get_settlement_day().date()

    # берём ближайший купон и его fix_date
    try:
        with Client(token) as client:
            r = client.instruments.get_bond_coupons(
                instrument_id=uid,
                from_=datetime.today() - timedelta(days=30),
                to=datetime.today() + timedelta(days=365),
            )
    except Exception as e:
        await m.answer(f"Ошибка при запросе купонов: {e}")
        return

    if not getattr(r, "events", None):
        await m.answer("По этой облигации купоны не найдены.")
        return

    today = date.today()

    future = [ev for ev in r.events
              if getattr(ev, "coupon_date", None) and ev.coupon_date.date() >= today]

    if not future:
        await m.answer("Не нашёл ближайший будущий купон (в горизонте года).")
        return

    # берём реально ближайший
    nxt = min(future, key=lambda ev: ev.coupon_date)

    coupon_date = nxt.coupon_date.date()
    fix_date = _pb_to_date(getattr(nxt, "fix_date", None))
    pay = nxt.pay_one_bond.units + (nxt.pay_one_bond.nano * 10 ** (-9))

    eligible = (fix_date is None) or (fix_date >= settle)

    ticker = ticker_bonds.get(uid, "—")
    name = name_bonds.get(uid, "—")

    txt = (
        f"🧪 <b>cutoff_test</b>\n"
        f"<b>{html.escape(str(name))}</b> ({html.escape(str(ticker))})\n"
        f"uid: <code>{html.escape(uid)}</code>\n\n"
        f"Расчётный день покупки сегодня (T+1): <b>{settle.strftime('%d.%m.%Y')}</b>\n"
        f"Ближайший купон: <b>{coupon_date.strftime('%d.%m.%Y')}</b>, {pay:.2f} ₽\n"
        f"fix_date (фиксация реестра): <b>{fix_date.strftime('%d.%m.%Y') if fix_date else '— (нет в API)'}</b>\n\n"
        f"По логике бота купон при покупке сегодня: "
        f"{'<b>УЧИТЫВАЕТСЯ</b>' if eligible else '<b>НЕ учитывается</b>'}"
    )
    await m.answer(txt)

@dp.message(Command("wl","watchlist"))
async def cmd_wl(m: Message):
    st = USER_STATES.setdefault(m.from_user.id, ScreenerState())
    st.wl_cmd_msg_id = m.message_id            # ← сохраняем командное сообщение
    st.wl_page_idx = 0
    txt, total = _format_ticker_list_grouped("👀 Watchlist", st.watchlist, st.wl_page_idx, 23)
    st.wl_pages_count = total
    msg = await m.answer(txt, reply_markup=list_edit_keyboard("wl", st), link_preview_options=LP_DISABLED)
    st.wl_view_msg_id = msg.message_id

@dp.message(Command("bl","blacklist"))
async def cmd_bl(m: Message):
    st = USER_STATES.setdefault(m.from_user.id, ScreenerState())
    st.bl_cmd_msg_id = m.message_id            # ← сохраняем командное сообщение
    st.bl_page_idx = 0
    txt, total = _format_ticker_list_grouped("⛔ Blacklist", st.blacklist, st.bl_page_idx, 23)
    st.bl_pages_count = total
    msg = await m.answer(txt, reply_markup=list_edit_keyboard("bl", st), link_preview_options=LP_DISABLED)
    st.bl_view_msg_id = msg.message_id

@dp.message(Command("alerts", "alert", "al"))
async def cmd_alerts(m: Message):
    uid = m.from_user.id
    ui = ALERT_UI.setdefault(uid, AlertsUIState())
    ui.screen = "root"
    ui.selected_idx = 0

    text, kb = _alerts_render_root(uid)

    # пытаемся переиспользовать уже созданное "alerts-сообщение"
    if ui.chat_id == m.chat.id and ui.message_id:
        try:
            await bot.edit_message_text(
                chat_id=ui.chat_id,
                message_id=ui.message_id,
                text=text,
                reply_markup=kb,
                link_preview_options=LP_DISABLED
            )
            return
        except TelegramBadRequest:
            pass

    msg = await m.answer(text, reply_markup=kb, link_preview_options=LP_DISABLED)
    ui.chat_id = m.chat.id
    ui.message_id = msg.message_id


@dp.callback_query(F.data.startswith("alerts:"))
async def cb_alerts(q: CallbackQuery):
    """
    Единый callback-хендлер для /alerts: root/manage + мастер (wizard) в одном сообщении.
    ВАЖНО: раньше оставался stage-1 stub, который дергал _alerts_add_demo() и _alerts_edit_message().
    """
    uid = q.from_user.id
    ui = _alerts_ui(uid)
    ui.busy_until = time.time() + 2.0

    # привязываем состояние к сообщению
    if q.message:
        ui.chat_id = q.message.chat.id
        ui.message_id = q.message.message_id

    data = (q.data or "").strip()

    # закрыть (удалить) уведомление-алерт
    if data == "alerts:close":
        try:
            if q.message:
                await q.message.delete()
        except Exception:
            pass
        try:
            await q.answer()
        except Exception:
            pass
        return

    def _reset_runtime_state(alert_id: int):
        # сбрасываем "кросс"-состояния, чтобы после правок не было фантомных/пропущенных срабатываний
        try:
            _ALERTS_LAST_TRUTH[uid].pop(alert_id, None)
        except Exception:
            pass
        try:
            _ALERTS_ONCE_FIRED[uid].pop(alert_id, None)
        except Exception:
            pass
        try:
            _ALERTS_DIGEST_BUF[uid].pop(alert_id, None)
        except Exception:
            pass
        try:
            _ALERTS_DIGEST_SENT[uid].pop(alert_id, None)
        except Exception:
            pass

    try:
        # ===========================
        # 1) root/manage/select/mute
        # ===========================
        if data == "alerts:root":
            ui.screen = "root"
            ui.awaiting = None

        elif data == "alerts:manage":
            ui.screen = "manage"
            ui.awaiting = None

        elif data == "alerts:select":
            ui.screen = "select_num"
            ui.awaiting = None

        elif data.startswith("alerts:sel:"):
            # выбрать номер
            try:
                idx = int(data.split(":")[2])
            except Exception:
                idx = 0
            ui.selected_idx = max(0, idx)
            ui.screen = "manage"
            ui.awaiting = None

        elif data == "alerts:prev":
            rules = _alerts_rules(uid)
            if rules:
                ui.selected_idx = (ui.selected_idx - 1) % len(rules)
            ui.screen = "manage"
            ui.awaiting = None

        elif data == "alerts:next":
            rules = _alerts_rules(uid)
            if rules:
                ui.selected_idx = (ui.selected_idx + 1) % len(rules)
            ui.screen = "manage"
            ui.awaiting = None

        elif data == "alerts:toggle":
            rules = _alerts_rules(uid)
            if rules:
                idx = min(max(ui.selected_idx, 0), len(rules) - 1)
                rules[idx].enabled = not bool(getattr(rules[idx], "enabled", True))
            ui.screen = "manage"
            ui.awaiting = None

        elif data == "alerts:delete":
            rules = _alerts_rules(uid)
            if rules:
                idx = min(max(ui.selected_idx, 0), len(rules) - 1)
                alert_id = getattr(rules[idx], "alert_id", None) or (idx + 1)
                rules.pop(idx)
                _reset_runtime_state(int(alert_id))
                ui.selected_idx = max(0, min(ui.selected_idx, len(rules) - 1))
            ui.screen = "manage"
            ui.awaiting = None

        elif data == "alerts:edit":
            # редактируем текущий алерт через wizard
            rules = _alerts_rules(uid)
            if not rules:
                ui.screen = "root"
                ui.awaiting = None
            else:
                idx = min(max(ui.selected_idx, 0), len(rules) - 1)
                ui.wizard_edit_idx = idx
                _awz_clear(uid)
                ALERT_WIZARD[uid] = _awz_draft_from_rule(uid, rules[idx])
                ui.screen = "aw_summary"
                ui.awaiting = None

        elif data == "alerts:new":
            # старт wizard создания
            ui.wizard_edit_idx = None
            _awz_clear(uid)
            _awz_draft(uid)  # создаём пустой черновик
            ui.screen = "aw_step1"
            ui.awaiting = None

        elif data == "alerts:mute":
            ui.screen = "mute"
            ui.awaiting = None

        elif data.startswith("alerts:mute:"):
            # выбор mute
            choice = data.split(":")[2]
            now = _msk_now() if "_msk_now" in globals() else datetime.now()

            if choice == "1h":
                ui.mute_until = (now + timedelta(hours=1)).timestamp()
            elif choice == "eod":
                eod = now.replace(hour=23, minute=59, second=59, microsecond=0)
                ui.mute_until = eod.timestamp()
            elif choice == "1w":
                ui.mute_until = (now + timedelta(days=7)).timestamp()
            elif choice == "forever":
                ui.mute_until = (now + timedelta(days=3650)).timestamp()  # ~10 лет
            elif choice == "off":
                ui.mute_until = None

            ui.screen = "root"
            ui.awaiting = None

            # ===========================
            # 2) wizard callbacks: alerts:w:...
            # ===========================

        elif data.startswith("alerts:w:"):
            parts = data.split(":")
            cmd = parts[2] if len(parts) > 2 else ""
            arg = parts[3] if len(parts) > 3 else ""

            d = _awz_draft(uid)

            if cmd in ("step1", "step2", "step3", "step4", "step5"):
                # сбрасываем временный выбор рейтингов при выходе из окна выбора кредит. рейтинга
                if cmd == "step3" and ui.screen == "aw_cond_cr":
                    d.cr_pick = set()

                ui.screen = f"aw_{cmd}"
                ui.awaiting = None

            elif cmd == "expr_left":
                _awz_expr_build_default(d)
                d.expr_cursor = max(0, int(d.expr_cursor or 0) - 1)
                ui.screen = "aw_step3"
                ui.awaiting = None

            elif cmd == "expr_right":
                _awz_expr_build_default(d)
                d.expr_cursor = min(len(d.expr_tokens or []), int(d.expr_cursor or 0) + 1)
                ui.screen = "aw_step3"
                ui.awaiting = None

            elif cmd == "expr_del":
                _awz_expr_build_default(d)

                cur = max(0, min(int(d.expr_cursor or 0), len(d.expr_tokens or [])))
                if cur <= 0:
                    ui.screen = "aw_step3"
                    ui.awaiting = None
                else:
                    tok = str(d.expr_tokens[cur - 1])

                    # удаляем токен слева от курсора
                    del d.expr_tokens[cur - 1]
                    d.expr_cursor = cur - 1

                    # если это было условие Ck — удаляем сам объект условия и переиндексируем всё
                    if tok.startswith("C"):
                        try:
                            k = int(tok[1:])
                            _awz_expr_remove_condition(d, k)
                        except Exception:
                            pass

                    ui.screen = "aw_step3"
                    ui.awaiting = None

            elif cmd == "expr_ins":
                _awz_expr_build_default(d)
                tok = (arg or "").strip()
                tok = tok.replace(" ", "")
                if tok in ("(", ")", "AND", "OR"):
                    cur = max(0, min(int(d.expr_cursor or 0), len(d.expr_tokens or [])))
                    d.expr_tokens.insert(cur, tok)
                    d.expr_cursor = cur + 1
                ui.screen = "aw_step3"
                ui.awaiting = None

            elif cmd == "expr_clear":
                d.expr_tokens = []
                d.expr_cursor = 0
                d.conditions = []
                ui.screen = "aw_step3"
                ui.awaiting = None

            elif cmd == "expr_add":
                _awz_expr_build_default(d)
                ui.screen = "aw_adv_param"
                ui.awaiting = None

            elif cmd == "expr_done":
                _awz_expr_build_default(d)
                ok, _ = _awz_expr_validate(list(d.expr_tokens or []))
                if ok:
                    ui.screen = "aw_step4"
                else:
                    ui.screen = "aw_step3"
                ui.awaiting = None

            elif cmd == "cancel":
                ui.wizard_edit_idx = None
                _awz_clear(uid)
                ui.screen = "root"
                ui.awaiting = None

            elif cmd == "scope":
                # Шаг 1: scope
                if arg == "papers":
                    d.scope_kind = "papers"
                    d.papers = []
                    ui.screen = "aw_papers"
                    ui.awaiting = "aw_papers"
                elif arg == "watchlist":
                    d.scope_kind = "watchlist"
                    ui.screen = "aw_step2"
                    ui.awaiting = None
                elif arg == "market":
                    d.scope_kind = "market"
                    ui.screen = "aw_step2"
                    ui.awaiting = None
                elif arg == "screener":
                    d.scope_kind = "screener"
                    ui.screen = "aw_step2"
                    ui.awaiting = None
                elif arg == "grouping":
                    d.scope_kind = "grouping"
                    # пока реализован выбор по рейтингу (как в _awz_render)
                    d.grouping_kind = "Rating"
                    d.grouping_values = set()
                    ui.screen = "aw_group_rating"
                    ui.awaiting = None
                else:
                    ui.screen = "aw_step1"

            elif cmd == "gval":
                # выбор значений группировки (rating)
                if arg:
                    if arg in d.grouping_values:
                        d.grouping_values.remove(arg)
                    else:
                        d.grouping_values.add(arg)
                ui.screen = "aw_group_rating"
                ui.awaiting = None

            elif cmd == "fcat":
                # Шаг 2: фильтры — категории
                if arg == "yield":
                    ui.screen = "aw_f_yield"
                elif arg == "dta":
                    ui.screen = "aw_f_dta"
                elif arg == "ratings":
                    ui.screen = "aw_f_ratings"
                elif arg == "sectors":
                    ui.screen = "aw_f_sectors"
                elif arg == "countries":
                    ui.screen = "aw_f_countries"
                elif arg == "coupons":
                    ui.screen = "aw_f_coupons"
                else:
                    ui.screen = "aw_step2"
                ui.awaiting = None
            elif cmd == "sect":
                # toggle sector
                val = unquote(arg or "")
                d.filters.sectors = set(d.filters.sectors or set())
                if val:
                    if val in d.filters.sectors:
                        d.filters.sectors.remove(val)
                    else:
                        d.filters.sectors.add(val)
                ui.screen = "aw_f_sectors"
                ui.awaiting = None

            elif cmd == "ctry_i":
                # toggle country by index (stable ids)
                try:
                    i = int(arg)
                except Exception:
                    i = -1
                opts = collect_all_countries(snapshot_data())
                if 0 <= i < len(opts):
                    val = opts[i]
                    d.filters.countries = set(d.filters.countries or set())
                    if val in d.filters.countries:
                        d.filters.countries.remove(val)
                    else:
                        d.filters.countries.add(val)
                ui.screen = "aw_f_countries"
                ui.awaiting = None

            elif cmd == "cpn":
                # toggle coupons per year
                try:
                    n = int(arg)
                except Exception:
                    n = None
                if n is not None:
                    d.filters.coupons_per_year = set(d.filters.coupons_per_year or set())
                    if n in d.filters.coupons_per_year:
                        d.filters.coupons_per_year.remove(n)
                    else:
                        d.filters.coupons_per_year.add(n)
                ui.screen = "aw_f_coupons"
                ui.awaiting = None

            elif cmd == "rtf":
                # фильтр рейтингов: toggle
                if arg:
                    d.filters.ratings = set(d.filters.ratings or set())
                    if arg in d.filters.ratings:
                        d.filters.ratings.remove(arg)
                    else:
                        d.filters.ratings.add(arg)
                ui.screen = "aw_f_ratings"
                ui.awaiting = None

            elif cmd == "fr":
                # ввод диапазона фильтра (используем тот же parse_range что и в скринере)
                if arg:
                    ui.screen = "aw_filter_range"
                    ui.awaiting = f"aw_filter_range:{arg}"
                else:
                    ui.screen = "aw_step2"
                    ui.awaiting = None
            elif cmd == "filters_clear":
                d.filters = FilterState()
                d.exclude_papers = []  # важно: тоже чистим
                ui.screen = "aw_step2"
                ui.awaiting = None
            elif cmd == "excl":
                if arg == "start":
                    ui.screen = "aw_excl"
                    ui.awaiting = "aw_excl"
                elif arg == "clear":
                    d.exclude_papers = []
                    ui.screen = "aw_step2"
                    ui.awaiting = None
                else:
                    ui.screen = "aw_step2"
                    ui.awaiting = None

            elif cmd == "toggle" and arg == "has_offer":
                # None -> True -> False -> None
                cur = d.filters.has_offer
                if cur is None:
                    d.filters.has_offer = False  # excluded first
                elif cur is False:
                    d.filters.has_offer = True  # then included
                else:
                    d.filters.has_offer = None

                ui.screen = "aw_step2"
                ui.awaiting = None

            elif cmd == "ratings":
                # совместимость со старым мастером
                ui.screen = "aw_f_ratings"
                ui.awaiting = None

            elif cmd == "ctype":
                # Шаг 3: тип условий
                if arg == "simple":
                    d.cond_kind = "simple"
                    d.conditions = []
                    ui.screen = "aw_simple_param"
                else:
                    d.cond_kind = "adv"
                    d.conditions = []
                    ui.screen = "aw_adv_logic"
                ui.awaiting = None

            elif cmd == "logic":
                # adv: И/ИЛИ
                d.logic = "and" if arg == "and" else "or"
                ui.screen = "aw_adv_param"
                ui.awaiting = None

            elif cmd == "addcond":
                ui.screen = "aw_adv_param"
                ui.awaiting = None
            elif cmd == "range":
                # выбор параметра условия
                if arg == "cr":
                    # ✅ всегда начинаем выбор рейтингов с нуля (не подтягиваем из предыдущих CR-условий)
                    d.cr_pick = set()
                    ui.screen = "aw_cond_cr"
                    ui.awaiting = "aw_cond_cr"

                elif arg:
                    ui.screen = "aw_cond_range"
                    ui.awaiting = f"aw_cond_range:{arg}"
                else:
                    ui.screen = "aw_step3"
                    ui.awaiting = None
            elif cmd == "cr_t":
                bucket = (arg or "").strip().upper()
                if bucket in RATING_ORDER:
                    d.cr_pick = set(d.cr_pick or set())
                    if bucket in d.cr_pick:
                        d.cr_pick.remove(bucket)
                    else:
                        d.cr_pick.add(bucket)
                ui.screen = "aw_cond_cr"
                ui.awaiting = "aw_cond_cr"

            elif cmd == "cr_clear":
                d.cr_pick = set()
                ui.screen = "aw_cond_cr"
                ui.awaiting = "aw_cond_cr"

            elif cmd == "cr_done":
                picked = _ratings_sorted_desc(list(d.cr_pick or set()))
                if not picked:
                    ui.screen = "aw_cond_cr"
                    ui.awaiting = "aw_cond_cr"
                else:
                    cond = AlertCond(key="cr", rng=None, vals=picked)
                    d.cr_pick = set()

                    d.conditions.append(cond)
                    cid = len(d.conditions) - 1

                    d.expr_tokens = list(getattr(d, "expr_tokens", []) or [])
                    if getattr(d, "expr_cursor", None) is None:
                        d.expr_cursor = len(d.expr_tokens)

                    cur = max(0, min(int(d.expr_cursor or 0), len(d.expr_tokens)))
                    d.expr_tokens.insert(cur, f"C{cid}")
                    d.expr_cursor = cur + 1

                    ui.screen = "aw_step3"
                    ui.awaiting = None

            elif cmd == "expr_clear":
                d.expr_tokens = []
                d.conditions = []
                ui.screen = "aw_step3"
                ui.awaiting = None

            elif cmd == "expr_open":
                # можно открыть только когда ждём условие
                if _awz_expr_expect_cond(d.expr_tokens):
                    d.expr_tokens.append("(")
                ui.screen = "aw_step3"
                ui.awaiting = None

            elif cmd == "expr_close":
                # можно закрыть только после условия/")" и если есть что закрывать
                if (not _awz_expr_expect_cond(d.expr_tokens)) and _awz_expr_open_cnt(d.expr_tokens) > 0:
                    d.expr_tokens.append(")")
                ui.screen = "aw_step3"
                ui.awaiting = None

            elif cmd == "expr_add":
                # добавить первое/следующее условие без оператора (только когда ждём условие)
                if _awz_expr_expect_cond(d.expr_tokens):
                    ui.screen = "aw_simple_param"
                    ui.awaiting = None
                else:
                    ui.screen = "aw_step3"
                    ui.awaiting = None

            elif cmd == "expr_and":
                if not _awz_expr_expect_cond(d.expr_tokens):
                    d.expr_tokens.append("AND")
                    ui.screen = "aw_simple_param"
                else:
                    ui.screen = "aw_step3"
                ui.awaiting = None

            elif cmd == "expr_or":
                if not _awz_expr_expect_cond(d.expr_tokens):
                    d.expr_tokens.append("OR")
                    ui.screen = "aw_simple_param"
                else:
                    ui.screen = "aw_step3"
                ui.awaiting = None

            elif cmd == "expr_done":
                # валидируем: должны закончить на условии/")" и скобки должны быть закрыты
                if _awz_expr_expect_cond(d.expr_tokens) or _awz_expr_open_cnt(d.expr_tokens) != 0 or not d.conditions:
                    ui.screen = "aw_step3"
                    ui.awaiting = None
                else:
                    ui.screen = "aw_step4"
                    ui.awaiting = None

            elif cmd == "trig":
                # Шаг 4: тип срабатывания
                if arg in ("once", "repeat5"):
                    d.trigger_kind = arg
                    d.digest_kind = ""
                    d.digest_weekday = None
                    d.digest_time = None
                    ui.screen = "aw_step5"
                    ui.awaiting = None
                elif arg == "digest":
                    ui.screen = "aw_digest_kind"
                    ui.awaiting = None
                else:
                    ui.screen = "aw_step4"
                    ui.awaiting = None

            elif cmd == "digest_kind":
                ui.screen = "aw_digest_kind"
                ui.awaiting = None

            elif cmd == "digest":
                if arg == "day":
                    d.trigger_kind = "digest_day"
                    d.digest_weekday = None
                    ui.screen = "aw_time_pick"
                else:
                    d.trigger_kind = "digest_week"
                    ui.screen = "aw_weekday"
                ui.awaiting = None

            elif cmd == "wd":
                try:
                    d.digest_weekday = int(arg)
                except Exception:
                    d.digest_weekday = 0
                ui.screen = "aw_time_pick"
                ui.awaiting = None

            elif cmd == "time":
                if arg == "other":
                    ui.screen = "aw_time_input"
                    ui.awaiting = "aw_time_input"
                else:
                    d.digest_time = arg
                    ui.screen = "aw_step5"
                    ui.awaiting = None

            elif cmd == "time_back":
                if d.trigger_kind == "digest_week":
                    ui.screen = "aw_weekday"
                else:
                    ui.screen = "aw_digest_kind"
                ui.awaiting = None

            elif cmd == "name":
                if arg == "enter":
                    ui.screen = "aw_name_input"
                    ui.awaiting = "aw_name_input"
                else:
                    d.name = ""
                    ui.screen = "aw_summary"
                    ui.awaiting = None
            elif cmd == "save":
                if d.name is None:
                    # имя ещё не задано: вернуть на шаг 5
                    ui.screen = "aw_step5"
                    ui.awaiting = None
                else:
                    rules = _alerts_rules(uid)

                    # либо создаём новый, либо обновляем существующий
                    if ui.wizard_edit_idx is not None and rules:
                        idx = min(max(ui.wizard_edit_idx, 0), len(rules) - 1)
                        r = rules[idx]
                        alert_id = getattr(r, "alert_id", None) or (idx + 1)
                        _awz_write_to_rule(uid, d, r)
                        _reset_runtime_state(int(alert_id))
                        ui.selected_idx = idx
                        await _alerts_primary_check_after_save(uid, r, _msk_now())

                    else:
                        r = AlertRule(alert_id=_alerts_next_id(uid), label="")
                        _awz_write_to_rule(uid, d, r)
                        rules.append(r)
                        _reset_runtime_state(int(r.alert_id))
                        await _alerts_primary_check_after_save(uid, r, _msk_now())
                        ui.selected_idx = len(rules) - 1

                    ui.wizard_edit_idx = None
                    _awz_clear(uid)
                    ui.screen = "manage"
                    ui.awaiting = None

            elif cmd == "save_draft":
                # старый stub: просто покажем summary
                ui.screen = "aw_summary"
                ui.awaiting = None
            elif cmd == "pcat":
                ui.screen = "aw_param_group"
                ui.awaiting = f"aw_param_group:{arg}"

            elif cmd == "param_root":
                # back from category list -> parameter picker
                ui.screen = "aw_adv_param"
                ui.awaiting = None

            elif cmd == "pback":
                # back from parameter picker -> expression editor (Step 3)
                ui.screen = "aw_step3"
                ui.awaiting = None

            else:
                # неизвестная команда wizard — на всякий случай в root
                ui.screen = "root"
                ui.awaiting = None

    except Exception as e:
        print(f"[{_ts()}] ⚠️ alerts cb error: {e}")

    # обновляем UI (всё внутри одного сообщения)
    try:
        text_out, kb_out = _alerts_apply_screen(uid)
        await _alerts_edit(uid, text_out, kb_out)
    except Exception as e:
        print(f"[{_ts()}] ⚠️ alerts edit error: {e}")

    try:
        await q.answer()
    except Exception:
        pass

@dp.callback_query(F.data.startswith("adg:"))
async def on_alerts_digest_nav(q: CallbackQuery):
    uid = q.from_user.id
    if not q.message:
        try:
            await q.answer()
        except Exception:
            pass
        return

    msg_id = q.message.message_id
    st = _ALERTS_DIGEST_VIEW.get(uid, {}).get(msg_id)
    action = (q.data.split(":", 1)[1] or "").strip()

    if not st or not st.get("pages"):
        try:
            await q.answer("This digest is no longer available.")
        except Exception:
            pass
        return

    pages: List[str] = st["pages"]
    idx = int(st.get("page_idx") or 0)
    total = len(pages)

    if action == "close":
        try:
            await bot.delete_message(chat_id=q.message.chat.id, message_id=msg_id)
        except Exception:
            pass
        _ALERTS_DIGEST_VIEW[uid].pop(msg_id, None)
        try:
            await q.answer()
        except Exception:
            pass
        return

    if action == "first":
        idx = 0
    elif action == "prev":
        idx = max(0, idx - 1)
    elif action == "next":
        idx = min(total - 1, idx + 1)
    elif action == "last":
        idx = total - 1

    st["page_idx"] = idx
    kb = _alerts_digest_nav_kb(idx, total)

    try:
        await bot.edit_message_text(
            chat_id=q.message.chat.id,
            message_id=msg_id,
            text=pages[idx],
            reply_markup=kb,
            parse_mode="HTML",
            disable_web_page_preview=True
        )
    except TelegramBadRequest:
        pass
    except Exception:
        pass

    try:
        await q.answer()
    except Exception:
        pass

@dp.message(Command("screener", "sc"))
async def on_screener(m: Message):
    st = USER_STATES.setdefault(m.from_user.id, ScreenerState())

    # МЯГКАЯ ОСТАНОВКА предыдущего скринера: убираем клавиатуру и гасим таймер (сообщение не удаляем)
    if getattr(st, "active", False) and getattr(st, "chat_id", None) and getattr(st, "message_id", None):
        try:
            await bot.edit_message_reply_markup(
                chat_id=st.chat_id,
                message_id=st.message_id,
                reply_markup=None
            )
        except TelegramBadRequest:
            pass
        _cancel_idle_timer(st)
        st.message_id = None

    # Запуск нового скринера
    st.active = True
    st.chat_id = m.chat.id
    st.message_id = None
    st.awaiting_input = None
    st.prompt_msg_id = None
    st.ui_mode = "main"
    _cancel_idle_timer(st)  # сбрасываем возможные «хвосты»
    st.page_idx = 0
    st.tx_rows_cap = 40
    st.scope_uids = None
    await safe_send_initial(m, st)

@dp.message(F.text, ~F.text.startswith("/"), _awaiting_list_edit)
async def on_list_edit_input(m: Message):
    # отмена операции списков
    txt = (m.text or "").strip()
    if txt.lower() == "/cancel":
        if st and st.prompt_msg_id:
            try:
                await bot.delete_message(chat_id=m.chat.id, message_id=st.prompt_msg_id)
            except TelegramBadRequest:
                pass
            st.prompt_msg_id = None
        st.awaiting_input = None
        # удаляем и сообщение пользователя с /cancel
        try:
            await bot.delete_message(m.chat.id, m.message_id)
        except Exception:
            pass
        return

    st = USER_STATES.setdefault(m.from_user.id, ScreenerState())
    which, mode = st.awaiting_input.split(":")
    which_set = st.watchlist if which == "wl" else st.blacklist

    tickers = [t.strip().upper() for t in re.split(r"[,\s]+", m.text.strip()) if t.strip()]
    if mode == "add":
        which_set.update(tickers)
    else:
        for t in tickers:
            which_set.discard(t)

    # очистка подсказок
    try:
        if st.prompt_msg_id:
            await bot.delete_message(m.chat.id, st.prompt_msg_id)
        await bot.delete_message(m.chat.id, m.message_id)
    except Exception:
        pass
    st.prompt_msg_id = None
    st.awaiting_input = None

    # точечная перерисовка WL/BL после изменения списка
    if which == "wl":
        txt, total = _format_ticker_list_grouped("👀 Watchlist", st.watchlist, st.wl_page_idx, 23)
        st.wl_pages_count = total
        if st.wl_view_msg_id:
            try:
                await bot.edit_message_text(
                    chat_id=m.chat.id,
                    message_id=st.wl_view_msg_id,
                    text=txt,
                    reply_markup=list_edit_keyboard("wl", st),
                    link_preview_options=LP_DISABLED
                )
            except TelegramBadRequest:
                pass
    else:
        txt, total = _format_ticker_list_grouped("⛔ Blacklist", st.blacklist, st.bl_page_idx, 23)
        st.bl_pages_count = total
        if st.bl_view_msg_id:
            try:
                await bot.edit_message_text(
                    chat_id=m.chat.id,
                    message_id=st.bl_view_msg_id,
                    text=txt,
                    reply_markup=list_edit_keyboard("bl", st),
                    link_preview_options=LP_DISABLED
                )
            except TelegramBadRequest:
                pass

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

@dp.callback_query(F.data == "noop")
async def on_noop(q: CallbackQuery):
    await q.answer()

@dp.callback_query(F.data.in_({"wl:add","wl:del","bl:add","bl:del"}))
async def on_list_edit_start(q: CallbackQuery, bot: Bot):
    st = USER_STATES.get(q.from_user.id) or ScreenerState()
    st.awaiting_input = q.data
    if q.data.startswith("wl:"):
        st.wl_view_msg_id = q.message.message_id
    else:
        st.bl_view_msg_id = q.message.message_id
    prompt = await q.message.answer("Send ticker(s).\n"
                                    "Separators: space/comma.\n"
                                    "To cancel — /cancel")
    st.prompt_msg_id = prompt.message_id
    await q.answer()

@dp.callback_query(F.data.startswith("listnav:"))
async def on_list_nav(q: CallbackQuery, bot: Bot):
    st = USER_STATES.setdefault(q.from_user.id, ScreenerState())
    try:
        _, which, action = q.data.split(":")
    except Exception:
        await q.answer(); return

    # выберем нужные поля состояния
    if which == "wl":
        page_idx = st.wl_page_idx
        ticks = st.watchlist
        msg_id = st.wl_view_msg_id or q.message.message_id
    else:
        page_idx = st.bl_page_idx
        ticks = st.blacklist
        msg_id = st.bl_view_msg_id or q.message.message_id

    if action == "first": page_idx = 0
    elif action == "prev": page_idx = max(0, page_idx - 1)
    elif action == "next": page_idx = page_idx + 1
    elif action == "last": page_idx = 10**9
    elif action == "close":
        # удаляем сообщение списка
        try:
            if which == "wl" and st.wl_view_msg_id:
                await bot.delete_message(q.message.chat.id, st.wl_view_msg_id)
            if which == "bl" and st.bl_view_msg_id:
                await bot.delete_message(q.message.chat.id, st.bl_view_msg_id)
        except Exception:
            pass
        # удаляем командное сообщение (/wl или /bl)
        try:
            if which == "wl" and st.wl_cmd_msg_id:
                await bot.delete_message(q.message.chat.id, st.wl_cmd_msg_id)
            if which == "bl" and st.bl_cmd_msg_id:
                await bot.delete_message(q.message.chat.id, st.bl_cmd_msg_id)
        except Exception:
            pass
        # чистим стейт
        if which == "wl":
            st.wl_view_msg_id = None
            st.wl_cmd_msg_id = None
            st.wl_pages_count = 1
            st.wl_page_idx = 0
        else:
            st.bl_view_msg_id = None
            st.bl_cmd_msg_id = None
            st.bl_pages_count = 1
            st.bl_page_idx = 0
        await q.answer()
        return
    else:
        await q.answer(); return

    title = "👀 Watchlist" if which == "wl" else "⛔ Blacklist"
    txt, total = _format_ticker_list_grouped(title, ticks, page_idx, 23)
    # зафиксируем индекс и число страниц
    if which == "wl":
        st.wl_pages_count = total
        st.wl_page_idx = min(page_idx, total - 1)
        msg_id = st.wl_view_msg_id or msg_id
    else:
        st.bl_pages_count = total
        st.bl_page_idx = min(page_idx, total - 1)
        msg_id = st.bl_view_msg_id or msg_id

    try:
        await bot.edit_message_text(
            chat_id=q.message.chat.id,
            message_id=msg_id,
            text=txt,
            reply_markup=list_edit_keyboard(which, st),
            link_preview_options=LP_DISABLED
        )
    except TelegramBadRequest:
        # если исходное сообщение потеряли — отправим новое
        msg = await q.message.answer(txt, reply_markup=list_edit_keyboard(which, st), link_preview_options=LP_DISABLED)
        if which == "wl": st.wl_view_msg_id = msg.message_id
        else:             st.bl_view_msg_id = msg.message_id

    await q.answer()

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
        "cr": ("Credit rating", ""),
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
        f"Enter the range for <b>{html.escape(title)}</b>\n"
        f"Format: <code>min-max</code>, <code>&gt;=x</code>, <code>&lt;=y</code>, <code>&gt;x</code>, <code>&lt;y</code>\n"
        f"Examples: <code>{examples}</code>, <code>{ge}</code>, <code>{le}</code>\n"
        f"Cancel: /cancel"
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
    if await _alerts_try_consume_text(m):
        return
    st = USER_STATES.setdefault(m.from_user.id, ScreenerState())
    if st.awaiting_input and st.awaiting_input.startswith("range:"):
        key = st.awaiting_input.split(":",1)[1]
        rng = parse_range(m.text)
        if rng is None:
            title, unit = pretty_key_and_unit(key)
            hint_raw = "Например: 10-15, >=12.3, <=9.5, >12.3, <9.5" if unit != "%" else "Например: 10-15%, >=12.3%, <=9.5%, >12.3%, <9.5%"
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
            n = int(m.text.strip())
            if n < 1: raise ValueError()
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
            reply = await m.reply("Введите целое >= 1.", link_preview_options=LP_DISABLED)
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
        msg = await q.message.answer("Введите новое значение Group size (>=1):", link_preview_options=LP_DISABLED)
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

async def change_watcher_loop():
    from time import monotonic
    while True:
        try:
            loop = asyncio.get_running_loop()
            _ = await loop.run_in_executor(None, DATA_CHANGE_QUEUE.get)

            # быстро дреним очередь (без sleep(1))
            t0 = monotonic()
            while (monotonic() - t0) < 0.25:
                try:
                    DATA_CHANGE_QUEUE.get_nowait()
                except Empty:
                    break
                await asyncio.sleep(0)

            # 1) скринер
            await rerender_all_active()

            # 2) алерты (если кто-то сейчас на экране алертов/визарда)
            # ✅ сразу проверяем алерты на новых данных
            await _alerts_process_bucket(datetime.now(MSK_TZ), force=True)

            for uid, ui in list(ALERT_UI.items()):
                if not ui or not ui.chat_id or not ui.message_id:
                    continue
                # обновляем только если пользователь действительно в alerts UI
                if getattr(ui, "busy_until", 0.0) > time.time():
                    continue
                if (ui.screen or "").startswith(("root", "manage", "aw_")):
                    txt, kb = _alerts_apply_screen(uid)
                    await _alerts_edit(uid, txt, kb)

        except Exception:
            await asyncio.sleep(0.5)

async def bot_main():
    asyncio.create_task(change_watcher_loop())
    asyncio.create_task(alerts_engine_loop())   # <-- ДОБАВИТЬ
    await dp.start_polling(bot, handle_signals=False)

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

# ====================== Hourly refresh using existing list_after_smartlab() ======================
from datetime import timezone, time as dtime

SMARTLAB_TZ = timezone(timedelta(hours=3))          # МСК без tzdata
SMARTLAB_SLEEP_START = dtime(0, 6)                 # 00:06 МСК
SMARTLAB_SLEEP_END = dtime(6, 6)                   # 06:06 МСК

SMARTLAB_LAST_PARSE_AT = None
SMARTLAB_LAST_PARSE_LOCK = threading.Lock()

def smartlab_mark_parsed(ts: datetime | None = None):
    """Фиксируем время последнего успешного парсинга smartlab (в МСК)."""
    global SMARTLAB_LAST_PARSE_AT
    if ts is None:
        ts = datetime.now(SMARTLAB_TZ)
    with SMARTLAB_LAST_PARSE_LOCK:
        SMARTLAB_LAST_PARSE_AT = ts

def smartlab_last_parsed() -> datetime | None:
    with SMARTLAB_LAST_PARSE_LOCK:
        return SMARTLAB_LAST_PARSE_AT

_SMARTLAB_REFRESH_THREAD = None

def _smartlab_hourly_refresh_loop(period_sec: int = 3600):
    import io, contextlib

    def _naive_time(dt: datetime) -> dtime:
        # dt aware -> сравниваем как naive time
        return dt.astimezone(SMARTLAB_TZ).time().replace(tzinfo=None)

    def _next_occurrence(now: datetime, target: dtime) -> datetime:
        base = now.astimezone(SMARTLAB_TZ)
        cand = base.replace(hour=target.hour, minute=target.minute, second=0, microsecond=0)
        if cand <= base:
            cand += timedelta(days=1)
        return cand

    def _in_quiet_window(now: datetime) -> bool:
        t = _naive_time(now)
        return SMARTLAB_SLEEP_START <= t < SMARTLAB_SLEEP_END

    just_woke = False
    paused_logged = False

    while True:
        now = datetime.now(SMARTLAB_TZ)

        # 1) Ночной период: НЕ парсим, просто спим до 06:06
        if _in_quiet_window(now):
            if not paused_logged:
                print(f"[{now:%Y-%m-%d %H:%M:%S}] 🌙 SmartLab: пауза до {SMARTLAB_SLEEP_END.strftime('%H:%M')} МСК")
                paused_logged = True
            wake_dt = _next_occurrence(now, SMARTLAB_SLEEP_END)
            time.sleep(max(1.0, (wake_dt - now).total_seconds()))
            just_woke = True
            continue

        paused_logged = False

        # 2) Сразу после 06:06 НЕ парсим "с ходу": ждём, пока engine_refresh_all отметит парсинг
        if just_woke:
            # ждём до 20 минут, чтобы engine_refresh_all успел сделать list_after_smartlab() и smartlab_mark_parsed()
            wake_anchor = now.replace(hour=SMARTLAB_SLEEP_END.hour, minute=SMARTLAB_SLEEP_END.minute, second=0, microsecond=0)
            deadline = datetime.now(SMARTLAB_TZ) + timedelta(minutes=20)
            while datetime.now(SMARTLAB_TZ) < deadline:
                lp = smartlab_last_parsed()
                if lp and lp >= (wake_anchor - timedelta(minutes=2)):
                    break
                time.sleep(20)
            just_woke = False
            # после ожидания просто пересчитываем next_due на следующей итерации
            continue

        # 3) Планируем следующий парсинг: строго через час после последнего
        now = datetime.now(SMARTLAB_TZ)
        last = smartlab_last_parsed()
        if last is None:
            # если почему-то метки нет — позволяем один парсинг "сейчас"
            next_due = now
        else:
            next_due = last + timedelta(seconds=period_sec)

        # если следующий due попадает после начала ночной паузы — спим до 00:06 и уйдём в quiet
        pause_start_dt = _next_occurrence(now, SMARTLAB_SLEEP_START)
        if now < pause_start_dt and next_due > pause_start_dt:
            time.sleep(max(1.0, (pause_start_dt - now).total_seconds()))
            continue

        # если ещё рано — ждём до next_due
        if next_due > now:
            time.sleep(max(1.0, (next_due - now).total_seconds()))
            continue

        # 4) Пора парсить smartlab
        try:
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                _, new_ratings, new_offers = list_after_smartlab()

            # отметка времени (ВАЖНО для "час от последнего")
            smartlab_mark_parsed(datetime.now(SMARTLAB_TZ))

            changed = False
            updated_ratings = 0
            updated_offers = 0
            uids = list(uid_bonds)[:]  # слепок текущего списка

            for uid in uids:
                nr = new_ratings.get(uid)
                if nr and credit_rating_bonds.get(uid) != nr:
                    credit_rating_bonds[uid] = nr
                    changed = True
                    updated_ratings += 1

                no = new_offers.get(uid)
                if offer_or_call_option_date_bonds.get(uid) != no:
                    if no is None:
                        offer_or_call_option_date_bonds.pop(uid, None)
                    else:
                        offer_or_call_option_date_bonds[uid] = no
                    changed = True
                    updated_offers += 1

            if changed:
                notify_data_changed("smartlab_hourly_refresh")
                print(f"[{datetime.now(SMARTLAB_TZ):%Y-%m-%d %H:%M:%S}] ✅ Кредитные рейтинги обновлены "
                      f"(рейтинги: {updated_ratings}, оферты: {updated_offers})")
            else:
                print(f"[{datetime.now(SMARTLAB_TZ):%Y-%m-%d %H:%M:%S}] ℹ️ Проверка кредитных рейтингов: изменений нет")

        except Exception as e:
            print(f"[{datetime.now(SMARTLAB_TZ):%Y-%m-%d %H:%M:%S}] ⚠️ Ошибка обновления кредитных рейтингов: {e}")

def start_smartlab_hourly_refresh(period_sec: int = 3600):
    global _SMARTLAB_REFRESH_THREAD
    if _SMARTLAB_REFRESH_THREAD and _SMARTLAB_REFRESH_THREAD.is_alive():
        return
    _SMARTLAB_REFRESH_THREAD = threading.Thread(
        target=_smartlab_hourly_refresh_loop,
        args=(period_sec,),
        daemon=True,
        name="smartlab_hourly_refresh",
    )
    _SMARTLAB_REFRESH_THREAD.start()

def confirming_list_for_analysis():
    print('Подтверждение списка облигаций…')
    dicts = [ticker_bonds, name_bonds, coupon_quantity_per_year_bonds, maturity_date_bonds, nominal_bonds, aci_bonds, country_of_risk_bonds, sector_bonds, issue_size_bonds, credit_rating_bonds, offer_or_call_option_date_bonds]
    for d in dicts:
        for key in list(d.keys()):
            if key not in uid_bonds:
                del d[key]
    return uid_bonds, ticker_bonds, name_bonds, coupon_quantity_per_year_bonds, maturity_date_bonds, nominal_bonds, aci_bonds, country_of_risk_bonds, sector_bonds, issue_size_bonds, credit_rating_bonds, offer_or_call_option_date_bonds

# fix_date по купонам: instrument_id -> {coupon_date: fix_date}
coupon_fix_dates_bonds = {}

# кеш "расчётного дня" (T+1 по торговому календарю)
_SETTLEMENT_DAY_CACHE = None
_SETTLEMENT_DAY_CACHE_FOR = None

def _pb_to_date(x):
    """Приводит protobuf Timestamp/Datetime к date."""
    if x is None:
        return None
    if isinstance(x, datetime):
        return x.date()
    try:
        return x.ToDatetime().date()  # protobuf Timestamp
    except Exception:
        pass
    try:
        return datetime.fromtimestamp(x.seconds).date()
    except Exception:
        return None

def _call_trading_schedules(client, exchange: str, from_dt: datetime, to_dt: datetime):
    """Совместимость: в разных версиях SDK метод может называться по-разному и иметь разные имена аргументов."""
    svc = client.instruments
    for name in ("trading_schedules", "get_trading_schedules"):
        m = getattr(svc, name, None)
        if not m:
            continue

        # пробуем самые частые сигнатуры
        attempts = [
            {"exchange": exchange, "from_": from_dt, "to": to_dt},
            {"exchange": exchange, "from": from_dt, "to": to_dt},
            {"exchange": exchange, "from_": from_dt, "to_": to_dt},
            {"exchange": exchange, "from": from_dt, "to_": to_dt},
        ]
        for kw in attempts:
            try:
                return m(**kw)
            except TypeError:
                continue

        # на крайний случай — позиционные
        return m(exchange, from_dt, to_dt)

    raise AttributeError("TradingSchedules method not found in tinkoff SDK")

def _next_trading_day_tinkoff(exchange: str = "MOEX", max_ahead_days: int = 90) -> datetime:
    """Возвращает ближайший следующий торговый день (>= завтра) по TradingSchedules."""
    today = date.today()
    start_from = today + timedelta(days=1)

    best = None
    step = 7  # на случай ограничений по диапазону — ходим окнами

    with Client(token) as client:
        for offset in range(0, max_ahead_days, step):
            # ВАЖНО: TradingSchedules ругается, если from_dt "в прошлом" относительно момента запроса.
            # Поэтому начинаем окна строго с "завтра", а не с "сегодня 00:00".
            from_dt = datetime.combine(start_from + timedelta(days=offset), datetime.min.time())
            to_dt = from_dt + timedelta(days=step + 1)

            resp = _call_trading_schedules(client, exchange, from_dt, to_dt)

            schedules = getattr(resp, "exchanges", None) or getattr(resp, "trading_schedules", None) or []
            for sch in schedules:
                for d in getattr(sch, "days", []):
                    dd = _pb_to_date(getattr(d, "date", None))
                    if not dd:
                        continue
                    if dd >= start_from and getattr(d, "is_trading_day", False):
                        if best is None or dd < best:
                            best = dd

            if best is not None:
                return datetime.combine(best, datetime.min.time())

    raise RuntimeError("Не найден следующий торговый день через TradingSchedules")


def get_settlement_day(force: bool = False) -> datetime:
    """
    'Расчётный день покупки сегодня' (T+1, но с учётом неторговых дней).
    Кешируем на сутки.
    """
    global _SETTLEMENT_DAY_CACHE, _SETTLEMENT_DAY_CACHE_FOR
    today = date.today()

    if force or _SETTLEMENT_DAY_CACHE is None or _SETTLEMENT_DAY_CACHE_FOR != today:
        try:
            _SETTLEMENT_DAY_CACHE = _next_trading_day_tinkoff(exchange="MOEX", max_ahead_days=90)
            _SETTLEMENT_DAY_CACHE_FOR = today
        except Exception as e:
            # Если force=True — пусть пробросится (setting_timeframes поймает и красиво залогирует).
            if force:
                raise

            # Иначе (когда нас вызывают глубоко в расчётах) — НЕ падаем, а используем фолбэк.
            print(f"[{_ts()}] ⚠️ Не удалось получить TradingSchedules ({e}). Фолбэк: пропускаю только выходные.")
            d = datetime.today() + timedelta(days=1)
            while d.weekday() >= 5:
                d += timedelta(days=1)
            _SETTLEMENT_DAY_CACHE = d
            _SETTLEMENT_DAY_CACHE_FOR = today

    return _SETTLEMENT_DAY_CACHE


def setting_timeframes():
    print('Установка временных интервалов…')
    try:
        d = get_settlement_day(force=True)
        print(f"[{_ts()}] ✅ Расчётный день T+1 (TradingSchedules): {d.date()}")
        return d
    except Exception as e:
        # ВАЖНО: сохраняем фолбэк в кеш, чтобы дальше (в расчётах доходности/цен)
        # get_settlement_day() не пытался снова дергать TradingSchedules и не падал.
        global _SETTLEMENT_DAY_CACHE, _SETTLEMENT_DAY_CACHE_FOR

        print(f"[{_ts()}] ⚠️ Не удалось получить TradingSchedules ({e}). Фолбэк: пропускаю только выходные.")
        d = datetime.today() + timedelta(days=1)
        while d.weekday() >= 5:
            d += timedelta(days=1)

        _SETTLEMENT_DAY_CACHE = d
        _SETTLEMENT_DAY_CACHE_FOR = date.today()
        return d

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
    global coupon_fix_dates_bonds
    coupon_fix_dates_bonds = {}
    coupons_to_maturity_bonds, weighted_coupons_to_maturity_bonds, coupons_to_offer_bonds, weighted_coupons_to_offer_bonds = {}, {}, {}, {}
    count = 0
    for instrument_id in tqdm(uid_bonds[:]):
        time.sleep(0.5)

        r = None
        last_err = None
        max_retries = 4

        for attempt in range(max_retries + 1):
            try:
                with Client(token) as client:
                    r = client.instruments.get_bond_coupons(
                        instrument_id=instrument_id,
                        from_=day,
                        to=datetime.today() + timedelta(weeks=49999)
                    )
                break

            except Exception as e:
                last_err = e
                msg = str(e)

                # retry только для явно сетевых/UNAVAILABLE ошибок
                retryable = (
                    "UNAVAILABLE" in msg
                    or "failed to connect" in msg
                    or "tcp handshaker shutdown" in msg
                    or "timeout" in msg.lower()
                )

                if retryable and attempt < max_retries:
                    sleep_s = min(60.0, 1.0 * (2 ** attempt))  # 1,2,4,8...
                    print(f"[{_ts()}] ⚠️ GetBondCoupons retry {attempt+1}/{max_retries} for {instrument_id}: {type(e).__name__}: {e} (sleep {sleep_s}s)")
                    time.sleep(sleep_s)
                    continue

                # не роняем процесс
                print(f"[{_ts()}] ❌ GetBondCoupons skip {instrument_id}: {type(e).__name__}: {e}")
                r = None
                break

        if r is None:
            # чтобы дальше код не ожидал купоны для этой бумаги
            count += 1
            try:
                uid_bonds.remove(instrument_id)
            except Exception:
                pass
            continue

        if len(r.events) != 0:
            coupons, weighted_coupons, coupons_to_offer, weighted_coupons_to_offer = {}, {}, {}, {}
            fix_dates = {}
            for event in r.events:
                cdt = event.coupon_date.date()
                fdt = _pb_to_date(getattr(event, "fix_date", None))
                fix_dates[cdt] = fdt
                coupon = event.pay_one_bond.units + (event.pay_one_bond.nano * 10 ** (-9))
                weighted_coupon = (event.pay_one_bond.units + (event.pay_one_bond.nano * 10 ** (-9))) * survival_on_date(credit_rating_bonds[instrument_id], event.coupon_date.date(), lambdas_by_rating)
                coupons[event.coupon_date.date()] = coupon
                weighted_coupons[event.coupon_date.date()] = weighted_coupon
                if instrument_id in offer_or_call_option_date_bonds.keys():
                    if event.coupon_date.date() <= offer_or_call_option_date_bonds[instrument_id]:
                        coupons_to_offer[event.coupon_date.date()] = coupon
                        weighted_coupons_to_offer[event.coupon_date.date()] = weighted_coupon
            coupon_fix_dates_bonds[instrument_id] = fix_dates
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

def historic_volume_request(max_retries: int = 3):
    print('Запрос исторических данных по объёмам за сегодня…')

    # ВАЖНО: клиент создаём один раз, а не на каждую бумагу
    with Client(token) as client:
        for uid in tqdm(uid_bonds):
            ok = False
            last_err = None

            for attempt in range(max_retries + 1):
                try:
                    r = client.market_data.get_candles(
                        interval=CandleInterval.CANDLE_INTERVAL_DAY,
                        instrument_id=uid,
                        from_=datetime.today(),
                        to=datetime.today() + timedelta(days=1),
                    )

                    # Обычно на день приходит 1 свеча, но на всякий случай берём последнюю
                    if getattr(r, "candles", None):
                        volume_bonds[uid] = r.candles[-1].volume
                    ok = True
                    break

                except Exception as e:
                    last_err = e
                    # экспоненциальная пауза: 0.5, 1, 2, ...
                    if attempt < max_retries:
                        time.sleep(0.5 * (2 ** attempt))
                    else:
                        # Не роняем процесс — просто пропускаем uid
                        print(f"[{_ts()}] ⚠️ historic_volume_request: skip {uid} after retries: {type(e).__name__}: {e}")

            # небольшая пауза, чтобы не долбить API
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
        price = (last_price.price.units + (last_price.price.nano * 10 ** (-9))) / 100 * nominal_bonds.get(instrument_id, 0)

        if price != 0:
            last_price_bonds[instrument_id] = price

            profitability_and_duration_calculation(price, instrument_id)

        else:
            count += 1
            if instrument_id in uid_bonds:
                uid_bonds.remove(instrument_id)
    confirming_list_for_analysis()
    print('Из-за отсутсвия информации по последним ценам, было удалено', count, 'облигаций(и, я).')
    return last_price_bonds

# --- Health-check подписок ---
_SUBS_BY_QID: dict[int, set[str]] = {}  # по id(queue) -> множество instrument_uid

def _make_candles_subscribe_request(uids: list[str]) -> MarketDataRequest:
    """Единообразно строим запрос на подписку дневных свечей по списку UID."""
    return MarketDataRequest(
        subscribe_candles_request=SubscribeCandlesRequest(
            subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
            waiting_close=False,
            instruments=[
                CandleInstrument(
                    instrument_id=uid,
                    interval=SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_DAY,
                )
                for uid in uids
            ],
        )
    )

async def volumes_and_last_prices_stream(uids_chunk: list[str], q: asyncio.Queue, name: str):
    """
    Один gRPC-стрим на батч uid'ов. Использует ПЕРЕДАННУЮ очередь q (ту же,
    что и guard), чтобы принимать управляющие команды (get_my_subscriptions и т.п.).
    """
    backoff = 1.0
    while True:
        last_seen = time.monotonic()
        _SUBS_BY_QID[id(q)] = set()

        async def request_iterator():
            # первичная подписка на весь батч
            print(f"[{_ts()}] [INIT] {name}: подписка на {len(uids_chunk)}")
            yield _make_candles_subscribe_request(uids_chunk)
            # управленческие команды от guard'а
            while True:
                try:
                    req = await asyncio.wait_for(q.get(), timeout=5)
                    yield req
                except asyncio.TimeoutError:
                    # просто держим генератор «живым»
                    pass

        try:
            async with AsyncClient(token) as client:
                stream = client.market_data_stream.market_data_stream(request_iterator())
                async for r in stream:
                    # --- HEARTBEAT / Ping ---
                    if getattr(r, "ping", None):
                        last_seen = time.monotonic()

                    # --- ответы о подписках (init + get_my_subscriptions) ---
                    if getattr(r, "subscribe_candles_response", None):
                        subs = {
                            s.instrument_uid
                            for s in r.subscribe_candles_response.candles_subscriptions
                            if s.subscription_status == SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS
                               and s.interval == SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_DAY
                        }
                        _SUBS_BY_QID[id(q)] = subs
                        print(f"[{_ts()}] [STATE] {name}: активных подписок: {len(subs)}")
                        last_seen = time.monotonic()

                    if getattr(r, "get_my_subscriptions_response", None):
                        subs = {
                            s.instrument_uid
                            for s in r.get_my_subscriptions_response.candles_subscriptions
                            if s.subscription_status == SubscriptionStatus.SUBSCRIPTION_STATUS_SUCCESS
                               and s.interval == SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_DAY
                        }
                        _SUBS_BY_QID[id(q)] = subs
                        print(f"[{_ts()}] [STATE] {name}: активных подписок: {len(subs)}")
                        last_seen = time.monotonic()

                    # --- сами свечи ---
                    if getattr(r, "candle", None):
                        last_seen = time.monotonic()
                        instrument_id = r.candle.instrument_uid
                        volume_bonds[instrument_id] = r.candle.volume
                        if r.candle.close is not None and r.candle.close != 0:
                            price = (r.candle.close.units + (r.candle.close.nano * 1e-9)) / 100 * nominal_bonds.get(instrument_id, 0)
                            if price != last_price_bonds[instrument_id]:
                                last_price_bonds[instrument_id] = price
                                asyncio.create_task(asyncio.to_thread(
                                    profitability_and_duration_calculation, price, instrument_id
                                ))

                    # --- soft-watchdog: нет пингов/данных слишком долго — перезапустим стрим ---
                    if time.monotonic() - last_seen > HEARTBEAT_SEC:
                        print(f"[{_ts()}] [DOWN] {name}: нет данных/пингов {HEARTBEAT_SEC}s — перезапускаю стрим")
                        break

            # backoff & повтор
            delay = min(backoff, BACKOFF_MAX)
            print(f"[{_ts()}] [RECONNECT] {name}: retry через {delay:.1f}s")
            await asyncio.sleep(delay + random.random())
            backoff = min(backoff * 2, BACKOFF_MAX)

        except AioRpcError as e:
            code = getattr(e, "code", lambda: None)()
            details = getattr(e, "details", lambda: "")()
            print(f"[{_ts()}] [DOWN] {name}: {code} {details} — реконнект")
            delay = min(backoff, BACKOFF_MAX)
            await asyncio.sleep(delay + random.random())
            backoff = min(backoff * 2, BACKOFF_MAX)
        except Exception as e:
            print(f"[{_ts()}] [DOWN] {name}: Unexpected {e!r} — реконнект")
            await asyncio.sleep(3)
            backoff = min(backoff * 2, BACKOFF_MAX)
        else:
            backoff = 1.0

async def subscriptions_guard(workers: list[dict], period_sec: int = 300):
    while True:
        await asyncio.sleep(period_sec)

        # запросим состав подписок
        for w in workers:
            q = w["q"]
            _SUBS_BY_QID[id(q)] = set()
            # если у воркера нет очереди (стрим в реконнекте) — пропускаем
            if q is not None:
                await q.put(MarketDataRequest(get_my_subscriptions=GetMySubscriptions()))

        await asyncio.sleep(5)

        for w in workers:
            q = w["q"]
            if q is None:
                print(f"[{_ts()}] [WAIT] {w['name']}: стрим перезапускается — проверку подписок пропустил")
                continue
            expected = set(w["uids"])
            actual = _SUBS_BY_QID.get(id(q), set())
            missing = expected - actual
            if missing:
                print(f"[{_ts()}] [HEAL] {w['name']}: не хватает {len(missing)} — повторная подписка на батч")
                await q.put(_make_candles_subscribe_request(list(expected)))
            else:
                print(f"[{_ts()}] [OK] {w['name']}: подписки на месте ({len(actual)}/{len(expected)})")

def profitability_and_duration_calculation(price, instrument_id):
    pv = price + aci_bonds[instrument_id]
    dy1 = 0.01
    today_ = date.today()

    settlement_date = get_settlement_day().date()

    cm_dates = sorted(coupons_to_maturity_bonds[instrument_id].keys())
    cm_cfs = [coupons_to_maturity_bonds[instrument_id][d] for d in cm_dates]

    # гарантируем одинаковый порядок по тем же датам
    w_cm_dates = cm_dates
    w_cm_cfs = [weighted_coupons_to_maturity_bonds[instrument_id][d] for d in cm_dates]

    fd_map = coupon_fix_dates_bonds.get(instrument_id) or {}
    if cm_dates:
        fd0 = fd_map.get(cm_dates[0])
        if fd0 and fd0 < settlement_date:
            cm_dates = cm_dates[1:]
            cm_cfs = cm_cfs[1:]
            w_cm_dates = w_cm_dates[1:]
            w_cm_cfs = w_cm_cfs[1:]

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
        [today_, *cm_dates, maturity_date_bonds[instrument_id]],
        [-pv, *cm_cfs, nominal_bonds[instrument_id]]
    )

    weighted_YTM_xirr = px.xirr(
        [today_, *w_cm_dates, maturity_date_bonds[instrument_id]],
        [-pv, *w_cm_cfs,
         nominal_bonds[instrument_id] * survival_on_date(credit_rating_bonds[instrument_id],
                                                         maturity_date_bonds[instrument_id], lambdas_by_rating)]
    )

    dates_m = [*cm_dates, maturity_date_bonds[instrument_id]]
    cfs_m = [*cm_cfs, nominal_bonds[instrument_id]]
    mac_ytm, dmod_ytm, cmod_ytm = mac_dur_conv(dates_m, cfs_m, YTM_xirr, pv)

    w_dates_m = [*w_cm_dates, maturity_date_bonds[instrument_id]]
    w_cfs_m = [*w_cm_cfs, nominal_bonds[instrument_id] * survival_on_date(credit_rating_bonds[instrument_id], maturity_date_bonds[instrument_id], lambdas_by_rating)]
    w_mac_ytm, w_dmod_ytm, w_cmod_ytm = mac_dur_conv(w_dates_m, w_cfs_m, YTM_xirr, pv)

    if instrument_id in offer_or_call_option_date_bonds.keys():

        co_dates = sorted(coupons_to_offer_bonds[instrument_id].keys())
        co_cfs = [coupons_to_offer_bonds[instrument_id][d] for d in co_dates]

        w_co_dates = co_dates
        w_co_cfs = [weighted_coupons_to_offer_bonds[instrument_id][d] for d in co_dates]

        fd_map = coupon_fix_dates_bonds.get(instrument_id) or {}
        if co_dates:
            fd0 = fd_map.get(co_dates[0])
            if fd0 and fd0 < settlement_date:
                co_dates = co_dates[1:]
                co_cfs = co_cfs[1:]
                w_co_dates = w_co_dates[1:]
                w_co_cfs = w_co_cfs[1:]

        YTC_xirr = px.xirr(
            [today_, *co_dates,
             offer_or_call_option_date_bonds[instrument_id]],
            [-pv, *co_cfs,
             nominal_bonds[instrument_id]]
        )

        weighted_YTC_xirr = px.xirr(
            [today_, *w_co_dates,
             offer_or_call_option_date_bonds[instrument_id]],
            [-pv, *w_co_cfs,
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
smartlab_mark_parsed()
uid_bonds, ticker_bonds, name_bonds, coupon_quantity_per_year_bonds, maturity_date_bonds, nominal_bonds, aci_bonds, country_of_risk_bonds, sector_bonds, issue_size_bonds, credit_rating_bonds, offer_or_call_option_date_bonds = confirming_list_for_analysis()
start_smartlab_hourly_refresh(3600)
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

# ====================== ENGINE scheduler (streams pause) ======================

try:
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
except Exception:
    ZoneInfo = None
    ZoneInfoNotFoundError = Exception

try:
    MSK_TZ = ZoneInfo("Europe/Moscow") if ZoneInfo else timezone(timedelta(hours=3))
except ZoneInfoNotFoundError:
    MSK_TZ = timezone(timedelta(hours=3))
    print(f"[{_ts()}] ⚠️ tzdata не найден: использую фиксированный UTC+3 для МСК (ZoneInfo недоступен)")

ENGINE_STOP_TIME = dtime(0, 6)   # 00:06 МСК
ENGINE_START_TIME = dtime(6, 6)  # 06:06 МСК

_ENGINE_TASKS = []
_ENGINE_RUNNING = False
_LAST_SESSION_DATE = None  # дата «движкового дня» (стартует в 06:06)

def _msk_now() -> datetime:
    return datetime.now(MSK_TZ)

def _session_date(dt: datetime) -> date:
    # «движковый день» начинается в 06:06 МСК (а не в полночь)
    return dt.date() if dt.time() >= ENGINE_START_TIME else (dt.date() - timedelta(days=1))

def _seconds_until(target_t: dtime) -> float:
    now = _msk_now()
    target = now.replace(hour=target_t.hour, minute=target_t.minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()

def engine_refresh_all():
    """Утренний перезапуск движка: заново тянем данные и ОБНОВЛЯЕМ глобальные словари (in-place)."""
    global day, lambdas_by_rating

    print(f"[{_ts()}] 🔄 ENGINE: утреннее обновление данных (API/парсинг/кэшфлоу)…")

    # 1) обновляем базовые справочники (in-place)
    new_ticker, new_name, new_cpy, new_mat, new_nom, new_aci, new_country, new_sector, new_issue, new_issue_money = initial_list()

    ticker_bonds.clear(); ticker_bonds.update(new_ticker)
    name_bonds.clear(); name_bonds.update(new_name)
    coupon_quantity_per_year_bonds.clear(); coupon_quantity_per_year_bonds.update(new_cpy)
    maturity_date_bonds.clear(); maturity_date_bonds.update(new_mat)
    nominal_bonds.clear(); nominal_bonds.update(new_nom)
    aci_bonds.clear(); aci_bonds.update(new_aci)
    country_of_risk_bonds.clear(); country_of_risk_bonds.update(new_country)
    sector_bonds.clear(); sector_bonds.update(new_sector)
    issue_size_bonds.clear(); issue_size_bonds.update(new_issue)
    issue_size_money_bonds.clear(); issue_size_money_bonds.update(new_issue_money)

    # 2) smartlab (uids/рейтинги/оферты) — тоже обновляем in-place
    new_uids, new_ratings, new_offers = list_after_smartlab()
    smartlab_mark_parsed()
    uid_bonds[:] = new_uids

    credit_rating_bonds.clear(); credit_rating_bonds.update(new_ratings)
    offer_or_call_option_date_bonds.clear(); offer_or_call_option_date_bonds.update(new_offers)

    confirming_list_for_analysis()

    # smartlab hourly thread уже должен быть запущен; вызов безопасен (не создаст второй поток)
    start_smartlab_hourly_refresh(3600)

    # 3) таймфреймы/лямбды
    day = setting_timeframes()
    lambdas_by_rating = building_lambdas()

    # 4) кэшфлоу купонов
    new_c2m, new_wc2m, new_c2o, new_wc2o = coupon_cashflows()
    coupons_to_maturity_bonds.clear(); coupons_to_maturity_bonds.update(new_c2m)
    weighted_coupons_to_maturity_bonds.clear(); weighted_coupons_to_maturity_bonds.update(new_wc2m)
    coupons_to_offer_bonds.clear(); coupons_to_offer_bonds.update(new_c2o)
    weighted_coupons_to_offer_bonds.clear(); weighted_coupons_to_offer_bonds.update(new_wc2o)

    # 5) сбрасываем вычисленные метрики (чтобы не висели по удалённым uid) — in-place
    for d in [
        YTM_xirr_bonds, weighted_YTM_xirr_bonds,
        YTC_xirr_bonds, weighted_YTC_xirr_bonds,
        YTW_xirr_bonds, weighted_YTW_xirr_bonds,
        days_YTW_bonds, days_weighted_YTW_bonds,

        macaulay_YTM_bonds, modified_YTM_bonds, convexity_YTM_bonds,
        macaulay_YTC_bonds, modified_YTC_bonds, convexity_YTC_bonds,
        macaulay_YTW_bonds, modified_YTW_bonds, convexity_YTW_bonds,

        weighted_macaulay_YTM_bonds, weighted_modified_YTM_bonds, weighted_convexity_YTM_bonds,
        weighted_macaulay_YTC_bonds, weighted_modified_YTC_bonds, weighted_convexity_YTC_bonds,
        weighted_macaulay_YTW_bonds, weighted_modified_YTW_bonds, weighted_convexity_YTW_bonds,

        dpp_1pct_up_YTM_bonds, dpp_1pct_down_YTM_bonds,
        dpp_1pct_up_YTC_bonds, dpp_1pct_down_YTC_bonds,
        dpp_1pct_up_YTW_bonds, dpp_1pct_down_YTW_bonds,

        weighted_dpp_1pct_up_YTM_bonds, weighted_dpp_1pct_down_YTM_bonds,
        weighted_dpp_1pct_up_YTC_bonds, weighted_dpp_1pct_down_YTC_bonds,
        weighted_dpp_1pct_up_YTW_bonds, weighted_dpp_1pct_down_YTW_bonds,
    ]:
        d.clear()

        # 6) объёмы/цены (in-place)
    volume_bonds.clear()
    try:
        historic_volume_request()  # заполняет volume_bonds
    except Exception as e:
        print(f"[{_ts()}] ⚠️ ENGINE: historic_volume_request failed: {type(e).__name__}: {e}")

    last_price_bonds.clear()
    last_price_bonds.update(historic_prices_request())  # пересчитает доходности внутри

    notify_data_changed("engine_refresh")
    print(f"[{_ts()}] ✅ ENGINE: обновление завершено. Облигаций: {len(uid_bonds)}")


async def _engine_start_streams():
    global _ENGINE_TASKS, _ENGINE_RUNNING
    if _ENGINE_RUNNING:
        return

    workers = []
    batch_size = 295
    for i in range(0, len(uid_bonds), batch_size):
        chunk = uid_bonds[i:i + batch_size]
        q: asyncio.Queue = asyncio.Queue()
        workers.append({"name": f"batch-{i // batch_size + 1}", "uids": chunk, "q": q})

    _ENGINE_TASKS = [
        asyncio.create_task(
            volumes_and_last_prices_stream(w["uids"], w["q"], w["name"]),
            name=f"stream:{w['name']}"
        )
        for w in workers
    ]
    _ENGINE_TASKS.append(
        asyncio.create_task(subscriptions_guard(workers, period_sec=300), name="subscriptions_guard")
    )

    _ENGINE_RUNNING = True
    print(f"[{_ts()}] 🟢 ENGINE: стримы запущены ({len(workers)} батчей, всего uid: {len(uid_bonds)})")


async def _engine_stop_streams():
    global _ENGINE_TASKS, _ENGINE_RUNNING
    if not _ENGINE_RUNNING:
        return
    print(f"[{_ts()}] 🛑 ENGINE: останавливаю стримы…")

    for t in _ENGINE_TASKS:
        t.cancel()
    await asyncio.gather(*_ENGINE_TASKS, return_exceptions=True)

    _ENGINE_TASKS = []
    _ENGINE_RUNNING = False
    print(f"[{_ts()}] ✅ ENGINE: стримы остановлены")


async def engine_supervisor():
    global _LAST_SESSION_DATE

    # На старте файла данные уже инициализированы твоим текущим блоком выше
    _LAST_SESSION_DATE = _session_date(_msk_now())

    while True:
        now = _msk_now()

        # ночной простой (00:06..06:06 МСК)
        if ENGINE_STOP_TIME <= now.time() < ENGINE_START_TIME:
            await _engine_stop_streams()
            await asyncio.sleep(_seconds_until(ENGINE_START_TIME))
            continue

        # активный период (06:06..00:06 МСК)
        session = _session_date(now)
        if session != _LAST_SESSION_DATE and now.time() >= ENGINE_START_TIME:
            try:
                await asyncio.to_thread(engine_refresh_all)
                _LAST_SESSION_DATE = session
            except Exception as e:
                print(f"[{_ts()}] ⚠️ ENGINE supervisor: refresh failed: {type(e).__name__}: {e}")
                # чтобы не крутиться в tight-loop и не спамить API/логи
                await asyncio.sleep(60)

        if not _ENGINE_RUNNING:
            await _engine_start_streams()

        # ждём до 00:06 и гасим стримы
        await asyncio.sleep(_seconds_until(ENGINE_STOP_TIME))
        await _engine_stop_streams()

asyncio.run(engine_supervisor())
