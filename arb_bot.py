"""
Arbitrage Bot: Polymarket <-> Opinion (LIVE)
Auto-discovers ALL markets on both platforms.
Uses Claude API for intelligent market matching.
Executes real trades with Telegram human-in-the-loop confirmation.
"""

import os
import json
import time
import uuid
import sqlite3
import asyncio
import logging
import hashlib
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, asdict, field
from typing import Optional, Tuple, Dict, Any, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, MarketOrderArgs, OrderType
from py_clob_client.order_builder.constants import BUY, SELL

from opinion_clob_sdk import Client as OpinionClient
from opinion_clob_sdk.chain.py_order_utils.model.order import PlaceOrderDataInput
from opinion_clob_sdk.chain.py_order_utils.model.sides import OrderSide
from opinion_clob_sdk.chain.py_order_utils.model.order_type import (
    LIMIT_ORDER,
    MARKET_ORDER,
)

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
log = logging.getLogger("arb")


# ═══════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════

DRY_RUN = os.getenv("DRY_RUN", "false").lower() in ("true", "1", "yes")

# Polymarket
PM_HOST = "https://clob.polymarket.com"
PM_GAMMA = "https://gamma-api.polymarket.com"
PM_CHAIN_ID = 137
PM_PRIVATE_KEY = os.getenv("PM_PRIVATE_KEY", "")
PM_FUNDER = os.getenv("PM_FUNDER", "")
PM_SIGNATURE_TYPE = int(os.getenv("PM_SIGNATURE_TYPE", "2"))

# Opinion
OP_HOST = "https://proxy.opinion.trade:8443"
OP_OPENAPI = f"{OP_HOST}/openapi"
OP_APIKEY = os.getenv("OPINION_APIKEY", "")
OP_PRIVATE_KEY = os.getenv("OPINION_PRIVATE_KEY", "")
OP_MULTI_SIG = os.getenv("OPINION_MULTI_SIG", "")
OP_CHAIN_ID = int(os.getenv("OPINION_CHAIN_ID", "56"))
OP_RPC = os.getenv("OPINION_RPC", "https://bsc-dataseed.binance.org")

# Telegram
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = int(os.getenv("TELEGRAM_ALLOWED_CHAT_ID", "0"))

# Claude API (for market matching)
CLAUDE_API_KEY = os.getenv("CLAUDE_API_KEY", "")

# Tuning
SCAN_INTERVAL = float(os.getenv("SCAN_INTERVAL_SEC", "10"))
MATCH_REFRESH_INTERVAL = int(os.getenv("MATCH_REFRESH_SEC", "600"))  # re-match every 10 min
MIN_EDGE = float(os.getenv("MIN_EDGE", "0.03"))
MAX_TRADE_USD = float(os.getenv("MAX_TRADE_USD", "50"))       # абсолютный потолок на сделку
MIN_TRADE_USD = float(os.getenv("MIN_TRADE_USD", "5"))        # минимальная сделка (не стоит ниже)
BASE_TRADE_USD = float(os.getenv("BASE_TRADE_USD", "10"))     # базовая ставка при MIN_EDGE
EDGE_SCALE_FACTOR = float(os.getenv("EDGE_SCALE_FACTOR", "3"))  # множитель масштабирования от edge
MAX_BALANCE_PCT = float(os.getenv("MAX_BALANCE_PCT", "0.15")) # макс % от баланса на одну сделку
MIN_LIQUIDITY = float(os.getenv("MIN_LIQUIDITY", "100"))  # min shares at BBO
DEDUP_TTL = float(os.getenv("DEDUP_TTL_SEC", "180"))
MAX_EVENT_DAYS = int(os.getenv("MAX_EVENT_DAYS", "3"))  # только события, завершающиеся в ближайшие N дней
PM_FEE = float(os.getenv("PM_FEE_RATE", "0.02"))
OP_FEE = float(os.getenv("OP_FEE_RATE", "0.01"))

DB_PATH = os.getenv("DB_PATH", "arb.sqlite3")
MATCHES_CACHE = os.getenv("MATCHES_CACHE", "matches_cache.json")

# ═══════════════════════════════════════════════════════════
# PROXY (для стран где Polymarket / Opinion заблокированы)
# ═══════════════════════════════════════════════════════════
# Поддерживает HTTP, HTTPS, SOCKS5 прокси
# Примеры:
#   PROXY_URL="http://user:pass@1.2.3.4:8080"
#   PROXY_URL="socks5://user:pass@1.2.3.4:1080"
#   PROXY_URL="http://1.2.3.4:3128"
#
# Если нужны разные прокси для разных сервисов:
#   PM_PROXY_URL  — только для Polymarket
#   OP_PROXY_URL  — только для Opinion
#   TG_PROXY_URL  — только для Telegram
# Если они не заданы, используется общий PROXY_URL.

PROXY_URL = os.getenv("PROXY_URL", "")
PM_PROXY_URL = os.getenv("PM_PROXY_URL", "") or PROXY_URL
OP_PROXY_URL = os.getenv("OP_PROXY_URL", "") or PROXY_URL
TG_PROXY_URL = os.getenv("TG_PROXY_URL", "") or PROXY_URL
CLAUDE_PROXY_URL = os.getenv("CLAUDE_PROXY_URL", "") or PROXY_URL


def _build_proxies(proxy_url: str) -> Optional[Dict[str, str]]:
    """Build proxies dict for requests from a single URL."""
    if not proxy_url:
        return None
    return {"http": proxy_url, "https": proxy_url}


def _make_session(proxy_url: str = "") -> requests.Session:
    """Create a requests.Session with optional proxy and retries."""
    s = requests.Session()
    proxies = _build_proxies(proxy_url)
    if proxies:
        s.proxies.update(proxies)
    retry = Retry(total=2, backoff_factor=0.5, status_forcelist=[502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


# Shared sessions per service (created once, reuse connections)
pm_session = _make_session(PM_PROXY_URL)
op_session = _make_session(OP_PROXY_URL)
claude_session = _make_session(CLAUDE_PROXY_URL)

if PROXY_URL:
    log.info("Global proxy: %s", PROXY_URL.split("@")[-1] if "@" in PROXY_URL else PROXY_URL)
if PM_PROXY_URL and PM_PROXY_URL != PROXY_URL:
    log.info("PM proxy: %s", PM_PROXY_URL.split("@")[-1] if "@" in PM_PROXY_URL else PM_PROXY_URL)
if OP_PROXY_URL and OP_PROXY_URL != PROXY_URL:
    log.info("OP proxy: %s", OP_PROXY_URL.split("@")[-1] if "@" in OP_PROXY_URL else OP_PROXY_URL)


# ═══════════════════════════════════════════════════════════
# DATA MODELS
# ═══════════════════════════════════════════════════════════

@dataclass
class MarketInfo:
    platform: str        # "PM" or "OP"
    market_id: str
    title: str
    yes_token: str
    no_token: str
    yes_price: float
    no_price: float
    volume_24h: float
    active: bool


@dataclass
class MatchedPair:
    name: str
    pm_market_id: str
    pm_title: str
    pm_yes: str
    pm_no: str
    op_market_id: str
    op_title: str
    op_yes: str
    op_no: str
    confidence: float    # 0-1 from Claude


@dataclass
class Opportunity:
    trade_id: str
    ts: int
    pair_name: str
    side: str            # YES / NO
    buy_venue: str       # POLYMARKET / OPINION
    sell_venue: str
    buy_token: str
    sell_token: str
    buy_price: float
    sell_price: float
    edge: float
    size: float
    buy_depth: float
    sell_depth: float
    pm_market_id: str
    op_market_id: int
    meta: Dict[str, Any] = field(default_factory=dict)


# ═══════════════════════════════════════════════════════════
# DATABASE
# ═══════════════════════════════════════════════════════════

def init_db():
    con = sqlite3.connect(DB_PATH)
    con.executescript("""
        CREATE TABLE IF NOT EXISTS trades (
            trade_id TEXT PRIMARY KEY,
            created_ts INTEGER, payload TEXT, status TEXT, result TEXT, updated_ts INTEGER
        );
        CREATE TABLE IF NOT EXISTS daily_stats (
            date TEXT PRIMARY KEY,
            profit REAL DEFAULT 0, fee REAL DEFAULT 0,
            executed INTEGER DEFAULT 0, skipped INTEGER DEFAULT 0, failed INTEGER DEFAULT 0
        );
    """)
    con.commit()
    con.close()


def db_save_trade(opp: Opportunity, status="PENDING"):
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "INSERT OR REPLACE INTO trades VALUES(?,?,?,?,?,?)",
        (opp.trade_id, opp.ts, json.dumps(asdict(opp)), status, None, int(time.time())),
    )
    con.commit()
    con.close()


def db_get_trade(tid: str) -> Optional[Opportunity]:
    con = sqlite3.connect(DB_PATH)
    row = con.execute("SELECT payload FROM trades WHERE trade_id=?", (tid,)).fetchone()
    con.close()
    return Opportunity(**json.loads(row[0])) if row else None


def db_update(tid: str, status: str, result: str = None):
    con = sqlite3.connect(DB_PATH)
    con.execute(
        "UPDATE trades SET status=?, result=?, updated_ts=? WHERE trade_id=?",
        (status, result, int(time.time()), tid),
    )
    con.commit()
    con.close()


def db_stats(profit=0, fee=0, executed=False, skipped=False, failed=False):
    today = time.strftime("%Y-%m-%d")
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        INSERT INTO daily_stats VALUES(?,?,?,?,?,?)
        ON CONFLICT(date) DO UPDATE SET
            profit=profit+?, fee=fee+?,
            executed=executed+?, skipped=skipped+?, failed=failed+?
    """, (today, profit, fee, int(executed), int(skipped), int(failed),
          profit, fee, int(executed), int(skipped), int(failed)))
    con.commit()
    con.close()


# ═══════════════════════════════════════════════════════════
# MARKET DISCOVERY: Polymarket
# ═══════════════════════════════════════════════════════════

def _is_ending_soon(date_str: Optional[str], max_days: int) -> bool:
    """Check if a date string is within max_days from now."""
    if not date_str:
        return False
    try:
        # Polymarket uses ISO format: "2026-03-04T00:00:00Z" or "2026-03-04"
        date_str = date_str.replace("Z", "+00:00")
        if "T" in date_str:
            dt = datetime.fromisoformat(date_str)
        else:
            dt = datetime.fromisoformat(date_str + "T23:59:59+00:00")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        deadline = now + timedelta(days=max_days)
        return now <= dt <= deadline
    except (ValueError, TypeError):
        return False


def fetch_pm_markets() -> List[MarketInfo]:
    """Fetch active events from Polymarket Gamma API, filtered to ending within MAX_EVENT_DAYS."""
    markets = []
    offset = 0
    limit = 100
    skipped = 0

    while True:
        try:
            r = pm_session.get(
                f"{PM_GAMMA}/events",
                params={
                    "active": "true", "closed": "false",
                    "limit": limit, "offset": offset,
                },
                timeout=15,
            )
            r.raise_for_status()
            events = r.json()
        except Exception as e:
            log.warning("PM events fetch error (offset=%d): %s", offset, e)
            break

        if not events:
            break

        for ev in events:
            for mkt in ev.get("markets", []):
                # --- End date filter ---
                end_date = mkt.get("endDateIso") or mkt.get("umaEndDateIso") or ev.get("endDate")
                if not _is_ending_soon(end_date, MAX_EVENT_DAYS):
                    skipped += 1
                    continue

                tokens = mkt.get("clobTokenIds", [])
                outcomes = mkt.get("outcomes", "[]")
                if isinstance(outcomes, str):
                    try:
                        outcomes = json.loads(outcomes)
                    except Exception:
                        outcomes = []
                prices = mkt.get("outcomePrices", "[]")
                if isinstance(prices, str):
                    try:
                        prices = json.loads(prices)
                    except Exception:
                        prices = []

                if len(tokens) < 2 or len(outcomes) < 2:
                    continue
                if not mkt.get("enableOrderBook", True):
                    continue

                yes_price = float(prices[0]) if prices else 0
                no_price = float(prices[1]) if len(prices) > 1 else 0

                markets.append(MarketInfo(
                    platform="PM",
                    market_id=mkt.get("conditionId", mkt.get("id", "")),
                    title=mkt.get("question", ev.get("title", "?")),
                    yes_token=tokens[0],
                    no_token=tokens[1],
                    yes_price=yes_price,
                    no_price=no_price,
                    volume_24h=float(mkt.get("volume24hr", 0)),
                    active=True,
                ))

        offset += limit
        if len(events) < limit:
            break
        time.sleep(0.3)

    log.info("Fetched %d PM markets ending within %d days (skipped %d)", len(markets), MAX_EVENT_DAYS, skipped)
    return markets


# ═══════════════════════════════════════════════════════════
# MARKET DISCOVERY: Opinion
# ═══════════════════════════════════════════════════════════

def fetch_op_markets() -> List[MarketInfo]:
    """Fetch active markets from Opinion OpenAPI, filtered to ending within MAX_EVENT_DAYS."""
    markets = []
    page = 1
    limit = 50
    skipped = 0

    while True:
        try:
            r = op_session.get(
                f"{OP_OPENAPI}/market",
                params={"status": "activated", "page": page, "limit": limit},
                headers={"apikey": OP_APIKEY},
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.warning("OP markets fetch error (page=%d): %s", page, e)
            break

        if data.get("code") != 0:
            log.warning("OP API error: %s", data.get("msg"))
            break

        result = data.get("result", {})
        items = result.get("list", [])

        if not items:
            break

        for mkt in items:
            # --- End date filter ---
            # Opinion может отдавать endDate как ISO строку или unix timestamp
            end_date = mkt.get("endDate") or mkt.get("endDateIso") or mkt.get("deadline")
            end_ok = False
            if end_date:
                if isinstance(end_date, (int, float)):
                    # Unix timestamp (seconds or milliseconds)
                    ts = end_date if end_date < 1e12 else end_date / 1000
                    end_ok = _is_ending_soon(
                        datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(), MAX_EVENT_DAYS
                    )
                else:
                    end_ok = _is_ending_soon(str(end_date), MAX_EVENT_DAYS)

            if not end_ok:
                skipped += 1
                continue

            yes_tok = mkt.get("yesTokenId", "")
            no_tok = mkt.get("noTokenId", "")
            if not yes_tok or not no_tok:
                continue

            markets.append(MarketInfo(
                platform="OP",
                market_id=str(mkt.get("marketId", "")),
                title=mkt.get("marketTitle", "?"),
                yes_token=yes_tok,
                no_token=no_tok,
                yes_price=float(mkt.get("yesPrice", 0) or 0),
                no_price=float(mkt.get("noPrice", 0) or 0),
                volume_24h=float(mkt.get("volume24h", 0) or 0),
                active=True,
            ))

        total = result.get("total", 0)
        if page * limit >= total:
            break
        page += 1
        time.sleep(0.2)

    log.info("Fetched %d OP markets ending within %d days (skipped %d)", len(markets), MAX_EVENT_DAYS, skipped)
    return markets


# ═══════════════════════════════════════════════════════════
# CLAUDE API: INTELLIGENT MARKET MATCHING
# ═══════════════════════════════════════════════════════════

def match_markets_with_claude(
    pm_markets: List[MarketInfo],
    op_markets: List[MarketInfo],
) -> List[MatchedPair]:
    """
    Send market titles to Claude API to find matching pairs
    between Polymarket and Opinion.
    """
    if not CLAUDE_API_KEY:
        log.error("CLAUDE_API_KEY not set — cannot match markets")
        return []

    # Prepare concise lists
    pm_list = [{"id": m.market_id, "t": m.title[:120]} for m in pm_markets]
    op_list = [{"id": m.market_id, "t": m.title[:120]} for m in op_markets]

    # Build index for quick lookup
    pm_idx = {m.market_id: m for m in pm_markets}
    op_idx = {m.market_id: m for m in op_markets}

    # Chunk if needed (Claude can handle a lot but let's be safe)
    # Send in batches of 200 PM x all OP
    all_pairs: List[MatchedPair] = []
    chunk_size = 200

    for i in range(0, len(pm_list), chunk_size):
        pm_chunk = pm_list[i:i + chunk_size]

        prompt = f"""You are a prediction market analyst. Match markets that refer to THE SAME event/question between Polymarket (PM) and Opinion (OP).

CRITICAL RULES:
- Only match if both markets ask essentially the same question with the same resolution criteria
- YES on PM must mean YES on OP (same direction)
- Ignore slight wording differences but ensure the core event and timeframe match
- Do NOT match if dates/thresholds differ (e.g. "BTC 100k by June" vs "BTC 100k by December")
- Return confidence 0.0-1.0 (only include matches with confidence >= 0.85)

PM markets:
{json.dumps(pm_chunk, ensure_ascii=False)}

OP markets:
{json.dumps(op_list, ensure_ascii=False)}

Return ONLY a JSON array of matches. No other text. Each match:
{{"pm_id": "...", "op_id": "...", "name": "short name", "confidence": 0.95}}

If no matches found, return: []"""

        try:
            resp = claude_session.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": CLAUDE_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 4096,
                    "messages": [{"role": "user", "content": prompt}],
                },
                timeout=60,
            )
            resp.raise_for_status()
            content = resp.json()["content"][0]["text"]

            # Parse JSON from response (handle markdown fences)
            content = content.strip()
            if content.startswith("```"):
                content = content.split("\n", 1)[1]
                content = content.rsplit("```", 1)[0]

            matches = json.loads(content)

            for m in matches:
                pm_id = str(m["pm_id"])
                op_id = str(m["op_id"])
                conf = float(m.get("confidence", 0))

                if conf < 0.85:
                    continue
                if pm_id not in pm_idx or op_id not in op_idx:
                    continue

                pm = pm_idx[pm_id]
                op = op_idx[op_id]

                all_pairs.append(MatchedPair(
                    name=m.get("name", pm.title[:50]),
                    pm_market_id=pm_id,
                    pm_title=pm.title,
                    pm_yes=pm.yes_token,
                    pm_no=pm.no_token,
                    op_market_id=op_id,
                    op_title=op.title,
                    op_yes=op.yes_token,
                    op_no=op.no_token,
                    confidence=conf,
                ))

            log.info("Claude matched %d pairs from chunk %d", len(matches), i // chunk_size)

        except Exception as e:
            log.error("Claude matching error: %s", e)

        time.sleep(1)  # rate limit

    log.info("Total matched pairs: %d", len(all_pairs))
    return all_pairs


def load_cached_matches() -> Tuple[List[MatchedPair], float]:
    """Load cached matches from disk."""
    try:
        with open(MATCHES_CACHE, "r") as f:
            data = json.load(f)
        ts = data.get("timestamp", 0)
        pairs = [MatchedPair(**p) for p in data.get("pairs", [])]
        return pairs, ts
    except (FileNotFoundError, json.JSONDecodeError):
        return [], 0


def save_cached_matches(pairs: List[MatchedPair]):
    with open(MATCHES_CACHE, "w") as f:
        json.dump({
            "timestamp": time.time(),
            "pairs": [asdict(p) for p in pairs],
        }, f, indent=2)


async def refresh_matches() -> List[MatchedPair]:
    """Fetch all markets from both platforms and match via Claude."""
    log.info("Refreshing market matches...")
    pm = await asyncio.to_thread(fetch_pm_markets)
    op = await asyncio.to_thread(fetch_op_markets)

    if not pm or not op:
        log.warning("No markets fetched (PM=%d, OP=%d)", len(pm), len(op))
        pairs, _ = load_cached_matches()
        return pairs

    pairs = await asyncio.to_thread(match_markets_with_claude, pm, op)

    if pairs:
        save_cached_matches(pairs)
    else:
        # Fall back to cache
        pairs, _ = load_cached_matches()

    return pairs


# ═══════════════════════════════════════════════════════════
# ORDERBOOK & BBO
# ═══════════════════════════════════════════════════════════

_pm_ro = ClobClient(PM_HOST)
# Patch the internal session of py-clob-client to use proxy
if PM_PROXY_URL:
    _pm_proxies = _build_proxies(PM_PROXY_URL)
    if hasattr(_pm_ro, 'session') and _pm_proxies:
        _pm_ro.session.proxies.update(_pm_proxies)
    # Also set env vars as fallback (some HTTP libs read these)
    os.environ.setdefault("HTTP_PROXY", PM_PROXY_URL)
    os.environ.setdefault("HTTPS_PROXY", PM_PROXY_URL)


def get_pm_book(token_id: str) -> Optional[Dict]:
    try:
        return _pm_ro.get_order_book(token_id)
    except Exception as e:
        log.debug("PM book error %s: %s", token_id[:16], e)
        return None


def get_op_book(token_id: str) -> Optional[Dict]:
    if not OP_APIKEY:
        return None
    try:
        r = op_session.get(
            f"{OP_OPENAPI}/token/orderbook",
            params={"token_id": token_id},
            headers={"apikey": OP_APIKEY},
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("code") != 0:
            return None
        return data.get("result")
    except Exception as e:
        log.debug("OP book error %s: %s", token_id[:16], e)
        return None


def parse_levels(levels) -> List[Tuple[float, float]]:
    out = []
    for x in (levels or []):
        if isinstance(x, dict):
            p, s = float(x.get("price", 0)), float(x.get("size", 0))
        else:
            p, s = float(x[0]), float(x[1])
        if p > 0 and s > 0:
            out.append((p, s))
    return out


def bbo(book: Optional[Dict]):
    """Returns (best_bid, best_ask) as (price, size) or None."""
    if not book:
        return None, None
    bids = sorted(parse_levels(book.get("bids")), key=lambda x: -x[0])
    asks = sorted(parse_levels(book.get("asks")), key=lambda x: x[0])
    return (bids[0] if bids else None), (asks[0] if asks else None)


# ═══════════════════════════════════════════════════════════
# BALANCE CHECKING
# ═══════════════════════════════════════════════════════════

_balance_cache: Dict[str, Tuple[float, float]] = {}  # venue -> (balance, timestamp)
BALANCE_CACHE_TTL = 30  # секунд


def get_pm_balance() -> float:
    """Fetch USDC balance on Polymarket."""
    cached = _balance_cache.get("PM")
    if cached and time.time() - cached[1] < BALANCE_CACHE_TTL:
        return cached[0]

    balance = 0.0
    if DRY_RUN:
        balance = 1000.0  # виртуальный баланс для dry-run
    else:
        try:
            # py-clob-client: get_balance_allowance возвращает баланс USDC
            client = get_pm_client()
            if hasattr(client, 'get_balance_allowance'):
                resp = client.get_balance_allowance()
                # resp может быть dict с полем "balance" в wei или USDC
                if isinstance(resp, dict):
                    bal = resp.get("balance", 0)
                    # Polymarket хранит USDC с 6 decimals
                    balance = float(bal) / 1e6 if float(bal) > 1e4 else float(bal)
                else:
                    balance = float(resp) if resp else 0
            else:
                # Fallback: попробуем через Data API
                profile = pm_session.get(
                    "https://data-api.polymarket.com/profile",
                    headers={"Authorization": f"Bearer {PM_PRIVATE_KEY[:16]}"},
                    timeout=10,
                ).json()
                balance = float(profile.get("collateralBalance", 0))
        except Exception as e:
            log.warning("PM balance error: %s", e)
            # Возвращаем MAX_TRADE_USD как fallback
            balance = MAX_TRADE_USD

    _balance_cache["PM"] = (balance, time.time())
    log.debug("PM balance: $%.2f", balance)
    return balance


def get_op_balance() -> float:
    """Fetch USDT balance on Opinion."""
    cached = _balance_cache.get("OP")
    if cached and time.time() - cached[1] < BALANCE_CACHE_TTL:
        return cached[0]

    balance = 0.0
    if DRY_RUN:
        balance = 1000.0
    else:
        try:
            # opinion-clob-sdk: get_my_balances()
            client = get_op_client()
            if hasattr(client, 'get_my_balances'):
                resp = client.get_my_balances()
                # Ожидаем list of balances или dict
                if isinstance(resp, list):
                    for b in resp:
                        # Ищем USDT/USDC баланс
                        symbol = str(b.get("symbol", "") or b.get("token", "")).upper()
                        if symbol in ("USDT", "USDC", "BUSD"):
                            balance += float(b.get("available", 0) or b.get("balance", 0))
                elif isinstance(resp, dict):
                    balance = float(resp.get("available", 0) or resp.get("balance", 0))
            else:
                # Fallback через OpenAPI
                # Нужен wallet address — берём из private key
                from eth_account import Account
                acct = Account.from_key(OP_PRIVATE_KEY)
                r = op_session.get(
                    f"{OP_OPENAPI}/balance/{acct.address}",
                    headers={"apikey": OP_APIKEY},
                    timeout=10,
                )
                if r.status_code == 200:
                    data = r.json()
                    if data.get("code") == 0:
                        for b in data.get("result", {}).get("list", []):
                            balance += float(b.get("available", 0))
        except Exception as e:
            log.warning("OP balance error: %s", e)
            balance = MAX_TRADE_USD

    _balance_cache["OP"] = (balance, time.time())
    log.debug("OP balance: $%.2f", balance)
    return balance


def get_venue_balance(venue: str) -> float:
    """Get balance for the buy venue."""
    if "POLY" in venue.upper():
        return get_pm_balance()
    return get_op_balance()


def invalidate_balance_cache():
    """Call after execution to force fresh balance on next check."""
    _balance_cache.clear()


# ═══════════════════════════════════════════════════════════
# DYNAMIC POSITION SIZING
# ═══════════════════════════════════════════════════════════

def calculate_trade_size_usd(edge: float, buy_venue: str) -> float:
    """
    Рассчитать размер сделки в USD на основе:
    1. Edge — чем больше edge, тем больше ставка (линейное масштабирование)
    2. Баланс — не больше MAX_BALANCE_PCT от реального баланса
    3. Абсолютные лимиты — не меньше MIN_TRADE_USD, не больше MAX_TRADE_USD

    Формула:
        edge_ratio = edge / MIN_EDGE  (при MIN_EDGE=0.03: edge 0.03 -> 1x, edge 0.09 -> 3x)
        scaled = BASE_TRADE_USD * edge_ratio * EDGE_SCALE_FACTOR
                 но не больше MAX_TRADE_USD

    Пример с дефолтами (BASE=10, FACTOR=3, MIN_EDGE=0.03, MAX=50):
        edge 0.03 (3%) -> $10 * 1.0 * 3 = $30  -> capped by balance/MAX
        edge 0.05 (5%) -> $10 * 1.67 * 3 = $50  -> cap $50
        edge 0.10 (10%) -> $10 * 3.33 * 3 = $100 -> cap $50
        edge 0.15 (15%) -> huge -> cap $50
    """
    # Scale by edge
    edge_ratio = edge / MIN_EDGE if MIN_EDGE > 0 else 1.0
    sized = BASE_TRADE_USD * edge_ratio * EDGE_SCALE_FACTOR

    # Cap by absolute max
    sized = min(sized, MAX_TRADE_USD)

    # Cap by balance
    balance = get_venue_balance(buy_venue)
    balance_cap = balance * MAX_BALANCE_PCT
    sized = min(sized, balance_cap)

    # Floor
    if sized < MIN_TRADE_USD:
        if balance < MIN_TRADE_USD:
            log.warning("Balance too low on %s: $%.2f < MIN_TRADE_USD $%.2f",
                        buy_venue, balance, MIN_TRADE_USD)
            return 0
        sized = MIN_TRADE_USD

    return round(sized, 2)


def calculate_shares(trade_usd: float, price: float, depth: float) -> float:
    """Convert USD amount to shares, capped by orderbook depth."""
    if price <= 0:
        return 0
    shares = trade_usd / price
    return round(min(shares, depth), 1)


# ═══════════════════════════════════════════════════════════
# SCANNER
# ═══════════════════════════════════════════════════════════

def scan_pair(pair: MatchedPair) -> List[Opportunity]:
    out = []

    for side in ("YES", "NO"):
        pm_tok = pair.pm_yes if side == "YES" else pair.pm_no
        op_tok = pair.op_yes if side == "YES" else pair.op_no

        pm_book = get_pm_book(pm_tok)
        op_book = get_op_book(op_tok)

        pm_bid, pm_ask = bbo(pm_book)
        op_bid, op_ask = bbo(op_book)

        if not pm_bid or not pm_ask or not op_bid or not op_ask:
            continue

        # Direction 1: BUY PM, SELL OP
        edge1 = round(op_bid[0] - pm_ask[0], 6)
        depth1 = min(pm_ask[1], op_bid[1])
        if edge1 >= MIN_EDGE and depth1 >= MIN_LIQUIDITY:
            trade_usd = calculate_trade_size_usd(edge1, "POLYMARKET")
            size1 = calculate_shares(trade_usd, pm_ask[0], depth1)
            if size1 > 0:
                out.append(Opportunity(
                    trade_id=str(uuid.uuid4()), ts=int(time.time()),
                    pair_name=pair.name, side=side,
                    buy_venue="POLYMARKET", sell_venue="OPINION",
                    buy_token=pm_tok, sell_token=op_tok,
                    buy_price=pm_ask[0], sell_price=op_bid[0],
                    edge=edge1, size=round(size1, 1),
                    buy_depth=pm_ask[1], sell_depth=op_bid[1],
                    pm_market_id=pair.pm_market_id,
                    op_market_id=int(pair.op_market_id),
                    meta={"confidence": pair.confidence, "trade_usd": trade_usd},
                ))

        # Direction 2: BUY OP, SELL PM
        edge2 = round(pm_bid[0] - op_ask[0], 6)
        depth2 = min(op_ask[1], pm_bid[1])
        if edge2 >= MIN_EDGE and depth2 >= MIN_LIQUIDITY:
            trade_usd = calculate_trade_size_usd(edge2, "OPINION")
            size2 = calculate_shares(trade_usd, op_ask[0], depth2)
            if size2 > 0:
                out.append(Opportunity(
                    trade_id=str(uuid.uuid4()), ts=int(time.time()),
                    pair_name=pair.name, side=side,
                    buy_venue="OPINION", sell_venue="POLYMARKET",
                    buy_token=op_tok, sell_token=pm_tok,
                    buy_price=op_ask[0], sell_price=pm_bid[0],
                    edge=edge2, size=round(size2, 1),
                    buy_depth=op_ask[1], sell_depth=pm_bid[1],
                    pm_market_id=pair.pm_market_id,
                    op_market_id=int(pair.op_market_id),
                    meta={"confidence": pair.confidence, "trade_usd": trade_usd},
                ))

    return out


# ═══════════════════════════════════════════════════════════
# EXECUTION: Real Orders
# ═══════════════════════════════════════════════════════════

_pm_trade_client = None
_op_trade_client = None


def get_pm_client() -> ClobClient:
    global _pm_trade_client
    if not _pm_trade_client:
        _pm_trade_client = ClobClient(
            PM_HOST, key=PM_PRIVATE_KEY,
            chain_id=PM_CHAIN_ID,
            signature_type=PM_SIGNATURE_TYPE,
            funder=PM_FUNDER,
        )
        # Patch proxy into ClobClient's internal session
        if PM_PROXY_URL:
            _pm_proxies = _build_proxies(PM_PROXY_URL)
            if hasattr(_pm_trade_client, 'session') and _pm_proxies:
                _pm_trade_client.session.proxies.update(_pm_proxies)
        _pm_trade_client.set_api_creds(
            _pm_trade_client.create_or_derive_api_creds()
        )
    return _pm_trade_client


def get_op_client() -> OpinionClient:
    global _op_trade_client
    if not _op_trade_client:
        _op_trade_client = OpinionClient(
            host=OP_HOST,
            apikey=OP_APIKEY,
            chain_id=OP_CHAIN_ID,
            rpc_url=OP_RPC,
            private_key=OP_PRIVATE_KEY,
            multi_sig_addr=OP_MULTI_SIG,
        )
    return _op_trade_client


def execute_pm_order(token_id: str, side: str, price: float, size: float) -> Dict:
    """Place a GTC limit order on Polymarket."""
    if DRY_RUN:
        return {"status": "DRY_RUN", "filled": size, "price": price}

    client = get_pm_client()
    pm_side = BUY if side == "BUY" else SELL

    order_args = OrderArgs(
        token_id=token_id,
        price=price,
        size=size,
        side=pm_side,
    )
    signed = client.create_order(order_args)
    resp = client.post_order(signed, OrderType.GTC)
    log.info("PM order: %s %s @ %.4f x %.1f -> %s", side, token_id[:16], price, size, resp)
    return resp


def execute_op_order(
    market_id: int, token_id: str, side: str, price: float, amount_usd: float
) -> Dict:
    """Place a limit order on Opinion via SDK."""
    if DRY_RUN:
        return {"status": "DRY_RUN", "filled": amount_usd / price if price > 0 else 0, "price": price}

    client = get_op_client()
    op_side = OrderSide.BUY if side == "BUY" else OrderSide.SELL

    order = PlaceOrderDataInput(
        marketId=market_id,
        tokenId=token_id,
        side=op_side,
        orderType=LIMIT_ORDER,
        price=str(price),
        makerAmountInQuoteToken=amount_usd,
    )
    resp = client.place_order(order)
    log.info("OP order: %s %s @ %.4f $%.2f -> %s", side, token_id[:16], price, amount_usd, resp)
    return resp


def cancel_pm_order(order_id: str):
    try:
        get_pm_client().cancel(order_id)
    except Exception as e:
        log.error("PM cancel error: %s", e)


def recheck_and_execute(opp: Opportunity) -> Dict:
    """
    Re-verify edge, then execute both legs.
    Returns result dict with success/failure info.
    """
    result = {"success": False, "legs": [], "profit": 0, "fee": 0, "error": ""}

    # Re-fetch books
    if opp.buy_venue == "POLYMARKET":
        buy_book = get_pm_book(opp.buy_token)
        sell_book = get_op_book(opp.sell_token)
    else:
        buy_book = get_op_book(opp.buy_token)
        sell_book = get_pm_book(opp.sell_token)

    buy_bid, buy_ask = bbo(buy_book)
    sell_bid, sell_ask = bbo(sell_book)

    if not buy_ask or not sell_bid:
        result["error"] = "No liquidity"
        return result

    edge_now = sell_bid[0] - buy_ask[0]
    if edge_now < MIN_EDGE:
        result["error"] = f"Edge gone: {edge_now:.4f} < {MIN_EDGE:.4f}"
        return result

    buy_price = buy_ask[0]
    sell_price = sell_bid[0]

    # Dynamic sizing based on current edge and balance
    trade_usd = calculate_trade_size_usd(edge_now, opp.buy_venue)
    if trade_usd <= 0:
        result["error"] = f"Insufficient balance on {opp.buy_venue}"
        return result

    size = calculate_shares(trade_usd, buy_price, min(buy_ask[1], sell_bid[1]))
    if size <= 0:
        result["error"] = "No size after balance/depth check"
        return result

    amount_usd = buy_price * size

    # --- LEG 1: BUY ---
    try:
        if opp.buy_venue == "POLYMARKET":
            buy_resp = execute_pm_order(opp.buy_token, "BUY", buy_price, size)
        else:
            buy_resp = execute_op_order(opp.op_market_id, opp.buy_token, "BUY", buy_price, amount_usd)

        buy_fee = amount_usd * (PM_FEE if opp.buy_venue == "POLYMARKET" else OP_FEE)
        result["legs"].append({
            "venue": opp.buy_venue, "side": "BUY", "token_side": opp.side,
            "price": buy_price, "size": size, "fee": round(buy_fee, 2),
            "response": str(buy_resp)[:200],
        })
    except Exception as e:
        result["error"] = f"BUY failed: {e}"
        result["legs"].append({"venue": opp.buy_venue, "side": "BUY", "error": str(e)})
        return result

    # --- LEG 2: SELL ---
    sell_amount_usd = sell_price * size
    try:
        if opp.sell_venue == "POLYMARKET":
            sell_resp = execute_pm_order(opp.sell_token, "SELL", sell_price, size)
        else:
            sell_resp = execute_op_order(opp.op_market_id, opp.sell_token, "SELL", sell_price, sell_amount_usd)

        sell_fee = sell_amount_usd * (PM_FEE if opp.sell_venue == "POLYMARKET" else OP_FEE)
        result["legs"].append({
            "venue": opp.sell_venue, "side": "SELL", "token_side": opp.side,
            "price": sell_price, "size": size, "fee": round(sell_fee, 2),
            "response": str(sell_resp)[:200],
        })
    except Exception as e:
        result["error"] = f"SELL failed (OPEN POSITION on {opp.buy_venue}!): {e}"
        result["legs"].append({"venue": opp.sell_venue, "side": "SELL", "error": str(e)})
        return result

    # --- PROFIT ---
    gross = (sell_price - buy_price) * size
    total_fee = buy_fee + sell_fee
    net = gross - total_fee

    result["success"] = True
    result["profit"] = round(net, 2)
    result["fee"] = round(total_fee, 2)
    result["gross"] = round(gross, 2)
    result["size"] = round(size, 1)
    result["buy_price"] = buy_price
    result["sell_price"] = sell_price

    # Сбросить кэш балансов — после сделки баланс изменился
    invalidate_balance_cache()

    return result


# ═══════════════════════════════════════════════════════════
# TELEGRAM FORMATTING
# ═══════════════════════════════════════════════════════════

def esc(t: str) -> str:
    for ch in r"_*[]()~`>#+-=|{}.!":
        t = t.replace(ch, f"\\{ch}")
    return t


def fmt_discovery(opp: Opportunity) -> str:
    est_profit = opp.edge * opp.size
    conf = opp.meta.get("confidence", 0)
    trade_usd = opp.meta.get("trade_usd", 0)
    mode = "⚙️ _DRY\\-RUN_" if DRY_RUN else "🔴 _LIVE_"

    return (
        f"🟣 *{esc(opp.pair_name)}*\n"
        f"{esc(opp.side)} {esc(f'{opp.buy_price:.4f}')} → {esc(f'{opp.sell_price:.4f}')}\n"
        f"{esc(opp.buy_venue)} → {esc(opp.sell_venue)}\n\n"
        f"📊 Edge: *{esc(f'{opp.edge:.4f}')}* \\({esc(f'{opp.edge*100:.2f}')}%\\)\n"
        f"📦 Size: *{esc(f'{opp.size:.1f}')}* shares \\(\\~{esc(f'${trade_usd:.0f}')}\\)\n"
        f"💵 Est\\. profit: *\\~{esc(f'{est_profit:.2f}')}\\$*\n"
        f"🎯 Confidence: {esc(f'{conf:.0%}')}\n\n"
        f"{mode}"
    )


def fmt_result(opp: Opportunity, res: Dict) -> str:
    lines = [f"*{esc(opp.pair_name)}*", f"{esc(opp.buy_venue)} → {esc(opp.sell_venue)}", ""]

    for leg in res.get("legs", []):
        if "error" in leg:
            lines.append(f"❌ {esc(leg['venue'])} {esc(leg['side'])} \\- {esc(leg['error'][:80])}")
        else:
            lines.append(
                f"✅ {esc(leg['venue'])} {esc(leg['token_side'])} {esc(leg['side'])} "
                f"\\- {esc(f'{leg[\"price\"]:.4f}')} \\| {esc(f'{leg[\"size\"]:.1f}')} shares"
            )

    lines.append("")
    lines.append(
        f"💰 PROFIT \\- {esc(f'{res[\"profit\"]:.2f}')}\\$ "
        f"\\| FEE \\- {esc(f'{res[\"fee\"]:.2f}')}\\$"
    )

    if DRY_RUN:
        lines.extend(["", "⚙️ _DRY\\-RUN — no real orders_"])

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════
# TELEGRAM HANDLERS
# ═══════════════════════════════════════════════════════════

def auth(chat_id: int) -> bool:
    return chat_id == TG_CHAT


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    cid = update.effective_chat.id
    if not auth(cid):
        await update.message.reply_text(f"⛔ chat_id: {cid}")
        return
    mode = "DRY\\-RUN 🧪" if DRY_RUN else "LIVE 🔴"
    n_pairs = len(ctx.bot_data.get("pairs", []))
    await update.message.reply_text(
        f"✅ Bot active \\| *{mode}*\n"
        f"Matched pairs: {n_pairs}\n\n"
        f"/stats /history /pairs /balance /rematch /mode /whoami",
        parse_mode="MarkdownV2",
    )


async def cmd_whoami(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"chat_id: {update.effective_chat.id}")


async def cmd_mode(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not auth(update.effective_chat.id): return
    m = "DRY-RUN 🧪" if DRY_RUN else "LIVE 🔴"
    proxy_str = "off"
    if PROXY_URL:
        p = PROXY_URL.split("@")[-1] if "@" in PROXY_URL else PROXY_URL
        p = p.replace("http://", "").replace("https://", "").replace("socks5://", "")
        proxy_str = p
    await update.message.reply_text(
        f"Mode: {m}\n"
        f"Edge >= {MIN_EDGE}\n"
        f"Sizing: base ${BASE_TRADE_USD}, scale x{EDGE_SCALE_FACTOR}, cap ${MAX_TRADE_USD}\n"
        f"Max % balance per trade: {MAX_BALANCE_PCT:.0%}\n"
        f"Events: ending within {MAX_EVENT_DAYS} days\n"
        f"Proxy: {proxy_str}"
    )


async def cmd_balance(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not auth(update.effective_chat.id): return
    await update.message.reply_text("🔄 Checking balances...")
    invalidate_balance_cache()
    pm_bal = await asyncio.to_thread(get_pm_balance)
    op_bal = await asyncio.to_thread(get_op_balance)

    # Show what size would be at different edges
    examples = []
    for e in [MIN_EDGE, MIN_EDGE * 2, MIN_EDGE * 3]:
        s = calculate_trade_size_usd(e, "POLYMARKET")
        examples.append(f"  edge {e:.2%} → ${s:.0f}")

    await update.message.reply_text(
        f"💰 Balances:\n"
        f"  Polymarket: ${pm_bal:.2f}\n"
        f"  Opinion: ${op_bal:.2f}\n\n"
        f"📐 Sizing examples (PM):\n" + "\n".join(examples)
    )


async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not auth(update.effective_chat.id): return
    today = time.strftime("%Y-%m-%d")
    con = sqlite3.connect(DB_PATH)
    row = con.execute("SELECT * FROM daily_stats WHERE date=?", (today,)).fetchone()
    con.close()
    if not row:
        await update.message.reply_text("No activity today.")
        return
    await update.message.reply_text(
        f"📊 {today}\n\nExecuted: {row[3]}\nSkipped: {row[4]}\nFailed: {row[5]}\n"
        f"Profit: {row[1]:.2f}$\nFees: {row[2]:.2f}$\nNet: {row[1]-row[2]:.2f}$"
    )


async def cmd_pairs(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not auth(update.effective_chat.id): return
    pairs = ctx.bot_data.get("pairs", [])
    if not pairs:
        await update.message.reply_text("No matched pairs yet. Use /rematch")
        return
    lines = [f"📋 Matched pairs ({len(pairs)}):\n"]
    for p in pairs[:30]:
        lines.append(f"• {p.name} ({p.confidence:.0%})")
    if len(pairs) > 30:
        lines.append(f"... and {len(pairs)-30} more")
    await update.message.reply_text("\n".join(lines))


async def cmd_history(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not auth(update.effective_chat.id): return
    con = sqlite3.connect(DB_PATH)
    rows = con.execute(
        "SELECT trade_id, status, payload, result FROM trades ORDER BY created_ts DESC LIMIT 10"
    ).fetchall()
    con.close()
    if not rows:
        await update.message.reply_text("No trades yet.")
        return
    lines = ["📜 Recent:\n"]
    for r in rows:
        p = json.loads(r[2])
        icon = {"EXECUTED": "✅", "SKIPPED": "❌", "FAILED": "⚠️"}.get(r[1], "⏳")
        net = ""
        if r[3]:
            res = json.loads(r[3])
            net = f" → {res.get('profit', 0):.2f}$"
        lines.append(f"{icon} {r[0][:8]} {p.get('pair_name', '?')} edge={p.get('edge', 0):.4f}{net}")
    await update.message.reply_text("\n".join(lines))


async def cmd_rematch(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if not auth(update.effective_chat.id): return
    await update.message.reply_text("🔄 Re-matching markets via Claude...")
    pairs = await refresh_matches()
    ctx.bot_data["pairs"] = pairs
    ctx.bot_data["last_match"] = time.time()
    await update.message.reply_text(f"✅ Found {len(pairs)} matching pairs")


async def on_button(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if not auth(query.message.chat_id):
        await query.edit_message_text("⛔")
        return

    data = query.data or ""
    try:
        action, tid = data.split(":", 1)
    except ValueError:
        return

    opp = db_get_trade(tid)
    if not opp:
        await query.edit_message_text("Trade expired.")
        return

    if action == "SKIP":
        db_update(tid, "SKIPPED")
        db_stats(skipped=True)
        await query.edit_message_text(f"❌ Skipped: {opp.pair_name} (edge {opp.edge:.4f})")
        return

    if action == "EXEC":
        db_update(tid, "EXECUTING")
        await query.edit_message_text("⏳ Re\\-checking prices\\.\\.\\.", parse_mode="MarkdownV2")
        await asyncio.sleep(1)

        try:
            res = await asyncio.to_thread(recheck_and_execute, opp)
        except Exception as e:
            db_update(tid, "FAILED", json.dumps({"error": str(e)}))
            db_stats(failed=True)
            await query.edit_message_text(f"❌ Error: {e}")
            return

        if res["success"]:
            db_update(tid, "EXECUTED", json.dumps(res))
            db_stats(profit=res["profit"], fee=res["fee"], executed=True)
            await query.edit_message_text(fmt_result(opp, res), parse_mode="MarkdownV2")
        else:
            db_update(tid, "FAILED", json.dumps(res))
            db_stats(failed=True)
            err_text = res.get("error", "Unknown error")
            # Format legs if any
            leg_lines = []
            for leg in res.get("legs", []):
                if "error" in leg:
                    leg_lines.append(f"❌ {leg['venue']} {leg['side']}: {leg['error'][:80]}")
                else:
                    leg_lines.append(f"✅ {leg['venue']} {leg['side']} @ {leg.get('price', 0):.4f}")
            full = f"⚠️ {opp.pair_name}\n\n{err_text}"
            if leg_lines:
                full += "\n\n" + "\n".join(leg_lines)
            await query.edit_message_text(full)


# ═══════════════════════════════════════════════════════════
# MAIN LOOP
# ═══════════════════════════════════════════════════════════

async def scanner_loop(app: Application):
    dedup: Dict[tuple, float] = {}

    # Initial match
    pairs = await refresh_matches()
    app.bot_data["pairs"] = pairs
    app.bot_data["last_match"] = time.time()

    log.info("Scanner started with %d pairs", len(pairs))

    while True:
        try:
            now = time.time()

            # Refresh matches periodically
            if now - app.bot_data.get("last_match", 0) > MATCH_REFRESH_INTERVAL:
                pairs = await refresh_matches()
                app.bot_data["pairs"] = pairs
                app.bot_data["last_match"] = now
                log.info("Refreshed: %d pairs", len(pairs))

            pairs = app.bot_data.get("pairs", [])
            dedup = {k: v for k, v in dedup.items() if now - v < DEDUP_TTL}

            for pair in pairs:
                opps = await asyncio.to_thread(scan_pair, pair)

                for opp in opps:
                    k = (opp.pair_name, opp.side, opp.buy_venue,
                         round(opp.buy_price, 3), round(opp.sell_price, 3))
                    if k in dedup:
                        continue
                    dedup[k] = now

                    db_save_trade(opp)

                    kb = InlineKeyboardMarkup([[
                        InlineKeyboardButton("✅ EXECUTE", callback_data=f"EXEC:{opp.trade_id}"),
                        InlineKeyboardButton("❌ SKIP", callback_data=f"SKIP:{opp.trade_id}"),
                    ]])

                    try:
                        await app.bot.send_message(
                            chat_id=TG_CHAT,
                            text=fmt_discovery(opp),
                            parse_mode="MarkdownV2",
                            reply_markup=kb,
                        )
                    except Exception as e:
                        log.error("TG error: %s", e)

                # Small delay between pairs to respect rate limits
                await asyncio.sleep(0.1)

        except Exception as e:
            log.exception("Scanner: %s", e)

        await asyncio.sleep(SCAN_INTERVAL)


async def post_init(app: Application):
    init_db()
    mode = "DRY-RUN" if DRY_RUN else "LIVE"
    try:
        await app.bot.send_message(TG_CHAT, f"🤖 Bot started [{mode}]\nMatching markets...")
    except Exception:
        pass
    asyncio.create_task(scanner_loop(app))


def main():
    errors = []
    if not TG_TOKEN: errors.append("TELEGRAM_BOT_TOKEN")
    if TG_CHAT == 0: errors.append("TELEGRAM_ALLOWED_CHAT_ID")
    if not OP_APIKEY: errors.append("OPINION_APIKEY")
    if not CLAUDE_API_KEY: errors.append("CLAUDE_API_KEY")

    if not DRY_RUN:
        if not PM_PRIVATE_KEY: errors.append("PM_PRIVATE_KEY")
        if not PM_FUNDER: errors.append("PM_FUNDER")
        if not OP_PRIVATE_KEY: errors.append("OPINION_PRIVATE_KEY")

    if errors:
        print(f"❌ Missing env vars: {', '.join(errors)}")
        print("   See .env.example")
        raise SystemExit(1)

    builder = Application.builder().token(TG_TOKEN).post_init(post_init)

    # Telegram proxy (SOCKS5 or HTTP)
    if TG_PROXY_URL:
        from telegram.request import HTTPXRequest
        tg_request = HTTPXRequest(proxy=TG_PROXY_URL)
        builder = builder.request(tg_request).get_updates_request(
            HTTPXRequest(proxy=TG_PROXY_URL)
        )
        log.info("Telegram proxy: %s",
                 TG_PROXY_URL.split("@")[-1] if "@" in TG_PROXY_URL else TG_PROXY_URL)

    app = builder.build()
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("whoami", cmd_whoami))
    app.add_handler(CommandHandler("mode", cmd_mode))
    app.add_handler(CommandHandler("balance", cmd_balance))
    app.add_handler(CommandHandler("stats", cmd_stats))
    app.add_handler(CommandHandler("pairs", cmd_pairs))
    app.add_handler(CommandHandler("history", cmd_history))
    app.add_handler(CommandHandler("rematch", cmd_rematch))
    app.add_handler(CallbackQueryHandler(on_button))

    log.info("Starting... DRY_RUN=%s", DRY_RUN)
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
