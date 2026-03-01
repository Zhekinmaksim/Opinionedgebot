# OpinionEdgeBot

Cross-venue arbitrage detection and execution tool for prediction markets.  
Monitors **Opinion** and **Polymarket** orderbooks in real-time, finds matching events via LLM, and alerts on pricing discrepancies.

> **Status:** Pre-alpha · Actively developed · Not financial advice

## How It Works

```
┌──────────────┐    ┌──────────────┐
│  Polymarket  │    │   Opinion    │
│  Gamma API   │    │   OpenAPI    │
│ (all events) │    │(all markets) │
└──────┬───────┘    └──────┬───────┘
       │                   │
       └────────┬──────────┘
                ▼
        ┌───────────────┐
        │  Claude API   │  ← LLM market matching (every 10 min)
        └───────┬───────┘
                ▼
        ┌───────────────┐
        │   Scanner     │  ← Orderbook polling (every 10s)
        └───────┬───────┘
                ▼
        ┌───────────────┐
        │   Telegram    │  ← Alert + EXECUTE / SKIP buttons
        └───────┬───────┘
                ▼
    ┌───────────────────────┐
    │  Hybrid Execution     │
    │  🤖 PM = auto (API)   │
    │  👤 OP = manual alert │
    └───────────────────────┘
```

1. **Discover** — fetches all active events from Polymarket (Gamma API) and Opinion (OpenAPI), filtered to events ending within a configurable window (default: 3 days)
2. **Match** — sends market titles to Claude API to find identical events across platforms (confidence ≥ 85%)
3. **Scan** — polls both orderbooks every 10 seconds, calculates cross-venue edge
4. **Alert** — sends Telegram notification with edge %, estimated profit, and action buttons
5. **Execute** — on confirmation: places Polymarket order via API, shows detailed manual instruction for Opinion leg
6. **Remind** — periodically reminds if the Opinion leg hasn't been confirmed

## Key Features

- **Full market auto-discovery** — no manual market configuration needed
- **LLM-powered matching** — handles different wording, languages, and slight variations
- **Hybrid execution** — Polymarket auto, Opinion manual (no Opinion trading API required)
- **Dynamic position sizing** — scales with edge magnitude and available balance
- **Human-in-the-loop** — every trade requires explicit Telegram confirmation
- **Proxy support** — HTTP/HTTPS/SOCKS5 for geo-restricted access
- **Risk controls** — min edge, max trade size, balance cap, liquidity floor, dedup

## Opinion API Usage

This tool uses the **Opinion OpenAPI** in **read-only mode** only:

| Endpoint | Purpose | Frequency |
|----------|---------|-----------|
| `GET /market` | Discover active markets | Every 10 min |
| `GET /token/orderbook` | Bid/ask depth for edge detection | Every 10s per matched pair |
| `GET /token/latest-price` | Price monitoring | On-demand |

No order placement, position management, or on-chain operations are performed through the API. All Opinion trades are executed manually through the web interface.

Estimated load: **~2-5 requests/second** (well within 15 TPS limit).

## Quick Start

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install py-clob-client python-telegram-bot requests python-dotenv

cp env.example .env
# Fill in: TELEGRAM_BOT_TOKEN, TELEGRAM_ALLOWED_CHAT_ID,
#          CLAUDE_API_KEY, PM_PRIVATE_KEY, PM_FUNDER, OPINION_APIKEY

python arb_bot.py
```

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `DRY_RUN` | Simulate without real orders | `true` |
| `MIN_EDGE` | Minimum spread to trigger alert | `0.03` (3%) |
| `MAX_TRADE_USD` | Absolute cap per trade | `$50` |
| `MAX_BALANCE_PCT` | Max % of balance per trade | `15%` |
| `MAX_EVENT_DAYS` | Only events ending within N days | `3` |
| `SCAN_INTERVAL_SEC` | Orderbook polling interval | `10s` |
| `PROXY_URL` | HTTP/SOCKS5 proxy | — |

## Bot Commands

| Command | Description |
|---------|-------------|
| `/start` | Bot status and matched pairs count |
| `/pairs` | List matched market pairs |
| `/balance` | Polymarket balance + sizing examples |
| `/stats` | Today's P&L and trade counts |
| `/history` | Last 10 trades with status |
| `/rematch` | Force re-matching via Claude |
| `/mode` | Current config and proxy status |

## Roadmap

- [x] Auto-discovery of all markets on both platforms
- [x] LLM-based cross-venue market matching
- [x] Dynamic edge-based position sizing
- [x] Polymarket auto-execution via CLOB API
- [x] Telegram alerts with manual Opinion instructions
- [x] Reminder system for open positions
- [x] Proxy support (HTTP/SOCKS5)
- [ ] Opinion CLOB SDK integration (when API key available)
- [ ] WebSocket orderbook streaming (lower latency)
- [ ] Multi-leg execution with auto-retry
- [ ] Web dashboard for P&L tracking

## Disclaimer

This software is provided as-is for educational and research purposes. Prediction markets involve significant risk — event outcomes are binary and you can lose your entire position. Always conduct your own research and only trade with capital you can afford to lose. The authors are not responsible for any financial losses.

## License

MIT
