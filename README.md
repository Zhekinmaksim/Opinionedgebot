# Opinionedgebot
Pre-alpha arbitrage bot for Opinion prediction markets (market data + execution + risk controls)

## Goal
Stream markets in real time, detect mispricings vs. external reference prices, and execute risk-limited orders to improve liquidity and market efficiency.

## Planned features
- WebSocket market data ingestion (book/trades)
- Edge computation + configurable sizing
- Order management (place/cancel/replace) with idempotency
- Risk limits (max exposure, max loss, cooldowns)
- Logs, monitoring, PnL tracking

## Status
Planning / pre-alpha (not live yet).
