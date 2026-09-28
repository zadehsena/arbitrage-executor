# Arbitrage scanner

A private, read-only Rust scanner for cross-venue sports-market research.

This repository is intentionally separate from the dashboard. It contains no
browser surface, no committed credentials, and no order,
preview-order, cancel, balance, or portfolio calls.

## Read-only cross-venue probe

```bash
cargo run --bin market_probe -- mlb
```

This command samples likely CFB, NFL, MLB, or tennis event matches and prints **review-only
executable best-ask comparisons**. Polymarket US BBO reads are public; Kalshi
order-book reads are signed `GET` requests and use `KALSHI_API_KEY_ID` and
`KALSHI_PRIVATE_KEY_PATH` from the ignored `.env` file. It has no order,
preview-order, cancel, balance, or portfolio calls.

The output is deliberately not a trading signal: fees, rules, available depth
on both sides, and cross-venue fill risk are not yet modelled.

## Continuous scanner (dry-run only)

```bash
cargo run --bin stream_scanner -- tennis
```

This discovers matching two-way moneylines, maintains in-memory books from
both WebSocket feeds, and writes qualifying **net-fee candidates** to the
ignored `scanner-candidates.jsonl`. It uses the current published Kalshi and
Polymarket US taker-fee formulas for standard sports markets, but does not
submit trades. On a stream disconnect it clears both books, reconnects with
exponential backoff, and waits for fresh snapshots. It starts the next market
discovery one minute before its five-minute subscription refresh.

For NFL and CFB, the scanner also discovers full-game, half, and quarter
spreads. These require an exact half-point line, matching game period, and matching
opposite outcomes; a full-game, half, or quarter market is never paired with a
different scope. NFL and CFB full-game totals are also included. They remain dry-run
observations, and their settlement-rule parity is not automatically verified.

For NFL it also checks standard full-game player props (passing, rushing, and
receiving yards; receptions) and team props (points and selected yardage
totals). For CFB it checks the corresponding team props. Props require an exact
line, entity, and provider market-type match; period, ladder, season, and
multi-stat props are excluded. CFB player props are not subscribed because no
matching single-game Kalshi series has been configured and verified.

Both tools accept `cfb`, `nfl`, `mlb`, `tennis`, `cs2`, `valorant`, `dota2`,
`lol`, or `r6` (default: `cfb`). The `tennis` option discovers both ATP and
WTA match-winner markets. The scanner also accepts `all`, which runs each
sport independently in one process and splits subscriptions before either
venue's 100-market cap.

Candidates are simulated across up to 25 whole contracts of L2 book depth,
must show at least $0.25 net profit after fees, and must survive a fresh update
from both venues. They are dry-run observations,
not proof that both venues' settlement rules are equivalent.

Each scanner candidate explicitly reports that it was not executed and performs
signed, read-only balance requests that are stored with the candidate in the
local JSON journal. Incoming book updates evaluate only pairs using the changed
market; a bounded reporting queue keeps balance requests and journal writes out
of the market-data loop. It does not call order, cancel, or portfolio APIs. Set
these ignored `.env` variables before running the scanner:

```bash
KALSHI_API_KEY_ID=...
KALSHI_PRIVATE_KEY_PATH=/absolute/path/to/kalshi-private-key.pem
POLYMARKET_US_KEY_ID=...
POLYMARKET_US_SECRET_KEY=... # base64-encoded Ed25519 private key
```

## Before any live-trading work

1. Replay recorded signals against historical order-book snapshots.
2. Run in dry-run mode until fills, fees, slippage, and settlement mapping are reconciled.
3. Add venue adapters with keys stored outside the repository.
4. Add exchange-specific order/fill reconciliation and bounded emergency hedging.
5. Require an explicit live-mode gate in addition to risk limits.
