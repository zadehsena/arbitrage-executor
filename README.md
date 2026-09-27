# Arbitrage executor

A private, dry-run-first Rust execution core for cross-venue market research.

This repository is intentionally separate from the dashboard. It contains no
browser surface, no committed credentials, and no live order submission capability.

## Current safety boundary

- Every proposed trade passes deterministic risk checks.
- Approved trades are **recorded as dry runs only**.
- Each decision is appended to a local JSONL journal.
- Live venue adapters are not implemented yet.

## Intended architecture

```text
market-data streams → normalized signal → risk gate → parallel leg plan → journal
```

The future venue adapters must re-check executable order-book depth, fees,
market rules, and available balance immediately before order submission. A
cross-venue trade is never atomic: one leg can fill while the other does not.

## Run a dry run

```bash
cargo run -- --signal examples/sample-signal.json
```

The result is written to `execution-journal.jsonl` by default.

## Read-only cross-venue probe

```bash
cargo run --bin market_probe -- cfb
```

This command samples likely CFB or NFL event matches and prints **review-only
executable best-ask comparisons**. Polymarket US BBO reads are public; Kalshi
order-book reads are signed `GET` requests and use `KALSHI_API_KEY_ID` and
`KALSHI_PRIVATE_KEY_PATH` from the ignored `.env` file. It has no order,
preview-order, cancel, balance, or portfolio calls.

The output is deliberately not a trading signal: fees, rules, available depth
on both sides, and cross-venue fill risk are not yet modelled.

## Read-only streaming probe

```bash
cargo run --bin stream_probe
```

This validates authenticated WebSocket subscriptions to one matched game on
both venues, reads six market-data messages, and exits. It has no order routes.

## Continuous scanner (dry-run only)

```bash
cargo run --bin stream_scanner -- cfb
```

This discovers matching two-way moneylines, maintains in-memory books from
both WebSocket feeds, and writes qualifying **net-fee candidates** to the
ignored `scanner-candidates.jsonl`. It uses the current published Kalshi and
Polymarket US taker-fee formulas for standard sports markets, but does not
submit trades. On a stream disconnect it clears both books, reconnects with
exponential backoff, and waits for fresh snapshots; it also refreshes the
matched-market universe every five minutes.

Before candidates can be logged, create `rules-approved.json` from
`examples/rules-approved.json` and replace the example with an event title
whose settlement rules you have manually verified on both venues. Candidates
are then simulated across up to 25 whole contracts of L2 book depth and must
survive a fresh update from both venues.

## Before any live-trading work

1. Replay recorded signals against historical order-book snapshots.
2. Run in dry-run mode until fills, fees, slippage, and settlement mapping are reconciled.
3. Add venue adapters with keys stored outside the repository.
4. Add exchange-specific order/fill reconciliation and bounded emergency hedging.
5. Require an explicit live-mode gate in addition to risk limits.
