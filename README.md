# Arbitrage scanner

A private, read-only Rust scanner for cross-venue sports-market research.

This repository is intentionally separate from the dashboard. It contains no
browser surface, committed credentials, or order, preview-order, or cancel
calls. The scanner makes read-only balance requests for qualifying candidates.

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
cargo run --release --bin stream_scanner -- atp
cargo run --release --bin stream_scanner -- wta
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

For MLB, the scanner checks full-game and first-five-innings spreads and totals,
plus team run totals and player hits, home runs, RBIs, total bases, and combined
hits + runs + RBIs. Player props match Kalshi's half-run strike to
Polymarket's integer "at least" line. Markets require the same scheduled start
time, statistic, team or player, line, and game period. Same-day duplicate MLB
matchups with ambiguous game identifiers are skipped.

For tennis, the scanner discovers ATP and WTA matches separately. It checks
match winners, ATP match-game spreads, ATP and WTA match-game totals, ATP match-set
totals, set winners, and exact match scores. Pairs require the same scheduled
date and players; spreads and totals also require the same market type and exact
line, while set winners and exact scores require the same set or score and
player. Match-winner discovery uses the full player names from Kalshi's
contracts because its event titles can abbreviate players to short surnames.
Kalshi currently has no WTA game-spread series configured. Tennis
matches with duplicate same-day player pairings are skipped.

For CS2, Valorant, Dota 2, LoL, and R6, the scanner checks match winners and
individual map winners (called game winners by Polymarket for Dota 2 and LoL). It also
checks series total maps for CS2, Valorant, and LoL, and series total games for
Dota 2. Teams, scheduled starts within two hours, map number, and total line
must agree; a Polymarket UTC date may be the next day after Kalshi's Eastern
date. Esports map handicaps and round totals have no configured equivalent
Kalshi series; R6 has no configured series total.
Discovery category counts include only configured comparable market types.

Both tools accept `cfb`, `nfl`, `mlb`, `atp`, `wta`, `tennis`, `cs2`,
`valorant`, `dota2`, `lol`, or `r6` (default: `cfb`). Run `atp` and `wta`
separately, or use `tennis` to scan both in one process. The scanner also
accepts `all`, which runs ATP and WTA independently alongside the other sports
and splits subscriptions before either venue's 100-market cap.

Candidates are simulated across up to 25 whole contracts of L2 book depth,
must show at least $0.25 net profit after fees, and must survive a fresh update
from both venues. They are dry-run observations,
not proof that both venues' settlement rules are equivalent.

### Optional Novig dry-run feed

Set both variables below in the ignored `.env` file to add Novig's live book
feed to `stream_scanner`. The key must have `trading` or `trading::read` scope;
the management key created in Novig's Profile cannot open the book WebSocket.
Use a read-only trading key where available. Keep its PEM outside the repository
and never paste it into chat or commit it.

```bash
NOVIG_KEY_ID=...
NOVIG_PRIVATE_KEY_PATH=/absolute/path/to/novig-trading-read.pem
# NOVIG_ENV=qa  # only for a QA key; production is the default
```

Check the key and a live read-only book subscription with
`cargo run --release --bin stream_scanner -- novig-check`.

The key created in Novig Profile is a management key. To set up a separate
read-only scanner key, preserve its ID and PEM path as
`NOVIG_MANAGEMENT_KEY_ID` and `NOVIG_MANAGEMENT_PRIVATE_KEY_PATH` in `.env`,
then run these explicit account-setup steps:

```bash
python3 scripts/novig_account_setup.py status
python3 scripts/novig_account_setup.py open-subaccount
python3 scripts/novig_account_setup.py issue-read-key
```

The first write opens one unfunded subaccount and stores its trading key under
`~/.secrets`; the second creates a `trading::read` key. The script prints the
new `NOVIG_KEY_ID` and `NOVIG_PRIVATE_KEY_PATH` to set for the scanner. It
never transfers funds or submits orders. It refuses to open a second
subaccount when one already exists. Python `requests` and `cryptography` are
required; Novig account creation may also require completed identity checks.

The scanner currently pairs Novig **two-way match/game moneylines** against
Kalshi or Polymarket when the existing Kalshi–Polymarket match and Novig outcome
names identify the same fixture unambiguously. NFL, CFB, MLB, ATP, and WTA are
queried; abbreviated outcomes that cannot be safely aligned are skipped. Novig
spreads, totals, and props appear in Novig's discovery inventory but are not
yet matched or subscribed. The Novig discovery row counts open markets on
events with an eligible game moneyline. The `Matched` row and `Market pairs`
count Kalshi–Polymarket pairs. Discovery output is dimmed in terminals; live
opportunity logs use the normal terminal style.
Novig's 1¢ payout units are converted to whole $1 payout contracts; its market
fee coefficient is applied conservatively even before a game starts. A Novig
connection or catalog failure leaves Kalshi–Polymarket scanning active. Novig
balances are shown as unavailable. No Novig order or account mutation route is
called.

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
