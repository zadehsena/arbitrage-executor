# Arbitrage scanner

A private, read-only Rust scanner for cross-venue sports-market research.

This repository is intentionally separate from the dashboard. It contains no
browser surface, committed credentials, or order, preview-order, or cancel
calls. The scanner makes read-only balance requests for qualifying candidates.

## Read-only cross-venue probe

```bash
cargo run --bin market_probe -- mlb
```

This command samples likely CFB, NFL, NBA, college basketball, MLB, or tennis event matches and prints **review-only
executable best-ask comparisons**. Polymarket US BBO reads are public; Kalshi
order-book reads are signed `GET` requests and use `KALSHI_API_KEY_ID` and
`KALSHI_PRIVATE_KEY_PATH` from the ignored `.env` file. It has no order,
preview-order, cancel, balance, or portfolio calls.

The output is deliberately not a trading signal: fees, rules, available depth
on both sides, and cross-venue fill risk are not yet modelled.

## Continuous scanner (dry-run only)

```bash
cargo run --release --bin stream_scanner -- cbb
cargo run --release --bin stream_scanner -- wcbb
cargo run --release --bin stream_scanner -- nba
cargo run --release --bin stream_scanner -- atp
cargo run --release --bin stream_scanner -- wta
```

This discovers matching two-way moneylines, maintains in-memory books from
both WebSocket feeds, and writes qualifying **net-fee candidates** to the
ignored `scanner-candidates.jsonl`. It estimates standard taker fees using current Kalshi series metadata and an
explicit Polymarket account policy. It does not submit trades. On a stream disconnect it clears both books, reconnects with
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

For NBA, the scanner checks full-game moneylines, spreads, and totals using
Kalshi's `KXNBAGAME`, `KXNBASPREAD`, and `KXNBATOTAL` series. Matches require the
same scheduled date and exact NBA teams, including distinct Lakers and Clippers
aliases. Spreads and totals require exact lines and market types; integer spreads
and duplicate same-day matchups are skipped. NBA player props, team totals,
and half/quarter markets are not configured. Settlement-rule parity is not
automatically verified.

For college basketball, `cbb` scans men's games and `wcbb` scans women's games.
Both are included independently in `all`. Each checks full-game moneylines,
spreads, and totals using its own Kalshi `KXNCAAMB*` or `KXNCAAWB*` series and
Polymarket league. Matches require the same scheduled date and two matching
schools; school aliases include State/St. and verified venue name variants.
Women's `(W)` suffixes are normalized within the women's league. Duplicate
same-day matchups are skipped. Lines and full-game market types must agree;
integer spreads, team totals, player props, and half/quarter markets are excluded.
Settlement-rule parity is not automatically verified. The probe also accepts
`cbb` and `wcbb`; `ncaab`/`ncaamb` alias `cbb`, and `ncaawb` aliases `wcbb`.

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

Both tools accept `cfb`, `nfl`, `nba`, `cbb`, `wcbb`, `mlb`, `atp`, `wta`, `tennis`, `cs2`,
`valorant`, `dota2`, `lol`, or `r6` (default: `cfb`). Run `atp` and `wta`
separately, or use `tennis` to scan both in one process. The scanner also
accepts `all`, which runs ATP and WTA independently alongside the other sports
and splits subscriptions before either venue's 100-market cap.

Kalshi/Polymarket candidates choose the matched whole-contract size with the
highest total net profit across visible L2 book depth, including slippage and
order-wide cumulative fee rounding. Equal-profit sizes favor less capital. Logs show
visible contracts on each selected side and the capital required per venue;
visible depth includes prices that may be unprofitable. Sizing is hypothetical
and is not constrained by account balances. Candidates must show at least
$0.01 net profit after fees and must survive a fresh update from both venues. Only directions with valid fee metadata and a current, explicit settlement-rule
approval can emit candidates. Approvals are operator reviews, not exchange
guarantees. Execution remains dry-run only.

Kalshi/Polymarket opportunity logs use aligned venue columns and show ROI as
net profit divided by combined required capital (including fees). The KALSHI
and POLYMARKET headers link directly to their markets in terminals supporting
OSC 8 hyperlinks. Redirected output and `TERM=dumb` use plain headers.
Use your terminal's link gesture (usually Cmd-click on macOS or Ctrl-click on
Linux/Windows) to open them. The Polymarket US link includes the selected market
slug; the Kalshi link uses the selected contract ticker. URLs and market
identifiers are also saved in the JSONL journal.

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

## Fees and settlement review

Run public discovery and produce a review report without starting WebSockets,
reading balances, or writing candidate records:

```bash
cargo run --bin stream_scanner -- nba --review
cargo run --bin stream_scanner -- cbb --review
```

Reports are written atomically to `market-reviews/<sport>.json`. Normal scanner
runs also refresh them during discovery. The discovery table still shows all
matched markets. A separate review summary counts individual hedge directions:
a moneyline pair usually has two, while a spread/total pair has one. By default
**zero directions are settlement-approved**, so no candidates are reported.
The existing `preview` command is synthetic and does not pass the approval gate.
The `market_probe` is still a review-only BBO comparison, with no fee or settlement
qualification; do not use its before-fee edge as an approved scanner candidate.

### Fee policy

With no `fee-policy.json`, the scanner uses Polymarket US coefficient `0.095`
(the highest standard published tier), rather than assuming the `0.0695` tier.
This is a conservative estimate, **not an automatically verified account tier**.
Kalshi coefficients come from each series' `fee_type` and `fee_multiplier`.
Unknown models, missing fee data, and expired snapshots block candidate reporting.
Upcoming Kalshi fee transitions invalidate the estimate at the transition time.

Copy `fee-policy.example.json` to `fee-policy.json` to set account assumptions.
A lower Polymarket rate requires a `polymarket_account_rate` object containing
`coefficient`, RFC3339 `verified_at` and `expires_at` timestamps, and a nonempty
`evidence` string describing the actual account fee screen or exchange confirmation.
Overrides must be verified on the current Eastern calendar day and last at most
24 hours. Their effective expiry is capped at the next Eastern midnight; set an
earlier expiry for an onboarding/override window that ends sooner. Expired or malformed policies fail closed rather
than quietly falling back to a cheaper rate. No signed account-history requests
or estimates of tier eligibility from balances are made.

Kalshi balance precision defaults to `$0.01`, conservatively allowing for a
non-direct account. Only set `kalshi_balance_precision` to `0.0001` after verifying
direct-member precision. Polymarket fees use the banker's-rounded cumulative
exact fee cap across an order; rounding is not restarted at each price level.
Kalshi estimates align aggregate cost plus fee to the account's balance grid.
L2 aggregation hides individual fill fragmentation, so these remain estimates;
actual exchange fills, six-decimal trade fees, account overrides, and ledger
rebates must still be reconciled before execution. Maker rebates are not credited.
Fee models, assumptions, expiry, and settlement-review fingerprints are logged
with every candidate. Fees are rechecked during each discovery and expire after
at most ten minutes without a refresh; a snapshot is never kept indefinitely.

Sources: [Polymarket US fees](https://docs.polymarket.us/fees),
[Kalshi fee models](https://docs.kalshi.com/api-reference/market/get-series),
[Kalshi rounding](https://docs.kalshi.com/getting_started/fee_rounding), and
[Kalshi fee schedule](https://kalshi.com/docs/kalshi-fee-schedule.pdf).

### Settlement approval

Each report direction contains both venues' captured rule text, market identity,
line/scope, settlement sources, the Polymarket hedge side, and a SHA-256 fingerprint.
Kalshi's linked current contract PDF is downloaded into `market-reviews/terms/`
and its content hash is included. Read the PDF alongside the captured descriptions;
linked general rulebooks and any clarifications also need to be checked manually.
Reports highlight fair-price and postponement clauses. They do not infer parity
from similar wording, or automatically approve a market type across games.

For each exact direction being reviewed:

1. Compare the event, participants, statistic, threshold, period, and hedge side.
2. Compare overtime/ties, postponement windows, cancellations/voids, fair-price
   outcomes, abandonment/retirement, player participation, settlement sources,
   and corrections. Confirm the combined payout is at least `$1` for every
   applicable outcome. A fair-price clause can prevent that guarantee even
   when both normal game outcomes are hedged. Leave such a direction unapproved
   unless the full terms establish the required payout property.
3. Copy `settlement-approvals.example.json` to `settlement-approvals.json` and
   add a completed `settlement-review-template.json` object to its `approvals`
   array. Record the exact fingerprint, reviewer, source URLs, reasoning,
   and current timestamps. All eight checks must be explicitly true; do not
   mark an unresolved or inapplicable-looking clause true without documenting
   why its payout is covered. Approval expires within seven days at most.
4. Rerun `--review` to confirm the intended direction is approved. The other
   hedge direction requires its own review. Changed captured rules, PDF contents,
   event details, or hedge direction require a new fingerprint and approval.

Missing evidence, missing approvals, incomplete checks, and expired approvals
block candidates. Recheck external clarifications during a review: a hash of
captured evidence cannot detect changes to external documents not included in it.
No approvals are shipped, and no claim of verified settlement parity is made
for the previously added sports.

Optional paths: `SCANNER_FEE_POLICY`, `SCANNER_SETTLEMENT_APPROVALS`, and
`SCANNER_REVIEW_DIR`. Paths and policies are reread on each discovery. Keep these
local files out of version control; the supplied examples contain no approvals.

## Feed integrity

Run a ten-second, authenticated **market-data-only** smoke check for one matched
pair after discovery. It does not read balances, write candidate records, or send
orders; the existing feed credentials are required:

```bash
cargo run --bin stream_scanner -- nba --feed-check
```

The scanner validates market data before changing executable books:

- Kalshi `sid`/`seq` must be contiguous across **all** order-book messages on
  the subscription, including YES-side deltas. Each market requires an initial
  snapshot. Both sides are checked for invalid levels, negative resulting sizes,
  duplicate price levels, and crossed books.
- Polymarket US full-book images require an explicit open market state and a
  valid `transactTime`. Older snapshots and conflicting images with the same
  timestamp trigger recovery. Exact duplicates do not refresh local freshness.
  Non-open states clear executable liquidity and pending confirmations. This
  feed does not document a sequence counter, so no gap-free sequence guarantee
  is claimed for it; each accepted full image replaces the previous book.
- Exchange source times may be at most two seconds old and at most 250 ms in
  the future. Keep the machine clock synchronized. These conservative limits
  can reject valid but idle/stale-timestamped books rather than treating an
  old source image as fresh merely because it just arrived locally.
- WebSocket keep-alive pings run every five seconds. Fifteen seconds without
  received frames, subscription errors, malformed payloads, or missing initial
  books after twenty seconds end the session. Reconnect starts with empty
  books and confirmations; it does not apply a post-gap delta to the old book.
- Candidate reporting uses a nonblocking bounded queue. Under reporting
  pressure it drops candidate notifications and keeps consuming feeds.
  Stale, suspended, empty, or no-longer-profitable books reset pending
  confirmation rather than counting an earlier observation as confirmation.

These changes validate the existing read-only streaming path. They do not
establish that quoted depth will still exist when an order reaches an exchange.
Sources: [Kalshi order-book protocol](https://docs.kalshi.com/websockets/orderbook-updates),
[Polymarket US market feed](https://docs.polymarket.us/api-reference/websocket/markets),
and [Polymarket US heartbeats](https://docs.polymarket.us/api-reference/websocket/overview).

## Paper handling of one-sided fills

The execution supervisor in `src/execution.rs` is **paper only** and is not
connected to scanner candidates or any order API. Its offline scenario runner
uses no credentials or network calls:

```bash
cargo run --bin paper_executor -- tests/fixtures/one_sided_fill.json /tmp/paper-fill-run.jsonl
cargo run --bin paper_executor -- --replay /tmp/paper-fill-run.jsonl
```

Choose a new journal filename for each run; existing journals are never overwritten.
The supplied scenario buys ten contracts on one venue, fills six on the other,
and sells the four excess contracts. Its final position is six complementary
contracts on each venue, rather than selling the entire first leg.

Plans specify entry price limits, entry fee reserves, entry and unhedged timeouts,
a recovery deadline, an exit quote-age limit, maximum recovery loss, and a maximum
number of exit attempts. All generated orders are IOC **limit** intents. An IOC
can still partially fill; neither IOC nor FOK makes orders across venues atomic.
There is no unbounded market-order fallback.

The controller handles cumulative fill reports, duplicate/stale reports, rejection,
partial fills, unknown outcomes, disconnects, late fills during cancellation,
operator stops, and partial emergency exits. On failure or timeout it requests
cancellation and reconciliation of outstanding orders, then waits for terminal
reports before issuing another order. Sending a cancel does not count as a
cancellation confirmation. Unknown outcomes never trigger blind replacement
orders. Stable client IDs identify entries and each bounded exit attempt.

Once all outstanding orders are confirmed terminal, a mismatch requests a fresh
exit bid on the venue holding excess contracts. The proposed sell is limited to
the excess and available quote size. Worst-case recovery loss includes the
allocated entry cost/fees, the sell limit price, and the supplied exit fee bound.
Losses are accumulated across attempts. A stale quote sends no order. An
unacceptable exit price, exhausted attempts, a fee/price breach, an unknown order
past deadline, or an operator stop halts the controller and reports the remaining
exposure. A halt does **not** mean the position was closed or loss was prevented.

Scenario times and quote times are monotonic milliseconds in the same simulated
clock domain. Quantities use millionths of a contract (`quantity_micros`), money
uses millionths of a dollar, and prices use `$0.0001` ticks (`4000` = `$0.40`).
The CLI fsyncs a hash-chained event/action/state journal and verifies deterministic
replay, rejecting truncated, reordered, or inconsistent records. These are
simulated reports, not evidence that an exchange order filled. Journals contain
no credentials.

Before live execution, venue adapters must normalize authoritative cumulative
reports, reconcile ambiguous outcomes against actual orders/fills/positions,
enforce each market's size/tick rules, use validated feeds and actual fee bounds,
and persist intents before dispatch. A continuously running supervisor must drive
timers even when no order reports arrive. Restart replay verifies the paper log;
it does not automatically resume or retry live orders. Cash reservation, portfolio
exposure limits, and live-trading authorization remain separate unfinished work.

## Before any live-trading work

1. Replay recorded signals against historical order-book snapshots.
2. Run in dry-run mode until fills, fees, slippage, and settlement mapping are reconciled.
3. Add venue adapters with keys stored outside the repository.
4. Add exchange-specific order/fill reconciliation and bounded emergency hedging.
5. Require an explicit live-mode gate in addition to risk limits.
