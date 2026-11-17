//! Deterministic PAPER execution supervisor. Actions are intents, never API calls.
//! Confirmed cumulative reports drive positions; a cancel request never closes an
//! order, and an unknown outcome never authorizes a replacement order.
use serde::{Deserialize, Serialize};

pub const CONTRACT_SCALE: u64 = 1_000_000;
const PRICE_SCALE: u128 = 10_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Venue {
    Kalshi,
    Polymarket,
}
impl Venue {
    fn index(self) -> usize {
        if self == Self::Kalshi { 0 } else { 1 }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Submitted,
    Working,
    Unknown,
    Filled,
    Canceled,
    Rejected,
}
impl Status {
    fn terminal(self) -> bool {
        matches!(self, Self::Filled | Self::Canceled | Self::Rejected)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Ready,
    EntryPending,
    Canceling,
    AwaitingExitQuote,
    Exiting,
    Hedged,
    Flat,
    Halted,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub id: String,
    pub quantity_micros: u64,
    pub entry_limit_ticks: [u32; 2],
    pub entry_fee_reserve_microusd: [u64; 2],
    pub entry_timeout_ms: u64,
    pub max_unhedged_ms: u64,
    pub max_recovery_ms: u64,
    pub max_quote_age_ms: u64,
    pub max_loss_microusd: u64,
    pub max_exit_attempts: u8,
}
impl Plan {
    pub fn validate(&self) -> Result<(), String> {
        if self.id.is_empty()
            || self.id.len() > 64
            || !self
                .id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-')
            || self.quantity_micros == 0
            || self.quantity_micros > 1_000_000 * CONTRACT_SCALE
            || self
                .entry_limit_ticks
                .iter()
                .any(|p| !(1..10000).contains(p))
            || self.entry_timeout_ms == 0
            || self.max_unhedged_ms == 0
            || self.max_recovery_ms == 0
            || self.max_quote_age_ms == 0
            || self.max_exit_attempts == 0
            || self.max_exit_attempts > 10
            || self.max_loss_microusd == 0
            || self
                .entry_fee_reserve_microusd
                .iter()
                .any(|f| *f > self.quantity_micros)
        {
            return Err("invalid paper plan: require bounded positive quantities, prices, timeouts, fee reserves, and exit limits".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Report {
    pub client_order_id: String,
    pub revision: u64,
    pub status: Status,
    pub filled_micros: u64,
    pub notional_microusd: u64,
    pub fees_microusd: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExitQuote {
    pub venue: Venue,
    pub observed_at_ms: u64,
    pub bid_ticks: u32,
    pub available_micros: u64,
    pub fee_bound_microusd: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Event {
    Start,
    Report { report: Report },
    Tick,
    Disconnect,
    Stop,
    ExitQuote { quote: ExitQuote },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimedEvent {
    pub now_ms: u64,
    pub event: Event,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Action {
    SubmitIoc {
        client_order_id: String,
        venue: Venue,
        side: Side,
        quantity_micros: u64,
        limit_ticks: u32,
        fee_bound_microusd: u64,
    },
    Cancel {
        client_order_id: String,
    },
    Reconcile {
        client_order_id: String,
    },
    NeedExitQuote {
        venue: Venue,
        quantity_micros: u64,
    },
    Alert {
        reason: String,
        positions_micros: [u64; 2],
    },
}

#[derive(Clone, Debug, Serialize)]
struct Order {
    id: String,
    venue: Venue,
    side: Side,
    quantity: u64,
    limit: u32,
    fee_bound: u64,
    report: Option<Report>,
    cancel_requested: bool,
    reconcile_requested: bool,
}
impl Order {
    fn filled(&self) -> u64 {
        self.report.as_ref().map_or(0, |r| r.filled_micros)
    }
    fn terminal(&self) -> bool {
        self.report.as_ref().is_some_and(|r| r.status.terminal())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Summary {
    pub phase: Phase,
    pub positions_micros: [u64; 2],
    pub unhedged_micros: u64,
    pub realized_loss_microusd: u64,
    pub exit_attempts: u8,
    pub open_or_unknown_orders: usize,
}
#[derive(Clone, Debug, Serialize)]
pub struct Controller {
    plan: Plan,
    phase: Phase,
    orders: Vec<Order>,
    started_at: Option<u64>,
    last_ms: Option<u64>,
    unhedged_since: Option<u64>,
    recovery_started: Option<u64>,
    exit_attempts: u8,
}

fn ceil_div(a: u128, b: u128) -> u128 {
    a.div_ceil(b)
}
fn notional(qty: u64, price: u32) -> u64 {
    (qty as u128 * price as u128 / PRICE_SCALE) as u64
}
impl Controller {
    pub fn new(plan: Plan) -> Result<Self, String> {
        plan.validate()?;
        Ok(Self {
            plan,
            phase: Phase::Ready,
            orders: vec![],
            started_at: None,
            last_ms: None,
            unhedged_since: None,
            recovery_started: None,
            exit_attempts: 0,
        })
    }
    pub fn summary(&self) -> Summary {
        let positions = self.positions();
        Summary {
            phase: self.phase,
            positions_micros: positions,
            unhedged_micros: positions[0].abs_diff(positions[1]),
            realized_loss_microusd: self.loss(),
            exit_attempts: self.exit_attempts,
            open_or_unknown_orders: self.orders.iter().filter(|o| !o.terminal()).count(),
        }
    }
    fn totals(&self, venue: Venue, side: Side) -> (u64, u64, u64) {
        self.orders
            .iter()
            .filter(|o| o.venue == venue && o.side == side)
            .fold((0, 0, 0), |(q, n, f), o| {
                if let Some(r) = &o.report {
                    (
                        q.saturating_add(r.filled_micros),
                        n.saturating_add(r.notional_microusd),
                        f.saturating_add(r.fees_microusd),
                    )
                } else {
                    (q, n, f)
                }
            })
    }
    fn positions(&self) -> [u64; 2] {
        [Venue::Kalshi, Venue::Polymarket].map(|v| {
            self.totals(v, Side::Buy)
                .0
                .saturating_sub(self.totals(v, Side::Sell).0)
        })
    }
    fn loss_after(&self, venue: Venue, more_qty: u64, more_revenue: u64, more_fee: u64) -> u64 {
        let (bought, spent, entry_fee) = self.totals(venue, Side::Buy);
        let (sold, revenue, exit_fee) = self.totals(venue, Side::Sell);
        if bought == 0 {
            return more_fee;
        }
        let basis = ceil_div(
            (spent as u128 + entry_fee as u128) * (sold as u128 + more_qty as u128),
            bought as u128,
        );
        (basis + exit_fee as u128 + more_fee as u128)
            .saturating_sub(revenue as u128 + more_revenue as u128)
            .min(u64::MAX as u128) as u64
    }
    fn loss(&self) -> u64 {
        self.loss_after(Venue::Kalshi, 0, 0, 0)
            .saturating_add(self.loss_after(Venue::Polymarket, 0, 0, 0))
    }
    fn alert(&self, reason: impl Into<String>) -> Action {
        Action::Alert {
            reason: reason.into(),
            positions_micros: self.positions(),
        }
    }
    fn cancel_and_reconcile(&mut self, actions: &mut Vec<Action>) {
        for order in &mut self.orders {
            if !order.terminal() {
                if !order.cancel_requested {
                    actions.push(Action::Cancel {
                        client_order_id: order.id.clone(),
                    });
                    order.cancel_requested = true;
                }
                if !order.reconcile_requested {
                    actions.push(Action::Reconcile {
                        client_order_id: order.id.clone(),
                    });
                    order.reconcile_requested = true;
                }
            }
        }
    }
    fn recover(&mut self, now: u64, actions: &mut Vec<Action>) {
        if self.phase != Phase::Halted {
            self.phase = Phase::Canceling;
        }
        self.recovery_started.get_or_insert(now);
        self.cancel_and_reconcile(actions);
    }
    fn submit(
        &mut self,
        id: String,
        venue: Venue,
        side: Side,
        quantity: u64,
        limit: u32,
        fee_bound: u64,
        actions: &mut Vec<Action>,
    ) {
        actions.push(Action::SubmitIoc {
            client_order_id: id.clone(),
            venue,
            side,
            quantity_micros: quantity,
            limit_ticks: limit,
            fee_bound_microusd: fee_bound,
        });
        self.orders.push(Order {
            id,
            venue,
            side,
            quantity,
            limit,
            fee_bound,
            report: None,
            cancel_requested: false,
            reconcile_requested: false,
        });
    }
    /// Caller must persist the event and resulting state/actions before dispatch.
    /// This repository's paper executor only journals actions; it cannot trade.
    pub fn apply(&mut self, input: &TimedEvent) -> Vec<Action> {
        let mut actions = vec![];
        if self.last_ms.is_some_and(|last| input.now_ms < last) {
            self.phase = Phase::Halted;
            self.cancel_and_reconcile(&mut actions);
            actions.push(self.alert("execution clock regressed"));
            return actions;
        }
        self.last_ms = Some(input.now_ms);
        let result = self.process(input, &mut actions).and_then(|_| {
            if !matches!(input.event, Event::Tick) {
                self.process(
                    &TimedEvent {
                        now_ms: input.now_ms,
                        event: Event::Tick,
                    },
                    &mut actions,
                )
            } else {
                Ok(())
            }
        });
        if let Err(reason) = result {
            self.phase = Phase::Halted;
            self.cancel_and_reconcile(&mut actions);
            actions.push(self.alert(reason));
        }
        actions
    }
    fn process(&mut self, input: &TimedEvent, actions: &mut Vec<Action>) -> Result<(), String> {
        let now = input.now_ms;
        match &input.event {
            Event::Start => {
                if self.started_at.is_some() {
                    return Ok(());
                } // Idempotent start.
                if self.phase == Phase::Halted {
                    return Ok(());
                }
                self.started_at = Some(now);
                self.phase = Phase::EntryPending;
                for venue in [Venue::Kalshi, Venue::Polymarket] {
                    let i = venue.index();
                    self.submit(
                        format!("{}-entry-{i}", self.plan.id),
                        venue,
                        Side::Buy,
                        self.plan.quantity_micros,
                        self.plan.entry_limit_ticks[i],
                        self.plan.entry_fee_reserve_microusd[i],
                        actions,
                    );
                }
            }
            Event::Report { report } => {
                let order = self
                    .orders
                    .iter_mut()
                    .find(|o| o.id == report.client_order_id)
                    .ok_or(
                        "report for unknown client order ID; reconcile account before continuing",
                    )?;
                if let Some(previous) = &order.report {
                    if report.revision < previous.revision {
                        if report.filled_micros > previous.filled_micros {
                            return Err("out-of-order report contradicts known fills".into());
                        }
                        return Ok(());
                    }
                    if report.revision == previous.revision {
                        return if previous == report {
                            Ok(())
                        } else {
                            Err("conflicting duplicate order report".into())
                        };
                    }
                    if report.filled_micros < previous.filled_micros
                        || report.notional_microusd < previous.notional_microusd
                        || report.fees_microusd < previous.fees_microusd
                    {
                        return Err("cumulative fills, costs, or fees regressed; manual reconciliation required".into());
                    }
                    if previous.status.terminal() && !report.status.terminal() {
                        return Err("terminal order became active again".into());
                    }
                }
                if report.filled_micros > order.quantity
                    || report.notional_microusd > report.filled_micros
                    || (report.status == Status::Filled && report.filled_micros != order.quantity)
                    || (report.filled_micros == 0 && report.notional_microusd != 0)
                    || (report.filled_micros > 0 && report.notional_microusd == 0)
                {
                    return Err("invalid cumulative fill report".into());
                }
                let terminal_changed = order.terminal() && report.filled_micros != order.filled();
                let bad_limit = match order.side {
                    Side::Buy => {
                        report.notional_microusd as u128
                            > ceil_div(
                                report.filled_micros as u128 * order.limit as u128,
                                PRICE_SCALE,
                            )
                    }
                    Side::Sell => {
                        report.notional_microusd < notional(report.filled_micros, order.limit)
                    }
                };
                let over_fee = report.fees_microusd > order.fee_bound;
                order.report = Some(report.clone()); // Preserve confirmed exposure even on a risk breach.
                if bad_limit || over_fee {
                    return Err("confirmed fill breached limit price or reserved fee bound".into());
                }
                if terminal_changed || report.status == Status::Unknown {
                    self.recover(now, actions);
                }
                if report.status == Status::Rejected || report.status == Status::Canceled {
                    self.recover(now, actions);
                }
            }
            Event::Disconnect => {
                if self.started_at.is_some() {
                    self.recover(now, actions);
                }
            }
            Event::Stop => {
                self.phase = Phase::Halted;
                self.cancel_and_reconcile(actions);
                actions.push(self.alert("operator stop; unresolved orders require reconciliation"));
            }
            Event::Tick => {}
            Event::ExitQuote { quote } => {
                if self.phase != Phase::AwaitingExitQuote {
                    return Ok(());
                }
                let positions = self.positions();
                let venue = if positions[0] > positions[1] {
                    Venue::Kalshi
                } else {
                    Venue::Polymarket
                };
                if quote.venue != venue
                    || quote.observed_at_ms > now
                    || now - quote.observed_at_ms > self.plan.max_quote_age_ms
                    || !(1..10000).contains(&quote.bid_ticks)
                    || quote.available_micros == 0
                {
                    actions.push(self.alert(
                        "exit quote is stale, invalid, or for the wrong venue; no exit sent",
                    ));
                    return Ok(());
                }
                // Expiry is checked before issuing an action, including quotes
                // arriving after the recovery deadline without an intervening tick.
                if self
                    .recovery_started
                    .is_some_and(|t| now.saturating_sub(t) >= self.plan.max_recovery_ms)
                {
                    return Err("recovery deadline exceeded; exposure remains".into());
                }
                if self.exit_attempts >= self.plan.max_exit_attempts {
                    return Err("emergency exit attempt limit reached; exposure remains".into());
                }
                let qty = positions[0]
                    .abs_diff(positions[1])
                    .min(quote.available_micros);
                let other = if venue == Venue::Kalshi {
                    Venue::Polymarket
                } else {
                    Venue::Kalshi
                };
                let worst_loss = self
                    .loss_after(
                        venue,
                        qty,
                        notional(qty, quote.bid_ticks),
                        quote.fee_bound_microusd,
                    )
                    .saturating_add(self.loss_after(other, 0, 0, 0));
                if worst_loss > self.plan.max_loss_microusd {
                    return Err("exit would exceed maximum loss; exposure remains and requires intervention".into());
                }
                self.exit_attempts += 1;
                self.phase = Phase::Exiting;
                self.submit(
                    format!("{}-exit-{}", self.plan.id, self.exit_attempts),
                    venue,
                    Side::Sell,
                    qty,
                    quote.bid_ticks,
                    quote.fee_bound_microusd,
                    actions,
                );
            }
        }
        if self.started_at.is_none() || self.phase == Phase::Halted {
            return Ok(());
        }
        let positions = self.positions();
        if positions[0] != positions[1] {
            self.unhedged_since.get_or_insert(now);
        } else {
            self.unhedged_since = None;
        }
        if self.loss() > self.plan.max_loss_microusd {
            return Err("realized recovery loss exceeded limit; halt".into());
        }
        if self.orders.iter().any(|o| !o.terminal()) {
            let entry_timeout = self.phase == Phase::EntryPending
                && now.saturating_sub(self.started_at.unwrap()) >= self.plan.entry_timeout_ms;
            let unhedged_timeout = self.phase == Phase::EntryPending
                && self
                    .unhedged_since
                    .is_some_and(|t| now.saturating_sub(t) >= self.plan.max_unhedged_ms);
            if entry_timeout || unhedged_timeout {
                self.recover(now, actions);
            }
            if self
                .recovery_started
                .is_some_and(|t| now.saturating_sub(t) >= self.plan.max_recovery_ms)
            {
                return Err(
                    "recovery deadline exceeded with unknown/open orders; no replacement sent"
                        .into(),
                );
            }
            return Ok(());
        }
        if positions[0] == positions[1] {
            self.phase = if positions[0] == 0 {
                Phase::Flat
            } else {
                Phase::Hedged
            };
            return Ok(());
        }
        self.recovery_started.get_or_insert(now);
        if now.saturating_sub(self.recovery_started.unwrap()) >= self.plan.max_recovery_ms {
            return Err("recovery deadline exceeded; exposure remains".into());
        }
        if self.exit_attempts >= self.plan.max_exit_attempts {
            return Err("emergency exit attempt limit reached; exposure remains".into());
        }
        if self.phase != Phase::AwaitingExitQuote {
            self.phase = Phase::AwaitingExitQuote;
            actions.push(Action::NeedExitQuote {
                venue: if positions[0] > positions[1] {
                    Venue::Kalshi
                } else {
                    Venue::Polymarket
                },
                quantity_micros: positions[0].abs_diff(positions[1]),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn controller() -> Controller {
        Controller::new(Plan {
            id: "test".into(),
            quantity_micros: 10 * CONTRACT_SCALE,
            entry_limit_ticks: [4000, 5000],
            entry_fee_reserve_microusd: [30000, 30000],
            entry_timeout_ms: 1000,
            max_unhedged_ms: 200,
            max_recovery_ms: 1500,
            max_quote_age_ms: 100,
            max_loss_microusd: 200000,
            max_exit_attempts: 2,
        })
        .unwrap()
    }
    fn apply(c: &mut Controller, now_ms: u64, event: Event) -> Vec<Action> {
        c.apply(&TimedEvent { now_ms, event })
    }
    fn report(id: &str, status: Status, qty: u64, cost: u64) -> Event {
        Event::Report {
            report: Report {
                client_order_id: id.into(),
                revision: 1,
                status,
                filled_micros: qty,
                notional_microusd: cost,
                fees_microusd: 0,
            },
        }
    }
    fn quote(now: u64, qty: u64, bid_ticks: u32) -> Event {
        Event::ExitQuote {
            quote: ExitQuote {
                venue: Venue::Kalshi,
                observed_at_ms: now,
                bid_ticks,
                available_micros: qty,
                fee_bound_microusd: 10000,
            },
        }
    }
    fn submit_count(actions: &[Action]) -> usize {
        actions
            .iter()
            .filter(|a| matches!(a, Action::SubmitIoc { .. }))
            .count()
    }
    fn one_sided() -> Controller {
        let mut c = controller();
        apply(&mut c, 0, Event::Start);
        apply(
            &mut c,
            10,
            report("test-entry-0", Status::Filled, 10_000_000, 4_000_000),
        );
        apply(&mut c, 20, report("test-entry-1", Status::Rejected, 0, 0));
        c
    }
    #[test]
    fn matched_fills_and_start_retries_do_not_duplicate_orders() {
        let mut c = controller();
        assert_eq!(submit_count(&apply(&mut c, 0, Event::Start)), 2);
        assert_eq!(submit_count(&apply(&mut c, 1, Event::Start)), 0);
        let first = report("test-entry-0", Status::Filled, 10_000_000, 4_000_000);
        apply(&mut c, 10, first.clone());
        apply(&mut c, 11, first);
        apply(
            &mut c,
            20,
            report("test-entry-1", Status::Filled, 10_000_000, 5_000_000),
        );
        assert_eq!(c.summary().phase, Phase::Hedged);
        assert_eq!(c.summary().unhedged_micros, 0);
    }
    #[test]
    fn partial_hedge_only_unwinds_the_excess() {
        let mut c = controller();
        apply(&mut c, 0, Event::Start);
        apply(
            &mut c,
            10,
            report("test-entry-0", Status::Filled, 10_000_000, 4_000_000),
        );
        let actions = apply(
            &mut c,
            20,
            report("test-entry-1", Status::Canceled, 6_000_000, 3_000_000),
        );
        assert!(actions.contains(&Action::NeedExitQuote {
            venue: Venue::Kalshi,
            quantity_micros: 4_000_000
        }));
        let actions = apply(&mut c, 30, quote(30, 10_000_000, 3800));
        assert!(actions.iter().any(|a| matches!(
            a,
            Action::SubmitIoc {
                quantity_micros: 4_000_000,
                side: Side::Sell,
                ..
            }
        )));
        apply(
            &mut c,
            40,
            report("test-exit-1", Status::Filled, 4_000_000, 1_520_000),
        );
        assert_eq!(c.summary().phase, Phase::Hedged);
        assert_eq!(c.summary().positions_micros, [6_000_000, 6_000_000]);
        assert_eq!(c.summary().realized_loss_microusd, 80000);
    }
    #[test]
    fn timeout_cancels_and_reconciles_before_any_exit() {
        let mut c = controller();
        apply(&mut c, 0, Event::Start);
        apply(
            &mut c,
            10,
            report("test-entry-0", Status::Filled, 10_000_000, 4_000_000),
        );
        let a = apply(&mut c, 211, Event::Tick);
        assert!(a.contains(&Action::Cancel {
            client_order_id: "test-entry-1".into()
        }));
        assert!(a.contains(&Action::Reconcile {
            client_order_id: "test-entry-1".into()
        }));
        assert_eq!(
            submit_count(&apply(&mut c, 220, quote(220, 10_000_000, 3900))),
            0
        );
        // The pending order fills during cancellation. Never sell the full
        // original leg or assume that a cancel request means no fill.
        apply(
            &mut c,
            230,
            report("test-entry-1", Status::Canceled, 8_000_000, 4_000_000),
        );
        let a = apply(&mut c, 240, quote(240, 10_000_000, 3900));
        assert!(a.iter().any(|a| matches!(
            a,
            Action::SubmitIoc {
                quantity_micros: 2_000_000,
                ..
            }
        )));
        assert!(!a.iter().any(|a| matches!(a, Action::Cancel { .. })));
    }
    #[test]
    fn unknown_results_never_trigger_blind_retries() {
        let mut c = controller();
        apply(&mut c, 0, Event::Start);
        apply(
            &mut c,
            10,
            report("test-entry-0", Status::Filled, 10_000_000, 4_000_000),
        );
        let a = apply(&mut c, 20, Event::Disconnect);
        assert_eq!(submit_count(&a), 0);
        assert_eq!(submit_count(&apply(&mut c, 30, Event::Disconnect)), 0);
        assert!(apply(&mut c, 40, Event::Tick).is_empty());
        let a = apply(&mut c, 1520, Event::Tick);
        assert!(a.iter().any(|a| matches!(a, Action::Alert { .. })));
        assert_eq!(c.summary().phase, Phase::Halted);
        assert_eq!(c.summary().positions_micros, [10_000_000, 0]);
        assert_eq!(
            submit_count(&apply(&mut c, 1530, quote(1530, 10_000_000, 3900))),
            0
        );
    }
    #[test]
    fn stale_quote_loss_budget_and_deadline_prevent_exit_submission() {
        let mut c = one_sided();
        assert_eq!(
            submit_count(&apply(&mut c, 200, quote(20, 10_000_000, 3900))),
            0
        );
        assert_eq!(c.summary().phase, Phase::AwaitingExitQuote);
        assert_eq!(
            submit_count(&apply(&mut c, 201, quote(201, 10_000_000, 1000))),
            0
        );
        assert_eq!(c.summary().phase, Phase::Halted);
        let mut c = one_sided();
        assert_eq!(
            submit_count(&apply(&mut c, 1520, quote(1520, 10_000_000, 3900))),
            0
        );
        assert_eq!(c.summary().phase, Phase::Halted);
    }
    #[test]
    fn rejected_and_partial_exits_have_bounded_attempts() {
        let mut c = one_sided();
        apply(&mut c, 30, quote(30, 10_000_000, 3900));
        apply(
            &mut c,
            40,
            report("test-exit-1", Status::Canceled, 5_000_000, 1_950_000),
        );
        let a = apply(&mut c, 50, quote(50, 10_000_000, 3900));
        assert!(a.iter().any(|a| matches!(
            a,
            Action::SubmitIoc {
                quantity_micros: 5_000_000,
                ..
            }
        )));
        apply(&mut c, 60, report("test-exit-2", Status::Rejected, 0, 0));
        assert_eq!(c.summary().phase, Phase::Halted);
        assert_eq!(c.summary().positions_micros, [5_000_000, 0]);
        assert_eq!(
            submit_count(&apply(&mut c, 70, quote(70, 10_000_000, 3900))),
            0
        );
    }
    #[test]
    fn fractional_fills_and_report_regressions_are_accounted_for() {
        let mut c = controller();
        apply(&mut c, 0, Event::Start);
        apply(
            &mut c,
            10,
            report("test-entry-0", Status::Canceled, 500_000, 200_000),
        );
        apply(&mut c, 20, report("test-entry-1", Status::Rejected, 0, 0));
        let a = apply(&mut c, 30, quote(30, 500_000, 3900));
        assert!(a.iter().any(|a| matches!(
            a,
            Action::SubmitIoc {
                quantity_micros: 500_000,
                ..
            }
        )));
        apply(
            &mut c,
            40,
            report("test-exit-1", Status::Filled, 500_000, 195_000),
        );
        assert_eq!(c.summary().phase, Phase::Flat);
        let bad = Event::Report {
            report: Report {
                client_order_id: "test-entry-0".into(),
                revision: 2,
                status: Status::Canceled,
                filled_micros: 400_000,
                notional_microusd: 160_000,
                fees_microusd: 0,
            },
        };
        apply(&mut c, 50, bad);
        assert_eq!(c.summary().phase, Phase::Halted);
    }
    #[test]
    fn late_fill_while_exiting_forces_reconciliation_before_replacement() {
        let mut c = one_sided();
        apply(&mut c, 30, quote(30, 10_000_000, 3900));
        let late = Event::Report {
            report: Report {
                client_order_id: "test-entry-1".into(),
                revision: 2,
                status: Status::Canceled,
                filled_micros: 2_000_000,
                notional_microusd: 1_000_000,
                fees_microusd: 0,
            },
        };
        let a = apply(&mut c, 40, late);
        assert_eq!(submit_count(&a), 0);
        assert!(a.contains(&Action::Cancel {
            client_order_id: "test-exit-1".into()
        }));
        assert_eq!(
            submit_count(&apply(&mut c, 50, quote(50, 10_000_000, 3900))),
            0
        );
    }
    #[test]
    fn confirmed_fee_breach_preserves_exposure_and_halts() {
        let mut c = controller();
        apply(&mut c, 0, Event::Start);
        apply(
            &mut c,
            10,
            Event::Report {
                report: Report {
                    client_order_id: "test-entry-0".into(),
                    revision: 1,
                    status: Status::Filled,
                    filled_micros: 10_000_000,
                    notional_microusd: 4_000_000,
                    fees_microusd: 100000,
                },
            },
        );
        assert_eq!(c.summary().phase, Phase::Halted);
        assert_eq!(c.summary().positions_micros, [10_000_000, 0]);
    }
}
