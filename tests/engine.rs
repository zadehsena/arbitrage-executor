use arbitrage_executor::{engine::ExecutionEngine, model::{Decision, Leg, Side, Signal, Venue}, risk::RiskLimits};

fn signal() -> Signal {
    Signal {
        signal_id: "test-1".into(),
        event_key: "event-1".into(),
        expected_edge_bps: 150,
        max_total_cost_cents: 1_000,
        legs: vec![
            Leg { venue: Venue::Kalshi, market_id: "KX-YES".into(), outcome: "yes".into(), side: Side::Buy, limit_price_cents: 49, quantity: 10 },
            Leg { venue: Venue::PolymarketUs, market_id: "PM-NO".into(), outcome: "no".into(), side: Side::Buy, limit_price_cents: 49, quantity: 10 },
        ],
    }
}

#[tokio::test]
async fn approves_a_bounded_two_venue_dry_run() {
    let plan = ExecutionEngine::new(RiskLimits::default()).evaluate(signal()).await;
    assert_eq!(plan.decision, Decision::DryRunApproved);
    assert_eq!(plan.estimated_cost_cents, 980);
}

#[tokio::test]
async fn rejects_a_signal_below_the_edge_floor() {
    let mut candidate = signal();
    candidate.expected_edge_bps = 99;
    let plan = ExecutionEngine::new(RiskLimits::default()).evaluate(candidate).await;
    assert!(matches!(plan.decision, Decision::Rejected { .. }));
}
