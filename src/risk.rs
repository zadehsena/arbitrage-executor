use crate::model::Signal;

#[derive(Debug, Clone)]
pub struct RiskLimits {
    pub minimum_edge_bps: u32,
    pub maximum_legs: usize,
    pub maximum_contracts_per_leg: u32,
    pub maximum_total_cost_cents: u64,
}

impl Default for RiskLimits {
    fn default() -> Self {
        Self {
            minimum_edge_bps: 100,
            maximum_legs: 2,
            maximum_contracts_per_leg: 10,
            maximum_total_cost_cents: 1_000,
        }
    }
}

pub fn estimated_cost_cents(signal: &Signal) -> u64 {
    signal.legs.iter().map(|leg| u64::from(leg.limit_price_cents) * u64::from(leg.quantity)).sum()
}

pub fn validate(signal: &Signal, limits: &RiskLimits) -> Result<u64, String> {
    if signal.signal_id.trim().is_empty() || signal.event_key.trim().is_empty() {
        return Err("signal_id and event_key are required".into());
    }
    if signal.legs.len() != 2 {
        return Err("exactly two legs are required for a cross-venue dry run".into());
    }
    if signal.legs.len() > limits.maximum_legs {
        return Err("signal exceeds the maximum number of legs".into());
    }
    if signal.expected_edge_bps < limits.minimum_edge_bps {
        return Err("expected edge is below the configured minimum".into());
    }
    if signal.legs.iter().any(|leg| leg.market_id.trim().is_empty() || leg.outcome.trim().is_empty()) {
        return Err("each leg requires a market ID and outcome".into());
    }
    if signal.legs.iter().any(|leg| leg.limit_price_cents == 0 || leg.limit_price_cents >= 100) {
        return Err("limit prices must be between 1 and 99 cents".into());
    }
    if signal.legs.iter().any(|leg| leg.quantity == 0 || leg.quantity > limits.maximum_contracts_per_leg) {
        return Err("leg quantity exceeds the configured limit".into());
    }
    if signal.legs[0].venue == signal.legs[1].venue {
        return Err("legs must use different venues".into());
    }

    let cost = estimated_cost_cents(signal);
    if cost > limits.maximum_total_cost_cents || cost > signal.max_total_cost_cents {
        return Err("estimated cost exceeds the configured or signal limit".into());
    }
    Ok(cost)
}
