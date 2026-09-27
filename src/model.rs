use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Venue {
    Kalshi,
    PolymarketUs,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Leg {
    pub venue: Venue,
    pub market_id: String,
    pub outcome: String,
    pub side: Side,
    pub limit_price_cents: u8,
    pub quantity: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Signal {
    pub signal_id: String,
    pub event_key: String,
    pub expected_edge_bps: u32,
    pub max_total_cost_cents: u64,
    pub legs: Vec<Leg>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    DryRunApproved,
    Rejected { reason: String },
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ExecutionPlan {
    pub signal_id: String,
    pub event_key: String,
    pub estimated_cost_cents: u64,
    pub decision: Decision,
}
