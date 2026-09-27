use crate::{model::{Decision, ExecutionPlan, Signal}, risk::{estimated_cost_cents, validate, RiskLimits}};

#[derive(Debug, Clone)]
pub struct ExecutionEngine {
    limits: RiskLimits,
}

impl ExecutionEngine {
    pub fn new(limits: RiskLimits) -> Self {
        Self { limits }
    }

    /// Produces a dry-run plan only. Live order submission is intentionally
    /// absent until venue adapters and reconciliation are independently tested.
    pub async fn evaluate(&self, signal: Signal) -> ExecutionPlan {
        let estimated_cost_cents = estimated_cost_cents(&signal);
        let decision = match validate(&signal, &self.limits) {
            Ok(_) => Decision::DryRunApproved,
            Err(reason) => Decision::Rejected { reason },
        };
        ExecutionPlan {
            signal_id: signal.signal_id,
            event_key: signal.event_key,
            estimated_cost_cents,
            decision,
        }
    }
}
