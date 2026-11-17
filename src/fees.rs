//! Fee estimates for whole-contract, standard binary taker orders.
//! L2 data cannot reproduce the exchange's individual fills or account ledger.
use chrono::{DateTime, Datelike, Duration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const POLY_CONSERVATIVE_RATE: f64 = 0.095;

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(tag = "model", rename_all = "snake_case")]
pub enum FeeModel {
    KalshiQuadratic {
        coefficient: f64,
        balance_precision: f64,
    },
    PolymarketStandard {
        coefficient: f64,
    },
}

impl FeeModel {
    pub fn exact(self, price: f64, contracts: u64) -> f64 {
        let coefficient = match self {
            Self::KalshiQuadratic { coefficient, .. }
            | Self::PolymarketStandard { coefficient } => coefficient,
        };
        coefficient * contracts as f64 * price * (1.0 - price)
    }

    pub fn rounded(self, cost: f64, exact_fee: f64) -> f64 {
        match self {
            Self::PolymarketStandard { .. } => bankers_round(exact_fee),
            Self::KalshiQuadratic {
                balance_precision, ..
            } => {
                // Apply the balance grid to cost + trade fee, not the fee alone.
                // This estimates one order's fee accumulator across L2 levels.
                let trade_fee = ceil_grid(exact_fee, 0.000001);
                (ceil_grid(cost + trade_fee, balance_precision) - cost).max(0.0)
            }
        }
    }
}

fn ceil_grid(value: f64, grid: f64) -> f64 {
    (value / grid - 1e-8).ceil() * grid
}

pub fn bankers_round(value: f64) -> f64 {
    let cents = value * 100.0;
    let lower = cents.floor();
    let rounded = if (cents - lower - 0.5).abs() < 1e-9 {
        if (lower as i64) % 2 == 0 {
            lower
        } else {
            lower + 1.0
        }
    } else {
        cents.round()
    };
    rounded / 100.0
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FeePolicy {
    pub polymarket_account_rate: Option<AccountRate>,
    pub kalshi_balance_precision: Option<f64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountRate {
    pub coefficient: f64,
    pub verified_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub evidence: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct ResolvedPolicy {
    pub polymarket_coefficient: f64,
    pub polymarket_basis: String,
    pub kalshi_balance_precision: f64,
    pub expires_at: DateTime<Utc>,
}

fn eastern_offset(utc: DateTime<Utc>) -> i64 {
    let year = utc.year();
    let sunday = |month: u32, nth: u32| {
        let first = NaiveDate::from_ymd_opt(year, month, 1).unwrap();
        let day = 1 + (7 - first.weekday().num_days_from_sunday()) % 7 + 7 * (nth - 1);
        first.with_day(day).unwrap()
    };
    let start = sunday(3, 2).and_hms_opt(7, 0, 0).unwrap().and_utc();
    let end = sunday(11, 1).and_hms_opt(6, 0, 0).unwrap().and_utc();
    if utc >= start && utc < end { -4 } else { -5 }
}

fn eastern_date(utc: DateTime<Utc>) -> NaiveDate {
    (utc + Duration::hours(eastern_offset(utc))).date_naive()
}

fn next_eastern_midnight(now: DateTime<Utc>) -> DateTime<Utc> {
    let next = eastern_date(now)
        .succ_opt()
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc();
    let guess = next + Duration::hours(5);
    next - Duration::hours(eastern_offset(guess))
}

impl FeePolicy {
    pub fn resolve(&self, now: DateTime<Utc>) -> Result<ResolvedPolicy, String> {
        let precision = self.kalshi_balance_precision.unwrap_or(0.01);
        if precision != 0.01 && precision != 0.0001 {
            return Err("kalshi_balance_precision must be 0.01 (conservative) or 0.0001 (verified direct member)".into());
        }
        let (coefficient, basis, expires_at) = match &self.polymarket_account_rate {
            None => (
                POLY_CONSERVATIVE_RATE,
                "conservative published tier; account tier unverified".into(),
                now + Duration::minutes(10),
            ),
            Some(rate) => {
                if !rate.coefficient.is_finite()
                    || !(0.0..=1.0).contains(&rate.coefficient)
                    || rate.verified_at > now
                    || rate.expires_at <= now
                    || rate.expires_at <= rate.verified_at
                    || rate.expires_at - rate.verified_at > Duration::hours(24)
                    || rate.evidence.trim().is_empty()
                    || eastern_date(rate.verified_at) != eastern_date(now)
                {
                    return Err("invalid or expired Polymarket account fee rate: require coefficient 0..1, evidence, and a current verification valid for at most 24 hours".into());
                }
                (
                    rate.coefficient,
                    format!("operator-verified account rate: {}", rate.evidence),
                    rate.expires_at.min(next_eastern_midnight(now)),
                )
            }
        };
        Ok(ResolvedPolicy {
            polymarket_coefficient: coefficient,
            polymarket_basis: basis,
            kalshi_balance_precision: precision,
            expires_at,
        })
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct LegFees {
    pub kalshi: FeeModel,
    pub polymarket: FeeModel,
    pub basis: Value,
    pub expires_at: DateTime<Utc>,
}

impl LegFees {
    pub fn from_series(
        series: &Value,
        changes: &Value,
        policy: &ResolvedPolicy,
        now: DateTime<Utc>,
    ) -> Result<Self, String> {
        let fee_type = series["fee_type"]
            .as_str()
            .ok_or("Kalshi fee type missing")?;
        if !matches!(
            fee_type,
            "quadratic" | "quadratic_with_maker_fees" | "quadratic_with_combo_maker_fees"
        ) {
            return Err(format!("unsupported Kalshi fee type: {fee_type}"));
        }
        let multiplier = series["fee_multiplier"]
            .as_f64()
            .filter(|m| m.is_finite() && *m >= 0.0 && *m <= 100.0)
            .ok_or("invalid Kalshi fee multiplier")?;
        let mut expires_at = policy.expires_at.min(now + Duration::minutes(10));
        for change in changes["series_fee_change_arr"]
            .as_array()
            .ok_or("Kalshi fee-change response missing")?
        {
            let when = change["scheduled_ts"]
                .as_str()
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .ok_or("invalid fee-change timestamp")?
                .with_timezone(&Utc);
            // No historical changes are requested. Refuse a transition already due
            // rather than assuming the series endpoint has caught up.
            if when <= now {
                return Err("Kalshi fee transition requires a fresh series response".into());
            }
            expires_at = expires_at.min(when);
        }
        if expires_at <= now {
            return Err("fee policy expired".into());
        }
        Ok(Self {
            kalshi: FeeModel::KalshiQuadratic {
                coefficient: 0.07 * multiplier,
                balance_precision: policy.kalshi_balance_precision,
            },
            polymarket: FeeModel::PolymarketStandard {
                coefficient: policy.polymarket_coefficient,
            },
            basis: json!({"kalshi_series": series["ticker"], "kalshi_fee_type": fee_type, "kalshi_fee_multiplier": multiplier, "account_policy": policy, "rounding": "order-wide cumulative L2 estimate; individual fill fragmentation and ledger rebates are not reconstructed", "polymarket_rounding": "bankers-rounded cumulative exact fee cap", "sources": ["https://docs.polymarket.us/fees", "https://docs.kalshi.com/getting_started/fee_rounding", "https://kalshi.com/docs/kalshi-fee-schedule.pdf"]}),
            expires_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn account_rates_stop_at_eastern_midnight_including_dst_changes() {
        for (now, midnight) in [
            ("2026-11-16T23:00:00Z", "2026-11-17T05:00:00Z"),
            ("2026-07-01T23:00:00Z", "2026-07-02T04:00:00Z"),
            ("2026-03-08T05:30:00Z", "2026-03-09T04:00:00Z"),
            ("2026-11-01T04:30:00Z", "2026-11-02T05:00:00Z"),
        ] {
            let now = DateTime::parse_from_rfc3339(now)
                .unwrap()
                .with_timezone(&Utc);
            let expected = DateTime::parse_from_rfc3339(midnight)
                .unwrap()
                .with_timezone(&Utc);
            assert_eq!(next_eastern_midnight(now), expected);
            let p = FeePolicy {
                polymarket_account_rate: Some(AccountRate {
                    coefficient: 0.0695,
                    verified_at: now,
                    expires_at: now + Duration::hours(24),
                    evidence: "account fee screen".into(),
                }),
                ..Default::default()
            };
            assert_eq!(
                p.resolve(now).unwrap().expires_at,
                expected.min(now + Duration::hours(24))
            );
            assert!(p.resolve(expected + Duration::seconds(1)).is_err());
        }
    }

    #[test]
    fn published_poly_examples_and_ties() {
        let model = FeeModel::PolymarketStandard {
            coefficient: 0.0695,
        };
        for (price, fee) in [
            (0.1, 0.63),
            (0.65, 1.58),
            (0.3, 1.46),
            (0.9, 0.63),
            (0.5, 1.74),
        ] {
            assert!((model.rounded(price * 100.0, model.exact(price, 100)) - fee).abs() < 1e-9);
        }
        assert_eq!(bankers_round(0.005), 0.0);
        assert_eq!(bankers_round(0.015), 0.02);
        assert_eq!(bankers_round(0.025), 0.02);
    }
    #[test]
    fn defaults_are_conservative_and_overrides_expire() {
        let now = Utc::now();
        let defaults = FeePolicy::default().resolve(now).unwrap();
        assert_eq!(defaults.polymarket_coefficient, 0.095);
        assert_eq!(defaults.kalshi_balance_precision, 0.01);
        for (verified_at, expires_at) in [
            (now, now),
            (now, now + Duration::hours(25)),
            (now + Duration::hours(1), now + Duration::hours(2)),
        ] {
            let p = FeePolicy {
                polymarket_account_rate: Some(AccountRate {
                    coefficient: 0.0695,
                    verified_at,
                    expires_at,
                    evidence: "account fee screen".into(),
                }),
                ..Default::default()
            };
            assert!(p.resolve(now).is_err());
        }
    }
    #[test]
    fn series_multiplier_transition_and_unknown_models() {
        let now = Utc::now();
        let p = FeePolicy::default().resolve(now).unwrap();
        let mut series = json!({"ticker":"KXTEST", "fee_type":"quadratic", "fee_multiplier":2});
        let change = now + Duration::seconds(90);
        let fee = LegFees::from_series(
            &series,
            &json!({"series_fee_change_arr":[{"scheduled_ts":change}]}),
            &p,
            now,
        )
        .unwrap();
        assert_eq!(fee.expires_at, change);
        assert!((fee.kalshi.exact(0.5, 100) - 3.5).abs() < 1e-9);
        assert!((fee.kalshi.rounded(0.5001, 0.0175) - 0.0199).abs() < 1e-9);
        series["fee_type"] = json!("flat");
        assert!(
            LegFees::from_series(&series, &json!({"series_fee_change_arr":[]}), &p, now).is_err()
        );
    }
}
