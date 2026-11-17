//! Explicit, expiring reviews of exact contract pairs. Text similarity is not
//! proof of complementary settlement, especially on cancellation/fair-price paths.
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

pub fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub fn select(value: &Value, fields: &[&str]) -> Value {
    Value::Object(
        fields
            .iter()
            .filter_map(|key| value.get(*key).map(|v| ((*key).into(), v.clone())))
            .collect(),
    )
}

#[derive(Clone, Debug, Serialize)]
pub struct RuleReview {
    pub fingerprint: String,
    pub evidence: Value,
    pub missing: Vec<String>,
    pub warnings: Vec<String>,
}

impl RuleReview {
    pub fn new(
        kalshi_market: &Value,
        series: &Value,
        terms_hash: Option<&str>,
        poly_market: &Value,
        poly_event: &Value,
        poly_long: bool,
    ) -> Self {
        let evidence = json!({
            "schema_version":1,
            "kalshi_market":select(kalshi_market, &["ticker", "event_ticker", "market_type", "notional_value_dollars", "title", "yes_sub_title", "no_sub_title", "rules_primary", "rules_secondary", "strike_type", "floor_strike", "cap_strike", "custom_strike", "settlement_bounds_type", "settlement_value_dollars", "early_close_condition", "expiration_time", "latest_expiration_time", "occurrence_datetime"]),
            "kalshi_series":select(series, &["ticker", "title", "contract_terms_url", "contract_url", "settlement_sources"]),
            "kalshi_terms_sha256":terms_hash,
            "polymarket_market":select(poly_market, &["slug", "question", "description", "sportsMarketType", "marketType", "line", "resolutionSource", "resolutionRules", "rules", "gameStartTime", "endDate", "outcomes", "assetPriceTerms"]),
            "polymarket_sides": poly_market["marketSides"].as_array().map(|sides| {
                let mut sides: Vec<_> = sides.iter().map(|side| {
                    let mut result = select(side, &["long", "description", "teamId", "playerId"]);
                    result["team"] = select(&side["team"], &["id", "name", "safeName", "league"]);
                    result["player"] = select(&side["player"], &["id", "name", "safeName"]);
                    result
                }).collect();
                sides.sort_by_key(|s| s["long"].as_bool()); sides
            }),
            "polymarket_event":select(poly_event, &["slug", "title", "startDate", "description", "resolutionSource"]),
            "polymarket_hedge_side": if poly_long { "long" } else { "short" },
        });
        let mut missing = Vec::new();
        for (label, value) in [
            ("Kalshi contract ticker", &kalshi_market["ticker"]),
            ("Polymarket market slug", &poly_market["slug"]),
            ("Kalshi primary rules", &kalshi_market["rules_primary"]),
            ("Polymarket market description", &poly_market["description"]),
            ("Kalshi terms URL", &series["contract_terms_url"]),
        ] {
            if value.as_str().is_none_or(|s| s.trim().is_empty()) {
                missing.push(label.into());
            }
        }
        let notional = kalshi_market["notional_value_dollars"]
            .as_f64()
            .or_else(|| {
                kalshi_market["notional_value_dollars"]
                    .as_str()?
                    .parse::<f64>()
                    .ok()
            });
        if kalshi_market["market_type"].as_str() != Some("binary") || notional != Some(1.0) {
            missing.push("verified $1 Kalshi binary contract notional".into());
        }
        let sides = poly_market["marketSides"].as_array();
        if !sides.is_some_and(|s| {
            s.len() == 2
                && s.iter()
                    .filter(|v| v["long"].as_bool() == Some(true))
                    .count()
                    == 1
                && s.iter()
                    .filter(|v| v["long"].as_bool() == Some(false))
                    .count()
                    == 1
        }) {
            missing.push("two explicit Polymarket long/short sides".into());
        }
        if terms_hash.is_none() {
            missing.push("downloaded Kalshi contract terms".into());
        }
        if series["settlement_sources"]
            .as_array()
            .is_none_or(|s| s.is_empty())
        {
            missing.push("Kalshi settlement sources".into());
        }
        let mut warnings = vec!["Manually compare every payout path; matching teams, scope, and line does not establish rule parity.".into()];
        let combined = format!(
            "{} {} {}",
            kalshi_market["rules_primary"],
            kalshi_market["rules_secondary"],
            poly_market["description"]
        )
        .to_lowercase();
        if combined.contains("fair price") || combined.contains("fair market price") {
            warnings.push("A fair-price settlement clause is present. Independently chosen venue settlement prices need not sum to $1; a normal winner hedge is not a guaranteed cancellation hedge.".into());
        }
        if combined.contains("postpon") || combined.contains("reschedul") {
            warnings.push("Compare postponement windows, original start-time definitions, and restart/abandonment treatment.".into());
        }
        Self {
            fingerprint: digest(&serde_json::to_vec(&evidence).expect("JSON evidence")),
            evidence,
            missing,
            warnings,
        }
    }

    pub fn decision(&self, approvals: &Approvals, now: DateTime<Utc>) -> ReviewDecision {
        let mut reasons = self.missing.clone();
        let approval = approvals
            .approvals
            .iter()
            .find(|a| a.fingerprint == self.fingerprint);
        if let Some(a) = approval {
            if !a.checks.complete() {
                reasons.push("all settlement checks must be explicitly true".into());
            }
            if a.reviewed_by.trim().is_empty()
                || a.notes.trim().is_empty()
                || a.sources.is_empty()
                || a.sources.iter().any(|s| s.trim().is_empty())
            {
                reasons.push("reviewer, reasoning, and sources are required".into());
            }
            if a.reviewed_at > now
                || a.expires_at <= now
                || a.expires_at <= a.reviewed_at
                || a.expires_at - a.reviewed_at > Duration::days(7)
            {
                reasons.push("review must be current and expire within seven days".into());
            }
        } else {
            reasons.push("no approval for these exact rules and hedge direction".into());
        }
        ReviewDecision {
            approved: reasons.is_empty(),
            reasons,
            expires_at: approval.map(|a| a.expires_at),
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Approvals {
    pub approvals: Vec<Approval>,
}

impl Approvals {
    pub fn validate(&self) -> Result<(), String> {
        let mut seen = std::collections::HashSet::new();
        if self.approvals.iter().any(|a| !seen.insert(&a.fingerprint)) {
            return Err("duplicate settlement approval fingerprint".into());
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Approval {
    pub fingerprint: String,
    pub reviewed_by: String,
    pub reviewed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub notes: String,
    pub sources: Vec<String>,
    pub checks: Checks,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checks {
    pub same_event_and_participants: bool,
    pub same_statistic_line_and_period: bool,
    pub overtime_and_ties: bool,
    pub postponement_and_rescheduling: bool,
    pub cancellation_void_and_fair_price: bool,
    pub abandonment_retirement_and_participation: bool,
    pub settlement_sources_and_corrections: bool,
    pub combined_payout_at_least_one_dollar_in_all_cases: bool,
}
impl Checks {
    fn complete(&self) -> bool {
        self.same_event_and_participants
            && self.same_statistic_line_and_period
            && self.overtime_and_ties
            && self.postponement_and_rescheduling
            && self.cancellation_void_and_fair_price
            && self.abandonment_retirement_and_participation
            && self.settlement_sources_and_corrections
            && self.combined_payout_at_least_one_dollar_in_all_cases
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ReviewDecision {
    pub approved: bool,
    pub reasons: Vec<String>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    fn review() -> RuleReview {
        RuleReview::new(
            &json!({"ticker":"K-A", "market_type":"binary", "notional_value_dollars":"1.0000", "rules_primary":"A wins", "rules_secondary":"Cancelled games settle at a fair price"}),
            &json!({"contract_terms_url":"https://assets.kalshi.com/terms.pdf", "settlement_sources":[{"name":"League"}]}),
            Some("abc"),
            &json!({"slug":"p", "description":"A wins, overtime included", "marketSides":[{"long":true},{"long":false}]}),
            &json!({"slug":"event"}),
            false,
        )
    }
    #[test]
    fn rules_hash_ignores_quotes_but_changes_with_rules_and_direction() {
        let r = review();
        assert!(r.warnings.iter().any(|w| w.contains("fair-price")));
        let mut changed = r.evidence.clone();
        changed["kalshi_market"]["rules_secondary"] = json!("New cancellation policy");
        assert_ne!(
            r.fingerprint,
            digest(&serde_json::to_vec(&changed).unwrap())
        );
        let k = json!({"ticker":"K-A", "rules_primary":"A wins", "last_price_dollars":"0.20"});
        let mut k2 = k.clone();
        k2["last_price_dollars"] = json!("0.80");
        let p = json!({"description":"A wins", "marketSides":[{"long":false,"description":"No"},{"long":true,"description":"Yes"}]});
        let a = RuleReview::new(&k, &Value::Null, None, &p, &Value::Null, false);
        let b = RuleReview::new(&k2, &Value::Null, None, &p, &Value::Null, false);
        assert_eq!(a.fingerprint, b.fingerprint);
        assert_ne!(
            a.fingerprint,
            RuleReview::new(&k, &Value::Null, None, &p, &Value::Null, true).fingerprint
        );
    }
    #[test]
    fn approval_fails_closed_and_expires() {
        let now = Utc::now();
        let r = review();
        assert!(!r.decision(&Approvals::default(), now).approved);
        let value = json!({"approvals":[{"fingerprint":r.fingerprint, "reviewed_by":"Reviewer", "reviewed_at":now, "expires_at":now + Duration::hours(1), "notes":"Exact rules and all payout cases compared", "sources":["https://example.com/rules"], "checks":{
            "same_event_and_participants":true,"same_statistic_line_and_period":true,"overtime_and_ties":true,"postponement_and_rescheduling":true,"cancellation_void_and_fair_price":true,"abandonment_retirement_and_participation":true,"settlement_sources_and_corrections":true,"combined_payout_at_least_one_dollar_in_all_cases":true}}]});
        let mut a: Approvals = serde_json::from_value(value).unwrap();
        assert!(r.decision(&a, now).approved);
        assert!(!r.decision(&a, now + Duration::hours(2)).approved);
        a.approvals[0].checks.cancellation_void_and_fair_price = false;
        assert!(!r.decision(&a, now).approved);
        let empty = RuleReview::new(
            &Value::Null,
            &Value::Null,
            None,
            &Value::Null,
            &Value::Null,
            false,
        );
        assert!(!empty.missing.is_empty());
    }
}
