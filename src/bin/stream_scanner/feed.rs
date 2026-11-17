//! Validation is performed before mutating any executable book. Errors end the
//! connection session, dropping both venues' books and all confirmations.
use super::*;

const MAX_FUTURE_MS: i64 = 250;

fn source_time(value: &Value) -> Option<i64> {
    value.as_i64().or_else(|| value.as_str()?.parse().ok())
}
fn check_time(timestamp: i64, now_ms: i64) -> Result<(), String> {
    let age = now_ms
        .checked_sub(timestamp)
        .ok_or("invalid feed timestamp")?;
    if age < -MAX_FUTURE_MS || age > MAX_AGE.as_millis() as i64 {
        return Err("feed source timestamp is stale or in the future".into());
    }
    Ok(())
}

pub(super) fn checked_levels(levels: &Value) -> Result<BTreeMap<i32, f64>, String> {
    let mut result = BTreeMap::new();
    let mut seen = HashSet::new();
    for level in levels.as_array().ok_or("book levels missing")? {
        let p = number(level.get(0).or_else(|| level.pointer("/px/value")))
            .ok_or("invalid book price")?;
        let q = number(level.get(1).or_else(|| level.get("qty"))).ok_or("invalid book quantity")?;
        let ticks = checked_price(p)?;
        if !q.is_finite() || q < 0.0 || !seen.insert(ticks) {
            return Err("negative, nonfinite, or duplicate book level".into());
        }
        if q > 0.0 {
            result.insert(ticks, q);
        }
    }
    Ok(result)
}
fn checked_price(p: f64) -> Result<i32, String> {
    if !p.is_finite()
        || !(0.0..=1.0).contains(&p)
        || (p * 10000.0 - (p * 10000.0).round()).abs() > 1e-6
    {
        return Err("book price outside supported four-decimal grid".into());
    }
    Ok((p * 10000.0).round() as i32)
}

#[derive(Default)]
pub(super) struct KalshiIntegrity {
    sequence: Option<(u64, u64)>,
    snapshots: HashSet<String>,
    yes_books: HashMap<String, BTreeMap<i32, f64>>,
}
impl KalshiIntegrity {
    pub fn initialized(&self) -> usize {
        self.snapshots.len()
    }
    pub fn apply<'a>(
        &mut self,
        payload: &'a Value,
        books: &mut HashMap<String, KalshiBook>,
        now_ms: i64,
    ) -> Result<Option<&'a str>, String> {
        let kind = payload["type"]
            .as_str()
            .ok_or("Kalshi message type missing")?;
        if kind == "error" {
            return Err("Kalshi subscription returned an error".into());
        }
        if !matches!(kind, "orderbook_snapshot" | "orderbook_delta") {
            return Ok(None);
        }
        let sid = payload["sid"]
            .as_u64()
            .ok_or("Kalshi subscription ID missing")?;
        let seq = payload["seq"].as_u64().ok_or("Kalshi sequence missing")?;
        if let Some((previous_sid, previous_seq)) = self.sequence {
            if sid != previous_sid || previous_seq.checked_add(1) != Some(seq) {
                return Err("Kalshi sequence gap, duplicate, reordering, or subscription change; resnapshot required".into());
            }
        } else if kind != "orderbook_snapshot" {
            return Err("Kalshi delta arrived before initial snapshot".into());
        }
        let ticker = payload["msg"]["market_ticker"]
            .as_str()
            .ok_or("Kalshi ticker missing")?;
        if !books.contains_key(ticker) {
            return Err("Kalshi returned an unsubscribed market".into());
        }
        let sending =
            source_time(&payload["sending_ts_ms"]).ok_or("Kalshi source timestamp missing")?;
        check_time(sending, now_ms)?;
        if kind == "orderbook_snapshot" {
            let levels = checked_levels(&payload["msg"]["no_dollars_fp"])?;
            // Validate the other side too, even though candidate pricing only
            // needs NO bids. Malformed data must not become a valid snapshot.
            let yes = checked_levels(&payload["msg"]["yes_dollars_fp"])?;
            if levels
                .last_key_value()
                .zip(yes.last_key_value())
                .is_some_and(|((n, _), (y, _))| n + y > 10000)
            {
                return Err("crossed Kalshi binary book".into());
            }
            let book = books.get_mut(ticker).unwrap();
            book.no = levels;
            book.updated = Some(Instant::now());
            book.generation += 1;
            self.snapshots.insert(ticker.into());
            self.yes_books.insert(ticker.into(), yes);
        } else {
            if !self.snapshots.contains(ticker) {
                return Err("Kalshi market delta arrived before its snapshot".into());
            }
            let msg = &payload["msg"];
            let event_time = source_time(&msg["ts_ms"])
                .or_else(|| {
                    msg["ts"]
                        .as_str()
                        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                        .map(|t| t.timestamp_millis())
                })
                .ok_or("Kalshi event timestamp missing")?;
            check_time(event_time, now_ms)?;
            let p = checked_price(
                number(msg.get("price_dollars")).ok_or("Kalshi delta price missing")?,
            )?;
            let delta = number(msg.get("delta_fp"))
                .filter(|d| d.is_finite())
                .ok_or("invalid Kalshi delta quantity")?;
            let side = msg["side"].as_str().ok_or("Kalshi delta side missing")?;
            let (levels, other_best) = match side {
                "no" => (
                    &mut books.get_mut(ticker).unwrap().no,
                    self.yes_books[ticker].last_key_value().map(|(p, _)| *p),
                ),
                "yes" => (
                    self.yes_books.get_mut(ticker).unwrap(),
                    books[ticker].no.last_key_value().map(|(p, _)| *p),
                ),
                _ => return Err("invalid Kalshi delta side".into()),
            };
            let quantity = levels.get(&p).copied().unwrap_or(0.0) + delta;
            if quantity < -1e-9 || !quantity.is_finite() {
                return Err("Kalshi delta produced negative quantity".into());
            }
            let old_best = levels.last_key_value().map(|(p, _)| *p);
            let next_best = if quantity > 1e-9 {
                Some(old_best.map_or(p, |old| old.max(p)))
            } else if old_best == Some(p) {
                levels.range(..p).next_back().map(|(p, _)| *p)
            } else {
                old_best
            };
            if next_best
                .zip(other_best)
                .is_some_and(|(a, b)| a + b > 10000)
            {
                return Err("Kalshi delta crossed the binary book".into());
            }
            if quantity <= 1e-9 {
                levels.remove(&p);
            } else {
                levels.insert(p, quantity);
            }
            let book = books.get_mut(ticker).unwrap();
            book.updated = Some(Instant::now());
            book.generation += 1;
        }

        self.sequence = Some((sid, seq));
        Ok(Some(ticker))
    }
}

#[derive(Default)]
pub(super) struct PolyIntegrity {
    versions: HashMap<String, (i64, String)>,
}
impl PolyIntegrity {
    pub fn initialized(&self) -> usize {
        self.versions.len()
    }
    pub fn apply<'a>(
        &mut self,
        payload: &'a Value,
        books: &mut HashMap<String, PolyBook>,
        now_ms: i64,
    ) -> Result<Option<&'a str>, String> {
        if payload.get("error").is_some_and(|v| !v.is_null()) {
            return Err("Polymarket subscription returned an error".into());
        }
        let Some(data) = payload.get("marketData") else {
            if payload.get("marketDataLite").is_some() {
                return Err("unexpected Polymarket lite data; full book required".into());
            }
            return Ok(None);
        };
        let slug = data["marketSlug"]
            .as_str()
            .ok_or("Polymarket market slug missing")?;
        if !books.contains_key(slug) {
            return Err("Polymarket returned an unsubscribed market".into());
        }
        let timestamp = data["transactTime"]
            .as_str()
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|t| t.timestamp_millis())
            .ok_or("Polymarket source timestamp missing")?;
        check_time(timestamp, now_ms)?;
        let state = data["state"]
            .as_str()
            .ok_or("Polymarket market state missing")?;
        if !matches!(
            state,
            "MARKET_STATE_OPEN"
                | "MARKET_STATE_PREOPEN"
                | "MARKET_STATE_SUSPENDED"
                | "MARKET_STATE_HALTED"
                | "MARKET_STATE_EXPIRED"
                | "MARKET_STATE_TERMINATED"
                | "MARKET_STATE_MATCH_AND_CLOSE_AUCTION"
        ) {
            return Err("unknown Polymarket market state".into());
        }
        let (bids, offers) = if state == "MARKET_STATE_OPEN" {
            let bids = checked_levels(&data["bids"])?;
            let offers = checked_levels(&data["offers"])?;
            if bids
                .last_key_value()
                .zip(offers.first_key_value())
                .is_some_and(|((b, _), (a, _))| b >= a)
            {
                return Err("locked or crossed Polymarket book".into());
            }
            (bids, offers)
        } else {
            (BTreeMap::new(), BTreeMap::new())
        };
        if let Some((previous, previous_state)) = self.versions.get(slug) {
            if timestamp < *previous {
                return Err("Polymarket snapshot timestamp regressed".into());
            }
            if timestamp == *previous {
                if state != previous_state
                    || books[slug].bids != bids
                    || books[slug].offers != offers
                {
                    return Err(
                        "different Polymarket snapshots share a timestamp; order is ambiguous"
                            .into(),
                    );
                }
                return Ok(None); // A duplicate must not refresh local freshness.
            }
        }
        let book = books.get_mut(slug).unwrap();
        book.bids = bids;
        book.offers = offers;
        book.updated = (state == "MARKET_STATE_OPEN").then(Instant::now);
        book.generation += 1;
        self.versions
            .insert(slug.into(), (timestamp, state.to_owned()));
        Ok(Some(slug))
    }
}

#[derive(Default)]
pub(super) struct FeedHealth {
    last_received: [u64; 2],
    last_ping: u64,
}
impl FeedHealth {
    pub fn received(&mut self, venue: usize, now_ms: u64) {
        self.last_received[venue] = now_ms;
    }
    pub fn ping_sent(&mut self, now_ms: u64) {
        self.last_ping = now_ms;
    }
    pub fn check(
        &self,
        now_ms: u64,
        expected: [usize; 2],
        initialized: [usize; 2],
    ) -> Result<bool, String> {
        if self
            .last_received
            .iter()
            .any(|last| now_ms.saturating_sub(*last) > 15000)
        {
            return Err(
                "market-data connection silent for 15 seconds; rebuilding both books".into(),
            );
        }
        if now_ms > 20000 && (initialized[0] < expected[0] || initialized[1] < expected[1]) {
            return Err("subscription did not deliver all initial books within 20 seconds".into());
        }
        Ok(now_ms.saturating_sub(self.last_ping) >= 5000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn snapshot(seq: u64) -> Value {
        json!({"type":"orderbook_snapshot","sid":1,"seq":seq,"sending_ts_ms":10000,"msg":{"market_ticker":"K","yes_dollars_fp":[["0.4","10"]],"no_dollars_fp":[["0.5","10"]]}})
    }
    fn delta(seq: u64, side: &str) -> Value {
        json!({"type":"orderbook_delta","sid":1,"seq":seq,"sending_ts_ms":10000,"msg":{"market_ticker":"K","price_dollars":"0.5","delta_fp":"1","side":side,"ts_ms":10000}})
    }
    fn poly() -> Value {
        json!({"marketData":{"marketSlug":"p","state":"MARKET_STATE_OPEN","transactTime":"1970-01-01T00:00:10Z","bids":[["0.4","10"]],"offers":[["0.5","10"]]}})
    }
    #[test]
    fn transport_silence_and_missing_subscriptions_trigger_recovery() {
        let mut health = FeedHealth::default();
        assert!(!health.check(0, [2, 2], [0, 0]).unwrap());
        assert!(health.check(5000, [2, 2], [2, 2]).unwrap());
        health.ping_sent(5000);
        assert!(!health.check(5001, [2, 2], [2, 2]).unwrap());
        assert!(health.check(15001, [2, 2], [2, 2]).is_err());
        health.received(0, 20000);
        health.received(1, 20000);
        assert!(health.check(20001, [2, 2], [2, 1]).is_err());
        assert!(health.check(20001, [2, 2], [2, 2]).is_ok());
    }
    #[test]
    fn deltas_validate_both_sides_before_mutation() {
        let mut feed = KalshiIntegrity::default();
        let mut books = HashMap::from([("K".into(), KalshiBook::default())]);
        feed.apply(&snapshot(1), &mut books, 10000).unwrap();
        let mut bad = delta(2, "yes");
        bad["msg"]["price_dollars"] = json!("0.6");
        assert!(feed.apply(&bad, &mut books, 10000).is_err());
        assert_eq!(books["K"].generation, 1);
        bad["msg"]["price_dollars"] = json!("0.4");
        bad["msg"]["delta_fp"] = json!("-11");
        assert!(feed.apply(&bad, &mut books, 10000).is_err());
        assert_eq!(feed.yes_books["K"][&4000], 10.0);
    }
    #[test]
    fn sequences_are_subscription_wide_and_require_fresh_snapshots() {
        for bad in [1, 2, 4] {
            let mut feed = KalshiIntegrity::default();
            let mut books = HashMap::from([("K".into(), KalshiBook::default())]);
            feed.apply(&snapshot(1), &mut books, 10000).unwrap();
            feed.apply(&delta(2, "yes"), &mut books, 10000).unwrap();
            assert!(feed.apply(&delta(bad, "no"), &mut books, 10000).is_err());
            assert_eq!(books["K"].no[&5000], 10.0);
        }
        let mut feed = KalshiIntegrity::default();
        let mut books = HashMap::from([("K".into(), KalshiBook::default())]);
        assert!(feed.apply(&delta(1, "no"), &mut books, 10000).is_err());
        feed.apply(&snapshot(11), &mut books, 10000).unwrap();
        feed.apply(&delta(12, "no"), &mut books, 10000).unwrap();
        assert_eq!(books["K"].no[&5000], 11.0);
    }
    #[test]
    fn bad_timestamps_and_malformed_books_are_rejected_atomically() {
        for value in [
            json!([["NaN", "1"]]),
            json!([["0.4", "-1"]]),
            json!([["0.4", "1"], ["0.4", "2"]]),
            json!([["1.2", "1"]]),
            json!([["0.40001", "1"]]),
        ] {
            assert!(checked_levels(&value).is_err());
        }
        for now in [7000, 13000] {
            assert!(
                KalshiIntegrity::default()
                    .apply(
                        &snapshot(1),
                        &mut HashMap::from([("K".into(), KalshiBook::default())]),
                        now
                    )
                    .is_err()
            );
        }
        let mut feed = KalshiIntegrity::default();
        let mut books = HashMap::from([("K".into(), KalshiBook::default())]);
        feed.apply(&snapshot(1), &mut books, 10000).unwrap();
        let mut d = delta(2, "no");
        d["msg"]["delta_fp"] = json!("-11");
        assert!(feed.apply(&d, &mut books, 10000).is_err());
        assert_eq!(books["K"].no[&5000], 10.0);
    }
    #[test]
    fn poly_duplicates_regressions_and_halts_cannot_refresh_executable_books() {
        let mut feed = PolyIntegrity::default();
        let mut books = HashMap::from([("p".into(), PolyBook::default())]);
        feed.apply(&poly(), &mut books, 10000).unwrap();
        let updated = books["p"].updated;
        assert!(feed.apply(&poly(), &mut books, 11000).unwrap().is_none());
        assert_eq!(books["p"].updated, updated);
        assert_eq!(books["p"].generation, 1);
        let mut halted = poly();
        halted["marketData"]["state"] = json!("MARKET_STATE_HALTED");
        halted["marketData"]["transactTime"] = json!("1970-01-01T00:00:11Z");
        feed.apply(&halted, &mut books, 11000).unwrap();
        assert!(books["p"].updated.is_none());
        assert!(books["p"].bids.is_empty());
        assert!(feed.apply(&poly(), &mut books, 11000).is_err());
    }
    #[test]
    fn poly_requires_complete_open_books_and_consistent_timestamps() {
        for field in ["transactTime", "state", "bids", "offers"] {
            let mut value = poly();
            value["marketData"].as_object_mut().unwrap().remove(field);
            assert!(
                PolyIntegrity::default()
                    .apply(
                        &value,
                        &mut HashMap::from([("p".into(), PolyBook::default())]),
                        10000
                    )
                    .is_err()
            );
        }
        let mut feed = PolyIntegrity::default();
        let mut books = HashMap::from([("p".into(), PolyBook::default())]);
        feed.apply(&poly(), &mut books, 10000).unwrap();
        let mut changed = poly();
        changed["marketData"]["bids"][0][1] = json!("11");
        assert!(feed.apply(&changed, &mut books, 10000).is_err());
        assert_eq!(books["p"].bids[&4000], 10.0);
    }
}
