//! Read-only Novig v3 catalog and order-book support.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use ed25519_dalek::{Signer as _, pkcs8::DecodePrivateKey as _};
use reqwest::Client;
use serde_json::Value;
use sha2::{Digest as _, Sha256};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env,
    error::Error,
    fs,
    time::{SystemTime, UNIX_EPOCH},
};

pub fn hosts() -> Result<(&'static str, &'static str), Box<dyn Error>> {
    match env::var("NOVIG_ENV").as_deref() {
        Ok("qa") => Ok(("https://api.qa.novig.com", "wss://api.qa.novig.com/v3/ws")),
        Ok("production") | Err(env::VarError::NotPresent) => {
            Ok(("https://api.novig.com", "wss://api.novig.com/v3/ws"))
        }
        _ => Err("NOVIG_ENV must be production or qa".into()),
    }
}

#[derive(Clone, Debug)]
pub struct Market {
    pub id: String,
    pub event: String,
    pub outcomes: [(String, String); 2],
    pub fee_coefficient: f64,
}

pub struct Inventory {
    pub games: usize,
    pub moneylines: usize,
    pub spreads: usize,
    pub totals: usize,
    pub props: usize,
    /// Full-game two-way moneylines eligible for the current Novig matcher.
    pub markets: Vec<Market>,
}

pub fn enabled() -> Result<bool, Box<dyn Error>> {
    let id = env::var("NOVIG_KEY_ID").is_ok();
    let path = env::var("NOVIG_PRIVATE_KEY_PATH").is_ok();
    if id != path {
        return Err("set both NOVIG_KEY_ID and NOVIG_PRIVATE_KEY_PATH, or neither".into());
    }
    Ok(id)
}

/// Sign a read-only GET. No private key is sent to the server.
pub fn signed_get_headers(path: &str) -> Result<Vec<(&'static str, String)>, Box<dyn Error>> {
    let key_id = env::var("NOVIG_KEY_ID")?;
    let pem = fs::read_to_string(env::var("NOVIG_PRIVATE_KEY_PATH")?)?;
    let key = ed25519_dalek::SigningKey::from_pkcs8_pem(&pem)
        .map_err(|_| "Novig requires an Ed25519 PKCS#8 trading or trading::read private key")?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .to_string();
    let empty_hash = Sha256::digest([]);
    let hash = empty_hash
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let message = format!("NOVIG-V3\n{timestamp}\nGET\n{path}\n\n{hash}");
    let signature = STANDARD.encode(key.sign(message.as_bytes()).to_bytes());
    Ok(vec![
        ("Novig-Key-Id", key_id),
        ("Novig-Timestamp", timestamp),
        ("Novig-Signature", signature),
    ])
}

pub fn websocket_headers() -> Result<Vec<(&'static str, String)>, Box<dyn Error>> {
    signed_get_headers("/v3/ws")
}

pub async fn moneyline_markets(
    client: &Client,
    league: &str,
) -> Result<Vec<Market>, Box<dyn Error>> {
    Ok(inventory(client, league).await?.markets)
}

fn parse_moneyline(item: &Value) -> Option<Market> {
    if item["marketType"].as_str() != Some("MONEY") || item["status"].as_str() != Some("OPEN") {
        return None;
    }
    let outcomes = item["outcomes"].as_array()?;
    if outcomes.len() != 2 {
        return None;
    }
    let a = outcomes[0]["outcomeId"].as_str()?.to_owned();
    let b = outcomes[0]["name"].as_str()?.to_owned();
    let c = outcomes[1]["outcomeId"].as_str()?.to_owned();
    let d = outcomes[1]["name"].as_str()?.to_owned();
    let coefficient = item["fee"]["coefficient"].as_str()?.parse::<f64>().ok()?;
    if !(0.0..=1.0).contains(&coefficient) {
        return None;
    }
    Some(Market {
        id: item["marketId"].as_str()?.into(),
        event: item["eventId"].as_str()?.into(),
        outcomes: [(a, b), (c, d)],
        fee_coefficient: coefficient,
    })
}

pub async fn inventory(client: &Client, league: &str) -> Result<Inventory, Box<dyn Error>> {
    let mut after: Option<String> = None;
    let mut items = Vec::new();
    loop {
        let mut url = reqwest::Url::parse(&format!("{}/v3/public/catalog/markets", hosts()?.0))?;
        url.query_pairs_mut()
            .append_pair("league", league)
            .append_pair("limit", "5000");
        if let Some(cursor) = &after {
            url.query_pairs_mut().append_pair("after", cursor);
        }
        let body: Value = client
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        items.extend(
            body["items"]
                .as_array()
                .ok_or("Novig market list missing items")?
                .iter()
                .cloned(),
        );
        after = body["next"].as_str().map(str::to_owned);
        if after.is_none() {
            break;
        }
    }
    Ok(summarize_inventory(&items))
}

fn summarize_inventory(items: &[Value]) -> Inventory {
    let markets = items.iter().filter_map(parse_moneyline).collect::<Vec<_>>();
    let game_events = markets
        .iter()
        .map(|market| market.event.clone())
        .collect::<HashSet<_>>();
    let mut found = Inventory {
        games: game_events.len(),
        moneylines: 0,
        spreads: 0,
        totals: 0,
        props: 0,
        markets,
    };
    for item in items {
        if item["status"].as_str() != Some("OPEN")
            || !item["eventId"]
                .as_str()
                .is_some_and(|id| game_events.contains(id))
        {
            continue;
        }
        let Some(kind) = item["marketType"].as_str() else {
            continue;
        };
        if kind.starts_with("MONEY") {
            found.moneylines += 1;
        } else if kind.starts_with("SPREAD") {
            found.spreads += 1;
        } else if kind.starts_with("TOTAL") {
            found.totals += 1;
        } else {
            found.props += 1;
        }
    }
    found
}

pub async fn event_titles(
    client: &Client,
    league: &str,
) -> Result<HashMap<String, String>, Box<dyn Error>> {
    let mut after: Option<String> = None;
    let mut out = HashMap::new();
    loop {
        let mut url = reqwest::Url::parse(&format!("{}/v3/public/catalog/events", hosts()?.0))?;
        url.query_pairs_mut()
            .append_pair("league", league)
            .append_pair("limit", "5000");
        if let Some(cursor) = &after {
            url.query_pairs_mut().append_pair("after", cursor);
        }
        let body: Value = client
            .get(url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        for item in body["items"]
            .as_array()
            .ok_or("Novig event list missing items")?
        {
            if let Some((id, title)) = item["eventId"].as_str().zip(item["description"].as_str()) {
                out.insert(id.into(), title.into());
            }
        }
        after = body["next"].as_str().map(str::to_owned);
        if after.is_none() {
            return Ok(out);
        }
    }
}

#[derive(Default, Debug)]
pub struct Book {
    pub seq: Option<u64>,
    orders: HashMap<String, (String, i32, u64)>,
    pub updated: Option<std::time::Instant>,
    pub generation: u64,
}

fn price_milli(value: &Value) -> Option<i32> {
    let raw = value.as_str()?;
    let (whole, frac) = raw.split_once('.')?;
    if whole != "0" || !(1..=3).contains(&frac.len()) || !frac.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let milli = frac.parse::<i32>().ok()? * 10_i32.pow((3 - frac.len()) as u32);
    (1..=999).contains(&milli).then_some(milli)
}

impl Book {
    pub fn snapshot(&mut self, value: &Value) -> bool {
        let Some(seq) = value["seq"].as_u64() else {
            return false;
        };
        let Some(outcomes) = value["orders"].as_object() else {
            return false;
        };
        let mut orders = HashMap::new();
        for (outcome, ladder) in outcomes {
            let Some(ladder) = ladder.as_array() else {
                return false;
            };
            for level in ladder {
                let Some((id, price, qty)) = level["order"]
                    .as_str()
                    .or_else(|| level["orderId"].as_str())
                    .zip(price_milli(&level["price"]))
                    .zip(level["qty"].as_u64())
                    .map(|((a, b), c)| (a, b, c))
                else {
                    return false;
                };
                if qty > 0 {
                    orders.insert(id.to_owned(), (outcome.clone(), price, qty));
                }
            }
        }
        self.orders = orders;
        self.seq = Some(seq);
        self.updated = Some(std::time::Instant::now());
        self.generation += 1;
        true
    }

    /// Returns false on a gap or malformed batch; the caller must request a snapshot.
    pub fn delta(&mut self, value: &Value) -> bool {
        let Some(seq) = value["seq"].as_u64() else {
            return false;
        };
        if self.seq.and_then(|old| old.checked_add(1)) != Some(seq) {
            self.invalidate();
            return false;
        }
        let Some(changes) = value["deltas"].as_array() else {
            self.invalidate();
            return false;
        };
        for change in changes {
            let Some(id) = change["order"]
                .as_str()
                .or_else(|| change["orderId"].as_str())
            else {
                self.invalidate();
                return false;
            };
            match change["kind"].as_str() {
                Some("remove") => {
                    if self.orders.remove(id).is_none() {
                        self.invalidate();
                        return false;
                    }
                }
                Some("add") => {
                    let Some((outcome, price, qty)) = change["outcome"]
                        .as_str()
                        .zip(price_milli(&change["price"]))
                        .zip(change["qty"].as_u64())
                        .map(|((a, b), c)| (a, b, c))
                    else {
                        self.invalidate();
                        return false;
                    };
                    if qty > 0 {
                        self.orders.insert(id.into(), (outcome.into(), price, qty));
                    }
                }
                _ => {
                    self.invalidate();
                    return false;
                }
            }
        }
        self.seq = Some(seq);
        self.updated = Some(std::time::Instant::now());
        self.generation += 1;
        true
    }

    pub fn invalidate(&mut self) {
        self.seq = None;
        self.updated = None;
        self.orders.clear();
    }

    /// Buy the requested outcome by lifting bids on its opposite outcome.
    pub fn fill(
        &self,
        opposite_outcome: &str,
        target_dollar_contracts: u64,
        coefficient: f64,
    ) -> (u64, f64, f64, usize) {
        let mut ladder = BTreeMap::<i32, u64>::new();
        for (outcome, price, qty) in self.orders.values() {
            if outcome == opposite_outcome {
                *ladder.entry(*price).or_default() += qty;
            }
        }
        let mut remaining = target_dollar_contracts * 100;
        let (mut cost, mut fee, mut levels) = (0.0, 0.0, 0);
        for (bid, available) in ladder.iter().rev() {
            let qty = remaining.min(*available);
            if qty == 0 {
                continue;
            }
            let p = f64::from(1000 - *bid) / 1000.0;
            let contracts = qty as f64 / 100.0;
            cost += p * contracts;
            // Conservatively apply the market's fee even for pregame books.
            fee += (coefficient * p * (1.0 - p) * contracts * 100_000.0).ceil() / 100_000.0;
            levels += 1;
            remaining -= qty;
            if remaining == 0 {
                break;
            }
        }
        (target_dollar_contracts * 100 - remaining, cost, fee, levels)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn inventory_counts_game_derivatives_but_excludes_futures() {
        let inventory = summarize_inventory(&[
            json!({"marketId":"m1","eventId":"game","marketType":"MONEY","status":"OPEN","fee":{"coefficient":"0.03"},"outcomes":[{"outcomeId":"a","name":"A"},{"outcomeId":"b","name":"B"}]}),
            json!({"eventId":"game","marketType":"SPREAD","status":"OPEN"}),
            json!({"eventId":"game","marketType":"TOTAL","status":"OPEN"}),
            json!({"eventId":"game","marketType":"TEAM_TOTAL","status":"OPEN"}),
            json!({"eventId":"game","marketType":"RECEIVING_YARDS","status":"OPEN"}),
            json!({"eventId":"game","marketType":"SPREAD","status":"CLOSED"}),
            json!({"eventId":"season","marketType":"MVP_WINNER","status":"OPEN"}),
        ]);
        assert_eq!(inventory.games, 1);
        assert_eq!(inventory.moneylines, 1);
        assert_eq!(inventory.spreads, 1);
        assert_eq!(inventory.totals, 1);
        assert_eq!(inventory.props, 2);
        assert_eq!(inventory.markets.len(), 1);
    }

    #[test]
    fn novig_qty_is_one_cent_and_buy_uses_opposite_bid() {
        let mut book = Book::default();
        assert!(book.snapshot(
            &json!({"seq":1,"orders":{"A":[{"order":"a","price":"0.665","qty":110}],"B":[]}})
        ));
        let (qty, cost, fee, levels) = book.fill("A", 1, 0.03);
        assert_eq!((qty, levels), (100, 1));
        assert!((cost - 0.335).abs() < 1e-10);
        assert!((fee - 0.00669).abs() < 1e-10);
    }

    #[test]
    fn sequence_gap_invalidates_book() {
        let mut book = Book::default();
        assert!(book.snapshot(&json!({"seq":7,"orders":{}})));
        assert!(!book.delta(&json!({"seq":9,"deltas":[]})));
        assert!(book.updated.is_none());
    }

    #[test]
    fn observed_public_book_accepts_short_decimal_and_order_id() {
        let mut book = Book::default();
        assert!(book.snapshot(
            &json!({"seq":1096,"orders":{"A":[{"orderId":"a","price":"0.48","qty":100}],"B":[]}})
        ));
        let (qty, cost, _, _) = book.fill("A", 1, 0.03);
        assert_eq!(qty, 100);
        assert!((cost - 0.52).abs() < 1e-10);
    }
}
