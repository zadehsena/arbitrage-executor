use arbitrage_executor::sports::{SPORTS_USAGE, selected_sports};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use ed25519_dalek::{Signer as _, pkcs8::DecodePrivateKey as _};
use rand::rngs::OsRng;
use reqwest::{Client, header::HeaderMap};
use rsa::{
    RsaPrivateKey,
    pkcs1::DecodeRsaPrivateKey as _,
    pss::BlindedSigningKey,
    signature::{RandomizedSigner as _, SignatureEncoding as _},
};
use serde_json::Value;
use sha2::Sha256;
use std::{
    collections::HashSet,
    env,
    error::Error,
    fs,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const KALSHI_BASE_URL: &str = "https://api.elections.kalshi.com/trade-api/v2";
const KALSHI_ORDERBOOK_BASE_URL: &str = "https://external-api.kalshi.com";
const POLYMARKET_US_BASE_URL: &str = "https://gateway.polymarket.us";

#[derive(Debug, Clone)]
struct EventRef {
    id: String,
    title: String,
}

#[derive(Debug, Clone)]
struct MarketRef {
    outcome: String,
    id: String,
    is_short: bool,
}

#[derive(Debug, Clone)]
struct ExecutableQuote {
    outcome: String,
    ask: f64,
    depth: Option<f64>,
}

fn words(value: &str) -> HashSet<String> {
    const STOP_WORDS: &[&str] = &[
        "a", "an", "and", "at", "for", "in", "of", "the", "to", "vs", "will",
    ];
    value
        .to_lowercase()
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty() && !STOP_WORDS.contains(word))
        .map(str::to_owned)
        .collect()
}

fn similarity(left: &str, right: &str) -> f64 {
    let left_words = words(left);
    let right_words = words(right);
    if left_words.is_empty() || right_words.is_empty() {
        return 0.0;
    }
    let common = left_words.intersection(&right_words).count() as f64;
    let total = left_words.union(&right_words).count() as f64;
    common / total
}

fn normalize(value: &str) -> String {
    value
        .to_lowercase()
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .collect()
}

fn canonical_team(value: &str) -> String {
    let normalized = normalize(value);
    match normalized.as_str() {
        "arizona" | "arizonadiamondbacks" => "arizonadiamondbacks",
        "atlanta" | "atlantabraves" => "atlantabraves",
        "as" | "athletics" | "oakland" | "oaklandathletics" => "athletics",
        "baltimore" | "baltimoreorioles" => "baltimoreorioles",
        "boston" | "bostonredsox" => "bostonredsox",
        "chicagoc" | "chicagocubs" => "chicagocubs",
        "chicagows" | "chicagowhitesox" => "chicagowhitesox",
        "cincinnati" | "cincinnatireds" => "cincinnatireds",
        "cleveland" | "clevelandguardians" => "clevelandguardians",
        "colorado" | "coloradorockies" => "coloradorockies",
        "detroit" | "detroittigers" => "detroittigers",
        "houston" | "houstonastros" => "houstonastros",
        "kansascity" | "kansascityroyals" => "kansascityroyals",
        "losangelesa" | "losangelesangels" => "losangelesangels",
        "losangelesd" | "losangelesdodgers" => "losangelesdodgers",
        "miami" | "miamimarlins" => "miamimarlins",
        "milwaukee" | "milwaukeebrewers" => "milwaukeebrewers",
        "minnesota" | "minnesotatwins" => "minnesotatwins",
        "newyorkm" | "newyorkmets" => "newyorkmets",
        "newyorky" | "newyorkyankees" => "newyorkyankees",
        "philadelphia" | "philadelphiaphillies" => "philadelphiaphillies",
        "pittsburgh" | "pittsburghpirates" => "pittsburghpirates",
        "sandiego" | "sandiegopadres" => "sandiegopadres",
        "sanfrancisco" | "sanfranciscogiants" => "sanfranciscogiants",
        "seattle" | "seattlemariners" => "seattlemariners",
        "stlouis" | "stlouiscardinals" => "stlouiscardinals",
        "tampabay" | "tampabayrays" => "tampabayrays",
        "texas" | "texasrangers" => "texasrangers",
        "toronto" | "torontobluejays" => "torontobluejays",
        "washington" | "washingtonnationals" => "washingtonnationals",
        _ => normalized.as_str(),
    }
    .to_owned()
}

fn same_team(left: &str, right: &str) -> bool {
    let left = canonical_team(left);
    let right = canonical_team(right);
    !left.is_empty()
        && !right.is_empty()
        && (left == right || left.contains(&right) || right.contains(&left))
}

fn kalshi_winner(title: &str) -> Option<&str> {
    title
        .strip_suffix(" wins")
        .or_else(|| title.strip_prefix("Will ")?.split_once(" win the ").map(|(team, _)| team))
}

fn event_team_keys(value: &str) -> Option<[String; 2]> {
    let teams: Vec<_> = value.split("vs").map(canonical_team).collect();
    (teams.len() == 2).then(|| [teams[0].clone(), teams[1].clone()])
}

fn event_similarity(left: &str, right: &str) -> f64 {
    let title_similarity = similarity(left, right);
    match (event_team_keys(left), event_team_keys(right)) {
        (Some(left), Some(right)) => {
            let shared = left
                .iter()
                .filter(|team| right.iter().any(|other| same_team(team, other)))
                .count();
            title_similarity.max(shared as f64 / 2.0)
        }
        _ => title_similarity,
    }
}

fn number(value: Option<&Value>) -> Option<f64> {
    value.and_then(|value| value.as_f64().or_else(|| value.as_str()?.parse().ok()))
}

async fn get_json(client: &Client, url: &str) -> Result<Value, Box<dyn Error>> {
    Ok(client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?)
}

fn cents(value: f64) -> f64 {
    if value > 1.0 { value / 100.0 } else { value }
}

fn display_depth(depth: Option<f64>) -> String {
    depth
        .map(|value| format!("{value:.2} contracts"))
        .unwrap_or_else(|| "side depth unavailable".to_owned())
}

fn level(value: Option<&Value>) -> Option<(f64, f64)> {
    let values = value?.as_array()?;
    Some((cents(number(values.first())?), number(values.get(1))?))
}

fn kalshi_headers(path: &str) -> Result<HeaderMap, Box<dyn Error>> {
    let key_id = env::var("KALSHI_API_KEY_ID")?;
    let private_key_path = env::var("KALSHI_PRIVATE_KEY_PATH")?;
    let private_key = fs::read_to_string(private_key_path)?;
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .to_string();
    let presign = format!("{timestamp}GET{path}");

    // Kalshi chooses the verifier from the registered key; support both documented key types.
    let signature = if let Ok(key) = ed25519_dalek::SigningKey::from_pkcs8_pem(&private_key) {
        BASE64.encode(key.sign(presign.as_bytes()).to_bytes())
    } else if let Ok(key) = RsaPrivateKey::from_pkcs1_pem(&private_key)
        .or_else(|_| RsaPrivateKey::from_pkcs8_pem(&private_key))
    {
        let signer = BlindedSigningKey::<Sha256>::new(key);
        BASE64.encode(
            signer
                .sign_with_rng(&mut OsRng, presign.as_bytes())
                .to_bytes(),
        )
    } else {
        return Err("KALSHI_PRIVATE_KEY_PATH is not a supported Ed25519 or RSA PEM key".into());
    };

    let mut headers = HeaderMap::new();
    headers.insert("KALSHI-ACCESS-KEY", key_id.parse()?);
    headers.insert("KALSHI-ACCESS-SIGNATURE", signature.parse()?);
    headers.insert("KALSHI-ACCESS-TIMESTAMP", timestamp.parse()?);
    Ok(headers)
}

async fn kalshi_buy_yes_quote(
    client: &Client,
    market: &MarketRef,
) -> Result<ExecutableQuote, Box<dyn Error>> {
    let path = format!("/trade-api/v2/markets/{}/orderbook?depth=1", market.id);
    let payload = client
        .get(format!("{KALSHI_ORDERBOOK_BASE_URL}{path}"))
        .headers(kalshi_headers(&path)?)
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    // Kalshi returns bids only. A NO bid at p is the executable YES ask at 1 - p.
    let (no_bid, no_bid_depth) = level(payload.pointer("/orderbook_fp/no_dollars/0"))
        .ok_or("Kalshi has no executable YES ask")?;
    Ok(ExecutableQuote {
        outcome: market.outcome.clone(),
        ask: 1.0 - no_bid,
        depth: Some(no_bid_depth),
    })
}

async fn polymarket_buy_yes_quote(
    client: &Client,
    market: &MarketRef,
) -> Result<ExecutableQuote, Box<dyn Error>> {
    let payload = get_json(
        client,
        &format!("{POLYMARKET_US_BASE_URL}/v1/markets/{}/bbo", market.id),
    )
    .await?;
    let quote_path = if market.is_short {
        "/marketData/shortQuote/value"
    } else {
        "/marketData/longQuote/value"
    };
    let ask = number(payload.pointer(quote_path)).ok_or("Polymarket did not return a best ask")?;
    // The lightweight BBO response publishes ask depth only for its primary long quote.
    // Do not attribute that size to the opposite outcome.
    let depth = (!market.is_short)
        .then(|| number(payload.pointer("/marketData/askDepth")))
        .flatten();
    Ok(ExecutableQuote {
        outcome: market.outcome.clone(),
        ask: cents(ask),
        depth,
    })
}

async fn event_refs(
    client: &Client,
    league: &str,
    series: &str,
    limit: usize,
) -> Result<(Vec<EventRef>, Vec<EventRef>), Box<dyn Error>> {
    let kalshi_url =
        format!("{KALSHI_BASE_URL}/events?series_ticker={series}&status=open&limit={limit}");
    let poly_url =
        format!("{POLYMARKET_US_BASE_URL}/v2/leagues/{league}/events?limit={limit}&offset=0");
    let (kalshi, polymarket) =
        tokio::join!(get_json(client, &kalshi_url), get_json(client, &poly_url));
    let kalshi = kalshi?;
    let polymarket = polymarket?;
    let kalshi_events = kalshi["events"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|event| {
            Some(EventRef {
                id: event["event_ticker"].as_str()?.to_owned(),
                title: event["title"].as_str()?.to_owned(),
            })
        })
        .collect();
    let polymarket_events = polymarket["events"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|event| {
            Some(EventRef {
                id: event["slug"].as_str()?.to_owned(),
                title: event["title"].as_str()?.to_owned(),
            })
        })
        .collect();
    Ok((kalshi_events, polymarket_events))
}

fn match_events(
    kalshi: Vec<EventRef>,
    polymarket: Vec<EventRef>,
) -> Vec<(EventRef, EventRef, f64)> {
    let mut remaining = kalshi;
    let mut matches = Vec::new();
    for poly in polymarket {
        let Some((index, score)) = remaining
            .iter()
            .enumerate()
            .map(|(index, event)| (index, event_similarity(&event.title, &poly.title)))
            .max_by(|left, right| left.1.total_cmp(&right.1))
        else {
            continue;
        };
        if score >= 0.72 {
            matches.push((remaining.remove(index), poly, score));
        }
    }
    matches
}

fn kalshi_moneyline_markets(payload: &Value) -> Vec<MarketRef> {
    payload["markets"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|market| {
            let outcome = kalshi_winner(market["title"].as_str()?)?.to_owned();
            let id = market["ticker"].as_str()?.to_owned();
            Some(MarketRef {
                outcome,
                id,
                is_short: false,
            })
        })
        .collect()
}

fn polymarket_moneyline_markets(payload: &Value) -> Vec<MarketRef> {
    payload["event"]["markets"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|market| {
            market["question"].as_str().is_some_and(|question| {
                question
                    .to_lowercase()
                    .contains("who will win in the upcoming")
            })
        })
        .flat_map(|market| {
            let id = market["slug"].as_str().map(str::to_owned);
            market["marketSides"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(move |side| {
                    let outcome = side["team"]["safeName"]
                        .as_str()
                        .filter(|name| !name.is_empty())
                        .or_else(|| side["team"]["name"].as_str())?
                        .to_owned();
                    Some(MarketRef {
                        outcome,
                        id: id.clone()?,
                        is_short: !side["long"].as_bool()?,
                    })
                })
        })
        .collect()
}

async fn inspect_match(
    client: &Client,
    kalshi: &EventRef,
    polymarket: &EventRef,
    score: f64,
) -> Result<(), Box<dyn Error>> {
    let kalshi_url = format!("{KALSHI_BASE_URL}/events/{}", kalshi.id);
    let polymarket_url = format!("{POLYMARKET_US_BASE_URL}/v1/events/slug/{}", polymarket.id);
    let (kalshi_detail, polymarket_detail) = tokio::join!(
        get_json(client, &kalshi_url),
        get_json(client, &polymarket_url)
    );
    let kalshi_detail = kalshi_detail?;
    let polymarket_detail = polymarket_detail?;
    let kalshi_markets = kalshi_moneyline_markets(&kalshi_detail);
    let polymarket_markets = polymarket_moneyline_markets(&polymarket_detail);
    if kalshi_markets.len() != 2 || polymarket_markets.len() != 2 {
        return Ok(());
    }
    let mut aligned: Vec<(&MarketRef, &MarketRef)> = Vec::new();
    for kalshi_market in &kalshi_markets {
        let matching = polymarket_markets
            .iter()
            .filter(|market| same_team(&kalshi_market.outcome, &market.outcome))
            .collect::<Vec<_>>();
        if matching.len() != 1 {
            return Ok(());
        }
        aligned.push((kalshi_market, matching[0]));
    }
    if aligned.len() != 2 || aligned[0].1.outcome == aligned[1].1.outcome {
        return Ok(());
    }
    let first = aligned[0];
    let second = aligned[1];
    let (first_kalshi, second_kalshi, first_poly, second_poly) = tokio::join!(
        kalshi_buy_yes_quote(client, first.0),
        kalshi_buy_yes_quote(client, second.0),
        polymarket_buy_yes_quote(client, first.1),
        polymarket_buy_yes_quote(client, second.1),
    );
    let (first_kalshi, second_kalshi, first_poly, second_poly) =
        match (first_kalshi, second_kalshi, first_poly, second_poly) {
            (Ok(a), Ok(b), Ok(c), Ok(d)) => (a, b, c, d),
            (a, b, c, d) => {
                eprintln!(
                    "  Skipping executable comparison: {}",
                    a.err()
                        .or_else(|| b.err())
                        .or_else(|| c.err())
                        .or_else(|| d.err())
                        .unwrap()
                );
                return Ok(());
            }
        };
    let best_total = (first_kalshi.ask + second_poly.ask).min(second_kalshi.ask + first_poly.ask);
    println!("\n{:.0}% title match · {}", score * 100.0, polymarket.title);
    println!(
        "  {:<24} Kalshi ask ${:.3} ({}) | Polymarket ask ${:.3} ({})",
        first_poly.outcome,
        first_kalshi.ask,
        display_depth(first_kalshi.depth),
        first_poly.ask,
        display_depth(first_poly.depth)
    );
    println!(
        "  {:<24} Kalshi ask ${:.3} ({}) | Polymarket ask ${:.3} ({})",
        second_poly.outcome,
        second_kalshi.ask,
        display_depth(second_kalshi.depth),
        second_poly.ask,
        display_depth(second_poly.depth)
    );
    if best_total < 1.0 {
        println!(
            "  REVIEW: executable best-ask edge +{:.2}% (before fees, rule parity, and fill risk)",
            (1.0 - best_total) * 100.0
        );
    } else {
        println!("  No executable best-ask cross-venue edge.");
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();
    let selection = env::args().nth(1).unwrap_or_else(|| "cfb".to_owned());
    let sports = selected_sports(&selection)
        .ok_or_else(|| format!("Usage: cargo run --bin market_probe -- {SPORTS_USAGE}"))?;
    let client = Client::builder()
        .user_agent("arbitrage-executor-dry-run/0.1")
        .build()?;
    for sport in sports {
        let (kalshi, polymarket) =
            event_refs(&client, sport.polymarket_league, sport.kalshi_series, 40).await?;
        let matches = match_events(kalshi, polymarket);
        println!(
            "Read-only executable probe: {} likely {} event matches. Only GET market-data endpoints are called.",
            matches.len(),
            sport.label
        );
        for (kalshi, polymarket, score) in matches.into_iter().take(5) {
            inspect_match(&client, &kalshi, &polymarket, score).await?;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{event_similarity, kalshi_winner, polymarket_moneyline_markets};
    use serde_json::json;

    #[test]
    fn moneyline_uses_side_flags_even_when_the_api_reorders_sides() {
        let payload = json!({"event":{"markets":[{"slug":"game","question":"Who will win in the upcoming game?","marketSides":[
            {"long":false,"team":{"safeName":"B"}},
            {"long":true,"team":{"safeName":"A"}}
        ]}]}});
        let markets = polymarket_moneyline_markets(&payload);
        assert_eq!(markets.len(), 2);
        assert!(markets[0].is_short);
        assert!(!markets[1].is_short);
    }

    #[test]
    fn matches_abbreviated_mlb_team_names() {
        assert_eq!(
            event_similarity("New York M vs Washington", "New York Mets vs. Washington Nationals"),
            1.0
        );
    }

    #[test]
    fn matches_surname_only_tennis_titles() {
        assert_eq!(
            event_similarity("Dart vs Lemaitre", "Harriet Dart vs. Tiphanie Lemaitre"),
            1.0
        );
    }

    #[test]
    fn extracts_r6_winner_from_kalshi_question_title() {
        assert_eq!(
            kalshi_winner("Will Heretics win the Heretics vs. Rebels Gaming R6 match?"),
            Some("Heretics")
        );
    }
}
