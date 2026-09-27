//! Continuous, read-only cross-venue scanner.
//!
//! It only opens market-data WebSockets and appends pre-fee candidates to a
//! local journal. There are intentionally no order, balance, or portfolio APIs.

use arbitrage_executor::sports::{SCANNER_USAGE, Sport, selected_sports};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use ed25519_dalek::{Signer as _, pkcs8::DecodePrivateKey as _};
use futures_util::{SinkExt as _, StreamExt as _, future::join_all};
use rand::rngs::OsRng;
use reqwest::{Client, Url};
use rsa::{
    RsaPrivateKey,
    pkcs1::DecodeRsaPrivateKey as _,
    pss::BlindedSigningKey,
    signature::{RandomizedSigner as _, SignatureEncoding as _},
};
use serde_json::{Value, json};
use sha2::Sha256;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env,
    error::Error,
    fs::{self, OpenOptions},
    io::Write,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::header::{HeaderName, HeaderValue},
    },
};

const KALSHI_REST: &str = "https://api.elections.kalshi.com/trade-api/v2";
const POLY_REST: &str = "https://gateway.polymarket.us";
const KALSHI_WS: &str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
const POLY_WS: &str = "wss://api.polymarket.us/v1/ws/markets";
const KALSHI_TAKER_FEE_RATE: f64 = 0.07;
const POLYMARKET_US_TAKER_FEE_RATE: f64 = 0.0695;
const MIN_NET_PROFIT_DOLLARS: f64 = 0.01;
const MAX_SIMULATED_CONTRACTS: u64 = 25;
const MAX_AGE: Duration = Duration::from_secs(2);
const DISCOVERY_REFRESH: Duration = Duration::from_secs(300);
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(30);
const MAX_MARKETS_PER_SUBSCRIPTION: usize = 100;

#[derive(Clone)]
struct Event {
    id: String,
    title: String,
}
#[derive(Clone)]
struct Pair {
    sport: &'static str,
    title: String,
    kalshi: [String; 2],
    teams: [String; 2],
    poly_slug: String,
    poly_long_team: String,
}
#[derive(Default)]
struct KalshiBook {
    no: BTreeMap<i32, f64>,
    updated: Option<Instant>,
    generation: u64,
}
#[derive(Default)]
struct PolyBook {
    bids: BTreeMap<i32, f64>,
    offers: BTreeMap<i32, f64>,
    updated: Option<Instant>,
    generation: u64,
}

#[derive(Debug, Clone)]
struct Fill {
    contracts: u64,
    cost: f64,
    fee: f64,
    levels: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Observation {
    kalshi_generation: u64,
    polymarket_generation: u64,
}

fn number(v: Option<&Value>) -> Option<f64> {
    v.and_then(|v| v.as_f64().or_else(|| v.as_str()?.parse().ok()))
}
fn price(v: f64) -> i32 {
    (if v > 1.0 { v / 100.0 } else { v } * 10_000.0).round() as i32
}
fn dollar(v: i32) -> f64 {
    f64::from(v) / 10_000.0
}
fn normal(v: &str) -> String {
    v.to_lowercase()
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect()
}
fn same_team(a: &str, b: &str) -> bool {
    let (a, b) = (canonical_team(a), canonical_team(b));
    !a.is_empty()
        && !b.is_empty()
        && (a == b
            || a.contains(&b)
            || b.contains(&a)
            || (team_city(&a).is_some() && team_city(&a) == team_city(&b)))
}
fn team_city(value: &str) -> Option<&'static str> {
    match value {
        "arizona" | "arizonadiamondbacks" | "arizonacardinals" => Some("arizona"),
        "atlanta" | "atlantabraves" | "atlantafalcons" => Some("atlanta"),
        "baltimore" | "baltimoreorioles" | "baltimoreravens" => Some("baltimore"),
        "carolina" | "carolinapanthers" => Some("carolina"),
        "chicagocubs" | "chicagowhitesox" | "chicagobears" => Some("chicago"),
        "cincinnatireds" | "cincinnatibengals" => Some("cincinnati"),
        "clevelandguardians" | "clevelandbrowns" => Some("cleveland"),
        "dallas" | "dallascowboys" => Some("dallas"),
        "denver" | "denverbroncos" => Some("denver"),
        "detroit" | "detroittigers" | "detroitlions" => Some("detroit"),
        "greenbay" | "greenbaypackers" => Some("greenbay"),
        "houston" | "houstonastros" | "houstontexans" => Some("houston"),
        "indianapolis" | "indianapoliscolts" => Some("indianapolis"),
        "jacksonville" | "jacksonvillejaguars" => Some("jacksonville"),
        "kansascityroyals" | "kansascitychiefs" => Some("kansascity"),
        "lasvegas" | "lasvegasraiders" => Some("lasvegas"),
        "losangelesangels" | "losangelesdodgers" | "losangeleschargers" | "losangelesrams" => Some("losangeles"),
        "miamimarlins" | "miamidolphins" => Some("miami"),
        "minnesotatwins" | "minnesotavikings" => Some("minnesota"),
        "newengland" | "newenglandpatriots" => Some("newengland"),
        "neworleans" | "neworleanssaints" => Some("neworleans"),
        "philadelphiaphillies" | "philadelphiaeagles" => Some("philadelphia"),
        "pittsburghpirates" | "pittsburghsteelers" => Some("pittsburgh"),
        "sandiegopadres" => Some("sandiego"),
        "sanfranciscogiants" | "sanfrancisco49ers" => Some("sanfrancisco"),
        "seattlemariners" | "seattleseahawks" => Some("seattle"),
        "tampabayrays" | "tampabaybuccaneers" => Some("tampabay"),
        "tennessee" | "tennesseetitans" => Some("tennessee"),
        "washingtonnationals" | "washingtoncommanders" => Some("washington"),
        _ => None,
    }
}
fn canonical_team(value: &str) -> String {
    let normalized = normal(value);
    match normalized.as_str() {
        "aricardinals" | "arizonacardinals" => "arizonacardinals",
        "atlfalcons" | "atlantafalcons" => "atlantafalcons",
        "bufbills" | "buffalobills" => "buffalobills",
        "balravens" | "baltimoreravens" => "baltimoreravens",
        "carpanthers" | "carolinapanthers" => "carolinapanthers",
        "chibears" | "chicagobears" => "chicagobears",
        "cinbengals" | "cincinnatibengals" => "cincinnatibengals",
        "clebrowns" | "clevelandbrowns" => "clevelandbrowns",
        "dalcowboys" | "dallascowboys" => "dallascowboys",
        "denbroncos" | "denverbroncos" => "denverbroncos",
        "detlions" | "detroitlions" => "detroitlions",
        "gbpackers" | "greenbaypackers" => "greenbaypackers",
        "houtexans" | "houstontexans" => "houstontexans",
        "indcolts" | "indianapoliscolts" => "indianapoliscolts",
        "jacjaguars" | "jacksonvillejaguars" => "jacksonvillejaguars",
        "kcchiefs" | "kansascitychiefs" => "kansascitychiefs",
        "lachargers" | "losangelesc" | "losangeleschargers" => "losangeleschargers",
        "larams" | "losangelesr" | "losangelesrams" => "losangelesrams",
        "lvraiders" | "lasvegasraiders" => "lasvegasraiders",
        "miadolphins" | "miamidolphins" => "miamidolphins",
        "minvikings" | "minnesotavikings" => "minnesotavikings",
        "nepatriots" | "newenglandpatriots" => "newenglandpatriots",
        "nosaints" | "neworleanssaints" => "neworleanssaints",
        "nyjets" | "newyorkj" | "newyorkjets" => "newyorkjets",
        "nygiants" | "newyorkg" | "newyorkgiants" => "newyorkgiants",
        "phieagles" | "philadelphiaeagles" => "philadelphiaeagles",
        "pitsteelers" | "pittsburghsteelers" => "pittsburghsteelers",
        "sf49ers" | "sanfrancisco49ers" => "sanfrancisco49ers",
        "seaseahawks" | "seattleseahawks" => "seattleseahawks",
        "tbbuccaneers" | "tampabaybuccaneers" => "tampabaybuccaneers",
        "tentitans" | "tennesseetitans" => "tennesseetitans",
        "wascommanders" | "washingtoncommanders" => "washingtoncommanders",
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
fn words(v: &str) -> HashSet<String> {
    v.to_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|w| {
            !w.is_empty()
                && ![
                    "a", "an", "and", "at", "for", "in", "of", "the", "to", "vs", "will",
                ]
                .contains(w)
        })
        .map(str::to_owned)
        .collect()
}
fn similarity(a: &str, b: &str) -> f64 {
    let (a, b) = (words(a), words(b));
    if a.is_empty() || b.is_empty() {
        0.0
    } else {
        a.intersection(&b).count() as f64 / a.union(&b).count() as f64
    }
}
fn event_team_keys(value: &str) -> Option<[String; 2]> {
    let teams: Vec<_> = value.split("vs").map(canonical_team).collect();
    (teams.len() == 2).then(|| [teams[0].clone(), teams[1].clone()])
}
fn event_similarity(a: &str, b: &str) -> f64 {
    let title_similarity = similarity(a, b);
    match (event_team_keys(a), event_team_keys(b)) {
        (Some(a), Some(b)) => {
            let shared = a
                .iter()
                .filter(|team| b.iter().any(|other| same_team(team, other)))
                .count();
            title_similarity.max(shared as f64 / 2.0)
        }
        _ => title_similarity,
    }
}

async fn get(client: &Client, url: String) -> Result<Value, Box<dyn Error>> {
    Ok(client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

fn events(payload: &Value, id_field: &str) -> Vec<Event> {
    payload["events"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|event| {
            Some(Event {
                id: event[id_field].as_str()?.into(),
                title: event["title"].as_str()?.into(),
            })
        })
        .collect()
}

async fn kalshi_events(client: &Client, series: &str) -> Result<Vec<Event>, Box<dyn Error>> {
    let mut all = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let mut url = Url::parse(&format!("{KALSHI_REST}/events"))?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("series_ticker", series);
            query.append_pair("status", "open");
            query.append_pair("limit", "100");
            if let Some(cursor) = &cursor {
                query.append_pair("cursor", cursor);
            }
        }
        let payload = get(client, url.into()) .await?;
        all.extend(events(&payload, "event_ticker"));
        cursor = payload["cursor"]
            .as_str()
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        if cursor.is_none() {
            return Ok(all);
        }
    }
}

async fn polymarket_events(client: &Client, league: &str) -> Result<Vec<Event>, Box<dyn Error>> {
    let mut all = Vec::new();
    let mut offset = 0;
    loop {
        let payload = get(
            client,
            format!("{POLY_REST}/v2/leagues/{league}/events?limit=100&offset={offset}"),
        )
        .await?;
        let page = events(&payload, "slug");
        let count = page.len();
        all.extend(page);
        if count < 100 {
            return Ok(all);
        }
        offset += count;
    }
}

async fn discover(
    client: &Client,
    sport: Sport,
) -> Result<Vec<Pair>, Box<dyn Error>> {
    let (k, p) = tokio::join!(
        kalshi_events(client, sport.kalshi_series),
        polymarket_events(client, sport.polymarket_league)
    );
    let k = k?;
    let p = p?;
    println!(
        "{} discovery: {} Kalshi open events | {} Polymarket open events",
        sport.label,
        k.len(),
        p.len()
    );
    let mut remaining = k;
    let mut matches = Vec::new();
    for p in p {
        if let Some((i, score)) = remaining
            .iter()
            .enumerate()
            .map(|(i, k)| (i, event_similarity(&k.title, &p.title)))
            .max_by(|a, b| a.1.total_cmp(&b.1))
        {
            if score >= 0.72 {
                matches.push((remaining.remove(i), p));
            }
        }
    }
    let mut out = Vec::new();
    for (k, p) in matches {
        let (kd, pd) = tokio::join!(
            get(client, format!("{KALSHI_REST}/events/{}", k.id)),
            get(client, format!("{POLY_REST}/v1/events/slug/{}", p.id))
        );
        let kd = kd?;
        let pd = pd?;
        let km: Vec<(String, String)> = kd["markets"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|m| {
                Some((
                    m["title"].as_str()?.strip_suffix(" wins")?.into(),
                    m["ticker"].as_str()?.into(),
                ))
            })
            .collect();
        let Some(pm) = pd["event"]["markets"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|m| {
                m["question"]
                    .as_str()
                    .is_some_and(|q| q.to_lowercase().contains("who will win in the upcoming"))
            })
        else {
            continue;
        };
        let Some(slug) = pm["slug"].as_str() else {
            continue;
        };
        let ps: Vec<String> = pm["marketSides"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|s| {
                s["team"]["safeName"]
                    .as_str()
                    .or_else(|| s["team"]["name"].as_str())
                    .map(str::to_owned)
            })
            .collect();
        if km.len() != 2 || ps.len() != 2 {
            continue;
        }
        let mut teams = [String::new(), String::new()];
        let mut tickers = [String::new(), String::new()];
        for (i, (team, ticker)) in km.into_iter().enumerate() {
            if let Some(pt) = ps.iter().find(|pt| same_team(&team, pt)) {
                teams[i] = pt.clone();
                tickers[i] = ticker;
            }
        }
        if teams.iter().all(|t| !t.is_empty()) {
            out.push(Pair {
                sport: sport.label,
                title: p.title,
                kalshi: tickers,
                teams,
                poly_slug: slug.into(),
                poly_long_team: ps[0].clone(),
            });
        }
    }
    Ok(out)
}

fn now() -> Result<String, Box<dyn Error>> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .to_string())
}
fn kalshi_sig(t: &str) -> Result<String, Box<dyn Error>> {
    let pem = fs::read_to_string(env::var("KALSHI_PRIVATE_KEY_PATH")?)?;
    let msg = format!("{t}GET/trade-api/ws/v2");
    if let Ok(k) = ed25519_dalek::SigningKey::from_pkcs8_pem(&pem) {
        return Ok(BASE64.encode(k.sign(msg.as_bytes()).to_bytes()));
    };
    let k = RsaPrivateKey::from_pkcs1_pem(&pem).or_else(|_| RsaPrivateKey::from_pkcs8_pem(&pem))?;
    Ok(BASE64.encode(
        BlindedSigningKey::<Sha256>::new(k)
            .sign_with_rng(&mut OsRng, msg.as_bytes())
            .to_bytes(),
    ))
}
fn poly_sig(t: &str) -> Result<String, Box<dyn Error>> {
    let raw = BASE64.decode(env::var("POLYMARKET_US_SECRET_KEY")?)?;
    let raw: [u8; 32] = raw
        .get(..32)
        .ok_or("POLYMARKET_US_SECRET_KEY is too short")?
        .try_into()?;
    Ok(BASE64.encode(
        ed25519_dalek::SigningKey::from_bytes(&raw)
            .sign(format!("{t}GET/v1/ws/markets").as_bytes())
            .to_bytes(),
    ))
}
fn request(
    url: &str,
    headers: &[(&str, String)],
) -> Result<tokio_tungstenite::tungstenite::http::Request<()>, Box<dyn Error>> {
    let mut r = url.into_client_request()?;
    for (n, v) in headers {
        r.headers_mut().insert(
            HeaderName::from_bytes(n.as_bytes())?,
            HeaderValue::from_str(v)?,
        );
    }
    Ok(r)
}

fn set_levels(target: &mut BTreeMap<i32, f64>, levels: Option<&Vec<Value>>) {
    target.clear();
    for level in levels.into_iter().flatten() {
        let p = number(level.get(0).or_else(|| level.pointer("/px/value")));
        let q = number(level.get(1).or_else(|| level.get("qty")));
        if let (Some(p), Some(q)) = (p, q) {
            if q > 0.0 {
                target.insert(price(p), q);
            }
        }
    }
}
fn k_snapshot(payload: &Value, books: &mut HashMap<String, KalshiBook>) {
    let Some(t) = payload
        .pointer("/msg/market_ticker")
        .and_then(Value::as_str)
    else {
        return;
    };
    let book = books.entry(t.into()).or_default();
    set_levels(
        &mut book.no,
        payload
            .pointer("/msg/no_dollars_fp")
            .and_then(Value::as_array),
    );
    book.updated = Some(Instant::now());
    book.generation += 1;
}
fn k_delta(payload: &Value, books: &mut HashMap<String, KalshiBook>) {
    let msg = &payload["msg"];
    let (Some(t), Some(p), Some(d), Some(side)) = (
        msg["market_ticker"].as_str(),
        number(msg.get("price_dollars")),
        number(msg.get("delta_fp")),
        msg["side"].as_str(),
    ) else {
        return;
    };
    if side != "no" {
        return;
    };
    let book = books.entry(t.into()).or_default();
    let key = price(p);
    let next = book.no.get(&key).copied().unwrap_or(0.0) + d;
    if next <= 0.0 {
        book.no.remove(&key);
    } else {
        book.no.insert(key, next);
    }
    book.updated = Some(Instant::now());
    book.generation += 1;
}
fn p_book(payload: &Value, books: &mut HashMap<String, PolyBook>) {
    let Some(data) = payload.get("marketData") else {
        return;
    };
    let Some(slug) = data["marketSlug"].as_str() else {
        return;
    };
    let book = books.entry(slug.into()).or_default();
    set_levels(&mut book.bids, data.get("bids").and_then(Value::as_array));
    set_levels(
        &mut book.offers,
        data.get("offers").and_then(Value::as_array),
    );
    book.updated = Some(Instant::now());
    book.generation += 1;
}

fn kalshi_taker_fee(price: f64, contracts: u64) -> f64 {
    // Kalshi rounds its market-order fee up to a centicent ($0.0001).
    (KALSHI_TAKER_FEE_RATE * contracts as f64 * price * (1.0 - price) * 10_000.0).ceil() / 10_000.0
}

fn bankers_round_to_cents(value: f64) -> f64 {
    let cents = value * 100.0;
    let lower = cents.floor();
    let fraction = cents - lower;
    let rounded = if (fraction - 0.5).abs() < 1e-9 {
        if (lower as i64).rem_euclid(2) == 0 {
            lower
        } else {
            lower + 1.0
        }
    } else {
        cents.round()
    };
    rounded / 100.0
}

fn polymarket_us_taker_fee(price: f64, contracts: u64) -> f64 {
    bankers_round_to_cents(POLYMARKET_US_TAKER_FEE_RATE * contracts as f64 * price * (1.0 - price))
}

fn kalshi_yes_fill(book: &KalshiBook, target: u64) -> Fill {
    let mut fill = Fill {
        contracts: 0,
        cost: 0.0,
        fee: 0.0,
        levels: 0,
    };
    for (&no_bid, &available) in book.no.iter().rev() {
        let quantity = (available.floor() as u64).min(target - fill.contracts);
        if quantity == 0 {
            continue;
        }
        let price = 1.0 - dollar(no_bid);
        fill.contracts += quantity;
        fill.cost += price * quantity as f64;
        fill.fee += kalshi_taker_fee(price, quantity);
        fill.levels += 1;
        if fill.contracts == target {
            break;
        }
    }
    fill
}

fn polymarket_fill(book: &PolyBook, buy_long: bool, target: u64) -> Fill {
    let mut fill = Fill {
        contracts: 0,
        cost: 0.0,
        fee: 0.0,
        levels: 0,
    };
    let levels: Box<dyn Iterator<Item = (&i32, &f64)>> = if buy_long {
        Box::new(book.offers.iter())
    } else {
        Box::new(book.bids.iter().rev())
    };
    for (&book_price, &available) in levels {
        let quantity = (available.floor() as u64).min(target - fill.contracts);
        if quantity == 0 {
            continue;
        }
        let price = if buy_long {
            dollar(book_price)
        } else {
            1.0 - dollar(book_price)
        };
        fill.contracts += quantity;
        fill.cost += price * quantity as f64;
        fill.fee += polymarket_us_taker_fee(price, quantity);
        fill.levels += 1;
        if fill.contracts == target {
            break;
        }
    }
    fill
}

fn candidate(
    pairs: &[Pair],
    kb: &HashMap<String, KalshiBook>,
    pb: &HashMap<String, PolyBook>,
    pending: &mut HashMap<String, Observation>,
    emitted: &mut HashMap<String, Observation>,
) -> Result<(), Box<dyn Error>> {
    for pair in pairs {
        let Some(poly) = pb.get(&pair.poly_slug) else {
            continue;
        };
        if poly.updated.is_none_or(|t| t.elapsed() > MAX_AGE) {
            continue;
        };
        if poly.bids.is_empty() || poly.offers.is_empty() {
            continue;
        }
        for i in 0..2 {
            let Some(k) = kb.get(&pair.kalshi[i]) else {
                continue;
            };
            if k.updated.is_none_or(|t| t.elapsed() > MAX_AGE) {
                continue;
            };
            if k.no.is_empty() {
                continue;
            }
            let opposite_team = &pair.teams[1 - i];
            let opposite_is_long = same_team(opposite_team, &pair.poly_long_team);
            let kalshi_initial = kalshi_yes_fill(k, MAX_SIMULATED_CONTRACTS);
            let polymarket_initial =
                polymarket_fill(poly, opposite_is_long, MAX_SIMULATED_CONTRACTS);
            let contracts = kalshi_initial.contracts.min(polymarket_initial.contracts);
            if contracts == 0 {
                continue;
            }
            let kalshi_fill = kalshi_yes_fill(k, contracts);
            let polymarket_fill = polymarket_fill(poly, opposite_is_long, contracts);
            let key = format!("{}:{}", pair.title, pair.teams[i]);
            let gross_profit = contracts as f64 - kalshi_fill.cost - polymarket_fill.cost;
            let net_profit = gross_profit - kalshi_fill.fee - polymarket_fill.fee;
            let observation = Observation {
                kalshi_generation: k.generation,
                polymarket_generation: poly.generation,
            };
            if contracts > 0 && net_profit >= MIN_NET_PROFIT_DOLLARS {
                let Some(previous) = pending.get(&key).copied() else {
                    pending.insert(key, observation);
                    continue;
                };
                if observation.kalshi_generation <= previous.kalshi_generation
                    || observation.polymarket_generation <= previous.polymarket_generation
                {
                    continue;
                }
                if emitted.get(&key).copied() == Some(observation) {
                    continue;
                }
                emitted.insert(key.clone(), observation);
                pending.insert(key, observation);
                println!(
                    "CONFIRMED NET CANDIDATE [{}] +${net_profit:.2} on {contracts} contracts | {} | Kalshi {} ${:.4} avg + ${:.4} fee ({} levels), Polymarket {} ${:.4} avg + ${:.4} fee ({} levels)",
                    pair.sport,
                    pair.title,
                    pair.teams[i],
                    kalshi_fill.cost / contracts as f64,
                    kalshi_fill.fee,
                    kalshi_fill.levels,
                    opposite_team,
                    polymarket_fill.cost / contracts as f64,
                    polymarket_fill.fee,
                    polymarket_fill.levels,
                );
                let line = json!({"kind":"confirmed_net_candidate","sport":pair.sport,"event":pair.title,"kalshi_outcome":pair.teams[i],"kalshi_average_price":kalshi_fill.cost/contracts as f64,"kalshi_fee":kalshi_fill.fee,"kalshi_levels":kalshi_fill.levels,"polymarket_outcome":opposite_team,"polymarket_average_price":polymarket_fill.cost/contracts as f64,"polymarket_fee":polymarket_fill.fee,"polymarket_levels":polymarket_fill.levels,"contracts":contracts,"gross_profit":gross_profit,"net_profit":net_profit,"note":"dry run only; settlement-rule parity and fill risk remain unverified"});
                let mut file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("scanner-candidates.jsonl")?;
                writeln!(file, "{}", serde_json::to_string(&line)?)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairs_each_kalshi_outcome_with_the_opposite_polymarket_outcome() {
        let pair = Pair {
            sport: "test",
            title: "A vs B".into(),
            kalshi: ["K-A".into(), "K-B".into()],
            teams: ["A".into(), "B".into()],
            poly_slug: "pm-a-b".into(),
            poly_long_team: "A".into(),
        };
        assert!(!same_team(&pair.teams[1], &pair.poly_long_team));
        assert!(same_team(&pair.teams[0], &pair.poly_long_team));
    }

    #[test]
    fn splits_subscriptions_before_the_market_cap() {
        let pair = Pair {
            sport: "test",
            title: "A vs B".into(),
            kalshi: ["K-A".into(), "K-B".into()],
            teams: ["A".into(), "B".into()],
            poly_slug: "pm-a-b".into(),
            poly_long_team: "A".into(),
        };
        let pairs = (0..51)
            .map(|index| Pair {
                kalshi: [format!("K-{index}-A"), format!("K-{index}-B")],
                poly_slug: format!("pm-{index}"),
                ..pair.clone()
            })
            .collect::<Vec<_>>();
        let batches = subscription_batches(&pairs);
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 50);
        assert_eq!(batches[1].len(), 1);
    }

    #[test]
    fn fees_remove_a_half_cent_gross_edge_at_mid_prices() {
        let contracts = 10;
        let gross = (1.0 - 0.500 - 0.495) * contracts as f64;
        let net =
            gross - kalshi_taker_fee(0.500, contracts) - polymarket_us_taker_fee(0.495, contracts);
        assert!(net < 0.0);
    }

    #[test]
    fn depth_fill_consumes_multiple_levels_at_their_actual_prices() {
        let mut no = BTreeMap::new();
        no.insert(4_200, 2.0); // Buying YES costs $0.5800.
        no.insert(4_100, 2.0); // Buying YES costs $0.5900.
        let fill = kalshi_yes_fill(
            &KalshiBook {
                no,
                ..Default::default()
            },
            3,
        );
        assert_eq!(fill.contracts, 3);
        assert_eq!(fill.levels, 2);
        assert!((fill.cost - 1.75).abs() < 1e-9);
        assert!(fill.fee > 0.0);
    }

    #[test]
    fn matches_abbreviated_and_surname_only_event_titles() {
        assert_eq!(
            event_similarity("New York M vs Washington", "New York Mets vs. Washington Nationals"),
            1.0
        );
        assert_eq!(
            event_similarity("Dart vs Lemaitre", "Harriet Dart vs. Tiphanie Lemaitre"),
            1.0
        );
    }

    #[test]
    fn matches_abbreviated_nfl_team_names() {
        assert_eq!(
            event_similarity("Los Angeles C vs Buffalo", "LA Chargers vs BUF Bills"),
            1.0
        );
        assert_eq!(
            event_similarity("New York J vs Detroit", "NY Jets vs DET Lions"),
            1.0
        );
    }
}

async fn run_session(pairs: &[Pair]) -> Result<(), Box<dyn Error>> {
    let tickers: Vec<String> = pairs
        .iter()
        .flat_map(|pair| pair.kalshi.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let slugs: Vec<String> = pairs
        .iter()
        .map(|pair| pair.poly_slug.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if tickers.len() > MAX_MARKETS_PER_SUBSCRIPTION || slugs.len() > MAX_MARKETS_PER_SUBSCRIPTION {
        return Err("a venue subscription exceeds the 100-market limit".into());
    }
    let t = now()?;
    let kr = request(
        KALSHI_WS,
        &[
            ("KALSHI-ACCESS-KEY", env::var("KALSHI_API_KEY_ID")?),
            ("KALSHI-ACCESS-TIMESTAMP", t.clone()),
            ("KALSHI-ACCESS-SIGNATURE", kalshi_sig(&t)?),
        ],
    )?;
    let t = now()?;
    let pr = request(
        POLY_WS,
        &[
            ("X-PM-Access-Key", env::var("POLYMARKET_US_KEY_ID")?),
            ("X-PM-Timestamp", t.clone()),
            ("X-PM-Signature", poly_sig(&t)?),
        ],
    )?;
    let (mut ks, _) = connect_async(kr).await?;
    let (mut ps, _) = connect_async(pr).await?;
    ks.send(Message::Text(json!({"id":1,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":tickers}}).to_string().into())).await?;
    ps.send(Message::Text(json!({"subscribe":{"requestId":"read-only-scanner","subscriptionType":"SUBSCRIPTION_TYPE_MARKET_DATA","marketSlugs":slugs}}).to_string().into())).await?;
    println!(
        "Read-only streaming scanner: {} matched games. Fresh books required; no order routes exist.",
        pairs.len()
    );
    // These books intentionally exist only for one connection session. A reconnect
    // starts empty and waits for new snapshots before any candidate can be emitted.
    let (mut kb, mut pb, mut pending, mut emitted) = (
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
    );
    let refresh_timer = tokio::time::sleep(DISCOVERY_REFRESH);
    tokio::pin!(refresh_timer);
    loop {
        tokio::select! {
            _ = &mut refresh_timer => return Ok(()),
            message = ks.next() => {
                let message = message.ok_or("Kalshi WebSocket closed")??;
                if let Ok(value) = serde_json::from_str::<Value>(message.to_text().unwrap_or("")) {
                    match value["type"].as_str() {
                        Some("orderbook_snapshot") => k_snapshot(&value, &mut kb),
                        Some("orderbook_delta") => k_delta(&value, &mut kb),
                        _ => {}
                    }
                    candidate(pairs, &kb, &pb, &mut pending, &mut emitted)?;
                }
            }
            message = ps.next() => {
                let message = message.ok_or("Polymarket WebSocket closed")??;
                if let Ok(value) = serde_json::from_str::<Value>(message.to_text().unwrap_or("")) {
                    p_book(&value, &mut pb);
                    candidate(pairs, &kb, &pb, &mut pending, &mut emitted)?;
                }
            }
        }
    }
}

fn subscription_batches(pairs: &[Pair]) -> Vec<Vec<Pair>> {
    let mut batches: Vec<Vec<Pair>> = Vec::new();
    let mut tickers = HashSet::new();
    let mut slugs = HashSet::new();
    for pair in pairs {
        let next_tickers: HashSet<_> = pair.kalshi.iter().cloned().collect();
        let adds_tickers = next_tickers.iter().filter(|ticker| !tickers.contains(*ticker)).count();
        let adds_slug = usize::from(!slugs.contains(&pair.poly_slug));
        if !batches.is_empty()
            && (tickers.len() + adds_tickers > MAX_MARKETS_PER_SUBSCRIPTION
                || slugs.len() + adds_slug > MAX_MARKETS_PER_SUBSCRIPTION)
        {
            batches.push(Vec::new());
            tickers.clear();
            slugs.clear();
        }
        if batches.is_empty() {
            batches.push(Vec::new());
        }
        tickers.extend(next_tickers);
        slugs.insert(pair.poly_slug.clone());
        batches.last_mut().expect("batch exists").push(pair.clone());
    }
    batches
}

async fn run_batches(pairs: &[Pair]) -> Result<(), Box<dyn Error>> {
    let batches = subscription_batches(pairs);
    println!(
        "Subscription plan: {} matched games across {} independent batch(es).",
        pairs.len(),
        batches.len()
    );
    let results = join_all(batches.iter().map(|batch| run_session(batch))).await;
    for result in results {
        result?;
    }
    Ok(())
}

fn reconnect_delay(attempt: u32) -> Duration {
    let seconds = 2_u64.saturating_pow(attempt.min(4));
    Duration::from_secs(seconds).min(MAX_RECONNECT_BACKOFF)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();
    let selection = env::args().nth(1).unwrap_or_else(|| "cfb".into());
    if selection == "all" {
        let cfb = scan_forever("cfb", selected_sports("cfb").expect("supported selection"));
        let nfl = scan_forever("nfl", selected_sports("nfl").expect("supported selection"));
        let mlb = scan_forever("mlb", selected_sports("mlb").expect("supported selection"));
        let tennis = scan_forever(
            "tennis",
            selected_sports("tennis").expect("supported selection"),
        );
        tokio::try_join!(cfb, nfl, mlb, tennis)?;
        return Ok(());
    }
    let sports = selected_sports(&selection)
        .ok_or_else(|| format!("Usage: cargo run --bin stream_scanner -- {SCANNER_USAGE}"))?;
    scan_forever(&selection, sports).await
}

async fn scan_forever(selection: &str, sports: &[Sport]) -> Result<(), Box<dyn Error>> {
    let client = Client::builder()
        .user_agent("arbitrage-executor-read-only/0.1")
        .build()?;
    let mut reconnect_attempt = 0;
    loop {
        let pairs = match async {
            let mut pairs = Vec::new();
            for sport in sports {
                pairs.extend(discover(&client, *sport).await?);
            }
            Ok::<_, Box<dyn Error>>(pairs)
        }
        .await
        {
            Ok(pairs) if !pairs.is_empty() => pairs,
            Ok(_) => {
                eprintln!("{selection}: no matched two-way moneylines found; retrying discovery shortly.");
                tokio::time::sleep(reconnect_delay(reconnect_attempt)).await;
                reconnect_attempt += 1;
                continue;
            }
            Err(error) => {
                eprintln!("{selection}: market discovery failed: {error}. Retrying shortly.");
                tokio::time::sleep(reconnect_delay(reconnect_attempt)).await;
                reconnect_attempt += 1;
                continue;
            }
        };
        match run_batches(&pairs).await {
            Ok(()) => {
                println!("{selection}: refreshing matched-market subscriptions.");
                reconnect_attempt = 0;
            }
            Err(error) => {
                let delay = reconnect_delay(reconnect_attempt);
                eprintln!(
                    "{selection}: market-data stream ended: {error}. Reconnecting in {}s.",
                    delay.as_secs()
                );
                tokio::time::sleep(delay).await;
                reconnect_attempt += 1;
            }
        }
    }
}
