//! Continuous, read-only cross-venue scanner.
//!
//! It only opens market-data WebSockets, reads account balances, and appends
//! net-fee candidates to a local journal. There are intentionally no order,
//! cancel, or portfolio APIs.

use arbitrage_executor::sports::{SCANNER_USAGE, Sport, selected_sports};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{Local, NaiveDate};
use ed25519_dalek::{Signer as _, pkcs8::DecodePrivateKey as _};
use futures_util::{
    SinkExt as _, StreamExt as _,
    future::{join_all, try_join_all},
};
use rand::rngs::OsRng;
use reqwest::{Client, StatusCode, Url};
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
    sync::{Mutex, OnceLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;
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
const MIN_NET_PROFIT_DOLLARS: f64 = 0.25;
const MAX_SIMULATED_CONTRACTS: u64 = 25;
const MAX_AGE: Duration = Duration::from_secs(2);
const DISCOVERY_REFRESH: Duration = Duration::from_secs(300);
const DISCOVERY_PREFETCH_LEAD: Duration = Duration::from_secs(60);
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(30);
const MAX_MARKETS_PER_SUBSCRIPTION: usize = 100;
const POLY_PRIVATE_REST: &str = "https://api.polymarket.us";
const KALSHI_REQUEST_SPACING: Duration = Duration::from_millis(125);
const KALSHI_RATE_LIMIT_RETRIES: u32 = 5;
static NEXT_KALSHI_REQUEST: OnceLock<Mutex<Instant>> = OnceLock::new();

#[derive(Clone)]
struct Event {
    id: String,
    title: String,
}

#[derive(Default)]
struct MarketCounts {
    games: usize,
    moneylines: usize,
    spreads: usize,
    totals: usize,
    props: usize,
}

#[derive(Default)]
struct KalshiSeriesEvents {
    events: Vec<Event>,
    details: HashMap<String, Value>,
    open_markets: usize,
    two_way_moneylines: usize,
}
#[derive(Clone)]
struct Pair {
    sport: &'static str,
    title: String,
    market: String,
    kalshi: [String; 2],
    teams: [String; 2],
    poly_slug: String,
    poly_long_is_first: bool,
}

struct PairIndex<'a> {
    by_kalshi: HashMap<&'a str, Vec<usize>>,
    by_polymarket: HashMap<&'a str, Vec<usize>>,
}

fn pair_index(pairs: &[Pair]) -> PairIndex<'_> {
    let mut index = PairIndex {
        by_kalshi: HashMap::new(),
        by_polymarket: HashMap::new(),
    };
    for (position, pair) in pairs.iter().enumerate() {
        for ticker in pair.kalshi.iter().filter(|ticker| !ticker.is_empty()) {
            index
                .by_kalshi
                .entry(ticker.as_str())
                .or_default()
                .push(position);
        }
        index
            .by_polymarket
            .entry(pair.poly_slug.as_str())
            .or_default()
            .push(position);
    }
    index
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PropKind {
    Player,
    Team,
}

#[derive(Clone, Copy)]
struct PropSeries {
    kalshi_series: &'static str,
    kind: PropKind,
    /// Exact normalized Polymarket sports-market type. A different statistic
    /// must never be treated as the opposite side of this prop.
    poly_type: &'static str,
    label: &'static str,
}

const NFL_PROP_SERIES: &[PropSeries] = &[
    PropSeries {
        kalshi_series: "KXNFLPASSYDS",
        kind: PropKind::Player,
        poly_type: "footballplayerfullgamepassingyards",
        label: "Passing yards",
    },
    PropSeries {
        kalshi_series: "KXNFLRSHYDS",
        kind: PropKind::Player,
        poly_type: "footballplayerfullgamerushingyards",
        label: "Rushing yards",
    },
    PropSeries {
        kalshi_series: "KXNFLRECYDS",
        kind: PropKind::Player,
        poly_type: "footballplayerfullgamereceivingyards",
        label: "Receiving yards",
    },
    PropSeries {
        kalshi_series: "KXNFLREC",
        kind: PropKind::Player,
        poly_type: "footballplayerfullgamereceptions",
        label: "Receptions",
    },
    PropSeries {
        kalshi_series: "KXNFLTEAMTOTAL",
        kind: PropKind::Team,
        poly_type: "footballteamfullgametotal",
        label: "Team points",
    },
    PropSeries {
        kalshi_series: "KXNFLTEAMYDS",
        kind: PropKind::Team,
        poly_type: "footballteamfullgameyards",
        label: "Team yards",
    },
    PropSeries {
        kalshi_series: "KXNFLTEAMRSHYDS",
        kind: PropKind::Team,
        poly_type: "footballteamfullgamerushingyards",
        label: "Team rushing yards",
    },
    PropSeries {
        kalshi_series: "KXNFLTEAMRECYDS",
        kind: PropKind::Team,
        poly_type: "footballteamfullgamereceivingyards",
        label: "Team receiving yards",
    },
];

const CFB_PROP_SERIES: &[PropSeries] = &[
    PropSeries {
        kalshi_series: "KXNCAAFTEAMTOTAL",
        kind: PropKind::Team,
        poly_type: "footballteamfullgametotal",
        label: "Team points",
    },
    PropSeries {
        kalshi_series: "KXNCAAFTEAMYDS",
        kind: PropKind::Team,
        poly_type: "footballteamfullgameyards",
        label: "Team yards",
    },
    PropSeries {
        kalshi_series: "KXNCAAFTEAMRSHYDS",
        kind: PropKind::Team,
        poly_type: "footballteamfullgamerushingyards",
        label: "Team rushing yards",
    },
    PropSeries {
        kalshi_series: "KXNCAAFTEAMRECYDS",
        kind: PropKind::Team,
        poly_type: "footballteamfullgamereceivingyards",
        label: "Team receiving yards",
    },
];

#[derive(Clone, Copy)]
struct SpreadSeries {
    kalshi_series: &'static str,
    /// The period token used by Polymarket's sportsMarketType after normalization.
    /// An empty token denotes full game.
    period: &'static str,
    label: &'static str,
}

const NFL_SPREAD_SERIES: &[SpreadSeries] = &[
    SpreadSeries {
        kalshi_series: "KXNFLSPREAD",
        period: "",
        label: "Full game",
    },
    SpreadSeries {
        kalshi_series: "KXNFL1HSPREAD",
        period: "firsthalf",
        label: "1st half",
    },
    SpreadSeries {
        kalshi_series: "KXNFL2HSPREAD",
        period: "secondhalf",
        label: "2nd half",
    },
    SpreadSeries {
        kalshi_series: "KXNFL1QSPREAD",
        period: "firstquarter",
        label: "1st quarter",
    },
    SpreadSeries {
        kalshi_series: "KXNFL2QSPREAD",
        period: "secondquarter",
        label: "2nd quarter",
    },
    SpreadSeries {
        kalshi_series: "KXNFL3QSPREAD",
        period: "thirdquarter",
        label: "3rd quarter",
    },
    SpreadSeries {
        kalshi_series: "KXNFL4QSPREAD",
        period: "fourthquarter",
        label: "4th quarter",
    },
];

const CFB_SPREAD_SERIES: &[SpreadSeries] = &[
    SpreadSeries {
        kalshi_series: "KXNCAAFSPREAD",
        period: "",
        label: "Full game",
    },
    SpreadSeries {
        kalshi_series: "KXNCAAF1HSPREAD",
        period: "firsthalf",
        label: "1st half",
    },
    SpreadSeries {
        kalshi_series: "KXNCAAF2HSPREAD",
        period: "secondhalf",
        label: "2nd half",
    },
    SpreadSeries {
        kalshi_series: "KXNCAAF1QSPREAD",
        period: "firstquarter",
        label: "1st quarter",
    },
    SpreadSeries {
        kalshi_series: "KXNCAAF2QSPREAD",
        period: "secondquarter",
        label: "2nd quarter",
    },
    SpreadSeries {
        kalshi_series: "KXNCAAF3QSPREAD",
        period: "thirdquarter",
        label: "3rd quarter",
    },
    SpreadSeries {
        kalshi_series: "KXNCAAF4QSPREAD",
        period: "fourthquarter",
        label: "4th quarter",
    },
];
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

#[derive(serde::Serialize)]
struct BalanceSnapshot {
    kalshi: String,
    polymarket: String,
    buying_power: String,
}

struct DisplaySide<'a> {
    outcome: &'a str,
    average_price: f64,
    fee: f64,
    levels: usize,
}

fn format_candidate_log(
    timestamp: &str,
    event: &str,
    market: &str,
    net_profit: f64,
    contracts: u64,
    kalshi: DisplaySide<'_>,
    polymarket: DisplaySide<'_>,
    balances: &BalanceSnapshot,
) -> String {
    format!(
        "---\n\n[{timestamp}] {event}\nMarket: {market}\nNet: +${net_profit:.2} on {contracts} contracts\n\n\
         Kalshi\n    {} @ ${:.4}\n    Fee: ${:.4} | Levels: {}\n\n\
         Polymarket\n    {} @ ${:.4}\n    Fee: ${:.4} | Levels: {}\n\n\
         Execution\n    Mode: Dry run\n\n\
         Balances\n    Kalshi:       {}\n    Polymarket:  {}\n    Buying power: {}\n\n---",
        kalshi.outcome,
        kalshi.average_price,
        kalshi.fee,
        kalshi.levels,
        polymarket.outcome,
        polymarket.average_price,
        polymarket.fee,
        polymarket.levels,
        balances.kalshi,
        balances.polymarket,
        balances.buying_power,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Observation {
    kalshi_generation: u64,
    polymarket_generation: u64,
}

struct CandidateRecord {
    timestamp: String,
    pair: Pair,
    kalshi_side: usize,
    kalshi_fill: Fill,
    polymarket_fill: Fill,
    contracts: u64,
    gross_profit: f64,
    net_profit: f64,
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
    let (raw_a, raw_b) = (normal(a), normal(b));
    let (a, b) = (canonical_team(a), canonical_team(b));
    let abbreviated_state = |short: &str, long: &str| {
        short
            .strip_suffix("st")
            .is_some_and(|school| !school.is_empty() && long == format!("{school}state"))
    };
    !a.is_empty()
        && !b.is_empty()
        && (a == b
            || abbreviated_state(&a, &b)
            || abbreviated_state(&b, &a)
            || (a.len() >= 4 && b.ends_with(&a) && a != "state")
            || (b.len() >= 4 && a.ends_with(&b) && b != "state")
            || (team_city(&a).is_some()
                && team_city(&a) == team_city(&b)
                && (team_city(&a) == Some(raw_a.as_str())
                    || team_city(&b) == Some(raw_b.as_str()))))
}
fn team_city(value: &str) -> Option<&'static str> {
    match value {
        "arizona" | "arizonadiamondbacks" | "arizonacardinals" => Some("arizona"),
        "atlanta" | "atlantabraves" | "atlantafalcons" => Some("atlanta"),
        "baltimore" | "baltimoreorioles" | "baltimoreravens" => Some("baltimore"),
        "buffalo" | "buffalobills" => Some("buffalo"),
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
        "losangelesangels" | "losangelesdodgers" | "losangeleschargers" | "losangelesrams" => {
            Some("losangeles")
        }
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
    let teams: Vec<_> = value
        .split(':')
        .next()
        .unwrap_or(value)
        .split("vs")
        .map(|team| team.trim().to_owned())
        .collect();
    (teams.len() == 2).then(|| [teams[0].clone(), teams[1].clone()])
}
fn event_similarity(a: &str, b: &str) -> f64 {
    let title_similarity = similarity(a, b);
    match (event_team_keys(a), event_team_keys(b)) {
        (Some(a), Some(b)) => {
            let direct = same_team(&a[0], &b[0]) && same_team(&a[1], &b[1]);
            let reversed = same_team(&a[0], &b[1]) && same_team(&a[1], &b[0]);
            if direct || reversed { 1.0 } else { 0.0 }
        }
        _ => title_similarity,
    }
}

fn kalshi_event_date(id: &str) -> Option<NaiveDate> {
    id.split_once('-')
        .and_then(|(_, suffix)| suffix.get(..7))
        .and_then(|date| NaiveDate::parse_from_str(date, "%y%b%d").ok())
}

fn polymarket_event_date(id: &str) -> Option<NaiveDate> {
    let mut pieces = id.rsplit('-');
    let day = pieces.next()?.parse().ok()?;
    let month = pieces.next()?.parse().ok()?;
    let year = pieces.next()?.parse().ok()?;
    NaiveDate::from_ymd_opt(year, month, day)
}

fn event_match_score(kalshi: &Event, polymarket: &Event, sport: Sport) -> f64 {
    if matches!(sport.label, "NFL" | "CFB") {
        let kalshi_date = kalshi_event_date(&kalshi.id);
        if kalshi_date.is_none() || kalshi_date != polymarket_event_date(&polymarket.id) {
            return 0.0;
        }
    }
    event_similarity(&kalshi.title, &polymarket.title)
}

struct EventMatcher<'a> {
    sport: Sport,
    all: Vec<&'a Event>,
    by_date: HashMap<NaiveDate, Vec<&'a Event>>,
}

impl<'a> EventMatcher<'a> {
    fn new(events: &'a [Event], sport: Sport) -> Self {
        let mut by_date: HashMap<NaiveDate, Vec<&Event>> = HashMap::new();
        if matches!(sport.label, "NFL" | "CFB") {
            for event in events {
                if let Some(date) = polymarket_event_date(&event.id) {
                    by_date.entry(date).or_default().push(event);
                }
            }
        }
        Self {
            sport,
            all: events.iter().collect(),
            by_date,
        }
    }

    fn best_match(&self, kalshi: &Event) -> Option<&'a Event> {
        let candidates = if matches!(self.sport.label, "NFL" | "CFB") {
            self.by_date
                .get(&kalshi_event_date(&kalshi.id)?)?
                .as_slice()
        } else {
            self.all.as_slice()
        };
        candidates
            .iter()
            .copied()
            .map(|event| (event, event_similarity(&kalshi.title, &event.title)))
            .max_by(|a, b| a.1.total_cmp(&b.1))
            .filter(|(_, score)| *score >= 0.72)
            .map(|(event, _)| event)
    }
}

fn kalshi_winner(title: &str) -> Option<&str> {
    title.strip_suffix(" wins").or_else(|| {
        title
            .strip_prefix("Will ")?
            .split_once(" win the ")
            .map(|(team, _)| team)
    })
}

fn market_team(market: &Value, long: bool) -> Option<String> {
    market["marketSides"]
        .as_array()?
        .iter()
        .find(|side| side["long"].as_bool() == Some(long))?["team"]["safeName"]
        .as_str()
        .or_else(|| {
            market["marketSides"]
                .as_array()?
                .iter()
                .find(|side| side["long"].as_bool() == Some(long))?["team"]["name"]
                .as_str()
        })
        .map(str::to_owned)
}

fn side_label(market: &Value, long: bool) -> Option<String> {
    market["marketSides"]
        .as_array()?
        .iter()
        .find(|side| side["long"].as_bool() == Some(long))?["description"]
        .as_str()
        .map(str::to_owned)
}

fn full_game_total(market: &Value) -> bool {
    market["marketType"].as_str() == Some("totals")
        && market["sportsMarketType"].as_str().is_some_and(|kind| {
            matches!(
                normal(kind).as_str(),
                "footballgametotal" | "footballgamefullgametotal"
            )
        })
}

fn prop_series(sport: Sport) -> &'static [PropSeries] {
    match sport.label {
        "NFL" => NFL_PROP_SERIES,
        "CFB" => CFB_PROP_SERIES,
        _ => &[],
    }
}

fn spread_series(sport: Sport) -> &'static [SpreadSeries] {
    match sport.label {
        "NFL" => NFL_SPREAD_SERIES,
        "CFB" => CFB_SPREAD_SERIES,
        _ => &[],
    }
}

fn spread_scope_matches(market: &Value, spec: SpreadSeries) -> bool {
    if market["marketType"].as_str() != Some("spreads") {
        return false;
    }
    let scope = market["sportsMarketType"]
        .as_str()
        .map(normal)
        .unwrap_or_default();
    let expected = if spec.period.is_empty() {
        "footballteamspread".to_owned()
    } else {
        format!("footballteam{}spread", spec.period)
    };
    scope == expected || (spec.period.is_empty() && scope == "footballteamfullgamespread")
}

fn full_game_prop(market: &Value, spec: PropSeries) -> bool {
    let Some(sports_type) = market["sportsMarketType"].as_str() else {
        return false;
    };
    let canonical = normal(sports_type);
    canonical == spec.poly_type && number(market.get("line")).is_some()
}

fn poly_prop_entity(market: &Value, kind: PropKind) -> Option<String> {
    match kind {
        PropKind::Team => market_team(market, true),
        PropKind::Player => {
            let side = market["marketSides"]
                .as_array()?
                .iter()
                .find(|side| side["long"].as_bool() == Some(true))?;
            market["player"]["safeName"]
                .as_str()
                .or_else(|| market["player"]["name"].as_str())
                .or_else(|| market["participant"]["safeName"].as_str())
                .or_else(|| market["participant"]["name"].as_str())
                .or_else(|| side["player"]["safeName"].as_str())
                .or_else(|| side["player"]["name"].as_str())
                .or_else(|| side["participant"]["safeName"].as_str())
                .or_else(|| side["participant"]["name"].as_str())
                .map(str::to_owned)
        }
    }
}

fn kalshi_prop_entity(market: &Value, kind: PropKind) -> Option<String> {
    let title = market["title"].as_str()?;
    match kind {
        // Kalshi's standard player props are titled, for example,
        // "Matthew Stafford: 250+ passing yards".
        PropKind::Player => title
            .split_once(':')
            .map(|(player, _)| player.trim().to_owned()),
        // Kalshi's standard team props are titled, for example,
        // "DEN Broncos over 17.5 points scored" for points and
        // "MIA Dolphins : 250+" for yardage.
        PropKind::Team => {
            if let Some((team, _)) = title.split_once(':') {
                return Some(team.trim().to_owned());
            }
            let line = number(market.get("floor_strike"))?;
            let marker = format!(" over {line}");
            title
                .to_lowercase()
                .find(&marker)
                .map(|index| title[..index].trim().to_owned())
        }
    }
}

fn same_prop_entity(a: &str, b: &str, kind: PropKind) -> bool {
    match kind {
        PropKind::Player => normal(a) == normal(b),
        PropKind::Team => same_team(a, b),
    }
}

fn pair_props(kalshi: &Value, polymarket: &Value, sport: Sport, spec: PropSeries) -> Vec<Pair> {
    let Some(poly_markets) = polymarket
        .pointer("/event/markets")
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };
    kalshi["markets"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|market| kalshi_market_open(market))
        .filter_map(|market| {
            let line = number(market.get("floor_strike"))?;
            let entity = kalshi_prop_entity(market, spec.kind)?;
            let poly = poly_markets.iter().find(|poly| {
                polymarket_market_open(poly)
                    && full_game_prop(poly, spec)
                    && number(poly.get("line")).is_some_and(|value| (value - line).abs() < 1e-9)
                    && poly_prop_entity(poly, spec.kind)
                        .is_some_and(|other| same_prop_entity(&entity, &other, spec.kind))
                    && side_label(poly, true)
                        .is_some_and(|label| label.to_lowercase().contains("over"))
                    && side_label(poly, false)
                        .is_some_and(|label| label.to_lowercase().contains("under"))
            })?;
            Some(Pair {
                sport: sport.label,
                title: polymarket["event"]["title"].as_str()?.to_owned(),
                market: match spec.kind {
                    PropKind::Player => {
                        format!("Player {} ({line})", spec.label.to_lowercase())
                    }
                    PropKind::Team => format!("{} ({line})", spec.label),
                },
                // A prop is a binary over/under market, so only Kalshi YES
                // (Over) is paired with Polymarket's opposite, Under, side.
                kalshi: [market["ticker"].as_str()?.to_owned(), String::new()],
                teams: [
                    format!("{entity} Over {line}"),
                    format!("{entity} Under {line}"),
                ],
                poly_slug: poly["slug"].as_str()?.to_owned(),
                poly_long_is_first: true,
            })
        })
        .collect()
}

fn pair_spreads(kalshi: &Value, polymarket: &Value, sport: Sport, spec: SpreadSeries) -> Vec<Pair> {
    let Some(poly_markets) = polymarket
        .pointer("/event/markets")
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };
    kalshi["markets"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|market| kalshi_market_open(market))
        .filter_map(|market| {
            let line = number(market.get("floor_strike"))?;
            // Opposing teams' integer spread contracts can both lose when the
            // winning margin lands exactly on the line.
            if ((line.fract().abs()) - 0.5).abs() >= 1e-9 {
                return None;
            }
            let (kalshi_team, remainder) = market["yes_sub_title"].as_str()?.split_once(" wins")?;
            if !remainder.contains("by over") {
                return None;
            }
            let kalshi_team = kalshi_team.trim().to_owned();
            let poly = poly_markets.iter().find(|poly| {
                if !polymarket_market_open(poly)
                    || !spread_scope_matches(poly, spec)
                    || number(poly.get("line"))
                        .is_none_or(|value| (value.abs() - line).abs() >= 1e-9)
                {
                    return false;
                }
                let long = market_team(poly, true);
                let short = market_team(poly, false);
                match (long, short, number(poly.get("line"))) {
                    (Some(long), Some(short), Some(poly_line)) => {
                        let matches_long = same_team(&kalshi_team, &long);
                        let matches_short = same_team(&kalshi_team, &short);
                        matches_long != matches_short
                            && ((matches_long && (poly_line + line).abs() < 1e-9)
                                || (matches_short && (poly_line - line).abs() < 1e-9))
                    }
                    _ => false,
                }
            })?;
            let long = market_team(poly, true)?;
            let short = market_team(poly, false)?;
            let opposite = if same_team(&kalshi_team, &long) {
                short
            } else {
                long.clone()
            };
            Some(Pair {
                sport: sport.label,
                title: polymarket["event"]["title"].as_str()?.to_owned(),
                market: format!("{} spread ({line:+})", spec.label),
                kalshi: [market["ticker"].as_str()?.to_owned(), String::new()],
                teams: [
                    format!("{kalshi_team} -{line}"),
                    format!("{opposite} +{line}"),
                ],
                poly_slug: poly["slug"].as_str()?.to_owned(),
                poly_long_is_first: same_team(&kalshi_team, &long),
            })
        })
        .collect()
}

fn pair_totals(kalshi: &Value, polymarket: &Value, sport: Sport) -> Vec<Pair> {
    let Some(poly_markets) = polymarket
        .pointer("/event/markets")
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };
    kalshi["markets"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|market| kalshi_market_open(market))
        .filter_map(|market| {
            let line = number(market.get("floor_strike"))?;
            if !market["title"]
                .as_str()?
                .to_lowercase()
                .starts_with("over ")
            {
                return None;
            }
            let poly = poly_markets.iter().find(|poly| {
                polymarket_market_open(poly)
                    && full_game_total(poly)
                    && number(poly.get("line")).is_some_and(|value| (value - line).abs() < 1e-9)
                    && side_label(poly, true)
                        .is_some_and(|label| label.to_lowercase().contains("over"))
                    && side_label(poly, false)
                        .is_some_and(|label| label.to_lowercase().contains("under"))
            })?;
            Some(Pair {
                sport: sport.label,
                title: polymarket["event"]["title"].as_str()?.to_owned(),
                market: format!("Full game total ({line})"),
                kalshi: [market["ticker"].as_str()?.to_owned(), String::new()],
                teams: [format!("Over {line}"), format!("Under {line}")],
                poly_slug: poly["slug"].as_str()?.to_owned(),
                poly_long_is_first: true,
            })
        })
        .collect()
}

fn align_moneyline(
    kalshi: Vec<(String, String)>,
    polymarket: [String; 2],
) -> Option<([String; 2], [String; 2], bool)> {
    if kalshi.len() != 2 || same_team(&polymarket[0], &polymarket[1]) {
        return None;
    }
    let mut teams = [String::new(), String::new()];
    let mut tickers = [String::new(), String::new()];
    for (i, (team, ticker)) in kalshi.into_iter().enumerate() {
        let matches = [
            same_team(&team, &polymarket[0]),
            same_team(&team, &polymarket[1]),
        ];
        if matches[0] == matches[1] {
            return None;
        }
        teams[i] = polymarket[usize::from(matches[1])].clone();
        tickers[i] = ticker;
    }
    if teams[0] == teams[1] {
        return None;
    }
    let poly_long_is_first = teams[0] == polymarket[0];
    Some((teams, tickers, poly_long_is_first))
}

async fn wait_for_kalshi_request_slot() {
    let next_request = NEXT_KALSHI_REQUEST.get_or_init(|| Mutex::new(Instant::now()));
    let delay = {
        let mut next = next_request
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let now = Instant::now();
        let slot = (*next).max(now);
        *next = slot + KALSHI_REQUEST_SPACING;
        slot.saturating_duration_since(now)
    };
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
}

async fn get(client: &Client, url: String) -> Result<Value, Box<dyn Error>> {
    let is_kalshi = url.starts_with(KALSHI_REST);
    for attempt in 0..=KALSHI_RATE_LIMIT_RETRIES {
        if is_kalshi {
            wait_for_kalshi_request_slot().await;
        }
        let response = client.get(&url).send().await?;
        if is_kalshi && response.status() == StatusCode::TOO_MANY_REQUESTS {
            if attempt < KALSHI_RATE_LIMIT_RETRIES {
                let delay = Duration::from_millis(250 * 2_u64.pow(attempt));
                tokio::time::sleep(delay).await;
                continue;
            }
        }
        return Ok(response.error_for_status()?.json().await?);
    }
    unreachable!("Kalshi request loop always returns on its final attempt")
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

fn kalshi_market_open(market: &Value) -> bool {
    market["status"]
        .as_str()
        .is_none_or(|status| matches!(status, "open" | "active"))
}

fn kalshi_event_counts(event: &Value) -> Result<(usize, bool), Box<dyn Error>> {
    let markets = event["markets"]
        .as_array()
        .ok_or("Kalshi nested event markets missing")?;
    let open = markets
        .iter()
        .filter(|market| kalshi_market_open(market))
        .collect::<Vec<_>>();
    let winners = open
        .iter()
        .filter_map(|market| market["title"].as_str().and_then(kalshi_winner))
        .collect::<Vec<_>>();
    Ok((open.len(), winners.len() == 2 && winners[0] != winners[1]))
}

async fn kalshi_events(
    client: &Client,
    series: &str,
) -> Result<KalshiSeriesEvents, Box<dyn Error>> {
    let mut all = KalshiSeriesEvents::default();
    let mut cursor: Option<String> = None;
    loop {
        let mut url = Url::parse(&format!("{KALSHI_REST}/events"))?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("series_ticker", series);
            query.append_pair("status", "open");
            query.append_pair("limit", "100");
            query.append_pair("with_nested_markets", "true");
            if let Some(cursor) = &cursor {
                query.append_pair("cursor", cursor);
            }
        }
        let payload = get(client, url.into()).await?;
        for event in payload["events"]
            .as_array()
            .ok_or("Kalshi events response missing events")?
        {
            let (open_markets, two_way_moneyline) = kalshi_event_counts(event)?;
            all.open_markets += open_markets;
            all.two_way_moneylines += usize::from(two_way_moneyline);
            if let (Some(id), Some(title)) =
                (event["event_ticker"].as_str(), event["title"].as_str())
            {
                all.events.push(Event {
                    id: id.to_owned(),
                    title: title.to_owned(),
                });
                all.details.insert(id.to_owned(), event.clone());
            }
        }
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

fn polymarket_market_open(market: &Value) -> bool {
    market["active"].as_bool() != Some(false)
        && market["closed"].as_bool() != Some(true)
        && market["archived"].as_bool() != Some(true)
}

fn polymarket_moneyline(market: &Value) -> bool {
    market["question"].as_str().is_some_and(|question| {
        question
            .to_lowercase()
            .contains("who will win in the upcoming")
    })
}

async fn polymarket_details(
    client: &Client,
    events: &[Event],
) -> Result<HashMap<String, Value>, Box<dyn Error>> {
    let mut details = HashMap::new();
    for batch in events.chunks(4) {
        let responses = join_all(
            batch
                .iter()
                .map(|event| get(client, format!("{POLY_REST}/v1/events/slug/{}", event.id))),
        )
        .await;
        for (event, response) in batch.iter().zip(responses) {
            details.insert(event.id.clone(), response?);
        }
    }
    Ok(details)
}

fn polymarket_counts(
    details: &HashMap<String, Value>,
    sport: Sport,
) -> Result<MarketCounts, Box<dyn Error>> {
    let mut counts = MarketCounts {
        games: details.len(),
        ..Default::default()
    };
    let mut seen_slugs = HashSet::new();
    for detail in details.values() {
        let markets = detail
            .pointer("/event/markets")
            .and_then(Value::as_array)
            .ok_or("Polymarket event detail missing markets")?;
        for market in markets {
            let Some(slug) = market["slug"].as_str() else {
                continue;
            };
            if !polymarket_market_open(market) || !seen_slugs.insert(slug) {
                continue;
            }
            if polymarket_moneyline(market) {
                counts.moneylines += 1;
            } else if spread_series(sport)
                .iter()
                .any(|spec| spread_scope_matches(market, *spec))
            {
                counts.spreads += 1;
            } else if full_game_total(market) && matches!(sport.label, "NFL" | "CFB") {
                counts.totals += 1;
            } else if prop_series(sport)
                .iter()
                .any(|spec| full_game_prop(market, *spec))
            {
                counts.props += 1;
            }
        }
    }
    Ok(counts)
}

async fn discover_props(
    client: &Client,
    sport: Sport,
    matcher: &EventMatcher<'_>,
    polymarket_details: &HashMap<String, Value>,
    matched_event_ids: &mut HashSet<String>,
) -> Result<(Vec<Pair>, usize), Box<dyn Error>> {
    let mut out = Vec::new();
    let mut open_markets = 0;
    for spec in prop_series(sport) {
        let series = kalshi_events(client, spec.kalshi_series).await?;
        open_markets += series.open_markets;
        for kalshi_event in &series.events {
            let Some(poly_event) = matcher.best_match(kalshi_event) else {
                continue;
            };
            let kalshi_detail = series
                .details
                .get(&kalshi_event.id)
                .ok_or("Kalshi event missing from discovery cache")?;
            let poly_detail = polymarket_details
                .get(&poly_event.id)
                .ok_or("Polymarket event detail missing from discovery cache")?;
            let pairs = pair_props(kalshi_detail, poly_detail, sport, *spec);
            if !pairs.is_empty() {
                matched_event_ids.insert(poly_event.id.clone());
            }
            out.extend(pairs);
        }
    }
    Ok((out, open_markets))
}

async fn discover_spreads(
    client: &Client,
    sport: Sport,
    matcher: &EventMatcher<'_>,
    polymarket_details: &HashMap<String, Value>,
    matched_event_ids: &mut HashSet<String>,
) -> Result<(Vec<Pair>, usize), Box<dyn Error>> {
    let mut out = Vec::new();
    let mut open_markets = 0;
    for spec in spread_series(sport) {
        let series = kalshi_events(client, spec.kalshi_series).await?;
        open_markets += series.open_markets;
        for kalshi_event in &series.events {
            let Some(poly_event) = matcher.best_match(kalshi_event) else {
                continue;
            };
            let kalshi_detail = series
                .details
                .get(&kalshi_event.id)
                .ok_or("Kalshi event missing from discovery cache")?;
            let poly_detail = polymarket_details
                .get(&poly_event.id)
                .ok_or("Polymarket event detail missing from discovery cache")?;
            let pairs = pair_spreads(kalshi_detail, poly_detail, sport, *spec);
            if !pairs.is_empty() {
                matched_event_ids.insert(poly_event.id.clone());
            }
            out.extend(pairs);
        }
    }
    Ok((out, open_markets))
}

async fn discover(client: &Client, sport: Sport) -> Result<Vec<Pair>, Box<dyn Error>> {
    let (k, p) = tokio::join!(
        kalshi_events(client, sport.kalshi_series),
        polymarket_events(client, sport.polymarket_league)
    );
    let k = k?;
    let p = p?;
    let poly_details = polymarket_details(client, &p).await?;
    let polymarket_counts = polymarket_counts(&poly_details, sport)?;
    let mut kalshi_counts = MarketCounts {
        games: k.events.len(),
        moneylines: k.two_way_moneylines,
        ..Default::default()
    };
    let polymarket_events_for_derivatives = p.clone();
    let matcher = EventMatcher::new(&polymarket_events_for_derivatives, sport);
    let mut matched_event_ids = HashSet::new();
    let kalshi_details = k.details;
    let mut remaining = k.events;
    let mut matches = Vec::new();
    for p in p {
        if let Some((i, score)) = remaining
            .iter()
            .enumerate()
            .map(|(i, k)| (i, event_match_score(k, &p, sport)))
            .max_by(|a, b| a.1.total_cmp(&b.1))
        {
            if score >= 0.72 {
                matches.push((remaining.remove(i), p));
            }
        }
    }
    let mut out = Vec::new();
    for (k, p) in matches {
        let kd = kalshi_details
            .get(&k.id)
            .ok_or("Kalshi event missing from discovery cache")?;
        let pd = poly_details
            .get(&p.id)
            .ok_or("Polymarket event detail missing from discovery cache")?;
        let km: Vec<(String, String)> = kd["markets"]
            .as_array()
            .into_iter()
            .flatten()
            .filter(|market| kalshi_market_open(market))
            .filter_map(|m| {
                Some((
                    kalshi_winner(m["title"].as_str()?)?.into(),
                    m["ticker"].as_str()?.into(),
                ))
            })
            .collect();
        let Some(pm) = pd["event"]["markets"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|market| polymarket_market_open(market) && polymarket_moneyline(market))
        else {
            continue;
        };
        let Some(slug) = pm["slug"].as_str() else {
            continue;
        };
        let (Some(long), Some(short)) = (market_team(pm, true), market_team(pm, false)) else {
            continue;
        };
        let ps = [long, short];
        let Some((teams, tickers, poly_long_is_first)) = align_moneyline(km, ps) else {
            continue;
        };
        out.push(Pair {
            sport: sport.label,
            title: p.title,
            market: "Full game moneyline".into(),
            kalshi: tickers,
            teams,
            poly_slug: slug.into(),
            poly_long_is_first,
        });
        matched_event_ids.insert(p.id);
    }
    let moneyline_pairs = out.len();
    // Spread series are separate by game period. A half or quarter is paired
    // only with the identical Polymarket period and an exact line.
    let (spreads, kalshi_spreads) = discover_spreads(
        client,
        sport,
        &matcher,
        &poly_details,
        &mut matched_event_ids,
    )
    .await?;
    kalshi_counts.spreads = kalshi_spreads;
    let spread_pairs = spreads.len();
    out.extend(spreads);

    // Full-game totals live in separate Kalshi series for both football leagues.
    let total_series = match sport.label {
        "NFL" => Some("KXNFLTOTAL"),
        "CFB" => Some("KXNCAAFTOTAL"),
        _ => None,
    };
    let before_totals = out.len();
    if let Some(total_series) = total_series {
        let series = kalshi_events(client, total_series).await?;
        kalshi_counts.totals = series.open_markets;
        for kalshi_event in &series.events {
            let Some(poly_event) = matcher.best_match(kalshi_event) else {
                continue;
            };
            let kalshi_detail = series
                .details
                .get(&kalshi_event.id)
                .ok_or("Kalshi event missing from discovery cache")?;
            let poly_detail = poly_details
                .get(&poly_event.id)
                .ok_or("Polymarket event detail missing from discovery cache")?;
            let pairs = pair_totals(kalshi_detail, poly_detail, sport);
            if !pairs.is_empty() {
                matched_event_ids.insert(poly_event.id.clone());
            }
            out.extend(pairs);
        }
    }
    let total_pairs = out.len() - before_totals;
    let (props, kalshi_props) = discover_props(
        client,
        sport,
        &matcher,
        &poly_details,
        &mut matched_event_ids,
    )
    .await?;
    kalshi_counts.props = kalshi_props;
    let prop_pairs = props.len();
    out.extend(props);
    let batch_count = subscription_batches(&out).len();
    println!(
        "\n{} discovery\n\
         Kalshi\n    Games:       {}\n    Moneylines:  {}\n    Spreads:     {}\n    Totals:      {}\n    Props:       {}\n\n\
         Polymarket\n    Games:       {}\n    Moneylines:  {}\n    Spreads:     {}\n    Totals:      {}\n    Props:       {}\n\n\
         Matched\n    Games:       {}\n    Moneylines:  {moneyline_pairs}\n    Spreads:     {spread_pairs}\n    Totals:      {total_pairs}\n    Props:       {prop_pairs}\n    Total pairs: {}\n\n\
         Subscriptions\n    Batches:     {batch_count}\n",
        sport.label,
        kalshi_counts.games,
        kalshi_counts.moneylines,
        kalshi_counts.spreads,
        kalshi_counts.totals,
        kalshi_counts.props,
        polymarket_counts.games,
        polymarket_counts.moneylines,
        polymarket_counts.spreads,
        polymarket_counts.totals,
        polymarket_counts.props,
        matched_event_ids.len(),
        out.len(),
    );
    Ok(out)
}

fn now() -> Result<String, Box<dyn Error>> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .to_string())
}

fn clock() -> String {
    Local::now().format("%H:%M:%S").to_string()
}
fn kalshi_sig(t: &str, path: &str) -> Result<String, Box<dyn Error>> {
    let pem = fs::read_to_string(env::var("KALSHI_PRIVATE_KEY_PATH")?)?;
    let msg = format!("{t}GET{path}");
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
fn poly_sig(t: &str, path: &str) -> Result<String, Box<dyn Error>> {
    let raw = BASE64.decode(env::var("POLYMARKET_US_SECRET_KEY")?)?;
    let raw: [u8; 32] = raw
        .get(..32)
        .ok_or("POLYMARKET_US_SECRET_KEY is too short")?
        .try_into()?;
    Ok(BASE64.encode(
        ed25519_dalek::SigningKey::from_bytes(&raw)
            .sign(format!("{t}GET{path}").as_bytes())
            .to_bytes(),
    ))
}

fn dollars(value: Option<&Value>) -> Option<f64> {
    value.and_then(|value| {
        value
            .as_f64()
            .or_else(|| value.as_i64().map(|value| value as f64))
            .or_else(|| value.as_str()?.parse().ok())
    })
}

async fn kalshi_balance(client: &Client) -> Result<String, Box<dyn Error>> {
    let path = "/trade-api/v2/portfolio/balance";
    let timestamp = now()?;
    let payload = client
        .get(format!("{KALSHI_REST}/portfolio/balance"))
        .header("KALSHI-ACCESS-KEY", env::var("KALSHI_API_KEY_ID")?)
        .header("KALSHI-ACCESS-TIMESTAMP", &timestamp)
        .header("KALSHI-ACCESS-SIGNATURE", kalshi_sig(&timestamp, path)?)
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    let available = dollars(payload.get("balance_dollars"))
        .or_else(|| dollars(payload.get("balance")).map(|value| value / 100.0))
        .ok_or("Kalshi balance response did not include an available balance")?;
    Ok(format!("${available:.2}"))
}

async fn polymarket_balance(client: &Client) -> Result<(String, String), Box<dyn Error>> {
    let path = "/v1/account/balances";
    let timestamp = now()?;
    let payload = client
        .get(format!("{POLY_PRIVATE_REST}{path}"))
        .header("X-PM-Access-Key", env::var("POLYMARKET_US_KEY_ID")?)
        .header("X-PM-Timestamp", &timestamp)
        .header("X-PM-Signature", poly_sig(&timestamp, path)?)
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    let balance = payload["balances"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|balance| balance["currency"].as_str() == Some("USD"))
        .or_else(|| {
            payload["balances"]
                .as_array()
                .and_then(|balances| balances.first())
        })
        .ok_or("Polymarket balance response did not include a balance")?;
    let current = dollars(balance.get("currentBalance"))
        .ok_or("Polymarket balance response did not include currentBalance")?;
    let buying_power = dollars(balance.get("buyingPower"));
    Ok((
        format!("${current:.2}"),
        buying_power
            .map(|value| format!("${value:.2}"))
            .unwrap_or_else(|| "unavailable".into()),
    ))
}

async fn balance_snapshot(client: &Client) -> BalanceSnapshot {
    let (kalshi, polymarket) = tokio::join!(kalshi_balance(client), polymarket_balance(client));
    let (polymarket, buying_power) =
        polymarket.unwrap_or_else(|_| ("unavailable".into(), "unavailable".into()));
    BalanceSnapshot {
        // Avoid rendering provider errors here: they can include account-specific details.
        kalshi: kalshi.unwrap_or_else(|_| "unavailable".into()),
        polymarket,
        buying_power,
    }
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

fn set_levels(target: &mut BTreeMap<i32, f64>, levels: &[Value]) {
    target.clear();
    for level in levels {
        let p = number(level.get(0).or_else(|| level.pointer("/px/value")));
        let q = number(level.get(1).or_else(|| level.get("qty")));
        if let (Some(p), Some(q)) = (p, q) {
            if q > 0.0 {
                target.insert(price(p), q);
            }
        }
    }
}
fn k_snapshot<'a>(payload: &'a Value, books: &mut HashMap<String, KalshiBook>) -> Option<&'a str> {
    let Some(t) = payload
        .pointer("/msg/market_ticker")
        .and_then(Value::as_str)
    else {
        return None;
    };
    let levels = payload
        .pointer("/msg/no_dollars_fp")
        .and_then(Value::as_array)?;
    let book = books.get_mut(t)?;
    set_levels(&mut book.no, levels);
    book.updated = Some(Instant::now());
    book.generation += 1;
    Some(t)
}
fn k_delta<'a>(payload: &'a Value, books: &mut HashMap<String, KalshiBook>) -> Option<&'a str> {
    let msg = &payload["msg"];
    let (Some(t), Some(p), Some(d), Some(side)) = (
        msg["market_ticker"].as_str(),
        number(msg.get("price_dollars")),
        number(msg.get("delta_fp")),
        msg["side"].as_str(),
    ) else {
        return None;
    };
    if side != "no" {
        return None;
    };
    let book = books.get_mut(t)?;
    let key = price(p);
    if book.updated.is_none() {
        return None;
    }
    let next = book.no.get(&key).copied().unwrap_or(0.0) + d;
    if next <= 0.0 {
        book.no.remove(&key);
    } else {
        book.no.insert(key, next);
    }
    book.updated = Some(Instant::now());
    book.generation += 1;
    Some(t)
}
fn p_book<'a>(payload: &'a Value, books: &mut HashMap<String, PolyBook>) -> Option<&'a str> {
    let Some(data) = payload.get("marketData") else {
        return None;
    };
    let Some(slug) = data["marketSlug"].as_str() else {
        return None;
    };
    let bids = data.get("bids").and_then(Value::as_array)?;
    let offers = data.get("offers").and_then(Value::as_array)?;
    let book = books.get_mut(slug)?;
    set_levels(&mut book.bids, bids);
    set_levels(&mut book.offers, offers);
    book.updated = Some(Instant::now());
    book.generation += 1;
    Some(slug)
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

fn polymarket_fill_levels<'a>(
    levels: impl Iterator<Item = (&'a i32, &'a f64)>,
    buy_long: bool,
    target: u64,
) -> Fill {
    let mut fill = Fill {
        contracts: 0,
        cost: 0.0,
        fee: 0.0,
        levels: 0,
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

fn polymarket_fill(book: &PolyBook, buy_long: bool, target: u64) -> Fill {
    if buy_long {
        polymarket_fill_levels(book.offers.iter(), true, target)
    } else {
        polymarket_fill_levels(book.bids.iter().rev(), false, target)
    }
}

async fn candidate<'a>(
    pairs: &'a [Pair],
    indices: &[usize],
    kb: &HashMap<String, KalshiBook>,
    pb: &HashMap<String, PolyBook>,
    pending: &mut HashMap<(&'a str, &'a str), Observation>,
    emitted: &mut HashMap<(&'a str, &'a str), Observation>,
    reporter: &mpsc::Sender<CandidateRecord>,
) -> Result<(), Box<dyn Error>> {
    for &index in indices {
        let pair = &pairs[index];
        let Some(poly) = pb.get(&pair.poly_slug) else {
            continue;
        };
        if poly.updated.is_none_or(|t| t.elapsed() > MAX_AGE) {
            continue;
        };
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
            let opposite_is_long = (1 - i == 0) == pair.poly_long_is_first;
            let kalshi_initial = kalshi_yes_fill(k, MAX_SIMULATED_CONTRACTS);
            let polymarket_initial =
                polymarket_fill(poly, opposite_is_long, MAX_SIMULATED_CONTRACTS);
            let contracts = kalshi_initial.contracts.min(polymarket_initial.contracts);
            if contracts == 0 {
                continue;
            }
            let kalshi_fill = if kalshi_initial.contracts == contracts {
                kalshi_initial
            } else {
                kalshi_yes_fill(k, contracts)
            };
            let polymarket_fill = if polymarket_initial.contracts == contracts {
                polymarket_initial
            } else {
                polymarket_fill(poly, opposite_is_long, contracts)
            };
            let key = (pair.poly_slug.as_str(), pair.kalshi[i].as_str());
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
                let permit = reporter
                    .reserve()
                    .await
                    .map_err(|_| "candidate reporter stopped")?;
                emitted.insert(key, observation);
                pending.insert(key, observation);
                permit.send(CandidateRecord {
                    timestamp: clock(),
                    pair: pair.clone(),
                    kalshi_side: i,
                    kalshi_fill,
                    polymarket_fill,
                    contracts,
                    gross_profit,
                    net_profit,
                });
            }
        }
    }
    Ok(())
}

async fn report_candidates(
    mut receiver: mpsc::Receiver<CandidateRecord>,
    client: Client,
    mut file: std::fs::File,
) -> std::io::Result<()> {
    while let Some(record) = receiver.recv().await {
        let balances = balance_snapshot(&client).await;
        let pair = &record.pair;
        let kalshi_outcome = &pair.teams[record.kalshi_side];
        let polymarket_outcome = &pair.teams[1 - record.kalshi_side];
        let kalshi_average = record.kalshi_fill.cost / record.contracts as f64;
        let polymarket_average = record.polymarket_fill.cost / record.contracts as f64;
        let line = json!({"kind":"confirmed_net_candidate","sport":pair.sport,"event":pair.title,"market":pair.market,"kalshi_outcome":kalshi_outcome,"kalshi_average_price":kalshi_average,"kalshi_fee":record.kalshi_fill.fee,"kalshi_levels":record.kalshi_fill.levels,"polymarket_outcome":polymarket_outcome,"polymarket_average_price":polymarket_average,"polymarket_fee":record.polymarket_fill.fee,"polymarket_levels":record.polymarket_fill.levels,"contracts":record.contracts,"gross_profit":record.gross_profit,"net_profit":record.net_profit,"execution":{"status":"not_executed","mode":"dry_run"},"current_balances":&balances,"note":"dry run only; settlement-rule parity and fill risk remain unverified"});
        writeln!(
            file,
            "{}",
            serde_json::to_string(&line).map_err(std::io::Error::other)?
        )?;
        println!(
            "{}\n",
            format_candidate_log(
                &record.timestamp,
                &pair.title,
                &pair.market,
                record.net_profit,
                record.contracts,
                DisplaySide {
                    outcome: kalshi_outcome,
                    average_price: kalshi_average,
                    fee: record.kalshi_fill.fee,
                    levels: record.kalshi_fill.levels,
                },
                DisplaySide {
                    outcome: polymarket_outcome,
                    average_price: polymarket_average,
                    fee: record.polymarket_fill.fee,
                    levels: record.polymarket_fill.levels,
                },
                &balances,
            )
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_only_open_kalshi_markets_and_two_way_moneylines() {
        let event = json!({"markets":[
            {"status":"active","title":"Will A win the A vs. B NFL game?"},
            {"status":"active","title":"Will B win the A vs. B NFL game?"},
            {"status":"closed","title":"Will C win the A vs. B NFL game?"}
        ]});
        assert_eq!(kalshi_event_counts(&event).unwrap(), (2, true));
    }

    #[test]
    fn counts_open_polymarket_markets_by_scanned_category() {
        let detail = json!({"event":{"markets":[
            {"slug":"moneyline","question":"Who will win in the upcoming A vs B game?"},
            {"slug":"spread","marketType":"spreads","sportsMarketType":"football_team_spread","line":-3.5},
            {"slug":"total","marketType":"totals","sportsMarketType":"football_game_total","line":47.5},
            {"slug":"prop","sportsMarketType":"football_player_full_game_passing_yards","line":224.5},
            {"slug":"closed","closed":true,"marketType":"spreads","sportsMarketType":"football_team_spread","line":-3.5},
            {"slug":"spread","marketType":"spreads","sportsMarketType":"football_team_spread","line":-3.5},
            {"slug":"unscanned","marketType":"totals","sportsMarketType":"football_game_first_half_total","line":22.5}
        ]}});
        let details = HashMap::from([("game".to_owned(), detail)]);
        let counts = polymarket_counts(&details, selected_sports("nfl").unwrap()[0]).unwrap();
        assert_eq!(counts.games, 1);
        assert_eq!(counts.moneylines, 1);
        assert_eq!(counts.spreads, 1);
        assert_eq!(counts.totals, 1);
        assert_eq!(counts.props, 1);
    }

    #[test]
    fn pairs_each_kalshi_outcome_with_the_opposite_polymarket_outcome() {
        let pair = Pair {
            sport: "test",
            title: "A vs B".into(),
            market: "Full game moneyline".into(),
            kalshi: ["K-A".into(), "K-B".into()],
            teams: ["A".into(), "B".into()],
            poly_slug: "pm-a-b".into(),
            poly_long_is_first: true,
        };
        assert!((1 == 0) != pair.poly_long_is_first);
        assert!((1 - 1 == 0) == pair.poly_long_is_first);
    }

    #[test]
    fn pairs_only_an_exact_full_game_spread_line() {
        let kalshi = json!({"markets":[{"ticker":"K-SPREAD","floor_strike":3.5,"yes_sub_title":"A wins by over 3.5 points"}]});
        let polymarket = json!({"event":{"title":"A vs B","markets":[
            {"marketType":"spreads","sportsMarketType":"football_team_spread","line":-3.5,"slug":"pm-spread","marketSides":[
                {"long":true,"team":{"safeName":"A"}}, {"long":false,"team":{"safeName":"B"}}
            ]},
            {"marketType":"spreads","sportsMarketType":"football_team_first_half_spread","line":-3.5,"slug":"pm-first-half","marketSides":[
                {"long":true,"team":{"safeName":"A"}}, {"long":false,"team":{"safeName":"B"}}
            ]}
        ]}});
        let pairs = pair_spreads(
            &kalshi,
            &polymarket,
            selected_sports("nfl").unwrap()[0],
            NFL_SPREAD_SERIES[0],
        );
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].poly_slug, "pm-spread");
        assert_eq!(pairs[0].teams, ["A -3.5", "B +3.5"]);
    }

    #[test]
    fn pairs_only_the_matching_half_or_quarter_spread_scope() {
        let kalshi = json!({"markets":[{"ticker":"K-1H","floor_strike":3.5,"yes_sub_title":"A wins 1H by over 3.5 points"}]});
        let polymarket = json!({"event":{"title":"A vs B","markets":[
            {"marketType":"spreads","sportsMarketType":"football_team_first_half_spread","line":-3.5,"slug":"pm-1h","marketSides":[
                {"long":true,"team":{"safeName":"A"}}, {"long":false,"team":{"safeName":"B"}}
            ]},
            {"marketType":"spreads","sportsMarketType":"football_team_first_quarter_spread","line":-3.5,"slug":"pm-1q","marketSides":[
                {"long":true,"team":{"safeName":"A"}}, {"long":false,"team":{"safeName":"B"}}
            ]}
        ]}});
        let pairs = pair_spreads(
            &kalshi,
            &polymarket,
            selected_sports("nfl").unwrap()[0],
            NFL_SPREAD_SERIES[1],
        );
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].poly_slug, "pm-1h");
        assert_eq!(pairs[0].title, "A vs B");
        assert_eq!(pairs[0].market, "1st half spread (+3.5)");
    }

    #[test]
    fn pairs_only_an_exact_full_game_total_line() {
        let kalshi = json!({"markets":[{"ticker":"K-TOTAL","floor_strike":47.5,"title":"Over 47.5 points?"}]});
        let polymarket = json!({"event":{"title":"A vs B","markets":[
            {"marketType":"totals","sportsMarketType":"football_team_full_game_total","line":47.5,"slug":"pm-team-total","marketSides":[
                {"long":true,"description":"Over 47.5"}, {"long":false,"description":"Under 47.5"}
            ]},
            {"marketType":"totals","sportsMarketType":"football_game_total","line":47.5,"slug":"pm-total","marketSides":[
                {"long":true,"description":"Over 47.5"}, {"long":false,"description":"Under 47.5"}
            ]},
            {"marketType":"totals","sportsMarketType":"football_game_first_half_total","line":47.5,"slug":"pm-first-half","marketSides":[
                {"long":true,"description":"Over 47.5"}, {"long":false,"description":"Under 47.5"}
            ]}
        ]}});
        for sport in ["nfl", "cfb"] {
            let pairs = pair_totals(&kalshi, &polymarket, selected_sports(sport).unwrap()[0]);
            assert_eq!(pairs.len(), 1);
            assert_eq!(pairs[0].poly_slug, "pm-total");
            assert_eq!(pairs[0].teams, ["Over 47.5", "Under 47.5"]);
        }
    }

    #[test]
    fn pairs_only_an_exact_full_game_nfl_player_prop() {
        let kalshi = json!({"markets":[
            {"ticker":"K-PASS","title":"Bo Nix: 225+ passing yards","floor_strike":224.5},
            {"ticker":"K-OTHER","title":"Bo Nix: 250+ passing yards","floor_strike":249.5}
        ]});
        let polymarket = json!({"event":{"title":"Denver vs Los Angeles","markets":[
            {"slug":"pm-passing-completions","sportsMarketType":"football_player_full_game_passing_yards_completions","line":224.5,"player":{"safeName":"Bo Nix"},"marketSides":[
                {"long":true,"description":"Over"}, {"long":false,"description":"Under"}
            ]},
            {"slug":"pm-pass","sportsMarketType":"football_player_full_game_passing_yards","line":224.5,"player":{"safeName":"Bo Nix"},"marketSides":[
                {"long":true,"description":"Over"}, {"long":false,"description":"Under"}
            ]},
            {"slug":"pm-half","sportsMarketType":"football_player_first_half_passing_yards","line":224.5,"player":{"safeName":"Bo Nix"},"marketSides":[
                {"long":true,"description":"Over"}, {"long":false,"description":"Under"}
            ]}
        ]}});
        let pairs = pair_props(
            &kalshi,
            &polymarket,
            selected_sports("nfl").unwrap()[0],
            NFL_PROP_SERIES[0],
        );
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].poly_slug, "pm-pass");
        assert_eq!(pairs[0].teams, ["Bo Nix Over 224.5", "Bo Nix Under 224.5"]);
    }

    #[test]
    fn pairs_only_an_exact_full_game_cfb_team_prop() {
        let kalshi = json!({"markets":[
            {"ticker":"K-TEAM","title":"Ohio State over 27.5 points scored","floor_strike":27.5}
        ]});
        let polymarket = json!({"event":{"title":"Ohio State vs Michigan","markets":[
            {"slug":"pm-team-touchdowns","sportsMarketType":"football_team_full_game_total_touchdowns","line":27.5,"marketSides":[
                {"long":true,"description":"Over","team":{"safeName":"Ohio State"}},
                {"long":false,"description":"Under","team":{"safeName":"Ohio State"}}
            ]},
            {"slug":"pm-team","sportsMarketType":"football_team_full_game_total","line":27.5,"marketSides":[
                {"long":true,"description":"Over","team":{"safeName":"Ohio State"}},
                {"long":false,"description":"Under","team":{"safeName":"Ohio State"}}
            ]},
            {"slug":"pm-quarter","sportsMarketType":"football_team_first_quarter_total","line":27.5,"marketSides":[
                {"long":true,"description":"Over","team":{"safeName":"Ohio State"}},
                {"long":false,"description":"Under","team":{"safeName":"Ohio State"}}
            ]}
        ]}});
        let pairs = pair_props(
            &kalshi,
            &polymarket,
            selected_sports("cfb").unwrap()[0],
            CFB_PROP_SERIES[0],
        );
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].poly_slug, "pm-team");
    }

    #[test]
    fn team_prop_accepts_the_same_nfl_team_with_venue_specific_names() {
        let kalshi = json!({"markets":[{"ticker":"K-KC","title":"KC Chiefs over 24.5 points scored","floor_strike":24.5}]});
        let polymarket = json!({"event":{"title":"Kansas City vs Miami","markets":[
            {"slug":"pm-kc","sportsMarketType":"football_team_full_game_total","line":24.5,"marketSides":[
                {"long":true,"description":"Over","team":{"safeName":"Kansas City Chiefs"}},
                {"long":false,"description":"Under","team":{"safeName":"Kansas City Chiefs"}}
            ]}
        ]}});
        let pairs = pair_props(
            &kalshi,
            &polymarket,
            selected_sports("nfl").unwrap()[0],
            NFL_PROP_SERIES[4],
        );
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].poly_slug, "pm-kc");
    }

    #[test]
    fn team_yardage_prop_extracts_team_before_the_threshold() {
        let kalshi = json!({"markets":[{"ticker":"K-MIA-YDS","title":"MIA Dolphins : 250+","floor_strike":249.5}]});
        let polymarket = json!({"event":{"title":"Kansas City vs Miami","markets":[
            {"slug":"pm-mia-yds","sportsMarketType":"football_team_full_game_yards","line":249.5,"marketSides":[
                {"long":true,"description":"Over","team":{"safeName":"Miami Dolphins"}},
                {"long":false,"description":"Under","team":{"safeName":"Miami Dolphins"}}
            ]}
        ]}});
        let pairs = pair_props(
            &kalshi,
            &polymarket,
            selected_sports("nfl").unwrap()[0],
            NFL_PROP_SERIES[5],
        );
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].poly_slug, "pm-mia-yds");
    }

    #[test]
    fn distinct_football_teams_do_not_match_by_shared_city_or_school_name() {
        assert!(!same_team("LA Chargers", "Los Angeles Rams"));
        assert!(!same_team("Michigan", "Michigan State"));
        assert!(same_team("Ohio St.", "Ohio State"));
        assert_eq!(
            event_similarity("Michigan vs Michigan State", "Michigan vs Ohio State"),
            0.0
        );
    }

    #[test]
    fn moneyline_requires_two_distinct_team_matches() {
        let kalshi = vec![
            ("Michigan".into(), "K-MICH".into()),
            ("Michigan State".into(), "K-MSU".into()),
        ];
        let valid =
            align_moneyline(kalshi.clone(), ["Michigan State".into(), "Michigan".into()]).unwrap();
        assert_eq!(valid.0, ["Michigan", "Michigan State"]);
        assert!(!valid.2);
        assert!(align_moneyline(kalshi, ["Michigan".into(), "Michigan".into()]).is_none());
    }

    #[test]
    fn football_event_match_requires_the_same_scheduled_date() {
        let kalshi = Event {
            id: "KXNFLGAME-26SEP27KCMIA".into(),
            title: "KC Chiefs vs MIA Dolphins".into(),
        };
        let poly = Event {
            id: "nfl-kc-mia-2026-09-27".into(),
            title: "KC Chiefs vs MIA Dolphins".into(),
        };
        let sport = selected_sports("nfl").unwrap()[0];
        assert_eq!(event_match_score(&kalshi, &poly, sport), 1.0);
        let different_week = Event {
            id: "nfl-kc-mia-2026-10-04".into(),
            ..poly
        };
        assert_eq!(event_match_score(&kalshi, &different_week, sport), 0.0);
    }

    #[test]
    fn football_match_index_checks_only_games_on_the_same_date() {
        let kalshi = Event {
            id: "KXNFLGAME-26SEP27KCMIA".into(),
            title: "KC Chiefs vs MIA Dolphins".into(),
        };
        let events = [
            Event {
                id: "nfl-kc-mia-2026-10-04".into(),
                title: kalshi.title.clone(),
            },
            Event {
                id: "nfl-kc-buf-2026-09-27".into(),
                title: "KC Chiefs vs BUF Bills".into(),
            },
            Event {
                id: "nfl-kc-mia-2026-09-27".into(),
                title: kalshi.title.clone(),
            },
        ];
        let matcher = EventMatcher::new(&events, selected_sports("nfl").unwrap()[0]);
        assert_eq!(matcher.best_match(&kalshi).unwrap().id, events[2].id);
    }

    #[test]
    fn spread_uses_long_side_when_kalshi_names_the_short_team() {
        let kalshi = json!({"markets":[{"ticker":"K-LAC","floor_strike":3.5,"yes_sub_title":"LA Chargers wins by over 3.5 points"}]});
        let polymarket = json!({"event":{"title":"Chargers vs Rams","markets":[
            {"marketType":"spreads","sportsMarketType":"football_team_spread","line":3.5,"slug":"pm-la","marketSides":[
                {"long":true,"team":{"safeName":"Los Angeles Rams"}},
                {"long":false,"team":{"safeName":"Los Angeles Chargers"}}
            ]}
        ]}});
        let pairs = pair_spreads(
            &kalshi,
            &polymarket,
            selected_sports("nfl").unwrap()[0],
            NFL_SPREAD_SERIES[0],
        );
        assert_eq!(pairs.len(), 1);
        assert!(!pairs[0].poly_long_is_first);
        assert_eq!(
            pairs[0].teams,
            ["LA Chargers -3.5", "Los Angeles Rams +3.5"]
        );
    }

    #[test]
    fn spread_rejects_integer_line_with_exact_margin_gap() {
        let kalshi = json!({"markets":[{"ticker":"K-SPREAD","floor_strike":3,"yes_sub_title":"A wins by over 3 points"}]});
        let polymarket = json!({"event":{"title":"A vs B","markets":[
            {"marketType":"spreads","sportsMarketType":"football_team_spread","line":-3,"slug":"pm-spread","marketSides":[
                {"long":true,"team":{"safeName":"A"}}, {"long":false,"team":{"safeName":"B"}}
            ]}
        ]}});
        assert!(
            pair_spreads(
                &kalshi,
                &polymarket,
                selected_sports("cfb").unwrap()[0],
                CFB_SPREAD_SERIES[0]
            )
            .is_empty()
        );
    }

    #[test]
    fn splits_subscriptions_before_the_market_cap() {
        let pair = Pair {
            sport: "test",
            title: "A vs B".into(),
            market: "Full game moneyline".into(),
            kalshi: ["K-A".into(), "K-B".into()],
            teams: ["A".into(), "B".into()],
            poly_slug: "pm-a-b".into(),
            poly_long_is_first: true,
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
    fn indexes_only_pairs_affected_by_each_market_update() {
        let pair = Pair {
            sport: "test",
            title: "A vs B".into(),
            market: "Full game moneyline".into(),
            kalshi: ["K-A".into(), "K-B".into()],
            teams: ["A".into(), "B".into()],
            poly_slug: "pm-a-b".into(),
            poly_long_is_first: true,
        };
        let pairs = [
            pair.clone(),
            Pair {
                kalshi: ["K-C".into(), String::new()],
                ..pair
            },
        ];
        let index = pair_index(&pairs);
        assert_eq!(index.by_kalshi.get("K-A"), Some(&vec![0]));
        assert_eq!(index.by_kalshi.get("K-C"), Some(&vec![1]));
        assert_eq!(index.by_polymarket.get("pm-a-b"), Some(&vec![0, 1]));
        assert!(index.by_kalshi.get("other").is_none());
    }

    #[test]
    fn kalshi_deltas_wait_for_a_full_snapshot() {
        let mut books = HashMap::from([("K-A".into(), KalshiBook::default())]);
        let delta = json!({"msg":{"market_ticker":"K-A","side":"no","price_dollars":"0.6000","delta_fp":"2"}});
        assert_eq!(k_delta(&delta, &mut books), None);
        assert!(books["K-A"].no.is_empty());
        let incomplete = json!({"msg":{"market_ticker":"K-A"}});
        assert_eq!(k_snapshot(&incomplete, &mut books), None);
        assert!(books["K-A"].updated.is_none());
        let snapshot = json!({"msg":{"market_ticker":"K-A","no_dollars_fp":[["0.6000","25"]]}});
        assert_eq!(k_snapshot(&snapshot, &mut books), Some("K-A"));
        assert_eq!(k_delta(&delta, &mut books), Some("K-A"));
        assert_eq!(books["K-A"].no.get(&6_000), Some(&27.0));
    }

    #[test]
    fn incomplete_polymarket_book_does_not_replace_a_snapshot() {
        let mut books = HashMap::from([("pm".into(), PolyBook::default())]);
        let complete = json!({"marketData":{"marketSlug":"pm","bids":[["0.4","10"]],"offers":[]}});
        assert_eq!(p_book(&complete, &mut books), Some("pm"));
        let incomplete = json!({"marketData":{"marketSlug":"pm","bids":[]}});
        assert_eq!(p_book(&incomplete, &mut books), None);
        assert_eq!(books["pm"].bids.get(&4_000), Some(&10.0));
    }

    #[tokio::test]
    async fn candidate_uses_the_needed_polymarket_side_without_waiting_for_both() {
        let pair = Pair {
            sport: "test",
            title: "A vs B".into(),
            market: "Full game moneyline".into(),
            kalshi: ["K-A".into(), String::new()],
            teams: ["A".into(), "B".into()],
            poly_slug: "pm".into(),
            poly_long_is_first: false,
        };
        let kalshi = HashMap::from([(
            "K-A".into(),
            KalshiBook {
                no: BTreeMap::from([(6_000, 25.0)]),
                updated: Some(Instant::now()),
                generation: 1,
            },
        )]);
        let polymarket = HashMap::from([(
            "pm".into(),
            PolyBook {
                offers: BTreeMap::from([(5_000, 25.0)]),
                updated: Some(Instant::now()),
                generation: 1,
                ..Default::default()
            },
        )]);
        let mut pending = HashMap::from([(
            ("pm", "K-A"),
            Observation {
                kalshi_generation: 0,
                polymarket_generation: 0,
            },
        )]);
        let mut emitted = HashMap::new();
        let (sender, mut receiver) = mpsc::channel(1);
        candidate(
            &[pair],
            &[0],
            &kalshi,
            &polymarket,
            &mut pending,
            &mut emitted,
            &sender,
        )
        .await
        .unwrap();
        let record = receiver.try_recv().unwrap();
        assert_eq!(record.contracts, 25);
        assert!(record.net_profit >= MIN_NET_PROFIT_DOLLARS);
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
            event_similarity(
                "New York M vs Washington",
                "New York Mets vs. Washington Nationals"
            ),
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

    #[test]
    fn extracts_r6_winner_from_kalshi_question_title() {
        assert_eq!(
            kalshi_winner("Will Heretics win the Heretics vs. Rebels Gaming R6 match?"),
            Some("Heretics")
        );
    }
}

async fn run_session(
    pairs: &[Pair],
    reporter: &mpsc::Sender<CandidateRecord>,
) -> Result<(), Box<dyn Error>> {
    let index = pair_index(pairs);
    let tickers: Vec<String> = pairs
        .iter()
        .flat_map(|pair| {
            pair.kalshi
                .iter()
                .filter(|ticker| !ticker.is_empty())
                .cloned()
        })
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
            (
                "KALSHI-ACCESS-SIGNATURE",
                kalshi_sig(&t, "/trade-api/ws/v2")?,
            ),
        ],
    )?;
    let t = now()?;
    let pr = request(
        POLY_WS,
        &[
            ("X-PM-Access-Key", env::var("POLYMARKET_US_KEY_ID")?),
            ("X-PM-Timestamp", t.clone()),
            ("X-PM-Signature", poly_sig(&t, "/v1/ws/markets")?),
        ],
    )?;
    let ((mut ks, _), (mut ps, _)) = tokio::try_join!(connect_async(kr), connect_async(pr))?;
    ks.send(Message::Text(json!({"id":1,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":tickers}}).to_string().into())).await?;
    ps.send(Message::Text(json!({"subscribe":{"requestId":"read-only-scanner","subscriptionType":"SUBSCRIPTION_TYPE_MARKET_DATA","marketSlugs":slugs}}).to_string().into())).await?;
    // These books intentionally exist only for one connection session. A reconnect
    // starts empty and waits for new snapshots before any candidate can be emitted.
    let mut kb = tickers
        .iter()
        .map(|ticker| (ticker.clone(), KalshiBook::default()))
        .collect::<HashMap<_, _>>();
    let mut pb = slugs
        .iter()
        .map(|slug| (slug.clone(), PolyBook::default()))
        .collect::<HashMap<_, _>>();
    let (mut pending, mut emitted) = (HashMap::new(), HashMap::new());
    let refresh_timer = tokio::time::sleep(DISCOVERY_REFRESH);
    tokio::pin!(refresh_timer);
    loop {
        tokio::select! {
            _ = &mut refresh_timer => return Ok(()),
            message = ks.next() => {
                let message = message.ok_or("Kalshi WebSocket closed")??;
                if let Ok(value) = serde_json::from_str::<Value>(message.to_text().unwrap_or("")) {
                    let changed = match value["type"].as_str() {
                        Some("orderbook_snapshot") => k_snapshot(&value, &mut kb),
                        Some("orderbook_delta") => k_delta(&value, &mut kb),
                        _ => None,
                    };
                    if let Some(indices) = changed.and_then(|ticker| index.by_kalshi.get(ticker)) {
                        candidate(pairs, indices, &kb, &pb, &mut pending, &mut emitted, reporter).await?;
                    }
                }
            }
            message = ps.next() => {
                let message = message.ok_or("Polymarket WebSocket closed")??;
                if let Ok(value) = serde_json::from_str::<Value>(message.to_text().unwrap_or("")) {
                    if let Some(indices) = p_book(&value, &mut pb).and_then(|slug| index.by_polymarket.get(slug)) {
                        candidate(pairs, indices, &kb, &pb, &mut pending, &mut emitted, reporter).await?;
                    }
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
        let next_tickers: HashSet<_> = pair
            .kalshi
            .iter()
            .filter(|ticker| !ticker.is_empty())
            .cloned()
            .collect();
        let adds_tickers = next_tickers
            .iter()
            .filter(|ticker| !tickers.contains(*ticker))
            .count();
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

async fn run_batches(
    pairs: &[Pair],
    reporter: &mpsc::Sender<CandidateRecord>,
) -> Result<(), Box<dyn Error>> {
    let batches = subscription_batches(pairs);
    try_join_all(batches.iter().map(|batch| run_session(batch, reporter))).await?;
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
    if selection == "preview" {
        print_candidate_preview().await?;
        return Ok(());
    }
    let sports =
        if selection == "all" {
            None
        } else {
            Some(selected_sports(&selection).ok_or_else(|| {
                format!("Usage: cargo run --bin stream_scanner -- {SCANNER_USAGE}")
            })?)
        };
    let journal = OpenOptions::new()
        .create(true)
        .append(true)
        .open("scanner-candidates.jsonl")?;
    let balance_client = Client::builder()
        .user_agent("arbitrage-executor-read-only/0.1")
        .timeout(Duration::from_secs(3))
        .build()?;
    let (reporter_tx, reporter_rx) = mpsc::channel(128);
    let reporting = report_candidates(reporter_rx, balance_client, journal);
    let scanning = async {
        if let Some(sports) = sports {
            return scan_forever(&selection, sports, &reporter_tx).await;
        }
        let cfb = scan_forever(
            "cfb",
            selected_sports("cfb").expect("supported selection"),
            &reporter_tx,
        );
        let nfl = scan_forever(
            "nfl",
            selected_sports("nfl").expect("supported selection"),
            &reporter_tx,
        );
        let mlb = scan_forever(
            "mlb",
            selected_sports("mlb").expect("supported selection"),
            &reporter_tx,
        );
        let cs2 = scan_forever(
            "cs2",
            selected_sports("cs2").expect("supported selection"),
            &reporter_tx,
        );
        let valorant = scan_forever(
            "valorant",
            selected_sports("valorant").expect("supported selection"),
            &reporter_tx,
        );
        let dota2 = scan_forever(
            "dota2",
            selected_sports("dota2").expect("supported selection"),
            &reporter_tx,
        );
        let lol = scan_forever(
            "lol",
            selected_sports("lol").expect("supported selection"),
            &reporter_tx,
        );
        let r6 = scan_forever(
            "r6",
            selected_sports("r6").expect("supported selection"),
            &reporter_tx,
        );
        let tennis = scan_forever(
            "tennis",
            selected_sports("tennis").expect("supported selection"),
            &reporter_tx,
        );
        tokio::try_join!(cfb, nfl, mlb, cs2, valorant, dota2, lol, r6, tennis)?;
        Ok(())
    };
    tokio::pin!(reporting, scanning);
    tokio::select! {
        result = &mut scanning => result,
        result = &mut reporting => {
            result?;
            Err("candidate reporter stopped unexpectedly".into())
        }
    }
}

async fn print_candidate_preview() -> Result<(), Box<dyn Error>> {
    let client = Client::builder()
        .user_agent("arbitrage-executor-read-only/0.1")
        .build()?;
    let balances = balance_snapshot(&client).await;
    let examples = [
        (
            "Los Angeles Dodgers vs. San Francisco Giants",
            "Full game moneyline",
            0.17,
            25,
            "Dodgers",
            0.9300,
            0.1140,
            1,
            "Giants",
            0.0550,
            0.0900,
            1,
        ),
        (
            "New York Yankees vs. Boston Red Sox",
            "Full game moneyline",
            1.42,
            50,
            "Yankees",
            0.6100,
            0.0800,
            2,
            "Red Sox",
            0.3500,
            0.0600,
            1,
        ),
        (
            "Team Vitality vs. Natus Vincere",
            "Match winner",
            0.64,
            20,
            "Team Vitality",
            0.4700,
            0.0710,
            1,
            "Natus Vincere",
            0.4700,
            0.0680,
            3,
        ),
    ];
    for (
        title,
        market,
        net,
        contracts,
        kalshi_team,
        kalshi_price,
        kalshi_fee,
        kalshi_levels,
        poly_team,
        poly_price,
        poly_fee,
        poly_levels,
    ) in examples
    {
        if net < MIN_NET_PROFIT_DOLLARS {
            continue;
        }
        println!(
            "{}\n",
            format_candidate_log(
                &clock(),
                title,
                market,
                net,
                contracts,
                DisplaySide {
                    outcome: kalshi_team,
                    average_price: kalshi_price,
                    fee: kalshi_fee,
                    levels: kalshi_levels,
                },
                DisplaySide {
                    outcome: poly_team,
                    average_price: poly_price,
                    fee: poly_fee,
                    levels: poly_levels,
                },
                &balances,
            )
        );
    }
    Ok(())
}

async fn discover_sports(client: &Client, sports: &[Sport]) -> Result<Vec<Pair>, Box<dyn Error>> {
    let mut pairs = Vec::new();
    for sport in sports {
        pairs.extend(discover(client, *sport).await?);
    }
    Ok(pairs)
}

async fn scan_forever(
    selection: &str,
    sports: &[Sport],
    reporter: &mpsc::Sender<CandidateRecord>,
) -> Result<(), Box<dyn Error>> {
    let client = Client::builder()
        .user_agent("arbitrage-executor-read-only/0.1")
        .build()?;
    let mut reconnect_attempt = 0;
    let mut prepared_pairs = None;
    loop {
        let discovered = match prepared_pairs.take() {
            Some(pairs) => Ok(pairs),
            None => discover_sports(&client, sports).await,
        };
        let pairs = match discovered {
            Ok(pairs) if !pairs.is_empty() => pairs,
            Ok(_) => {
                eprintln!("{selection}: no matched markets found; retrying discovery shortly.");
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
        let active = run_batches(&pairs, reporter);
        let prefetch = async {
            tokio::time::sleep(DISCOVERY_REFRESH - DISCOVERY_PREFETCH_LEAD).await;
            discover_sports(&client, sports).await
        };
        tokio::pin!(active, prefetch);
        let mut prefetched = None;
        let stream_result = loop {
            tokio::select! {
                result = &mut active => break result,
                result = &mut prefetch, if prefetched.is_none() => prefetched = Some(result),
            }
        };
        match stream_result {
            Ok(()) => {
                println!(
                    "[{}] {}: Refreshing market subscriptions...\n",
                    clock(),
                    selection.to_uppercase()
                );
                reconnect_attempt = 0;
                let next = match prefetched {
                    Some(result) => result,
                    None => prefetch.await,
                };
                match next {
                    Ok(pairs) if !pairs.is_empty() => prepared_pairs = Some(pairs),
                    Ok(_) => eprintln!(
                        "{selection}: no matched markets found in refresh; retrying discovery."
                    ),
                    Err(error) => {
                        eprintln!("{selection}: refresh discovery failed: {error}. Retrying.");
                        tokio::time::sleep(reconnect_delay(reconnect_attempt)).await;
                        reconnect_attempt += 1;
                    }
                }
            }
            Err(error) => {
                if let Some(Ok(pairs)) = prefetched {
                    if !pairs.is_empty() {
                        prepared_pairs = Some(pairs);
                    }
                }
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
