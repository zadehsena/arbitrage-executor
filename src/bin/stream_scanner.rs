//! Continuous, read-only cross-venue scanner.
//!
//! It only opens market-data WebSockets, reads account balances, and appends
//! net-fee candidates to a local journal. There are intentionally no order,
//! cancel, or portfolio APIs.

use arbitrage_executor::fees::{FeeModel, FeePolicy, LegFees};
use arbitrage_executor::settlement::{Approvals, ReviewDecision, RuleReview, digest};
use arbitrage_executor::sports::{
    SCANNER_USAGE, Sport, college_team_key, nba_team_key, same_college_team, same_nba_team,
    selected_sports,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::{Datelike, Local, NaiveDate, NaiveDateTime, Utc};
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
    io::{self, IsTerminal, Write},
    sync::{Arc, Mutex, OnceLock},
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

#[path = "stream_scanner/feed.rs"]
mod feed;
use feed::{FeedHealth, KalshiIntegrity, PolyIntegrity};

#[path = "stream_scanner/safety.rs"]
mod safety;
use safety::{LegSafety, prepare_pair_safety};

const KALSHI_REST: &str = "https://api.elections.kalshi.com/trade-api/v2";
const POLY_REST: &str = "https://gateway.polymarket.us";
const KALSHI_WS: &str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
const POLY_WS: &str = "wss://api.polymarket.us/v1/ws/markets";
const MIN_NET_PROFIT_DOLLARS: f64 = 0.01;
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

fn discovery_row(label: &str, counts: &MarketCounts) -> String {
    format!(
        "{label:<13}{:>7}{:>13}{:>10}{:>9}{:>8}",
        counts.games, counts.moneylines, counts.spreads, counts.totals, counts.props
    )
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
    poly_url: String,
    poly_long_is_first: bool,
    safety: [Option<Arc<LegSafety>>; 2],
}

struct PairIndex<'a> {
    by_kalshi: HashMap<&'a str, Vec<usize>>,
    by_polymarket: HashMap<&'a str, Vec<usize>>,
}

struct Discovery {
    pairs: Vec<Pair>,
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
        if !pair.poly_slug.is_empty() {
            index
                .by_polymarket
                .entry(pair.poly_slug.as_str())
                .or_default()
                .push(position);
        }
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
        poly_type: "footballteampointsfullgametotal",
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
        poly_type: "footballteampointsfullgametotal",
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

const MLB_PROP_SERIES: &[PropSeries] = &[
    PropSeries {
        kalshi_series: "KXMLBHIT",
        kind: PropKind::Player,
        poly_type: "baseballplayerhits",
        label: "Hits",
    },
    PropSeries {
        kalshi_series: "KXMLBHR",
        kind: PropKind::Player,
        poly_type: "baseballplayerhomeruns",
        label: "Home runs",
    },
    PropSeries {
        kalshi_series: "KXMLBRBI",
        kind: PropKind::Player,
        poly_type: "baseballplayerrbis",
        label: "RBIs",
    },
    PropSeries {
        kalshi_series: "KXMLBTB",
        kind: PropKind::Player,
        poly_type: "baseballplayertotalbases",
        label: "Total bases",
    },
    PropSeries {
        kalshi_series: "KXMLBHRR",
        kind: PropKind::Player,
        poly_type: "baseballplayerhitsrunsrbis",
        label: "Hits + runs + RBIs",
    },
    PropSeries {
        kalshi_series: "KXMLBTEAMTOTAL",
        kind: PropKind::Team,
        poly_type: "baseballteamtotalruns",
        label: "Team runs",
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

const CBB_SPREAD_SERIES: &[SpreadSeries] = &[SpreadSeries {
    kalshi_series: "KXNCAAMBSPREAD",
    period: "",
    label: "Full game",
}];
const WCBB_SPREAD_SERIES: &[SpreadSeries] = &[SpreadSeries {
    kalshi_series: "KXNCAAWBSPREAD",
    period: "",
    label: "Full game",
}];

const NBA_SPREAD_SERIES: &[SpreadSeries] = &[SpreadSeries {
    kalshi_series: "KXNBASPREAD",
    period: "",
    label: "Full game",
}];

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

const MLB_SPREAD_SERIES: &[SpreadSeries] = &[
    SpreadSeries {
        kalshi_series: "KXMLBSPREAD",
        period: "",
        label: "Full game",
    },
    SpreadSeries {
        kalshi_series: "KXMLBF5SPREAD",
        period: "firstfive",
        label: "First 5 innings",
    },
];

const ATP_SPREAD_SERIES: &[SpreadSeries] = &[SpreadSeries {
    kalshi_series: "KXATPGSPREAD",
    period: "games",
    label: "Match games",
}];

#[derive(Clone, Copy)]
struct TotalSeries {
    kalshi_series: &'static str,
    poly_type: &'static str,
    label: &'static str,
}

const CBB_TOTAL_SERIES: &[TotalSeries] = &[TotalSeries {
    kalshi_series: "KXNCAAMBTOTAL",
    poly_type: "basketballteamfullgametotal",
    label: "Full game",
}];
const WCBB_TOTAL_SERIES: &[TotalSeries] = &[TotalSeries {
    kalshi_series: "KXNCAAWBTOTAL",
    poly_type: "basketballteamfullgametotal",
    label: "Full game",
}];

const NBA_TOTAL_SERIES: &[TotalSeries] = &[TotalSeries {
    kalshi_series: "KXNBATOTAL",
    poly_type: "basketballteamfullgametotal",
    label: "Full game",
}];

const NFL_TOTAL_SERIES: &[TotalSeries] = &[TotalSeries {
    kalshi_series: "KXNFLTOTAL",
    poly_type: "footballgametotal",
    label: "Full game",
}];
const CFB_TOTAL_SERIES: &[TotalSeries] = &[TotalSeries {
    kalshi_series: "KXNCAAFTOTAL",
    poly_type: "footballgametotal",
    label: "Full game",
}];
const MLB_TOTAL_SERIES: &[TotalSeries] = &[
    TotalSeries {
        kalshi_series: "KXMLBTOTAL",
        poly_type: "baseballteamfullgametotal",
        label: "Full game",
    },
    TotalSeries {
        kalshi_series: "KXMLBF5TOTAL",
        poly_type: "baseballteamfirstfivetotal",
        label: "First 5 innings",
    },
];

const CS2_TOTAL_SERIES: &[TotalSeries] = &[TotalSeries {
    kalshi_series: "KXCS2TOTALMAPS",
    poly_type: "esportsseriestotalmaps",
    label: "Series maps",
}];
const VALORANT_TOTAL_SERIES: &[TotalSeries] = &[TotalSeries {
    kalshi_series: "KXVALORANTTOTALMAPS",
    poly_type: "esportsseriestotalmaps",
    label: "Series maps",
}];
const DOTA2_TOTAL_SERIES: &[TotalSeries] = &[TotalSeries {
    kalshi_series: "KXDOTA2TOTALMAPS",
    poly_type: "esportsseriestotalgames",
    label: "Series games",
}];
const LOL_TOTAL_SERIES: &[TotalSeries] = &[TotalSeries {
    kalshi_series: "KXLOLTOTALMAPS",
    poly_type: "esportsseriestotalgames",
    label: "Series games",
}];
const ATP_TOTAL_SERIES: &[TotalSeries] = &[
    TotalSeries {
        kalshi_series: "KXATPGTOTAL",
        poly_type: "tennismatchtotalgames",
        label: "Match games",
    },
    TotalSeries {
        kalshi_series: "KXATPTOTALSETS",
        poly_type: "tennismatchtotalsets",
        label: "Match sets",
    },
];
const WTA_TOTAL_SERIES: &[TotalSeries] = &[TotalSeries {
    kalshi_series: "KXWTAGTOTAL",
    poly_type: "tennismatchtotalgames",
    label: "Match games",
}];

#[derive(Clone, Copy)]
enum TennisPropKind {
    SetWinner,
    ExactScore,
}

const ATP_PROP_SERIES: &[(&str, TennisPropKind)] = &[
    ("KXATPSETWINNER", TennisPropKind::SetWinner),
    ("KXATPEXACTMATCH", TennisPropKind::ExactScore),
];
const WTA_PROP_SERIES: &[(&str, TennisPropKind)] = &[
    ("KXWTASETWINNER", TennisPropKind::SetWinner),
    ("KXWTAEXACTMATCH", TennisPropKind::ExactScore),
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

fn kalshi_market_url(ticker: &str) -> String {
    let mut url = Url::parse("https://kalshi.com/markets/").expect("static Kalshi URL");
    url.path_segments_mut()
        .expect("URL supports paths")
        .pop_if_empty()
        .push(ticker);
    url.into()
}

fn polymarket_market_url(event_slug: &str, market_slug: &str) -> String {
    let mut url = Url::parse("https://polymarket.us/event/").expect("static Polymarket URL");
    url.path_segments_mut()
        .expect("URL supports paths")
        .pop_if_empty()
        .push(event_slug);
    url.query_pairs_mut().append_pair("marketSlug", market_slug);
    url.into()
}

fn attach_polymarket_urls(pairs: &mut [Pair], details: &HashMap<String, Value>) {
    let mut urls = HashMap::new();
    for (event_slug, detail) in details {
        for market in detail["event"]["markets"].as_array().into_iter().flatten() {
            if let Some(slug) = market["slug"].as_str() {
                urls.insert(slug, polymarket_market_url(event_slug, slug));
            }
        }
    }
    for pair in pairs {
        if let Some(url) = urls.get(pair.poly_slug.as_str()) {
            pair.poly_url = url.clone();
        }
    }
}

struct DisplaySide<'a> {
    url: &'a str,
    available: u64,
    outcome: &'a str,
    average_price: f64,
    fee: f64,
    levels: usize,
}

fn comma_count(value: u64) -> String {
    let digits = value.to_string();
    let mut result = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index) % 3 == 0 {
            result.push(',');
        }
        result.push(digit);
    }
    result
}

fn compact_position(event: &str, outcome: &str) -> String {
    let (team, line) = outcome
        .split_once(" +")
        .map(|(team, line)| (team, format!(" +{line}")))
        .or_else(|| {
            outcome
                .split_once(" -")
                .map(|(team, line)| (team, format!(" -{line}")))
        })
        .unwrap_or((outcome, String::new()));
    for event_team in event.split(" vs. ").flat_map(|part| part.split(" vs ")) {
        let Some(abbreviation) = event_team.split_whitespace().next() else {
            continue;
        };
        if !(2..=4).contains(&abbreviation.len())
            || !abbreviation.chars().all(|ch| ch.is_ascii_uppercase())
        {
            continue;
        }
        let city_matches =
            team_city(&canonical_team(event_team)).is_some_and(|city| normal(team) == city);
        if same_team(team, event_team) || city_matches {
            return format!("{abbreviation}{line}");
        }
    }
    outcome.to_owned()
}

fn terminal_market_link(label: &str, url: &str) -> String {
    if io::stdout().is_terminal()
        && env::var("TERM").as_deref() != Ok("dumb")
        && url.starts_with("https://")
        && !url.chars().any(char::is_control)
    {
        format!("\x1b]8;;{url}\x1b\\{label}\x1b]8;;\x1b\\")
    } else {
        label.to_owned()
    }
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
    use std::fmt::Write as _;

    let kalshi_required = kalshi.average_price * contracts as f64 + kalshi.fee;
    let polymarket_required = polymarket.average_price * contracts as f64 + polymarket.fee;
    let required = kalshi_required + polymarket_required;
    let roi = if required > 0.0 {
        net_profit / required * 100.0
    } else {
        0.0
    };
    let market = market
        .split_whitespace()
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect::<Vec<_>>()
        .join(" ");
    let rows = [
        (
            "Position",
            compact_position(event, kalshi.outcome),
            compact_position(event, polymarket.outcome),
        ),
        (
            "Price",
            format!("${:.4}", kalshi.average_price),
            format!("${:.4}", polymarket.average_price),
        ),
        (
            "Fee",
            format!("${:.4}", kalshi.fee),
            format!("${:.4}", polymarket.fee),
        ),
        (
            "Levels",
            comma_count(kalshi.levels as u64),
            comma_count(polymarket.levels as u64),
        ),
        (
            "Visible Depth",
            comma_count(kalshi.available),
            comma_count(polymarket.available),
        ),
        (
            "Required",
            format!("${kalshi_required:.4}"),
            format!("${polymarket_required:.4}"),
        ),
    ];
    // Expand the first venue column for long player/team names instead of
    // truncating positions or letting them run into the second venue.
    let width = rows
        .iter()
        .map(|(_, left, _)| left.chars().count() + 3)
        .max()
        .unwrap_or(20)
        .max(20);
    let mut output = format!(
        "[{timestamp}] {event}\n{market}\n\nNET PROFIT   +${net_profit:.2}    CONTRACTS   {}    ROI   {roi:+.2}%\n\n",
        comma_count(contracts),
    );
    // Pad using the visible label length, excluding hyperlink escape codes.
    let kalshi_header = terminal_market_link("KALSHI", kalshi.url);
    let polymarket_header = terminal_market_link("POLYMARKET", polymarket.url);
    let gap = " ".repeat(width - "KALSHI".len());
    writeln!(output, "{:17}{kalshi_header}{gap}{polymarket_header}", "").unwrap();
    for (label, left, right) in rows {
        writeln!(output, "{label:17}{left:<width$}{right}").unwrap();
    }
    writeln!(
        output,
        "\n{:17}DRY RUN (depth optimized, balances ignored)",
        "Execution"
    )
    .unwrap();
    writeln!(
        output,
        "\n{:17}Kalshi {}  |  Polymarket {}",
        "Balances", balances.kalshi, balances.polymarket
    )
    .unwrap();
    write!(output, "\n{}", "─".repeat(80)).unwrap();
    output
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
    kalshi_available: u64,
    polymarket_available: u64,
    kalshi_fill: Fill,
    polymarket_fill: Fill,
    contracts: u64,
    gross_profit: f64,
    net_profit: f64,
}

fn number(v: Option<&Value>) -> Option<f64> {
    v.and_then(|v| v.as_f64().or_else(|| v.as_str()?.parse().ok()))
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
fn is_esports(sport: Sport) -> bool {
    matches!(sport.label, "CS2" | "VALORANT" | "DOTA2" | "LOL" | "R6")
}
fn is_tennis(sport: Sport) -> bool {
    matches!(sport.label, "ATP" | "WTA")
}
fn tennis_player_key(value: &str) -> String {
    let key = normal(value);
    match key.as_str() {
        "adolfodanielvallejo" | "adolfovallejo" => "adolfovallejo",
        "mayarsherifahmedabdelaziz" | "maiarsherifahmedabdelaziz" | "mayarsherif" => "mayarsherif",
        "yuliiastarodubtseva" | "yuliastarodubtseva" => "yuliastarodubtseva",
        _ => key.as_str(),
    }
    .to_owned()
}
fn same_tennis_player(a: &str, b: &str) -> bool {
    let (a, b) = (tennis_player_key(a), tennis_player_key(b));
    !a.is_empty()
        && !b.is_empty()
        && (a == b || (a.len() >= 4 && b.ends_with(&a)) || (b.len() >= 4 && a.ends_with(&b)))
}
fn esports_team_key(value: &str) -> String {
    let key = value
        .to_lowercase()
        .chars()
        .filter(|character| character.is_alphanumeric())
        .collect::<String>();
    // Venue names observed for the same team; keep this list explicit so an
    // academy or similarly named roster cannot match by a shared substring.
    match key.as_str() {
        "berlininternationalgaming" => "big",
        "yakultsbrothers" => "yakultbrothers",
        "senshiesportsclub" => "senshiesports",
        "thesecretclubesport" => "thesecretclub",
        "barçaesports" => "barcaesports",
        "pcificesports" => "pcific",
        _ => &key,
    }
    .to_owned()
}
fn same_esports_team(a: &str, b: &str) -> bool {
    let (a, b) = (esports_team_key(a), esports_team_key(b));
    !a.is_empty() && a == b
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
        "atlanta" | "atlbraves" | "atlantabraves" => "atlantabraves",
        "as" | "athletics" | "oakland" | "oaklandathletics" => "athletics",
        "baltimore" | "baltimoreorioles" => "baltimoreorioles",
        "boston" | "bostonredsox" => "bostonredsox",
        "chicagoc" | "chicagocubs" => "chicagocubs",
        "chicagows" | "chiwhitesox" | "chicagowhitesox" => "chicagowhitesox",
        "cincinnati" | "cincinnatireds" => "cincinnatireds",
        "cleveland" | "cleguardians" | "clevelandguardians" => "clevelandguardians",
        "colorado" | "coloradorockies" => "coloradorockies",
        "detroit" | "detroittigers" => "detroittigers",
        "houston" | "houstonastros" => "houstonastros",
        "kansascity" | "kansascityroyals" => "kansascityroyals",
        "losangelesa" | "losangelesangels" => "losangelesangels",
        "losangelesd" | "losangelesdodgers" => "losangelesdodgers",
        "miami" | "miamimarlins" => "miamimarlins",
        "milwaukee" | "milbrewers" | "milwaukeebrewers" => "milwaukeebrewers",
        "minnesota" | "minnesotatwins" => "minnesotatwins",
        "newyorkm" | "newyorkmets" => "newyorkmets",
        "newyorky" | "nyyankees" | "newyorkyankees" => "newyorkyankees",
        "philadelphia" | "phiphillies" | "philadelphiaphillies" => "philadelphiaphillies",
        "pittsburgh" | "pittsburghpirates" => "pittsburghpirates",
        "sandiego" | "sdpadres" | "sandiegopadres" => "sandiegopadres",
        "sanfrancisco" | "sanfranciscogiants" => "sanfranciscogiants",
        "seattle" | "seattlemariners" => "seattlemariners",
        "stlouis" | "stlouiscardinals" => "stlouiscardinals",
        "tampabay" | "tbrays" | "tampabayrays" => "tampabayrays",
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
    let matchup = value
        .strip_prefix("Game ")
        .and_then(|rest| {
            let (number, matchup) = rest.split_once(": ")?;
            number.parse::<u8>().ok().map(|_| matchup)
        })
        .unwrap_or(value);
    let teams: Vec<_> = matchup
        .split(':')
        .next()
        .unwrap_or(matchup)
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

fn matchup_key(event: &Event, date: NaiveDate, sport: Sport) -> Option<String> {
    let [first, second] = event_team_keys(&event.title)?;
    let key = if is_esports(sport) {
        esports_team_key
    } else if matches!(sport.label, "CBB" | "WCBB") {
        college_team_key
    } else if is_tennis(sport) {
        tennis_player_key
    } else {
        canonical_team
    };
    let mut teams = if sport.label == "NBA" {
        [
            nba_team_key(&first)?.to_owned(),
            nba_team_key(&second)?.to_owned(),
        ]
    } else {
        [key(&first), key(&second)]
    };
    if teams[0].is_empty() || teams[1].is_empty() || teams[0] == teams[1] {
        return None;
    }
    teams.sort();
    Some(format!("{date}:{}:{}", teams[0], teams[1]))
}

fn eastern_utc_offset_hours(date: NaiveDate) -> i64 {
    let year = date.year();
    let second_sunday_march = {
        let first = NaiveDate::from_ymd_opt(year, 3, 1).expect("valid March date");
        let first_sunday = 1 + (7 - first.weekday().num_days_from_sunday()) % 7;
        first
            .with_day(first_sunday + 7)
            .expect("valid March Sunday")
    };
    let first_sunday_november = {
        let first = NaiveDate::from_ymd_opt(year, 11, 1).expect("valid November date");
        let first_sunday = 1 + (7 - first.weekday().num_days_from_sunday()) % 7;
        first.with_day(first_sunday).expect("valid November Sunday")
    };
    if date >= second_sunday_march && date < first_sunday_november {
        4
    } else {
        5
    }
}

fn eastern_date_from_utc(utc: NaiveDateTime) -> NaiveDate {
    let first_guess = (utc - chrono::Duration::hours(eastern_utc_offset_hours(utc.date()))).date();
    (utc - chrono::Duration::hours(eastern_utc_offset_hours(first_guess))).date()
}

fn kalshi_start_utc(id: &str) -> Option<NaiveDateTime> {
    let kalshi_local = id
        .split_once('-')
        .and_then(|(_, suffix)| suffix.get(..11))
        .and_then(|value| NaiveDateTime::parse_from_str(value, "%y%b%d%H%M").ok())?;
    Some(kalshi_local + chrono::Duration::hours(eastern_utc_offset_hours(kalshi_local.date())))
}

fn polymarket_start_utc(polymarket_detail: &Value) -> Option<NaiveDateTime> {
    polymarket_detail
        .pointer("/event/startDate")
        .and_then(Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.naive_utc())
}

fn start_time_delta_minutes(kalshi_event: &Event, polymarket_detail: &Value) -> Option<i64> {
    Some(
        (kalshi_start_utc(&kalshi_event.id)? - polymarket_start_utc(polymarket_detail)?)
            .num_minutes()
            .abs(),
    )
}

fn mlb_start_time_matches(kalshi_event: &Event, polymarket_detail: &Value) -> bool {
    start_time_delta_minutes(kalshi_event, polymarket_detail) == Some(0)
}

fn same_scheduled_start_time(kalshi_event: &Event, poly_detail: &Value, sport: Sport) -> bool {
    if sport.label == "MLB" {
        mlb_start_time_matches(kalshi_event, poly_detail)
    } else if is_esports(sport) {
        start_time_delta_minutes(kalshi_event, poly_detail).is_some_and(|minutes| minutes <= 120)
    } else {
        true
    }
}

fn event_match_score(kalshi: &Event, polymarket: &Event, sport: Sport) -> f64 {
    if matches!(
        sport.label,
        "NFL" | "CFB" | "NBA" | "CBB" | "WCBB" | "MLB" | "ATP" | "WTA"
    ) {
        let kalshi_date = kalshi_event_date(&kalshi.id);
        if kalshi_date.is_none() || kalshi_date != polymarket_event_date(&polymarket.id) {
            return 0.0;
        }
    }
    if is_esports(sport) {
        let (Some(kalshi_date), Some(poly_date)) = (
            kalshi_event_date(&kalshi.id),
            polymarket_event_date(&polymarket.id),
        ) else {
            return 0.0;
        };
        if poly_date != kalshi_date && poly_date != kalshi_date + chrono::Duration::days(1) {
            return 0.0;
        }
        return match (
            event_team_keys(&kalshi.title),
            event_team_keys(&polymarket.title),
        ) {
            (Some(a), Some(b))
                if (same_esports_team(&a[0], &b[0]) && same_esports_team(&a[1], &b[1]))
                    || (same_esports_team(&a[0], &b[1]) && same_esports_team(&a[1], &b[0])) =>
            {
                1.0
            }
            _ => 0.0,
        };
    }
    if is_tennis(sport) || matches!(sport.label, "NBA" | "CBB" | "WCBB") {
        let same = if sport.label == "NBA" {
            same_nba_team
        } else if matches!(sport.label, "CBB" | "WCBB") {
            same_college_team
        } else {
            same_tennis_player
        };
        return match (
            event_team_keys(&kalshi.title),
            event_team_keys(&polymarket.title),
        ) {
            (Some(a), Some(b))
                if (same(&a[0], &b[0]) && same(&a[1], &b[1]))
                    || (same(&a[0], &b[1]) && same(&a[1], &b[0])) =>
            {
                1.0
            }
            _ => 0.0,
        };
    }
    event_similarity(&kalshi.title, &polymarket.title)
}

struct EventMatcher<'a> {
    sport: Sport,
    all: Vec<&'a Event>,
    by_date: HashMap<NaiveDate, Vec<&'a Event>>,
    blocked_matchups: HashSet<String>,
    poly_start_times: HashMap<&'a str, NaiveDateTime>,
}

impl<'a> EventMatcher<'a> {
    fn new(events: &'a [Event], sport: Sport) -> Self {
        let mut by_date: HashMap<NaiveDate, Vec<&Event>> = HashMap::new();
        if matches!(
            sport.label,
            "NFL" | "CFB" | "NBA" | "CBB" | "WCBB" | "MLB" | "ATP" | "WTA"
        ) {
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
            blocked_matchups: HashSet::new(),
            poly_start_times: HashMap::new(),
        }
    }

    fn set_poly_start_times(&mut self, details: &HashMap<String, Value>) {
        if !is_esports(self.sport) {
            return;
        }
        for event in &self.all {
            if let Some(start) = details.get(&event.id).and_then(polymarket_start_utc) {
                self.poly_start_times.insert(event.id.as_str(), start);
            }
        }
    }

    fn block_duplicate_matchups(&mut self, kalshi_games: &[Event]) {
        if !matches!(self.sport.label, "MLB" | "NBA" | "CBB" | "WCBB")
            && !is_esports(self.sport)
            && !is_tennis(self.sport)
        {
            return;
        }
        let mut counts = HashMap::new();
        for event in kalshi_games {
            if let Some(key) =
                kalshi_event_date(&event.id).and_then(|date| matchup_key(event, date, self.sport))
            {
                *counts.entry(key).or_insert(0_usize) += 1;
            }
        }
        for (key, count) in counts {
            if count > 1 {
                self.blocked_matchups.insert(key);
            }
        }
        let mut poly_counts = HashMap::new();
        for event in &self.all {
            let date = if is_esports(self.sport) {
                self.poly_start_times
                    .get(event.id.as_str())
                    .map(|start| eastern_date_from_utc(*start))
            } else {
                polymarket_event_date(&event.id)
            };
            if let Some(key) = date.and_then(|date| matchup_key(event, date, self.sport)) {
                *poly_counts.entry(key).or_insert(0_usize) += 1;
            }
        }
        for (key, count) in poly_counts {
            if count > 1 {
                self.blocked_matchups.insert(key);
            }
        }
    }

    fn is_blocked(&self, event: &Event) -> bool {
        (matches!(self.sport.label, "MLB" | "NBA" | "CBB" | "WCBB")
            || is_esports(self.sport)
            || is_tennis(self.sport))
            && kalshi_event_date(&event.id)
                .and_then(|date| matchup_key(event, date, self.sport))
                .is_some_and(|key| self.blocked_matchups.contains(&key))
    }

    fn best_match(&self, kalshi: &Event) -> Option<&'a Event> {
        if self.is_blocked(kalshi) {
            return None;
        }
        let candidates = if matches!(
            self.sport.label,
            "NFL" | "CFB" | "NBA" | "CBB" | "WCBB" | "MLB" | "ATP" | "WTA"
        ) {
            self.by_date
                .get(&kalshi_event_date(&kalshi.id)?)?
                .as_slice()
        } else {
            self.all.as_slice()
        };
        if is_esports(self.sport) {
            let start = kalshi_start_utc(&kalshi.id)?;
            candidates
                .iter()
                .copied()
                .filter(|event| event_match_score(kalshi, event, self.sport) >= 0.72)
                .filter_map(|event| {
                    let delta = (start - *self.poly_start_times.get(event.id.as_str())?)
                        .num_minutes()
                        .abs();
                    (delta <= 120).then_some((event, delta))
                })
                .min_by_key(|(_, delta)| *delta)
                .map(|(event, _)| event)
        } else {
            candidates
                .iter()
                .copied()
                .map(|event| (event, event_match_score(kalshi, event, self.sport)))
                .max_by(|a, b| a.1.total_cmp(&b.1))
                .filter(|(_, score)| *score >= 0.72)
                .map(|(event, _)| event)
        }
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
        .find(|side| side["long"].as_bool() == Some(long))?["team"]["name"]
        .as_str()
        .or_else(|| {
            market["marketSides"]
                .as_array()?
                .iter()
                .find(|side| side["long"].as_bool() == Some(long))?["team"]["safeName"]
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

fn total_scope_matches(market: &Value, spec: TotalSeries) -> bool {
    if market["marketType"].as_str() != Some("totals") {
        return false;
    }
    let Some(kind) = market["sportsMarketType"].as_str().map(normal) else {
        return false;
    };
    if kind == spec.poly_type
        || (spec.poly_type == "footballgametotal" && kind == "footballgamefullgametotal")
    {
        return true;
    }
    // Polymarket currently labels whole-game NFL totals as "team" totals.
    // The game-total slug and absence of a team on either side distinguish
    // them from actual team-point totals.
    spec.poly_type == "footballgametotal"
        && kind == "footballteamfullgametotal"
        && market["slug"]
            .as_str()
            .is_some_and(|slug| slug.contains("-total-"))
        && market["marketSides"].as_array().is_some_and(|sides| {
            sides.len() == 2
                && sides
                    .iter()
                    .all(|side| side["team"].is_null() && side["teamId"].is_null())
        })
}

fn prop_series(sport: Sport) -> &'static [PropSeries] {
    match sport.label {
        "NFL" => NFL_PROP_SERIES,
        "CFB" => CFB_PROP_SERIES,
        "MLB" => MLB_PROP_SERIES,
        _ => &[],
    }
}

fn spread_series(sport: Sport) -> &'static [SpreadSeries] {
    match sport.label {
        "NBA" => NBA_SPREAD_SERIES,
        "CBB" => CBB_SPREAD_SERIES,
        "WCBB" => WCBB_SPREAD_SERIES,
        "NFL" => NFL_SPREAD_SERIES,
        "CFB" => CFB_SPREAD_SERIES,
        "MLB" => MLB_SPREAD_SERIES,
        "ATP" => ATP_SPREAD_SERIES,
        _ => &[],
    }
}

fn total_series(sport: Sport) -> &'static [TotalSeries] {
    match sport.label {
        "NBA" => NBA_TOTAL_SERIES,
        "CBB" => CBB_TOTAL_SERIES,
        "WCBB" => WCBB_TOTAL_SERIES,
        "NFL" => NFL_TOTAL_SERIES,
        "CFB" => CFB_TOTAL_SERIES,
        "MLB" => MLB_TOTAL_SERIES,
        "CS2" => CS2_TOTAL_SERIES,
        "VALORANT" => VALORANT_TOTAL_SERIES,
        "DOTA2" => DOTA2_TOTAL_SERIES,
        "LOL" => LOL_TOTAL_SERIES,
        "ATP" => ATP_TOTAL_SERIES,
        "WTA" => WTA_TOTAL_SERIES,
        _ => &[],
    }
}

fn esports_map_series(sport: Sport) -> Option<(&'static str, &'static str, &'static str)> {
    match sport.label {
        "CS2" => Some(("KXCS2MAP", "Map", "esportsmapwinner")),
        "VALORANT" => Some(("KXVALORANTMAP", "Map", "esportsmapwinner")),
        "DOTA2" => Some(("KXDOTA2MAP", "Game", "esportsgamewinner")),
        "LOL" => Some(("KXLOLMAP", "Game", "esportsgamewinner")),
        "R6" => Some(("KXR6MAP", "Map", "esportsmapwinner")),
        _ => None,
    }
}

fn esports_map_market_number(market: &Value, sport: Sport) -> Option<u8> {
    let (_, _, kind) = esports_map_series(sport)?;
    if market["marketType"].as_str() != Some("props") {
        return None;
    }
    let sports_type = normal(market["sportsMarketType"].as_str()?);
    let number = sports_type.strip_prefix(kind)?.parse::<u8>().ok()?;
    (number > 0
        && side_label(market, true).as_deref() == Some("Yes")
        && side_label(market, false).as_deref() == Some("No"))
    .then_some(number)
}

fn tennis_prop_series(sport: Sport) -> &'static [(&'static str, TennisPropKind)] {
    match sport.label {
        "ATP" => ATP_PROP_SERIES,
        "WTA" => WTA_PROP_SERIES,
        _ => &[],
    }
}

fn tennis_set_winner_number(market: &Value) -> Option<u8> {
    if market["marketType"].as_str() != Some("props") {
        return None;
    }
    let kind = normal(market["sportsMarketType"].as_str()?);
    let number = kind
        .strip_prefix("tennisset")?
        .strip_suffix("winner")?
        .parse::<u8>()
        .ok()?;
    (number > 0
        && side_label(market, true).as_deref() == Some("Yes")
        && side_label(market, false).as_deref() == Some("No"))
    .then_some(number)
}

fn tennis_exact_score_market(market: &Value) -> bool {
    market["marketType"].as_str() == Some("props")
        && market["sportsMarketType"].as_str().map(normal).as_deref()
            == Some("tennismatchexactscore")
        && side_label(market, true).as_deref() == Some("Yes")
        && side_label(market, false).as_deref() == Some("No")
}

fn tennis_score(value: &str) -> Option<(u8, u8)> {
    let (winner, loser) = value.split_once('-')?;
    let (winner, loser) = (winner.parse::<u8>().ok()?, loser.parse::<u8>().ok()?);
    (winner >= 2 && winner <= 3 && loser < winner).then_some((winner, loser))
}

fn spread_scope_matches(market: &Value, sport: Sport, spec: SpreadSeries) -> bool {
    if market["marketType"].as_str() != Some("spreads") {
        return false;
    }
    let scope = market["sportsMarketType"]
        .as_str()
        .map(normal)
        .unwrap_or_default();
    let expected = if is_tennis(sport) {
        "tennismatchgamesspread".to_owned()
    } else if matches!(sport.label, "NBA" | "CBB" | "WCBB") {
        "basketballteamfullgamespread".to_owned()
    } else if sport.label == "MLB" {
        if spec.period.is_empty() {
            "baseballteamfullgamespread".to_owned()
        } else {
            format!("baseballteam{}spread", spec.period)
        }
    } else if spec.period.is_empty() {
        "footballteamspread".to_owned()
    } else {
        format!("footballteam{}spread", spec.period)
    };
    scope == expected
        || (matches!(sport.label, "NFL" | "CFB")
            && spec.period.is_empty()
            && scope == "footballteamfullgamespread")
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
    if sport.label == "MLB" {
        return pair_mlb_props(kalshi, polymarket, sport, spec);
    }
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
                safety: [None, None],
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
                poly_url: String::new(),
                poly_long_is_first: true,
            })
        })
        .collect()
}

fn pair_mlb_props(kalshi: &Value, polymarket: &Value, sport: Sport, spec: PropSeries) -> Vec<Pair> {
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
            let strike = number(market.get("floor_strike"))?;
            let entity = match spec.kind {
                PropKind::Player => market["title"]
                    .as_str()?
                    .split_once(':')?
                    .0
                    .trim()
                    .to_owned(),
                PropKind::Team => market["yes_sub_title"]
                    .as_str()?
                    .split_once(" over ")?
                    .0
                    .trim()
                    .to_owned(),
            };
            let poly = poly_markets.iter().find(|poly| {
                if !polymarket_market_open(poly)
                    || normal(poly["sportsMarketType"].as_str().unwrap_or("")) != spec.poly_type
                    || side_label(poly, true).as_deref() != Some("Yes")
                    || side_label(poly, false).as_deref() != Some("No")
                {
                    return false;
                }
                let Some(poly_line) = number(poly.get("line")) else {
                    return false;
                };
                let question = poly["question"].as_str().unwrap_or("");
                match spec.kind {
                    PropKind::Player => {
                        let Some((name, threshold)) = question
                            .strip_prefix("Will ")
                            .and_then(|question| question.split_once(" record at least "))
                        else {
                            return false;
                        };
                        let stated_threshold = threshold
                            .split_whitespace()
                            .next()
                            .and_then(|value| value.parse::<f64>().ok());
                        normal(name) == normal(&entity)
                            && stated_threshold
                                .is_some_and(|value| (value - poly_line).abs() < 1e-9)
                            && (poly_line - strike - 0.5).abs() < 1e-9
                    }
                    PropKind::Team => {
                        let Some((name, threshold)) = question
                            .strip_prefix("Will ")
                            .and_then(|question| question.split_once(" score more than "))
                        else {
                            return false;
                        };
                        let stated_threshold = threshold
                            .split_whitespace()
                            .next()
                            .and_then(|value| value.parse::<f64>().ok());
                        same_team(name, &entity)
                            && market_team(poly, true).is_some_and(|name| same_team(&name, &entity))
                            && stated_threshold
                                .is_some_and(|value| (value - poly_line).abs() < 1e-9)
                            && (poly_line - strike).abs() < 1e-9
                    }
                }
            })?;
            let threshold = if spec.kind == PropKind::Player {
                strike + 0.5
            } else {
                strike
            };
            let teams = match spec.kind {
                PropKind::Player => [
                    format!(
                        "{entity} at least {threshold} {}",
                        spec.label.to_lowercase()
                    ),
                    format!("{entity} below {threshold} {}", spec.label.to_lowercase()),
                ],
                PropKind::Team => [
                    format!("{entity} Over {threshold} runs"),
                    format!("{entity} Under {threshold} runs"),
                ],
            };
            Some(Pair {
                safety: [None, None],
                sport: sport.label,
                title: polymarket["event"]["title"].as_str()?.to_owned(),
                market: match spec.kind {
                    PropKind::Player => {
                        format!("Player {} ({threshold})", spec.label.to_lowercase())
                    }
                    PropKind::Team => format!("{} ({threshold})", spec.label),
                },
                kalshi: [market["ticker"].as_str()?.to_owned(), String::new()],
                teams,
                poly_slug: poly["slug"].as_str()?.to_owned(),
                poly_url: String::new(),
                poly_long_is_first: true,
            })
        })
        .collect()
}

fn pair_esports_map_winners(kalshi: &Value, polymarket: &Value, sport: Sport) -> Vec<Pair> {
    let Some((_, unit, _)) = esports_map_series(sport) else {
        return Vec::new();
    };
    let Some(number) = kalshi["title"]
        .as_str()
        .and_then(|title| title.rsplit_once(": Map "))
        .and_then(|(_, number)| number.parse::<u8>().ok())
    else {
        return Vec::new();
    };
    let suffix = format!(" wins map {number}");
    let kalshi_outcomes: Vec<_> = kalshi["markets"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|market| kalshi_market_open(market))
        .filter_map(|market| {
            Some((
                market["title"].as_str()?.strip_suffix(&suffix)?.to_owned(),
                market["ticker"].as_str()?.to_owned(),
            ))
        })
        .collect();
    if kalshi_outcomes.len() != 2 {
        return Vec::new();
    }
    let Some(poly_markets) = polymarket
        .pointer("/event/markets")
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };
    poly_markets
        .iter()
        .filter(|market| polymarket_market_open(market))
        .filter_map(|market| {
            if esports_map_market_number(market, sport) != Some(number) {
                return None;
            }
            let subject = market_team(market, true)?;
            if !market_team(market, false).is_some_and(|other| same_esports_team(&subject, &other))
            {
                return None;
            }
            let marker = format!(" win {unit} {number} vs ");
            let (question_subject, question_opponent) = market["question"]
                .as_str()?
                .strip_prefix("Will ")?
                .split_once(&marker)?;
            if !same_esports_team(question_subject, &subject) {
                return None;
            }
            let question_opponent = question_opponent.trim_end_matches('?').trim();
            let other = kalshi_outcomes
                .iter()
                .find(|(team, _)| !same_esports_team(team, &subject))?
                .0
                .clone();
            if !same_esports_team(question_opponent, &other) {
                return None;
            }
            let (teams, tickers, poly_long_is_first) =
                align_moneyline_with(kalshi_outcomes.clone(), [subject, other], same_esports_team)?;
            Some(Pair {
                safety: [None, None],
                sport: sport.label,
                title: polymarket["event"]["title"].as_str()?.to_owned(),
                market: format!("{unit} {number} winner"),
                kalshi: tickers,
                teams,
                poly_slug: market["slug"].as_str()?.to_owned(),
                poly_url: String::new(),
                poly_long_is_first,
            })
        })
        .collect()
}

fn pair_tennis_set_winners(kalshi: &Value, polymarket: &Value, sport: Sport) -> Vec<Pair> {
    let Some(number) = kalshi["title"]
        .as_str()
        .and_then(|title| title.rsplit_once(": Set "))
        .and_then(|(_, suffix)| suffix.strip_suffix(" Winner"))
        .and_then(|number| number.parse::<u8>().ok())
    else {
        return Vec::new();
    };
    let outcomes: Vec<_> = kalshi["markets"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|market| kalshi_market_open(market))
        .filter_map(|market| {
            Some((
                market["yes_sub_title"].as_str()?.to_owned(),
                market["ticker"].as_str()?.to_owned(),
            ))
        })
        .collect();
    if outcomes.len() != 2 || same_tennis_player(&outcomes[0].0, &outcomes[1].0) {
        return Vec::new();
    }
    let Some(poly_markets) = polymarket
        .pointer("/event/markets")
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };
    poly_markets
        .iter()
        .filter(|market| polymarket_market_open(market))
        .filter_map(|market| {
            if tennis_set_winner_number(market) != Some(number) {
                return None;
            }
            let subject = market_team(market, true)?;
            if !market_team(market, false).is_some_and(|other| same_tennis_player(&subject, &other))
            {
                return None;
            }
            let marker = format!(" win set {number} against ");
            let (question_subject, question_opponent) = market["question"]
                .as_str()?
                .strip_prefix("Will ")?
                .split_once(&marker)?;
            let question_opponent = question_opponent.trim_end_matches('?').trim();
            let other = outcomes
                .iter()
                .find(|(player, _)| !same_tennis_player(player, &subject))?
                .0
                .clone();
            if !same_tennis_player(question_subject, &subject)
                || !same_tennis_player(question_opponent, &other)
            {
                return None;
            }
            let (teams, tickers, poly_long_is_first) =
                align_moneyline_with(outcomes.clone(), [subject, other], same_tennis_player)?;
            Some(Pair {
                safety: [None, None],
                sport: sport.label,
                title: polymarket["event"]["title"].as_str()?.to_owned(),
                market: format!("Set {number} winner"),
                kalshi: tickers,
                teams,
                poly_slug: market["slug"].as_str()?.to_owned(),
                poly_url: String::new(),
                poly_long_is_first,
            })
        })
        .collect()
}

fn pair_tennis_exact_scores(kalshi: &Value, polymarket: &Value, sport: Sport) -> Vec<Pair> {
    let Some(poly_markets) = polymarket
        .pointer("/event/markets")
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };
    let Some([first, second]) = polymarket["event"]["title"]
        .as_str()
        .and_then(event_team_keys)
    else {
        return Vec::new();
    };
    kalshi["markets"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|market| kalshi_market_open(market))
        .filter_map(|market| {
            let (player, score) = market["yes_sub_title"].as_str()?.rsplit_once(" wins ")?;
            let score = tennis_score(score)?;
            if market["custom_strike"]["Set Score"]
                .as_str()
                .and_then(tennis_score)
                != Some(score)
            {
                return None;
            }
            let player_is_first = same_tennis_player(player, &first);
            if player_is_first == same_tennis_player(player, &second) {
                return None;
            }
            let poly = poly_markets.iter().find(|poly| {
                if !polymarket_market_open(poly) || !tennis_exact_score_market(poly) {
                    return false;
                }
                let Some((poly_player, poly_score)) = poly["question"]
                    .as_str()
                    .and_then(|question| question.rsplit_once(" wins "))
                else {
                    return false;
                };
                tennis_score(poly_score) == Some(score)
                    && same_tennis_player(poly_player, player)
                    && same_tennis_player(
                        poly_player,
                        if player_is_first { &first } else { &second },
                    )
                    && !same_tennis_player(
                        poly_player,
                        if player_is_first { &second } else { &first },
                    )
            })?;
            let outcome = format!("{player} wins {}-{}", score.0, score.1);
            Some(Pair {
                safety: [None, None],
                sport: sport.label,
                title: polymarket["event"]["title"].as_str()?.to_owned(),
                market: format!("Exact match score ({}-{})", score.0, score.1),
                kalshi: [market["ticker"].as_str()?.to_owned(), String::new()],
                teams: [outcome.clone(), format!("Not {outcome}")],
                poly_slug: poly["slug"].as_str()?.to_owned(),
                poly_url: String::new(),
                poly_long_is_first: true,
            })
        })
        .collect()
}

fn pair_spreads(kalshi: &Value, polymarket: &Value, sport: Sport, spec: SpreadSeries) -> Vec<Pair> {
    if is_tennis(sport) {
        return pair_tennis_game_spreads(kalshi, polymarket, sport, spec);
    }
    let same = if sport.label == "NBA" {
        same_nba_team
    } else if matches!(sport.label, "CBB" | "WCBB") {
        same_college_team
    } else {
        same_team
    };
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
            let (kalshi_team, remainder) = market["yes_sub_title"]
                .as_str()
                .and_then(|title| title.split_once(" wins"))
                .or_else(|| market["title"].as_str()?.split_once(" wins"))?;
            if !remainder.contains("by over") {
                return None;
            }
            let kalshi_team = kalshi_team.trim().to_owned();
            let poly = poly_markets.iter().find(|poly| {
                if !polymarket_market_open(poly)
                    || !spread_scope_matches(poly, sport, spec)
                    || number(poly.get("line"))
                        .is_none_or(|value| (value.abs() - line).abs() >= 1e-9)
                {
                    return false;
                }
                let long = market_team(poly, true);
                let short = market_team(poly, false);
                match (long, short, number(poly.get("line"))) {
                    (Some(long), Some(short), Some(poly_line)) => {
                        let matches_long = same(&kalshi_team, &long);
                        let matches_short = same(&kalshi_team, &short);
                        matches_long != matches_short
                            && ((matches_long && (poly_line + line).abs() < 1e-9)
                                || (matches_short && (poly_line - line).abs() < 1e-9))
                    }
                    _ => false,
                }
            })?;
            let long = market_team(poly, true)?;
            let short = market_team(poly, false)?;
            let opposite = if same(&kalshi_team, &long) {
                short
            } else {
                long.clone()
            };
            Some(Pair {
                safety: [None, None],
                sport: sport.label,
                title: polymarket["event"]["title"].as_str()?.to_owned(),
                market: format!("{} spread ({line:+})", spec.label),
                kalshi: [market["ticker"].as_str()?.to_owned(), String::new()],
                teams: [
                    format!("{kalshi_team} -{line}"),
                    format!("{opposite} +{line}"),
                ],
                poly_slug: poly["slug"].as_str()?.to_owned(),
                poly_url: String::new(),
                poly_long_is_first: same(&kalshi_team, &long),
            })
        })
        .collect()
}

fn pair_tennis_game_spreads(
    kalshi: &Value,
    polymarket: &Value,
    sport: Sport,
    spec: SpreadSeries,
) -> Vec<Pair> {
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
            if ((line.fract().abs()) - 0.5).abs() >= 1e-9 {
                return None;
            }
            let subtitle = market["yes_sub_title"].as_str()?;
            let (player, handicap) = subtitle.rsplit_once(" -")?;
            if !handicap.ends_with(" games")
                || (handicap.strip_suffix(" games")?.parse::<f64>().ok()? - line).abs() >= 1e-9
            {
                return None;
            }
            let poly = poly_markets.iter().find(|poly| {
                if !polymarket_market_open(poly)
                    || !spread_scope_matches(poly, sport, spec)
                    || number(poly.get("line"))
                        .is_none_or(|value| (value.abs() - line).abs() >= 1e-9)
                {
                    return false;
                }
                let (Some(long), Some(short), Some(poly_line)) = (
                    market_team(poly, true),
                    market_team(poly, false),
                    number(poly.get("line")),
                ) else {
                    return false;
                };
                let matches_long = same_tennis_player(player, &long);
                let matches_short = same_tennis_player(player, &short);
                matches_long != matches_short
                    && ((matches_long && (poly_line + line).abs() < 1e-9)
                        || (matches_short && (poly_line - line).abs() < 1e-9))
                    && side_label(poly, true)
                        .and_then(|value| value.parse::<f64>().ok())
                        .is_some_and(|value| (value - poly_line).abs() < 1e-9)
                    && side_label(poly, false)
                        .and_then(|value| value.parse::<f64>().ok())
                        .is_some_and(|value| (value + poly_line).abs() < 1e-9)
            })?;
            let long = market_team(poly, true)?;
            let short = market_team(poly, false)?;
            let poly_long_is_first = same_tennis_player(player, &long);
            let opposite = if poly_long_is_first { short } else { long };
            Some(Pair {
                safety: [None, None],
                sport: sport.label,
                title: polymarket["event"]["title"].as_str()?.to_owned(),
                market: format!("{} spread (-{line})", spec.label),
                kalshi: [market["ticker"].as_str()?.to_owned(), String::new()],
                teams: [format!("{player} -{line}"), format!("{opposite} +{line}")],
                poly_slug: poly["slug"].as_str()?.to_owned(),
                poly_url: String::new(),
                poly_long_is_first,
            })
        })
        .collect()
}

fn pair_totals(kalshi: &Value, polymarket: &Value, sport: Sport, spec: TotalSeries) -> Vec<Pair> {
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
                .as_str()
                .is_some_and(|title| title.starts_with("Over "))
                && !market["yes_sub_title"]
                    .as_str()
                    .is_some_and(|title| title.starts_with("Over "))
            {
                return None;
            }
            let poly = poly_markets.iter().find(|poly| {
                polymarket_market_open(poly)
                    && total_scope_matches(poly, spec)
                    && number(poly.get("line")).is_some_and(|value| (value - line).abs() < 1e-9)
                    && side_label(poly, true)
                        .is_some_and(|label| label.to_lowercase().contains("over"))
                    && side_label(poly, false)
                        .is_some_and(|label| label.to_lowercase().contains("under"))
            })?;
            Some(Pair {
                safety: [None, None],
                sport: sport.label,
                title: polymarket["event"]["title"].as_str()?.to_owned(),
                market: format!("{} total ({line})", spec.label),
                kalshi: [market["ticker"].as_str()?.to_owned(), String::new()],
                teams: [format!("Over {line}"), format!("Under {line}")],
                poly_slug: poly["slug"].as_str()?.to_owned(),
                poly_url: String::new(),
                poly_long_is_first: true,
            })
        })
        .collect()
}

fn align_moneyline(
    kalshi: Vec<(String, String)>,
    polymarket: [String; 2],
) -> Option<([String; 2], [String; 2], bool)> {
    align_moneyline_with(kalshi, polymarket, same_team)
}

fn align_moneyline_with(
    kalshi: Vec<(String, String)>,
    polymarket: [String; 2],
    same: fn(&str, &str) -> bool,
) -> Option<([String; 2], [String; 2], bool)> {
    if kalshi.len() != 2 || same(&polymarket[0], &polymarket[1]) {
        return None;
    }
    let mut teams = [String::new(), String::new()];
    let mut tickers = [String::new(), String::new()];
    for (i, (team, ticker)) in kalshi.into_iter().enumerate() {
        let matches = [same(&team, &polymarket[0]), same(&team, &polymarket[1])];
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

fn kalshi_tennis_match_title(event: &Value) -> Option<String> {
    let winners: Vec<_> = event["markets"]
        .as_array()?
        .iter()
        .filter(|market| kalshi_market_open(market))
        .filter_map(|market| market["title"].as_str().and_then(kalshi_winner))
        .collect();
    (winners.len() == 2 && !same_tennis_player(winners[0], winners[1]))
        .then(|| format!("{} vs {}", winners[0], winners[1]))
}

async fn kalshi_events(
    client: &Client,
    series: &str,
    catalog: &mut HashMap<String, Value>,
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
            for market in event["markets"].as_array().into_iter().flatten() {
                if let Some(ticker) = market["ticker"].as_str() {
                    catalog.insert(ticker.to_owned(), market.clone());
                }
            }
            let (open_markets, two_way_moneyline) = kalshi_event_counts(event)?;
            all.open_markets += open_markets;
            all.two_way_moneylines += usize::from(two_way_moneyline);
            if let (Some(id), Some(title)) =
                (event["event_ticker"].as_str(), event["title"].as_str())
            {
                let title = if matches!(series, "KXATPMATCH" | "KXWTAMATCH") {
                    kalshi_tennis_match_title(event).unwrap_or_else(|| title.to_owned())
                } else {
                    title.to_owned()
                };
                all.events.push(Event {
                    id: id.to_owned(),
                    title,
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
                .any(|spec| spread_scope_matches(market, sport, *spec))
            {
                counts.spreads += 1;
            } else if total_series(sport)
                .iter()
                .any(|spec| total_scope_matches(market, *spec))
            {
                counts.totals += 1;
            } else if prop_series(sport)
                .iter()
                .any(|spec| full_game_prop(market, *spec))
                || esports_map_market_number(market, sport).is_some()
                || (is_tennis(sport)
                    && (tennis_set_winner_number(market).is_some()
                        || tennis_exact_score_market(market)))
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
    catalog: &mut HashMap<String, Value>,
) -> Result<(Vec<Pair>, usize), Box<dyn Error>> {
    let mut out = Vec::new();
    let mut open_markets = 0;
    for spec in prop_series(sport) {
        let series = kalshi_events(client, spec.kalshi_series, catalog).await?;
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
            if !same_scheduled_start_time(kalshi_event, poly_detail, sport) {
                continue;
            }
            let pairs = pair_props(kalshi_detail, poly_detail, sport, *spec);
            if !pairs.is_empty() {
                matched_event_ids.insert(poly_event.id.clone());
            }
            out.extend(pairs);
        }
    }
    Ok((out, open_markets))
}

async fn discover_esports_maps(
    client: &Client,
    sport: Sport,
    matcher: &EventMatcher<'_>,
    polymarket_details: &HashMap<String, Value>,
    matched_event_ids: &mut HashSet<String>,
    catalog: &mut HashMap<String, Value>,
) -> Result<(Vec<Pair>, usize), Box<dyn Error>> {
    let Some((series_ticker, _, _)) = esports_map_series(sport) else {
        return Ok((Vec::new(), 0));
    };
    let series = kalshi_events(client, series_ticker, catalog).await?;
    let mut out = Vec::new();
    for kalshi_event in &series.events {
        let Some(poly_event) = matcher.best_match(kalshi_event) else {
            continue;
        };
        let kalshi_detail = series
            .details
            .get(&kalshi_event.id)
            .ok_or("Kalshi map event missing from discovery cache")?;
        let poly_detail = polymarket_details
            .get(&poly_event.id)
            .ok_or("Polymarket event detail missing from discovery cache")?;
        if !same_scheduled_start_time(kalshi_event, poly_detail, sport) {
            continue;
        }
        let pairs = pair_esports_map_winners(kalshi_detail, poly_detail, sport);
        if !pairs.is_empty() {
            matched_event_ids.insert(poly_event.id.clone());
        }
        out.extend(pairs);
    }
    Ok((out, series.open_markets))
}

async fn discover_tennis_props(
    client: &Client,
    sport: Sport,
    matcher: &EventMatcher<'_>,
    polymarket_details: &HashMap<String, Value>,
    matched_event_ids: &mut HashSet<String>,
    catalog: &mut HashMap<String, Value>,
) -> Result<(Vec<Pair>, usize), Box<dyn Error>> {
    let mut out = Vec::new();
    let mut open_markets = 0;
    for &(series_ticker, kind) in tennis_prop_series(sport) {
        let series = kalshi_events(client, series_ticker, catalog).await?;
        open_markets += series.open_markets;
        for kalshi_event in &series.events {
            let Some(poly_event) = matcher.best_match(kalshi_event) else {
                continue;
            };
            let kalshi_detail = series
                .details
                .get(&kalshi_event.id)
                .ok_or("Kalshi tennis prop event missing from discovery cache")?;
            let poly_detail = polymarket_details
                .get(&poly_event.id)
                .ok_or("Polymarket event detail missing from discovery cache")?;
            let pairs = match kind {
                TennisPropKind::SetWinner => {
                    pair_tennis_set_winners(kalshi_detail, poly_detail, sport)
                }
                TennisPropKind::ExactScore => {
                    pair_tennis_exact_scores(kalshi_detail, poly_detail, sport)
                }
            };
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
    catalog: &mut HashMap<String, Value>,
) -> Result<(Vec<Pair>, usize), Box<dyn Error>> {
    let mut out = Vec::new();
    let mut open_markets = 0;
    for spec in spread_series(sport) {
        let series = kalshi_events(client, spec.kalshi_series, catalog).await?;
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
            if !same_scheduled_start_time(kalshi_event, poly_detail, sport) {
                continue;
            }
            let pairs = pair_spreads(kalshi_detail, poly_detail, sport, *spec);
            if !pairs.is_empty() {
                matched_event_ids.insert(poly_event.id.clone());
            }
            out.extend(pairs);
        }
    }
    Ok((out, open_markets))
}

async fn discover(client: &Client, sport: Sport) -> Result<Discovery, Box<dyn Error>> {
    let mut catalog = HashMap::new();
    let (k, p) = tokio::join!(
        kalshi_events(client, sport.kalshi_series, &mut catalog),
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
    let mut matcher = EventMatcher::new(&polymarket_events_for_derivatives, sport);
    matcher.set_poly_start_times(&poly_details);
    matcher.block_duplicate_matchups(&k.events);
    let mut matched_event_ids = HashSet::new();
    let kalshi_details = k.details;
    let mut remaining = k.events;
    let mut matches = Vec::new();
    for p in p {
        let poly_event_detail = poly_details.get(&p.id);
        if let Some((i, score)) = remaining
            .iter()
            .enumerate()
            .map(|(i, k)| {
                let delta =
                    poly_event_detail.and_then(|detail| start_time_delta_minutes(k, detail));
                (
                    i,
                    if matcher.is_blocked(k)
                        || poly_event_detail
                            .is_none_or(|detail| !same_scheduled_start_time(k, detail, sport))
                    {
                        0.0
                    } else {
                        let score = event_match_score(k, &p, sport);
                        if is_esports(sport) && score > 0.0 {
                            score - delta.unwrap_or(120) as f64 / 10_000.0
                        } else {
                            score
                        }
                    },
                )
            })
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
        if !same_scheduled_start_time(&k, pd, sport) {
            continue;
        }
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
        let aligned = if is_esports(sport) {
            align_moneyline_with(km, ps, same_esports_team)
        } else if matches!(sport.label, "CBB" | "WCBB") {
            align_moneyline_with(km, ps, same_college_team)
        } else if sport.label == "NBA" {
            align_moneyline_with(km, ps, same_nba_team)
        } else if is_tennis(sport) {
            align_moneyline_with(km, ps, same_tennis_player)
        } else {
            align_moneyline(km, ps)
        };
        let Some((teams, tickers, poly_long_is_first)) = aligned else {
            continue;
        };
        out.push(Pair {
            safety: [None, None],
            sport: sport.label,
            title: p.title,
            market: if is_esports(sport) {
                "Match winner".into()
            } else {
                "Full game moneyline".into()
            },
            kalshi: tickers,
            teams,
            poly_slug: slug.into(),
            poly_url: String::new(),
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
        &mut catalog,
    )
    .await?;
    kalshi_counts.spreads = kalshi_spreads;
    let spread_pairs = spreads.len();
    out.extend(spreads);

    let before_totals = out.len();
    for spec in total_series(sport) {
        let series = kalshi_events(client, spec.kalshi_series, &mut catalog).await?;
        kalshi_counts.totals += series.open_markets;
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
            if !same_scheduled_start_time(kalshi_event, poly_detail, sport) {
                continue;
            }
            let pairs = pair_totals(kalshi_detail, poly_detail, sport, *spec);
            if !pairs.is_empty() {
                matched_event_ids.insert(poly_event.id.clone());
            }
            out.extend(pairs);
        }
    }
    let total_pairs = out.len() - before_totals;
    let (map_props, map_markets) = discover_esports_maps(
        client,
        sport,
        &matcher,
        &poly_details,
        &mut matched_event_ids,
        &mut catalog,
    )
    .await?;
    kalshi_counts.props += map_markets;
    let mut prop_pairs = map_props.len();
    out.extend(map_props);
    let (props, kalshi_props) = discover_props(
        client,
        sport,
        &matcher,
        &poly_details,
        &mut matched_event_ids,
        &mut catalog,
    )
    .await?;
    kalshi_counts.props += kalshi_props;
    prop_pairs += props.len();
    out.extend(props);
    let (tennis_props, kalshi_tennis_props) = discover_tennis_props(
        client,
        sport,
        &matcher,
        &poly_details,
        &mut matched_event_ids,
        &mut catalog,
    )
    .await?;
    kalshi_counts.props += kalshi_tennis_props;
    prop_pairs += tennis_props.len();
    out.extend(tennis_props);
    let kalshi_polymarket_pairs = out.len();
    attach_polymarket_urls(&mut out, &poly_details);
    prepare_pair_safety(client, sport, &mut out, &catalog, &poly_details).await?;
    let batch_count = subscription_batches(&out).len();
    let matched_games = matched_event_ids
        .iter()
        .filter_map(|id| {
            let event = polymarket_events_for_derivatives
                .iter()
                .find(|event| &event.id == id)?;
            let date = polymarket_event_date(id)?;
            Some(matchup_key(event, date, sport).unwrap_or_else(|| format!("polymarket:{id}")))
        })
        .collect::<HashSet<_>>();
    let matched_counts = MarketCounts {
        games: matched_games.len(),
        moneylines: moneyline_pairs,
        spreads: spread_pairs,
        totals: total_pairs,
        props: prop_pairs,
    };
    let summary = format!(
        "\n{} discovery\n\
         {:13}{:>7}{:>13}{:>10}{:>9}{:>8}\n\
         {}\n{}\n{}\n\n\
         Market pairs: {}\n\
         Subscription batches: {batch_count}",
        sport.label,
        "",
        "Games",
        "Moneylines",
        "Spreads",
        "Totals",
        "Props",
        discovery_row("Kalshi", &kalshi_counts),
        discovery_row("Polymarket", &polymarket_counts),
        discovery_row("Matched", &matched_counts),
        kalshi_polymarket_pairs,
    );
    if io::stdout().is_terminal() && env::var_os("NO_COLOR").is_none() {
        println!("\x1b[2m{summary}\x1b[0m\n");
    } else {
        println!("{summary}\n");
    }
    Ok(Discovery { pairs: out })
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

#[cfg(test)]
fn kalshi_taker_fee(price: f64, contracts: u64) -> f64 {
    let m = default_kalshi_fee();
    m.rounded(price * contracts as f64, m.exact(price, contracts))
}
#[cfg(test)]
fn polymarket_us_taker_fee(price: f64, contracts: u64) -> f64 {
    let m = default_poly_fee();
    m.rounded(price * contracts as f64, m.exact(price, contracts))
}
#[cfg(test)]
fn default_kalshi_fee() -> FeeModel {
    FeeModel::KalshiQuadratic {
        coefficient: 0.07,
        balance_precision: 0.01,
    }
}
#[cfg(test)]
fn default_poly_fee() -> FeeModel {
    FeeModel::PolymarketStandard { coefficient: 0.095 }
}

#[cfg(test)]
fn kalshi_yes_fill(book: &KalshiBook, target: u64) -> Fill {
    let mut fill = Fill {
        contracts: 0,
        cost: 0.0,
        fee: 0.0,
        levels: 0,
    };
    let model = default_kalshi_fee();
    let mut exact_fee = 0.0;
    for (&no_bid, &available) in book.no.iter().rev() {
        let quantity = (available.floor() as u64).min(target - fill.contracts);
        if quantity == 0 {
            continue;
        }
        let price = 1.0 - dollar(no_bid);
        fill.contracts += quantity;
        fill.cost += price * quantity as f64;
        exact_fee += model.exact(price, quantity);
        fill.fee = model.rounded(fill.cost, exact_fee);
        fill.levels += 1;
        if fill.contracts == target {
            break;
        }
    }
    fill
}

#[cfg(test)]
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
    let model = default_poly_fee();
    let mut exact_fee = 0.0;
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
        exact_fee += model.exact(price, quantity);
        fill.fee = model.rounded(fill.cost, exact_fee);
        fill.levels += 1;
        if fill.contracts == target {
            break;
        }
    }
    fill
}

#[cfg(test)]
fn polymarket_fill(book: &PolyBook, buy_long: bool, target: u64) -> Fill {
    if buy_long {
        polymarket_fill_levels(book.offers.iter(), true, target)
    } else {
        polymarket_fill_levels(book.bids.iter().rev(), false, target)
    }
}

// Price levels in execution order, with whole contracts available at each price.
type AskLevels = Vec<(f64, u64)>;

fn executable_levels(k: &KalshiBook, p: &PolyBook, buy_long: bool) -> (AskLevels, AskLevels) {
    let kalshi =
        k.no.iter()
            .rev()
            .map(|(&price, &qty)| (1.0 - dollar(price), qty.floor() as u64))
            .filter(|&(_, qty)| qty > 0)
            .collect();
    let poly: AskLevels = if buy_long {
        p.offers
            .iter()
            .map(|(&price, &qty)| (dollar(price), qty.floor() as u64))
            .collect()
    } else {
        p.bids
            .iter()
            .rev()
            .map(|(&price, &qty)| (1.0 - dollar(price), qty.floor() as u64))
            .collect()
    };
    (
        kalshi,
        poly.into_iter().filter(|&(_, qty)| qty > 0).collect(),
    )
}

fn visible_contracts(levels: &[(f64, u64)]) -> u64 {
    levels
        .iter()
        .fold(0u64, |total, &(_, qty)| total.saturating_add(qty))
}

struct FillCursor<'a> {
    levels: &'a [(f64, u64)],
    index: usize,
    taken: u64,
    completed: Fill,
    fee: FeeModel,
    completed_exact_fee: f64,
}

impl<'a> FillCursor<'a> {
    fn new(levels: &'a [(f64, u64)], fee: FeeModel) -> Self {
        Self {
            levels,
            index: 0,
            taken: 0,
            completed: Fill {
                contracts: 0,
                cost: 0.0,
                fee: 0.0,
                levels: 0,
            },
            fee,
            completed_exact_fee: 0.0,
        }
    }

    fn next_price(&self) -> Option<f64> {
        self.levels.get(self.index).map(|&(price, _)| price)
    }

    fn advance(&mut self) -> Fill {
        let (price, available) = self.levels[self.index];
        self.taken += 1;
        let fill = Fill {
            contracts: self.completed.contracts + self.taken,
            cost: self.completed.cost + price * self.taken as f64,
            // Polymarket caps taker rounding across the whole order; Kalshi's
            // accumulator also carries across fills. Never reset at a level.
            fee: self.fee.rounded(
                self.completed.cost + price * self.taken as f64,
                self.completed_exact_fee + self.fee.exact(price, self.taken),
            ),
            levels: self.completed.levels + 1,
        };
        if self.taken == available {
            self.completed_exact_fee += self.fee.exact(price, self.taken);
            self.completed = fill.clone();
            self.taken = 0;
            self.index += 1;
        }
        fill
    }
}

#[cfg(test)]
fn optimal_matched_fills(kalshi: &[(f64, u64)], poly: &[(f64, u64)]) -> Option<(Fill, Fill)> {
    optimal_matched_fills_with_fees(kalshi, poly, default_kalshi_fee(), default_poly_fee())
}

fn optimal_matched_fills_with_fees(
    kalshi: &[(f64, u64)],
    poly: &[(f64, u64)],
    kalshi_fee: FeeModel,
    poly_fee: FeeModel,
) -> Option<(Fill, Fill)> {
    let mut k = FillCursor::new(kalshi, kalshi_fee);
    let mut p = FillCursor::new(poly, poly_fee);
    let mut best = None;
    let mut best_net = 0.0;
    while k.next_price().is_some() && p.next_price().is_some() {
        // Fee accumulators can return rounding overpayments. Keep checking
        // past a zero gross marginal edge until even an optimistic rounding
        // bound cannot beat the best size.
        let k_fill = k.advance();
        let p_fill = p.advance();
        let net = k_fill.contracts as f64 - k_fill.cost - p_fill.cost - k_fill.fee - p_fill.fee;
        // Keep the smaller size when profits tie. Do not stop at the first
        // fee-induced loss: rounding can make later sizes more profitable.
        if net > best_net + 1e-9 {
            best_net = net;
            best = Some((k_fill.clone(), p_fill.clone()));
        }
        let exact_fee = |cursor: &FillCursor<'_>| {
            cursor.completed_exact_fee
                + if cursor.taken > 0 {
                    cursor
                        .fee
                        .exact(cursor.levels[cursor.index].0, cursor.taken)
                } else {
                    0.0
                }
        };
        let optimistic =
            k_fill.contracts as f64 - k_fill.cost - p_fill.cost - exact_fee(&k) - exact_fee(&p)
                + 0.005;
        if k.next_price()
            .zip(p.next_price())
            .is_some_and(|(kp, pp)| kp + pp >= 1.0)
            && optimistic <= best_net + 1e-9
        {
            break;
        }
    }
    best
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
            pending.retain(|(slug, _), _| *slug != pair.poly_slug);
            continue;
        };
        for i in 0..2 {
            let key = (pair.poly_slug.as_str(), pair.kalshi[i].as_str());
            let Some(safety) = pair.safety[i].as_ref().filter(|s| s.eligible(Utc::now())) else {
                continue;
            };
            let fees = safety.fees.as_ref().expect("eligible fees");
            let Some(k) = kb.get(&pair.kalshi[i]) else {
                pending.remove(&key);
                continue;
            };
            if k.updated.is_none_or(|t| t.elapsed() > MAX_AGE) {
                pending.remove(&key);
                continue;
            };
            if k.no.is_empty() {
                pending.remove(&key);
                continue;
            }
            let opposite_is_long = (1 - i == 0) == pair.poly_long_is_first;
            let (kalshi_levels, polymarket_levels) = executable_levels(k, poly, opposite_is_long);
            let kalshi_available = visible_contracts(&kalshi_levels);
            let polymarket_available = visible_contracts(&polymarket_levels);
            let Some((kalshi_fill, polymarket_fill)) = optimal_matched_fills_with_fees(
                &kalshi_levels,
                &polymarket_levels,
                fees.kalshi,
                fees.polymarket,
            ) else {
                pending.remove(&key);
                continue;
            };
            let contracts = kalshi_fill.contracts;
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
                let permit = match reporter.try_reserve() {
                    Ok(permit) => permit,
                    Err(mpsc::error::TrySendError::Full(_)) => continue,
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        return Err("candidate reporter stopped".into());
                    }
                };
                if !safety.eligible(Utc::now()) {
                    continue;
                }
                emitted.insert(key, observation);
                pending.insert(key, observation);
                permit.send(CandidateRecord {
                    timestamp: clock(),
                    pair: pair.clone(),
                    kalshi_side: i,
                    kalshi_available,
                    polymarket_available,
                    kalshi_fill,
                    polymarket_fill,
                    contracts,
                    gross_profit,
                    net_profit,
                });
            } else {
                pending.remove(&key);
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
        let kalshi_url = kalshi_market_url(&pair.kalshi[record.kalshi_side]);
        let kalshi_average = record.kalshi_fill.cost / record.contracts as f64;
        let polymarket_average = record.polymarket_fill.cost / record.contracts as f64;
        let line = json!({"kind":"confirmed_net_candidate","sport":pair.sport,"event":pair.title,"market":pair.market,"kalshi_url":kalshi_url,"polymarket_url":pair.poly_url,"kalshi_ticker":pair.kalshi[record.kalshi_side],"polymarket_market_slug":pair.poly_slug,"kalshi_outcome":kalshi_outcome,"kalshi_average_price":kalshi_average,"kalshi_fee":record.kalshi_fill.fee,"kalshi_levels":record.kalshi_fill.levels,"kalshi_available_contracts":record.kalshi_available,"kalshi_required_capital":record.kalshi_fill.cost + record.kalshi_fill.fee,"polymarket_outcome":polymarket_outcome,"polymarket_average_price":polymarket_average,"polymarket_fee":record.polymarket_fill.fee,"polymarket_levels":record.polymarket_fill.levels,"polymarket_available_contracts":record.polymarket_available,"polymarket_required_capital":record.polymarket_fill.cost + record.polymarket_fill.fee,"sizing":"max_net_profit_visible_depth","balance_constrained":false,"contracts":record.contracts,"gross_profit":record.gross_profit,"net_profit":record.net_profit,"execution":{"status":"not_executed","mode":"dry_run"},"current_balances":&balances,"fees":pair.safety[record.kalshi_side].as_ref().and_then(|s| s.fees.as_ref()),"settlement_review":pair.safety[record.kalshi_side].as_ref().map(|s| json!({"fingerprint":s.rules.fingerprint,"decision":s.decision})),"note":"dry run only; settlement review is operator supplied, fees are L2 estimates, and fills are not guaranteed"});
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
                    url: &kalshi_url,
                    available: record.kalshi_available,
                    outcome: kalshi_outcome,
                    average_price: kalshi_average,
                    fee: record.kalshi_fill.fee,
                    levels: record.kalshi_fill.levels,
                },
                DisplaySide {
                    url: &pair.poly_url,
                    available: record.polymarket_available,
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
    fn college_live_formats_match_both_leagues_and_full_game_lines() {
        let fixtures: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/college_basketball_markets.json"
        ))
        .unwrap();
        for league in ["cbb", "wcbb"] {
            let fixture = &fixtures[league];
            let sport = selected_sports(league).unwrap()[0];
            let game = &fixture["kalshi"];
            let detail = &fixture["polymarket"];
            let kalshi = Event {
                id: game["event_ticker"].as_str().unwrap().into(),
                title: game["title"].as_str().unwrap().into(),
            };
            let poly = Event {
                id: detail["event"]["slug"].as_str().unwrap().into(),
                title: detail["event"]["title"].as_str().unwrap().into(),
            };
            assert_eq!(event_match_score(&kalshi, &poly, sport), 1.0, "{league}");
            let outcomes = game["markets"]
                .as_array()
                .unwrap()
                .iter()
                .map(|m| {
                    (
                        kalshi_winner(m["title"].as_str().unwrap())
                            .unwrap()
                            .to_owned(),
                        m["ticker"].as_str().unwrap().to_owned(),
                    )
                })
                .collect();
            let moneyline = detail["event"]["markets"]
                .as_array()
                .unwrap()
                .iter()
                .find(|m| polymarket_moneyline(m))
                .unwrap();
            assert!(
                align_moneyline_with(
                    outcomes,
                    [
                        market_team(moneyline, true).unwrap(),
                        market_team(moneyline, false).unwrap()
                    ],
                    same_college_team
                )
                .is_some()
            );
            let next_day = Event {
                id: format!("{league}-a-b-2026-12-31"),
                title: poly.title.clone(),
            };
            assert_eq!(event_match_score(&kalshi, &next_day, sport), 0.0);
            let duplicates = [poly.clone(), poly];
            let mut matcher = EventMatcher::new(&duplicates, sport);
            matcher.block_duplicate_matchups(&[kalshi.clone()]);
            assert!(matcher.best_match(&kalshi).is_none());
            assert!(prop_series(sport).is_empty());
        }
        let fixture = &fixtures["cbb"];
        for league in ["cbb", "wcbb"] {
            let sport = selected_sports(league).unwrap()[0];
            let spreads = pair_spreads(
                &fixture["spread"],
                &fixture["polymarket"],
                sport,
                spread_series(sport)[0],
            );
            assert_eq!(spreads.len(), 1, "{league}");
            let totals = pair_totals(
                &fixture["total"],
                &fixture["polymarket"],
                sport,
                total_series(sport)[0],
            );
            assert_eq!(totals.len(), 1, "{league}");
            let counts = polymarket_counts(
                &HashMap::from([("sample".into(), fixture["polymarket"].clone())]),
                sport,
            )
            .unwrap();
            assert_eq!(
                (
                    counts.moneylines,
                    counts.spreads,
                    counts.totals,
                    counts.props
                ),
                (1, 1, 1, 0)
            );
            let mut wrong_scope = fixture["polymarket"].clone();
            for m in wrong_scope["event"]["markets"].as_array_mut().unwrap() {
                if m["sportsMarketType"] == "basketball_team_full_game_spread" {
                    m["sportsMarketType"] = json!("basketball_team_first_half_spread");
                }
                if m["sportsMarketType"] == "basketball_team_full_game_total" {
                    m["sportsMarketType"] = json!("basketball_team_total_points");
                }
            }
            assert!(
                pair_spreads(
                    &fixture["spread"],
                    &wrong_scope,
                    sport,
                    spread_series(sport)[0]
                )
                .is_empty()
            );
            assert!(
                pair_totals(
                    &fixture["total"],
                    &wrong_scope,
                    sport,
                    total_series(sport)[0]
                )
                .is_empty()
            );
        }
    }

    fn nba_fixture() -> Value {
        serde_json::from_str(include_str!("../../tests/fixtures/nba_markets.json")).unwrap()
    }

    #[test]
    fn nba_live_formats_match_events_moneylines_spreads_and_totals() {
        let fixture = nba_fixture();
        let sport = selected_sports("nba").unwrap()[0];
        let game = &fixture["kalshi_game"];
        let detail = &fixture["polymarket"];
        let kalshi = Event {
            id: game["event_ticker"].as_str().unwrap().into(),
            title: game["title"].as_str().unwrap().into(),
        };
        let poly = Event {
            id: detail["event"]["slug"].as_str().unwrap().into(),
            title: detail["event"]["title"].as_str().unwrap().into(),
        };
        assert_eq!(event_match_score(&kalshi, &poly, sport), 1.0);
        let outcomes = game["markets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| {
                (
                    kalshi_winner(m["title"].as_str().unwrap())
                        .unwrap()
                        .to_owned(),
                    m["ticker"].as_str().unwrap().to_owned(),
                )
            })
            .collect();
        let moneyline = detail["event"]["markets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| polymarket_moneyline(m))
            .unwrap();
        let aligned = align_moneyline_with(
            outcomes,
            [
                market_team(moneyline, true).unwrap(),
                market_team(moneyline, false).unwrap(),
            ],
            same_nba_team,
        )
        .unwrap();
        assert!(aligned.2);
        let spreads = pair_spreads(
            &fixture["kalshi_spread"],
            detail,
            sport,
            NBA_SPREAD_SERIES[0],
        );
        assert_eq!(spreads.len(), 1);
        assert!(!spreads[0].poly_long_is_first);
        assert!(spreads[0].teams[0].starts_with("Cleveland -15.5"));
        let totals = pair_totals(&fixture["kalshi_total"], detail, sport, NBA_TOTAL_SERIES[0]);
        assert_eq!(totals.len(), 1);
        assert_eq!(totals[0].teams, ["Over 213.5", "Under 213.5"]);
        let counts = polymarket_counts(&HashMap::from([(poly.id, detail.clone())]), sport).unwrap();
        assert_eq!(
            (
                counts.moneylines,
                counts.spreads,
                counts.totals,
                counts.props
            ),
            (1, 1, 1, 0)
        );
    }

    #[test]
    fn nba_rejects_wrong_date_teams_and_duplicate_games() {
        let sport = selected_sports("nba").unwrap()[0];
        let kalshi = Event {
            id: "KXNBAGAME-26NOV16LALLAC".into(),
            title: "Los Angeles L vs Los Angeles C".into(),
        };
        let poly = Event {
            id: "nba-lal-lac-2026-11-16".into(),
            title: "LA Clippers vs. LAL Lakers".into(),
        };
        assert_eq!(event_match_score(&kalshi, &poly, sport), 1.0);
        let next_day = Event {
            id: "nba-lal-lac-2026-11-17".into(),
            title: poly.title.clone(),
        };
        assert_eq!(event_match_score(&kalshi, &next_day, sport), 0.0);
        let wrong_team = Event {
            id: poly.id.clone(),
            title: "LA Lakers vs. LA Lakers".into(),
        };
        assert_eq!(event_match_score(&kalshi, &wrong_team, sport), 0.0);
        let events = [poly.clone(), next_day];
        let mut matcher = EventMatcher::new(&events, sport);
        assert_eq!(matcher.best_match(&kalshi).unwrap().id, poly.id);
        matcher.block_duplicate_matchups(&[kalshi.clone(), kalshi.clone()]);
        assert!(matcher.best_match(&kalshi).is_none());
        let duplicates = [poly.clone(), poly];
        let mut matcher = EventMatcher::new(&duplicates, sport);
        matcher.block_duplicate_matchups(&[kalshi.clone()]);
        assert!(matcher.best_match(&kalshi).is_none());
    }

    #[test]
    fn nba_spreads_and_totals_require_exact_scope_line_and_orientation() {
        let sport = selected_sports("nba").unwrap()[0];
        let fixture = nba_fixture();
        let detail = &fixture["polymarket"];
        for (field, value) in [
            (
                "sportsMarketType",
                json!("basketball_team_first_half_spread"),
            ),
            ("line", json!(15.75)),
            ("line", json!(-15.5)),
        ] {
            let mut detail = detail.clone();
            let markets = detail["event"]["markets"].as_array_mut().unwrap();
            markets.retain(|m| m["sportsMarketType"] == "basketball_team_full_game_spread");
            markets[0][field] = value;
            assert!(
                pair_spreads(
                    &fixture["kalshi_spread"],
                    &detail,
                    sport,
                    NBA_SPREAD_SERIES[0]
                )
                .is_empty()
            );
        }
        for (field, value) in [
            ("sportsMarketType", json!("basketball_team_total_points")),
            (
                "sportsMarketType",
                json!("basketball_team_first_half_total"),
            ),
            ("line", json!(214.0)),
        ] {
            let mut detail = detail.clone();
            let markets = detail["event"]["markets"].as_array_mut().unwrap();
            markets.retain(|m| m["sportsMarketType"] == "basketball_team_full_game_total");
            markets[0][field] = value;
            assert!(
                pair_totals(
                    &fixture["kalshi_total"],
                    &detail,
                    sport,
                    NBA_TOTAL_SERIES[0]
                )
                .is_empty()
            );
        }
        let mut integer = fixture["kalshi_spread"].clone();
        for m in integer["markets"].as_array_mut().unwrap() {
            m["floor_strike"] = json!(27);
        }
        assert!(pair_spreads(&integer, detail, sport, NBA_SPREAD_SERIES[0]).is_empty());
    }

    #[test]
    fn market_links_do_not_add_empty_path_segments() {
        assert_eq!(
            kalshi_market_url("KXNFLGAME-26NOV01PHIWAS-PHI"),
            "https://kalshi.com/markets/KXNFLGAME-26NOV01PHIWAS-PHI"
        );
        assert_eq!(
            polymarket_market_url("nfl-phi-was-2026-11-01", "aec-nfl-phi-was-2026-11-01"),
            "https://polymarket.us/event/nfl-phi-was-2026-11-01?marketSlug=aec-nfl-phi-was-2026-11-01"
        );
    }

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
            safety: [None, None],
            sport: "test",
            title: "A vs B".into(),
            market: "Full game moneyline".into(),
            kalshi: ["K-A".into(), "K-B".into()],
            teams: ["A".into(), "B".into()],
            poly_slug: "pm-a-b".into(),
            poly_url: String::new(),
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
            let pairs = pair_totals(
                &kalshi,
                &polymarket,
                selected_sports(sport).unwrap()[0],
                total_series(selected_sports(sport).unwrap()[0])[0],
            );
            assert_eq!(pairs.len(), 1);
            assert_eq!(pairs[0].poly_slug, "pm-total");
            assert_eq!(pairs[0].teams, ["Over 47.5", "Under 47.5"]);
        }
    }

    #[test]
    fn pairs_current_polymarket_nfl_game_total_without_team_totals() {
        let kalshi = json!({"markets":[{"ticker":"K-TOTAL","floor_strike":47.5,"title":"Over 47.5 points?"}]});
        let polymarket = json!({"event":{"title":"IND Colts vs WAS Commanders","markets":[
            {"marketType":"totals","sportsMarketType":"football_team_points_full_game_total","line":47.5,"slug":"tsc-nfl-ind-was-2026-10-04-tt-was-47pt5","marketSides":[
                {"long":true,"description":"Over","team":{"name":"Washington Commanders"}},
                {"long":false,"description":"Under","team":{"name":"Washington Commanders"}}
            ]},
            {"marketType":"totals","sportsMarketType":"football_team_full_game_total","line":47.5,"slug":"tsc-nfl-ind-was-2026-10-04-total-47pt5","marketSides":[
                {"long":true,"description":"Over"}, {"long":false,"description":"Under"}
            ]},
            {"marketType":"totals","sportsMarketType":"football_team_full_game_total","line":47.5,"slug":"tsc-nfl-ind-was-2026-10-04-tt-ind-total-47pt5","marketSides":[
                {"long":true,"description":"Over","team":{"name":"Indianapolis Colts"}},
                {"long":false,"description":"Under","team":{"name":"Indianapolis Colts"}}
            ]}
        ]}});
        let sport = selected_sports("nfl").unwrap()[0];
        let spec = NFL_TOTAL_SERIES[0];
        let pairs = pair_totals(&kalshi, &polymarket, sport, spec);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].poly_slug, "tsc-nfl-ind-was-2026-10-04-total-47pt5");
        let details = HashMap::from([("game".to_owned(), polymarket)]);
        assert_eq!(polymarket_counts(&details, sport).unwrap().totals, 1);
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
            {"slug":"pm-team","sportsMarketType":"football_team_points_full_game_total","line":27.5,"marketSides":[
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
            {"slug":"pm-kc","sportsMarketType":"football_team_points_full_game_total","line":24.5,"marketSides":[
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
    fn mlb_requires_same_date_and_skips_doubleheaders() {
        let kalshi = Event {
            id: "KXMLBGAME-26SEP291400PHIATL".into(),
            title: "Philadelphia vs Atlanta".into(),
        };
        let poly = [
            Event {
                id: "mlb-phi-atl-2026-09-29".into(),
                title: "Philadelphia Phillies vs. Atlanta Braves".into(),
            },
            Event {
                id: "mlb-phi-atl-2026-09-30".into(),
                title: "Philadelphia Phillies vs. Atlanta Braves".into(),
            },
        ];
        let sport = selected_sports("mlb").unwrap()[0];
        assert_eq!(event_match_score(&kalshi, &poly[0], sport), 1.0);
        assert_eq!(event_match_score(&kalshi, &poly[1], sport), 0.0);
        let mut matcher = EventMatcher::new(&poly, sport);
        assert_eq!(matcher.best_match(&kalshi).unwrap().id, poly[0].id);
        let first_game = Event {
            title: "Game 1: Philadelphia vs Atlanta".into(),
            ..kalshi.clone()
        };
        matcher.block_duplicate_matchups(&[first_game]);
        assert_eq!(matcher.best_match(&kalshi).unwrap().id, poly[0].id);
        let second_game = Event {
            id: "KXMLBGAME-26SEP291900PHIATL".into(),
            title: "Game 2: Philadelphia vs Atlanta".into(),
        };
        matcher.block_duplicate_matchups(&[kalshi.clone(), second_game]);
        assert!(matcher.best_match(&kalshi).is_none());
        assert!(matcher.is_blocked(&kalshi));
    }

    #[test]
    fn mlb_requires_the_same_scheduled_start_time() {
        let september = Event {
            id: "KXMLBGAME-26SEP291400PHIATL".into(),
            title: "Philadelphia vs Atlanta".into(),
        };
        let matching = json!({"event":{"startDate":"2026-09-29T18:00:00Z"}});
        let another_game = json!({"event":{"startDate":"2026-09-29T21:00:00Z"}});
        assert!(mlb_start_time_matches(&september, &matching));
        assert!(!mlb_start_time_matches(&september, &another_game));
        let winter = Event {
            id: "KXMLBGAME-26NOV151400PHIATL".into(),
            title: september.title,
        };
        assert!(mlb_start_time_matches(
            &winter,
            &json!({"event":{"startDate":"2026-11-15T19:00:00Z"}})
        ));
    }

    #[test]
    fn mlb_matches_polymarket_city_abbreviations_with_team_names() {
        let sport = selected_sports("mlb").unwrap()[0];
        for (kalshi_title, poly_title) in [
            (
                "Game 3: Philadelphia vs Atlanta",
                "Game 3: PHI Phillies vs. ATL Braves",
            ),
            (
                "Game 1: Chicago WS vs Cleveland",
                "Game 1: CHI White Sox vs. CLE Guardians",
            ),
            (
                "Game 1: New York Y vs Tampa Bay",
                "Game 1: NY Yankees vs. TB Rays",
            ),
            (
                "Game 1: San Diego vs Milwaukee",
                "Game 1: SD Padres vs. MIL Brewers",
            ),
        ] {
            let kalshi = Event {
                id: "KXMLBGAME-26OCT031300CWSCLE".into(),
                title: kalshi_title.into(),
            };
            let poly = Event {
                id: "mlb-cws-cle-2026-10-03".into(),
                title: poly_title.into(),
            };
            assert_eq!(
                event_match_score(&kalshi, &poly, sport),
                1.0,
                "{kalshi_title} / {poly_title}"
            );
        }
    }

    #[test]
    fn mlb_uses_full_team_name_before_short_safe_name() {
        let market = json!({"marketSides":[
            {"long":true,"team":{"name":"New York Yankees","safeName":"Yankees"}},
            {"long":false,"team":{"name":"Boston Red Sox","safeName":"Red Sox"}}
        ]});
        assert_eq!(
            market_team(&market, true).as_deref(),
            Some("New York Yankees")
        );
        let kalshi = vec![
            ("New York Y".into(), "K-NYY".into()),
            ("Boston".into(), "K-BOS".into()),
        ];
        assert!(
            align_moneyline(
                kalshi,
                [
                    market_team(&market, true).unwrap(),
                    market_team(&market, false).unwrap()
                ]
            )
            .is_some()
        );
    }

    #[test]
    fn mlb_spreads_require_same_period_and_half_point_line() {
        let kalshi = json!({"markets":[
            {"ticker":"K-F5","floor_strike":1.5,"title":"Boston wins first 5 innings by over 1.5 runs?","yes_sub_title":"Boston -1.5 first 5 innings"},
            {"ticker":"K-INTEGER","floor_strike":2.0,"title":"Boston wins first 5 innings by over 2 runs?","yes_sub_title":"Boston -2 first 5 innings"}
        ]});
        let poly = json!({"event":{"title":"Boston Red Sox vs. New York Yankees","markets":[
            {"slug":"full","marketType":"spreads","sportsMarketType":"baseball_team_full_game_spread","line":-1.5,"marketSides":[
                {"long":true,"team":{"name":"Boston Red Sox"}},{"long":false,"team":{"name":"New York Yankees"}}]},
            {"slug":"f5","marketType":"spreads","sportsMarketType":"baseball_team_first_five_spread","line":-1.5,"marketSides":[
                {"long":true,"team":{"name":"Boston Red Sox"}},{"long":false,"team":{"name":"New York Yankees"}}]}
        ]}});
        let pairs = pair_spreads(
            &kalshi,
            &poly,
            selected_sports("mlb").unwrap()[0],
            MLB_SPREAD_SERIES[1],
        );
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].poly_slug, "f5");
        assert_eq!(pairs[0].kalshi[0], "K-F5");
    }

    #[test]
    fn mlb_totals_require_same_period_and_line() {
        let kalshi = json!({"markets":[
            {"ticker":"K-F5","floor_strike":3.5,"title":"First 5 innings: Over 3.5 runs","yes_sub_title":"Over 3.5 runs in the first 5 innings"}
        ]});
        let poly = json!({"event":{"title":"Boston vs New York","markets":[
            {"slug":"full","marketType":"totals","sportsMarketType":"baseball_team_full_game_total","line":3.5,"marketSides":[
                {"long":true,"description":"Over"},{"long":false,"description":"Under"}]},
            {"slug":"wrong-line","marketType":"totals","sportsMarketType":"baseball_team_first_five_total","line":4.5,"marketSides":[
                {"long":true,"description":"Over"},{"long":false,"description":"Under"}]},
            {"slug":"f5","marketType":"totals","sportsMarketType":"baseball_team_first_five_total","line":3.5,"marketSides":[
                {"long":true,"description":"Over"},{"long":false,"description":"Under"}]}
        ]}});
        let pairs = pair_totals(
            &kalshi,
            &poly,
            selected_sports("mlb").unwrap()[0],
            MLB_TOTAL_SERIES[1],
        );
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].poly_slug, "f5");
    }

    #[test]
    fn mlb_player_and_team_props_require_exact_entity_stat_and_threshold() {
        let sport = selected_sports("mlb").unwrap()[0];
        let hits = json!({"markets":[
            {"ticker":"K-HIT","title":"Bryce Harper: 2+ hits?","floor_strike":1.5}
        ]});
        let poly = json!({"event":{"title":"Philadelphia Phillies vs. Atlanta Braves","markets":[
            {"slug":"wrong-stat","sportsMarketType":"baseball_player_home_runs","line":2,"question":"Will Bryce Harper record at least 2 home runs in PHI vs ATL?","marketSides":[{"long":true,"description":"Yes"},{"long":false,"description":"No"}]},
            {"slug":"wrong-player","sportsMarketType":"baseball_player_hits","line":2,"question":"Will Trea Turner record at least 2 hits in PHI vs ATL?","marketSides":[{"long":true,"description":"Yes"},{"long":false,"description":"No"}]},
            {"slug":"wrong-line","sportsMarketType":"baseball_player_hits","line":3,"question":"Will Bryce Harper record at least 3 hits in PHI vs ATL?","marketSides":[{"long":true,"description":"Yes"},{"long":false,"description":"No"}]},
            {"slug":"hits","sportsMarketType":"baseball_player_hits","line":2,"question":"Will Bryce Harper record at least 2 hits in PHI vs ATL?","marketSides":[{"long":true,"description":"Yes"},{"long":false,"description":"No"}]},
            {"slug":"team","sportsMarketType":"baseball_team_total_runs","line":2.5,"question":"Will Atlanta Braves score more than 2.5 runs in PHI vs ATL?","marketSides":[{"long":true,"description":"Yes","team":{"name":"Atlanta Braves","safeName":"Braves"}},{"long":false,"description":"No","team":{"name":"Atlanta Braves","safeName":"Braves"}}]}
        ]}});
        let player_pairs = pair_props(&hits, &poly, sport, MLB_PROP_SERIES[0]);
        assert_eq!(player_pairs.len(), 1);
        assert_eq!(player_pairs[0].poly_slug, "hits");
        let team = json!({"markets":[
            {"ticker":"K-TEAM","title":"Will Atlanta score over 2.5 runs?","yes_sub_title":"Atlanta over 2.5 runs scored","floor_strike":2.5}
        ]});
        let team_pairs = pair_props(&team, &poly, sport, MLB_PROP_SERIES[5]);
        assert_eq!(team_pairs.len(), 1);
        assert_eq!(team_pairs[0].poly_slug, "team");
    }

    #[test]
    fn esports_matches_exact_teams_on_the_same_date_and_nearby_start_time() {
        let sport = selected_sports("cs2").unwrap()[0];
        let kalshi = Event {
            id: "KXCS2GAME-26SEP280630MELNXS".into(),
            title: "mellren vs. Nexus".into(),
        };
        let poly = Event {
            id: "cs2-nxs-mel-2026-09-28".into(),
            title: "Nexus vs. mellren".into(),
        };
        assert_eq!(event_match_score(&kalshi, &poly, sport), 1.0);
        assert!(same_scheduled_start_time(
            &kalshi,
            &json!({"event":{"startDate":"2026-09-28T11:15:00Z"}}),
            sport,
        ));
        assert!(!same_scheduled_start_time(
            &kalshi,
            &json!({"event":{"startDate":"2026-09-28T14:00:00Z"}}),
            sport,
        ));
        assert_eq!(
            event_match_score(
                &kalshi,
                &Event {
                    id: "cs2-nxs-mel-2026-09-30".into(),
                    ..poly.clone()
                },
                sport
            ),
            0.0
        );
        assert!(!same_esports_team("Team Spirit", "Team Spirit Academy"));
        assert!(!same_esports_team("123", "ЯЧЁ123"));
        assert!(same_esports_team("Berlin International Gaming", "BIG"));
        assert!(same_esports_team("Yakult's Brothers", "Yakult Brothers"));
        assert!(same_esports_team("Barça eSports", "Barca eSports"));
        assert!(same_esports_team("Senshi Esports Club", "Senshi Esports"));
    }

    #[test]
    fn esports_matches_next_utc_day_by_nearest_scheduled_start() {
        let sport = selected_sports("dota2").unwrap()[0];
        let kalshi = Event {
            id: "KXDOTA2GAME-26SEP282300XEKIN".into(),
            title: "Xipto Esports vs. Team Kinetix".into(),
        };
        let poly = [
            Event {
                id: "dota2-tke-xe-2026-09-29".into(),
                title: "Team Kinetix vs. Xipto Esports".into(),
            },
            Event {
                id: "dota2-tke-xe-repeat-2026-09-29".into(),
                title: "Team Kinetix vs. Xipto Esports".into(),
            },
        ];
        let details = HashMap::from([
            (
                poly[0].id.clone(),
                json!({"event":{"startDate":"2026-09-29T03:00:00Z"}}),
            ),
            (
                poly[1].id.clone(),
                json!({"event":{"startDate":"2026-09-29T03:45:00Z"}}),
            ),
        ]);
        let mut matcher = EventMatcher::new(&poly, sport);
        matcher.set_poly_start_times(&details);
        assert_eq!(event_match_score(&kalshi, &poly[0], sport), 1.0);
        assert_eq!(matcher.best_match(&kalshi).unwrap().id, poly[0].id);
        matcher.block_duplicate_matchups(&[kalshi.clone()]);
        assert!(matcher.best_match(&kalshi).is_none());
    }

    #[test]
    fn cs2_map_prop_pairs_only_the_same_map_and_opposite_team() {
        let kalshi = json!({"title":"mellren vs. Nexus: Map 2","markets":[
            {"ticker":"K-MEL","title":"mellren wins map 2"},
            {"ticker":"K-NXS","title":"Nexus wins map 2"}
        ]});
        let polymarket = json!({"event":{"title":"Nexus vs. mellren","markets":[
            {"slug":"map1","marketType":"props","sportsMarketType":"esports_map_winner_1","question":"Will Nexus win Map 1 vs mellren?","marketSides":[{"long":true,"description":"Yes","team":{"name":"Nexus"}},{"long":false,"description":"No","team":{"name":"Nexus"}}]},
            {"slug":"wrong-opponent","marketType":"props","sportsMarketType":"esports_map_winner_2","question":"Will Nexus win Map 2 vs mellren Academy?","marketSides":[{"long":true,"description":"Yes","team":{"name":"Nexus"}},{"long":false,"description":"No","team":{"name":"Nexus"}}]},
            {"slug":"map2","marketType":"props","sportsMarketType":"esports_map_winner_2","question":"Will Nexus win Map 2 vs mellren?","marketSides":[{"long":true,"description":"Yes","team":{"name":"Nexus"}},{"long":false,"description":"No","team":{"name":"Nexus"}}]}
        ]}});
        let pairs =
            pair_esports_map_winners(&kalshi, &polymarket, selected_sports("cs2").unwrap()[0]);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].market, "Map 2 winner");
        assert_eq!(pairs[0].poly_slug, "map2");
        assert_eq!(pairs[0].teams, ["mellren", "Nexus"]);
        assert!(!pairs[0].poly_long_is_first);
    }

    #[test]
    fn dota2_game_winner_and_series_total_use_the_exact_market_types() {
        let sport = selected_sports("dota2").unwrap()[0];
        let kalshi_map = json!({"title":"Team Kinetix vs. Xipto Esports: Map 2","markets":[
            {"ticker":"K-TKE","title":"Team Kinetix wins map 2"},
            {"ticker":"K-XE","title":"Xipto Esports wins map 2"}
        ]});
        let polymarket = json!({"event":{"title":"Team Kinetix vs. Xipto Esports","markets":[
            {"slug":"game2","marketType":"props","sportsMarketType":"esports_game_winner_2","question":"Will Team Kinetix win Game 2 vs Xipto Esports?","marketSides":[{"long":true,"description":"Yes","team":{"name":"Team Kinetix"}},{"long":false,"description":"No","team":{"name":"Team Kinetix"}}]},
            {"slug":"rounds","marketType":"totals","sportsMarketType":"esports_map_total_rounds_2","line":2.5,"marketSides":[{"long":true,"description":"Over"},{"long":false,"description":"Under"}]},
            {"slug":"games","marketType":"totals","sportsMarketType":"esports_series_total_games","line":2.5,"marketSides":[{"long":true,"description":"Over"},{"long":false,"description":"Under"}]}
        ]}});
        let map_pairs = pair_esports_map_winners(&kalshi_map, &polymarket, sport);
        assert_eq!(map_pairs.len(), 1);
        assert_eq!(map_pairs[0].poly_slug, "game2");
        let kalshi_total = json!({"markets":[
            {"ticker":"K-TOTAL","title":"Will over 2.5 maps be played?","yes_sub_title":"Over 2.5 maps","floor_strike":2.5}
        ]});
        let total_pairs = pair_totals(&kalshi_total, &polymarket, sport, DOTA2_TOTAL_SERIES[0]);
        assert_eq!(total_pairs.len(), 1);
        assert_eq!(total_pairs[0].poly_slug, "games");
        assert!(total_series(selected_sports("r6").unwrap()[0]).is_empty());
    }

    #[test]
    fn lol_map_winner_and_series_total_match_polymarket_game_types() {
        let sport = selected_sports("lol").unwrap()[0];
        let kalshi_map = json!({"title":"Golden Lions vs. KaBuM! Ilha das Lendas: Map 1","markets":[
            {"ticker":"K-GL","title":"Golden Lions wins map 1"},
            {"ticker":"K-KBM","title":"KaBuM! Ilha das Lendas wins map 1"}
        ]});
        let polymarket = json!({"event":{"title":"KaBuM! Ilha das Lendas vs. Golden Lions","markets":[
            {"slug":"game1","marketType":"props","sportsMarketType":"esports_game_winner_1","question":"Will KaBuM! Ilha das Lendas win Game 1 vs Golden Lions?","marketSides":[{"long":true,"description":"Yes","team":{"name":"KaBuM! Ilha das Lendas"}},{"long":false,"description":"No","team":{"name":"KaBuM! Ilha das Lendas"}}]},
            {"slug":"total","marketType":"totals","sportsMarketType":"esports_series_total_games","line":3.5,"marketSides":[{"long":true,"description":"Over"},{"long":false,"description":"Under"}]}
        ]}});
        let map_pairs = pair_esports_map_winners(&kalshi_map, &polymarket, sport);
        assert_eq!(map_pairs.len(), 1);
        assert_eq!(map_pairs[0].poly_slug, "game1");
        assert_eq!(map_pairs[0].market, "Game 1 winner");
        let kalshi_total = json!({"markets":[
            {"ticker":"K-TOTAL","title":"Will over 3.5 maps be played?","yes_sub_title":"Over 3.5 maps","floor_strike":3.5}
        ]});
        let total_pairs = pair_totals(&kalshi_total, &polymarket, sport, LOL_TOTAL_SERIES[0]);
        assert_eq!(total_pairs.len(), 1);
        assert_eq!(total_pairs[0].poly_slug, "total");
    }

    #[test]
    fn tennis_pairs_only_matching_game_spread_and_total_lines() {
        let sport = selected_sports("tennis").unwrap()[0];
        let kalshi_spread = json!({"markets":[
            {"ticker":"K-SPREAD","yes_sub_title":"Hubert Hurkacz -2.5 games","floor_strike":2.5}
        ]});
        let kalshi_total = json!({"markets":[
            {"ticker":"K-TOTAL","title":"Over 23.5 games","floor_strike":23.5}
        ]});
        let polymarket = json!({"event":{"title":"Denis Shapovalov vs. Hubert Hurkacz","markets":[
            {"slug":"sets-spread","marketType":"spreads","sportsMarketType":"tennis_match_sets_spread","line":2.5,"marketSides":[{"long":true,"description":"+2.50","team":{"name":"Denis Shapovalov"}},{"long":false,"description":"-2.50","team":{"name":"Hubert Hurkacz"}}]},
            {"slug":"games-spread","marketType":"spreads","sportsMarketType":"tennis_match_games_spread","line":2.5,"marketSides":[{"long":true,"description":"+2.50","team":{"name":"Denis Shapovalov"}},{"long":false,"description":"-2.50","team":{"name":"Hubert Hurkacz"}}]},
            {"slug":"sets-total","marketType":"totals","sportsMarketType":"tennis_match_total_sets","line":23.5,"marketSides":[{"long":true,"description":"Over"},{"long":false,"description":"Under"}]},
            {"slug":"games-total","marketType":"totals","sportsMarketType":"tennis_match_total_games","line":23.5,"marketSides":[{"long":true,"description":"Over"},{"long":false,"description":"Under"}]}
        ]}});
        let spread_pairs = pair_spreads(&kalshi_spread, &polymarket, sport, ATP_SPREAD_SERIES[0]);
        assert_eq!(spread_pairs.len(), 1);
        assert_eq!(spread_pairs[0].poly_slug, "games-spread");
        assert!(!spread_pairs[0].poly_long_is_first);
        let total_pairs = pair_totals(&kalshi_total, &polymarket, sport, ATP_TOTAL_SERIES[0]);
        assert_eq!(total_pairs.len(), 1);
        assert_eq!(total_pairs[0].poly_slug, "games-total");
    }

    #[test]
    fn tennis_set_winner_and_exact_score_require_matching_player_and_score() {
        let sport = selected_sports("tennis").unwrap()[0];
        let kalshi_set = json!({"title":"Hubert Hurkacz vs Denis Shapovalov: Set 1 Winner","markets":[
            {"ticker":"K-HUR","yes_sub_title":"Hubert Hurkacz"},
            {"ticker":"K-SHA","yes_sub_title":"Denis Shapovalov"}
        ]});
        let kalshi_score = json!({"markets":[
            {"ticker":"K-SCORE","yes_sub_title":"Hubert Hurkacz wins 2-1","custom_strike":{"Set Score":"2-1"}}
        ]});
        let polymarket = json!({"event":{"title":"Denis Shapovalov vs. Hubert Hurkacz","markets":[
            {"slug":"set1","marketType":"props","sportsMarketType":"tennis_set_1_winner","question":"Will Denis Shapovalov win set 1 against Hubert Hurkacz?","marketSides":[{"long":true,"description":"Yes","team":{"name":"Denis Shapovalov"}},{"long":false,"description":"No","team":{"name":"Denis Shapovalov"}}]},
            {"slug":"set2","marketType":"props","sportsMarketType":"tennis_set_2_winner","question":"Will Denis Shapovalov win set 2 against Hubert Hurkacz?","marketSides":[{"long":true,"description":"Yes","team":{"name":"Denis Shapovalov"}},{"long":false,"description":"No","team":{"name":"Denis Shapovalov"}}]},
            {"slug":"wrong-score","marketType":"props","sportsMarketType":"tennis_match_exact_score","question":"Hurkacz wins 2-0","marketSides":[{"long":true,"description":"Yes"},{"long":false,"description":"No"}]},
            {"slug":"score","marketType":"props","sportsMarketType":"tennis_match_exact_score","question":"Hurkacz wins 2-1","marketSides":[{"long":true,"description":"Yes"},{"long":false,"description":"No"}]}
        ]}});
        let set_pairs = pair_tennis_set_winners(&kalshi_set, &polymarket, sport);
        assert_eq!(set_pairs.len(), 1);
        assert_eq!(set_pairs[0].poly_slug, "set1");
        assert!(!set_pairs[0].poly_long_is_first);
        let score_pairs = pair_tennis_exact_scores(&kalshi_score, &polymarket, sport);
        assert_eq!(score_pairs.len(), 1);
        assert_eq!(score_pairs[0].poly_slug, "score");
    }

    #[test]
    fn tennis_event_match_requires_same_scheduled_date() {
        let sport = selected_sports("tennis").unwrap()[0];
        let kalshi = Event {
            id: "KXATPMATCH-26SEP28HURSHA".into(),
            title: "Hurkacz vs Shapovalov".into(),
        };
        let same_day = Event {
            id: "atp-den-sha-hub-hur-2026-09-28".into(),
            title: "Denis Shapovalov vs. Hubert Hurkacz".into(),
        };
        let next_day = Event {
            id: "atp-den-sha-hub-hur-2026-09-29".into(),
            ..same_day.clone()
        };
        assert_eq!(event_match_score(&kalshi, &same_day, sport), 1.0);
        assert_eq!(event_match_score(&kalshi, &next_day, sport), 0.0);
    }

    #[test]
    fn tennis_game_discovery_uses_full_contract_names_for_short_surnames() {
        let raw = json!({
            "title":"Dart vs Ma",
            "markets":[
                {"title":"Harriet Dart wins"},
                {"title":"Yexin Ma wins"}
            ]
        });
        let title = kalshi_tennis_match_title(&raw).unwrap();
        assert_eq!(title, "Harriet Dart vs Yexin Ma");
        let kalshi = Event {
            id: "KXWTAMATCH-26SEP28DARMA".into(),
            title,
        };
        let polymarket = Event {
            id: "wta-hardar-yexma-2026-09-28".into(),
            title: "Harriet Dart vs. Yexin Ma".into(),
        };
        assert_eq!(
            event_match_score(&kalshi, &polymarket, selected_sports("wta").unwrap()[0]),
            1.0
        );
    }

    #[test]
    fn tennis_name_aliases_match_only_verified_player_variants() {
        assert!(same_tennis_player(
            "Adolfo Daniel Vallejo",
            "Adolfo Vallejo"
        ));
        assert!(same_tennis_player(
            "Maiar Sherif Ahmed Abdelaziz",
            "Mayar Sherif"
        ));
        assert!(same_tennis_player(
            "Yuliia Starodubtseva",
            "Yulia Starodubtseva"
        ));
        assert!(!same_tennis_player("Yexin Ma", "Yexin Wang"));
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
            safety: [None, None],
            sport: "test",
            title: "A vs B".into(),
            market: "Full game moneyline".into(),
            kalshi: ["K-A".into(), "K-B".into()],
            teams: ["A".into(), "B".into()],
            poly_slug: "pm-a-b".into(),
            poly_url: String::new(),
            poly_long_is_first: true,
        };
        let pairs = (0..51)
            .map(|index| Pair {
                kalshi: [format!("K-{index}-A"), format!("K-{index}-B")],
                poly_slug: format!("pm-{index}"),
                poly_url: String::new(),
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
            safety: [None, None],
            sport: "test",
            title: "A vs B".into(),
            market: "Full game moneyline".into(),
            kalshi: ["K-A".into(), "K-B".into()],
            teams: ["A".into(), "B".into()],
            poly_slug: "pm-a-b".into(),
            poly_url: String::new(),
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

    #[tokio::test]
    async fn candidate_uses_the_needed_polymarket_side_without_waiting_for_both() {
        let pair = Pair {
            safety: [Some(safety::approved_test_safety()), None],
            sport: "test",
            title: "A vs B".into(),
            market: "Full game moneyline".into(),
            kalshi: ["K-A".into(), String::new()],
            teams: ["A".into(), "B".into()],
            poly_slug: "pm".into(),
            poly_url: String::new(),
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
        let pairs = [pair];
        candidate(
            &pairs,
            &[0],
            &kalshi,
            &polymarket,
            &mut pending,
            &mut emitted,
            &sender,
        )
        .await
        .unwrap();
        // Keep the reporting queue full. A second candidate must return
        // immediately so the receive loop can still process sequence gaps.
        for observation in pending.values_mut() {
            observation.kalshi_generation = 0;
            observation.polymarket_generation = 0;
        }
        emitted.clear();
        tokio::time::timeout(
            Duration::from_millis(20),
            candidate(
                &pairs,
                &[0],
                &kalshi,
                &polymarket,
                &mut pending,
                &mut emitted,
                &sender,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        let record = receiver.try_recv().unwrap();
        assert_eq!(record.contracts, 25);
        assert!(record.net_profit >= MIN_NET_PROFIT_DOLLARS);
    }

    #[tokio::test]
    async fn candidate_gate_blocks_missing_unapproved_and_expired_metadata() {
        let kalshi = HashMap::from([(
            "K-A".into(),
            KalshiBook {
                no: BTreeMap::from([(6000, 25.0)]),
                updated: Some(Instant::now()),
                generation: 1,
            },
        )]);
        let poly = HashMap::from([(
            "pm".into(),
            PolyBook {
                offers: BTreeMap::from([(5000, 25.0)]),
                updated: Some(Instant::now()),
                generation: 1,
                ..Default::default()
            },
        )]);
        for case in 0..5 {
            let mut metadata = safety::approved_test_safety();
            let data = Arc::make_mut(&mut metadata);
            match case {
                0 => data.decision.approved = false,
                1 => data.decision.expires_at = Some(Utc::now() - chrono::Duration::seconds(1)),
                2 => data.fees = None,
                3 => {
                    data.fees.as_mut().unwrap().expires_at =
                        Utc::now() - chrono::Duration::seconds(1)
                }
                _ => {}
            }
            let pair = Pair {
                safety: if case == 4 {
                    [None, None]
                } else {
                    [Some(metadata), None]
                },
                sport: "test",
                title: "A vs B".into(),
                market: "Full game moneyline".into(),
                kalshi: ["K-A".into(), String::new()],
                teams: ["A".into(), "B".into()],
                poly_slug: "pm".into(),
                poly_url: String::new(),
                poly_long_is_first: false,
            };
            let (sender, mut receiver) = mpsc::channel(1);
            let mut pending = HashMap::from([(
                ("pm", "K-A"),
                Observation {
                    kalshi_generation: 0,
                    polymarket_generation: 0,
                },
            )]);
            candidate(
                &[pair],
                &[0],
                &kalshi,
                &poly,
                &mut pending,
                &mut HashMap::new(),
                &sender,
            )
            .await
            .unwrap();
            assert!(receiver.try_recv().is_err(), "case {case}");
        }
    }

    #[test]
    fn account_fee_tier_can_remove_an_apparent_edge() {
        let k = [(0.47, 100)];
        let p = [(0.49, 100)];
        assert!(
            optimal_matched_fills_with_fees(
                &k,
                &p,
                default_kalshi_fee(),
                FeeModel::PolymarketStandard {
                    coefficient: 0.0695
                }
            )
            .is_some()
        );
        assert!(
            optimal_matched_fills_with_fees(
                &k,
                &p,
                default_kalshi_fee(),
                FeeModel::PolymarketStandard { coefficient: 0.095 }
            )
            .is_none()
        );
        let mut cursor = FillCursor::new(&[(0.49, 1), (0.50, 1)], default_poly_fee());
        assert_eq!(cursor.advance().fee, 0.02);
        assert_eq!(cursor.advance().fee, 0.05);
    }

    #[test]
    fn optimal_size_exceeds_old_cap_and_respects_thinner_book() {
        let k = [(0.30, 250)];
        let p = [(0.40, 120)];
        let (kf, pf) = optimal_matched_fills(&k, &p).unwrap();
        assert_eq!((kf.contracts, pf.contracts), (120, 120));
        assert_eq!(visible_contracts(&k), 250);
        assert_eq!(visible_contracts(&p), 120);
    }

    #[test]
    fn optimal_size_avoids_depth_that_erases_profit() {
        let k = [(0.30, 80), (0.65, 100)];
        let p = [(0.40, 180)];
        let (kf, pf) = optimal_matched_fills(&k, &p).unwrap();
        assert_eq!((kf.contracts, pf.contracts), (80, 80));
        assert!(optimal_matched_fills(&[], &p).is_none());
        assert!(optimal_matched_fills(&[(0.60, 10)], &p).is_none());
        assert!(optimal_matched_fills(&[(0.50, 10)], &[(0.495, 10)]).is_none());
    }

    #[test]
    fn optimizer_matches_exhaustive_fills_with_rounded_fees_and_both_poly_sides() {
        // Vary shallow edges, level boundaries, and fractional quantities.
        // Compare to independent fills at every feasible quantity, including
        // sizes past a losing marginal level and fee rounding discontinuities.
        for buy_long in [false, true] {
            for seed in 0..60 {
                let k = KalshiBook {
                    no: BTreeMap::from([(9_400 - seed * 7, 72.8), (8_900 - seed * 7, 61.2)]),
                    ..Default::default()
                };
                let asks = [
                    (8_800 + seed * 3, 39.9),
                    (9_100 + seed * 3, 70.1),
                    (9_800, 90.0),
                ];
                let mut p = PolyBook::default();
                if buy_long {
                    p.offers = BTreeMap::from(asks);
                } else {
                    p.bids = asks
                        .into_iter()
                        .map(|(price, qty)| (10_000 - price, qty))
                        .collect();
                }
                let (kl, pl) = executable_levels(&k, &p, buy_long);
                let mut expected = None;
                let mut best_net = 0.0;
                for qty in 1..=visible_contracts(&kl).min(visible_contracts(&pl)) {
                    let kf = kalshi_yes_fill(&k, qty);
                    let pf = polymarket_fill(&p, buy_long, qty);
                    let net = qty as f64 - kf.cost - pf.cost - kf.fee - pf.fee;
                    if net > best_net + 1e-9 {
                        best_net = net;
                        expected = Some((kf, pf));
                    }
                }
                let actual = optimal_matched_fills(&kl, &pl);
                assert_eq!(
                    actual.as_ref().map(|(k, _)| k.contracts),
                    expected.as_ref().map(|(k, _)| k.contracts)
                );
                if let (Some((ak, ap)), Some((ek, ep))) = (actual, expected) {
                    for (a, e) in [(ak, ek), (ap, ep)] {
                        assert!((a.cost - e.cost).abs() < 1e-9);
                        assert!((a.fee - e.fee).abs() < 1e-9);
                        assert_eq!(a.levels, e.levels);
                    }
                }
            }
        }
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
    refresh: Duration,
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
        .filter(|pair| !pair.poly_slug.is_empty())
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
    let (kalshi, polymarket) = tokio::try_join!(connect_async(kr), connect_async(pr))?;
    let (mut ks, mut ps) = (kalshi.0, polymarket.0);
    ks.send(Message::Text(json!({"id":1,"cmd":"subscribe","params":{"channels":["orderbook_delta"],"market_tickers":tickers,"update_interval_ms":0}}).to_string().into())).await?;
    ps.send(Message::Text(json!({"subscribe":{"requestId":"read-only-scanner","subscriptionType":"SUBSCRIPTION_TYPE_MARKET_DATA","marketSlugs":slugs,"responsesDebounced":false}}).to_string().into())).await?;
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
    let mut kalshi_integrity = KalshiIntegrity::default();
    let mut poly_integrity = PolyIntegrity::default();
    let connected = Instant::now();
    let mut feed_health = FeedHealth::default();
    let mut health = tokio::time::interval(Duration::from_secs(1));
    let refresh_timer = tokio::time::sleep(refresh);
    tokio::pin!(refresh_timer);
    loop {
        tokio::select! {
            _ = &mut refresh_timer => {
                if kalshi_integrity.initialized()<tickers.len() || poly_integrity.initialized()<slugs.len() {
                    return Err("session ended before all initial books were validated".into());
                }
                return Ok(());
            },
            _ = health.tick() => {
                let elapsed = connected.elapsed().as_millis() as u64;
                if feed_health.check(elapsed, [tickers.len(),slugs.len()], [kalshi_integrity.initialized(),poly_integrity.initialized()])? {
                    tokio::time::timeout(Duration::from_secs(1), ks.send(Message::Ping(Vec::new().into()))).await??;
                    tokio::time::timeout(Duration::from_secs(1), ps.send(Message::Ping(Vec::new().into()))).await??;
                    feed_health.ping_sent(elapsed);
                }
            },
            message = ks.next() => {
                let message = message.ok_or("Kalshi WebSocket closed")??;
                match message {
                    Message::Text(text) => {
                        let value: Value = serde_json::from_str(&text).map_err(|_| "malformed Kalshi JSON")?;
                        let changed = kalshi_integrity.apply(&value, &mut kb, Utc::now().timestamp_millis())?;
                        if let Some(indices) = changed.and_then(|ticker| index.by_kalshi.get(ticker)) {
                            candidate(pairs, indices, &kb, &pb, &mut pending, &mut emitted, reporter).await?;
                        }
                    },
                    Message::Ping(payload) => { tokio::time::timeout(Duration::from_secs(1), ks.send(Message::Pong(payload))).await??; },
                    Message::Pong(_) => {},
                    Message::Close(_) => return Err("Kalshi WebSocket closed".into()),
                    _ => return Err("unsupported Kalshi WebSocket frame".into()),
                }
                feed_health.received(0,connected.elapsed().as_millis() as u64);
            },
            message = ps.next() => {
                let message = message.ok_or("Polymarket WebSocket closed")??;
                match message {
                    Message::Text(text) => {
                        let value: Value = serde_json::from_str(&text).map_err(|_| "malformed Polymarket JSON")?;
                        let changed = poly_integrity.apply(&value, &mut pb, Utc::now().timestamp_millis())?;
                        if let Some(slug) = changed {
                            if pb[slug].updated.is_none() {
                                pending.retain(|(market, _), _| *market != slug);
                                emitted.retain(|(market, _), _| *market != slug);
                            }
                            if let Some(indices) = index.by_polymarket.get(slug) {
                                candidate(pairs, indices, &kb, &pb, &mut pending, &mut emitted, reporter).await?;
                            }
                        }
                    },
                    Message::Ping(payload) => { tokio::time::timeout(Duration::from_secs(1), ps.send(Message::Pong(payload))).await??; },
                    Message::Pong(_) => {},
                    Message::Close(_) => return Err("Polymarket WebSocket closed".into()),
                    _ => return Err("unsupported Polymarket WebSocket frame".into()),
                }
                feed_health.received(1,connected.elapsed().as_millis() as u64);
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
        let adds_slug = usize::from(!pair.poly_slug.is_empty() && !slugs.contains(&pair.poly_slug));
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
        if !pair.poly_slug.is_empty() {
            slugs.insert(pair.poly_slug.clone());
        }
        batches.last_mut().expect("batch exists").push(pair.clone());
    }
    batches
}

async fn run_batches(
    pairs: &[Pair],
    reporter: &mpsc::Sender<CandidateRecord>,
    refresh: Duration,
) -> Result<(), Box<dyn Error>> {
    let batches = subscription_batches(pairs);
    try_join_all(
        batches
            .iter()
            .map(|batch| run_session(batch, reporter, refresh)),
    )
    .await?;
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
    if env::args().skip(2).any(|arg| arg == "--review") {
        let client = Client::builder()
            .user_agent("arbitrage-executor-rules-review/0.1")
            .timeout(Duration::from_secs(20))
            .build()?;
        if let Some(sports) = sports {
            discover_sports(&client, sports).await?;
        } else {
            for selection in [
                "cfb", "nfl", "nba", "cbb", "wcbb", "mlb", "cs2", "valorant", "dota2", "lol", "r6",
                "atp", "wta",
            ] {
                discover_sports(
                    &client,
                    selected_sports(selection).expect("supported selection"),
                )
                .await?;
            }
        }
        return Ok(());
    }
    if env::args().skip(2).any(|arg| arg == "--feed-check") {
        let sports = sports.ok_or("--feed-check requires a specific sport, not all")?;
        let client = Client::builder()
            .user_agent("arbitrage-executor-feed-check/0.1")
            .timeout(Duration::from_secs(20))
            .build()?;
        let discovery = discover_sports(&client, sports).await?;
        let pair = discovery
            .pairs
            .first()
            .ok_or("no matched pair available for feed check")?;
        let (sender, _receiver) = mpsc::channel(1);
        run_session(std::slice::from_ref(pair), &sender, Duration::from_secs(10)).await?;
        println!(
            "Feed check passed for one matched pair over 10 seconds. Source timestamps, book contents, and Kalshi sequences validated. No orders sent or balances read."
        );
        return Ok(());
    }
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
        let cbb = scan_forever(
            "cbb",
            selected_sports("cbb").expect("supported selection"),
            &reporter_tx,
        );
        let wcbb = scan_forever(
            "wcbb",
            selected_sports("wcbb").expect("supported selection"),
            &reporter_tx,
        );
        let nba = scan_forever(
            "nba",
            selected_sports("nba").expect("supported selection"),
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
        let atp = scan_forever(
            "atp",
            selected_sports("atp").expect("supported selection"),
            &reporter_tx,
        );
        let wta = scan_forever(
            "wta",
            selected_sports("wta").expect("supported selection"),
            &reporter_tx,
        );
        tokio::try_join!(
            cfb, nfl, nba, cbb, wcbb, mlb, cs2, valorant, dota2, lol, r6, atp, wta
        )?;
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
                    url: "unavailable (synthetic preview)",
                    available: contracts,
                    outcome: kalshi_team,
                    average_price: kalshi_price,
                    fee: kalshi_fee,
                    levels: kalshi_levels,
                },
                DisplaySide {
                    url: "unavailable (synthetic preview)",
                    available: contracts,
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

async fn discover_sports(client: &Client, sports: &[Sport]) -> Result<Discovery, Box<dyn Error>> {
    let mut combined = Discovery { pairs: Vec::new() };
    for sport in sports {
        combined.pairs.extend(discover(client, *sport).await?.pairs);
    }
    Ok(combined)
}

async fn scan_forever(
    selection: &str,
    sports: &[Sport],
    reporter: &mpsc::Sender<CandidateRecord>,
) -> Result<(), Box<dyn Error>> {
    let client = Client::builder()
        .user_agent("arbitrage-executor-read-only/0.1")
        .timeout(Duration::from_secs(20))
        .build()?;
    let mut reconnect_attempt = 0;
    let mut prepared_pairs = None;
    loop {
        let discovered = match prepared_pairs.take() {
            Some(pairs) => Ok(pairs),
            None => discover_sports(&client, sports).await,
        };
        let discovery = match discovered {
            Ok(discovery) if !discovery.pairs.is_empty() => discovery,
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
        let refresh = DISCOVERY_REFRESH;
        let active = run_batches(&discovery.pairs, reporter, refresh);
        let prefetch = async {
            tokio::time::sleep(refresh - DISCOVERY_PREFETCH_LEAD).await;
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
                    Ok(discovery) if !discovery.pairs.is_empty() => {
                        prepared_pairs = Some(discovery)
                    }
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
                if let Some(Ok(discovery)) = prefetched {
                    if !discovery.pairs.is_empty() {
                        prepared_pairs = Some(discovery);
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
