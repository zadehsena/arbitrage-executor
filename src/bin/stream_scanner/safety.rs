use super::*;
use chrono::DateTime;
#[cfg(test)]
use chrono::Duration as ChronoDuration;
use serde::de::DeserializeOwned;
use std::path::Path;

#[derive(Clone, serde::Serialize)]
pub(super) struct LegSafety {
    pub fees: Option<LegFees>,
    pub fee_error: Option<String>,
    pub rules: RuleReview,
    pub decision: ReviewDecision,
}

impl LegSafety {
    pub fn eligible(&self, now: DateTime<Utc>) -> bool {
        self.fees.as_ref().is_some_and(|f| f.expires_at > now)
            && self.decision.approved
            && self.decision.expires_at.is_some_and(|until| until > now)
    }
}

fn load_json<T: DeserializeOwned + Default>(path: &Path) -> Result<T, Box<dyn Error>> {
    match fs::read(path) {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)
            .map_err(|e| format!("invalid {}: {e}", path.display()))?),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(T::default()),
        Err(e) => Err(e.into()),
    }
}

struct SeriesData {
    metadata: Value,
    terms_hash: Option<String>,
    fees: Option<LegFees>,
    error: Option<String>,
}

async fn download_terms(
    client: &Client,
    metadata: &Value,
    directory: &Path,
) -> Result<String, Box<dyn Error>> {
    let url = Url::parse(
        metadata["contract_terms_url"]
            .as_str()
            .ok_or("terms URL missing")?,
    )?;
    if url.scheme() != "https" || url.host_str() != Some("assets.kalshi.com") {
        return Err("unsupported contract-terms host".into());
    }
    let mut response = client.get(url).send().await?.error_for_status()?;
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len() + chunk.len() > 4 * 1024 * 1024 {
            return Err("contract terms exceed 4 MiB".into());
        }
        bytes.extend_from_slice(&chunk);
    }
    if !bytes.starts_with(b"%PDF-") {
        return Err("contract terms are not a PDF".into());
    }
    let hash = digest(&bytes);
    fs::create_dir_all(directory.join("terms"))?;
    let path = directory.join("terms").join(format!("{hash}.pdf"));
    if !path.exists() {
        fs::write(path, bytes)?;
    }
    Ok(hash)
}

pub(super) async fn prepare_pair_safety(
    client: &Client,
    sport: Sport,
    pairs: &mut [Pair],
    kalshi_markets: &HashMap<String, Value>,
    poly_details: &HashMap<String, Value>,
) -> Result<(), Box<dyn Error>> {
    let policy_path = env::var("SCANNER_FEE_POLICY").unwrap_or_else(|_| "fee-policy.json".into());
    let approval_path = env::var("SCANNER_SETTLEMENT_APPROVALS")
        .unwrap_or_else(|_| "settlement-approvals.json".into());
    let directory = env::var("SCANNER_REVIEW_DIR").unwrap_or_else(|_| "market-reviews".into());
    let directory = Path::new(&directory);
    let now = Utc::now();
    let policy = load_json::<FeePolicy>(Path::new(&policy_path))?.resolve(now)?;
    let approvals = load_json::<Approvals>(Path::new(&approval_path))?;
    approvals.validate()?;
    let mut series_data = HashMap::new();
    let series: std::collections::BTreeSet<_> = pairs
        .iter()
        .flat_map(|p| &p.kalshi)
        .filter_map(|ticker| ticker.split_once('-').map(|(series, _)| series.to_owned()))
        .collect();
    for ticker in series {
        let metadata = get(client, format!("{KALSHI_REST}/series/{ticker}")).await;
        let mut data = SeriesData {
            metadata: Value::Null,
            terms_hash: None,
            fees: None,
            error: None,
        };
        match metadata {
            Err(_) => data.error = Some("Kalshi series metadata unavailable".into()),
            Ok(payload) => {
                data.metadata = payload["series"].clone();
                // Fee changes are fetched without history; the first future
                // transition limits the lifetime of this fee snapshot.
                match get(
                    client,
                    format!("{KALSHI_REST}/series/fee_changes?series_ticker={ticker}"),
                )
                .await
                {
                    Ok(changes) => {
                        match LegFees::from_series(&data.metadata, &changes, &policy, now) {
                            Ok(fees) => data.fees = Some(fees),
                            Err(e) => data.error = Some(e),
                        }
                    }
                    Err(_) => data.error = Some("Kalshi scheduled fee changes unavailable".into()),
                }
                match download_terms(client, &data.metadata, directory).await {
                    Ok(hash) => data.terms_hash = Some(hash),
                    Err(e) => eprintln!(
                        "{}: contract terms unavailable for {ticker}: {e}",
                        sport.label
                    ),
                }
            }
        }
        series_data.insert(ticker, data);
    }
    let mut poly_markets = HashMap::new();
    for detail in poly_details.values() {
        for market in detail["event"]["markets"].as_array().into_iter().flatten() {
            if let Some(slug) = market["slug"].as_str() {
                poly_markets.insert(slug, (market, &detail["event"]));
            }
        }
    }
    let mut entries = Vec::new();
    let mut fee_valid = 0;
    let mut approved = 0;
    for pair in pairs {
        for side in 0..2 {
            let ticker = &pair.kalshi[side];
            if ticker.is_empty() {
                continue;
            }
            let series = ticker
                .split_once('-')
                .and_then(|(series, _)| series_data.get(series));
            let kalshi = kalshi_markets.get(ticker).unwrap_or(&Value::Null);
            let (poly, event) = poly_markets
                .get(pair.poly_slug.as_str())
                .copied()
                .unwrap_or((&Value::Null, &Value::Null));
            let poly_long = (1 - side == 0) == pair.poly_long_is_first;
            let rules = RuleReview::new(
                kalshi,
                series.map(|s| &s.metadata).unwrap_or(&Value::Null),
                series.and_then(|s| s.terms_hash.as_deref()),
                poly,
                event,
                poly_long,
            );
            let decision = rules.decision(&approvals, Utc::now());
            let safety = Arc::new(LegSafety {
                fees: series.and_then(|s| s.fees.clone()),
                fee_error: series
                    .and_then(|s| s.error.clone())
                    .or_else(|| series.is_none().then(|| "Kalshi series missing".into())),
                rules,
                decision,
            });
            fee_valid += usize::from(
                safety
                    .fees
                    .as_ref()
                    .is_some_and(|f| f.expires_at > Utc::now()),
            );
            approved += usize::from(safety.eligible(Utc::now()));
            entries.push(json!({"sport":pair.sport, "event":pair.title, "market":pair.market, "kalshi_ticker":ticker, "kalshi_url":kalshi_market_url(ticker), "kalshi_outcome":pair.teams[side], "polymarket_slug":pair.poly_slug, "polymarket_url":pair.poly_url, "polymarket_outcome":pair.teams[1-side], "assessment":safety.as_ref()}));
            pair.safety[side] = Some(safety);
        }
    }
    fs::create_dir_all(directory)?;
    let path = directory.join(format!("{}.json", sport.label.to_lowercase()));
    let temp = path.with_extension("json.tmp");
    let count = entries.len();
    let report = json!({"schema_version":1,"generated_at":Utc::now(),"sport":sport.label,"fee_policy":policy,"directions":count,"fee_valid":fee_valid,"approved":approved,"note":"Only approved, unexpired directions with valid fee metadata may emit candidates. No trades are executed. Review PDFs in terms/ alongside both venues' rule text. No approvals are generated automatically.","reviews":entries});
    fs::write(&temp, serde_json::to_vec_pretty(&report)?)?;
    fs::rename(&temp, &path)?;
    println!(
        "{} review: {fee_valid}/{count} directions have fee metadata; {approved}/{count} approved for candidate reporting. Report: {}",
        sport.label,
        path.display()
    );
    Ok(())
}

#[cfg(test)]
pub(super) fn approved_test_safety() -> Arc<LegSafety> {
    let now = Utc::now();
    Arc::new(LegSafety {
        fees: Some(LegFees {
            kalshi: default_kalshi_fee(),
            polymarket: default_poly_fee(),
            basis: json!({"test":true}),
            expires_at: now + ChronoDuration::hours(1),
        }),
        fee_error: None,
        rules: RuleReview::new(
            &Value::Null,
            &Value::Null,
            None,
            &Value::Null,
            &Value::Null,
            false,
        ),
        decision: ReviewDecision {
            approved: true,
            reasons: vec![],
            expires_at: Some(now + ChronoDuration::hours(1)),
        },
    })
}
