//! Read-only WebSocket connectivity probe. It deliberately has no order APIs.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use ed25519_dalek::{Signer as _, pkcs8::DecodePrivateKey as _};
use futures_util::{SinkExt as _, StreamExt as _};
use rand::rngs::OsRng;
use rsa::{
    RsaPrivateKey,
    pkcs1::DecodeRsaPrivateKey as _,
    pss::BlindedSigningKey,
    signature::{RandomizedSigner as _, SignatureEncoding as _},
};
use serde_json::{Value, json};
use sha2::Sha256;
use std::{
    env,
    error::Error,
    fs,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{
        Message,
        client::IntoClientRequest,
        http::header::{HeaderName, HeaderValue},
    },
};

const KALSHI_WS_URL: &str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
const POLYMARKET_WS_URL: &str = "wss://api.polymarket.us/v1/ws/markets";

fn timestamp_ms() -> Result<String, Box<dyn Error>> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .to_string())
}

fn kalshi_signature(timestamp: &str, path: &str) -> Result<String, Box<dyn Error>> {
    let pem = fs::read_to_string(env::var("KALSHI_PRIVATE_KEY_PATH")?)?;
    let message = format!("{timestamp}GET{path}");
    if let Ok(key) = ed25519_dalek::SigningKey::from_pkcs8_pem(&pem) {
        return Ok(BASE64.encode(key.sign(message.as_bytes()).to_bytes()));
    }
    let rsa =
        RsaPrivateKey::from_pkcs1_pem(&pem).or_else(|_| RsaPrivateKey::from_pkcs8_pem(&pem))?;
    let signer = BlindedSigningKey::<Sha256>::new(rsa);
    Ok(BASE64.encode(
        signer
            .sign_with_rng(&mut OsRng, message.as_bytes())
            .to_bytes(),
    ))
}

fn polymarket_signature(timestamp: &str, path: &str) -> Result<String, Box<dyn Error>> {
    let secret = BASE64.decode(env::var("POLYMARKET_US_SECRET_KEY")?)?;
    let key_bytes: [u8; 32] = secret
        .get(..32)
        .ok_or("POLYMARKET_US_SECRET_KEY is too short")?
        .try_into()?;
    let key = ed25519_dalek::SigningKey::from_bytes(&key_bytes);
    Ok(BASE64.encode(
        key.sign(format!("{timestamp}GET{path}").as_bytes())
            .to_bytes(),
    ))
}

fn authenticated_request(
    url: &str,
    headers: &[(&str, String)],
) -> Result<tokio_tungstenite::tungstenite::http::Request<()>, Box<dyn Error>> {
    let mut request = url.into_client_request()?;
    for (name, value) in headers {
        request.headers_mut().insert(
            HeaderName::from_bytes(name.as_bytes())?,
            HeaderValue::from_str(value)?,
        );
    }
    Ok(request)
}

fn summarize(source: &str, message: &Message) {
    let Ok(text) = message.to_text() else {
        return;
    };
    let Ok(payload) = serde_json::from_str::<Value>(text) else {
        return;
    };
    let message_type = payload["type"]
        .as_str()
        .or_else(|| payload.pointer("/marketData/state").and_then(Value::as_str))
        .unwrap_or("message");
    let market = payload
        .pointer("/msg/market_ticker")
        .and_then(Value::as_str)
        .or_else(|| {
            payload
                .pointer("/marketData/marketSlug")
                .and_then(Value::as_str)
        })
        .or_else(|| {
            payload
                .pointer("/marketDataLite/marketSlug")
                .and_then(Value::as_str)
        })
        .unwrap_or("subscription");
    println!("{source:<11} {message_type:<24} {market}");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenvy::dotenv().ok();
    let mut args = env::args().skip(1);
    let kalshi_a = args
        .next()
        .unwrap_or_else(|| "KXNCAAFGAME-26OCT01UNTTLSA-TLSA".to_owned());
    let kalshi_b = args
        .next()
        .unwrap_or_else(|| "KXNCAAFGAME-26OCT01UNTTLSA-UNT".to_owned());
    let polymarket = args
        .next()
        .unwrap_or_else(|| "aec-cfb-ntx-tulsa-2026-10-01".to_owned());

    let kalshi_timestamp = timestamp_ms()?;
    let kalshi_request = authenticated_request(
        KALSHI_WS_URL,
        &[
            ("KALSHI-ACCESS-KEY", env::var("KALSHI_API_KEY_ID")?),
            ("KALSHI-ACCESS-TIMESTAMP", kalshi_timestamp.clone()),
            (
                "KALSHI-ACCESS-SIGNATURE",
                kalshi_signature(&kalshi_timestamp, "/trade-api/ws/v2")?,
            ),
        ],
    )?;
    let polymarket_timestamp = timestamp_ms()?;
    let polymarket_request = authenticated_request(
        POLYMARKET_WS_URL,
        &[
            ("X-PM-Access-Key", env::var("POLYMARKET_US_KEY_ID")?),
            ("X-PM-Timestamp", polymarket_timestamp.clone()),
            (
                "X-PM-Signature",
                polymarket_signature(&polymarket_timestamp, "/v1/ws/markets")?,
            ),
        ],
    )?;

    let (mut kalshi_socket, _) = connect_async(kalshi_request).await?;
    let (mut polymarket_socket, _) = connect_async(polymarket_request).await?;
    kalshi_socket
        .send(Message::Text(
            json!({
                "id": 1,
                "cmd": "subscribe",
                "params": {"channels": ["orderbook_delta"], "market_tickers": [kalshi_a, kalshi_b]}
            })
            .to_string()
            .into(),
        ))
        .await?;
    polymarket_socket
        .send(Message::Text(
            json!({
                "subscribe": {
                    "requestId": "read-only-stream-probe",
                    "subscriptionType": "SUBSCRIPTION_TYPE_MARKET_DATA",
                    "marketSlugs": [polymarket]
                }
            })
            .to_string()
            .into(),
        ))
        .await?;

    println!(
        "Connected to both market-data streams. Reading six messages; no order routes exist in this binary."
    );
    let mut remaining = 6;
    while remaining > 0 {
        tokio::select! {
            message = kalshi_socket.next() => if let Some(Ok(message)) = message { summarize("Kalshi", &message); remaining -= 1; },
            message = polymarket_socket.next() => if let Some(Ok(message)) = message { summarize("Polymarket", &message); remaining -= 1; },
        }
    }
    Ok(())
}
