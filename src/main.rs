use arbitrage_executor::{engine::ExecutionEngine, journal, model::Signal, risk::RiskLimits};
use std::{env, fs, path::PathBuf, process::ExitCode};

#[tokio::main]
async fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let Some(flag) = args.next() else {
        eprintln!("Usage: arbitrage-executor --signal <signal.json> [--journal <journal.jsonl>]");
        return ExitCode::from(2);
    };
    if flag != "--signal" {
        eprintln!("Expected --signal");
        return ExitCode::from(2);
    }
    let Some(signal_path) = args.next() else {
        eprintln!("Missing signal file path");
        return ExitCode::from(2);
    };
    let journal_path = match (args.next().as_deref(), args.next()) {
        (Some("--journal"), Some(path)) => PathBuf::from(path),
        (None, None) => PathBuf::from("execution-journal.jsonl"),
        _ => {
            eprintln!("Usage: arbitrage-executor --signal <signal.json> [--journal <journal.jsonl>]");
            return ExitCode::from(2);
        }
    };

    let signal = match fs::read_to_string(&signal_path)
        .map_err(|error| error.to_string())
        .and_then(|body| serde_json::from_str::<Signal>(&body).map_err(|error| error.to_string())) {
        Ok(signal) => signal,
        Err(error) => {
            eprintln!("Could not load signal: {error}");
            return ExitCode::from(1);
        }
    };
    let plan = ExecutionEngine::new(RiskLimits::default()).evaluate(signal).await;
    if let Err(error) = journal::append(journal_path, &plan) {
        eprintln!("Could not write journal: {error}");
        return ExitCode::from(1);
    }
    println!("{}", serde_json::to_string_pretty(&plan).expect("plan is serializable"));
    ExitCode::SUCCESS
}
