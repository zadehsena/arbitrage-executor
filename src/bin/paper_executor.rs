//! Offline failure-scenario runner. No network client or credentials are used.
use arbitrage_executor::{
    execution::{Action, Controller, Plan, Summary, TimedEvent},
    settlement::digest,
};
use serde::{Deserialize, Serialize};
use std::{
    env,
    error::Error,
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Scenario {
    plan: Plan,
    events: Vec<TimedEvent>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    schema_version: u32,
    mode: String,
    plan: Plan,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    index: usize,
    previous_hash: String,
    event: TimedEvent,
    actions: Vec<Action>,
    summary: Summary,
    state_sha256: String,
}
fn state_hash(c: &Controller) -> String {
    digest(&serde_json::to_vec(c).expect("serializable paper state"))
}
fn replay(path: &Path) -> Result<Summary, Box<dyn Error>> {
    let bytes = fs::read(path)?;
    if bytes.last() != Some(&b'\n') {
        return Err("truncated journal: manual review required".into());
    }
    let text = std::str::from_utf8(&bytes)?;
    let mut lines = text.lines();
    let first = lines.next().ok_or("empty journal")?;
    let header: Header = serde_json::from_str(first)?;
    if header.schema_version != 1 || header.mode != "paper" {
        return Err("unsupported paper journal".into());
    }
    let mut c = Controller::new(header.plan)?;
    let mut previous = digest(first.as_bytes());
    for (index, line) in lines.enumerate() {
        let record: Record = serde_json::from_str(line)?;
        if record.index != index || record.previous_hash != previous {
            return Err("journal hash chain or record order mismatch".into());
        }
        let actual = c.apply(&record.event);
        if actual != record.actions
            || c.summary() != record.summary
            || state_hash(&c) != record.state_sha256
        {
            return Err("journal replay differs from recorded state/actions".into());
        }
        previous = digest(line.as_bytes());
    }
    Ok(c.summary())
}
fn run(scenario: &Path, journal: &Path) -> Result<Summary, Box<dyn Error>> {
    let input: Scenario = serde_json::from_slice(&fs::read(scenario)?)?;
    let mut c = Controller::new(input.plan.clone())?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(journal)?;
    let header = serde_json::to_string(&Header {
        schema_version: 1,
        mode: "paper".into(),
        plan: input.plan,
    })?;
    writeln!(file, "{header}")?;
    file.sync_all()?;
    let mut previous = digest(header.as_bytes());
    for (index, event) in input.events.into_iter().enumerate() {
        let actions = c.apply(&event);
        let line = serde_json::to_string(&Record {
            index,
            previous_hash: previous,
            event,
            actions,
            summary: c.summary(),
            state_sha256: state_hash(&c),
        })?;
        writeln!(file, "{line}")?;
        file.sync_all()?;
        previous = digest(line.as_bytes());
        // Intents are deliberately NOT dispatched to any exchange.
    }
    let expected = c.summary();
    if replay(journal)? != expected {
        return Err("post-write replay mismatch".into());
    }
    Ok(expected)
}
fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args().skip(1).collect();
    let summary = match args.as_slice() {
        [flag, path] if flag == "--replay" => replay(Path::new(path))?,
        [scenario, journal] => run(Path::new(scenario), Path::new(journal))?,
        _ => return Err(
            "Usage: paper_executor <scenario.json> <new-journal.jsonl> | --replay <journal.jsonl>"
                .into(),
        ),
    };
    println!(
        "PAPER ONLY — no orders sent\n{}",
        serde_json::to_string_pretty(&summary)?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn durable_scenario_replay_rejects_tampering_and_truncation() {
        let path =
            env::temp_dir().join(format!("arbitrage-paper-test-{}.jsonl", std::process::id()));
        let result = run(Path::new("tests/fixtures/one_sided_fill.json"), &path).unwrap();
        assert_eq!(result.positions_micros, [6_000_000, 6_000_000]);
        assert!(run(Path::new("tests/fixtures/one_sided_fill.json"), &path).is_err()); // No journal overwrite.
        let original = fs::read(&path).unwrap();
        fs::write(&path, &original[..original.len() - 1]).unwrap();
        assert!(replay(&path).is_err());
        let text = String::from_utf8(original)
            .unwrap()
            .replace("need_exit_quote", "tampered_action");
        fs::write(&path, text).unwrap();
        assert!(replay(&path).is_err());
        fs::remove_file(path).unwrap();
    }
}
