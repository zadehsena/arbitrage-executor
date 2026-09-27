use crate::model::ExecutionPlan;
use std::{fs::OpenOptions, io::{self, Write}, path::Path};

pub fn append(path: impl AsRef<Path>, plan: &ExecutionPlan) -> io::Result<()> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    serde_json::to_writer(&mut file, plan).map_err(io::Error::other)?;
    file.write_all(b"\n")?;
    file.sync_data()
}
