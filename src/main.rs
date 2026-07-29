use std::{
    process::{Command, Stdio},
    thread::sleep,
    time::Duration,
};

use serde::{Deserialize, Serialize};

// TODO: look at `which` crate
const CHROMIUM_PATH: &str = "/usr/local/bin/chromium";

#[derive(Deserialize, Serialize, Debug)]
#[serde(rename_all(deserialize = "camelCase"))]
struct Target {
    description: String,
    devtools_frontend_url: String,
    id: String,
    title: String,
    type_: String,
    url: String,
    web_socket_debugger_url: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let chromium = Command::new(CHROMIUM_PATH)
        .args([
            "--headless",
            "--disable-gpu",
            "--no-sandbox",
            "--remote-debugging-port=9222",
            "--no-first-run",
            "--no-default-browser-check",
            "--user-data-dir=/tmp/chrome-profile",
        ])
        .stderr(Stdio::null())
        .spawn()
        .expect("Could not spawn chromium");
    println!("Chromium started with pid: {}", chromium.id());

    // Give Chromium a moment to start
    sleep(Duration::new(2, 0));

    let targets = reqwest::get("http://localhost:9222/json")
        .await?
        .json::<Vec<Target>>()
        .await?;
    println!("{:#?}", targets);

    Ok(())
}
