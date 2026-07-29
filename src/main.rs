use std::{process::{Command, Stdio}, thread::sleep, time::Duration};

// TODO: look at `which` crate
const CHROMIUM_PATH: &str = "/usr/local/bin/chromium";


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
    sleep(Duration::new(1, 0));

    let body = reqwest::get("http://localhost:9222/json").await?.text().await?;

    println!("Response body: \n{}", body);

    Ok(())
}
