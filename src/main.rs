use std::{
    collections::HashMap,
    process::{Command, Stdio},
    sync::{Arc, atomic::AtomicU32},
    time::Duration,
};

use futures_util::{FutureExt, SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{RwLock, oneshot},
    time::sleep,
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

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

#[derive(Deserialize, Serialize, Debug)]
struct WsCommand {
    id: u32,
    method: String,
    params: serde_json::Value,
}

impl WsCommand {
    fn new(method: &str) -> Self {
        WsCommand {
            id: COMMAND_ID.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            method: method.to_string(),
            params: serde_json::Value::Null,
        }
    }

    fn with_params(mut self, params: serde_json::Value) -> Self {
        self.params = params;
        self
    }
}

#[derive(Deserialize, Serialize, Debug)]
struct WsResponse {
    id: u32,
    result: serde_json::Value,
}

static COMMAND_ID: AtomicU32 = AtomicU32::new(1);

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
    // TODO: figure out a better way to wait for Chromium to be ready
    sleep(Duration::new(2, 0)).await;

    let response = reqwest::get("http://localhost:9222/json")
        .await?
        .json::<Vec<Target>>()
        .await?;

    let (ws_stream, _) = connect_async(&response[0].web_socket_debugger_url)
        .await
        .expect("Failed to connect to WebSocket");

    let (mut sink, mut _source) = ws_stream.split();

    let pending = Arc::new(RwLock::new(HashMap::<
        u32,
        oneshot::Sender<serde_json::Value>,
    >::new()));
    let pending_for_reader = pending.clone();

    // Reader task to handle incoming messages from the WebSocket
    tokio::spawn(async move {
        while let Some(message) = _source.next().await {
            match message {
                Ok(Message::Text(text)) => {
                    println!("Received message: {:?}", text);

                    // If the message is a response to a command, send it to the corresponding oneshot channel
                    if let Ok(response) = serde_json::from_str::<WsResponse>(&text) {
                        pending_for_reader
                            .write()
                            .await
                            .remove(&response.id)
                            .map(|tx| tx.send(response.result));
                    }
                }
                Ok(Message::Close(_)) => break,
                _ => {}
            }
        }
    });

    let mut send_command = async |command: WsCommand| -> serde_json::Value {
        let pending = pending.clone();
        let json = serde_json::to_string(&command).expect("Failed to serialize command");
        let message = Message::text(json);
        sink.send(message).await.expect("Failed to send command");

        // Wait for the response
        let (tx, rx) = oneshot::channel::<serde_json::Value>();
        pending.write().await.insert(command.id, tx);
        let response = rx.await.expect("Failed to receive response");
        response
    };

    send_command(WsCommand::new("Page.enable"))
        .then(|res| async move { println!("Response from Page.enable: {:?}", res) })
        .await;

    send_command(
        WsCommand::new("Page.navigate")
            .with_params(serde_json::json!({ "url": "https://www.example.com" })),
    )
    .then(|res| async move { println!("Response from Page.navigate: {:?}", res) })
    .await;

    tokio::signal::ctrl_c().await.unwrap();

    Ok(())
}
