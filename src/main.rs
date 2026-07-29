use std::{
    collections::HashMap,
    process::{Command, Stdio},
    sync::{Arc, atomic::AtomicU32},
    time::Duration,
};

use anyhow::Result;
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::{
    net::TcpStream,
    sync::{Mutex, RwLock, oneshot},
    time::sleep,
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

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

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(untagged)]
enum WsResponse {
    Response { id: u32, result: Value },
    Event { method: String, params: Value },
}

type CommandMap = HashMap<u32, oneshot::Sender<WsResponse>>;

#[derive(Debug)]
struct CdpClient {
    sink: Arc<Mutex<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>>,
    pending: Arc<RwLock<CommandMap>>,
    next_id: Arc<AtomicU32>,
}

impl CdpClient {
    async fn connect(ws_url: &str) -> Result<Self> {
        let (ws_stream, _) = connect_async(ws_url)
            .await
            .expect("Failed to connect to WebSocket");

        let (sink, source) = ws_stream.split();

        let pending = Arc::new(RwLock::new(CommandMap::new()));

        let pending_for_reader = pending.clone();

        let client = Self {
            next_id: Arc::new(AtomicU32::new(1)),
            sink: Arc::new(Mutex::new(sink)),
            pending,
        };
        tokio::spawn(event_reader(source, pending_for_reader));

        Ok(client)
    }

    async fn send_message(&self, method: &str, params: Option<Value>) -> Result<WsResponse> {
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let message = Message::text(
            json!({
                "id": id,
                "method": method,
                "params": params
            })
            .to_string(),
        );

        // Wait for the response
        let (tx, rx) = oneshot::channel::<WsResponse>();
        self.pending.write().await.insert(id, tx);

        let mut sink = self.sink.lock().await;
        sink.send(message).await?;
        drop(sink); // Release the lock before awaiting the response

        let response = rx.await?;
        Ok(response)
    }
}

async fn event_reader(
    mut source: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    pending_for_reader: Arc<RwLock<CommandMap>>,
) {
    loop {
        let message = source
            .next()
            .await
            .expect("WebSocket connection closed unexpectedly")
            .expect("Failed to read message from WebSocket");

        match message {
            Message::Text(text) => {
                let response: WsResponse =
                    serde_json::from_str(&text).expect("Failed to parse WebSocket message");
                match &response {
                    WsResponse::Response { id, .. } => {
                        pending_for_reader
                            .write()
                            .await
                            .remove(&id)
                            .map(|tx| tx.send(response));
                    }
                    WsResponse::Event { method, params } => {
                        // TODO: handle events properly, related: broadcast channel
                        println!("Received event: {} with params: {:?}", method, params);
                    }
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
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

    let targets = reqwest::get("http://localhost:9222/json")
        .await?
        .json::<Vec<Target>>()
        .await?;

    let ws_url = &targets[0].web_socket_debugger_url;
    let client = CdpClient::connect(ws_url).await?;

    client
        .send_message("Page.enable", None)
        .await
        .expect("Failed to send command");

    client
        .send_message(
            "Page.navigate",
            Some(serde_json::json!({ "url": "https://www.example.com" })),
        )
        .await
        .expect("Failed to send command");

    // TODO: add a `goto` method to `CdpClient` that wraps `send_message`
    // it should wait for the `Page.loadEventFired` event before returning

    tokio::signal::ctrl_c().await.unwrap();

    Ok(())
}
