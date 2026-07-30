use std::{
    collections::HashMap,
    process::Stdio,
    sync::{Arc, atomic::AtomicU32},
    time::Duration,
};

use anyhow::{Result, anyhow};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use serde::Deserialize;
use serde_json::{Value, json};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    net::TcpStream,
    process::Command,
    sync::{Mutex, RwLock, oneshot},
    time::sleep,
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

// TODO: look at `which` crate
const CHROMIUM_PATH: &str = "/usr/local/bin/chromium";

type CommandId = u32;

type WsResponseResult = std::result::Result<CommandSuccess, ProtocolError>;

#[derive(Debug, Clone)]
enum WsResponse {
    Event(CdpEvent),
    CommandResponse(CommandResponse),
}

#[derive(Debug, Clone, Deserialize)]
struct CdpEvent {
    method: String,
    params: Value,
}

#[derive(Debug, Clone, Deserialize)]
struct CommandSuccess {
    id: CommandId,
    result: Value,
}

#[derive(Debug, Clone, Deserialize)]
struct CommandFailure {
    id: CommandId,
    error: ProtocolError,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum CommandResponse {
    Success(CommandSuccess),
    Failure(CommandFailure),
}

impl CommandResponse {
    fn id(&self) -> CommandId {
        match self {
            Self::Success(success) => success.id,
            Self::Failure(failure) => failure.id,
        }
    }

    fn into_result(self) -> WsResponseResult {
        match self {
            Self::Success(success) => Ok(success),
            Self::Failure(failure) => Err(failure.error),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Error)]
#[error("CDP error {code}: {message}")]
struct ProtocolError {
    code: i64,
    message: String,
}

fn parse_ws_response(text: &str) -> WsResponse {
    let value: Value = serde_json::from_str(text).expect("Failed to parse WebSocket message");
    let object = value
        .as_object()
        .expect("Incoming websocket message must be a JSON object");

    if object.contains_key("id") {
        let response: CommandResponse =
            serde_json::from_value(value).expect("Failed to deserialize command response");
        WsResponse::CommandResponse(response)
    } else if object.contains_key("method") {
        let event: CdpEvent =
            serde_json::from_value(value).expect("Failed to deserialize event message");
        WsResponse::Event(event)
    } else {
        panic!("Incoming websocket message must include either `id` or `method`");
    }
}

type CommandMap = HashMap<CommandId, oneshot::Sender<WsResponseResult>>;

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

    async fn send_command(&self, method: &str, params: Option<Value>) -> WsResponseResult {
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
        let (tx, rx) = oneshot::channel::<WsResponseResult>();
        self.pending.write().await.insert(id, tx);

        let mut sink = self.sink.lock().await;
        sink.send(message)
            .await
            .expect("Failed to send message to WebSocket");
        drop(sink); // Release the lock before awaiting the response

        let response = rx.await.expect("Failed to receive response");
        response
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
                let response = parse_ws_response(&text);
                match response {
                    WsResponse::CommandResponse(command_response) => {
                        let id = command_response.id();
                        let result = command_response.into_result();

                        pending_for_reader
                            .write()
                            .await
                            .remove(&id)
                            .map(|tx| tx.send(result));
                    }
                    WsResponse::Event(ws_event) => {
                        // TODO: handle events properly, related: broadcast channel
                        dbg!(ws_event);
                    }
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
}

async fn launch_chromium() -> Result<(tokio::process::Child, String)> {
    let mut child = Command::new(CHROMIUM_PATH)
        .args(&[
            "--headless",
            "--disable-gpu",
            "--no-sandbox",
            "--remote-debugging-port=0",
            "--no-first-run",
            "--no-default-browser-check",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()?;

    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow!("Failed to capture stderr"))?;
    let mut reader = BufReader::new(stderr).lines();

    let ws_url = tokio::time::timeout(Duration::from_secs(10), async {
        while let Some(line) = reader.next_line().await? {
            if let Some(idx) = line.find("ws://") {
                let ws_url = line[idx..].to_string();
                return Ok(ws_url);
            }
        }
        Err(anyhow!("Failed to find WebSocket URL in Chromium output"))
    })
    .await??;

    Ok((child, ws_url))
}

#[tokio::main]
async fn main() -> Result<()> {
    let (_chromium, ws_url) = launch_chromium().await?;

    println!("Connecting to WebSocket URL: {}", ws_url);
    let client = CdpClient::connect(&ws_url).await?;
    client
        .send_command(
            "Target.setAutoAttach",
            Some(json!({
                "autoAttach": true,
                "waitForDebuggerOnStart": false,
                "flatten": true
            })),
        )
        .await?;

    let target_id = client
        .send_command("Target.createTarget", Some(json!({ "url": "about:blank" })))
        .await?
        .result
        .get("targetId")
        .unwrap()
        .to_string();

    println!("Created target with ID: {}", target_id);
    // TODO: capture the `Target.attachedToTarget` event and extract the `sessionId` from it
    // maintain a map of `targetId` to `sessionId` in the `CdpClient` struct
    // then use the `sessionId` to send commands to the specific target
    // target can be a new struct

    // TODO: add a `goto` method to `CdpClient` that wraps `send_message`
    // it should wait for the `Page.loadEventFired` event before returning

    tokio::signal::ctrl_c().await.unwrap();

    Ok(())
}
