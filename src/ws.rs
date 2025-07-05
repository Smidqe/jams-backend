use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use actix_ws::{AggregatedMessage, CloseCode, CloseReason};
use futures::StreamExt;
use tokio::{sync::mpsc, time::interval};

use crate::server::ServerHandle;

/// How often heartbeat pings are sent
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

/// How long before lack of client response causes a timeout
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

/// Echo text & binary messages received from the client, respond to ping messages, and monitor
/// connection health to detect network issues and free up resources.
pub async fn handler(
    handle: Arc<ServerHandle>,
    mut session: actix_ws::Session,
    msg_stream: actix_ws::MessageStream,
) {
    let mut last_heartbeat = Instant::now();
    let mut interval = interval(HEARTBEAT_INTERVAL);

    let (conn_tx, mut conn_rx) = mpsc::unbounded_channel();
    println!("created channel");
    let id = handle.connect(conn_tx).await;
    println!("get id");
    let mut msg_stream = msg_stream
        .max_frame_size(128 * 1024)
        .aggregate_continuations()
        .max_continuation_size(2 * 1024 * 1024);

    let close_reason = loop {
        tokio::select! {
            Some(Ok(msg)) = msg_stream.next() => {
                match msg {
                    AggregatedMessage::Ping(bytes) => {
                        println!("Ping sent");
                        last_heartbeat = Instant::now();
                        session.pong(&bytes).await.unwrap();
                    }

                    AggregatedMessage::Pong(_) => {
                        println!("Pong received");
                        last_heartbeat = Instant::now();
                    }

                    AggregatedMessage::Close(reason) => break reason,
                    _ => break Some(CloseReason::from(CloseCode::Unsupported))
                }
            }

            Some(msg) = conn_rx.recv() => {
                println!("Received message: {}", msg);
                session.text(msg).await.unwrap()
            }

            _ = interval.tick() => {
                if Instant::now().duration_since(last_heartbeat) > CLIENT_TIMEOUT {
                    break None;
                }
                let _ = session.ping(b"").await;
            }

            else => {
                break None;
            }
        }
    };

    handle.disconnect(id).await;

    // attempt to close connection gracefully
    let _ = session.close(close_reason).await;
}
