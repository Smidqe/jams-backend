mod playlist;
mod song;

use std::{
    env::current_dir,
    path::PathBuf,
    sync::{atomic::AtomicBool, Arc},
};

use axum::{
    body::{Body, Bytes},
    debug_handler,
    extract::{
        ws::{Message, Utf8Bytes, WebSocket},
        State, WebSocketUpgrade,
    },
    http::StatusCode,
    response::Response,
    routing::{any, get},
    Router,
};

use clap::Parser;
use futures::{stream::SplitSink, SinkExt, StreamExt};
use parking_lot::RwLock;
use serde::Serialize;
use tokio::{spawn, sync::broadcast, try_join};

use crate::{playlist::Playlist, song::Song};

#[debug_handler]
async fn version() -> (StatusCode, String) {
    (StatusCode::OK, String::from("0.1"))
}

#[debug_handler]
async fn song(State(state): State<Arc<AppState>>) -> Response {
    let lock = state.song.read();

    return Response::builder()
        .header("Content-Type", "application/octet-stream")
        .body(Body::from(lock.bytes.clone()))
        .unwrap();
}

#[debug_handler]
async fn artwork(State(state): State<Arc<AppState>>) -> Response {
    let lock = state.song.read();

    return Response::builder()
        .header("Content-Type", "application/octet-stream")
        .body(Body::from(lock.artwork.clone()))
        .unwrap();
}

#[derive(Debug, Default)]
struct Songlet {
    artwork: Vec<u8>,
    bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
struct AppState {
    song: Arc<RwLock<Songlet>>,
    tx: broadcast::Sender<Command>,
}

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    #[arg(short, long)]
    path: String,
}

#[derive(Debug, Clone)]
pub enum Command {
    Shutdown,
    Song { song: Song },
    Playlist { items: Vec<Song> },
}

#[derive(Serialize)]
pub enum EventKind {
    Song,
}

#[derive(Serialize)]
pub struct Event<T>
where
    T: Serialize,
{
    kind: EventKind,
    payload: T,
}

fn get_song_folder() -> PathBuf {
    current_dir().expect("aaaa").join("songs")
}

#[debug_handler]
async fn ws_upgrader(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> Response {
    ws.on_upgrade(|s| ws_handler(s, state))
}

async fn serialize_and_send<T: Serialize>(payload: T, tx: &mut SplitSink<WebSocket, Message>) {
    let ev = serde_json::to_string(&Event {
        kind: EventKind::Song,
        payload,
    });

    if let Ok(s) = ev {
        let _ = tx.send(Message::Text(Utf8Bytes::from(s))).await;
    }
}

async fn ws_handler(mut socket: WebSocket, state: Arc<AppState>) {
    if socket.send(Message::Ping(Bytes::new())).await.is_err() {
        return;
    }

    let (mut tx, mut rx) = socket.split();
    let mut listen = state.tx.subscribe();

    let mut sending = spawn(async move {
        loop {
            match listen.recv().await {
                Ok(Command::Shutdown) => break,
                Ok(Command::Playlist { items }) => serialize_and_send(items, &mut tx).await,
                Ok(Command::Song { song }) => serialize_and_send(song, &mut tx).await,
                Err(e) => println!("err {}", e),
            }
        }
    });

    //if socket errors, break
    let mut receiving = spawn(async move {
        loop {
            match rx.next().await {
                Some(Err(_)) => break,
                Some(Ok(_)) | None => (),
            }
        }
    });

    tokio::select! {
        _ = (&mut sending) => {
            receiving.abort();
        }
        _ = (&mut receiving) => {
            sending.abort();
        }
    }
}

#[tokio::main]
async fn main() {
    ffmpeg_next::init().expect("eee");

    let (tx, _rx) = broadcast::channel(1000);
    let state = Arc::new(AppState {
        song: Arc::new(RwLock::new(Songlet::default())),
        tx,
    });

    let shutdown = Arc::new(AtomicBool::new(false));
    let playlist = spawn(Playlist::from(get_song_folder()).run(state.clone(), shutdown.clone()));

    let app = Router::new()
        .route("/version", get(version))
        .route("/stream", get(song))
        .route("/ws", any(ws_upgrader))
        .route("/artwork", get(artwork))
        .with_state(state);

    axum::serve(
        tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap(),
        app,
    )
    .await
    .unwrap();

    let _ = try_join!(playlist);
}
