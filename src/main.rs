mod playlist;
mod song;

use std::{
    env::current_dir,
    fs,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use axum::{
    body::{Body, Bytes},
    debug_handler,
    extract::{
        ws::{Message, Utf8Bytes, WebSocket},
        Request, State, WebSocketUpgrade,
    },
    http::{
        header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, RANGE},
        HeaderMap, HeaderValue, StatusCode,
    },
    response::Response,
    routing::{any, get},
    Router,
};

use clap::Parser;
use dirs::home_dir;
use futures::{stream::SplitSink, SinkExt, StreamExt};
use http_range_header::{EndPosition, StartPosition};
use parking_lot::RwLock;
use serde::Serialize;
use tokio::{spawn, sync::broadcast, try_join};

use crate::{playlist::Playlist, song::Song};

#[debug_handler]
async fn version() -> Response {
    Response::builder()
        .header("x-jams-version", "0.1")
        .status(StatusCode::NO_CONTENT)
        .body(Body::empty())
        .unwrap()
}

fn song_partial(headers: &HeaderMap, state: Arc<AppState>, size: usize) -> Response {
    let lock = state.song2.read();
    let range = parse_range_header(headers.get(RANGE).unwrap(), lock.len());
    let (start, end) = match range {
        (StartPosition::FromLast(start), EndPosition::LastByte) => ((start as usize), size),
        (StartPosition::FromLast(start), EndPosition::Index(end)) => {
            ((start as usize), (end as usize))
        }
        (StartPosition::Index(start), EndPosition::LastByte) => ((start as usize), size),
        (StartPosition::Index(start), EndPosition::Index(end)) => {
            ((start as usize), (end as usize))
        }
    };

    let bytes = Bytes::copy_from_slice(&lock[start..end]);

    Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header(CONTENT_RANGE, format!("bytes {}-{}/{}", start, end, size))
        .header(CONTENT_LENGTH, bytes.len())
        .body(Body::from(bytes))
        .unwrap()
}

#[debug_handler]
async fn song(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    request: Request,
) -> Response {
    println!("Song: HEAD: {:?}", headers);
    println!("Request: {:?}", request);

    let lock = state.song2.read();

    if headers.contains_key(RANGE) {
        return song_partial(&headers, state.clone(), lock.len());
    }

    Response::builder()
        .header(ACCEPT_RANGES, "bytes")
        .header(CONTENT_LENGTH, lock.len())
        .body(Body::from(Bytes::copy_from_slice(&lock)))
        .unwrap()
}

fn parse_range_header(header: &HeaderValue, max: usize) -> (StartPosition, EndPosition) {
    if let Ok(value) = header.to_str() {
        match http_range_header::parse_range_header(value) {
            Ok(parsed) => (parsed.ranges[0].start, parsed.ranges[0].end),
            Err(_) => (StartPosition::Index(0), EndPosition::Index(max as u64)),
        }
    } else {
        (StartPosition::Index(0), EndPosition::Index(max as u64))
    }
}

#[debug_handler]
async fn artwork(headers: HeaderMap, State(state): State<Arc<AppState>>) -> Response {
    let lock = state.artwork2.read();
    let body = match lock.len() {
        0 => Body::from(get_default_artwork()),
        _ => Body::from(state.artwork2.read().to_vec()),
    };

    return Response::builder()
        .header("Content-Type", "application/octet-stream")
        .body(body)
        .unwrap();
}

#[derive(Debug, Clone)]
struct AppState {
    song: Arc<RwLock<Song>>,
    playlist: Arc<RwLock<Vec<Song>>>,
    timestamp: Arc<AtomicU64>,
    tx: broadcast::Sender<Command>,

    song2: Arc<RwLock<Bytes>>,
    artwork2: Arc<RwLock<Bytes>>,
    default_artwork: Arc<Vec<u8>>,
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
    Sync { ts: u64 },
}

#[derive(Serialize)]
pub enum EventKind {
    Song,
    Sync,
    Playlist,
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
    home_dir()
        .expect("aaaa")
        .join("mounts/nas/pony/music/albums/Pony Music Archive 21.03 (High Quality)")
}

#[debug_handler]
async fn ws_upgrader(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> Response {
    ws.on_upgrade(|s| ws_handler(s, state))
}

async fn serialize_and_send<T: Serialize>(
    kind: EventKind,
    payload: T,
    tx: &mut SplitSink<WebSocket, Message>,
) {
    let ev = serde_json::to_string(&Event { kind, payload });

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

    let song = state.song.read().clone();

    //send song
    let _ = serialize_and_send(EventKind::Song, song, &mut tx).await;

    //send sync
    let _ = serialize_and_send(
        EventKind::Sync,
        state.timestamp.load(Ordering::Relaxed),
        &mut tx,
    )
    .await;

    let pl = state.playlist.read().clone();
    let _ = serialize_and_send(EventKind::Playlist, pl, &mut tx).await;

    let mut sending = spawn(async move {
        loop {
            match listen.recv().await {
                Ok(Command::Shutdown) => break,
                Ok(Command::Playlist { items }) => {
                    serialize_and_send(EventKind::Playlist, items, &mut tx).await
                }
                Ok(Command::Song { song }) => {
                    serialize_and_send(EventKind::Song, song, &mut tx).await
                }
                Ok(Command::Sync { ts }) => serialize_and_send(EventKind::Sync, ts, &mut tx).await,
                Err(e) => println!("err {}", e),
            }
        }
    });

    //if socket errors, break
    let mut receiving = spawn(async move {
        loop {
            println!("received message");
            match rx.next().await {
                Some(Ok(Message::Close(_))) | Some(Err(_)) => break,
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

fn get_default_artwork() -> Vec<u8> {
    let current = current_dir().expect("failed to get current dir");
    let path = current.join("artwork/default.jpg");

    fs::read(path).unwrap().to_vec()
}

#[tokio::main]
async fn main() {
    ffmpeg_next::init().expect("eee");

    let (tx, _rx) = broadcast::channel(1000);
    let state = Arc::new(AppState {
        song: Arc::new(RwLock::new(Song::default())),
        playlist: Arc::new(RwLock::new(Vec::default())),
        timestamp: Arc::new(AtomicU64::new(0)),
        tx,

        song2: Arc::new(RwLock::new(Bytes::new())),
        artwork2: Arc::new(RwLock::new(Bytes::new())),
        default_artwork: Arc::new(get_default_artwork()),
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
