mod error;
mod playlist;
mod song;

use std::{
    fs,
    ops::Range,
    path::{Path, PathBuf},
    sync::{Arc, atomic::AtomicBool},
};

use axum::{
    Router,
    body::{Body, Bytes},
    debug_handler,
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{Message, Utf8Bytes, WebSocket},
    },
    http::{
        HeaderMap, StatusCode,
        header::{self, ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_TYPE, RANGE},
    },
    response::{IntoResponse, Response},
    routing::{any, get},
};

use clap::Parser;
use futures::{SinkExt, StreamExt, stream::SplitSink};
use http_range_header::{EndPosition, StartPosition, SyntacticallyCorrectRange};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tokio::{spawn, sync::broadcast, try_join};

use crate::{playlist::Playlist, song::Song};

pub struct RequestedRange {
    pub start: usize,
    pub end: usize,
}

#[debug_handler]
async fn version() -> Response {
    (StatusCode::NO_CONTENT, [("x-jams-version", "0.1")]).into_response()
}

fn get_numerical_range_from_range(range: &SyntacticallyCorrectRange, max: u64) -> Range<usize> {
    let (start, end) = match (range.start, range.end) {
        //-xxx
        (StartPosition::FromLast(start), EndPosition::LastByte) => (max - start, max),
        //xxx-
        (StartPosition::Index(start), EndPosition::LastByte) => (start, max),
        //xxx-yyy
        (StartPosition::Index(start), EndPosition::Index(end)) => (start, end),
        //there's theoretical FromLast, Index range, but that doesn't make any sense
        _ => unreachable!(),
    };

    (start as usize)..(end as usize)
}

fn get_range_from_header(headers: &HeaderMap, size: u64) -> Range<usize> {
    let header = headers.get(RANGE).expect("range check missing");
    let presentation = header.to_str().expect("range header should be well formed");

    match http_range_header::parse_range_header(presentation) {
        //TODO: Support multipart ranges
        Ok(r) => get_numerical_range_from_range(&r.ranges[0], size),
        Err(_) => 0..0,
    }
}

async fn authenticate_session(headers: HeaderMap, State(state): State<Arc<AppState>>) -> Response {
    //ensure we have a proper header
    //ensure we have

    Response::default()
}

fn song_partial(bytes: Bytes, headers: HeaderMap) -> Response {
    let length = bytes.len() as u64;
    let parsed_range = get_range_from_header(&headers, length);
    let content_range = format!(
        "bytes {}-{}/{}",
        parsed_range.start, parsed_range.end, length
    );

    (
        StatusCode::PARTIAL_CONTENT,
        [
            (header::CONTENT_RANGE, content_range),
            (header::CONTENT_LENGTH, length.to_string()),
        ],
        Body::from(bytes.slice(parsed_range)),
    )
        .into_response()
}

#[derive(Deserialize)]
struct AAA {
    id: String,
}

#[debug_handler]
async fn song(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    query: Query<AAA>,
) -> Response {
    if !query.id.eq_ignore_ascii_case(state.song.read().id()) {
        return (StatusCode::NOT_FOUND).into_response();
    }

    let bytes = state.song.read().bytes();

    if headers.contains_key(RANGE) {
        return song_partial(bytes, headers);
    }

    (
        StatusCode::OK,
        [
            (ACCEPT_RANGES, "bytes"),
            (CONTENT_LENGTH, &bytes.len().to_string()),
        ],
        Body::from(bytes),
    )
        .into_response()
}

#[debug_handler]
async fn artwork(State(state): State<Arc<AppState>>) -> Response {
    let bytes = state.song.read().coverart();
    let body = if bytes.is_empty() {
        Body::from(Bytes::copy_from_slice(&state.default_artwork))
    } else {
        Body::from(bytes)
    };

    (
        StatusCode::OK,
        [(CONTENT_TYPE, "application/octet-stream")],
        body,
    )
        .into_response()
}

#[derive(Debug, Clone)]
struct AppState {
    song: Arc<RwLock<Song>>,
    tx: broadcast::Sender<Command>,
    default_artwork: Arc<Bytes>,
}

#[derive(Debug, Clone)]
pub enum Command {
    Shutdown,
    Song { song: Song },
    Sync { ts: u64 },
    Ping,
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

    let mut sending = spawn(async move {
        loop {
            match listen.recv().await {
                Ok(Command::Shutdown) => break,
                Ok(Command::Song { song }) => {
                    serialize_and_send(EventKind::Song, song, &mut tx).await
                }
                Ok(Command::Sync { ts }) => serialize_and_send(EventKind::Sync, ts, &mut tx).await,
                Ok(Command::Ping) => {
                    let _ = tx.send(Message::Ping(Bytes::new())).await;
                }
                Err(e) => println!("err {}", e),
            }
        }
    });

    //if socket errors, break
    let mut receiving = spawn(async move {
        loop {
            match rx.next().await {
                Some(Ok(Message::Close(_))) => {
                    println!("[ws]: Received a normal close");
                    break;
                }
                Some(Err(e)) => {
                    println!("[ws]: Connection ended with an error {}", e);
                    break;
                }
                Some(Ok(m)) => match m {
                    Message::Pong(_) => (),
                    _ => println!("[ws]: Got a message of {:?}", m),
                },
                None => (),
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

fn get_default_artwork(path: &Path) -> Bytes {
    Bytes::from(fs::read(path).expect("can't function without default artwork"))
}

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long)]
    artwork: String,
    #[arg(short, long)]
    source: String,
}

#[tokio::main]
async fn main() {
    ffmpeg_next::init().expect("eee");

    let args = Args::parse();

    let source = PathBuf::try_from(args.source.as_str()).expect("source folder does not exist");
    let artwork_path = PathBuf::try_from(args.artwork.as_str()).expect("artwork does not exist");

    let (tx, _rx) = broadcast::channel(1000);
    let state = Arc::new(AppState {
        song: Arc::new(RwLock::new(Song::default())),
        tx,

        default_artwork: Arc::new(get_default_artwork(artwork_path.as_path())),
    });

    let shutdown = Arc::new(AtomicBool::new(false));
    let playlist = spawn(Playlist::from(source.as_path()).run(state.clone(), shutdown.clone()));

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
