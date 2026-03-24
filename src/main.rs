mod event;
mod playlist;
mod song;

use std::{
    fs,
    ops::Range,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::{Body, Bytes},
    debug_handler,
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{
        HeaderMap, HeaderValue, StatusCode,
        header::{
            ACCEPT_RANGES, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE,
        },
    },
    response::{IntoResponse, Response},
    routing::{any, get},
};
use nanoid::nanoid;
use scc::HashSet;
use serde::Deserialize;

use crate::{
    event::{Event, EventKind},
    playlist::Playlist,
    song::Song,
};
use anyhow::Result;
use clap::Parser;
use futures::{SinkExt, StreamExt};
use http_range_header::{EndPosition, StartPosition};
use once_cell::sync::OnceCell;
use parking_lot::RwLock;
use tokio::{spawn, sync::broadcast, try_join};

pub struct Artwork(OnceCell<Bytes>);

impl Artwork {
    pub const fn new() -> Artwork {
        Artwork(OnceCell::new())
    }

    pub fn load_from_path(&self, path: &Path) {
        self.0.get_or_init(|| {
            Bytes::from(fs::read(path).expect("can't function without default artwork"))
        });
    }

    pub fn get(&self) -> Bytes {
        self.0
            .get()
            .map(|bytes| Bytes::copy_from_slice(bytes))
            .unwrap_or_default()
    }
}

impl Default for Artwork {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Deserialize)]
struct SongQuery {
    id: String,
    sid: String,
}

static DEFAULT_ARTWORK: Artwork = Artwork::new();

#[debug_handler]
async fn version() -> impl IntoResponse {
    (StatusCode::NO_CONTENT, [("x-jams-version", "0.1")])
}

fn get_header_and_byte_ranges(
    range: &HeaderValue,
    length: usize,
) -> Result<(Range<u64>, Range<usize>)> {
    let range = http_range_header::parse_range_header(range.to_str()?)?.ranges[0];

    match (range.start, range.end) {
        //xxx-
        (StartPosition::Index(start), EndPosition::LastByte) => {
            Ok((start..(length - 1) as u64, (start as usize)..length))
        }
        //xxx-yyy
        (StartPosition::Index(start), EndPosition::Index(end)) => {
            Ok((start..end, start as usize..end as usize))
        }
        _ => unreachable!(),
    }
}

#[debug_handler]
async fn song2(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    query: Query<SongQuery>,
) -> Response {
    let song = state.now.read();
    let mut response_headers = HeaderMap::new();
    let mut response_code = StatusCode::OK;

    response_headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str(song.get_content_type()).unwrap(),
    );

    //ensure request matches, and we have open session
    if query.id != song.id() || !state.sessions.contains_sync(&query.sid) {
        return (StatusCode::UNAUTHORIZED).into_response();
    }

    //get the content and content length
    //if invalid range was given we fallback to sending 200 and whole file
    let (bytes, length) = match headers
        .get(RANGE)
        .and_then(|range| get_header_and_byte_ranges(range, song.content_length()).ok())
    {
        Some((header_range, content_range)) => {
            response_code = StatusCode::PARTIAL_CONTENT;
            response_headers.insert(
                CONTENT_RANGE,
                HeaderValue::from_str(&format!(
                    "bytes {}-{}/{}",
                    header_range.start,
                    header_range.end,
                    song.content_length()
                ))
                .unwrap(),
            );

            song.bytes_slice_with_len(content_range)
        }
        None => song.bytes_slice_with_len(..),
    };

    response_headers.insert(CONTENT_LENGTH, HeaderValue::from(length));
    response_headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    response_headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));

    (response_code, response_headers, Body::from(bytes)).into_response()
}

#[debug_handler]
async fn artwork(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let bytes = state.now.read().coverart();

    let body = if bytes.is_empty() {
        Body::from(DEFAULT_ARTWORK.get())
    } else {
        Body::from(bytes)
    };

    (
        StatusCode::OK,
        [(CONTENT_TYPE, "application/octet-stream")],
        body,
    )
}

#[derive(Clone)]
struct AppState {
    now: Arc<RwLock<Song>>,
    broadcast: broadcast::Sender<Message>,
    playlist: Arc<RwLock<Vec<Song>>>,
    sessions: Arc<scc::HashSet<String>>,
}

#[debug_handler]
async fn ws_upgrader(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> Response {
    ws.on_upgrade(|s| ws_handler(s, state))
}

async fn ws_handler(mut socket: WebSocket, state: Arc<AppState>) {
    if socket.send(Message::Ping(Bytes::new())).await.is_err() {
        return;
    }

    let id = nanoid!();
    let events = vec![
        Event::new_as_message(EventKind::SessionId, id.clone()),
        Event::new_as_message(EventKind::Song, state.now.read().clone()),
    ];

    let _ = state.sessions.insert_async(id.clone()).await;

    for ev in events {
        if socket.send(ev).await.is_err() {
            return;
        }
    }

    let (mut tx, mut rx) = socket.split();
    let mut listen = state.broadcast.subscribe();

    let mut sending = spawn(async move {
        while let Ok(msg) = listen.recv().await {
            let _ = tx.send(msg).await;
        }
    });

    let mut receiving = spawn(async move {
        while let Some(Ok(msg)) = rx.next().await {
            if matches!(msg, Message::Close(_)) {
                break;
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

    state.sessions.remove_async(&id).await;
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
    DEFAULT_ARTWORK.load_from_path(&PathBuf::from(args.artwork.as_str()));

    let (tx, _) = broadcast::channel(1000);
    let song_folder = PathBuf::from(args.source.as_str());

    let state = Arc::new(AppState {
        now: Arc::new(RwLock::new(Song::default())),
        broadcast: tx,
        playlist: Arc::new(RwLock::new(Vec::new())),
        sessions: Arc::new(HashSet::new()),
    });

    let shutdown = Arc::new(AtomicBool::new(false));
    let playlist = spawn(Playlist::from(song_folder).run(state.clone(), shutdown.clone()));

    let shutdown_ping = shutdown.clone();
    let state_ping = state.clone();

    let ping = spawn(async move {
        loop {
            let _ = state_ping.broadcast.send(Message::Ping(Bytes::new()));

            if shutdown_ping.load(Ordering::Relaxed) {
                break;
            }

            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });

    let app = Router::new()
        .route("/version", get(version))
        .route("/stream", get(song2))
        .route("/ws", any(ws_upgrader))
        .route("/artwork", get(artwork))
        .with_state(state);

    axum::serve(
        tokio::net::TcpListener::bind("0.0.0.0:8080").await.unwrap(),
        app,
    )
    .await
    .unwrap();
    shutdown.store(true, Ordering::Relaxed);

    let _ = try_join!(playlist, ping);
}
