mod event;
mod playlist;
mod song;

use std::{
    fs,
    ops::Range,
    path::{Path, PathBuf},
    str::FromStr,
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
        State, WebSocketUpgrade,
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

use crate::{playlist::Playlist, song::Song};
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
        if let Some(bytes) = self.0.get() {
            Bytes::copy_from_slice(bytes)
        } else {
            Bytes::new()
        }
    }
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
async fn song2(headers: HeaderMap, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let song = state.now.read();
    let mut response_headers = HeaderMap::new();

    response_headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_str(song.get_content_type()).unwrap(),
    );

    let (status, length, body) = if let Some(range) = headers.get(RANGE) {
        let (header_range, byte_range) =
            match get_header_and_byte_ranges(range, song.content_length()) {
                Ok(s) => s,
                Err(_) => return (StatusCode::BAD_REQUEST, HeaderMap::new(), Body::empty()),
            };

        let slice = song.bytes_slice(byte_range);

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

        (StatusCode::PARTIAL_CONTENT, slice.len(), Body::from(slice))
    } else {
        (
            StatusCode::OK,
            song.content_length(),
            Body::from(song.bytes()),
        )
    };

    response_headers.insert(CONTENT_LENGTH, HeaderValue::from(length));
    response_headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    response_headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));

    (status, response_headers, body)
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
}

#[debug_handler]
async fn ws_upgrader(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> Response {
    ws.on_upgrade(|s| ws_handler(s, state))
}

async fn ws_handler(mut socket: WebSocket, state: Arc<AppState>) {
    if socket.send(Message::Ping(Bytes::new())).await.is_err() {
        return;
    }

    let (mut tx, mut rx) = socket.split();
    let mut listen = state.broadcast.subscribe();

    let mut sending = spawn(async move {
        let msg = state.now.read().as_message();

        if let Err(e) = tx.send(msg).await {
            println!("{}", e)
        }

        loop {
            let ev: Message = match listen.recv().await {
                Ok(msg) => msg,
                _ => continue,
            };

            if let Err(e) = tx.send(ev).await {
                eprintln!("[ws]: Failed to broadcast a message: {}", e);
            }
        }
    });

    //if socket errors, break
    let mut receiving = spawn(async move {
        loop {
            match rx.next().await {
                Some(Ok(Message::Close(_))) => {
                    break;
                }
                Some(Err(e)) => {
                    println!("[ws]: Connection ended with an error {}", e);
                    break;
                }
                _ => (),
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
    });

    let shutdown = Arc::new(AtomicBool::new(false));
    let playlist = spawn(Playlist::from(song_folder).run(state.clone(), shutdown.clone()));

    let shutdown_ping = shutdown.clone();
    let state_ping = state.clone();

    let ping = spawn(async move {
        loop {
            if shutdown_ping.load(Ordering::Relaxed) {
                break;
            }

            tokio::time::sleep(Duration::from_secs(10)).await;

            let _ = state_ping.broadcast.send(Message::Ping(Bytes::new()));
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
