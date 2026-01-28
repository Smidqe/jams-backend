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
        HeaderMap, StatusCode,
        header::{
            ACCEPT_RANGES, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE,
        },
    },
    response::{IntoResponse, Response},
    routing::{any, get},
};

use crate::{playlist::Playlist, song::Song};
use clap::Parser;
use futures::{SinkExt, StreamExt};
use http_range_header::{EndPosition, StartPosition, SyntacticallyCorrectRange};
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
async fn version() -> Response {
    (StatusCode::NO_CONTENT, [("x-jams-version", "0.1")]).into_response()
}

fn get_numerical_range_from_range(range: &SyntacticallyCorrectRange, max: u64) -> Range<usize> {
    let max_index = max - 1;
    let (start, end) = match (range.start, range.end) {
        //-xxx
        (StartPosition::FromLast(start), EndPosition::LastByte) => (max_index - start, max_index),
        //xxx-
        (StartPosition::Index(start), EndPosition::LastByte) => (start, max_index),
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

    println!("range: {}", presentation);

    match http_range_header::parse_range_header(presentation) {
        //TODO: Support multipart ranges
        Ok(r) => get_numerical_range_from_range(&r.ranges[0], size),
        Err(_) => 0..0,
    }
}

fn song_partial(bytes: Bytes, headers: HeaderMap, content_type: &str) -> Response {
    let length = bytes.len() as u64;
    let parsed_range = get_range_from_header(&headers, length);
    let content_range = format!(
        "bytes {}-{}/{}",
        parsed_range.start, parsed_range.end, length
    );

    println!("{:?}", headers);
    println!("{:?}", parsed_range);
    println!("{:?}", content_range);

    let slice = bytes.slice(parsed_range);
    let size = slice.len();
    let body = Body::from(slice);

    println!("size {size}");

    (
        StatusCode::PARTIAL_CONTENT,
        [
            (ACCEPT_RANGES, "bytes".to_string()),
            (CONTENT_RANGE, content_range),
            (CONTENT_TYPE, "audio/ogg".to_string()),
            (CONTENT_LENGTH, size.to_string()),
        ],
        body,
    )
        .into_response()
}

#[debug_handler]
async fn song(headers: HeaderMap, State(state): State<Arc<AppState>>) -> Response {
    let bytes = state.now.read().bytes();
    let content_type = state.now.read().get_content_type().to_string();

    if headers.contains_key(RANGE) {
        return song_partial(bytes, headers, content_type.as_str());
    }

    (
        StatusCode::OK,
        [
            (ACCEPT_RANGES, "bytes"),
            (CONTENT_TYPE, "audio/ogg"),
            (CACHE_CONTROL, "no-cache"),
            (CONTENT_LENGTH, &bytes.len().to_string()),
        ],
        Body::from(bytes),
    )
        .into_response()
}

#[debug_handler]
async fn artwork(State(state): State<Arc<AppState>>) -> Response {
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
        .into_response()
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
                Some(Ok(m)) => match m {
                    Message::Pong(_) | _ => (),
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
    DEFAULT_ARTWORK
        .load_from_path(&PathBuf::try_from(args.artwork.as_str()).expect("artwork does not exist"));

    let (tx, _) = broadcast::channel(1000);
    let song_folder =
        PathBuf::try_from(args.source.as_str()).expect("source folder does not exist");

    let state = Arc::new(AppState {
        now: Arc::new(RwLock::new(Song::default())),
        broadcast: tx,
        playlist: Arc::new(RwLock::new(Vec::new())),
    });

    let shutdown = Arc::new(AtomicBool::new(false));
    let playlist = spawn(Playlist::from(song_folder).run(state.clone(), shutdown.clone()));

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
    shutdown.store(true, Ordering::Relaxed);

    let _ = try_join!(playlist);
}
