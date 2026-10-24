mod event;
mod playlist;
mod radio;
mod song;
mod state;

use std::{
    ops::Range,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use arc_swap::ArcSwap;
use axum::{
    Router,
    body::{Body, Bytes},
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
use clap::Parser;
use lofty::picture::Picture;
use rand::{Rng, distr::Alphanumeric};
use serde::Deserialize;
use tokio::{
    select, spawn,
    sync::{broadcast, broadcast::error::RecvError, watch},
    time::Instant,
};

use crate::{
    event::Event,
    playlist::Playlist,
    radio::Radio,
    song::image_content_type,
    state::{AppState, CurrentSong},
};

fn load_default_artwork(path: &Path) -> (Bytes, HeaderValue) {
    let bytes = std::fs::read(path).expect("can't function without default artwork");
    let picture = Picture::from_reader(&mut bytes.as_slice())
        .expect("default artwork must be a PNG, JPEG, TIFF, BMP or GIF");
    (Bytes::from(bytes), image_content_type(picture.mime_type()))
}

#[derive(Deserialize)]
struct SongQuery {
    id: String,
    sid: String,
}

#[derive(Deserialize)]
struct ArtworkQuery {
    id: String,
}

async fn version() -> impl IntoResponse {
    (StatusCode::NO_CONTENT, [("x-jams-version", "0.1")])
}

/// TODO: This only considers first range, if multi range is given rest are ignored, probably not an issue though
fn parse_range(header: &HeaderValue, len: usize) -> Option<Range<usize>> {
    let v = http_range_header::parse_range_header(header.to_str().ok()?)
        .ok()?
        .validate(len as u64)
        .ok()?;
    let range = v.first()?;

    //bytes is always +1 to end
    Some(*range.start() as usize..*range.end() as usize + 1)
}

async fn stream(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<SongQuery>,
) -> Response {
    if !state.sessions.lock().unwrap().contains(&query.sid) {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    let now = state.current.load_full();

    if !now.is_current(&query.id) {
        return StatusCode::NOT_FOUND.into_response();
    }

    let track = &now.track;
    let len = track.bytes.len();

    let mut response_headers = HeaderMap::new();
    response_headers.insert(CONTENT_TYPE, HeaderValue::from_static(track.content_type));
    response_headers.insert(ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    response_headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-cache"));

    let (code, body) = match headers.get(RANGE) {
        None => (StatusCode::OK, track.bytes.clone()),
        Some(range_header) => match parse_range(range_header, len) {
            Some(range) => {
                response_headers.insert(
                    CONTENT_RANGE,
                    format!("bytes {}-{}/{}", range.start, range.end - 1, len)
                        .parse()
                        .unwrap(),
                );
                (StatusCode::PARTIAL_CONTENT, track.bytes.slice(range))
            }
            None => {
                response_headers.insert(CONTENT_RANGE, format!("bytes */{len}").parse().unwrap());
                return (StatusCode::RANGE_NOT_SATISFIABLE, response_headers).into_response();
            }
        },
    };
    response_headers.insert(CONTENT_LENGTH, HeaderValue::from(body.len()));
    (code, response_headers, Body::from(body)).into_response()
}

async fn artwork(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ArtworkQuery>,
) -> Response {
    let now = state.current.load();
    if !now.is_current(&query.id) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let track = &now.track;
    let (body, content_type) = if track.art.is_empty() {
        state.default_artwork.clone()
    } else {
        (track.art.clone(), track.art_type.clone())
    };

    (
        [
            (CONTENT_TYPE, content_type),
            (
                CACHE_CONTROL,
                HeaderValue::from_static("public, max-age=86400, immutable"),
            ),
        ],
        body,
    )
        .into_response()
}

async fn ws_upgrader(ws: WebSocketUpgrade, State(st): State<Arc<AppState>>) -> Response {
    ws.on_upgrade(|s| ws_handler(s, st))
}

async fn ws_handler(mut socket: WebSocket, state: Arc<AppState>) {
    let listen = state.broadcast.subscribe(); // subscribe before snapshotting
    let id = session_id();

    // Registered before the client learns the id, so /stream accepts it right away
    state.sessions.lock().unwrap().insert(id.clone());
    serve_session(&mut socket, &state, &id, listen).await;
    state.sessions.lock().unwrap().remove(&id);
}

async fn serve_session(
    socket: &mut WebSocket,
    state: &AppState,
    id: &str,
    mut listen: broadcast::Receiver<Message>,
) {
    let first: [Message; 2] = [
        Event::SessionId(id).into(),
        Event::song(&state.current.load()).into(),
    ];
    for msg in first {
        if socket.send(msg).await.is_err() {
            return;
        }
    }

    loop {
        let msg = select! {
            received = listen.recv() => match received {
                Ok(msg) => msg,
                // fell behind: resend a full snapshot instead of dropping the session
                Err(RecvError::Lagged(_)) => Event::song(&state.current.load()).into(),
                Err(RecvError::Closed) => return,
            },
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Close(_)) | Err(_)) | None => return,
                Some(Ok(_)) => continue, // clients don't send anything meaningful
            },
        };

        if socket.send(msg).await.is_err() {
            return;
        }
    }
}

/// 21 random alphanumeric characters from a cryptographically secure RNG.
fn session_id() -> String {
    rand::rng()
        .sample_iter(Alphanumeric)
        .take(21)
        .map(char::from)
        .collect()
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
    let args = Args::parse();
    let root = PathBuf::from(&args.source);
    let playlist = tokio::task::spawn_blocking(move || Playlist::scan(&root))
        .await
        .unwrap();
    let first = song::load(&playlist.get_at_cursor())
        .map(Arc::new)
        .expect("can't load first track");

    let now = Arc::new(CurrentSong {
        started: Instant::now(),
        track: first.clone(),
    });

    let (tx, _) = broadcast::channel(64);
    let state = Arc::new(AppState {
        current: ArcSwap::new(now.clone()),
        broadcast: tx,
        sessions: Mutex::default(),
        default_artwork: load_default_artwork(Path::new(&args.artwork)),
    });

    let (stop_tx, stop_rx) = watch::channel(false);
    let radio = spawn(Radio::new(playlist, state.clone(), now).run(stop_rx));

    let app = Router::new()
        .route("/version", get(version))
        .route("/stream", get(stream))
        .route("/ws", any(ws_upgrader))
        .route("/artwork", get(artwork))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:9000").await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .unwrap();

    let _ = stop_tx.send(true);
    let _ = radio.await;
}
