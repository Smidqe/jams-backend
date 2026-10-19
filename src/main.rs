mod event;
mod playlist;
mod radio;
mod song;
mod state;

use std::{
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
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
use futures::{SinkExt, StreamExt};
use lofty::picture::Picture;
use nanoid::nanoid;
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
    if !state.sessions.contains_sync(&query.sid) {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    let state_copy = state.current.load_full();

    if !state_copy.is_current(&query.id) {
        return StatusCode::NOT_FOUND.into_response();
    }

    let track = &state_copy.track;
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
    let mut listen = state.broadcast.subscribe();
    let id = nanoid!();

    let events = vec![
        Event::SessionId(&id).into(),
        Event::song(&state.current.load()).into(),
    ];

    for ev in events {
        if socket.send(ev).await.is_err() {
            return;
        }
    }

    let _ = state.sessions.insert_async(id.clone()).await;

    let (mut tx, mut rx) = socket.split();
    let st2 = state.clone();

    let mut sending = spawn(async move {
        loop {
            let msgs = match listen.recv().await {
                Ok(m) => vec![m],
                // fell behind: resend a full snapshot instead of dropping the session
                Err(RecvError::Lagged(_)) => vec![Event::song(&st2.current.load()).into()],
                Err(_) => return,
            };

            for m in msgs {
                if tx.send(m).await.is_err() {
                    return;
                }
            }
        }
    });

    let mut receiving = spawn(async move {
        while let Some(Ok(msg)) = rx.next().await {
            if matches!(msg, Message::Close(_)) {
                break;
            }
        }
    });

    select! {
        _ = &mut sending => receiving.abort(),
        _ = &mut receiving => sending.abort(),
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
    let args = Args::parse();
    let root = PathBuf::from(&args.source);
    let playlist = tokio::task::spawn_blocking(move || Playlist::scan(&root))
        .await
        .unwrap();
    let first = song::load(&playlist.get_at_cursor())
        .map(Arc::new)
        .expect("can't load first track");

    let started = Instant::now();
    let (tx, _) = broadcast::channel(64);
    let state = Arc::new(AppState {
        current: ArcSwap::from_pointee(CurrentSong {
            track: first.clone(),
            started,
        }),
        broadcast: tx,
        sessions: scc::HashSet::new(),
        default_artwork: load_default_artwork(Path::new(&args.artwork)),
    });

    let (stop_tx, stop_rx) = watch::channel(false);
    let radio = spawn(Radio::new(playlist, first, started, state.clone()).run(stop_rx));

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
