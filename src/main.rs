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
            ACCEPT_RANGES, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG,
            IF_NONE_MATCH, IF_RANGE, RANGE,
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

/// Whether an `If-None-Match` header names `etag`, or is `*`.
fn etag_matches(header: Option<&HeaderValue>, etag: &str) -> bool {
    let Some(value) = header.and_then(|h| h.to_str().ok()) else {
        return false;
    };
    value
        .split(',')
        .map(|tag| tag.trim().trim_start_matches("W/"))
        .any(|tag| tag == etag || tag == "*")
}

async fn stream(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<SongQuery>,
) -> Response {
    if !state.has_session(&query.sid) {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    let now = state.current.load_full();

    if !now.is_current(&query.id) {
        return StatusCode::NOT_FOUND.into_response();
    }

    let track = &now.track;
    let len = track.bytes.len();

    // The length changes the tag too, in case a file is ever replaced in place
    let etag = format!("\"{}-{len}\"", track.id);
    if etag_matches(headers.get(IF_NONE_MATCH), &etag) {
        return (StatusCode::NOT_MODIFIED, [(ETAG, etag)]).into_response();
    }

    // A range of an older version of the file isn't a range of this one
    let range_header = match headers.get(IF_RANGE) {
        Some(if_range) if if_range.as_bytes() != etag.as_bytes() => None,
        _ => headers.get(RANGE),
    };

    // No Range header means the whole file; an invalid one is answered right away
    let (code, range) = match range_header.map(|h| parse_range(h, len)) {
        None => (StatusCode::OK, 0..len),
        Some(Some(range)) => (StatusCode::PARTIAL_CONTENT, range),
        Some(None) => {
            let unsatisfied = [(CONTENT_RANGE, format!("bytes */{len}"))];
            return (StatusCode::RANGE_NOT_SATISFIABLE, unsatisfied).into_response();
        }
    };

    let mut response = Response::builder()
        .status(code)
        .header(CONTENT_TYPE, track.content_type)
        .header(ACCEPT_RANGES, "bytes")
        .header(CACHE_CONTROL, "no-cache")
        .header(ETAG, &etag)
        .header(CONTENT_LENGTH, range.len());
    if code == StatusCode::PARTIAL_CONTENT {
        let content_range = format!("bytes {}-{}/{len}", range.start, range.end - 1);
        response = response.header(CONTENT_RANGE, content_range);
    }

    response
        .body(Body::from(track.bytes.slice(range)))
        .expect("headers are all valid")
}

async fn artwork(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ArtworkQuery>,
) -> Response {
    let now = state.current.load();
    if !now.is_current(&query.id) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let (body, content_type) = now
        .track
        .art
        .clone()
        .unwrap_or_else(|| state.default_artwork.clone());

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

async fn ws_handler(mut socket: WebSocket, state: Arc<AppState>) {
    let listen = state.broadcast.subscribe(); // subscribe before snapshotting
    let id = session_id();

    // Registered before the client learns the id, so /stream accepts it right away
    state.add_session(id.clone());
    // Every way out of the session (closed, error, server shutdown) ends here
    let _ = serve_session(&mut socket, &state, &id, listen).await;
    state.remove_session(&id);
}

async fn serve_session(
    socket: &mut WebSocket,
    state: &AppState,
    id: &str,
    mut listen: broadcast::Receiver<Message>,
) -> Result<(), axum::Error> {
    socket.send(Event::SessionId(id).into()).await?;
    let song: Message = Event::song(&state.current.load()).into();
    socket.send(song).await?;

    loop {
        let msg = select! {
            received = listen.recv() => match received {
                Ok(msg) => msg,
                // fell behind: resend a full snapshot instead of dropping the session
                Err(RecvError::Lagged(_)) => Event::song(&state.current.load()).into(),
                Err(RecvError::Closed) => return Ok(()),
            },
            incoming = socket.recv() => match incoming.transpose()? {
                Some(Message::Close(_)) | None => return Ok(()),
                Some(_) => continue, // clients don't send anything meaningful
            },
        };

        socket.send(msg).await?;
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
        .expect("playlist scan panicked");
    let first = song::load(&playlist.get_at_cursor())
        .map(Arc::new)
        .expect("can't load first track");

    let now = Arc::new(CurrentSong {
        started: Instant::now(),
        track: first,
    });

    let (tx, _) = broadcast::channel(64);
    let default_artwork = load_default_artwork(Path::new(&args.artwork));
    let state = Arc::new(AppState::new(now.clone(), tx, default_artwork));

    let (stop_tx, stop_rx) = watch::channel(false);
    let radio = Radio {
        playlist,
        state: state.clone(),
        now,
    };
    let radio = spawn(radio.run(stop_rx));

    let app = Router::new()
        .route("/version", get(version))
        .route("/stream", get(stream))
        .route(
            "/ws",
            any(
                |ws: WebSocketUpgrade, State(state): State<Arc<AppState>>| async move {
                    ws.on_upgrade(|socket| ws_handler(socket, state))
                },
            ),
        )
        .route("/artwork", get(artwork))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:9000")
        .await
        .expect("can't listen on 0.0.0.0:9000, is the port in use?");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .expect("server failed");

    let _ = stop_tx.send(true);
    let _ = radio.await;
}
