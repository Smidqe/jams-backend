mod event;
mod playlist;
mod radio;
mod song;
mod state;

use std::{
    ops::{Bound, Range},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use axum::{
    Router,
    body::Body,
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, HeaderValue, StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
    routing::{any, get},
};
use clap::Parser;
use headers::{
    AcceptRanges, CacheControl, ContentLength, ContentRange, ETag, HeaderMapExt, IfNoneMatch,
    IfRange,
};
use lofty::picture::Picture;
use nanoid::nanoid;
use serde::Deserialize;
use tokio::{
    select, spawn,
    sync::{broadcast, broadcast::error::RecvError, watch},
    task::spawn_blocking,
};

use crate::{event::Event, playlist::Playlist, radio::Radio, song::Image, state::AppState};

fn load_default_artwork(path: &Path) -> Image {
    let bytes = std::fs::read(path).expect("can't function without default artwork");
    let picture = Picture::from_reader(&mut bytes.as_slice())
        .expect("default artwork must be a PNG, JPEG, TIFF, BMP or GIF");
    song::image(&picture)
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

/// The first range of a `Range` header within a file of `len` bytes, or `None` if
/// it can't be satisfied. Only the first is used; audio players ask for one at a time.
/// `headers` doesn't check ranges against the length, so that's done here: an end
/// past the file means "to the end", a start past it can't be served.
fn first_range(header: &headers::Range, len: usize) -> Option<Range<usize>> {
    let (start, end) = header.satisfiable_ranges(len as u64).next()?;
    let start = match start {
        Bound::Included(s) => s as usize,
        _ => 0,
    };
    let end = match end {
        Bound::Included(e) => (e as usize).saturating_add(1).min(len),
        _ => len,
    };
    (start < end).then_some(start..end)
}

async fn stream(
    headers: HeaderMap,
    State(state): State<Arc<AppState>>,
    Query(query): Query<SongQuery>,
) -> Response {
    if !state.has_session(&query.sid) {
        return StatusCode::UNAUTHORIZED.into_response();
    }

    let Some(now) = state.current_if(&query.id) else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let track = &now.track;
    let len = track.bytes.len();

    // The length changes the tag too, in case a file is ever replaced in place
    let etag: ETag = format!("\"{}-{len}\"", track.id)
        .parse()
        .expect("ids are hex, so the tag is valid");
    let mut out = HeaderMap::new();
    out.typed_insert(etag.clone());

    let not_modified = headers.typed_get::<IfNoneMatch>();
    if not_modified.is_some_and(|h| !h.precondition_passes(&etag)) {
        return (StatusCode::NOT_MODIFIED, out).into_response();
    }

    // A range of an older version of the file isn't a range of this one
    let range = match headers.typed_get::<IfRange>() {
        Some(if_range) if if_range.is_modified(Some(&etag), None) => None,
        _ => headers.typed_get::<headers::Range>(),
    };

    // No Range header means the whole file; an unsatisfiable one is answered right away
    let (code, range) = match range.map(|r| first_range(&r, len)) {
        None => (StatusCode::OK, 0..len),
        Some(Some(range)) => (StatusCode::PARTIAL_CONTENT, range),
        Some(None) => {
            out.typed_insert(ContentRange::unsatisfied_bytes(len as u64));
            return (StatusCode::RANGE_NOT_SATISFIABLE, out).into_response();
        }
    };

    if code == StatusCode::PARTIAL_CONTENT {
        let bytes = range.start as u64..range.end as u64;
        out.typed_insert(ContentRange::bytes(bytes, len as u64).expect("range is bounded"));
    }
    out.typed_insert(AcceptRanges::bytes());
    out.typed_insert(CacheControl::new().with_no_cache());
    out.typed_insert(ContentLength(range.len() as u64));
    out.insert(CONTENT_TYPE, HeaderValue::from_static(track.content_type));

    (code, out, Body::from(track.bytes.slice(range))).into_response()
}

async fn artwork(
    State(state): State<Arc<AppState>>,
    Query(query): Query<ArtworkQuery>,
) -> Response {
    let Some(now) = state.current_if(&query.id) else {
        return StatusCode::NOT_FOUND.into_response();
    };

    let (body, content_type) = now
        .track
        .art
        .clone()
        .unwrap_or_else(|| state.default_artwork.clone());

    let mut out = HeaderMap::new();
    out.insert(CONTENT_TYPE, content_type);
    out.typed_insert(
        CacheControl::new()
            .with_public()
            .with_max_age(Duration::from_secs(86400))
            .with_immutable(),
    );
    (out, body).into_response()
}

async fn ws_handler(mut socket: WebSocket, state: Arc<AppState>) {
    let listen = state.broadcast.subscribe(); // subscribe before snapshotting
    // 21 URL-safe characters (A-Z, a-z, 0-9, `_`, `-`) from a cryptographically secure RNG
    let id = nanoid!();

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
    socket
        .send(Event::song(&state.current.load()).into())
        .await?;

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

#[derive(Parser, Debug)]
struct Args {
    /// Default artwork image, for songs without a cover
    #[arg(short, long, env = "JAMS_ARTWORK")]
    artwork: String,
    /// Music library folder
    #[arg(short, long, env = "JAMS_SOURCE")]
    source: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let root = PathBuf::from(&args.source);
    let (playlist, now) = spawn_blocking(move || {
        let playlist = Playlist::scan(&root);
        let now = radio::first(&playlist).expect("can't load first track");
        (playlist, Arc::new(now))
    })
    .await
    .expect("startup panicked");

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
