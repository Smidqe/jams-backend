use crate::{
    event::{Event, EventKind},
    song::Track,
};
use arc_swap::ArcSwap;
use axum::extract::ws::Message;
use serde::Serialize;
use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::{sync::broadcast, time::Instant};

pub struct NowPlaying {
    pub current: Arc<Track>,
    pub started: Instant,
}

impl NowPlaying {
    pub fn position_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    pub fn is_current(&self, id: &str) -> bool {
        self.current.id == id
    }
}

pub struct AppState {
    pub now_playing: ArcSwap<NowPlaying>,
    pub broadcast: broadcast::Sender<Message>,
    pub sessions: scc::HashSet<String>,
}

pub fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

#[derive(Serialize)]
struct SongEvent<'a> {
    id: &'a str,
    duration_ms: u128,
    tags: &'a HashMap<String, String>,
    position_ms: u64,
    server_time_ms: u128,
}

#[derive(Serialize)]
pub struct SyncEvent<'a> {
    pub id: &'a str,
    pub position_ms: u64,
    pub server_time_ms: u128,
}

pub fn song_msg(song: &NowPlaying) -> Message {
    let (now, pos) = (unix_ms(), song.position_ms());

    Event::new_as_message(
        EventKind::Song,
        SongEvent {
            id: &song.current.id,
            duration_ms: song.current.duration_ms,
            tags: &song.current.tags,
            position_ms: pos,
            server_time_ms: now,
        },
    )
}

/// Everything a joining (or lagged) client needs to be fully in sync.
pub fn snapshot_msgs(s: &NowPlaying) -> Vec<Message> {
    vec![song_msg(s)]
}
