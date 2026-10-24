use std::{
    collections::HashMap,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::extract::ws::{Message, Utf8Bytes};
use serde::Serialize;

use crate::state::CurrentSong;

/// A `Sync` plus what the client needs to show the song.
#[derive(Serialize)]
pub struct SongEvent<'a> {
    #[serde(flatten)]
    pub sync: SyncEvent<'a>,
    pub duration_ms: u64,
    pub tags: &'a HashMap<String, String>,
}

#[derive(Serialize)]
pub struct SyncEvent<'a> {
    pub id: &'a str,
    pub position_ms: u64,
    pub server_time_ms: u64,
}

#[derive(Serialize)]
#[serde(tag = "kind", content = "payload")]
pub enum Event<'a> {
    SessionId(&'a str),
    Song(SongEvent<'a>),
    Sync(SyncEvent<'a>),
}

impl Event<'_> {
    pub fn song(now: &CurrentSong) -> Event<'_> {
        Event::Song(SongEvent {
            sync: SyncEvent::new(now),
            duration_ms: now.track.duration_ms,
            tags: &now.track.tags,
        })
    }

    pub fn sync(now: &CurrentSong) -> Event<'_> {
        Event::Sync(SyncEvent::new(now))
    }
}

impl SyncEvent<'_> {
    fn new(now: &CurrentSong) -> SyncEvent<'_> {
        SyncEvent {
            id: &now.track.id,
            position_ms: now.position_ms(),
            server_time_ms: unix_ms(),
        }
    }
}

impl From<Event<'_>> for Message {
    fn from(event: Event<'_>) -> Self {
        Message::Text(Utf8Bytes::from(
            serde_json::to_string(&event).expect("this should never fail"),
        ))
    }
}

pub fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is after 1970")
        .as_millis() as u64
}
