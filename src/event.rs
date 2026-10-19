use std::collections::HashMap;

use axum::extract::ws::{Message, Utf8Bytes};
use serde::Serialize;

use crate::state::{NowPlaying, unix_ms};

#[derive(Serialize)]
pub struct SongEvent<'a> {
    pub id: &'a str,
    pub duration_ms: u128,
    pub tags: &'a HashMap<String, String>,
    pub position_ms: u64,
    pub server_time_ms: u128,
}

#[derive(Serialize)]
pub struct SyncEvent<'a> {
    pub id: &'a str,
    pub position_ms: u64,
    pub server_time_ms: u128,
}

#[derive(Serialize)]
#[serde(tag = "kind", content = "payload")]
pub enum Event<'a> {
    SessionId(&'a str),
    Song(SongEvent<'a>),
    Sync(SyncEvent<'a>),
}

impl Event<'_> {
    pub fn song(now: &NowPlaying) -> Event<'_> {
        Event::Song(SongEvent {
            id: &now.current.id,
            duration_ms: now.current.duration_ms,
            tags: &now.current.tags,
            position_ms: now.position_ms(),
            server_time_ms: unix_ms(),
        })
    }

    pub fn sync(now: &NowPlaying) -> Event<'_> {
        Event::Sync(SyncEvent {
            id: &now.current.id,
            position_ms: now.position_ms(),
            server_time_ms: unix_ms(),
        })
    }
}

impl From<Event<'_>> for Message {
    fn from(event: Event<'_>) -> Self {
        Message::Text(Utf8Bytes::from(
            serde_json::to_string(&event).expect("this should never fail"),
        ))
    }
}
