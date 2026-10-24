use crate::song::Track;
use arc_swap::ArcSwap;
use axum::{body::Bytes, extract::ws::Message, http::HeaderValue};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};
use tokio::{sync::broadcast, time::Instant};

pub struct CurrentSong {
    pub track: Arc<Track>,
    pub started: Instant,
}

impl CurrentSong {
    pub fn position_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }

    pub fn is_current(&self, id: &str) -> bool {
        self.track.id == id
    }
}

pub struct AppState {
    pub current: ArcSwap<CurrentSong>,
    pub broadcast: broadcast::Sender<Message>,
    /// Only touched through the methods below, which never hold the lock
    /// across an `.await`, so a std mutex is fine.
    sessions: Mutex<HashSet<String>>,
    pub default_artwork: (Bytes, HeaderValue),
}

impl AppState {
    pub fn new(
        current: Arc<CurrentSong>,
        broadcast: broadcast::Sender<Message>,
        default_artwork: (Bytes, HeaderValue),
    ) -> Self {
        Self {
            current: ArcSwap::new(current),
            broadcast,
            sessions: Mutex::default(),
            default_artwork,
        }
    }

    pub fn add_session(&self, id: String) {
        self.sessions.lock().unwrap().insert(id);
    }

    pub fn remove_session(&self, id: &str) {
        self.sessions.lock().unwrap().remove(id);
    }

    pub fn has_session(&self, id: &str) -> bool {
        self.sessions.lock().unwrap().contains(id)
    }
}
