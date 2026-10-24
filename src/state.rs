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
    /// Never held across an `.await`, so a std mutex is fine.
    pub sessions: Mutex<HashSet<String>>,
    pub default_artwork: (Bytes, HeaderValue),
}
