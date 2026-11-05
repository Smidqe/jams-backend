use crate::song::{Image, Track, Upcoming};
use arc_swap::ArcSwap;
use axum::extract::ws::Message;
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};
use tokio::{sync::broadcast, time::Instant};

pub struct CurrentSong {
    pub track: Arc<Track>,
    pub started: Instant,
    /// The songs after this one, in play order.
    pub upcoming: Arc<[Upcoming]>,
}

impl CurrentSong {
    pub fn position_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }
}

pub struct AppState {
    pub current: ArcSwap<CurrentSong>,
    pub broadcast: broadcast::Sender<Message>,
    /// Only touched through the methods below, which never hold the lock
    /// across an `.await`, so a std mutex is fine.
    sessions: Mutex<HashSet<String>>,
    pub default_artwork: Image,
}

impl AppState {
    pub fn new(
        current: Arc<CurrentSong>,
        broadcast: broadcast::Sender<Message>,
        default_artwork: Image,
    ) -> Self {
        Self {
            current: ArcSwap::new(current),
            broadcast,
            sessions: Mutex::default(),
            default_artwork,
        }
    }

    /// The current song, if `id` is its id.
    pub fn current_if(&self, id: &str) -> Option<Arc<CurrentSong>> {
        Some(self.current.load_full()).filter(|now| now.track.id == id)
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
