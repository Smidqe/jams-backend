use crate::song::Track;
use arc_swap::ArcSwap;
use axum::extract::ws::Message;
use std::sync::Arc;
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
