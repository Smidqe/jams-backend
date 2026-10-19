use crate::{
    event::{Event, EventKind},
    playlist::Playlist,
    song::{self, Track},
    state::{AppState, NowPlaying, SyncEvent, song_msg, unix_ms},
};
use std::{sync::Arc, time::Duration};
use tokio::{
    select,
    sync::watch,
    task::spawn_blocking,
    time::{Instant, interval_at, sleep_until},
};

pub struct Radio {
    playlist: Playlist,
    current: Arc<Track>,
    started: Instant,
    state: Arc<AppState>,
}

impl Radio {
    pub fn get_loaded_track(pl: &Playlist) -> anyhow::Result<Track> {
        song::load(&pl.get_at_cursor())
    }

    pub fn new(
        playlist: Playlist,
        current: Arc<Track>,
        started: Instant,
        state: Arc<AppState>,
    ) -> Self {
        Self {
            playlist,
            current,
            started,
            state,
        }
    }

    fn publish(&self) {
        let s = Arc::new(NowPlaying {
            current: self.current.clone(),
            started: self.started,
        });

        self.state.now_playing.store(s.clone());

        let _ = self.state.broadcast.send(song_msg(&s));
    }

    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        loop {
            let end = self.started + Duration::from_millis(self.current.duration_ms as u64);

            self.publish();
            self.playlist.advance();

            let next = self.playlist.get_at_cursor();
            let preload = spawn_blocking(move || song::load(&next));

            let mut tick = interval_at(
                Instant::now() + Duration::from_secs(10),
                Duration::from_secs(10),
            );

            loop {
                select! {
                    _ = sleep_until(end) => break,
                    _ = tick.tick() => {
                        let _ = self.state.broadcast.send(Event::new_as_message(
                            EventKind::Sync,
                            SyncEvent {
                                id: &self.current.id,
                                position_ms: self.started.elapsed().as_millis() as u64,
                                server_time_ms: unix_ms(),
                            },
                        ));
                    }
                    _ = shutdown.changed() => return,
                }
            }

            match preload.await {
                Ok(Ok(t)) => self.current = Arc::new(t),
                Ok(Err(e)) => eprintln!("[radio]: failed to load track, replaying current: {e}"),
                Err(e) => eprintln!("[radio]: loader panicked, replaying current: {e}"),
            }

            self.started = end; // exact boundary, no drift
        }
    }
}
