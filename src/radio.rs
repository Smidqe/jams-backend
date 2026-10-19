use crate::{
    event::Event,
    playlist::Playlist,
    song::{self, Track},
    state::{AppState, CurrentSong},
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
    now: Arc<CurrentSong>,
}

impl Radio {
    pub fn new(
        playlist: Playlist,
        current: Arc<Track>,
        started: Instant,
        state: Arc<AppState>,
    ) -> Self {
        let now = Arc::new(CurrentSong {
            track: current.clone(),
            started,
        });

        Self {
            playlist,
            current,
            started,
            state,
            now,
        }
    }

    fn publish(&mut self) {
        let s = Arc::new(CurrentSong {
            track: self.current.clone(),
            started: self.started,
        });

        self.state.current.store(s.clone());
        self.now = s;

        let _ = self.state.broadcast.send(Event::song(&self.now).into());
    }

    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        loop {
            let end = self.started + Duration::from_millis(self.current.duration_ms as u64);

            self.publish();
            self.playlist.advance();

            let next = self.playlist.get_at_cursor();
            let preload = spawn_blocking(move || song::load(&next));

            let mut tick = interval_at(
                //we start sync from 10s from now
                Instant::now() + Duration::from_secs(10),
                //at 10s intervals
                Duration::from_secs(10),
            );

            loop {
                select! {
                    _ = sleep_until(end) => break,
                    _ = tick.tick() => {
                        let _ = self.state.broadcast.send(Event::sync(&self.now).into());
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
