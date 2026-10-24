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
    pub playlist: Playlist,
    pub state: Arc<AppState>,
    /// The song that's playing; the same one `state` holds.
    pub now: Arc<CurrentSong>,
}

impl Radio {
    fn publish(&mut self, track: Arc<Track>, started: Instant) {
        self.now = Arc::new(CurrentSong { track, started });
        self.state.current.store(self.now.clone());

        let _ = self.state.broadcast.send(Event::song(&self.now).into());
    }

    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        loop {
            let end = self.now.started + Duration::from_millis(self.now.track.duration_ms);

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
                    () = sleep_until(end) => break,
                    _ = tick.tick() => {
                        let _ = self.state.broadcast.send(Event::sync(&self.now).into());
                    }
                    _ = shutdown.changed() => return,
                }
            }

            // A panicking loader is just another failed load
            let track = match preload.await.map_err(anyhow::Error::from).and_then(|r| r) {
                Ok(t) => Arc::new(t),
                Err(e) => {
                    eprintln!("[radio]: failed to load track, replaying current: {e}");
                    self.now.track.clone()
                }
            };

            self.publish(track, end);
        }
    }
}
