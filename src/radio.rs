use crate::{
    event::Event,
    playlist::Playlist,
    song,
    state::{AppState, CurrentSong},
};
use std::{sync::Arc, time::Duration};
use tokio::{
    select,
    sync::watch,
    task::spawn_blocking,
    time::{Instant, interval_at, sleep_until},
};

/// How many songs the upcoming list shows.
const UPCOMING: usize = 20;
const SYNC: Duration = Duration::from_secs(10);

/// The song at the cursor, starting now, and the ones after it. Blocking.
pub fn first(playlist: &Playlist) -> anyhow::Result<CurrentSong> {
    let upcoming = playlist.upcoming(UPCOMING).map(song::peek).collect();
    let track = Arc::new(song::load(&playlist.get_at_cursor())?);
    Ok(CurrentSong {
        track,
        started: Instant::now(),
        upcoming,
    })
}

pub struct Radio {
    pub playlist: Playlist,
    pub state: Arc<AppState>,
    /// The song that's playing; the same one `state` holds.
    pub now: Arc<CurrentSong>,
}

impl Radio {
    fn publish(&mut self, now: CurrentSong) {
        self.now = Arc::new(now);
        self.state.current.store(self.now.clone());

        let _ = self.state.broadcast.send(Event::song(&self.now).into());
    }

    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        loop {
            let end = self.now.started + Duration::from_millis(self.now.track.duration_ms);

            self.playlist.advance();

            let next = self.playlist.get_at_cursor();
            // The list moves up by one, so usually only its new last song needs reading
            let kept = self.now.upcoming.len().saturating_sub(1);
            let missing: Vec<_> = self
                .playlist
                .upcoming(UPCOMING)
                .skip(kept)
                .cloned()
                .collect();
            let preload = spawn_blocking(move || {
                let peeked: Vec<_> = missing.iter().map(song::peek).collect();
                (song::load(&next), peeked)
            });

            let mut tick = interval_at(Instant::now() + SYNC, SYNC);

            loop {
                select! {
                    () = sleep_until(end) => break,
                    _ = tick.tick() => {
                        let _ = self.state.broadcast.send(Event::sync(&self.now).into());
                    }
                    _ = shutdown.changed() => return,
                }
            }

            // A panic is just another failed load; the next change fills the list up again
            let (loaded, tail) = match preload.await {
                Ok(done) => done,
                Err(e) => (Err(e.into()), Vec::new()),
            };
            let track = match loaded {
                Ok(t) => Arc::new(t),
                Err(e) => {
                    eprintln!("[radio]: failed to load track, replaying current: {e}");
                    self.now.track.clone()
                }
            };

            let upcoming: Arc<[_]> = self
                .now
                .upcoming
                .iter()
                .skip(1)
                .cloned()
                .chain(tail)
                .collect();

            self.publish(CurrentSong {
                track,
                started: end,
                upcoming,
            });
        }
    }
}
