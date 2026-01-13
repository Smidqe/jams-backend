use std::{
    io,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use rand::seq::SliceRandom;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use tokio::time::Instant;
use walkdir::{DirEntry, WalkDir};

use crate::{AppState, Song, event::Event, song::AudioFormat};

pub enum LoadOp {
    Load,
    Unload,
}

pub struct Playlist {
    songs: Vec<Song>,
    current: Song,
    cursor: usize,
    timestamp: i64,
}

impl Playlist {
    pub fn new(mut songs: Vec<Song>) -> Self {
        songs.shuffle(&mut rand::rng());

        Self {
            current: songs.get(0).expect("").clone(),
            songs,
            timestamp: 0,
            cursor: 0,
        }
    }

    fn maybe_do_sync(&mut self, state: &Arc<AppState>) {
        if self.timestamp > 0 && self.timestamp % 10 == 0 {
            *state.timestamp.write() = self.timestamp;
            let _ = state.tx.send(Event::Sync);
        }
    }

    fn maybe_load_next(&mut self, state: &Arc<AppState>) {
        if self.timestamp < self.current.duration() {
            return;
        }

        if self.cursor == self.songs.len() - 1 {
            self.cursor = 0;
        } else {
            self.cursor += 1;
        }

        self.current = self.songs.get(self.cursor).expect("").clone();
        self.load_and_write_to_state(state);

        let _ = state.tx.send(Event::Song);
    }

    fn load_and_write_to_state(&mut self, state: &Arc<AppState>) {
        //load the current song
        self.current.load();
        self.timestamp = 0;

        //write things to state
        *state.timestamp.write() = self.timestamp;
        *state.current.write() = self.current.clone();
    }

    pub async fn run(mut self, state: Arc<AppState>, shutdown: Arc<AtomicBool>) -> io::Result<()> {
        self.load_and_write_to_state(&state);

        loop {
            let start = Instant::now();

            //handle sync
            self.maybe_load_next(&state);
            self.maybe_do_sync(&state);

            if shutdown.load(Ordering::Relaxed) {
                break;
            }

            //loop usually runs in few microseconds, but in case it does we attempt to adjust for that
            let diff = 1000u128.saturating_sub(start.elapsed().as_millis());

            //and sleep the difference
            if diff > 0 {
                tokio::time::sleep(Duration::from_millis(diff as u64)).await;
            }

            self.timestamp += 1;
        }

        Ok(())
    }
}

impl From<&Path> for Playlist {
    fn from(path: &Path) -> Self {
        let entries: Vec<DirEntry> = WalkDir::new(path)
            .into_iter()
            .filter_map(|entry| {
                if let Ok(e) = entry
                    && AudioFormat::from(e.path()) != AudioFormat::FMT_AUDIO_UNKNOWN
                {
                    Some(e)
                } else {
                    None
                }
            })
            .collect();

        if entries.len() == 0 {
            panic!("no songs at given path");
        }

        Self::new(
            entries
                .par_iter()
                .filter_map(|s| Song::try_from(s).ok())
                .collect(),
        )
    }
}

impl From<&str> for Playlist {
    fn from(value: &str) -> Self {
        Self::from(PathBuf::from(value))
    }
}

impl From<PathBuf> for Playlist {
    fn from(value: PathBuf) -> Self {
        Self::from(value.as_path())
    }
}
