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
use serde::Serialize;
use tokio::time::Instant;
use walkdir::{DirEntry, WalkDir};

use crate::{AppState, Command, Song, song::AudioFormat};

#[derive(Clone, Serialize)]
pub struct Playlist {
    songs: Vec<Song>,
    #[serde(skip_serializing)]
    // timestamp in ms
    timestamp: i64,
    // current
    #[serde(skip_serializing)]
    cursor: usize,
}

impl Playlist {
    pub fn new(songs: Vec<Song>) -> Self {
        let mut playlist = Self {
            songs,
            cursor: 0,
            timestamp: 0,
        };

        playlist.shuffle();
        playlist
    }

    fn get_at_cursor(&self) -> &Song {
        self.songs
            .get(self.cursor)
            .expect("there's always one at cursor")
    }

    fn get_at_cursor_mut(&mut self) -> &mut Song {
        self.songs
            .get_mut(self.cursor)
            .expect("there's always one at cursor")
    }

    fn shuffle(&mut self) {
        self.songs.shuffle(&mut rand::rng());
    }

    fn load_next(&mut self, state: Arc<AppState>) {
        self.get_at_cursor_mut().unload();

        //if we're at the end, just cycle back
        if self.cursor == self.songs.len() - 1 {
            self.cursor = 0;
        } else {
            self.cursor += 1;
        }

        self.get_at_cursor_mut().load();
        self.timestamp = 0;

        *state.song.write() = self.get_at_cursor().clone();

        let _ = state.tx.send(Command::Song {
            song: self.get_at_cursor().clone(),
        });
    }

    pub async fn run(mut self, state: Arc<AppState>, shutdown: Arc<AtomicBool>) -> io::Result<()> {
        self.get_at_cursor_mut().load();

        *state.song.write() = self.get_at_cursor().clone();

        loop {
            let start = Instant::now();

            self.timestamp += 1;

            if self.timestamp >= self.get_at_cursor().duration() {
                self.load_next(state.clone())
            }

            //handle sync
            if self.timestamp > 0 && self.timestamp % 10 == 0 {
                let _ = state.tx.send(Command::Sync {
                    ts: self.timestamp as u64,
                });
            }

            if shutdown.load(Ordering::Relaxed) {
                break;
            }

            //loop usually runs in few microseconds, but in case it does we attempt to adjust for that
            let diff = 1000u128.saturating_sub(start.elapsed().as_millis());

            //and sleep the difference
            if diff > 0 {
                tokio::time::sleep(Duration::from_millis(diff as u64)).await;
            }
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
