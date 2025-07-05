use std::{
    io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use rand::seq::SliceRandom;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use walkdir::{DirEntry, WalkDir};

use crate::{AppState, Command, Song};

pub struct Playlist {
    songs: Vec<Song>,
    // timestamp in ms
    timestamp: u128,
    // current
    cursor: usize,
}

impl Playlist {
    pub fn new(songs: Vec<Song>) -> Self {
        Self {
            songs,
            cursor: 0,
            timestamp: 0,
        }
    }

    pub fn shuffle(&mut self) {
        self.songs.shuffle(&mut rand::rng());
    }

    pub fn advance(&mut self) {
        self.cursor += 1;

        if self.cursor >= self.songs.len() {
            self.cursor = 0;
        }

        self.timestamp = 0;
    }

    pub fn tick(&mut self) -> bool {
        self.timestamp += 1000000;

        let advance = match &self.songs.get(self.cursor) {
            None => false,
            Some(s) => self.timestamp >= s.duration(),
        };

        if advance {
            self.advance();
        }

        advance
    }

    fn write_current_to_state(&self, state: Arc<AppState>) {
        *state.song.write() = self.songs.get(self.cursor).unwrap().as_songlet();
    }

    fn write_song_to_socket(&self, state: Arc<AppState>) {
        let song = self.songs.get(self.cursor).unwrap();
        let _ = state.tx.send(Command::Song { song: song.clone() });
    }

    pub async fn run(mut self, state: Arc<AppState>, shutdown: Arc<AtomicBool>) -> io::Result<()> {
        self.write_current_to_state(state.clone());
        self.write_song_to_socket(state.clone());

        loop {
            if self.tick() {
                self.write_current_to_state(state.clone());
                self.write_song_to_socket(state.clone());
            }

            if shutdown.load(Ordering::Relaxed) {
                break;
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        Ok(())
    }
}

impl From<&Path> for Playlist {
    fn from(path: &Path) -> Self {
        let walker = WalkDir::new(path).into_iter();
        let is_valid_entry = |e: &DirEntry| {
            let is_traversal = e
                .file_name()
                .to_str()
                .map(|s| s.starts_with("."))
                .unwrap_or(false);

            !is_traversal
        };

        let entries: Vec<DirEntry> = walker
            .filter_entry(is_valid_entry)
            .flatten()
            .filter(|e| e.file_type().is_file())
            .collect();

        Self::new(entries.par_iter().map(Song::from).collect())
    }
}

impl From<PathBuf> for Playlist {
    fn from(value: PathBuf) -> Self {
        Self::from(value.as_path())
    }
}
