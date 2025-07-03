use std::path::{Path, PathBuf};

use rand::seq::SliceRandom;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use walkdir::{DirEntry, WalkDir};

use crate::Song;

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

    pub fn current_cloned(&self) -> Option<Song> {
        self.songs.get(self.cursor).cloned()
    }

    pub fn advance(&mut self) {
        //offload current
        if let Some(now) = self.songs.get_mut(self.cursor) {
            now.offload();
        }

        self.cursor += 1;

        if self.cursor >= self.songs.len() {
            self.cursor = 0;
        }

        if let Some(now) = self.songs.get_mut(self.cursor) {
            now.load();
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
