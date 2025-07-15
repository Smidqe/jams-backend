use std::{
    io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use axum::body::Bytes;
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
        let mut playlist = Self {
            songs,
            cursor: 0,
            timestamp: 0,
        };

        playlist.shuffle();

        if let Some(song) = playlist.songs.get_mut(playlist.cursor) {
            song.load();
        } else {
            println!("no song")
        }

        playlist
    }

    pub fn shuffle(&mut self) {
        self.songs.shuffle(&mut rand::rng());
    }

    pub fn advance(&mut self) {
        println!("advance");
        if let Some(song) = self.songs.get_mut(self.cursor) {
            song.unload();
        }

        self.cursor += 1;

        if self.cursor >= self.songs.len() {
            self.cursor = 0;
        }

        if let Some(song) = self.songs.get_mut(self.cursor) {
            song.load();
        }

        self.timestamp = 0;
    }

    pub fn tick(&mut self) -> bool {
        self.timestamp += 1;

        let advance = match &self.songs.get(self.cursor) {
            None => false,
            Some(s) => self.timestamp >= s.duration(),
        };

        println!(
            "now: {} s / song: {:?} s",
            self.timestamp,
            self.songs.get(self.cursor).unwrap().duration()
        );

        if advance {
            self.advance();
        }

        advance
    }

    fn write_current_to_state(&self, state: Arc<AppState>) {
        if let Some(song) = self.songs.get(self.cursor) {
            //TODO: This is expensive, keep serialized JSON in state insteads
            *state.song.write() = song.clone();
            *state.song2.write() = Bytes::copy_from_slice(song.bytes());
            *state.artwork2.write() = Bytes::copy_from_slice(song.artwork());
        }
    }

    fn write_song_to_socket(&self, state: Arc<AppState>) {
        let song = self.songs.get(self.cursor).unwrap();
        let _ = state.tx.send(Command::Song { song: song.clone() });
    }

    fn write_sync_to_socket(&self, state: Arc<AppState>) {
        let _ = state.tx.send(Command::Sync {
            ts: self.timestamp as u64,
        });
    }

    fn get_songs_wrapping(&self, count: usize) -> Vec<Song> {
        self.songs
            .iter()
            .cycle()
            .skip(self.cursor)
            .take(count)
            .cloned()
            .collect()
    }

    fn preload_next(&mut self, count: usize) {
        let song_count = self.songs.len();

        //load unless
        self.songs
            .iter_mut()
            .skip(self.cursor)
            .take(count)
            .for_each(|s| s.load());

        if song_count - self.cursor < count {
            self.songs
                .iter_mut()
                .take(song_count - self.cursor)
                .for_each(|s| s.load());
        }
    }

    fn write_playlist_to_state(&self, state: Arc<AppState>, size: usize, send: bool) {
        //BUG: When wrapping, the songs are not loaded
        let items = self.get_songs_wrapping(size);

        *state.playlist.write() = items.clone();

        if send {
            let _ = state.tx.send(Command::Playlist { items });
        }
    }

    pub async fn run(mut self, state: Arc<AppState>, shutdown: Arc<AtomicBool>) -> io::Result<()> {
        self.preload_next(10);

        self.write_current_to_state(state.clone());
        self.write_playlist_to_state(state.clone(), 10, false);

        loop {
            state
                .timestamp
                .store(self.timestamp as u64, Ordering::SeqCst);

            if self.tick() {
                tokio::time::sleep(Duration::from_secs(1)).await;

                self.write_current_to_state(state.clone());
                self.write_song_to_socket(state.clone());

                self.preload_next(10);
                self.write_playlist_to_state(state.clone(), 10, true);
            }

            //handle sync
            if self.timestamp > 0 && self.timestamp % 5 == 0 {
                self.write_sync_to_socket(state.clone());
            }

            if shutdown.load(Ordering::Relaxed) {
                break;
            }

            //TODO: This has a drift, calculate spent time from start of loop to this point and use from_millis
            //this is not generally a problem, but if there are much longer tracks then there's unnecessary jumping due to syncs
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
