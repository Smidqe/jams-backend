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

use crate::{AppState, Command, ShareableSong, Song};

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

    pub fn get_at_cursor(&self) -> &Song {
        self.songs
            .get(self.cursor)
            .expect("there's always one at cursor")
    }

    pub fn get_at_cursor_mut(&mut self) -> &mut Song {
        self.songs
            .get_mut(self.cursor)
            .expect("there's always one at cursor")
    }

    pub fn shuffle(&mut self) {
        self.songs.shuffle(&mut rand::rng());
    }

    pub fn advance(&mut self) {
        self.get_at_cursor_mut().unload_tags();

        //if we're at the end, just cycle back
        if self.cursor == self.songs.len() - 1 {
            self.cursor = 0;
        } else {
            self.cursor += 1;
        }

        self.get_at_cursor_mut().load_tags_and_duration();

        //reset timestamp
        self.timestamp = 0;
    }

    pub fn tick(&mut self) -> bool {
        self.timestamp += 1;

        let advance = self.timestamp >= self.get_at_cursor().duration();

        if advance {
            self.advance();
        }

        advance
    }

    fn write_to_state(&self, state: Arc<AppState>, size: usize) {
        let song = self.get_at_cursor();
        let playlist = self.get_songs_wrapping(size);

        *state.song.write() = song.clone();
        *state.shared_song.write() = ShareableSong {
            artwork: song.get_artwork_bytes(),
            content: song.get_file_bytes(),
        };

        *state.playlist.write() = playlist.clone();

        let _ = state.tx.send(Command::Song { song: song.clone() });
        let _ = state.tx.send(Command::Playlist { items: playlist });
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

    pub async fn run(mut self, state: Arc<AppState>, shutdown: Arc<AtomicBool>) -> io::Result<()> {
        self.preload_next(20);
        self.write_to_state(state.clone(), 20);

        loop {
            state
                .timestamp
                .store(self.timestamp as u64, Ordering::SeqCst);

            if self.tick() {
                //wait few seconds to account for sync drift
                tokio::time::sleep(Duration::from_secs(3)).await;

                self.preload_next(20);
                self.write_to_state(state.clone(), 20);
            }

            //handle sync
            if self.timestamp > 0 && self.timestamp % 5 == 0 {
                let _ = state.tx.send(Command::Sync {
                    ts: self.timestamp as u64,
                });
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
        let walker = WalkDir::new(path).follow_links(true).into_iter();
        let is_valid_entry = |e: &DirEntry| {
            !e.file_name()
                .to_str()
                .map(|s| s.starts_with("."))
                .unwrap_or(false)
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
