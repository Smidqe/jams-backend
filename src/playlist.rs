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

use crate::{
    AppState, Song,
    event::{Event, EventKind},
};

pub struct Playlist {
    songs: Vec<Song>,
    current: Song,
    cursor: usize,
    timestamp: i64,
}

impl Playlist {
    pub fn new(mut songs: Vec<Song>) -> Self {
        //shuffle first
        songs.shuffle(&mut rand::rng());

        //load the first song
        let mut song = songs
            .get_mut(0)
            .expect("we should always have atleast one")
            .clone();

        song.load();

        Self {
            current: song,
            songs,
            cursor: 0,
            timestamp: 0,
        }
    }

    fn advance_cursor(&mut self) {
        if self.cursor == self.songs.len() - 1 {
            self.cursor = 0;
        } else {
            self.cursor += 1;
        }

        self.timestamp = 0;
    }

    pub fn get_next_songs(&self, count: usize) -> Vec<Song> {
        self.songs.iter().cycle().take(count).cloned().collect()
    }

    fn write_current_to_state(&self, state: &Arc<AppState>) {
        *state.now.write() = self.current.clone()
    }

    fn write_playlist_to_state(&self, state: &Arc<AppState>) {
        *state.playlist.write() = self.songs.clone()
    }

    fn load_song_at_cursor(&mut self) {
        self.current = self.songs.get(self.cursor).expect("").clone();
        self.current.load();
    }

    fn maybe_do_sync(&self, state: &Arc<AppState>) {
        if self.timestamp < 0 || self.timestamp % 10 != 0 {
            return;
        }

        self.broadcast_sync(state);
    }

    fn maybe_load_next(&mut self) -> bool {
        if self.timestamp < self.current.duration() {
            return false;
        }

        self.advance_cursor();
        self.load_song_at_cursor();

        true
    }

    fn broadcast_sync(&self, state: &Arc<AppState>) {
        let _ = state
            .broadcast
            .send(Event::new_as_message(EventKind::Sync, self.timestamp));
    }

    fn broadcast_song(&self, state: &Arc<AppState>) {
        let _ = state.broadcast.send(self.current.clone().as_message());

        //TODO: Broadcast next <x> songs
        // let _ = state.broadcast(Event::new(EventKind::Upcoming, self.get_next_songs()).into())
    }

    pub async fn run(mut self, state: Arc<AppState>, shutdown: Arc<AtomicBool>) -> io::Result<()> {
        println!("running playlist");

        self.write_current_to_state(&state);
        self.write_playlist_to_state(&state);

        self.timestamp = 0;

        loop {
            let start = Instant::now();

            if self.maybe_load_next() {
                tokio::time::sleep(Duration::from_secs(1)).await;

                self.write_current_to_state(&state);
                self.broadcast_song(&state);
            }

            self.maybe_do_sync(&state);

            if shutdown.load(Ordering::Relaxed) {
                break;
            }

            tokio::time::sleep(Duration::from_millis(
                1000u128.saturating_sub(start.elapsed().as_millis()) as u64,
            ))
            .await;

            self.timestamp += 1;
        }

        Ok(())
    }
}

impl From<&Path> for Playlist {
    fn from(path: &Path) -> Self {
        let entries: Vec<DirEntry> = WalkDir::new(path)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .collect();

        if entries.is_empty() {
            panic!("[playlist]: No songs in {:?}", &path);
        }

        let songs: Vec<Song> = entries
            .par_iter()
            .filter_map(|s| Song::try_from(s).ok())
            .collect();

        println!("[playlist]: Amount of songs found: {}", songs.len());

        Self::new(songs)
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
