use std::path::Path;

use rand::seq::SliceRandom;
use walkdir::WalkDir;

use crate::song::SongFile;

pub struct Playlist {
    list: Box<[SongFile]>,
    cursor: usize,
}

impl Playlist {
    pub fn scan(root: &Path) -> Self {
        let mut songs: Vec<SongFile> = WalkDir::new(root)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_file())
            .filter_map(|entry| SongFile::from_path(entry.into_path()))
            .collect();

        assert!(!songs.is_empty(), "no songs found");

        songs.shuffle(&mut rand::rng());
        println!("[playlist]: {} songs", songs.len());

        Self {
            list: songs.into_boxed_slice(),
            cursor: 0,
        }
    }

    pub fn advance(&mut self) {
        self.cursor = (self.cursor + 1) % self.list.len();
    }

    pub fn get_at_cursor(&self) -> SongFile {
        self.list[self.cursor].clone()
    }
}
