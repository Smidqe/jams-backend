use std::{os::unix::ffi::OsStrExt, path::Path, sync::Arc};

use rand::seq::SliceRandom;
use walkdir::WalkDir;

use crate::song::{SongFile, content_type_for};

pub struct Playlist {
    pub list: Arc<[SongFile]>,
    pub cursor: usize,
}

impl Playlist {
    pub fn scan(root: &Path) -> Self {
        let mut v: Vec<SongFile> = WalkDir::new(root)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_file())
            .filter_map(|e| {
                let ct = content_type_for(e.path().extension()?.to_str()?)?;
                let id = blake3::hash(e.path().as_os_str().as_bytes())
                    .to_hex()
                    .to_string();
                Some(SongFile {
                    path: e.into_path(),
                    id,
                    content_type: ct,
                })
            })
            .collect();
        assert!(!v.is_empty(), "no songs found");

        v.shuffle(&mut rand::rng());
        println!("[playlist]: {} songs", v.len());

        Self {
            list: v.into(),
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
