use ffmpeg_next::{format::input, media::Type};
use serde::Serialize;
use std::{collections::HashMap, fs, path::PathBuf};

use crate::Songlet;

#[derive(Default, Debug, Clone, Serialize)]
pub struct Song {
    id: String,

    #[serde(skip_serializing)]
    path: PathBuf,
    duration: u128,
    tags: HashMap<String, String>,
}

impl Song {
    pub fn name(&self) -> String {
        if let Some(title) = self.tags.get("title") {
            title.to_string()
        } else {
            String::from("[unknown]")
        }
    }

    pub fn duration(&self) -> u128 {
        self.duration
    }

    pub fn tags(&self) -> &HashMap<String, String> {
        &self.tags
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn as_songlet(&self) -> Songlet {
        let bytes = fs::read(&self.path).unwrap_or_default();
        let mut fd = input(&self.path).expect("failed to load artwork");
        let frame = fd
            .packets()
            .find(|(s, _)| s.parameters().medium() == Type::Video);

        Songlet {
            artwork: frame
                .and_then(|(_, p)| p.data().map(|s| s.to_vec()))
                .unwrap_or_default(),
            bytes,
        }
    }
}

impl From<&walkdir::DirEntry> for Song {
    fn from(dir: &walkdir::DirEntry) -> Self {
        let path = dir.path();
        let bytes = fs::read(path).unwrap_or_default();
        let hash = blake3::hash(bytes.as_slice());

        let fd = input(&path).expect("msg");
        let stream = fd.streams().best(Type::Audio).unwrap();
        let tags = stream
            .metadata()
            .iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();

        Self {
            tags,
            id: hash.to_string(),
            path: path.to_path_buf(),
            duration: fd.duration() as u128,
        }
    }
}
