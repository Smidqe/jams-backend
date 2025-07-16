use axum::body::Bytes;
use ffmpeg_next::{format::input, media::Type, Rational};
use serde::Serialize;
use std::{collections::HashMap, fs, path::PathBuf};

#[derive(Default, Debug, Clone, Serialize)]
pub struct Song {
    id: String,

    #[serde(skip_serializing)]
    path: PathBuf,
    duration: u128,
    tags: HashMap<String, String>,
}

impl Song {
    pub fn id(&self) -> &str {
        &self.id
    }

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

    pub fn get_file_bytes(&self) -> Bytes {
        Bytes::from(fs::read(&self.path).unwrap_or_default())
    }

    pub fn get_artwork_bytes(&self) -> Bytes {
        let mut fd = input(&self.path).expect("failed to load artwork");
        let frame = fd
            .packets()
            .find(|(s, _)| s.parameters().medium() == Type::Video);

        Bytes::from(
            frame
                .and_then(|(_, p)| p.data().map(|s| s.to_vec()))
                .unwrap_or_default(),
        )
    }

    fn calculate_duration(&mut self, duration: i64, time_base: Rational) -> u128 {
        (duration as f64 / time_base.denominator() as f64).ceil() as u128
    }

    pub fn load_tags_and_duration(&mut self) {
        if self.duration > 0 {
            return;
        }

        let fd = input(self.path()).unwrap();
        let stream = fd.streams().best(Type::Audio).unwrap();
        let tags = stream
            .metadata()
            .iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();

        self.tags = tags;
        self.duration = self.calculate_duration(stream.duration(), stream.time_base());
    }

    pub fn unload_tags(&mut self) {
        self.tags.clear();
    }

    pub fn load(&mut self) {
        self.load_tags_and_duration();
    }
}

impl From<&walkdir::DirEntry> for Song {
    fn from(dir: &walkdir::DirEntry) -> Self {
        Self {
            tags: HashMap::default(),
            id: blake3::hash(dir.path().to_string_lossy().as_bytes()).to_string(),
            path: dir.path().to_path_buf(),
            duration: 0,
        }
    }
}
