use axum::body::Bytes;
use ffmpeg_next::{format::input, media::Type, Rational};
use serde::Serialize;
use std::{collections::HashMap, fs, path::PathBuf};

pub struct ShareableSong {
    content: Bytes,
    artwork: Bytes,
}

#[derive(Default, Debug, Clone, Serialize)]
pub struct Song {
    id: String,

    #[serde(skip_serializing)]
    path: PathBuf,

    #[serde(skip_serializing)]
    bytes: Vec<u8>,
    #[serde(skip_serializing)]
    artwork: Vec<u8>,

    duration: u128,
    tags: Option<HashMap<String, String>>,
}

impl Song {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn name(&self) -> String {
        match &self.tags {
            Some(tags) => {
                if let Some(title) = tags.get("title") {
                    title.to_string()
                } else {
                    String::from("[unknown]")
                }
            }
            None => String::from("[unknown]"),
        }
    }

    pub fn duration(&self) -> u128 {
        self.duration
    }

    pub fn tags(&self) -> Option<&HashMap<String, String>> {
        self.tags.as_ref()
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn artwork(&self) -> &[u8] {
        &self.artwork
    }

    fn load_artwork(&mut self) {
        if !self.artwork.is_empty() {
            return;
        }

        //TODO: Large artworks fail on mobile side, so could try and shrink them by ourselves
        // eg. /home/smidqe/mounts/nas/pony/music/albums/Pony Music Archive 21.03 (High Quality)/Artists/Likonan/WoodenToaster - Pinkie's Lie (Likonan Cover Instrumental).opus
        // which has Stream #0:1: Video: png, rgba(pc, gbr/bt709/unknown), 6180x6180 [SAR 2835:2835 DAR 1:1], 90k tbr, 90k tbn (attached pic) :|

        let mut fd = input(&self.path).expect("failed to load artwork");
        let frame = fd
            .packets()
            .find(|(s, _)| s.parameters().medium() == Type::Video);

        self.artwork = frame
            .and_then(|(_, p)| p.data().map(|s| s.to_vec()))
            .unwrap_or_default();
    }

    fn load_bytes(&mut self) {
        if !self.bytes.is_empty() {
            return;
        }

        self.bytes = fs::read(&self.path).unwrap_or_default();
    }

    fn calculate_duration(&mut self, duration: i64, time_base: Rational) -> u128 {
        (duration as f64 / time_base.denominator() as f64).ceil() as u128
    }

    fn load_tags_and_duration(&mut self) {
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

        self.tags = Some(tags);
        self.duration = self.calculate_duration(stream.duration(), stream.time_base());
    }

    pub fn load(&mut self) {
        self.load_artwork();
        self.load_bytes();
        self.load_tags_and_duration();
    }

    pub fn unload(&mut self) {
        self.artwork.clear();
        self.bytes.clear();
    }
}

impl From<&walkdir::DirEntry> for Song {
    fn from(dir: &walkdir::DirEntry) -> Self {
        Self {
            tags: None,
            id: blake3::hash(dir.path().to_string_lossy().as_bytes()).to_string(),
            path: dir.path().to_path_buf(),
            duration: 0,
            bytes: Vec::default(),
            artwork: Vec::default(),
        }
    }
}
