use ffmpeg_next::{format::input, media::Type, Rational};
use serde::Serialize;
use std::{collections::HashMap, fs, path::PathBuf};

use crate::Songlet;

#[derive(Default, Debug, Clone, Serialize)]
pub struct Song {
    id: String,

    #[serde(skip_serializing)]
    path: PathBuf,

    #[serde(skip_serializing)]
    bytes: Option<Vec<u8>>,
    #[serde(skip_serializing)]
    artwork: Option<Vec<u8>>,

    duration: Option<u128>,
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

    pub fn duration(&self) -> Option<&u128> {
        self.duration.as_ref()
    }

    pub fn tags(&self) -> Option<&HashMap<String, String>> {
        self.tags.as_ref()
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn as_songlet(&self) -> Songlet {
        let artwork = match self.artwork.as_ref() {
            Some(a) => a.clone(),
            None => Vec::new(),
        };

        Songlet {
            artwork: artwork,
            bytes: self.bytes.as_ref().unwrap().clone(),
        }
    }

    fn load_artwork(&mut self) {
        if self.artwork.is_some() {
            println!("Song {} already has artwork loaded", self.path().display());
            return;
        }

        let mut fd = input(&self.path).expect("failed to load artwork");
        let frame = fd
            .packets()
            .find(|(s, _)| s.parameters().medium() == Type::Video);

        self.artwork = frame.and_then(|(_, p)| p.data().map(|s| s.to_vec()));

        if self.artwork.is_none() || self.artwork.as_ref().is_some_and(|s| s.len() == 0) {
            eprintln!("No artwork found for song at {}", self.path.display());
        }
    }

    fn load_bytes(&mut self) {
        self.bytes = fs::read(&self.path).ok();
    }

    fn calculate_duration(&mut self, duration: i64, time_base: Rational) -> u128 {
        (duration as f64 / time_base.denominator() as f64).ceil() as u128
    }

    fn load_tags_and_duration(&mut self) {
        if self.duration.is_some() {
            return;
        }

        let fd = input(self.path()).unwrap();
        let stream = fd.streams().best(Type::Audio).unwrap();
        let tags = stream
            .metadata()
            .iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();

        println!(
            "Duration of {}: {}, {}",
            self.path.display(),
            stream.duration(),
            stream.time_base()
        );

        self.tags = Some(tags);
        self.duration = Some(self.calculate_duration(stream.duration(), stream.time_base()));
    }

    pub fn load(&mut self) {
        self.load_artwork();
        self.load_bytes();
        self.load_tags_and_duration();
    }

    pub fn unload(&mut self) {
        self.artwork = None;
        self.bytes = None;
        self.tags = None;
    }
}

impl From<&walkdir::DirEntry> for Song {
    fn from(dir: &walkdir::DirEntry) -> Self {
        Self {
            tags: None,
            id: blake3::hash(dir.path().to_string_lossy().as_bytes()).to_string(),
            path: dir.path().to_path_buf(),
            duration: None,
            bytes: None,
            artwork: None,
        }
    }
}
