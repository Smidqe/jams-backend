use axum::{body::Bytes, extract::ws::Message};
use ffmpeg_next::{format::input, media::Type};
use serde::Serialize;
use std::{
    collections::HashMap,
    fs,
    ops::RangeBounds,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
};

use crate::event::{Event, EventKind};

#[derive(Default, Clone, Debug, Serialize)]
pub struct Song {
    #[serde(skip_serializing)]
    path: PathBuf,
    duration: i64,
    #[serde(skip_serializing)]
    bytes: Bytes,
    #[serde(skip_serializing)]
    coverart: Bytes,
    tags: HashMap<String, String>,
    #[serde(skip_serializing)]
    content_type: String,
    id: String,
}

impl Song {
    pub fn load(&mut self) {
        //read the file as a whole
        self.bytes = Bytes::from(fs::read(&self.path).unwrap_or_default());

        //open it in ffmpeg
        let mut fd = input(&self.path).expect("file was (re)moved");

        //get coverart if it exists
        if let Some((_, packet)) = fd
            .packets()
            .find(|(s, _)| s.parameters().medium() == Type::Video)
        {
            self.coverart = Bytes::copy_from_slice(packet.data().unwrap_or_default());
        }

        //we must get tags from the audio source
        if let Some(stream) = fd.streams().best(Type::Audio) {
            self.tags =
                HashMap::from_iter(stream.metadata().iter().map(|(k, v)| (k.into(), v.into())))
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn unload(&mut self) {
        self.bytes.clear();
        self.coverart.clear();
        self.tags = HashMap::default();
    }

    pub fn coverart(&self) -> Bytes {
        self.coverart.clone()
    }

    pub fn bytes(&self) -> Bytes {
        self.bytes.clone()
    }

    pub fn bytes_slice(&self, range: impl RangeBounds<usize>) -> Bytes {
        self.bytes.slice(range)
    }

    pub fn duration(&self) -> i64 {
        self.duration
    }

    pub fn as_message(&self) -> Message {
        Event::new_as_message(EventKind::Song, self.clone())
    }

    pub fn get_content_type(&self) -> &str {
        self.content_type.as_str()
    }
}

impl TryFrom<&walkdir::DirEntry> for Song {
    type Error = bool;

    fn try_from(dir: &walkdir::DirEntry) -> Result<Self, Self::Error> {
        let path = dir.path();
        let content_type = mime_guess::from_path(&path);

        if content_type.is_empty() {
            return Err(false);
        }

        let fd = input(path).expect("failed to open file");
        let stream = match fd.streams().best(Type::Audio) {
            Some(s) => s,
            //no point going forward if we don't have audio track
            _ => return Err(false),
        };

        let duration =
            (stream.duration() as f64 / stream.time_base().denominator() as f64).ceil() as i64;

        let hash = blake3::hash(path.as_os_str().as_bytes())
            .to_hex()
            .to_string();

        Ok(Self {
            id: hash,
            path: path.to_path_buf(),
            duration: duration,
            bytes: Bytes::new(),
            coverart: Bytes::new(),
            tags: HashMap::default(),
            content_type: content_type.first().expect("aa").to_string(),
        })
    }
}
