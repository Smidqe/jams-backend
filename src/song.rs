use axum::{body::Bytes, extract::ws::Message};
use ffmpeg_next::{format::input, media::Type};
use serde::Serialize;
use std::{collections::HashMap, fs, ops::RangeBounds, os::unix::ffi::OsStrExt, path::PathBuf};

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
        println!("[song]: Loading song at {:?}", &self.path);

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

        //get tags (metadata) and duration from the audio stream
        if let Some(stream) = fd.streams().best(Type::Audio) {
            self.tags =
                HashMap::from_iter(stream.metadata().iter().map(|(k, v)| (k.into(), v.into())));
            self.duration =
                (stream.duration() as f64 / stream.time_base().denominator() as f64).ceil() as i64
        }
    }

    pub fn id(&self) -> &str {
        &self.id
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

    pub fn content_length(&self) -> usize {
        self.bytes.len()
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
        if dir.file_type().is_dir() {
            return Err(false);
        }

        let path = dir.path();
        let content_type = mime_guess::from_path(path);

        if content_type.is_empty() {
            return Err(false);
        }

        let mimetype = if let Some(mt) = content_type.first()
            && mt.type_() == "audio"
        {
            mt.to_owned()
        } else {
            return Err(false);
        };

        let hash = blake3::hash(path.as_os_str().as_bytes())
            .to_hex()
            .to_string();

        Ok(Self {
            id: hash,
            path: path.to_path_buf(),
            duration: 0,
            bytes: Bytes::new(),
            coverart: Bytes::new(),
            tags: HashMap::default(),
            content_type: mimetype.to_string(),
        })
    }
}
