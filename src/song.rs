use ffmpeg_next::{format::input, media::Type};
use std::{collections::HashMap, env::current_dir, fs, path::PathBuf};

#[derive(Default, Debug, Clone)]
pub struct Song {
    id: String,
    path: PathBuf,
    duration: u128,
    bytes: Vec<u8>,
    artwork: Option<Vec<u8>>,
    meta: HashMap<String, String>,
}

impl Song {
    pub fn name(&self) -> String {
        if let Some(title) = self.meta.get("title") {
            title.to_string()
        } else {
            String::from("[unknown]")
        }
    }

    pub fn duration(&self) -> u128 {
        self.duration
    }

    pub fn get_artwork(&self) -> &[u8] {
        if let Some(art) = &self.artwork {
            art.as_slice()
        } else {
            &[]
        }
    }

    pub fn get_song_bytes(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    pub fn load_artwork(&mut self) {
        let mut fd = input(&self.path).expect("failed to load artwork");
        let frame = fd
            .packets()
            .find(|(s, _)| s.parameters().medium() == Type::Video);

        self.artwork = match frame {
            Some((_, p)) => p.data().map(|s| s.to_vec()),
            None => None,
        };
    }

    pub fn load_file(&mut self) {
        self.bytes = fs::read(&self.path).unwrap_or_default()
    }

    pub fn load_tags(&mut self) {
        let fd = input(&self.path).expect("failed to load artwork");

        if let Some(s) = fd.streams().best(Type::Audio) {
            self.meta = s
                .metadata()
                .iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned()))
                .collect()
        }
    }

    pub fn load(&mut self) {
        self.load_file();
        self.load_artwork();
        self.load_tags();
    }

    pub fn offload(&mut self) {
        self.artwork = None;
        self.bytes.clear();
        self.meta = HashMap::new()
    }
}

impl From<&walkdir::DirEntry> for Song {
    fn from(value: &walkdir::DirEntry) -> Self {
        let path = value.path();

        if let Ok(fd) = input(value.path()) {
            let bytes = fs::read(value.path()).unwrap_or_default();
            let hash = blake3::hash(bytes.as_slice());

            Self {
                artwork: None,
                meta: HashMap::default(),
                bytes,
                id: hash.to_string(),
                path: path.to_path_buf(),
                duration: fd.duration() as u128,
            }
        } else {
            Song::default()
        }
    }
}
