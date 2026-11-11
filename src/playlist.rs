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

    /// The next `n` songs after the cursor, wrapping around. At most `len - 1`, so
    /// the current song never shows up as upcoming.
    pub fn upcoming(&self, n: usize) -> impl Iterator<Item = &SongFile> {
        let len = self.list.len();
        (1..=n.min(len - 1)).map(move |i| &self.list[(self.cursor + i) % len])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn playlist(len: usize, cursor: usize) -> Playlist {
        let list = (0..len)
            .map(|i| SongFile::from_path(format!("{i}.opus").into()).unwrap())
            .collect();
        Playlist { list, cursor }
    }

    fn upcoming_paths(p: &Playlist, n: usize) -> Vec<String> {
        p.upcoming(n)
            .map(|s| s.path.display().to_string())
            .collect()
    }

    #[test]
    fn upcoming_wraps_around() {
        let p = playlist(5, 3);
        assert_eq!(upcoming_paths(&p, 3), ["4.opus", "0.opus", "1.opus"]);
    }

    #[test]
    fn upcoming_never_includes_current() {
        let p = playlist(4, 1);
        assert_eq!(upcoming_paths(&p, 20), ["2.opus", "3.opus", "0.opus"]);
        assert_eq!(upcoming_paths(&playlist(1, 0), 20), Vec::<String>::new());
    }
}
