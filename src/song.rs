use axum::{body::Bytes, http::HeaderValue};
use lofty::{
    config::{GlobalOptions, ParseOptions, WriteOptions, apply_global_options},
    file::{FileType, TaggedFile},
    flac::FlacFile,
    ogg::{OpusFile, VorbisFile, tag::VorbisComments},
    picture::Picture,
    prelude::*,
    probe::Probe,
    tag::TagType,
};
use serde::Serialize;
use std::{
    collections::HashMap,
    fs::{self, File},
    hash::{DefaultHasher, Hash, Hasher},
    io::{BufReader, Cursor, Read, Seek},
    path::{Path, PathBuf},
};

/// Image bytes and their content type.
pub type Image = (Bytes, HeaderValue);

/// Tags by their Vorbis comment name.
pub type Tags = HashMap<String, String>;

/// Tags from outside the file's own tags (aliases, folder layout), used where it has none.
type ExtraTags = Vec<(&'static str, String)>;

#[derive(Clone)]
pub struct SongFile {
    pub path: PathBuf,
    pub id: String,
    pub content_type: &'static str,
}

impl SongFile {
    /// `None` for files that aren't a supported audio format.
    pub fn from_path(path: PathBuf) -> Option<Self> {
        let content_type = content_type_for(path.extension()?.to_str()?)?;

        // Same path gives the same id on every run of the same binary
        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        let id = format!("{:016x}", hasher.finish());

        Some(Self {
            path,
            id,
            content_type,
        })
    }
}

/// Fully loaded track. Lives behind an Arc, so clones are free.
pub struct Track {
    pub id: String,
    pub content_type: &'static str,
    /// The file as served by `/stream`: the audio without its embedded tags and cover.
    pub bytes: Bytes,
    /// Embedded cover art and its content type, if the file has one.
    pub art: Option<Image>,
    pub tags: Tags,
    pub duration_ms: u64,
}

/// A song in the upcoming list: just enough to show it.
#[derive(Clone, Serialize)]
pub struct Upcoming {
    pub id: String,
    pub duration_ms: u64,
    /// Only `UPCOMING_TAGS`, and only the non-empty ones.
    pub tags: Tags,
}

const UPCOMING_TAGS: &[&str] = &["TITLE", "ARTIST", "ALBUM"];

fn content_type_for(ext: &str) -> Option<&'static str> {
    match ext.to_ascii_lowercase().as_str() {
        "opus" | "ogg" => Some("audio/ogg"),
        "mp3" => Some("audio/mpeg"),
        "flac" => Some("audio/flac"),
        _ => None,
    }
}

/// The picture's bytes and content type.
pub fn image(p: &Picture) -> Image {
    let content_type = p
        .mime_type()
        .and_then(|m| HeaderValue::from_str(m.as_str()).ok())
        .unwrap_or(HeaderValue::from_static("application/octet-stream"));
    (Bytes::copy_from_slice(p.data()), content_type)
}

/// Embedded cover art can exceed lofty's default 16 MiB allocation limit.
const ALLOCATION_LIMIT: usize = 64 * 1024 * 1024;

/// Vorbis comment keys lofty doesn't know, mapped to the key they mean.
/// Built from what turned up in my own archive, so not comprehensive.
const ALIASES: &[(&str, &str)] = &[
    // FFmpeg's names for MP4 sort atoms
    ("artist-sort", "ARTISTSORT"),
    ("album-sort", "ALBUMSORT"),
    ("title-sort", "TITLESORT"),
    // ID3 frame descriptions written as keys
    ("Band", "ALBUMARTIST"),
    ("Part of a set", "DISCNUMBER"),
    ("Release time", "RELEASEDATE"),
    ("Initial key", "INITIALKEY"),
    ("Original artist(s)", "ORIGINALARTIST"),
    // ID3v2.3/2.4 frame IDs copied as keys
    ("TKEY", "INITIALKEY"),
    ("TPA", "DISCNUMBER"),
    ("TIT1", "GROUPING"),
    ("TIT3", "SUBTITLE"),
    ("TSRC", "ISRC"),
    ("TMED", "MEDIA"),
    ("TOLY", "LYRICIST"),
    ("TEXT", "LYRICIST"),
    ("TPE4", "REMIXER"),
    ("TOPE", "ORIGINALARTIST"),
    ("TSO2", "ALBUMARTISTSORT"),
    ("TSOC", "COMPOSERSORT"),
    // ID3v2.2 frame IDs (old iTunes) copied as keys
    ("TBP", "BPM"),
    ("TCM", "COMPOSER"),
    ("TCO", "GENRE"),
    ("TKE", "INITIALKEY"),
    ("TPB", "PUBLISHER"),
    ("TP4", "REMIXER"),
    ("TT1", "GROUPING"),
    ("TS2", "ALBUMARTISTSORT"),
    ("TSA", "ALBUMSORT"),
    ("TSC", "COMPOSERSORT"),
    ("TSP", "ARTISTSORT"),
    ("TST", "TITLESORT"),
    // Other taggers' names
    ("ORIGINAL DATE", "ORIGINALDATE"),
    ("ORIGINAL YEAR", "ORIGINALYEAR"),
    ("Release type", "RELEASETYPE"),
    ("Track:Comments", "COMMENT"),
    ("ALBUM_TITLE", "ALBUM"),
    ("ORIGARTIST", "ORIGINALARTIST"),
    ("REMIXEDBY", "REMIXER"),
    ("MusicMatch_Mood", "MOOD"),
    // Valid, but unknown to lofty
    ("URL_PUBLISHER", "URL_PUBLISHER"),
    ("URL_ARTIST", "URL_ARTIST"),
    ("URL_SOURCE", "URL_SOURCE"),
    ("URL_FILE", "URL_FILE"),
];

pub fn load(e: &SongFile) -> anyhow::Result<Track> {
    let bytes = Bytes::from(fs::read(&e.path)?);
    let (file, tags, duration_ms) =
        parse(Cursor::new(bytes.clone()), &e.path, ParseOptions::new())?;

    // TODO: Shrink big covers (e.g. to at most 1000x1000 JPEG with the `image` crate).
    // Some files embed 15 MB covers, and /artwork sends them in full to every listener.
    let art = file
        .tags()
        .iter()
        .find_map(|tag| tag.pictures().first())
        .map(image);

    Ok(Track {
        id: e.id.clone(),
        content_type: e.content_type,
        bytes: without_metadata(&bytes, &file, &e.path),
        art,
        tags,
        duration_ms,
    })
}

/// Tags and duration for the upcoming list, read without the cover or keeping the
/// file in memory. A file that can't be read gets its folder-layout tags and a zero
/// duration, so the list keeps its place.
pub fn peek(e: &SongFile) -> Upcoming {
    let options = ParseOptions::new().read_cover_art(false);
    let read = File::open(&e.path)
        .map_err(anyhow::Error::from)
        .and_then(|f| parse(BufReader::new(f), &e.path, options));

    let (mut tags, duration_ms) = match read {
        Ok((_, tags, duration_ms)) => (tags, duration_ms),
        Err(err) => {
            eprintln!("[song]: can't peek {}: {err}", e.path.display());
            let tags = path_tags(&e.path)
                .into_iter()
                .map(|(k, v)| (k.to_owned(), v))
                .collect();
            (tags, 0)
        }
    };
    tags.retain(|k, v| UPCOMING_TAGS.contains(&k.as_str()) && !v.trim().is_empty());

    Upcoming {
        id: e.id.clone(),
        duration_ms,
        tags,
    }
}

/// Reads the file and its tags: Vorbis comment names for every format, with
/// missing ones filled from `ALIASES`, then the folder layout.
fn parse<R: Read + Seek>(
    reader: R,
    path: &Path,
    options: ParseOptions,
) -> anyhow::Result<(TaggedFile, Tags, u64)> {
    // lofty fails with >16MB cover arts
    apply_global_options(GlobalOptions::new().allocation_limit(ALLOCATION_LIMIT));

    let (file, aliased) = read_tagged(reader, options)?;

    let duration_ms = file.properties().duration().as_millis() as u64;
    anyhow::ensure!(duration_ms > 0, "zero duration: {}", path.display());

    let mut tags = HashMap::new();
    for tag in file.tags() {
        for item in tag.items() {
            // Vorbis comment names for every format, so FE sees one set of keys
            let key = item
                .key()
                .map_key(TagType::VorbisComments)
                .or_else(|| item.key().map_key(tag.tag_type()));
            if let (Some(k), Some(v)) = (key, item.value().text()) {
                tags.entry(k.to_owned()).or_insert_with(|| v.to_owned());
            }
        }
    }

    // Aliases, then the folder layout, fill in tags the file is missing (or has empty)
    for (k, v) in aliased.into_iter().chain(path_tags(path)) {
        let tag = tags.entry(k.to_owned()).or_default();
        if tag.trim().is_empty() {
            *tag = v;
        }
    }

    Ok((file, tags, duration_ms))
}

/// The file with every embedded tag (and so the cover art) removed. Clients get
/// tags from the events and the cover from `/artwork`, so `/stream` doesn't need
/// them; in Ogg the cover also sits before the audio, so a big one delays playback.
/// Falls back to the original bytes if rewriting fails.
fn without_metadata(bytes: &Bytes, file: &TaggedFile, path: &Path) -> Bytes {
    let mut out = Cursor::new(bytes.to_vec());
    for tag in file.tags() {
        out.set_position(0);
        if let Err(e) = tag
            .tag_type()
            .remove_from(&mut out, WriteOptions::default())
        {
            eprintln!("[song]: serving {} with its tags: {e}", path.display());
            return bytes.clone();
        }
    }
    Bytes::from(out.into_inner())
}

/// Reads `F`, plus its raw Vorbis comments renamed through `ALIASES`, which
/// the generic tag would drop.
fn with_aliases<F: AudioFile + Into<TaggedFile>, R: Read + Seek>(
    probe: Probe<R>,
    options: ParseOptions,
    comments: fn(&F) -> Option<&VorbisComments>,
) -> anyhow::Result<(TaggedFile, ExtraTags)> {
    let f = F::read_from(&mut probe.into_inner(), options)?;
    let aliased = comments(&f)
        .into_iter()
        .flat_map(VorbisComments::items)
        .filter_map(|(k, v)| {
            let (_, to) = ALIASES
                .iter()
                .find(|(from, _)| from.eq_ignore_ascii_case(k))?;
            Some((*to, v.to_owned()))
        })
        .collect();
    Ok((f.into(), aliased))
}

fn read_tagged<R: Read + Seek>(
    reader: R,
    options: ParseOptions,
) -> anyhow::Result<(TaggedFile, ExtraTags)> {
    // Guessing leaves the reader where it started
    let probe = Probe::new(reader).options(options).guess_file_type()?;

    match probe.file_type() {
        Some(FileType::Opus) => {
            with_aliases(probe, options, |f: &OpusFile| Some(f.vorbis_comments()))
        }
        Some(FileType::Vorbis) => {
            with_aliases(probe, options, |f: &VorbisFile| Some(f.vorbis_comments()))
        }
        Some(FileType::Flac) => with_aliases(probe, options, FlacFile::vorbis_comments),
        _ => Ok((probe.read()?, Vec::new())),
    }
}

/// Fallback tags from the library's folder layout, for files missing them:
///
/// * `Artists/<artist>/<file>`: artist
/// * `Artists/<artist>/<album>/…/<file>`: artist and album
/// * `Albums/<album>/<file>`: album
/// * `Albums/<series>/<album>/<file>`: album as `<series> – <album>`
pub fn path_tags(path: &Path) -> ExtraTags {
    let mut tags = Vec::new();
    if let Some(stem) = path.file_stem() {
        tags.push(("TITLE", stem.to_string_lossy().into_owned()));
    }

    let folders: Vec<String> = path
        .parent()
        .into_iter()
        .flat_map(Path::components)
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect();
    let Some(start) = folders
        .iter()
        .rposition(|f| f == "Artists" || f == "Albums")
    else {
        return tags;
    };

    match (folders[start].as_str(), &folders[start + 1..]) {
        ("Artists", [artist, rest @ ..]) => {
            tags.push(("ARTIST", artist.clone()));
            if let Some(album) = rest.first() {
                tags.push(("ALBUM", album.clone()));
            }
        }
        ("Albums", [album]) => tags.push(("ALBUM", album.clone())),
        ("Albums", [series, album, ..]) => tags.push(("ALBUM", format!("{series} – {album}"))),
        _ => {}
    }

    tags
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peek_matches_load() {
        let e = SongFile::from_path("local_songs/galacontheme.opus".into()).unwrap();
        let track = load(&e).unwrap();
        let upcoming = peek(&e);

        assert_eq!(upcoming.id, track.id);
        assert_eq!(upcoming.duration_ms, track.duration_ms);
        assert!(upcoming.tags.contains_key("TITLE"));
        for (k, v) in &upcoming.tags {
            assert_eq!(track.tags.get(k), Some(v), "{k}");
        }
    }

    #[test]
    fn peek_falls_back_to_path() {
        let e = SongFile::from_path("Artists/Someone/Album/Song.opus".into()).unwrap();
        let upcoming = peek(&e);

        assert_eq!(upcoming.duration_ms, 0);
        assert_eq!(upcoming.tags["TITLE"], "Song");
        assert_eq!(upcoming.tags["ARTIST"], "Someone");
        assert_eq!(upcoming.tags["ALBUM"], "Album");
    }
}
