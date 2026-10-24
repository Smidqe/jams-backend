use axum::{body::Bytes, http::HeaderValue};
use lofty::{
    config::{GlobalOptions, ParseOptions, WriteOptions, apply_global_options},
    file::{FileType, TaggedFile},
    flac::FlacFile,
    ogg::{OpusFile, VorbisFile, tag::VorbisComments},
    picture::MimeType,
    prelude::*,
    probe::Probe,
    tag::TagType,
};
use std::io::Cursor;
use std::{
    collections::HashMap,
    fs,
    hash::{DefaultHasher, Hash, Hasher},
    path::{Path, PathBuf},
};

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
    pub art: Option<(Bytes, HeaderValue)>,
    pub tags: HashMap<String, String>,
    pub duration_ms: u64,
}

fn content_type_for(ext: &str) -> Option<&'static str> {
    match ext.to_ascii_lowercase().as_str() {
        "opus" | "ogg" => Some("audio/ogg"),
        "mp3" => Some("audio/mpeg"),
        "flac" => Some("audio/flac"),
        _ => None,
    }
}

pub fn image_content_type(mime: Option<&MimeType>) -> HeaderValue {
    mime.and_then(|m| HeaderValue::from_str(m.as_str()).ok())
        .unwrap_or(HeaderValue::from_static("application/octet-stream"))
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
    // lofty fails with >16MB cover arts
    apply_global_options(GlobalOptions::new().allocation_limit(ALLOCATION_LIMIT));

    let bytes = Bytes::from(fs::read(&e.path)?);
    let (file, aliased) = read_tagged(Cursor::new(bytes.clone()))?;

    let duration_ms = file.properties().duration().as_millis() as u64;
    anyhow::ensure!(duration_ms > 0, "zero duration: {}", e.path.display());

    let (mut tags, mut art) = (HashMap::new(), None);
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

        // TODO: Shrink big covers (e.g. to at most 1000x1000 JPEG with the `image` crate).
        // Some files embed 15 MB covers, and /artwork sends them in full to every listener.
        if art.is_none() {
            art = tag.pictures().first().map(|p| {
                (
                    Bytes::copy_from_slice(p.data()),
                    image_content_type(p.mime_type()),
                )
            });
        }
    }

    // Aliases, then the folder layout, fill in tags the file is missing (or has empty)
    for (k, v) in aliased.into_iter().chain(path_tags(&e.path)) {
        let tag = tags.entry(k.to_owned()).or_default();
        if tag.trim().is_empty() {
            *tag = v;
        }
    }

    Ok(Track {
        id: e.id.clone(),
        content_type: e.content_type,
        bytes: without_metadata(&bytes, &file, &e.path),
        art,
        tags,
        duration_ms,
    })
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
fn with_aliases<F: AudioFile + Into<TaggedFile>>(
    reader: &mut Cursor<Bytes>,
    options: ParseOptions,
    comments: fn(&F) -> Option<&VorbisComments>,
) -> anyhow::Result<(TaggedFile, Vec<(&'static str, String)>)> {
    let f = F::read_from(reader, options)?;
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

fn read_tagged(
    mut reader: Cursor<Bytes>,
) -> anyhow::Result<(TaggedFile, Vec<(&'static str, String)>)> {
    let probe = Probe::new(reader.clone()).guess_file_type()?;
    let options = ParseOptions::new();

    Ok(match probe.file_type() {
        Some(FileType::Opus) => {
            with_aliases::<OpusFile>(&mut reader, options, |f| Some(f.vorbis_comments()))?
        }
        Some(FileType::Vorbis) => {
            with_aliases::<VorbisFile>(&mut reader, options, |f| Some(f.vorbis_comments()))?
        }
        Some(FileType::Flac) => {
            with_aliases::<FlacFile>(&mut reader, options, FlacFile::vorbis_comments)?
        }
        _ => (probe.read()?, Vec::new()),
    })
}

/// Fallback tags from the library's folder layout, for files missing them:
///
/// * `Artists/<artist>/<file>`: artist
/// * `Artists/<artist>/<album>/…/<file>`: artist and album
/// * `Albums/<album>/<file>`: album
/// * `Albums/<series>/<album>/<file>`: album as `<series> – <album>`
pub fn path_tags(path: &Path) -> Vec<(&'static str, String)> {
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
