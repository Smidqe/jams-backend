use axum::{body::Bytes, http::HeaderValue};
use lofty::{
    config::{GlobalOptions, ParseOptions, apply_global_options},
    file::{FileType, TaggedFile},
    flac::FlacFile,
    ogg::{OpusFile, VorbisFile, tag::VorbisComments},
    picture::MimeType,
    prelude::*,
    probe::Probe,
    tag::TagType,
};
use std::io::Cursor;
use std::{collections::HashMap, fs, path::PathBuf};

#[derive(Clone)]
pub struct SongFile {
    pub path: PathBuf,
    pub id: String, // blake3 of path
    pub content_type: &'static str,
}

/// Fully loaded track. Lives behind an Arc, so clones are free.
pub struct Track {
    pub id: String,
    pub content_type: &'static str,
    pub bytes: Bytes,
    pub art: Bytes,
    pub art_type: HeaderValue,
    pub tags: HashMap<String, String>,
    pub duration_ms: u128,
}

pub fn content_type_for(ext: &str) -> Option<&'static str> {
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

/// Some files have wrong tags for their file format, lofty is format agnostic so it may just work, can't rely on it though.
/// This is not comprehensive, just from my own archive
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

    let duration_ms = file.properties().duration().as_millis();
    anyhow::ensure!(duration_ms > 0, "zero duration: {:?}", e.path);

    let (mut tags, mut art, mut art_mime) = (HashMap::new(), Bytes::new(), None);
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

        if art.is_empty()
            && let Some(p) = tag.pictures().first()
        {
            art = Bytes::copy_from_slice(p.data());
            art_mime = p.mime_type().cloned();
        }
    }

    //duplicate tags will be ignored, probably not a problem \\shrug
    for (k, v) in aliased {
        tags.entry(k.to_owned()).or_insert(v);
    }

    let art_type = image_content_type(art_mime.as_ref());

    Ok(Track {
        id: e.id.clone(),
        content_type: e.content_type,
        bytes,
        art,
        art_type,
        tags,
        duration_ms,
    })
}

fn read_tagged(reader: Cursor<Bytes>) -> anyhow::Result<(TaggedFile, Vec<(&'static str, String)>)> {
    let probe = Probe::new(reader).guess_file_type()?;
    let options = ParseOptions::new();

    Ok(match probe.file_type() {
        Some(FileType::Opus) => {
            let f = OpusFile::read_from(&mut probe.into_inner(), options)?;
            let aliased = aliased_comments(Some(f.vorbis_comments()));
            (f.into(), aliased)
        }
        Some(FileType::Vorbis) => {
            let f = VorbisFile::read_from(&mut probe.into_inner(), options)?;
            let aliased = aliased_comments(Some(f.vorbis_comments()));
            (f.into(), aliased)
        }
        Some(FileType::Flac) => {
            let f = FlacFile::read_from(&mut probe.into_inner(), options)?;
            let aliased = aliased_comments(f.vorbis_comments());
            (f.into(), aliased)
        }
        _ => (probe.read()?, Vec::new()),
    })
}

fn aliased_comments(tags: Option<&VorbisComments>) -> Vec<(&'static str, String)> {
    tags.into_iter()
        .flat_map(VorbisComments::items)
        .filter_map(|(k, v)| {
            let (_, to) = ALIASES
                .iter()
                .find(|(from, _)| from.eq_ignore_ascii_case(k))?;
            Some((*to, v.to_owned()))
        })
        .collect()
}
