mod playlist;

use std::{
    collections::HashMap,
    env::current_dir,
    fs::{self},
    mem::transmute,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{atomic::AtomicUsize, Arc, RwLock},
    time::Duration,
};

use actix_web::{
    get,
    http::{
        header::{self, HeaderName, HeaderValue},
        Error,
    },
    post, rt,
    web::{self, Bytes},
    App, HttpRequest, HttpResponse, HttpServer, Responder,
};
use actix_ws::AggregatedMessage;
use clap::Parser;
use dirs::home_dir;
use ffmpeg_next::{
    codec::{Context, Parameters},
    ffi::AVPixelFormat,
    format::{
        context::{input::PacketIter, Input},
        input, Pixel,
    },
    frame::Video,
    media::{self, Type},
    Packet, Stream, StreamMut,
};
use futures::{future::ok, stream::once};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use tokio_stream::StreamExt;
use turbojpeg::{image::EncodableLayout, Subsamp, YuvImage};
use walkdir::{DirEntry, WalkDir};

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello world!")
}

#[get("/version")]
async fn version() -> impl Responder {
    let mut response = HttpResponse::Ok().body("Hello world!");

    let headers = response.headers_mut();

    headers.append(
        HeaderName::from_str("x-jams-version").unwrap(),
        HeaderValue::from_str("0.1").unwrap(),
    );

    return response;
}

#[post("/connect")]
async fn connect() -> impl Responder {
    HttpResponse::Ok().body("test")
}

#[post("/echo")]
async fn echo(req_body: String) -> impl Responder {
    HttpResponse::Ok().body(req_body)
}

#[get("/stream")]
async fn streamEndpoint() -> impl Responder {
    //TODO: This should take in the song ID and get the artwork from there
    let path = get_song_folder().join("Aurelleah - Luna's Future -Aurelleah Remix-.mp3");
    let file = fs::read(path).unwrap();
    let body = once(ok::<_, Error>(Bytes::from(file)));

    HttpResponse::Ok()
        .content_type("audio/opus")
        .streaming(body)
}

#[get("/tags")]
async fn song_tags() -> impl Responder {
    HttpResponse::NoContent().finish()
}

#[get("/artwork")]
async fn artwork() -> impl Responder {
    let path = get_song_folder().join("Aurelleah - Luna's Future -Aurelleah Remix-.mp3");
    let stuff = extract_audio_title_and_tags(path);
    //return bytes from the appstate
    if let Some(artwork) = stuff.artwork {
        return HttpResponse::Ok().body(artwork);
    }

    HttpResponse::NoContent().finish()
}

async fn manual_hello() -> impl Responder {
    HttpResponse::Ok().body("Hey there!")
}

#[derive(Default, Debug)]
struct Song {
    id: String,
    title: String,
    artwork: Option<Vec<u8>>,
    meta: HashMap<String, String>,
}

#[derive(Default, Debug)]
pub struct Song2 {
    id: String,
    path: PathBuf,

    title: String,
    artwork: Option<Vec<u8>>,
    meta: HashMap<String, String>,
}

impl Song2 {
    pub fn name(&self) -> String {
        if let Some(title) = self.meta.get("title") {
            title.to_string()
        } else {
            String::from("[unknown]")
        }
    }

    pub fn duration(&self) -> Duration {
        if let Some(duration) = self.meta.get("duration") {
            Duration::from_secs(duration.parse().unwrap_or(0))
        } else {
            Duration::from_secs(0)
        }
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    pub fn has_artwork(&self) -> bool {
        self.artwork.is_some()
    }

    pub fn get_artwork(&self) -> &[u8] {
        if let Some(art) = &self.artwork {
            art.as_slice()
        } else {
            &[]
        }
    }
}

struct AppState {
    songs: Arc<RwLock<HashMap<usize, Song>>>,
    playlist: Arc<RwLock<Vec<usize>>>,
    current: AtomicUsize,
}

async fn ws_echo(req: HttpRequest, stream: web::Payload) -> Result<HttpResponse, Error> {
    let (res, mut session, stream) = actix_ws::handle(&req, stream).unwrap();

    let mut stream = stream
        .aggregate_continuations()
        // aggregate continuation frames up to 1MiB
        .max_continuation_size(2_usize.pow(20));

    // start task but don't wait for it
    rt::spawn(async move {
        // receive messages from websocket
        while let Some(msg) = stream.next().await {
            match msg {
                Ok(AggregatedMessage::Text(text)) => {
                    // echo text message
                    session.text(text).await.unwrap();
                }

                Ok(AggregatedMessage::Binary(bin)) => {
                    // echo binary message
                    session.binary(bin).await.unwrap();
                }

                Ok(AggregatedMessage::Ping(msg)) => {
                    // respond to PING frame with PONG frame
                    session.pong(&msg).await.unwrap();
                }

                _ => {}
            }
        }
    });

    // respond immediately with response connected to WS session
    Ok(res)
}

struct Frame {
    video: Video,
    raw: Vec<u8>,
    format: Pixel,
}

impl Default for Frame {
    fn default() -> Self {
        Self {
            video: Video::empty(),
            raw: Default::default(),
            format: Pixel::None,
        }
    }
}

fn extract_first_frame(input: &mut Input, params: Parameters, index: usize) -> Frame {
    let format: Pixel;

    unsafe {
        format = Pixel::from(transmute::<i32, AVPixelFormat>((*params.as_ptr()).format));
    }

    let context_decoder = Context::from_parameters(params).expect("should be a valid stream");
    let mut decoder = context_decoder
        .decoder()
        .video()
        .expect("it should have video stream?");

    let mut frame = Video::empty();

    for (_, packet) in input.packets().filter(|(s, _)| s.index() == index) {
        let _ = decoder.send_packet(&packet);
        let _ = decoder.send_eof();
        let _ = decoder.receive_frame(&mut frame);
    }

    let mut data = Vec::new();

    //append data together,
    for index in 0..frame.planes() {
        data.extend_from_slice(frame.data(index));
    }

    Frame {
        raw: data,
        video: frame,
        format,
    }
}

fn extract_audio_track_tags(input: &mut Input) -> HashMap<String, String> {
    let stream = match input.streams().best(Type::Audio) {
        Some(stream) => stream,
        None => return HashMap::new(),
    };

    stream
        .metadata()
        .iter()
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
}

fn extract_audio_title_and_tags<T: AsRef<Path>>(path: T) -> Song {
    let mut file = match input(&path) {
        Ok(inp) => inp,
        Err(_) => return Song::default(),
    };

    let art: Option<Vec<u8>> = extract_artwork(&mut file);
    let tags: HashMap<String, String> = extract_audio_track_tags(&mut file);

    let title = tags
        .get("title")
        .map_or("[unknown title]", |v| v)
        .to_string();

    let hash = blake3::hash(fs::read(path).expect("it's already there").as_slice());

    Song {
        id: hash.to_string(),
        meta: tags,
        artwork: art,
        title,
    }
}

fn get_subsampling(format: Pixel) -> Subsamp {
    let name = format!("{format:?}");
    let samplings_map: HashMap<&str, Subsamp> = HashMap::from([
        ("444", Subsamp::None),
        ("422", Subsamp::Sub2x1),
        ("420", Subsamp::Sub2x2),
        ("440", Subsamp::Sub1x2),
        ("441", Subsamp::Sub4x1),
    ]);

    samplings_map
        .iter()
        .find(|(nums, _)| name.contains(**nums))
        .map_or(Subsamp::Gray, |(_, s)| *s)
}

fn is_frame_yuv(format: Pixel) -> bool {
    format!("{format:?}").starts_with("YUV")
}

fn create_yuv_image(frame: Frame, quality: i32) -> Vec<u8> {
    let image = YuvImage {
        align: 64,
        height: frame.video.height() as usize,
        width: frame.video.width() as usize,
        pixels: frame.raw.as_slice(),
        subsamp: get_subsampling(frame.format),
    };

    match turbojpeg::compress_yuv(image, quality) {
        Ok(buf) => buf.as_bytes().to_vec(),
        Err(_) => Vec::default(),
    }
}

fn create_rgb_image(_frame: Frame, _quality: i32) -> Vec<u8> {
    Vec::new()
}

fn extract_artwork(input: &mut Input) -> Option<Vec<u8>> {
    let stream = input.streams().best(Type::Video);

    if let Some(stream) = stream {
        let frame = extract_first_frame(input, stream.parameters(), stream.index());
        let bytes = if is_frame_yuv(frame.format) {
            create_yuv_image(frame, 90)
        } else {
            create_rgb_image(frame, 90)
        };

        Some(bytes)
    } else {
        None
    }
}

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    #[arg(short, long)]
    path: String,
}

fn is_hidden(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with("."))
        .unwrap_or(false)
}

fn discover2(path: &PathBuf) -> Vec<DirEntry> {
    let walker = WalkDir::new(path).into_iter();

    Vec::from_iter(walker.filter_entry(|e| !is_hidden(e)).flatten())
}

async fn discover(path: &PathBuf) -> Vec<Song> {
    let files = discover2(path);
    let songs = files
        .iter()
        .map(|f| extract_audio_title_and_tags(f.path()))
        .collect();

    songs
}

fn get_song_folder() -> PathBuf {
    home_dir()
        .expect("aaaa")
        .join("projects/rust/jams-backend/songs")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    ffmpeg_next::init().unwrap();
    //Steps:
    //1. Load all files within the specified folder
    let path = get_song_folder();
    let songs = discover(&path).await;

    println!("{}", songs.len());
    println!(
        "{}",
        songs
            .iter()
            .filter(|s| s.artwork.as_ref().is_some())
            .count()
    );

    //2. Start up the playlist
    //3. Start up WS handler
    //4. Start up the server
    let playlist: Vec<String> = Vec::new();

    println!("Starting server");

    HttpServer::new(|| {
        App::new()
            .app_data(web::Data::new(AppState {
                songs: Arc::new(RwLock::new(HashMap::new())),
                playlist: Arc::new(RwLock::new(Vec::new())),
                current: AtomicUsize::new(0),
            }))
            .service(hello)
            .service(echo)
            .service(version)
            .service(streamEndpoint)
            .service(artwork)
            .route("/hey", web::get().to(manual_hello))
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
