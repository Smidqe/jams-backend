mod playlist;
mod song;

use std::{
    env::current_dir,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use actix_web::{
    get,
    http::Error,
    post, rt,
    web::{self, Bytes},
    App, HttpRequest, HttpResponse, HttpServer, Responder,
};
use actix_ws::AggregatedMessage;
use clap::Parser;
use ffmpeg_next::format::input;
use futures::{future::ok, stream::once, StreamExt};
use parking_lot::RwLock;

use crate::{playlist::Playlist, song::Song};

#[get("/version")]
async fn version() -> impl Responder {
    HttpResponse::NoContent()
        .append_header(("x-jams-version", "0.1"))
        .finish()
}

#[post("/connect")]
async fn connect() -> impl Responder {
    HttpResponse::Ok().body("test")
}

#[get("/stream")]
async fn stream_endpoint(data: web::Data<AppState>) -> impl Responder {
    let bytes = match data.current.read().as_ref() {
        Some(s) => Bytes::from(s.get_song_bytes().to_vec()),
        None => {
            eprintln!("Nothing here :/");
            Bytes::default()
        }
    };

    match bytes.len() {
        0 => HttpResponse::InternalServerError().finish(),
        _ => HttpResponse::Ok()
            .content_type("audio/opus")
            .streaming(once(ok::<_, Error>(bytes))),
    }
}

#[get("/artwork")]
async fn artwork(data: web::Data<AppState>) -> impl Responder {
    let bytes = match data.current.read().as_ref() {
        Some(s) => s.get_artwork().to_vec(),
        None => Vec::default(),
    };

    if bytes.is_empty() {
        HttpResponse::NoContent().finish()
    } else {
        HttpResponse::Ok().content_type("image/jpeg").body(bytes)
    }
}

struct AppState {
    current: Arc<RwLock<Option<Song>>>,
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

#[derive(Parser, Debug)]
#[command(version)]
struct Args {
    #[arg(short, long)]
    path: String,
}

fn get_song_folder() -> PathBuf {
    current_dir().expect("aaaa").join("songs")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    ffmpeg_next::init().expect("eee");
    //Steps:
    //1. Load all files within the specified folder

    //2. Start up the playlist thread
    //3. Start up WS handler
    //4. Start up the server

    let shutdown = AtomicBool::new(false);
    let current = Arc::new(RwLock::new(None));
    let state = web::Data::new(AppState {
        current: current.clone(),
    });

    //playlist thread
    thread::spawn(move || {
        let mut playlist = Playlist::from(get_song_folder());

        *current.write() = playlist.current_cloned();

        while !shutdown.load(Ordering::Relaxed) {
            if playlist.tick() {
                *current.write() = playlist.current_cloned();
            }

            thread::sleep(Duration::from_secs(1));
        }
    });

    println!("Starting server");

    HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .service(version)
            .service(stream_endpoint)
            .service(artwork)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
