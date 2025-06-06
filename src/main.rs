mod server;

use std::{
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    str::FromStr,
    sync::{Arc, Mutex},
};

use actix_web::{
    get,
    http::{header::{self, HeaderName, HeaderValue}, Error},
    post, rt,
    web::{self, Bytes},
    App, HttpRequest, HttpResponse, HttpServer, Responder,
};
use actix_ws::AggregatedMessage;
use async_walkdir::{Filtering, WalkDir};
use dirs::home_dir;
use ffmpeg_next::{codec::Context, format::input, frame::Video};
use futures::{future::ok, stream::once};
use metadata::MediaFileMetadata;
use tokio_stream::StreamExt;
use turbojpeg::{image::EncodableLayout, YuvImage};

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
    let home = home_dir();
    let path = home.unwrap().join("galacontheme.ogg");
    let file = fs::read(path).unwrap();
    let body = once(ok::<_, Error>(Bytes::from(file)));

    HttpResponse::Ok()
        .content_type("audio/opus")
        .streaming(body)
}

#[get("/artwork")]
async fn artwork() -> impl Responder {
    let home = home_dir();
    let path = home.unwrap().join("galacontheme.ogg");

	let artwork = extract_artwork(path, 90);

	if let Some(bytes) = artwork {
		HttpResponse::Ok().content_type("image/jpeg").body(bytes)
	} else {
		HttpResponse::NoContent().finish()
	}
}

async fn manual_hello() -> impl Responder {
    HttpResponse::Ok().body("Hey there!")
}

struct Song {
    title: String,
    artwork: Option<Vec<u8>>,
}

struct AppState {
    songs: Vec<Song>,
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

fn extract_artwork<T: Into<PathBuf>>(path: T, quality: i32) -> Option<Vec<u8>> {
	let mut file = match input(&path.into()) {
		Ok(inp) => inp,
		Err(_) => return None
	};

	//if there's a coverart, it's a separate "video" track
	let stream = file.streams().best(ffmpeg_next::media::Type::Video)?;
	
	//in theory it's always the second stream, but maybe not
	let index = stream.index();

	let context_decoder =
		Context::from_parameters(stream.parameters()).expect("should be a valid stream");
	
	let mut decoder = context_decoder
		.decoder()
		.video()
		.expect("it should have video stream?");

	let mut frame = Video::empty();

	//
	for (_, packet) in file.packets().filter(|(s, _)| s.index() == index) {
		println!("decoding packet");
		let _ = decoder.send_packet(&packet);
		let _ = decoder.receive_frame(&mut frame);
	}

	let _ = decoder.send_eof();
	let mut data = Vec::new();

	//append data together, 
	for index in 0..frame.planes() {
		data.extend_from_slice(frame.data(index));
	}

	let image = YuvImage {
		align: 32,
		height: frame.height() as usize,
		width: frame.width() as usize,
		pixels: data.as_slice(),
		subsamp: turbojpeg::Subsamp::Sub2x2,
	};

	match turbojpeg::compress_yuv(image, quality) {
		Ok(buf) => Some(buf.as_bytes().to_vec()),
		_ => None,
	}
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    ffmpeg_next::init().unwrap();
    //Steps:
    //1. Load all files within the specified folder
    //2. Start up the playlist
    //3. Start up WS handler
    //4. Start up the server

    HttpServer::new(|| {
        App::new()
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
