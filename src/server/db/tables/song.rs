use std::path::Path;

pub struct Song {
	title: String,
	path: Box<Path>, //
	duration: usize, //duration in s

	album: String,
	artist: String,

	//what else?
}