pub enum Event {
	Connect,
	Disconnect,
	SongChange
}

pub struct Server {
	sessions: HashMap<str, str>,
	songs: Vec<Song>
}