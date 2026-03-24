use axum::extract::ws::{Message, Utf8Bytes};
use serde::Serialize;

#[derive(Clone, Serialize, Copy)]
pub enum EventKind {
    Song,
    Sync,
    SessionId,
}

#[derive(Clone, Serialize)]
pub struct Event<T>
where
    T: Serialize,
{
    pub kind: EventKind,
    pub payload: T,
}

impl<T> Event<T>
where
    T: Serialize,
{
    pub fn new(kind: EventKind, payload: T) -> Event<T> {
        Event { kind, payload }
    }

    pub fn new_as_message(kind: EventKind, payload: T) -> Message {
        Event::new(kind, payload).into()
    }
}

impl<T> From<Event<T>> for Message
where
    T: Serialize,
{
    fn from(val: Event<T>) -> Self {
        Message::Text(Utf8Bytes::from(
            serde_json::to_string(&val).expect("this should never fail"),
        ))
    }
}
