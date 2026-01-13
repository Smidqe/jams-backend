use axum::extract::ws::{Message, Utf8Bytes};
use serde::Serialize;

#[derive(Clone, Serialize)]
pub enum Event {
    Song,
    Sync,
}

#[derive(Serialize)]
struct EventBody<T>
where
    T: Serialize,
{
    pub kind: Event,
    pub payload: T,
}

impl<T> Into<Message> for EventBody<T>
where
    T: Serialize,
{
    fn into(self) -> Message {
        Message::Text(Utf8Bytes::from(
            serde_json::to_string(&self).expect("this should never fail"),
        ))
    }
}

impl Event {
    pub fn as_message_with_payload<T>(&self, payload: &T) -> Message
    where
        T: Serialize + Clone,
    {
        EventBody {
            kind: self.clone(),
            payload: payload.clone(),
        }
        .into()
    }

    pub fn as_message_with_payload_str(&self, payload: &str) -> Message {
        EventBody {
            kind: self.clone(),
            payload: payload.to_string(),
        }
        .into()
    }
}
