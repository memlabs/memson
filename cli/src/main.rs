use futures::prelude::*;
use bson::{Bson, Document};
use tokio::net::TcpStream;
use tokio_serde::formats::*;
use tokio_util::codec::{FramedWrite, LengthDelimitedCodec};
use serde_json::Value;

#[tokio::main]
pub async fn main() {
    // Bind a server socket
    let socket = TcpStream::connect("127.0.0.1:17653").await.unwrap();

    // Delimit frames using a length header
    let length_delimited = FramedWrite::new(socket, LengthDelimitedCodec::new());

    // Serialize frames with JSON
    let mut serialized =
        tokio_serde::SymmetricallyFramed::new(length_delimited,  SymmetricalBincode::<Value>::default());

    let mut doc = Document::new();
    doc.insert("a", Bson::Int32(1));

    // Send the value
    serialized
        .send(Value::from(2))
        .await
        .unwrap()
}