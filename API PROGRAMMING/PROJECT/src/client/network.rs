use crate::{ClientMessage, ServerMessage};
use std::io;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader, Lines};

/// Reads newline-delimited server messages from an asynchronous stream.
pub struct ServerReader<R> {
    lines: Lines<BufReader<R>>,
}

impl<R: AsyncRead + Unpin> ServerReader<R> {
    /// Creates a server-message reader from any asynchronous byte stream.
    pub fn new(reader: R) -> Self {
        Self {
            lines: BufReader::new(reader).lines(),
        }
    }

    /// Awaits and deserializes the next server message.
    pub async fn read_message(&mut self) -> io::Result<Option<ServerMessage>> {
        let Some(line) = self.lines.next_line().await? else {
            return Ok(None);
        };

        Ok(Some(serde_json::from_str(&line)?))
    }
}

/// Sends a newline-delimited JSON client message through any asynchronous writer.
pub async fn send_message<W: AsyncWrite + Unpin>(
    writer: &mut W,
    msg: &ClientMessage,
) -> io::Result<()> {
    let mut json_str = serde_json::to_string(msg)?;
    json_str.push('\n');
    writer.write_all(json_str.as_bytes()).await?;
    writer.flush().await
}

#[cfg(test)]
mod tests {
    use super::{ServerReader, send_message};
    use crate::{ClientMessage, ServerMessage};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    #[tokio::test]
    async fn send_message_writes_one_json_line() {
        let (mut writer, reader) = tokio::io::duplex(1024);
        let message = ClientMessage::SendText {
            content: "Road clear".into(),
        };

        send_message(&mut writer, &message).await.unwrap();

        let mut lines = BufReader::new(reader).lines();
        assert_eq!(
            lines.next_line().await.unwrap().unwrap(),
            serde_json::to_string(&message).unwrap()
        );
    }

    #[tokio::test]
    async fn reader_decodes_fragmented_messages_in_order() {
        let (mut peer, stream) = tokio::io::duplex(1024);
        let first = ServerMessage::TextMessage {
            sender: "FleetAdmin".into(),
            content: "Slow down".into(),
        };
        let second = ServerMessage::ErrorMessage("Route unavailable".into());
        let first_json = serde_json::to_string(&first).unwrap();
        let second_json = serde_json::to_string(&second).unwrap();

        peer.write_all(first_json[..8].as_bytes()).await.unwrap();
        peer.write_all(format!("{}\n{second_json}\n", &first_json[8..]).as_bytes())
            .await
            .unwrap();
        drop(peer);

        let mut reader = ServerReader::new(stream);
        assert_eq!(reader.read_message().await.unwrap(), Some(first));
        assert_eq!(reader.read_message().await.unwrap(), Some(second));
    }

    #[tokio::test]
    async fn reader_returns_none_at_eof() {
        let (peer, stream) = tokio::io::duplex(64);
        drop(peer);

        assert_eq!(
            ServerReader::new(stream).read_message().await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn reader_accepts_crlf() {
        let (mut peer, stream) = tokio::io::duplex(256);
        let message = ServerMessage::ErrorMessage("road closed".into());
        let json = serde_json::to_string(&message).unwrap();
        peer.write_all(format!("{json}\r\n").as_bytes())
            .await
            .unwrap();
        drop(peer);

        assert_eq!(
            ServerReader::new(stream).read_message().await.unwrap(),
            Some(message)
        );
    }

    #[tokio::test]
    async fn reader_recovers_after_bad_message() {
        let (mut peer, stream) = tokio::io::duplex(256);
        let message = ServerMessage::ErrorMessage("road closed".into());
        let json = serde_json::to_string(&message).unwrap();
        peer.write_all(format!("not-json\n{json}\n").as_bytes())
            .await
            .unwrap();
        drop(peer);

        let mut reader = ServerReader::new(stream);
        assert!(reader.read_message().await.is_err());
        assert_eq!(reader.read_message().await.unwrap(), Some(message));
    }

    #[tokio::test]
    async fn send_fails_when_peer_is_closed() {
        let (mut writer, reader) = tokio::io::duplex(64);
        drop(reader);

        assert!(
            send_message(
                &mut writer,
                &ClientMessage::SendText {
                    content: "road clear".into(),
                },
            )
            .await
            .is_err()
        );
    }
}
