//! Content-Length framed JSON-RPC transport (LSP-style), as used by Emacs's
//! `jsonrpc-process-connection`.

use std::io;

use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Upper bound on a single frame. Protects the bridge from a runaway client.
pub const MAX_FRAME: usize = 64 * 1024 * 1024;

#[derive(Debug)]
pub enum FrameError {
    Eof,
    Io(io::Error),
    /// Header malformed. The stream is still usable if we resync on next frame.
    BadHeader(String),
    TooLarge(usize),
}

impl From<io::Error> for FrameError {
    fn from(e: io::Error) -> Self {
        FrameError::Io(e)
    }
}

/// Read one frame body.
pub async fn read_frame<R: AsyncBufRead + Unpin>(r: &mut R) -> Result<Vec<u8>, FrameError> {
    let mut len: Option<usize> = None;
    let mut line = String::new();
    let mut saw_any = false;
    loop {
        line.clear();
        let n = r.read_line(&mut line).await?;
        if n == 0 {
            return Err(FrameError::Eof);
        }
        let l = line.trim_end_matches(['\r', '\n']);
        if l.is_empty() {
            if !saw_any {
                continue; // tolerate stray blank lines between frames
            }
            break;
        }
        saw_any = true;
        if let Some((k, v)) = l.split_once(':') {
            if k.trim().eq_ignore_ascii_case("content-length") {
                len = Some(
                    v.trim()
                        .parse()
                        .map_err(|_| FrameError::BadHeader(format!("bad length: {v}")))?,
                );
            }
        } else {
            return Err(FrameError::BadHeader(l.to_string()));
        }
    }
    let len = len.ok_or_else(|| FrameError::BadHeader("missing Content-Length".into()))?;
    if len > MAX_FRAME {
        return Err(FrameError::TooLarge(len));
    }
    let mut buf = vec![0; len];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}

pub async fn write_frame<W: AsyncWrite + Unpin>(w: &mut W, body: &[u8]) -> io::Result<()> {
    w.write_all(format!("Content-Length: {}\r\n\r\n", body.len()).as_bytes()).await?;
    w.write_all(body).await?;
    w.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::BufReader;

    #[tokio::test]
    async fn roundtrip() {
        let mut out = Vec::new();
        write_frame(&mut out, b"{\"a\":1}").await.unwrap();
        write_frame(&mut out, "{\"é\":2}".as_bytes()).await.unwrap();
        let mut r = BufReader::new(&out[..]);
        assert_eq!(read_frame(&mut r).await.unwrap(), b"{\"a\":1}");
        assert_eq!(read_frame(&mut r).await.unwrap(), "{\"é\":2}".as_bytes());
        assert!(matches!(read_frame(&mut r).await, Err(FrameError::Eof)));
    }

    #[tokio::test]
    async fn rejects_missing_length() {
        let mut r = BufReader::new(&b"Content-Type: x\r\n\r\n{}"[..]);
        assert!(matches!(read_frame(&mut r).await, Err(FrameError::BadHeader(_))));
    }

    #[tokio::test]
    async fn rejects_oversize() {
        let s = format!("Content-Length: {}\r\n\r\n", MAX_FRAME + 1);
        let mut r = BufReader::new(s.as_bytes());
        assert!(matches!(read_frame(&mut r).await, Err(FrameError::TooLarge(_))));
    }

    #[tokio::test]
    async fn truncated_body_is_error() {
        let mut r = BufReader::new(&b"Content-Length: 10\r\n\r\n{}"[..]);
        assert!(matches!(read_frame(&mut r).await, Err(FrameError::Io(_))));
    }
}
