use crate::model::{HlsAudioStream, HlsStream, HlsSubtitleStream};
use futures::ready;
use pin_project::pin_project;

pub struct UploadedHlsStream {
    pub playlist_upload_result: S3UploadResult,
    pub stream_upload_result: S3UploadResult,
    pub hls_stream: HlsOutputStream,
}

pub struct S3UploadResult {
    pub path: String,
    pub bytes_read: usize,
    pub response_status: u16,
}

pub enum HlsOutputStream {
    Video(HlsStream),
    Audio(HlsAudioStream),
    Subtitle(HlsSubtitleStream),
}

impl HlsOutputStream {
    pub fn stream_file(&self) -> &str {
        match self {
            Self::Video(stream) => &stream.stream_file,
            Self::Audio(stream) => &stream.stream_file,
            Self::Subtitle(stream) => &stream.stream_file,
        }
    }

    pub fn stream_playlist(&self) -> &str {
        match self {
            Self::Video(stream) => &stream.stream_playlist,
            Self::Audio(stream) => &stream.stream_playlist,
            Self::Subtitle(stream) => &stream.stream_playlist,
        }
    }

    pub fn stream_mime_type(&self) -> &'static str {
        match self {
            Self::Video(_) => "video/mp4",
            Self::Audio(_) => "audio/mp4",
            Self::Subtitle(_) => "text/vtt",
        }
    }
}

#[pin_project]
pub struct ByteCountingReader<R> {
    #[pin]
    pub reader: R,
    pub byte_count: usize,
}

impl<R> ByteCountingReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            byte_count: 0,
        }
    }
}

impl<R: tokio::io::AsyncRead> tokio::io::AsyncRead for ByteCountingReader<R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.project();
        let previous_length = buf.filled().len();

        ready!(tokio::io::AsyncRead::poll_read(this.reader, cx, buf,))?;

        *this.byte_count += buf.filled().len() - previous_length;

        std::task::Poll::Ready(Ok(()))
    }
}
