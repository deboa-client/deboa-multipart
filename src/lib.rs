#[cfg(feature = "single-threaded")]
use async_lock::Mutex;
#[cfg(feature = "multi-threaded")]
use async_lock::RwLock;
use async_stream::try_stream;
#[cfg(feature = "single-threaded")]
use bytes::Bytes;
#[cfg(feature = "multi-threaded")]
use bytes::Bytes;
use bytes::BytesMut;
use futures::{StreamExt as _, TryStreamExt as _};
#[cfg(feature = "single-threaded")]
use http_body::Frame;
#[cfg(feature = "multi-threaded")]
use http_body::Frame;
use rand::distr::{Alphanumeric, SampleString as _};
use std::pin::Pin;
#[cfg(feature = "single-threaded")]
use std::rc::Rc;
#[cfg(feature = "multi-threaded")]
use std::sync::Arc;

#[cfg(test)]
mod tests;

pub(self) const MULTIPART_FORM_TYPE: &str = "multipart/form-data";
pub(self) const CRLF: &[u8] = b"\r\n";

/// Type to hold a stream of binary content
pub struct MimeStream {
    name: String,
    file_name: String,
    mime_type: String,
    inner: Pin<Box<dyn MultiPartFormStream>>,
}

/// Form parts
pub enum Part {
    /// String part
    String((String, String)),
    /// Streeam part (files)
    Stream(MimeStream),
}

impl Part {
    /// Create a string part
    pub fn string(key: &str, value: &str) -> Part {
        Part::String((key.into(), value.into()))
    }

    /// Create a stream part
    pub fn stream(stream: MimeStream) -> Part {
        Part::Stream(stream)
    }
}

#[cfg(feature = "multi-threaded")]
/// Trait bounds alias for async typed matchers
pub trait MultiPartFormStream:
    futures::Stream<Item = std::result::Result<Frame<Bytes>, std::io::Error>> + Send + Sync
{
}
#[cfg(feature = "multi-threaded")]
impl<T> MultiPartFormStream for T where
    T: futures::Stream<Item = std::result::Result<Frame<Bytes>, std::io::Error>> + Send + Sync
{
}

#[cfg(feature = "single-threaded")]
/// Trait bounds alias for async typed matchers
pub trait MultiPartFormStream:
    futures::Stream<Item = std::result::Result<Frame<Bytes>, std::io::Error>>
{
}
#[cfg(feature = "single-threaded")]
impl<T> MultiPartFormStream for T where
    T: futures::Stream<Item = std::result::Result<Frame<Bytes>, std::io::Error>>
{
}

#[cfg(feature = "multi-threaded")]
/// Multipart form
pub struct MultiPartForm {
    fields: Arc<RwLock<Vec<Part>>>,
    boundary: String,
}

#[cfg(feature = "single-threaded")]
/// Multipart form
pub struct MultiPartForm {
    fields: Rc<Mutex<Vec<Part>>>,
    boundary: String,
}

/// Implement the builder pattern for MultiPartForm.
///
/// # Returns
///
/// * `Self` - The multi part form.
///
/// # Examples
///
/// ```compile_fail
/// use deboa::form::MultiPartForm;
///
/// let mut client = Deboa::default();
/// let mut form = MultiPartForm::builder();
/// form.field("name", "deboa");
/// form.field("version", "0.0.1");
///
/// let request = DeboaRequest::post("https://example.com/register")?
///     .form(form.into())
///     .build()?;
///
/// let mut response = client.execute(request).await?;
/// ```
impl MultiPartForm {
    #[inline]
    /// Create a new multi part form.
    ///
    /// # Returns
    ///
    /// * `Self` - The multi part form.
    pub fn builder() -> Self {
        let boundary = Alphanumeric.sample_string(&mut rand::rng(), 10);
        #[cfg(feature = "multi-threaded")]
        let fields = Arc::new(RwLock::new(Vec::new()));
        #[cfg(feature = "single-threaded")]
        let fields = Rc::new(Mutex::new(Vec::new()));
        Self {
            fields,
            boundary: format!("DeboaFormBdry{}", boundary),
        }
    }

    #[inline]
    /// Returns form content-type
    ///
    /// # Returns
    ///
    /// * `String` - Form content-type
    pub fn content_type(&self) -> String {
        format!("{}; boundary={}", MULTIPART_FORM_TYPE, self.boundary)
    }

    #[inline]
    /// Get the boundary of the form.
    ///
    /// # Returns
    ///
    /// * `String` - The boundary.
    pub fn boundary(&self) -> String {
        self.boundary.to_string()
    }

    #[inline]
    /// Allow add a new form field, a string one mainly
    ///
    /// # Arguments
    ///
    /// * `key` - Field name
    /// * `value` - Field content
    ///
    /// # Returns
    ///
    /// * `Self` - Form reference for chained calls
    pub fn field(self, name: &str, value: &str) -> Self {
        #[cfg(feature = "multi-threaded")]
        self.fields
            .write_arc_blocking()
            .push(Part::string(name, value));
        #[cfg(feature = "single-threaded")]
        self.fields.lock_blocking().push(Part::string(name, value));
        self
    }

    #[inline]
    /// Add a stream to the form.
    ///
    /// # Arguments
    ///
    /// * `key` - The key.
    /// * `value` - The value.
    ///
    /// # Returns
    ///
    /// * `&mut Self` - The form.
    ///
    pub fn stream<S>(self, name: &str, file_name: &str, mime_type: &str, stream: S) -> Self
    where
        S: futures::Stream<Item = Result<Bytes, std::io::Error>> + Send + Sync + 'static,
    {
        let stream = Part::stream(MimeStream {
            name: name.into(),
            file_name: file_name.into(),
            mime_type: mime_type.into(),
            inner: Box::pin(stream.map_ok(Frame::data)),
        });
        #[cfg(feature = "multi-threaded")]
        self.fields.write_arc_blocking().push(stream);
        #[cfg(feature = "single-threaded")]
        self.fields.lock_blocking().push(stream);
        self
    }

    /// Build the form, returning it as a stream of Frame<Bytes>
    ///
    /// # Returns
    ///
    /// * `impl futures::Stream<Item = Result<Frame<Bytes>, Error>>` - Form content as a stream of Frame<Bytes>
    ///
    /// # Notes
    ///
    /// As opposite to EncodedForm, multipart doesn't have a quick way to convert to a HttṕBody
    /// you need call [http-body-utils::HttpBody::stream()]
    pub async fn build(self) -> Pin<Box<dyn MultiPartFormStream + 'static>> {
        let boundary = self.boundary;
        let fields = self.fields.clone();
        let stream = try_stream! {
            let mut boundary_prefix_buf = BytesMut::new();
            boundary_prefix_buf.extend_from_slice(b"--");
            boundary_prefix_buf.extend_from_slice(boundary.as_bytes());
            boundary_prefix_buf.extend_from_slice(CRLF);
            yield Frame::data(boundary_prefix_buf.freeze());

            #[cfg(feature = "multi-threaded")]
            let count = fields.read_arc().await.len();
            #[cfg(feature = "multi-threaded")]
            let mut fields = fields.write_arc().await;

            #[cfg(feature = "single-threaded")]
            let count = fields.lock().await.len();
            #[cfg(feature = "single-threaded")]
            let mut fields = fields.lock().await;

            for (index, part) in fields
                .iter_mut()
                .enumerate()
            {
                match part {
                    Part::String(field) => {
                        let mut buf = BytesMut::new();
                        buf.extend_from_slice(b"Content-Disposition: form-data; name=\"");
                        buf.extend_from_slice(field.1.as_bytes());
                        buf.extend_from_slice(b"\"");
                        buf.extend_from_slice(CRLF);
                        buf.extend_from_slice(CRLF);
                        buf.extend_from_slice(field.1.as_bytes());
                        buf.extend_from_slice(CRLF);
                        yield Frame::data(buf.freeze())
                    },
                    Part::Stream(mime_stream) => {
                        let mut buf = BytesMut::new();
                        buf.extend_from_slice(b"Content-Disposition: form-data; name=\"");
                        buf.extend_from_slice(mime_stream.name.as_bytes());
                        buf.extend_from_slice(b"\";");
                        buf.extend_from_slice(b"filename=\"");
                        buf.extend_from_slice(mime_stream.file_name.as_bytes());
                        buf.extend_from_slice(b"\";");
                        buf.extend_from_slice(CRLF);
                        buf.extend_from_slice(b"Content-Type: ");
                        buf.extend_from_slice(mime_stream.mime_type.as_bytes());
                        buf.extend_from_slice(CRLF);
                        buf.extend_from_slice(CRLF);
                        yield Frame::data(buf.freeze());

                        while let Some(Ok(chunk)) = mime_stream.inner.next().await
                        {
                            yield chunk;
                        }
                        yield Frame::data(CRLF.into());
                    },
                }

                let mut buf = BytesMut::new();
                let ending = if index != count - 1 { CRLF } else { b"--\r\n" };
                buf.extend_from_slice(b"--");
                buf.extend_from_slice(boundary.as_bytes());
                buf.extend_from_slice(ending);
                yield Frame::data(buf.freeze())
            }
        };

        Box::pin(stream)
    }
}
