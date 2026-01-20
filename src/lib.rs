#![forbid(unsafe_code)]

//! Async JSON stream reader for selective parsing of large payloads.
//!
//! This crate exposes the same token-based API used in Extract's connectors,
//! enabling efficient streaming reads without deserializing full documents.
//!
//! # Quick start
//! ```no_run
//! use asyncjsonstream::AsyncJsonStreamReader;
//! use std::io::Cursor;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), asyncjsonstream::AsyncJsonStreamReaderError> {
//!     let data = r#"{"status":"success","results":[{"id":1},{"id":2}]}"#;
//!     let mut reader = AsyncJsonStreamReader::new(Cursor::new(data.as_bytes().to_vec()));
//!
//!     while let Some(key) = reader.next_object_entry().await? {
//!         match key.as_str() {
//!             "status" => {
//!                 let status = reader.read_string().await?;
//!                 println!("status={status}");
//!             }
//!             "results" => {
//!                 while reader.start_array_item().await? {
//!                     let obj = reader.deserialize_object().await?;
//!                     println!("id={}", obj["id"]);
//!                 }
//!             }
//!             _ => {}
//!         }
//!     }
//!
//!     Ok(())
//! }
//! ```

use std::str::FromStr;

use bytes::BytesMut;
use serde_json::{Map, Value};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt};
const INTERNAL_BUFFER_SIZE: usize = 8 * 1024; // 8kb

/// Error types that can occur during streaming JSON parsing.
#[derive(Error, Debug)]
pub enum AsyncJsonStreamReaderError {
    /// I/O errors while reading from the stream.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// JSON parsing errors with context and byte position.
    #[error("JSON error: {error}, position: {position}")]
    JsonError { error: String, position: usize },

    /// The stream did not match the expected JSON token.
    #[error("Unexpected token: expected {expected}, found {found}")]
    UnexpectedToken {
        expected: &'static str,
        found: String,
    },

    /// Internal invariant violation in the reader.
    #[error("Internal state error: {message}")]
    InternalState { message: String },
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
enum ReaderState {
    Start,  // Initial state
    Object, // Inside an object, expecting a key or '}'
    Value,  // A key was read, value is next
}

/// Token types emitted by `next_token`.
#[derive(Debug, Eq, PartialEq, Clone)]
pub enum JsonToken {
    /// `{`
    StartObject,
    /// `,`
    EndObjectOrListItem,
    /// `}`
    EndObject,
    /// `[`
    StartArray,
    /// `]`
    EndArray,
    /// Object key.
    Key(String),
    /// String value.
    String(String),
    /// Number value (string form).
    Number(String),
    /// Boolean value.
    Boolean(bool),
    /// Null value.
    Null,
}

/// The main async JSON stream reader for selective parsing.
pub struct AsyncJsonStreamReader<R> {
    reader: R,
    buffer: BytesMut,
    position: usize,
    depth: usize,
    state: ReaderState,
    preserve_buffer: bool,
}

impl<R: AsyncRead + Unpin> AsyncJsonStreamReader<R> {
    /// Create a new stream reader with the default internal buffer size.
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            buffer: BytesMut::with_capacity(INTERNAL_BUFFER_SIZE),
            position: 0,
            depth: 0,
            state: ReaderState::Start,
            preserve_buffer: false,
        }
    }

    /// Peek at the next JSON token without consuming it.
    async fn peek_token(&mut self) -> Result<Option<JsonToken>, AsyncJsonStreamReaderError> {
        let saved_position = self.position;
        let saved_depth = self.depth;
        let saved_state = self.state;
        let saved_preserve_buffer = self.preserve_buffer;

        self.preserve_buffer = true;

        let result = self.next_token().await;

        self.position = saved_position;
        self.depth = saved_depth;
        self.state = saved_state;
        self.preserve_buffer = saved_preserve_buffer;

        result
    }

    /// Read the next JSON token from the stream.
    ///
    /// This is a low-level API; avoid mixing it with `next_object_entry`-based helpers.
    pub async fn next_token(&mut self) -> Result<Option<JsonToken>, AsyncJsonStreamReaderError> {
        loop {
            self.skip_whitespace();

            if self.position >= self.buffer.len() {
                // If we've consumed all data in the buffer, read more
                if !self.fill_buffer().await? {
                    return Ok(None); // End of stream
                }
                continue;
            }

            // Peek at the next character
            let ch = self.buffer[self.position];

            // Parse the appropriate token based on the character
            return Ok(Some(match ch {
                b'{' => {
                    self.position += 1;
                    self.depth += 1;
                    JsonToken::StartObject
                }
                b'}' => {
                    self.position += 1;
                    self.depth -= 1;
                    JsonToken::EndObject
                }
                b'[' => {
                    self.position += 1;
                    self.depth += 1;
                    JsonToken::StartArray
                }
                b']' => {
                    self.position += 1;
                    self.depth -= 1;
                    JsonToken::EndArray
                }
                b'"' => {
                    let s = self.parse_string().await?;

                    // Check if this string is a key (followed by colon)
                    self.skip_whitespace();
                    // Ensure buffer has data to determine if this string is a key that ends right at the buffer boundary
                    self.fill_buffer_if_needed(true).await?;
                    if self.position < self.buffer.len() && self.buffer[self.position] == b':' {
                        self.position += 1; // Skip the colon
                        JsonToken::Key(s)
                    } else {
                        JsonToken::String(s)
                    }
                }
                b',' => {
                    self.position += 1;
                    JsonToken::EndObjectOrListItem
                }
                b'n' => {
                    self.parse_literal("null").await?;
                    JsonToken::Null
                }
                b't' => {
                    self.parse_literal("true").await?;
                    JsonToken::Boolean(true)
                }
                b'f' => {
                    self.parse_literal("false").await?;
                    JsonToken::Boolean(false)
                }
                b'-' | b'0'..=b'9' => {
                    let num = self.parse_number().await?;

                    // According to JSON spec, numbers with leading zeros are invalid unless
                    // they are a single "0" or are followed by a fractional/exponent part.
                    let is_invalid_leading_zero = (num.starts_with('0')
                        && num.len() > 1
                        && !matches!(num.chars().nth(1), Some('.' | 'e' | 'E')))
                        || (num.starts_with("-0")
                            && num.len() > 2
                            && !matches!(num.chars().nth(2), Some('.' | 'e' | 'E')));

                    if is_invalid_leading_zero {
                        return Err(AsyncJsonStreamReaderError::JsonError {
                            error: "Invalid number: leading zeros are not allowed".to_string(),
                            position: self.position,
                        });
                    }

                    JsonToken::Number(num)
                }
                x => {
                    return Err(AsyncJsonStreamReaderError::JsonError {
                        error: format!("Unexpected JSON character: {}", x as char),
                        position: self.position,
                    });
                }
            }));
        }
    }

    /// Fill the buffer with more data from the reader.
    async fn fill_buffer(&mut self) -> Result<bool, AsyncJsonStreamReaderError> {
        if self.preserve_buffer {
            self.buffer.reserve(INTERNAL_BUFFER_SIZE);
        } else {
            // When not preserving, the caller should have consumed the entire buffer.
            if self.position != self.buffer.len() {
                return Err(AsyncJsonStreamReaderError::InternalState {
                    message: format!(
                        "fill_buffer called with an unconsumed buffer: position={}, buffer.len={}",
                        self.position,
                        self.buffer.len()
                    ),
                });
            }

            if !self.buffer.is_empty() {
                // If the buffer grew, replace it with a new one of the original size.
                // Otherwise, just clear it for reuse.
                if self.buffer.capacity() > INTERNAL_BUFFER_SIZE {
                    self.buffer = BytesMut::with_capacity(INTERNAL_BUFFER_SIZE);
                } else {
                    self.buffer.clear();
                }
                self.position = 0;
            }
        }

        Ok(self.reader.read_buf(&mut self.buffer).await? > 0)
    }

    async fn fill_buffer_if_needed(
        &mut self,
        allow_eof: bool,
    ) -> Result<(), AsyncJsonStreamReaderError> {
        if self.position >= self.buffer.len() {
            // Attempt to fill the buffer
            let read_bytes = self.fill_buffer().await?;

            // If we didn't read any bytes, and we're not allowed to EOF, error out
            if !read_bytes && !allow_eof {
                return Err(AsyncJsonStreamReaderError::JsonError {
                    error: "Unexpected EOF while requesting to fill buffer".to_string(),
                    position: self.position,
                });
            }
        }

        Ok(())
    }

    /// Skip whitespace in the buffer.
    fn skip_whitespace(&mut self) {
        while self.position < self.buffer.len() {
            match self.buffer[self.position] {
                b' ' | b'\n' | b'\r' | b'\t' => self.position += 1,
                _ => break,
            }
        }
    }

    /// Parse a JSON string.
    async fn parse_string(&mut self) -> Result<String, AsyncJsonStreamReaderError> {
        // Ensure our position is at a quote
        if self.position >= self.buffer.len() || self.buffer[self.position] != b'"' {
            return Err(AsyncJsonStreamReaderError::UnexpectedToken {
                expected: "\"",
                found: if self.position < self.buffer.len() {
                    format!("{}", self.buffer[self.position] as char)
                } else {
                    "EOF".to_string()
                },
            });
        }

        // Advance past the opening quote
        self.position += 1;

        let mut result_buf: Option<Vec<u8>> = None;
        let mut string_start_in_buffer = self.position;

        loop {
            let searchable_slice = &self.buffer[self.position..];
            let mut special_char_offset = None;

            // Scan for special characters `\` or `"`
            for (i, &byte) in searchable_slice.iter().enumerate() {
                if byte == b'\\' || byte == b'"' {
                    special_char_offset = Some(i);
                    break;
                }
            }

            if let Some(offset) = special_char_offset {
                let special_byte = searchable_slice[offset];
                let current_slice = &searchable_slice[..offset];

                if special_byte == b'"' {
                    // Found closing quote.
                    self.position += offset + 1; // move past string segment and quote
                    return if let Some(mut existing_buf) = result_buf {
                        // We had previous chunks, so append this last one.
                        existing_buf.extend_from_slice(current_slice);
                        String::from_utf8(existing_buf).map_err(|e| {
                            AsyncJsonStreamReaderError::JsonError {
                                error: format!("Invalid UTF-8 in string: {}", e),
                                position: self.position,
                            }
                        })
                    } else {
                        // Fast path: entire string was in one buffer chunk with no escapes.
                        String::from_utf8(current_slice.to_vec()).map_err(|e| {
                            AsyncJsonStreamReaderError::JsonError {
                                error: format!("Invalid UTF-8 in string: {}", e),
                                position: self.position,
                            }
                        })
                    };
                }

                // If we are here, special_byte is `\`
                let buf = if let Some(b) = result_buf.as_mut() {
                    b.extend_from_slice(current_slice);
                    b
                } else {
                    let leading_slice =
                        &self.buffer[string_start_in_buffer..self.position + offset];
                    result_buf.insert(leading_slice.to_vec())
                };

                self.position += offset + 1; // move past segment and `\`
                self.fill_buffer_if_needed(false).await?;

                // Handle the escaped character according to JSON spec
                let escaped_char_selector = self.buffer[self.position];
                self.position += 1; // move past escaped char selector

                match escaped_char_selector {
                    b'"' => buf.push(b'"'),
                    b'\\' => buf.push(b'\\'),
                    b'/' => buf.push(b'/'),
                    b'b' => buf.push(8),  // backspace
                    b'f' => buf.push(12), // formfeed
                    b'n' => buf.push(10), // newline
                    b'r' => buf.push(13), // carriage return
                    b't' => buf.push(9),  // tab
                    b'u' => {
                        let unicode_bytes = self.parse_unicode_escape().await?;
                        buf.extend_from_slice(&unicode_bytes);
                    }
                    _ => {
                        return Err(AsyncJsonStreamReaderError::JsonError {
                            error: format!(
                                "Invalid JSON escape sequence: \\{}",
                                escaped_char_selector as char
                            ),
                            position: self.position,
                        });
                    }
                }

                string_start_in_buffer = self.position; // for next non-escape chunk
                continue;
            }

            // No special characters in this chunk, so we need to read more data.
            let remaining_slice = &self.buffer[string_start_in_buffer..];
            result_buf
                .get_or_insert_with(|| Vec::with_capacity(remaining_slice.len() * 2))
                .extend_from_slice(remaining_slice);
            self.position = self.buffer.len();

            if !self.fill_buffer().await? {
                return Err(AsyncJsonStreamReaderError::JsonError {
                    error: "Unexpected EOF while parsing string".to_string(),
                    position: self.position,
                });
            }
            string_start_in_buffer = self.position;
        }
    }

    async fn parse_unicode_escape(&mut self) -> Result<Vec<u8>, AsyncJsonStreamReaderError> {
        let codepoint = self.read_four_hex_digits().await?;

        let final_codepoint = if (0xD800..=0xDBFF).contains(&codepoint) {
            // High surrogate. Must be followed by \uXXXX for low surrogate.
            self.fill_buffer_if_needed(false).await?;
            if self.buffer[self.position] != b'\\' {
                return Err(AsyncJsonStreamReaderError::JsonError {
                    error: "Unmatched high surrogate in Unicode escape sequence".to_string(),
                    position: self.position,
                });
            }
            self.position += 1;

            self.fill_buffer_if_needed(false).await?;
            if self.buffer[self.position] != b'u' {
                return Err(AsyncJsonStreamReaderError::JsonError {
                    error: "Unmatched high surrogate in Unicode escape sequence".to_string(),
                    position: self.position,
                });
            }
            self.position += 1;

            let low_surrogate = self.read_four_hex_digits().await?;
            if !(0xDC00..=0xDFFF).contains(&low_surrogate) {
                return Err(AsyncJsonStreamReaderError::JsonError {
                    error: format!(
                        "High surrogate not followed by low surrogate. Got {:x}",
                        low_surrogate
                    ),
                    position: self.position - 6,
                });
            }

            let high = codepoint - 0xD800;
            let low = low_surrogate - 0xDC00;
            0x10000 + (high << 10) + low
        } else if (0xDC00..=0xDFFF).contains(&codepoint) {
            // Low surrogate without a preceding high surrogate is an error.
            return Err(AsyncJsonStreamReaderError::JsonError {
                error: "Unmatched low surrogate in Unicode escape sequence".to_string(),
                position: self.position - 4,
            });
        } else {
            codepoint
        };

        let c = std::char::from_u32(final_codepoint).ok_or_else(|| {
            AsyncJsonStreamReaderError::JsonError {
                error: format!("Invalid unicode codepoint: {:x}", final_codepoint),
                position: self.position,
            }
        })?;

        let mut char_bytes_buf = [0u8; 4];
        let encoded_bytes_slice = c.encode_utf8(&mut char_bytes_buf);

        Ok(encoded_bytes_slice.as_bytes().to_vec())
    }

    async fn read_four_hex_digits(&mut self) -> Result<u32, AsyncJsonStreamReaderError> {
        let mut hex_chars = [0u8; 4];

        // Fast path: all 4 hex chars are in the current buffer
        if self.position + 4 <= self.buffer.len() {
            hex_chars.copy_from_slice(&self.buffer[self.position..self.position + 4]);
            self.position += 4;
        } else {
            // Slow path: unicode escape crosses buffer boundary
            for hex_char in &mut hex_chars {
                self.fill_buffer_if_needed(false).await?;
                *hex_char = self.buffer[self.position];
                self.position += 1;
            }
        }

        let hex_str =
            std::str::from_utf8(&hex_chars).map_err(|_| AsyncJsonStreamReaderError::JsonError {
                error: "Invalid UTF-8 in unicode escape sequence".to_string(),
                position: self.position - 4,
            })?;

        u32::from_str_radix(hex_str, 16).map_err(|_| AsyncJsonStreamReaderError::JsonError {
            error: format!("Invalid hex value in unicode escape sequence: {}", hex_str),
            position: self.position - 4,
        })
    }

    /// Parse a JSON number.
    async fn parse_number(&mut self) -> Result<String, AsyncJsonStreamReaderError> {
        let mut result = String::new();

        self.fill_buffer_if_needed(false).await?;

        // Read a negative sign if present
        if self.position < self.buffer.len() && self.buffer[self.position] == b'-' {
            result.push(self.buffer[self.position] as char);
            self.position += 1;
        }

        // Parse integer part
        let mut has_digits = false;
        loop {
            self.fill_buffer_if_needed(true).await?;

            // Reached EOF, stop
            if self.position >= self.buffer.len() {
                break;
            }

            let ch = self.buffer[self.position];
            if ch.is_ascii_digit() {
                result.push(ch as char);
                self.position += 1;
                has_digits = true;
            } else {
                break;
            }
        }

        if !has_digits {
            return Err(AsyncJsonStreamReaderError::JsonError {
                error: format!("InvalidNumber: {result}"),
                position: self.position,
            });
        }

        // Parse fractional part if present
        self.fill_buffer_if_needed(true).await?;
        if self.position < self.buffer.len() && self.buffer[self.position] == b'.' {
            result.push(self.buffer[self.position] as char);
            self.position += 1;

            // Ensure at least one digit after decimal point
            self.fill_buffer_if_needed(true).await?;
            if self.position >= self.buffer.len() {
                return Err(AsyncJsonStreamReaderError::JsonError {
                    error: "InvalidNumber: unexpected EOF after decimal point".to_string(),
                    position: self.position,
                });
            }
            if !self.buffer[self.position].is_ascii_digit() {
                return Err(AsyncJsonStreamReaderError::JsonError {
                    error: format!(
                        "Invalid character after decimal point: {}",
                        self.buffer[self.position] as char
                    ),
                    position: self.position,
                });
            }

            // Read whatever digits remain
            loop {
                self.fill_buffer_if_needed(true).await?;
                if self.position >= self.buffer.len() {
                    break;
                }

                let ch = self.buffer[self.position];
                if !ch.is_ascii_digit() {
                    break;
                }

                result.push(ch as char);
                self.position += 1;
            }
        }

        // Parse exponent if present
        self.fill_buffer_if_needed(true).await?;
        if self.position < self.buffer.len()
            && (self.buffer[self.position] == b'e' || self.buffer[self.position] == b'E')
        {
            result.push(self.buffer[self.position] as char);
            self.position += 1;

            // Parse exponent sign if present
            self.fill_buffer_if_needed(true).await?;
            if self.position < self.buffer.len()
                && (self.buffer[self.position] == b'+' || self.buffer[self.position] == b'-')
            {
                result.push(self.buffer[self.position] as char);
                self.position += 1;
            }

            // Ensure at least one digit in exponent
            self.fill_buffer_if_needed(true).await?;
            if self.position >= self.buffer.len() {
                return Err(AsyncJsonStreamReaderError::JsonError {
                    error: "InvalidNumber: unexpected EOF after exponent".to_string(),
                    position: self.position,
                });
            }
            if !self.buffer[self.position].is_ascii_digit() {
                return Err(AsyncJsonStreamReaderError::JsonError {
                    error: format!(
                        "Invalid character after exponent: {}",
                        self.buffer[self.position] as char
                    ),
                    position: self.position,
                });
            }

            // Read whatever digits remain
            loop {
                self.fill_buffer_if_needed(true).await?;
                if self.position >= self.buffer.len() {
                    break;
                }

                let ch = self.buffer[self.position];
                if !ch.is_ascii_digit() {
                    break;
                }

                result.push(ch as char);
                self.position += 1;
            }
        }

        // If we didn't read any digits, error out
        if result.is_empty() {
            return Err(AsyncJsonStreamReaderError::JsonError {
                error: "InvalidNumber: empty number".to_string(),
                position: self.position,
            });
        }

        Ok(result)
    }

    /// Parse a JSON literal (null, true, false).
    async fn parse_literal(
        &mut self,
        expected: &'static str,
    ) -> Result<(), AsyncJsonStreamReaderError> {
        let expected_bytes = expected.as_bytes();

        for &expected_byte in expected_bytes.iter() {
            // Ensure we have data available
            self.fill_buffer_if_needed(false).await?;

            let actual_byte = self.buffer[self.position];
            if actual_byte != expected_byte {
                return Err(AsyncJsonStreamReaderError::UnexpectedToken {
                    expected,
                    found: format!("{}", actual_byte as char),
                });
            }

            self.position += 1;
        }

        Ok(())
    }

    /// Read the next object key and position the reader on its value.
    pub async fn next_object_entry(
        &mut self,
    ) -> Result<Option<String>, AsyncJsonStreamReaderError> {
        if self.state == ReaderState::Start {
            self.start_object().await?;
            self.state = ReaderState::Object;
        }

        if self.state == ReaderState::Value {
            self.skip_value().await?;
            self.state = ReaderState::Object;
        }

        // Now self.state is ReaderState::Object
        match self.read_key().await? {
            Some(key) => {
                self.state = ReaderState::Value;
                Ok(Some(key))
            }
            None => {
                // End of object. No more keys.
                Ok(None)
            }
        }
    }

    async fn skip_value(&mut self) -> Result<(), AsyncJsonStreamReaderError> {
        let value_start_depth = self.depth;
        let token =
            self.next_token()
                .await?
                .ok_or_else(|| AsyncJsonStreamReaderError::JsonError {
                    error: "Unexpected EOF while skipping value".to_string(),
                    position: self.position,
                })?;

        match token {
            JsonToken::StartObject | JsonToken::StartArray => {
                // next_token increased depth. We need to consume until depth is back to value_start_depth.
                while self.depth > value_start_depth {
                    if self.next_token().await?.is_none() {
                        return Err(AsyncJsonStreamReaderError::JsonError {
                            error: "Unexpected EOF while skipping value".to_string(),
                            position: self.position,
                        });
                    }
                }
            }
            JsonToken::String(_)
            | JsonToken::Number(_)
            | JsonToken::Boolean(_)
            | JsonToken::Null => {
                // simple value, already consumed
            }
            _ => {
                return Err(AsyncJsonStreamReaderError::UnexpectedToken {
                    expected: "a value",
                    found: format!("{token:?}"),
                });
            }
        }
        Ok(())
    }

    /// Skip to a specific key in the current object without descending into nested objects.
    ///
    /// After this returns `Ok(())`, the reader is positioned on the value for `target_key`.
    /// Use `read_*` methods to consume that value before calling `next_object_entry`.
    pub async fn skip_to_key(
        &mut self,
        target_key: &str,
    ) -> Result<(), AsyncJsonStreamReaderError> {
        if self.state == ReaderState::Start {
            self.start_object().await?;
            self.state = ReaderState::Object;
        }

        if self.state == ReaderState::Value {
            self.skip_value().await?;
            self.state = ReaderState::Object;
        }

        let object_depth = self.depth;
        while let Some(token) = self.next_token().await? {
            match token {
                JsonToken::Key(key) if self.depth == object_depth && key == target_key => {
                    self.state = ReaderState::Value;
                    return Ok(());
                }
                JsonToken::EndObject if self.depth < object_depth => break,
                _ => continue,
            }
        }
        Err(AsyncJsonStreamReaderError::JsonError {
            error: "key not found".to_string(),
            position: self.position,
        })
    }

    /// Skip the next object value entirely.
    pub async fn skip_object(&mut self) -> Result<(), AsyncJsonStreamReaderError> {
        let start_depth = self.depth;
        self.start_object().await?;
        while self.depth > start_depth {
            self.next_token().await?;
        }
        self.state = ReaderState::Object;

        Ok(())
    }

    /// Read the next item in an array.
    ///
    /// Returns `true` if an item was read, `false` if the array ended.
    pub async fn start_array_item(&mut self) -> Result<bool, AsyncJsonStreamReaderError> {
        match self.next_token().await? {
            Some(JsonToken::StartArray) => {
                if let Some(JsonToken::EndArray) = self.peek_token().await? {
                    self.next_token().await?;
                    self.state = ReaderState::Object;
                    Ok(false)
                } else {
                    Ok(true)
                }
            }
            Some(JsonToken::EndObjectOrListItem) => Ok(true),
            Some(JsonToken::EndArray) => {
                self.state = ReaderState::Object;
                Ok(false)
            }
            unexpected => Err(AsyncJsonStreamReaderError::UnexpectedToken {
                expected: "[ or , or ]",
                found: format!("{unexpected:?}"),
            }),
        }
    }

    /// Start reading an object.
    pub async fn start_object(&mut self) -> Result<(), AsyncJsonStreamReaderError> {
        match self.next_token().await? {
            Some(JsonToken::StartObject) => {
                self.state = ReaderState::Object;
                Ok(())
            }
            unexpected => Err(AsyncJsonStreamReaderError::UnexpectedToken {
                expected: "{",
                found: format!("{unexpected:?}"),
            }),
        }
    }

    /// Read a string value.
    pub async fn read_string(&mut self) -> Result<String, AsyncJsonStreamReaderError> {
        match self.next_token().await? {
            Some(JsonToken::String(s)) => {
                self.state = ReaderState::Object;
                Ok(s)
            }
            unexpected => Err(AsyncJsonStreamReaderError::UnexpectedToken {
                expected: "string",
                found: format!("{unexpected:?}"),
            }),
        }
    }

    /// Read a nullable string value.
    pub async fn read_nullable_string(
        &mut self,
    ) -> Result<Option<String>, AsyncJsonStreamReaderError> {
        match self.next_token().await? {
            Some(JsonToken::String(s)) => {
                self.state = ReaderState::Object;
                Ok(Some(s))
            }
            Some(JsonToken::Null) => {
                self.state = ReaderState::Object;
                Ok(None)
            }
            unexpected => Err(AsyncJsonStreamReaderError::UnexpectedToken {
                expected: "string",
                found: format!("{unexpected:?}"),
            }),
        }
    }

    /// Read a number value.
    pub async fn read_number<T>(&mut self) -> Result<T, AsyncJsonStreamReaderError>
    where
        T: FromStr,
        <T as FromStr>::Err: std::fmt::Debug,
    {
        match self.next_token().await? {
            Some(JsonToken::Number(n)) => {
                let res = n
                    .parse()
                    .map_err(|e| AsyncJsonStreamReaderError::JsonError {
                        error: format!("Can't parse number {n}: {e:#?}"),
                        position: self.position,
                    });
                self.state = ReaderState::Object;
                Ok(res?)
            }
            unexpected => Err(AsyncJsonStreamReaderError::UnexpectedToken {
                expected: "number",
                found: format!("{unexpected:?}"),
            }),
        }
    }

    /// Read a boolean value.
    pub async fn read_boolean(&mut self) -> Result<bool, AsyncJsonStreamReaderError> {
        match self.next_token().await? {
            Some(JsonToken::Boolean(b)) => {
                self.state = ReaderState::Object;
                Ok(b)
            }
            unexpected => Err(AsyncJsonStreamReaderError::UnexpectedToken {
                expected: "boolean",
                found: format!("{unexpected:?}"),
            }),
        }
    }

    /// Read the next key in an object.
    pub async fn read_key(&mut self) -> Result<Option<String>, AsyncJsonStreamReaderError> {
        loop {
            match self.next_token().await? {
                Some(JsonToken::Key(k)) => {
                    return Ok(Some(k));
                }
                Some(JsonToken::EndObject) => {
                    return Ok(None);
                }
                Some(JsonToken::EndObjectOrListItem) => {
                    continue;
                }
                unexpected => {
                    return Err(AsyncJsonStreamReaderError::UnexpectedToken {
                        expected: "key or end of object",
                        found: format!("{unexpected:?}"),
                    });
                }
            };
        }
    }

    /// Read an object and deserialize it into a JSON map.
    pub async fn deserialize_object(
        &mut self,
    ) -> Result<Map<String, Value>, AsyncJsonStreamReaderError> {
        let start_depth = self.depth;
        let start_pos = self.position;

        self.preserve_buffer = true;
        self.start_object().await?;

        while self.depth > start_depth {
            self.next_token().await?;
        }

        let res = serde_json::from_slice(&self.buffer[start_pos..self.position]).map_err(|e| {
            AsyncJsonStreamReaderError::JsonError {
                error: format!("deserialize_object error: {e:?}"),
                position: start_pos,
            }
        });
        self.preserve_buffer = false;
        self.state = ReaderState::Object;
        res
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use std::io::Cursor;

    // Helper to create a reader from a JSON string
    fn create_reader(json: &str) -> AsyncJsonStreamReader<Cursor<Vec<u8>>> {
        AsyncJsonStreamReader::new(Cursor::new(json.as_bytes().to_vec()))
    }

    #[tokio::test]
    async fn test_empty_json_object() {
        let mut reader = create_reader("{}");

        assert_eq!(
            reader.next_token().await.unwrap(),
            Some(JsonToken::StartObject)
        );
        assert_eq!(
            reader.next_token().await.unwrap(),
            Some(JsonToken::EndObject)
        );
        assert_eq!(reader.next_token().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_empty_json_array() {
        let mut reader = create_reader("[]");

        assert_eq!(
            reader.next_token().await.unwrap(),
            Some(JsonToken::StartArray)
        );
        assert_eq!(
            reader.next_token().await.unwrap(),
            Some(JsonToken::EndArray)
        );
        assert_eq!(reader.next_token().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_simple_key_value_pair() {
        let mut reader = create_reader(r#"{"name": "test"}"#);

        let key = reader.next_object_entry().await.unwrap().unwrap();
        assert_eq!(key, "name");
        assert_eq!(reader.read_string().await.unwrap(), "test");

        assert!(reader.next_object_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_nested_objects() {
        let mut reader = create_reader(r#"{"outer": {"inner": 42}}"#);

        let key = reader.next_object_entry().await.unwrap().unwrap();
        assert_eq!(key, "outer");

        // The value for 'outer' is a JSON object. We can deserialize it entirely.
        let inner_obj = reader.deserialize_object().await.unwrap();
        assert_eq!(inner_obj.get("inner").unwrap().as_i64().unwrap(), 42);

        // After deserializing, the next entry should be None (end of outer object).
        assert!(reader.next_object_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_array_with_values() {
        let mut reader = create_reader(r#"[1, "text", true, null]"#);

        assert_eq!(
            reader.next_token().await.unwrap(),
            Some(JsonToken::StartArray)
        );
        assert_eq!(
            reader.next_token().await.unwrap(),
            Some(JsonToken::Number("1".to_string()))
        );
        assert_eq!(
            reader.next_token().await.unwrap(),
            Some(JsonToken::EndObjectOrListItem)
        );
        assert_eq!(
            reader.next_token().await.unwrap(),
            Some(JsonToken::String("text".to_string()))
        );
        assert_eq!(
            reader.next_token().await.unwrap(),
            Some(JsonToken::EndObjectOrListItem)
        );
        assert_eq!(
            reader.next_token().await.unwrap(),
            Some(JsonToken::Boolean(true))
        );
        assert_eq!(
            reader.next_token().await.unwrap(),
            Some(JsonToken::EndObjectOrListItem)
        );
        assert_eq!(reader.next_token().await.unwrap(), Some(JsonToken::Null));
        assert_eq!(
            reader.next_token().await.unwrap(),
            Some(JsonToken::EndArray)
        );
        assert_eq!(reader.next_token().await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_next_object_entry_skip_value() {
        let mut reader = create_reader(r#"{"a": 1, "b": {"foo": "bar"}, "c": 3}"#);

        // Read a
        let key = reader.next_object_entry().await.unwrap().unwrap();
        assert_eq!(key, "a");
        assert_eq!(reader.read_number::<i32>().await.unwrap(), 1);

        // Skip b by not reading it and just calling next_entry again
        let key = reader.next_object_entry().await.unwrap().unwrap();
        assert_eq!(key, "b");
        // We don't read the value of "b", next_entry() will skip it.

        // Read c
        let key = reader.next_object_entry().await.unwrap().unwrap();
        assert_eq!(key, "c");
        assert_eq!(reader.read_number::<i32>().await.unwrap(), 3);

        assert!(reader.next_object_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_skip_value_object() {
        let mut reader = create_reader(r#"{"name": {"first": "John", "last": "Doe"}, "age": 30}"#);

        let key = reader.next_object_entry().await.unwrap().unwrap();
        assert_eq!(key, "name");

        // Skip the entire nested object for "name" by calling skip_object
        reader.skip_object().await.unwrap();

        let key = reader.next_object_entry().await.unwrap().unwrap();
        assert_eq!(key, "age");
        assert_eq!(reader.read_number::<i32>().await.unwrap(), 30);
    }

    #[tokio::test]
    async fn test_read_methods() {
        let mut reader = create_reader(
            r#"{"str": "hello", "num": 42, "float": 1.337, "exp": -1.24e+10, "bool": true}"#,
        );

        let key = reader.next_object_entry().await.unwrap().unwrap();
        assert_eq!(key, "str");
        assert_eq!(reader.read_string().await.unwrap(), "hello");

        let key = reader.next_object_entry().await.unwrap().unwrap();
        assert_eq!(key, "num");
        assert_eq!(reader.read_number::<i64>().await.unwrap(), 42);

        let key = reader.next_object_entry().await.unwrap().unwrap();
        assert_eq!(key, "float");
        assert_eq!(reader.read_number::<f64>().await.unwrap(), 1.337_f64);

        let key = reader.next_object_entry().await.unwrap().unwrap();
        assert_eq!(key, "exp");
        assert_eq!(reader.read_number::<f64>().await.unwrap(), -1.24e+10);

        let key = reader.next_object_entry().await.unwrap().unwrap();
        assert_eq!(key, "bool");
        assert!(reader.read_boolean().await.unwrap());

        // No more keys
        assert!(reader.next_object_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_read_nullable_string_null_resets_state() {
        let mut reader = create_reader(r#"{"a": null, "b": "ok"}"#);

        let key = reader.next_object_entry().await.unwrap().unwrap();
        assert_eq!(key, "a");
        assert!(reader.read_nullable_string().await.unwrap().is_none());

        let key = reader.next_object_entry().await.unwrap().unwrap();
        assert_eq!(key, "b");
        assert_eq!(reader.read_string().await.unwrap(), "ok");

        assert!(reader.next_object_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_skip_to_key_ignores_nested_keys() {
        let mut reader = create_reader(r#"{"outer":{"id":1},"id":2,"tail":3}"#);

        reader.skip_to_key("id").await.unwrap();
        assert_eq!(reader.read_number::<i32>().await.unwrap(), 2);

        let key = reader.next_object_entry().await.unwrap().unwrap();
        assert_eq!(key, "tail");
        assert_eq!(reader.read_number::<i32>().await.unwrap(), 3);
    }

    #[tokio::test]
    async fn test_skip_value_errors_on_unexpected_eof() {
        let mut reader = create_reader(r#"{"a":{"b":1"#);

        let key = reader.next_object_entry().await.unwrap().unwrap();
        assert_eq!(key, "a");

        let err = reader.next_object_entry().await.unwrap_err();
        match err {
            AsyncJsonStreamReaderError::JsonError { error, .. } => {
                assert!(error.contains("Unexpected EOF while skipping value"));
            }
            _ => panic!("Expected JsonError for unexpected EOF"),
        }
    }

    #[tokio::test]
    async fn test_fill_buffer_invariant_error() {
        let mut reader = AsyncJsonStreamReader::new(Cursor::new(b"{}".to_vec()));
        reader.buffer.extend_from_slice(b"{}");
        reader.position = 0;

        let err = reader.fill_buffer().await.unwrap_err();
        match err {
            AsyncJsonStreamReaderError::InternalState { message } => {
                assert!(message.contains("unconsumed buffer"));
            }
            _ => panic!("Expected InternalState error"),
        }
    }

    #[tokio::test]
    async fn test_error_handling() {
        // Test invalid JSON
        let mut reader = create_reader(r#"{"missing_quote: 42}"#);
        assert!(reader.next_token().await.is_ok());
        assert!(reader.next_token().await.is_err());

        // // Test unexpected token
        // let mut reader = create_reader(r#"{"key": ]"#);
        // assert_eq!(
        //     reader.next_token().await.unwrap(),
        //     Some(JsonToken::StartObject)
        // );
        // assert_eq!(
        //     reader.next_token().await.unwrap(),
        //     Some(JsonToken::Key("key".to_string()))
        // );
        // assert_matches!(
        //     reader.next_token().await,
        //     Err(StreamingJsonError::UnexpectedToken { .. })
        // );
    }

    #[tokio::test]
    async fn test_user_example_scenario() {
        let json = r#"{"status":"success","blah":1234,"results":[{"name":"John","age":30},{"name":"Jane","age":25}]}"#;
        let mut reader = create_reader(json);

        let mut status = None;
        let mut results = Vec::new();

        while let Some(key) = reader.next_object_entry().await.unwrap() {
            match key.as_str() {
                "status" => {
                    status = Some(reader.read_string().await.unwrap());
                }
                "results" => {
                    while reader.start_array_item().await.unwrap() {
                        let obj = reader.deserialize_object().await.unwrap();
                        results.push(obj);
                    }
                }
                "blah" => {
                    // By not reading the value, we are testing that `next_entry` will
                    // correctly skip it before reading the next key.
                }
                _ => panic!("Unexpected key"),
            }
        }

        assert_eq!(status, Some("success".to_string()));
        assert_eq!(results.len(), 2);

        let john = &results[0];
        assert_eq!(john.get("name").unwrap().as_str().unwrap(), "John");
        assert_eq!(john.get("age").unwrap().as_i64().unwrap(), 30);

        let jane = &results[1];
        assert_eq!(jane.get("name").unwrap().as_str().unwrap(), "Jane");
        assert_eq!(jane.get("age").unwrap().as_i64().unwrap(), 25);
    }

    #[tokio::test]
    async fn test_empty_array_in_object() {
        let json = r#"{"status":"success","data":[],"count":0}"#;
        let mut reader = create_reader(json);

        let mut status = None;
        let mut data_items_count = 0;
        let mut count = None;

        while let Some(key) = reader.next_object_entry().await.unwrap() {
            match key.as_str() {
                "status" => {
                    status = Some(reader.read_string().await.unwrap());
                }
                "data" => {
                    while reader.start_array_item().await.unwrap() {
                        data_items_count += 1;
                        let _ = reader.deserialize_object().await.unwrap();
                    }
                }
                "count" => {
                    count = Some(reader.read_number::<i32>().await.unwrap());
                }
                _ => panic!("Unexpected key"),
            }
        }

        assert_eq!(status, Some("success".to_string()));
        assert_eq!(data_items_count, 0); // Empty array should not iterate
        assert_eq!(count, Some(0));
    }

    #[tokio::test]
    async fn test_deserialize_large_object() {
        // This test confirms that deserialize_object can now handle objects
        // larger than the internal buffer thanks to the buffer growth logic.
        let long_string = "a".repeat(INTERNAL_BUFFER_SIZE);
        let key_string = "long_key";
        let large_object_json =
            format!(r#"{{"key": "value", "{}": "{}"}}"#, key_string, long_string);
        let full_json = format!(r#"{{"results": [{}]}}"#, large_object_json);

        let mut reader = create_reader(&full_json);

        let key = reader.next_object_entry().await.unwrap().unwrap();
        assert_eq!(key, "results");

        // --- First object ---
        assert!(reader.start_array_item().await.unwrap());
        let obj1 = reader.deserialize_object().await.unwrap();
        assert_eq!(obj1.get("key").unwrap().as_str().unwrap(), "value");
        assert_eq!(obj1.get(key_string).unwrap().as_str().unwrap(), long_string);

        // End of array
        assert!(!reader.start_array_item().await.unwrap());

        // End of main object
        assert!(reader.next_object_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_key_crosses_chunk_boundary() {
        // Simulate the key ending at the very end of one chunk with the ':' in the next chunk.
        let part1 = b"{\"foo\"".to_vec();
        let part2 = b": \"bar\"}".to_vec();

        let reader_p1 = Cursor::new(part1);
        let reader_p2 = Cursor::new(part2);

        let chained_reader = tokio::io::AsyncReadExt::chain(reader_p1, reader_p2);
        let mut reader = AsyncJsonStreamReader::new(chained_reader);

        let key = reader.next_object_entry().await.unwrap().unwrap();
        assert_eq!(key, "foo");
        let value = reader.read_string().await.unwrap();
        assert_eq!(value, "bar");

        assert!(reader.next_object_entry().await.unwrap().is_none());
    }

    use serde::Deserialize;
    use std::time::Instant;

    fn generate_large_json(num_entries: usize) -> String {
        let mut results = Vec::with_capacity(num_entries);
        for i in 0..num_entries {
            results.push(format!(
                r#"{{"id": {}, "name": "name_{}", "extra_data": "some_long_string_to_ignore_{}", "value": {}, "is_active": {}}}"#,
                i,
                i,
                "x".repeat(100), // Filler data to be skipped
                i as f64 * 1.1,
                if i % 2 == 0 { "true" } else { "false" }
            ));
        }
        format!(
            r#"{{"status":"success","useless_data": "{}","results":[{}]}}"#,
            "y".repeat(1024), // More filler data
            results.join(",")
        )
    }

    #[tokio::test]
    #[ignore]
    async fn performance_test_very_large_json_selective_parsing() {
        const NUM_ENTRIES: usize = 500_000; // ~112MB JSON
        let json_data = generate_large_json(NUM_ENTRIES);
        let json_bytes = json_data.as_bytes().to_vec();

        // --- Benchmark AsyncJsonStreamReader ---
        println!("\n--- Performance Test: Selective Parsing (500k entries, ~112MB) ---");
        println!(
            "Parsing {} entries from a JSON of size {} bytes.",
            NUM_ENTRIES,
            json_data.len()
        );

        let start_async = Instant::now();

        let mut reader = create_reader(&json_data);
        let mut found_ids = Vec::with_capacity(NUM_ENTRIES);

        while let Some(key) = reader.next_object_entry().await.unwrap() {
            if key == "results" {
                while reader.start_array_item().await.unwrap() {
                    reader.start_object().await.unwrap();
                    let mut id = None;
                    while let Some(inner_key) = reader.read_key().await.unwrap() {
                        if inner_key == "id" {
                            id = Some(reader.read_number::<u64>().await.unwrap());
                        } else {
                            reader.skip_value().await.unwrap();
                        }
                    }
                    if let Some(id_val) = id {
                        found_ids.push(id_val);
                    }
                }
            }
        }
        let duration_async = start_async.elapsed();
        assert_eq!(found_ids.len(), NUM_ENTRIES);
        println!(
            "1. AsyncJsonStreamReader (manual parsing):  {:?}",
            duration_async
        );

        // --- Benchmark serde_json (deserializing to a struct that only contains the needed fields) ---
        #[derive(Deserialize)]
        struct ResultEntry {
            id: u64,
        }

        #[derive(Deserialize)]
        struct Response {
            results: Vec<ResultEntry>,
        }

        let start_serde_struct = Instant::now();
        let parsed: Response = serde_json::from_slice(&json_bytes).unwrap();
        let serde_ids: Vec<u64> = parsed.results.into_iter().map(|r| r.id).collect();
        let duration_serde_struct = start_serde_struct.elapsed();
        assert_eq!(serde_ids.len(), NUM_ENTRIES);
        println!(
            "2. serde_json (selective struct):          {:?}",
            duration_serde_struct
        );

        // --- Benchmark serde_json (deserializing to Value and then extracting) ---
        let start_serde_value = Instant::now();

        let parsed_value: serde_json::Value = serde_json::from_slice(&json_bytes).unwrap();
        let result_ids: Vec<u64> = parsed_value["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v["id"].as_u64().unwrap())
            .collect();

        let duration_serde_value = start_serde_value.elapsed();
        assert_eq!(result_ids.len(), NUM_ENTRIES);
        println!(
            "3. serde_json (generic Value):             {:?}",
            duration_serde_value
        );
    }

    #[tokio::test]
    async fn test_string_with_escapes() {
        let json = r#"{"key": "hello\nworld\t\"escaped quote\\final backslash\u0041BC"}"#;
        let mut reader = create_reader(json);

        let key = reader.next_object_entry().await.unwrap().unwrap();
        assert_eq!(key, "key");

        let expected_string = "hello\nworld\t\"escaped quote\\final backslashABC";
        let parsed_string = reader.read_string().await.unwrap();

        assert_eq!(parsed_string, expected_string);
    }

    #[tokio::test]
    async fn test_complex_json_correctness_vs_serde() {
        let complex_json = r#"
        {
            "string_key": "value",
            "int_key": 123,
            "float_key": -45.67,
            "scientific_notation": 1.23e-4,
            "bool_true": true,
            "bool_false": false,
            "null_key": null,
            "empty_string": "",
            "whitespace_string": "   \n\t   ",
            "escaped_string": "line1\nline2\tline3\"quote\\slash/",
            "unicode_string": "Alpha: \u03b1, Smiley: \uD83D\uDE00",
            "empty_object": {},
            "empty_array": [],
            "nested_object": {
                "a": 1,
                "b": "two",
                "c": [true, false, null]
            },
            "array_of_objects": [
                {"id": 1, "tags": ["A", "B"]},
                {"id": 2, "tags": ["C"]},
                {}
            ],
            "array_with_mixed_types": [
                "string",
                100,
                -1.1,
                true,
                null,
                {},
                []
            ],
            "key with spaces and symbols!@#$%^&*()": "value for complex key"
        }
        "#;

        // Use our stream reader. Note that deserialize_object itself uses next_token()
        // repeatedly, so this is an excellent integration test for the tokenizing logic.
        let mut reader = create_reader(complex_json);
        let our_result = reader.deserialize_object().await.unwrap();

        // Use serde_json as the ground truth
        let serde_result: Value = serde_json::from_str(complex_json).unwrap();
        let serde_map = serde_result.as_object().unwrap();

        assert_eq!(our_result, *serde_map);
    }

    #[tokio::test]
    async fn test_deeply_nested_object() {
        let mut json = "{\"key\": ".to_string();
        let depth = 50;
        for _ in 0..depth {
            json.push_str("{\"a\":");
        }
        json.push_str("42");
        for _ in 0..depth {
            json.push('}');
        }
        json.push('}');

        let mut reader = create_reader(&json);

        let key = reader.next_object_entry().await.unwrap().unwrap();
        assert_eq!(key, "key");

        // Deserialize the deeply nested object
        let obj = reader.deserialize_object().await.unwrap();

        // Verify some part of it
        let mut current = obj.get("a").unwrap();
        for _ in 0..depth - 1 {
            current = current.as_object().unwrap().get("a").unwrap();
        }
        assert_eq!(current.as_i64().unwrap(), 42);

        // Ensure we are at the end of the stream
        assert!(reader.next_object_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_invalid_unicode_hex_error() {
        let json = r#"{"key": "\u123G"}"#; // 'G' is not a valid hex digit
        let mut reader = create_reader(json);

        let key = reader.next_object_entry().await.unwrap().unwrap();
        assert_eq!(key, "key");

        let result = reader.read_string().await;

        assert_matches!(
            result,
            Err(AsyncJsonStreamReaderError::JsonError { error, .. }) if error.contains("Invalid hex value in unicode escape sequence")
        );
    }

    #[tokio::test]
    async fn test_number_parsing() {
        // Helper to check for a successfully parsed number
        async fn expect_number(json: &str, expected_num_str: &str) {
            let mut reader = create_reader(json);
            assert_matches!(
                reader.next_token().await.unwrap(),
                Some(JsonToken::Number(s)) if s == expected_num_str
            );
            // Ensure no trailing tokens
            assert_matches!(reader.next_token().await.unwrap(), None);
        }

        // Helper to check for a parse failure on the first token
        async fn expect_error_on_first_token(json: &str, expected_error_part: &str) {
            let mut reader = create_reader(json);
            let result = reader.next_token().await;
            assert_matches!(
                result,
                Err(e) if format!("{:?}", e).contains(expected_error_part)
            );
        }

        // --- Valid integers ---
        expect_number("0", "0").await;
        expect_number("123", "123").await;
        expect_number("-123", "-123").await;
        expect_number("-0", "-0").await; // Allowed by JSON spec, often read as 0.

        // --- Valid floats ---
        expect_number("0.123", "0.123").await;
        expect_number("-0.123", "-0.123").await;
        expect_number("123.456", "123.456").await;

        // --- Valid scientific notation ---
        expect_number("1e10", "1e10").await;
        expect_number("1E10", "1E10").await;
        expect_number("1e+10", "1e+10").await;
        expect_number("1e-10", "1e-10").await;
        expect_number("1.23e+10", "1.23e+10").await;
        expect_number("-1.23e-10", "-1.23e-10").await;
        expect_number("0e0", "0e0").await;

        // --- Invalid numbers (spec violations) ---
        expect_error_on_first_token("01", "leading zeros are not allowed").await;
        expect_error_on_first_token("-01", "leading zeros are not allowed").await;
        expect_error_on_first_token("1.", "unexpected EOF after decimal point").await;
        expect_error_on_first_token("1.e10", "Invalid character after decimal point").await;
        expect_error_on_first_token("1e", "unexpected EOF after exponent").await;
        expect_error_on_first_token("1E+", "unexpected EOF after exponent").await;
        expect_error_on_first_token("-", "InvalidNumber").await;
        expect_error_on_first_token("--1", "InvalidNumber").await; // Parses first '-', then errors.
    }

    #[tokio::test]
    async fn test_invalid_number_sequence() {
        let mut reader = create_reader("1.2.3");
        assert_matches!(reader.next_token().await.unwrap(), Some(JsonToken::Number(s)) if s == "1.2");
        assert_matches!(reader.next_token().await, Err(AsyncJsonStreamReaderError::JsonError { error, .. }) if error.contains("Unexpected JSON character: ."));

        let mut reader = create_reader("1a");
        assert_matches!(reader.next_token().await.unwrap(), Some(JsonToken::Number(s)) if s == "1");
        assert_matches!(reader.next_token().await, Err(AsyncJsonStreamReaderError::JsonError { error, .. }) if error.contains("Unexpected JSON character: a"));
    }

    #[tokio::test]
    async fn test_number_crosses_chunk_boundary() {
        async fn check_number_boundary(part1: &str, part2: &str, expected: &str) {
            let reader_p1 = Cursor::new(part1.as_bytes().to_vec());
            let reader_p2 = Cursor::new(part2.as_bytes().to_vec());
            let chained_reader = tokio::io::AsyncReadExt::chain(reader_p1, reader_p2);
            let mut reader = AsyncJsonStreamReader::new(chained_reader);

            assert_matches!(
                reader.next_token().await.unwrap(),
                Some(JsonToken::Number(s)) if s == expected
            );
        }

        check_number_boundary("-", "123", "-123").await;
        check_number_boundary("123", ".456", "123.456").await;
        check_number_boundary("123.", "456", "123.456").await;
        check_number_boundary("123.456", "e10", "123.456e10").await;
        check_number_boundary("123.456e", "+10", "123.456e+10").await;
        check_number_boundary("123.456e+", "10", "123.456e+10").await;
    }
}
