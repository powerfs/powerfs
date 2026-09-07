//! Content type detection for layout prediction.
//!
//! When the Filer returns `StorageMode::Empty` (no filename-based prediction
//! matched), the client uses the *content* of the first write to decide the
//! layout. Binary content (ELF executables, images, compressed archives,
//! databases, etc.) is much more likely to be large and benefit from
//! striping; text content (config files, scripts, source code) is usually
//! small and stays Inline/Flat.
//!
//! Detection heuristic (in priority order):
//! 1. **Known magic numbers** — ELF, PNG, JPEG, GIF, PDF, ZIP, GZIP, XZ,
//!    ZSTD, BZIP2, WASM, MP4, SQLite, etc. → Binary.
//! 2. **Null byte** — any NUL in the first 4 KB → Binary (text files never
//!    contain NUL).
//! 3. **Printable ratio** — if fewer than 90 % of bytes are printable ASCII
//!    (or common whitespace), treat as Binary; otherwise Text.
//!
//! The goal is *not* perfect MIME detection — it is a cheap, best-effort
//! signal that tips the Empty-state fallback toward Stripe for binary data.

/// Detected content type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ContentType {
    /// Likely a text file (config, script, source, log, …).
    Text,
    /// Likely a binary file (executable, image, archive, database, …).
    Binary,
}

/// How many leading bytes to inspect. 4 KB covers every common magic number
/// and gives a stable printable-ratio sample for small files.
const INSPECT_LEN: usize = 4096;

/// Minimum printable-ratio to classify as Text.
const PRINTABLE_RATIO_THRESHOLD: f32 = 0.90;

/// Detect whether `data` looks like text or binary content.
///
/// See the module docs for the detection heuristic. An empty slice is
/// classified as [`ContentType::Text`] (no evidence of binary content).
pub fn detect_content_type(data: &[u8]) -> ContentType {
    let sample = &data[..data.len().min(INSPECT_LEN)];

    if sample.is_empty() {
        return ContentType::Text;
    }

    // 1. Known magic numbers (strongest signal).
    if has_known_binary_magic(sample) {
        return ContentType::Binary;
    }

    // 2. Null byte — text files never contain NUL.
    if sample.contains(&0) {
        return ContentType::Binary;
    }

    // 3. Printable character ratio.
    let printable = sample.iter().filter(|&&b| is_printable_or_ws(b)).count();
    let ratio = printable as f32 / sample.len() as f32;
    if ratio >= PRINTABLE_RATIO_THRESHOLD {
        ContentType::Text
    } else {
        ContentType::Binary
    }
}

/// A byte is "text-like" if it is printable ASCII (0x20–0x7E) or common
/// whitespace (tab, LF, CR). We deliberately do NOT accept high bytes
/// (UTF-8 multibyte sequences) here because binary files are full of them;
/// the 90 % threshold tolerates a small fraction of non-ASCII bytes (e.g.
/// UTF-8 text with accented characters) while still rejecting binary data.
fn is_printable_or_ws(b: u8) -> bool {
    (0x20..=0x7E).contains(&b) || matches!(b, b'\t' | b'\n' | b'\r')
}

/// Check the leading bytes against a table of well-known binary magic
/// numbers. This is intentionally a *short, high-signal* list — the common
/// file types that PowerFS is likely to store (binaries, images, archives,
/// databases, media).
fn has_known_binary_magic(data: &[u8]) -> bool {
    // Each entry is a (magic_bytes, description) pair.
    const MAGICS: &[(&[u8], &str)] = &[
        // Executables / object files
        (b"\x7fELF", "ELF"),
        (b"MZ", "PE/DOS"),
        (b"\xfe\xed\xfa", "Mach-O"),
        (b"\xcf\xfa\xed\xfe", "Mach-O64"),
        // Images
        (b"\x89PNG\r\n\x1a\n", "PNG"),
        (b"\xff\xd8\xff", "JPEG"),
        (b"GIF87a", "GIF87"),
        (b"GIF89a", "GIF89"),
        (b"BM", "BMP"),
        (b"RIFF", "RIFF/WebP/WAV"),
        (b"II*\x00", "TIFF-LE"),
        (b"MM\x00*", "TIFF-BE"),
        // Archives / compression
        (b"PK\x03\x04", "ZIP"),
        (b"PK\x05\x06", "ZIP-empty"),
        (b"\x1f\x8b", "GZIP"),
        (b"\xfd7zXZ\x00", "XZ"),
        (b"\x28\xb5\x2f\xfd", "ZSTD"),
        (b"BZh", "BZIP2"),
        (b"\x04\x22\x4d\x18", "LZ4"),
        (b"ustar", "TAR"),
        // Documents / data
        (b"%PDF-", "PDF"),
        (b"SQLite format 3\x00", "SQLite"),
        (b"\xd0\xcf\x11\xe0", "OLE2/Office"),
        (b"OggS", "OGG"),
        // WebAssembly
        (b"\x00asm", "WASM"),
        // Parquet / columnar
        (b"PAR1", "Parquet"),
    ];

    for (magic, _desc) in MAGICS {
        if data.len() >= magic.len() && &data[..magic.len()] == *magic {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_is_text() {
        assert_eq!(detect_content_type(b""), ContentType::Text);
    }

    #[test]
    fn plain_text_is_text() {
        assert_eq!(
            detect_content_type(b"hello world\nthis is a config file\n"),
            ContentType::Text
        );
    }

    #[test]
    fn source_code_is_text() {
        let src = b"fn main() {\n    println!(\"hello\");\n}\n";
        assert_eq!(detect_content_type(src), ContentType::Text);
    }

    #[test]
    fn elf_is_binary() {
        assert_eq!(
            detect_content_type(b"\x7fELF\x02\x01\x01\x00"),
            ContentType::Binary
        );
    }

    #[test]
    fn png_is_binary() {
        assert_eq!(
            detect_content_type(b"\x89PNG\r\n\x1a\n\x00\x00"),
            ContentType::Binary
        );
    }

    #[test]
    fn gzip_is_binary() {
        assert_eq!(
            detect_content_type(b"\x1f\x8b\x08\x00"),
            ContentType::Binary
        );
    }

    #[test]
    fn null_byte_is_binary() {
        assert_eq!(detect_content_type(b"hello\x00world"), ContentType::Binary);
    }

    #[test]
    fn mostly_binary_bytes_is_binary() {
        // High ratio of non-printable bytes → binary
        let data: Vec<u8> = (0..=255u8).cycle().take(1024).collect();
        assert_eq!(detect_content_type(&data), ContentType::Binary);
    }

    #[test]
    fn utf8_text_with_accents_is_text() {
        // Mostly ASCII with a few accented chars — realistic for
        // config/source files. Non-ASCII fraction is well below 10 %.
        let line = "# server configuration file\nserver.name=café.example.com\n\
                    server.port=8080\nuser=admin\n# note: résumé of changes\n\
                    timeout=30\nretry=3\n# naïve implementation disabled\n";
        let data = line.repeat(10).into_bytes();
        assert_eq!(detect_content_type(&data), ContentType::Text);
    }
}
