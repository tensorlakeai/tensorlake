//! Neutralize control characters before printing untrusted text.
//!
//! Data from a sandbox (process names, file names, error bodies from the
//! sandbox proxy, ...) is controlled by the code running in it. Printed raw, it
//! can carry ANSI/OSC escape sequences that rewrite the operator's terminal,
//! title, or clipboard. These helpers render every control character as a
//! visible escape such as `\x1b`, so the text stays readable but inert.

use std::borrow::Cow;
use std::fmt::Write;

/// Text for one field of a table or listing. Escapes every control
/// character, including tab and newline, so a value can't forge columns or
/// rows.
pub fn field(text: &str) -> Cow<'_, str> {
    escape(text, false)
}

/// Text for a multi-line message such as an error. Keeps newlines and escapes
/// every other control character.
pub fn message(text: &str) -> Cow<'_, str> {
    escape(text, true)
}

fn escape(text: &str, keep_newlines: bool) -> Cow<'_, str> {
    let is_unsafe = |c: char| c.is_control() && !(keep_newlines && c == '\n');
    if !text.contains(is_unsafe) {
        return Cow::Borrowed(text);
    }
    let mut out = String::with_capacity(text.len() + 8);
    for c in text.chars() {
        if is_unsafe(c) {
            match c {
                '\t' => out.push_str("\\t"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                c if (c as u32) < 0x100 => {
                    // Writing to a String can't fail.
                    write!(out, "\\x{:02x}", c as u32).expect("write to String");
                }
                c => write!(out, "\\u{{{:x}}}", c as u32).expect("write to String"),
            }
        } else {
            out.push(c);
        }
    }
    Cow::Owned(out)
}

#[cfg(test)]
mod tests {
    use super::{field, message};
    use std::borrow::Cow;

    #[test]
    fn plain_and_unicode_text_pass_through() {
        assert!(matches!(field("python3 app.py"), Cow::Borrowed(_)));
        assert_eq!(field("données/日本語 ✓"), "données/日本語 ✓");
        assert_eq!(message("line one\nline two"), "line one\nline two");
    }

    #[test]
    fn escape_sequences_are_neutralized() {
        assert_eq!(
            field("\x1b]52;c;ZWNobyBoaQ==\x07"),
            "\\x1b]52;c;ZWNobyBoaQ==\\x07"
        );
        assert_eq!(field("\x1b]2;owned\x07"), "\\x1b]2;owned\\x07");
        assert_eq!(field("ok\x1b[2K\rfake"), "ok\\x1b[2K\\rfake");
        assert_eq!(field("csi\u{9b}31m"), "csi\\x9b31m");
        assert_eq!(field("del\x7f"), "del\\x7f");
    }

    #[test]
    fn fields_escape_tabs_and_newlines() {
        assert_eq!(field("a\tb\nc"), "a\\tb\\nc");
    }

    #[test]
    fn messages_keep_newlines_but_escape_other_controls() {
        assert_eq!(message("a\tb\nc\rd"), "a\\tb\nc\\rd");
        assert_eq!(message("bad\x1b]52;c;eA==\x07"), "bad\\x1b]52;c;eA==\\x07");
    }
}
