use std::collections::VecDeque;

use super::models::{OutputEvent, RunProcessEvent};

/// The last bytes of command output under a combined stdout/stderr limit.
/// Output is coalesced into at most one event per stream, followed by the last
/// valid exit event. Per-line timestamps are not preserved by this collector.
#[derive(Debug)]
pub struct LimitedRunProcessOutput {
    pub events: Vec<RunProcessEvent>,
    pub output_truncated: bool,
}

/// Retain a bounded tail of process events, sharing the byte budget across
/// stdout and stderr while preserving their labels in the result.
pub struct OutputCollector {
    limit: usize,
    bytes: VecDeque<u8>,
    // One stream tag per retained byte keeps storage bounded even when every
    // event is an empty line on alternating streams.
    streams: VecDeque<bool>,
    seen: [bool; 2],
    exit: Option<RunProcessEvent>,
    truncated: bool,
}

impl OutputCollector {
    pub fn new(max_output_bytes: usize) -> Self {
        Self {
            limit: max_output_bytes,
            bytes: VecDeque::new(),
            streams: VecDeque::new(),
            seen: [false; 2],
            exit: None,
            truncated: false,
        }
    }

    pub fn push(&mut self, event: RunProcessEvent) {
        match event {
            RunProcessEvent::Output(event) => {
                let is_stderr = event.stream.as_deref() == Some("stderr");
                if self.seen[usize::from(is_stderr)] {
                    self.append(b"\n", is_stderr);
                }
                self.seen[usize::from(is_stderr)] = true;
                self.append(event.line.as_bytes(), is_stderr);
            }
            RunProcessEvent::Exited {
                exit_code, signal, ..
            } if exit_code.is_some() || signal.is_some() => {
                self.exit = Some(event);
            }
            _ => {}
        }
    }

    fn append(&mut self, bytes: &[u8], is_stderr: bool) {
        if bytes.is_empty() {
            return;
        }
        let retained = if bytes.len() >= self.limit {
            self.truncated |= !self.bytes.is_empty() || bytes.len() > self.limit;
            self.bytes.clear();
            self.streams.clear();
            // Copy only the tail: never retain the allocation of an oversized
            // incoming line or iterate over all of its discarded bytes.
            &bytes[bytes.len() - self.limit..]
        } else {
            let discard = self
                .bytes
                .len()
                .saturating_add(bytes.len())
                .saturating_sub(self.limit);
            self.truncated |= discard > 0;
            self.bytes.drain(..discard);
            self.streams.drain(..discard);
            bytes
        };
        self.bytes.extend(retained);
        self.streams
            .extend(std::iter::repeat_n(is_stderr, retained.len()));
    }

    pub fn finish(mut self) -> LimitedRunProcessOutput {
        // A byte limit can cut the oldest character in the middle. Each event
        // is valid UTF-8, so discarding leading continuation bytes preserves
        // complete characters in both returned streams.
        while self.bytes.front().is_some_and(|byte| byte & 0xc0 == 0x80) {
            self.bytes.pop_front();
            self.streams.pop_front();
        }
        let mut output = [Vec::new(), Vec::new()];
        for (byte, is_stderr) in self.bytes.into_iter().zip(self.streams) {
            output[usize::from(is_stderr)].push(byte);
        }
        let mut events = Vec::with_capacity(3);
        for (index, bytes) in output.into_iter().enumerate() {
            if self.seen[index] {
                events.push(RunProcessEvent::Output(OutputEvent {
                    line: String::from_utf8(bytes)
                        .expect("output tail must retain complete UTF-8 characters"),
                    timestamp: serde_json::Value::Null,
                    stream: Some(if index == 0 { "stdout" } else { "stderr" }.into()),
                }));
            }
        }
        events.extend(self.exit);
        LimitedRunProcessOutput {
            events,
            output_truncated: self.truncated,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandboxes::models::ProcessExitReason;

    fn line(value: &str, stream: &str) -> RunProcessEvent {
        RunProcessEvent::Output(OutputEvent {
            line: value.into(),
            timestamp: 1.into(),
            stream: Some(stream.into()),
        })
    }

    fn output(result: &LimitedRunProcessOutput, stream: &str) -> String {
        result
            .events
            .iter()
            .filter_map(|event| match event {
                RunProcessEvent::Output(event) if event.stream.as_deref() == Some(stream) => {
                    Some(event.line.as_str())
                }
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn preserves_output_and_only_marks_actual_truncation() {
        let mut collector = OutputCollector::new(14);
        for event in [
            line("a", "stdout"),
            line("oops", "stderr"),
            line("", "stdout"),
            line("é\n🚀", "stdout"),
        ] {
            collector.push(event);
        }
        let result = collector.finish();
        assert_eq!(output(&result, "stdout"), "a\n\né\n🚀");
        assert_eq!(output(&result, "stderr"), "oops");
        assert!(!result.output_truncated);
    }

    #[test]
    fn bounds_both_streams_and_preserves_exit_after_discarding_output() {
        let mut collector = OutputCollector::new(7);
        let exit = RunProcessEvent::Exited {
            exit_code: None,
            signal: Some(9),
            oom_killed: false,
            reason: Some(ProcessExitReason::TimedOut),
        };
        for event in [
            line("abcd", "stdout"),
            line("efgh", "stderr"),
            line("end", "stdout"),
            exit.clone(),
        ] {
            collector.push(event);
        }
        let result = collector.finish();
        assert_eq!(output(&result, "stdout"), "\nend");
        assert_eq!(output(&result, "stderr"), "fgh");
        assert_eq!(result.events.last(), Some(&exit));
        assert!(result.output_truncated);
    }

    #[test]
    fn truncates_on_utf8_boundaries_and_counts_separators() {
        for (limit, expected, truncated) in [
            (0, "", true),
            (1, "", true),
            (2, "", true),
            (3, "界", true),
            (4, "\n界", true),
            (5, "\n界", true),
            (6, "é\n界", false),
        ] {
            let mut collector = OutputCollector::new(limit);
            collector.push(line("é", "stdout"));
            collector.push(line("界", "stdout"));
            let result = collector.finish();
            assert_eq!(output(&result, "stdout"), expected);
            assert_eq!(result.output_truncated, truncated);
        }
    }

    #[test]
    fn empty_output_at_zero_limit_is_not_truncated() {
        let mut collector = OutputCollector::new(0);
        collector.push(line("", "stdout"));
        collector.push(line("", "stderr"));
        assert!(!collector.finish().output_truncated);
    }

    #[test]
    fn many_empty_lines_and_one_large_line_keep_bounded_storage() {
        let mut collector = OutputCollector::new(1024);
        for _ in 0..100_000 {
            collector.push(line("", "stdout"));
        }
        let result = collector.finish();
        assert_eq!(result.events.len(), 1);
        assert_eq!(output(&result, "stdout").len(), 1024);
        assert!(result.output_truncated);

        let mut collector = OutputCollector::new(16);
        collector.push(line(&"x".repeat(1024 * 1024), "stdout"));
        let result = collector.finish();
        let RunProcessEvent::Output(event) = &result.events[0] else {
            panic!("expected output");
        };
        assert_eq!(event.line.len(), 16);
        assert!(event.line.capacity() <= 32);
        assert!(result.output_truncated);
    }
}
