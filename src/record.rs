use crc32fast::Hasher;

pub const REC_DATA: u8 = 0;
pub const REC_END: u8 = 1;

// Record format:
// kind u8
// if kind==DATA:
//   file_id u32
//   offset u64
//   len u32
//   crc32 u32
//   data [len]
// if kind==END: no fields
pub fn build_data_record(file_id: u32, offset: u64, data: &[u8]) -> Vec<u8> {
    let len = data.len() as u32;
    let crc = crc32(data);

    let mut out = Vec::with_capacity(1 + 4 + 8 + 4 + 4 + data.len());
    out.push(REC_DATA);
    out.extend_from_slice(&file_id.to_le_bytes());
    out.extend_from_slice(&offset.to_le_bytes());
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(data);
    out
}

pub fn build_end_record() -> Vec<u8> {
    vec![REC_END]
}

pub fn crc32(data: &[u8]) -> u32 {
    let mut h = Hasher::new();
    h.update(data);
    h.finalize()
}

#[derive(Debug, Clone)]
pub struct ChunkTask {
    pub file_id: u32,
    pub offset: u64,
    pub data: Vec<u8>,
    pub expected_crc32: u32,
}

/// Streaming parser that consumes bytes and yields complete ChunkTask(s).
/// Uses a cursor + occasional compaction to avoid frequent front-drain memmoves.
pub struct RecordParser {
    buf: Vec<u8>,
    pos: usize,
    state: State,
}

#[allow(clippy::enum_variant_names)]
enum State {
    NeedKind,
    NeedHeader,
    NeedData {
        file_id: u32,
        offset: u64,
        len: u32,
        crc: u32,
    },
}

impl RecordParser {
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            pos: 0,
            state: State::NeedKind,
        }
    }

    pub fn push(&mut self, bytes: &[u8], out: &mut Vec<ChunkTask>) {
        self.buf.extend_from_slice(bytes);

        loop {
            match self.state {
                State::NeedKind => {
                    if self.available() < 1 {
                        break;
                    }
                    let kind = self.buf[self.pos];
                    self.pos += 1;

                    if kind == REC_END {
                        // stream end marker
                        continue;
                    }
                    if kind == REC_DATA {
                        self.state = State::NeedHeader;
                        continue;
                    }

                    // Unknown data => reset parser state.
                    self.state = State::NeedKind;
                    self.buf.clear();
                    self.pos = 0;
                    break;
                }

                State::NeedHeader => {
                    if self.available() < 20 {
                        break;
                    }
                    let p = self.pos;
                    let file_id = u32::from_le_bytes(self.buf[p..p + 4].try_into().unwrap());
                    let offset = u64::from_le_bytes(self.buf[p + 4..p + 12].try_into().unwrap());
                    let len = u32::from_le_bytes(self.buf[p + 12..p + 16].try_into().unwrap());
                    let crc = u32::from_le_bytes(self.buf[p + 16..p + 20].try_into().unwrap());
                    self.pos += 20;
                    self.state = State::NeedData {
                        file_id,
                        offset,
                        len,
                        crc,
                    };
                }

                State::NeedData {
                    file_id,
                    offset,
                    len,
                    crc,
                } => {
                    let need = len as usize;
                    if self.available() < need {
                        break;
                    }

                    let start = self.pos;
                    let end = start + need;
                    let data = self.buf[start..end].to_vec();
                    self.pos = end;

                    out.push(ChunkTask {
                        file_id,
                        offset,
                        data,
                        expected_crc32: crc,
                    });
                    self.state = State::NeedKind;
                }
            }
        }

        self.compact_if_needed();
    }

    fn available(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    fn compact_if_needed(&mut self) {
        if self.pos == 0 {
            return;
        }

        if self.pos == self.buf.len() {
            self.buf.clear();
            self.pos = 0;
            return;
        }

        // Compact once consumed prefix gets large to avoid unbounded growth.
        if self.pos >= (self.buf.len() / 2) || self.pos >= (1 << 20) {
            self.buf.copy_within(self.pos.., 0);
            let new_len = self.buf.len() - self.pos;
            self.buf.truncate(new_len);
            self.pos = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_records_across_fragmented_pushes() {
        let rec1 = build_data_record(7, 10, b"abc");
        let rec2 = build_data_record(8, 20, b"xyz1");

        let mut parser = RecordParser::new();
        let mut out = vec![];

        let mut stream = vec![];
        stream.extend_from_slice(&rec1);
        stream.extend_from_slice(&rec2);
        stream.extend_from_slice(&build_end_record());

        parser.push(&stream[..5], &mut out);
        assert_eq!(out.len(), 0);
        parser.push(&stream[5..11], &mut out);
        assert_eq!(out.len(), 0);
        parser.push(&stream[11..], &mut out);

        assert_eq!(out.len(), 2);
        assert_eq!(out[0].file_id, 7);
        assert_eq!(out[0].offset, 10);
        assert_eq!(out[0].data, b"abc");
        assert_eq!(out[1].file_id, 8);
        assert_eq!(out[1].offset, 20);
        assert_eq!(out[1].data, b"xyz1");
    }
}
