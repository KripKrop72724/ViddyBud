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

/// Streaming parser that consumes bytes and yields complete ChunkTask(s)
pub struct RecordParser {
    buf: Vec<u8>,
    state: State,
}

enum State {
    NeedKind,
    NeedHeader, // need 20 bytes after kind
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
            state: State::NeedKind,
        }
    }

    pub fn push(&mut self, bytes: &[u8], out: &mut Vec<ChunkTask>) {
        self.buf.extend_from_slice(bytes);

        loop {
            match self.state {
                State::NeedKind => {
                    if self.buf.len() < 1 {
                        break;
                    }
                    let kind = self.buf[0];
                    self.buf.drain(0..1);
                    if kind == REC_END {
                        // ignore, stream end marker
                        continue;
                    } else if kind == REC_DATA {
                        self.state = State::NeedHeader;
                    } else {
                        // unknown -> drop
                        self.state = State::NeedKind;
                        self.buf.clear();
                        break;
                    }
                }
                State::NeedHeader => {
                    if self.buf.len() < 20 {
                        break;
                    }
                    let file_id = u32::from_le_bytes(self.buf[0..4].try_into().unwrap());
                    let offset = u64::from_le_bytes(self.buf[4..12].try_into().unwrap());
                    let len = u32::from_le_bytes(self.buf[12..16].try_into().unwrap());
                    let crc = u32::from_le_bytes(self.buf[16..20].try_into().unwrap());
                    self.buf.drain(0..20);
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
                    if self.buf.len() < need {
                        break;
                    }
                    let data = self.buf[0..need].to_vec();
                    self.buf.drain(0..need);

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
    }
}
