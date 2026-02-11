use crc32fast::Hasher;

pub const MAGIC: [u8; 4] = *b"F2V1";
pub const HEADER_LEN: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameType {
    Manifest = 0,
    Data = 1,
    End = 2,
}

#[derive(Debug, Clone)]
pub struct FrameHeader {
    pub version: u16,
    pub frame_type: FrameType,
    pub flags: u8,
    pub dataset_id: [u8; 16],
    pub video_id: u32,
    pub frame_index: u64,
    pub stream_offset: u64,
    pub payload_len: u32,
    pub payload_crc32: u32,
}

pub fn build_frame_bytes(
    frame_bytes: usize,
    hdr: &FrameHeader,
    payload: &[u8],
) -> Vec<u8> {
    let mut out = vec![0u8; frame_bytes];
    write_header(&mut out[..HEADER_LEN], hdr);

    let payload_cap = frame_bytes - HEADER_LEN;
    let n = payload.len().min(payload_cap);
    out[HEADER_LEN..HEADER_LEN + n].copy_from_slice(&payload[..n]);
    out
}

pub fn parse_header(buf: &[u8]) -> Option<FrameHeader> {
    if buf.len() < HEADER_LEN { return None; }
    if buf[0..4] != MAGIC { return None; }

    let version = u16::from_le_bytes([buf[4], buf[5]]);
    let ft = match buf[6] {
        0 => FrameType::Manifest,
        1 => FrameType::Data,
        2 => FrameType::End,
        _ => return None,
    };
    let flags = buf[7];
    let mut dataset_id = [0u8; 16];
    dataset_id.copy_from_slice(&buf[8..24]);
    let video_id = u32::from_le_bytes([buf[24], buf[25], buf[26], buf[27]]);
    let frame_index = u64::from_le_bytes(buf[28..36].try_into().ok()?);
    let stream_offset = u64::from_le_bytes(buf[36..44].try_into().ok()?);
    let payload_len = u32::from_le_bytes(buf[44..48].try_into().ok()?);
    let payload_crc32 = u32::from_le_bytes(buf[48..52].try_into().ok()?);

    Some(FrameHeader {
        version,
        frame_type: ft,
        flags,
        dataset_id,
        video_id,
        frame_index,
        stream_offset,
        payload_len,
        payload_crc32,
    })
}

fn write_header(dst: &mut [u8], hdr: &FrameHeader) {
    dst[..4].copy_from_slice(&MAGIC);
    dst[4..6].copy_from_slice(&hdr.version.to_le_bytes());
    dst[6] = hdr.frame_type as u8;
    dst[7] = hdr.flags;
    dst[8..24].copy_from_slice(&hdr.dataset_id);
    dst[24..28].copy_from_slice(&hdr.video_id.to_le_bytes());
    dst[28..36].copy_from_slice(&hdr.frame_index.to_le_bytes());
    dst[36..44].copy_from_slice(&hdr.stream_offset.to_le_bytes());
    dst[44..48].copy_from_slice(&hdr.payload_len.to_le_bytes());
    dst[48..52].copy_from_slice(&hdr.payload_crc32.to_le_bytes());
    // remaining bytes reserved=0
}

pub fn crc32(data: &[u8]) -> u32 {
    let mut h = Hasher::new();
    h.update(data);
    h.finalize()
}
