use std::{
    fs::{File, OpenOptions},
    io::Result,
    path::PathBuf,
};

pub(crate) type SegmentID = u32;

pub(crate) struct Segment {
    id: SegmentID,
    fd: File,
    current_block_number: u32,
    current_block_size: u32,
    closed: bool,
}

pub(crate) struct ChunkPosition {
    segment_id: SegmentID,
    block_number: u32,
    chunk_offset: u32,
    chunk_size: u32,
}

impl Segment {
    pub(crate) fn new(dir_path: PathBuf, ext: String, id: u32) -> Result<Segment> {
        let file_name = segment_file_name(dir_path, ext, id);
        let fd = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_name)?;
        Ok(Segment {
            id,
            fd,
            current_block_number: 0,
            current_block_size: 0,
            closed: false,
        })
    }

    // pub(crate) fn read(&mut self, pos: ChunkPosition) -> Result<Vec<u8>> {}

    // pub(crate) fn write(&mut self, data: &[u8]) -> Result<ChunkPosition> {}

    // pub(crate) fn sync(&mut self) -> Result<()> {}

    // pub(crate) fn close(&mut self) -> Result<()> {}
}

pub fn segment_file_name(dir_path: PathBuf, ext: String, id: u32) -> PathBuf {
    let name = std::format!("{:09}{}", id, ext);
    dir_path.join(name)
}
