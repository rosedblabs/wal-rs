use std::cmp::Ordering;
use std::fmt::{Debug, Formatter};
use std::fs;
use std::fs::File;
use std::io::{ErrorKind, Seek, SeekFrom, Write};
use std::ops::{Deref, DerefMut};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;
use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian};
use bytes::{BufMut, BytesMut};
use crc32fast::Hasher;
use lazy_static::lazy_static;
use pool::Pool;

use crate::cache::Cache;
use crate::error::WalError;
use crate::options::KB;

pub const CHUNK_HEADER_SIZE: u32 = 7;
pub const BLOCK_SIZE: u32 = 32 * KB;
const FILE_MODE_PERM: u32 = 0o644;

const BLOCK_POOL_MAX_SIZE: usize = 20;
const BUFFER_POOL_MAX_SIZE: usize = 20;
const BUFFER_POOL_PER_CAPACITY: usize = 16_384;

lazy_static! {
    static ref BUFFER_POOL: Mutex<Pool<BytesMut>> = Mutex::new(Pool::with_capacity(BUFFER_POOL_MAX_SIZE, 0, || BytesMut::with_capacity(BUFFER_POOL_PER_CAPACITY)));
}

#[derive(Debug, Copy, Clone, PartialEq)]
enum ChunkType {
    Full,
    First,
    Middle,
    Last,
}

impl TryFrom<u8> for ChunkType {
    type Error = anyhow::Error;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(ChunkType::Full),
            1 => Ok(ChunkType::First),
            2 => Ok(ChunkType::Middle),
            3 => Ok(ChunkType::Last),
            _ => Err(WalError::UnknownChunkType(value).into()),
        }
    }
}

impl Into<u8> for ChunkType {
    fn into(self) -> u8 {
        match self {
            ChunkType::Full => 0,
            ChunkType::First => 1,
            ChunkType::Middle => 2,
            ChunkType::Last => 3,
        }
    }
}

pub type SegmentID = u32;

#[derive(Clone)]
pub struct Segment {
    pub(crate) id: SegmentID,
    file: Arc<File>,
    path: String,
    current_block_number: u32,
    current_block_size: u32,
    closed: bool,
    cache: Option<Cache>,
    header: BytesMut,
    block_pool: Arc<Mutex<Pool<Block>>>,
}

#[derive(Debug, Default, PartialEq)]
pub struct ChunkPosition {
    pub segment_id: SegmentID,
    pub block_number: u32,
    pub chunk_offset: u64,
    pub chunk_size: u32,
}

impl Segment {
    pub fn write(&mut self, data: &[u8]) -> Result<ChunkPosition> {
        self.try_write(|s| {
            let mut buf = BUFFER_POOL.lock().unwrap().checkout().unwrap();
            let pos = s.write_to_buf(data, &mut buf)?;
            s.write_chunk_buf(&buf)?;
            Ok(pos)
        })
    }

    pub fn write_all(&mut self, data: &Vec<BytesMut>) -> Result<Vec<ChunkPosition>> {
        self.try_write(|s| {
            let mut buf = BUFFER_POOL.lock().unwrap().checkout().unwrap();
            let mut positions: Vec<ChunkPosition> = Vec::with_capacity(data.len());

            for datum in data {
                positions.push(s.write_to_buf(datum, &mut buf)?)
            }

            s.write_chunk_buf(&buf)?;

            Ok(positions)
        })
    }

    #[inline]
    fn try_write<F, T>(&mut self, func: F) -> Result<T> where F: Fn(&mut Self) -> Result<T> {
        if self.closed {
            return Err(WalError::SegmentFileClosed.into());
        }

        let origin_block_number = self.current_block_number;
        let origin_block_size = self.current_block_size;

        match func(self) {
            Ok(result) => Ok(result),
            Err(e) => {
                self.current_block_number = origin_block_number;
                self.current_block_size = origin_block_size;
                return Err(e);
            }
        }
    }

    fn write_to_buf(&mut self, data: &[u8], chunk_buf: &mut BytesMut) -> Result<ChunkPosition> {
        if self.closed {
            return Err(WalError::SegmentFileClosed.into());
        }

        let buf_start = chunk_buf.len() as u32;
        let data_size = data.len() as u32;
        let padding = self.fill_padding_if_need(chunk_buf);
        let mut position = ChunkPosition {
            segment_id: self.id,
            block_number: self.current_block_number,
            chunk_offset: self.current_block_size as u64,
            chunk_size: 0,
        };

        if self.current_block_size + data_size + CHUNK_HEADER_SIZE <= BLOCK_SIZE {
            self.append_chunk_buf(chunk_buf, data, ChunkType::Full);
            position.chunk_size = CHUNK_HEADER_SIZE + data_size;
        } else {
            let mut block_count: u32 = 0;
            let mut left_size = data_size;
            let mut curr_block_size = self.current_block_size;

            while left_size > 0 {
                let mut chunk_size = BLOCK_SIZE - curr_block_size - CHUNK_HEADER_SIZE;
                if chunk_size > left_size {
                    chunk_size = left_size;
                }

                let mut end = data_size - left_size + chunk_size;
                if end > data_size {
                    end = data_size;
                }

                let chunk_type = match Some(left_size) {
                    Some(s) if s == data_size => ChunkType::First,
                    Some(s) if s == chunk_size => ChunkType::Last,
                    _ => ChunkType::Middle,
                };

                let start = (data_size - left_size) as usize;
                let end = end as usize;
                self.append_chunk_buf(chunk_buf, &data[start..end], chunk_type);

                block_count += 1;
                left_size -= chunk_size;
                curr_block_size = (curr_block_size + chunk_size + CHUNK_HEADER_SIZE) % BLOCK_SIZE;
            }
            position.chunk_size = block_count * CHUNK_HEADER_SIZE + data_size;
        }

        let buf_end = chunk_buf.len() as u32;
        if position.chunk_size + padding != buf_end - buf_start {
            panic!("wrong!!! the chunk size {} is not equal to the buffer len {}", position.chunk_size + padding, buf_end - buf_start);
        }

        self.current_block_size += position.chunk_size;
        if self.current_block_size >= BLOCK_SIZE {
            self.current_block_number += self.current_block_size / BLOCK_SIZE;
            self.current_block_size = self.current_block_size % BLOCK_SIZE;
        }

        Ok(position)
    }

    fn write_chunk_buf(&mut self, chunk_buf: &BytesMut) -> Result<()> {
        if self.current_block_size > BLOCK_SIZE {
            panic!("wrong! can not exceed the block size");
        }

        self.file.write(&chunk_buf)?;

        Ok(())
    }

    fn fill_padding_if_need(&mut self, chunk_buf: &mut BytesMut) -> u32 {
        if self.current_block_size + CHUNK_HEADER_SIZE >= BLOCK_SIZE {
            if self.current_block_size < BLOCK_SIZE {
                let padding = BLOCK_SIZE - self.current_block_size;
                chunk_buf.put_slice(vec![0u8; padding as usize].as_slice());
                // a new block
                self.current_block_number += 1;
                self.current_block_size = 0;
                // return padding size
                return padding;
            }
        }
        0
    }

    fn append_chunk_buf(&mut self, chunk_buf: &mut BytesMut, data: &[u8], chunk_type: ChunkType) {
        self.header.clear();
        // Length 2 Bytes index: 4-5
        self.header.put_u16_le(data.len() as u16);
        // Type 1 Bytes index: 6
        self.header.put_u8(chunk_type.into());

        // Checksum 4 Bytes index: 0-3
        chunk_buf.put_u32_le(checksum(&self.header, data));
        // Header 3 Bytes index: 4-6
        chunk_buf.put_slice(&self.header);
        // Data index: 7-data.len()
        chunk_buf.put_slice(data);
    }

    pub fn read(&self, block_number: u32, chunk_offset: u64) -> Result<BytesMut> {
        match self.read_internal(block_number, chunk_offset) {
            Ok((value, _)) => Ok(value),
            Err(e) => Err(e),
        }
    }

    pub fn new_reader(&self) -> SegmentReader {
        SegmentReader {
            segment: self.clone(),
            block_number: 0,
            chunk_offset: 0,
        }
    }

    #[inline]
    fn read_internal(&self, block_number: u32, chunk_offset: u64) -> Result<(BytesMut, ChunkPosition)> {
        if self.closed {
            return Err(WalError::SegmentFileClosed.into());
        }

        let segment_size = self.size();
        let mut current_block_number = block_number;
        let mut current_chunk_offset = chunk_offset;
        let mut value = BytesMut::with_capacity(1024);
        let mut block = self.block_pool.lock().unwrap().checkout().unwrap();
        let mut next_chunk = ChunkPosition { segment_id: self.id, ..Default::default() };

        loop {
            let mut size = BLOCK_SIZE as u64;
            let offset = (current_block_number * BLOCK_SIZE) as u64;

            if offset >= segment_size {
                return Err(anyhow!(ErrorKind::UnexpectedEof));
            }

            if size + offset > segment_size {
                size = segment_size - offset;
            }

            self.load_cache_block(&mut block, current_block_number, offset, size)?;

            // header
            let header_start = current_chunk_offset as usize;
            let header_end = header_start + CHUNK_HEADER_SIZE as usize;
            let header = &block[header_start..header_end];

            // length
            let length = LittleEndian::read_u16(&header[4..6]);
            let data_end = header_end + length as usize;
            let data = &block[header_end..data_end];

            // checksum
            let checksum = checksum(&header[4..], &data);
            let saved_sum = LittleEndian::read_u32(&header[..4]);
            if saved_sum != checksum {
                return Err(WalError::InvalidCRC.into());
            }

            // copy data
            value.put_slice(data);

            // type
            let chunk_type: ChunkType = header[6].try_into()?;
            if chunk_type == ChunkType::Full || chunk_type == ChunkType::Last {
                next_chunk.block_number = current_block_number;
                next_chunk.chunk_offset = data_end as u64;
                if data_end as u32 + CHUNK_HEADER_SIZE >= BLOCK_SIZE {
                    next_chunk.block_number += 1;
                    next_chunk.chunk_offset = 0;
                }
                break;
            }

            current_block_number += 1;
            current_chunk_offset = 0;
        }

        Ok((value, next_chunk))
    }

    fn load_cache_block(&self, block: &mut BytesMut, current_block_number: u32, offset: u64, size: u64) -> Result<()> {
        let key = self.get_cache_key(current_block_number);

        match self.cache.as_ref() {
            None => {
                // cache disabled, read block directly from the segment file
                self.file.read_at(block, offset)?;
            }
            Some(cache_arc) => {
                let mut cache = cache_arc.lock().unwrap();
                match cache.get(&key) {
                    Some(val) => block.copy_from_slice(val),
                    None => {
                        // cache miss, read block from the segment file
                        self.file.read_at(block, offset)?;
                        if size == BLOCK_SIZE as u64 {
                            cache.put(key, block.to_owned());
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        if self.closed {
            return Ok(());
        }

        match self.file.sync_all() {
            Ok(_) => Ok(()),
            Err(e) => Err(e.into())
        }
    }

    pub fn remove(&mut self) -> Result<()> {
        if !self.closed {
            self.closed = true;
            // TODO: close file
        }

        Ok(fs::remove_file(self.path.as_str())?)
    }

    pub fn close(&mut self) -> Result<()> {
        if !self.closed {
            self.closed = true;
            // TODO: close file
        }

        Ok(())
    }

    pub fn size(&self) -> u64 {
        (self.current_block_size + self.current_block_number * BLOCK_SIZE) as u64
    }

    fn get_cache_key(&self, block_number: u32) -> u64 {
        (self.id as u64) << 32 | block_number as u64
    }
}

impl Debug for Segment {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f,
               "Segment id: {}, path: {}, current_block_number: {}, current_block_size: {}, closed: {}",
               self.id, self.path, self.current_block_number, self.current_block_size, self.closed
        )
    }
}

#[derive(Debug, Clone)]
struct Block(BytesMut);

impl Default for Block {
    fn default() -> Self {
        // The length of the data read from the file depends on the length of the block,
        // if the length of the block is 0, nothing can be read.
        Self(BytesMut::zeroed((CHUNK_HEADER_SIZE + BLOCK_SIZE) as usize))
    }
}

impl Deref for Block {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Block {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Clone)]
pub struct SegmentReader {
    pub segment: Segment,
    pub block_number: u32,
    pub chunk_offset: u64,
}

impl Iterator for SegmentReader {
    type Item = (BytesMut, ChunkPosition);

    fn next(&mut self) -> Option<Self::Item> {
        if self.segment.closed {
            return None;
        }

        let mut chunk_position = ChunkPosition {
            segment_id: self.segment.id,
            block_number: self.block_number,
            chunk_offset: self.chunk_offset,
            chunk_size: 0,
        };

        let result = self.segment.read_internal(self.block_number, self.chunk_offset);
        let (value, next_chunk) = match result {
            Ok(ok) => ok,
            Err(_) => return None,
        };

        chunk_position.chunk_size =
            next_chunk.block_number * BLOCK_SIZE + next_chunk.chunk_offset as u32 -
                (self.block_number * BLOCK_SIZE + self.chunk_offset as u32);

        self.block_number = next_chunk.block_number;
        self.chunk_offset = next_chunk.chunk_offset;

        Some((value, chunk_position))
    }
}

impl Eq for SegmentReader {}

impl PartialEq<Self> for SegmentReader {
    fn eq(&self, other: &Self) -> bool {
        self.segment.id == other.segment.id
            && self.chunk_offset == other.chunk_offset
            && self.block_number == other.block_number
    }
}

impl PartialOrd<Self> for SegmentReader {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SegmentReader {
    fn cmp(&self, other: &Self) -> Ordering {
        self.segment.id.cmp(&other.segment.id)
    }
}

pub fn open<'a>(dir_path: impl AsRef<Path>, ext_name: &str, id: u32, cache: Option<Cache>) -> Result<Segment> {
    let path = make_segment_file_name(dir_path, ext_name, id);
    let mut file = File::options()
        .read(true)
        .write(true)
        .create(true)
        .append(true)
        .mode(FILE_MODE_PERM)
        .open(&path)?;

    let offset = file.seek(SeekFrom::End(0))?;

    Ok(Segment {
        id,
        cache,
        path,
        closed: false,
        file: file.into(),
        current_block_size: offset as u32 % BLOCK_SIZE,
        current_block_number: offset as u32 / BLOCK_SIZE,
        header: BytesMut::with_capacity(CHUNK_HEADER_SIZE as usize),
        block_pool: Arc::new(Mutex::new(Pool::with_capacity(BLOCK_POOL_MAX_SIZE, 0, || Block::default()))),
    })
}

pub fn make_segment_file_name(dir_path: impl AsRef<Path>, ext_name: &str, id: SegmentID) -> String {
    let path = Path::new(dir_path.as_ref());

    if !path.exists() {
        fs::create_dir(dir_path.as_ref()).unwrap();
    }

    let path = path.join(format!("{id}{ext_name}").as_str());
    String::from(path.to_str().unwrap())
}

fn checksum(header: &[u8], data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(header);
    hasher.update(data);
    hasher.finalize()
}


#[cfg(test)]
mod tests {
    use std::{env, fs, panic};

    use bytes::BytesMut;
    use chrono::Local;
    use rand::{Rng, thread_rng};
    use rand::distributions::Alphanumeric;

    use crate::options::Options;
    use crate::segment;
    use crate::segment::{BLOCK_SIZE, Cache, CHUNK_HEADER_SIZE, ChunkPosition, ChunkType};

    #[inline]
    fn run_test<T>(func: T) where T: Fn(&mut segment::Segment) + panic::RefUnwindSafe {
        run_test_with_options(func, None);
    }

    #[inline]
    fn run_test_with_options<T>(func: T, opts: Option<Options>) where T: Fn(&mut segment::Segment) + panic::RefUnwindSafe {
        let dir_path = || {
            let rand_string: String = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(10)
                .map(char::from)
                .collect();

            env::temp_dir().join(format!("seg-{}{}", Local::now().timestamp_millis(), rand_string))
        };

        let mut opts = opts.unwrap_or_else(|| {
            Options { dir_path: dir_path().to_owned(), ..Options::default() }
        });

        if opts.dir_path.as_os_str().len() == 0 {
            opts.dir_path = dir_path();
        }

        let cache = match opts.block_cache > 0 {
            true => Some(Cache::with_capacity(opts.block_cache as usize)),
            false => None,
        };

        let result = panic::catch_unwind(|| {
            let mut segment = segment::open(&opts.dir_path, &opts.segment_file_ext, 1, cache.clone()).unwrap();
            func(&mut segment);
        });

        fs::remove_dir_all(&opts.dir_path).unwrap();
        result.unwrap();
    }

    #[inline]
    fn segment_many_chunks(size: usize, count: usize) {
        run_test(|seg| {
            let mut positions: Vec<ChunkPosition> = Vec::with_capacity(count);
            let val = "X".repeat(size).into_bytes();

            for _ in 0..count {
                positions.push(seg.write(&val).unwrap())
            }

            let mut reader = seg.new_reader();
            let mut values: Vec<BytesMut> = Vec::with_capacity(count);

            for i in 0..count {
                let (value, pos) = match reader.next() {
                    None => break,
                    Some(v) => v,
                };
                assert_eq!(val, value);
                assert_eq!(positions[i].segment_id, pos.segment_id);
                assert_eq!(positions[i].block_number, pos.block_number);
                assert_eq!(positions[i].chunk_offset, pos.chunk_offset);
                values.push(value);
            }

            assert_eq!(count, values.len());
        });
    }

    #[test]
    fn test_chunk_type_enum() {
        let chunk_type: u8 = ChunkType::Full.into();
        assert_eq!(chunk_type, 0);

        assert_eq!(ChunkType::try_from(0).unwrap(), ChunkType::Full);
        assert_eq!(ChunkType::try_from(1).unwrap(), ChunkType::First);
        assert_eq!(ChunkType::try_from(2).unwrap(), ChunkType::Middle);
        assert_eq!(ChunkType::try_from(3).unwrap(), ChunkType::Last);

        let chunk_type = ChunkType::try_from(5);
        assert!(chunk_type.is_err());
        assert_eq!(chunk_type.unwrap_err().to_string(), "Unknown chunk type: 5");
    }

    #[test]
    fn test_segment_write_full1() {
        run_test(|seg| {
            let val = "X".repeat(100).into_bytes();

            let pos1 = seg.write(&val).unwrap();
            let pos2 = seg.write(&val).unwrap();

            assert_eq!(val, seg.read(pos1.block_number, pos1.chunk_offset).unwrap());
            assert_eq!(val, seg.read(pos2.block_number, pos2.chunk_offset).unwrap());

            for _ in 0..100000 {
                let pos = seg.write(&val).unwrap();
                assert_eq!(val, seg.read(pos.block_number, pos.chunk_offset).unwrap());
            }
        });
    }

    #[test]
    fn test_segment_write_full1_with_cache() {
        let options = Options { block_cache: 100, ..Options::default() };

        run_test_with_options(|seg| {
            let val = "X".repeat(100).into_bytes();

            let pos1 = seg.write(&val).unwrap();
            let pos2 = seg.write(&val).unwrap();

            assert_eq!(val, seg.read(pos1.block_number, pos1.chunk_offset).unwrap());
            assert_eq!(val, seg.read(pos2.block_number, pos2.chunk_offset).unwrap());

            assert_eq!(0, seg.cache.as_ref().unwrap().lock().unwrap().len());

            for _ in 0..100000 {
                let pos = seg.write(&val).unwrap();
                assert_eq!(val, seg.read(pos.block_number, pos.chunk_offset).unwrap());
                assert_eq!(val, seg.read(pos.block_number, pos.chunk_offset).unwrap());
            }

            assert_eq!(100, seg.cache.as_ref().unwrap().lock().unwrap().len());
        }, Some(options));
    }

    #[test]
    fn test_segment_write_full2() {
        run_test(|seg| {
            let val = "X".repeat((BLOCK_SIZE - CHUNK_HEADER_SIZE) as usize).into_bytes();

            let pos1 = seg.write(&val).unwrap();
            assert_eq!(pos1.block_number, 0);
            assert_eq!(pos1.chunk_offset, 0);
            assert_eq!(val, seg.read(pos1.block_number, pos1.chunk_offset).unwrap());

            let pos2 = seg.write(&val).unwrap();
            assert_eq!(pos2.block_number, 1);
            assert_eq!(pos2.chunk_offset, 0);
            assert_eq!(val, seg.read(pos2.block_number, pos2.chunk_offset).unwrap());
        });
    }

    #[test]
    fn test_segment_write_padding() {
        run_test(|seg| {
            let val = "X".repeat((BLOCK_SIZE - CHUNK_HEADER_SIZE - 3) as usize).into_bytes();

            seg.write(&val).unwrap();

            let pos1 = seg.write(&val).unwrap();
            assert_eq!(pos1.block_number, 1);
            assert_eq!(pos1.chunk_offset, 0);
            assert_eq!(val, seg.read(pos1.block_number, pos1.chunk_offset).unwrap());
        });
    }

    #[test]
    fn test_segment_write_not_full() {
        run_test(|seg| {
            // FIRST-LAST
            let val = "X".repeat(BLOCK_SIZE as usize + 100).into_bytes();

            let pos1 = seg.write(&val).unwrap();
            assert_eq!(val, seg.read(pos1.block_number, pos1.chunk_offset).unwrap());

            let pos2 = seg.write(&val).unwrap();
            assert_eq!(val, seg.read(pos2.block_number, pos2.chunk_offset).unwrap());

            let pos3 = seg.write(&val).unwrap();
            assert_eq!(val, seg.read(pos3.block_number, pos3.chunk_offset).unwrap());

            // FIRST-MIDDLE-LAST
            let val = "X".repeat(BLOCK_SIZE as usize * 3 + 100).into_bytes();

            let pos4 = seg.write(&val).unwrap();
            assert_eq!(val, seg.read(pos4.block_number, pos4.chunk_offset).unwrap());
        });
    }

    #[test]
    fn test_segment_reader_full() {
        run_test(|seg| {
            let val = "X".repeat(BLOCK_SIZE as usize + 100).into_bytes();

            // FULL chunks
            let pos1 = seg.write(&val).unwrap();
            assert_eq!(val, seg.read(pos1.block_number, pos1.chunk_offset).unwrap());
            let pos2 = seg.write(&val).unwrap();
            assert_eq!(val, seg.read(pos2.block_number, pos2.chunk_offset).unwrap());

            let mut reader = seg.new_reader();

            let (bytes, rpos1) = reader.next().unwrap();
            assert_eq!(val, bytes);
            assert_eq!(pos1, rpos1);

            let (bytes, rpos2) = reader.next().unwrap();
            assert_eq!(val, bytes);
            assert_eq!(pos2, rpos2);

            assert!(reader.next().is_none());
        });
    }

    #[test]
    fn test_segment_reader_padding() {
        run_test(|seg| {
            let val = "X".repeat((BLOCK_SIZE - CHUNK_HEADER_SIZE - 7) as usize).into_bytes();

            let pos1 = seg.write(&val).unwrap();
            let pos2 = seg.write(&val).unwrap();

            let mut reader = seg.new_reader();

            let (bytes, rpos1) = reader.next().unwrap();
            assert_eq!(val, bytes);
            assert_eq!(pos1.segment_id, rpos1.segment_id);
            assert_eq!(pos1.block_number, rpos1.block_number);
            assert_eq!(pos1.chunk_offset, rpos1.chunk_offset);

            let (bytes, rpos2) = reader.next().unwrap();
            assert_eq!(val, bytes);
            assert_eq!(pos2.segment_id, rpos2.segment_id);
            assert_eq!(pos2.block_number, rpos2.block_number);
            assert_eq!(pos2.chunk_offset, rpos2.chunk_offset);

            assert!(reader.next().is_none());
        });
    }

    #[test]
    fn test_segment_reader_not_full() {
        run_test(|seg| {
            let val1 = "X".repeat(BLOCK_SIZE as usize + 100).into_bytes();
            let pos1 = seg.write(&val1).unwrap();
            let pos2 = seg.write(&val1).unwrap();

            let val2 = "X".repeat(BLOCK_SIZE as usize * 3 + 10).into_bytes();
            let pos3 = seg.write(&val2).unwrap();
            let pos4 = seg.write(&val2).unwrap();

            let mut reader = seg.new_reader();

            let (bytes1, rpos1) = reader.next().unwrap();
            assert_eq!(val1, bytes1);

            let (bytes2, rpos2) = reader.next().unwrap();
            assert_eq!(val1, bytes2);

            let (bytes3, rpos3) = reader.next().unwrap();
            assert_eq!(val2, bytes3);

            let (bytes4, rpos4) = reader.next().unwrap();
            assert_eq!(val2, bytes4);

            assert!(reader.next().is_none());

            assert_eq!(pos1, rpos1);
            assert_eq!(pos2, rpos2);
            assert_eq!(pos3, rpos3);
            assert_eq!(pos4, rpos4);
        });
    }

    #[test]
    fn test_segment_many_chunks_full() {
        segment_many_chunks(128, 1000000);
    }

    #[test]
    fn test_segment_many_chunks_not_full() {
        segment_many_chunks(BLOCK_SIZE as usize * 3 + 10, 10000);
    }

    #[test]
    fn test_segment_many_chunks_1block_10000count() {
        segment_many_chunks((BLOCK_SIZE - CHUNK_HEADER_SIZE) as usize, 10000);
    }

    #[test]
    fn test_segment_many_chunks_32block_1000count() {
        segment_many_chunks(32 * BLOCK_SIZE as usize, 1000);
    }

    #[test]
    fn test_segment_many_chunks_64block_100count() {
        segment_many_chunks(64 * BLOCK_SIZE as usize, 100);
    }
}
