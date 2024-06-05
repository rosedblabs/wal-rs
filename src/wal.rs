use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::{Mutex, RwLock};

use anyhow::Result;
use bytes::BytesMut;

use crate::cache::Cache;
use crate::error::WalError;
use crate::options::Options;
use crate::segment;
use crate::segment::{
    BLOCK_SIZE, CHUNK_HEADER_SIZE, ChunkPosition, make_segment_file_name, Segment, SegmentID,
    SegmentReader,
};

const INITIAL_SEGMENT_FILE_ID: SegmentID = 1;

pub struct WAL {
    inner: RwLock<Inner>,
    pending: Mutex<Pending>,
}

impl WAL {
    pub fn open_new_active_segment(&self) -> Result<()> {
        let mut wal = self.inner.write().unwrap();

        wal.rotate_active_segment()?;

        Ok(())
    }

    pub fn get_active_segment_id(&self) -> SegmentID {
        let wal = self.inner.read().unwrap();
        wal.active_segment.id
    }

    pub fn empty(&self) -> bool {
        let wal = self.inner.read().unwrap();
        wal.older_segments.len() == 0 && wal.active_segment.size() == 0
    }

    pub fn write(&self, data: &[u8]) -> Result<ChunkPosition> {
        let mut wal = self.inner.write().unwrap();

        if data.len() + CHUNK_HEADER_SIZE as usize > wal.options.segment_size as usize {
            return Err(WalError::ValueTooLarge.into());
        }

        if wal.is_full(data.len()) {
            wal.rotate_active_segment()?
        }

        // write the data to the active segment file
        let position = wal.active_segment.write(data)?;

        // update the written size field
        wal.bytes_write += position.chunk_size;

        // sync the active segment file if needed
        let need_sync = match !wal.options.sync && wal.options.bytes_per_sync > 0 {
            true => wal.bytes_write >= wal.options.bytes_per_sync, // disabled sync, check written size
            false => wal.options.sync,
        };

        if need_sync {
            wal.active_segment.sync()?;
            wal.bytes_write = 0;
        }

        Ok(position)
    }

    pub fn write_all(&mut self) -> Result<Vec<ChunkPosition>> {
        let mut pending = self.pending.lock().unwrap();

        if pending.pending_writes.len() == 0 {
            return Ok(vec![]);
        }

        let mut wal = self.inner.write().unwrap();

        if pending.pending_size > wal.options.segment_size {
            pending.clean_pending_writes();
            return Err(WalError::PendingSizeTooLarge.into());
        }

        if wal.active_segment.size() + pending.pending_size > wal.options.segment_size {
            if let Err(e) = wal.rotate_active_segment() {
                pending.clean_pending_writes();
                return Err(e);
            }
        }

        let result = wal.active_segment.write_all(&pending.pending_writes);
        pending.clean_pending_writes();
        result
    }

    pub fn clean_pending_writes(&mut self) {
        let mut pending = self.pending.lock().unwrap();
        pending.clean_pending_writes();
    }

    pub fn pending_writes(&mut self, data: &[u8]) {
        let mut pending = self.pending.lock().unwrap();
        pending.pending_writes(data);
    }

    pub fn read(&self, pos: &ChunkPosition) -> Result<BytesMut> {
        let wal = self.inner.read().unwrap();

        let segment = match pos.segment_id == wal.active_segment.id {
            true => Some(&wal.active_segment),
            false => wal.older_segments.get(&pos.segment_id),
        };

        if let Some(seg) = segment {
            seg.read(pos.block_number, pos.chunk_offset)
        } else {
            Err(WalError::SegmentFileNotFound(pos.segment_id, wal.options.segment_file_ext.clone()).into())
        }
    }

    pub fn new_reader(&self) -> Reader {
        self.new_reader_with_max(0)
    }

    pub fn new_reader_with_max(&self, segment_id: SegmentID) -> Reader {
        let wal = self.inner.read().unwrap();
        wal.new_reader_with_max(segment_id)
    }

    pub fn new_reader_with_start(&self, start_pos: &ChunkPosition) -> Reader {
        let wal = self.inner.read().unwrap();
        wal.new_reader_with_start(start_pos)
    }

    pub fn close(&mut self) -> Result<()> {
        let mut wal = self.inner.write().unwrap();

        if let Some(cache) = wal.block_cache.as_mut() {
            cache.lock().unwrap().clear();
        }

        let mut ids: Vec<SegmentID> = Vec::new();
        for (_, segment) in wal.older_segments.iter_mut() {
            segment.close()?;
            ids.push(segment.id);
        }

        for id in ids {
            wal.rename_ids.push(id);
        }

        let active_segment_id = wal.active_segment.id;
        wal.rename_ids.push(active_segment_id);

        wal.older_segments.clear();
        wal.active_segment.close()
    }

    pub fn delete(&self) -> Result<()> {
        let mut wal = self.inner.write().unwrap();

        if let Some(cache) = wal.block_cache.as_mut() {
            cache.lock().unwrap().clear();
        }

        for (_, segment) in wal.older_segments.iter_mut() {
            segment.remove()?;
        }

        wal.older_segments.clear();
        wal.active_segment.remove()
    }

    pub fn sync(&self) -> Result<()> {
        let wal = self.inner.write().unwrap();
        wal.active_segment.sync()
    }

    pub fn rename_file_ext(&self, ext: &String) -> Result<()> {
        if !ext.starts_with(".") {
            return Err(WalError::InvalidSegmentFileExt.into());
        }

        let mut wal = self.inner.write().unwrap();

        for id in &wal.rename_ids {
            let id = id.clone();
            let old_name = make_segment_file_name(&wal.options.dir_path, &wal.options.segment_file_ext, id);
            let new_name = make_segment_file_name(&wal.options.dir_path, &ext, id);
            fs::rename(&old_name, &new_name)?;
        }

        wal.options.segment_file_ext = ext.clone();

        Ok(())
    }
}

impl Drop for WAL {
    fn drop(&mut self) {
        self.sync().unwrap();
        self.close().unwrap();
    }
}

struct Inner {
    active_segment: Segment,                     // used for new incoming writes.
    older_segments: HashMap<SegmentID, Segment>, // only used for read.
    options: Options,
    block_cache: Option<Cache>,
    bytes_write: u32,
    rename_ids: Vec<SegmentID>,
}

impl Inner {
    fn is_full(&self, delta: usize) -> bool {
        self.active_segment.size() + max_data_write_size(delta as u64) > self.options.segment_size
    }

    fn rotate_active_segment(&mut self) -> Result<()> {
        self.active_segment.sync()?;
        self.bytes_write = 0;

        let segment = segment::open(
            &self.options.dir_path,
            &self.options.segment_file_ext,
            self.active_segment.id + 1,
            self.block_cache.clone(),
        )?;

        let older_segment = std::mem::replace(&mut self.active_segment, segment);
        self.older_segments.insert(older_segment.id, older_segment);

        Ok(())
    }

    fn new_reader_with_max(&self, segment_id: SegmentID) -> Reader {
        let mut segment_readers: Vec<SegmentReader> = Vec::with_capacity(self.older_segments.len() + 1);

        for (_, segment) in self.older_segments.iter() {
            if segment_id == 0 || segment.id <= segment_id {
                segment_readers.push(segment.new_reader());
            }
        }

        if segment_id == 0 || self.active_segment.id <= segment_id {
            segment_readers.push(self.active_segment.new_reader());
        }

        segment_readers.sort();

        Reader {
            segment_readers,
            current_reader: 0,
        }
    }

    fn new_reader_with_start(&self, start_pos: &ChunkPosition) -> Reader {
        let mut reader = self.new_reader_with_max(0);

        loop {
            let current_segment_id = match reader.current_segment_id() {
                None => break,
                Some(id) => id
            };

            // skip the segment readers whose id is less than the given position's segment id.
            if current_segment_id < start_pos.segment_id {
                reader.skip_current_segment();
                continue;
            }

            // skip the chunk whose position is less than the given position.
            let current_pos = reader.current_chunk_position();
            if current_pos.block_number >= start_pos.block_number &&
                current_pos.chunk_offset >= start_pos.chunk_offset {
                break;
            }

            // call Next to find again.
            reader.next();
        }

        reader
    }
}

#[derive(Default)]
struct Pending {
    pending_writes: Vec<BytesMut>,
    pending_size: u64,
}

impl Pending {
    fn clean_pending_writes(&mut self) {
        self.pending_size = 0;
        self.pending_writes.clear();
    }

    fn pending_writes(&mut self, data: &[u8]) {
        self.pending_size += max_data_write_size(data.len() as u64);
        self.pending_writes.push(data.into())
    }
}

pub struct Reader {
    segment_readers: Vec<SegmentReader>,
    current_reader: u32,
}

impl Reader {
    pub fn skip_current_segment(&mut self) {
        self.current_reader += 1;
    }

    pub fn current_segment_id(&self) -> Option<SegmentID> {
        if self.current_reader as usize >= self.segment_readers.len() {
            None
        } else {
            Some(self.segment_readers[self.current_reader as usize].segment.id)
        }
    }

    pub fn current_chunk_position(&self) -> ChunkPosition {
        let reader = &self.segment_readers[self.current_reader as usize].clone();
        ChunkPosition {
            segment_id: reader.segment.id,
            block_number: reader.block_number,
            chunk_offset: reader.chunk_offset,
            chunk_size: 0,
        }
    }
}

impl Iterator for Reader {
    type Item = (BytesMut, ChunkPosition);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_reader as usize >= self.segment_readers.len() {
            return None;
        }

        let item = self.segment_readers[self.current_reader as usize].next();
        if item.is_none() {
            self.current_reader += 1;
            return self.next();
        }

        item
    }
}

pub fn open<'a>(options: &Options) -> Result<WAL> {
    if !options.segment_file_ext.starts_with(".") {
        return Err(WalError::InvalidSegmentFileExt.into());
    }

    if options.block_cache > options.segment_size as u32 {
        return Err(WalError::BlockCacheSizeTooLarge.into());
    }

    // create the directory if not exists.
    fs::create_dir_all(&options.dir_path)?;

    // create the block cache if needed.
    let cache: Option<Cache> = match options.block_cache > 0 {
        true => {
            let mut lru_size = options.block_cache / BLOCK_SIZE;
            if options.block_cache % BLOCK_SIZE != 0 {
                lru_size += 1;
            }
            Some(Cache::with_capacity(lru_size as usize))
        }
        false => None,
    };

    let (active_segment, older_segments) = load_segments(
        &options.dir_path,
        &options.segment_file_ext,
        cache.clone(),
    )?;

    Ok(WAL {
        inner: RwLock::new(Inner {
            options: options.clone(),
            active_segment,
            older_segments,
            bytes_write: 0,
            rename_ids: vec![],
            block_cache: cache,
        }),
        pending: Mutex::new(Pending {
            pending_size: 0,
            pending_writes: vec![],
        }),
    })
}

fn load_segments(
    dir_path: &PathBuf,
    segment_file_ext: &str,
    cache: Option<Cache>,
) -> Result<(Segment, HashMap<SegmentID, Segment>)> {
    let mut older_segments: HashMap<SegmentID, Segment> = HashMap::new();
    let mut segment_ids: Vec<u32> = read_segment_ids(dir_path, segment_file_ext)?;

    if segment_ids.len() == 0 {
        return Ok((
            segment::open(
                dir_path,
                segment_file_ext,
                INITIAL_SEGMENT_FILE_ID,
                cache.clone(),
            )?,
            older_segments,
        ));
    }

    // open the segment files in order, get the max one as the active segment file.
    segment_ids.sort();

    for i in 0..segment_ids.len() {
        let segment = segment::open(dir_path, segment_file_ext, segment_ids[i], cache.clone())?;
        older_segments.insert(segment.id, segment);
    }

    return Ok((
        older_segments.remove(&segment_ids.last().unwrap()).unwrap(),
        older_segments,
    ));
}

fn read_segment_ids(dir_path: &PathBuf, segment_ext: &str) -> Result<Vec<u32>> {
    let mut segment_ids = Vec::new();

    // Read directory entries
    let entries = fs::read_dir(dir_path)?;

    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            continue;
        }

        // Extract segment id from filename
        let filename = entry.file_name().to_string_lossy().to_string();
        let id_str = filename.splitn(2, segment_ext).next();

        if id_str.is_none() {
            continue;
        }

        let id: u32 = match id_str.unwrap().parse() {
            Ok(num) => num,
            Err(_) => return Err(WalError::ParseSegmentFileIdFailed.into()),
        };

        segment_ids.push(id);
    }

    Ok(segment_ids)
}

fn max_data_write_size(size: u64) -> u64 {
    let chunk_header_size = CHUNK_HEADER_SIZE as u64;
    chunk_header_size + size + (size / BLOCK_SIZE as u64 + 1) * chunk_header_size
}

#[cfg(test)]
mod tests {
    use std::{env, fs, panic, thread};
    use std::path::PathBuf;
    use std::sync::Arc;

    use chrono::Local;
    use rand::{Rng, thread_rng};
    use rand::distributions::Alphanumeric;

    use crate::options::{MB, Options};
    use crate::segment::ChunkPosition;
    use crate::wal::{open, Reader, WAL};

    #[inline]
    fn run_test<T>(func: T)
        where
            T: Fn(&mut WAL) + panic::RefUnwindSafe,
    {
        run_test_with_options(func, None);
    }

    #[inline]
    fn run_test_with_options<T>(func: T, opts: Option<Options>)
        where
            T: Fn(&mut WAL) + panic::RefUnwindSafe,
    {
        let opts = default_opts(opts);

        let result = panic::catch_unwind(|| {
            let mut wal = open(&opts).unwrap();
            func(&mut wal);
        });

        fs::remove_dir_all(&opts.dir_path).unwrap();
        result.unwrap();
    }

    #[inline]
    fn default_opts(opts: Option<Options>) -> Options {
        let dir_path = || {
            let rand_string: String = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(10)
                .map(char::from)
                .collect();

            env::temp_dir().join(format!("wal-{}{}", Local::now().timestamp_millis(), rand_string))
        };

        let mut opts = opts.unwrap_or_else(|| Options {
            dir_path: dir_path().to_owned(),
            segment_size: 32 * MB as u64,
            ..Options::default()
        });

        if opts.dir_path.as_os_str().len() == 0 {
            opts.dir_path = dir_path();
        }

        opts
    }

    #[inline]
    fn write_all_and_iterate(wal: &mut WAL, count: usize, value_size: usize) {
        let val = "wal".repeat(value_size).into_bytes();

        for _ in 0..count {
            wal.pending_writes(&val);
        }

        let positions = wal.write_all().unwrap();
        let mut reader = wal.new_reader();
        assert_eq!(positions.len(), count);

        iterate_all(&mut reader, positions, &val, count);

        let pending = wal.pending.lock().unwrap();
        assert_eq!(pending.pending_writes.len(), 0);
    }

    #[inline]
    fn write_and_iterate(wal: &WAL, count: usize, value_size: usize) {
        let val = "wal".repeat(value_size).into_bytes();
        let mut positions: Vec<ChunkPosition> = Vec::new();

        for _ in 0..count {
            positions.push(wal.write(&val).unwrap());
        }

        let mut reader = wal.new_reader();
        iterate_all(&mut reader, positions, &val, count);
    }

    #[inline]
    fn iterate_all(
        reader: &mut Reader,
        positions: Vec<ChunkPosition>,
        expect_val: &[u8],
        expect_count: usize,
    ) {
        let mut count: usize = 0;

        loop {
            let (data, pos) = match reader.next() {
                None => break,
                Some(x) => x,
            };

            assert_eq!(expect_val, data);
            assert_eq!(positions[count].segment_id, pos.segment_id);
            assert_eq!(positions[count].block_number, pos.block_number);
            assert_eq!(positions[count].chunk_offset, pos.chunk_offset);
            count += 1;
        }

        assert_eq!(expect_count, count);
    }

    #[test]
    fn test_wal_write() {
        run_test(|wal| {
            let pos1 = wal.write(b"hello1").unwrap();
            let pos2 = wal.write(b"hello2").unwrap();
            let pos3 = wal.write(b"hello3").unwrap();
            assert_eq!("hello1", wal.read(&pos1).unwrap());
            assert_eq!("hello2", wal.read(&pos2).unwrap());
            assert_eq!("hello3", wal.read(&pos3).unwrap());
        });
    }

    #[test]
    fn test_wal_write_large() {
        run_test(|wal| {
            write_and_iterate(wal, 100000, 512);
        });
    }

    #[test]
    fn test_wal_write_large2() {
        run_test(|wal| {
            write_and_iterate(wal, 2000, 32 * 1024 * 3 + 10);
        });
    }

    #[test]
    fn test_wal_write_all() {
        run_test(|wal| {
            assert!(wal.empty());
            write_all_and_iterate(wal, 0, 10);
            assert!(wal.empty());
            write_all_and_iterate(wal, 10000, 512);
            assert!(!wal.empty());
        });
    }

    #[test]
    fn test_wal_open_new_active_segment() {
        run_test(|wal| {
            write_all_and_iterate(wal, 2000, 512);

            let old_segment_id = {
                let inner = wal.inner.read().unwrap();
                inner.active_segment.id
            };

            wal.open_new_active_segment().unwrap();

            let new_segment_id = {
                let inner = wal.inner.read().unwrap();
                inner.active_segment.id
            };

            assert_eq!(old_segment_id, new_segment_id - 1);

            let val = "wal".repeat(100).into_bytes();
            for _ in 0..100 {
                wal.write(&val).unwrap();
            }
        });
    }

    #[test]
    fn test_wal_is_empty() {
        run_test(|wal| {
            assert!(wal.empty());
            write_and_iterate(wal, 2000, 512);
            assert!(!wal.empty());
        });
    }

    #[test]
    fn test_wal_reader() {
        run_test(|wal| {
            let size = 100000;
            let val = "wal".repeat(512).into_bytes();

            for _ in 0..size {
                wal.write(&val).unwrap();
            }

            let validate = |w: &mut WAL, size: i32| {
                let mut count = 0;
                let mut reader = w.new_reader();

                loop {
                    let (_, pos) = match reader.next() {
                        None => break,
                        Some(x) => x,
                    };
                    assert_eq!(pos.segment_id, reader.current_segment_id().unwrap());
                    count += 1;
                }
                assert_eq!(count, size);
            };

            validate(wal, size);
            wal.close().unwrap();

            let opts = {
                let inner = wal.inner.read().unwrap();
                inner.options.clone()
            };

            let mut wal2 = open(&opts).unwrap();
            validate(&mut wal2, size);
        });
    }

    #[test]
    fn test_wal_delete() {
        run_test(|wal| {
            write_and_iterate(wal, 2000, 512);
            assert!(!wal.empty());
            wal.delete().unwrap();

            let opts = {
                let inner = wal.inner.read().unwrap();
                inner.options.clone()
            };

            let wal2 = open(&opts).unwrap();
            assert!(wal2.empty());
        });
    }

    #[test]
    fn test_wal_reader_with_start() {
        let mut opts = default_opts(None);
        opts.segment_size = 8 * MB as u64;

        run_test_with_options(|wal| {
            let mut reader1 = wal.new_reader_with_start(&ChunkPosition {
                segment_id: 0,
                block_number: 0,
                chunk_offset: 100,
                chunk_size: 0,
            });
            assert!(reader1.next().is_none());

            write_and_iterate(wal, 20000, 512);

            let mut reader2 = wal.new_reader_with_start(&ChunkPosition {
                segment_id: 0,
                block_number: 0,
                chunk_offset: 0,
                chunk_size: 0,
            });

            let (_, pos2) = reader2.next().unwrap();
            assert_eq!(pos2.block_number, 0);
            assert_eq!(pos2.chunk_offset, 0);

            let mut reader3 = wal.new_reader_with_start(&ChunkPosition {
                segment_id: 3,
                block_number: 5,
                chunk_offset: 0,
                chunk_size: 0,
            });

            let (_, pos3) = reader3.next().unwrap();
            assert_eq!(pos3.segment_id, 3);
            assert_eq!(pos3.block_number, 5);
        }, Some(opts));
    }

    #[test]
    fn test_wal_rename_file_ext() {
        let mut opts = default_opts(None);
        opts.segment_size = 8 * MB as u64;

        run_test_with_options(|wal| {
            write_and_iterate(wal, 20000, 512);
            wal.close().unwrap();

            let validate = |dir_path: &PathBuf, expect_file_ext: &String| {
                let entries = fs::read_dir(dir_path).unwrap();

                for entry in entries {
                    let entry = entry.unwrap();
                    if entry.file_type().unwrap().is_dir() {
                        continue;
                    }

                    let filename = entry.file_name().to_string_lossy().to_string();
                    assert!(filename.ends_with(expect_file_ext));
                }
            };

            let mut opts = {
                let inner = wal.inner.read().unwrap();
                inner.options.clone()
            };

            // Verify the old file ext
            validate(&opts.dir_path, &opts.segment_file_ext);

            opts.segment_file_ext = String::from(".new_seg");
            wal.rename_file_ext(&opts.segment_file_ext).unwrap();

            // Verify the new file ext after rename
            validate(&opts.dir_path, &opts.segment_file_ext);

            // Verify that write data after the rename
            let wal2 = open(&opts).unwrap();
            let val = "W".repeat(512).into_bytes();
            for _ in 0..20000 {
                wal2.write(&val).unwrap();
            }

            // Verify the new file ext after write
            validate(&opts.dir_path, &opts.segment_file_ext);
        }, Some(opts));
    }

    #[test]
    fn test_wal_drop() {
        let opts = default_opts(None);
        let data = "X".repeat(12).into_bytes();

        {
            let wal = open(&opts).unwrap();
            wal.write(&data).unwrap();
        }

        {
            let wal = open(&opts).unwrap();
            wal.write(&data).unwrap();
        }

        let wal = open(&opts).unwrap();
        let mut reader = wal.new_reader();

        let (val, _) = reader.next().unwrap();
        assert_eq!(data, val);

        let (val, _) = reader.next().unwrap();
        assert_eq!(data, val);

        assert!(reader.next().is_none());
    }

    #[test]
    fn test_wal_in_threads() {
        let opts = default_opts(None);
        let wal = open(&opts).unwrap();

        let a1 = Arc::new(wal);
        let a2 = a1.clone();

        let t1 = thread::spawn(move || {
            let b = a1.clone();
            for _ in 0..100 {
                let pos = b.write(b"t1.test").unwrap();
                assert_eq!("t1.test", b.read(&pos).unwrap());
            }
        });

        let t2 = thread::spawn(move || {
            let b = a2.clone();
            for _ in 0..100 {
                let pos = b.write(b"t2.test").unwrap();
                assert_eq!("t2.test", b.read(&pos).unwrap());
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();

        fs::remove_dir_all(&opts.dir_path).unwrap();
    }
}
