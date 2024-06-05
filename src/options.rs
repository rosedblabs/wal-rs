use std::env;
use std::path::PathBuf;

pub const B: u32 = 1;
pub const KB: u32 = 1024 * B;
pub const MB: u32 = 1024 * KB;
pub const GB: u32 = 1024 * MB;

#[derive(Debug, Clone)]
pub struct Options {
    pub dir_path: PathBuf,
    pub segment_size: u64,
    pub segment_file_ext: String,
    pub block_cache: u32,
    pub sync: bool,
    pub bytes_per_sync: u32,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            dir_path: env::temp_dir().join("wal"),
            segment_size: GB as u64,
            segment_file_ext: ".SEG".to_string(),
            block_cache: 32 * KB * 10,
            sync: false,
            bytes_per_sync: 0,
        }
    }
}
