use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum WalError {
    #[error("Unknown chunk type: {0}")]
    UnknownChunkType(u8),

    #[error("The segment file is closed")]
    SegmentFileClosed,

    #[error("Invalid crc, the data may be corrupted")]
    InvalidCRC,

    #[error("The segment file extension must start with '.'")]
    InvalidSegmentFileExt,

    #[error("BlockCache must be smaller than SegmentSize")]
    BlockCacheSizeTooLarge,

    #[error("Failed parse segment file id")]
    ParseSegmentFileIdFailed,

    #[error("The data size can't larger than segment size")]
    ValueTooLarge,

    #[error("The upper bound of pendingWrites can't larger than segment size")]
    PendingSizeTooLarge,

    #[error("The segment file {0}{1} not found")]
    SegmentFileNotFound(u32, String),
}
