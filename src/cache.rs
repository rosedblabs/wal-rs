use std::num::NonZeroUsize;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use lru::LruCache;

#[derive(Debug, Clone)]
pub struct Cache(Arc<Mutex<LruCache<u64, BytesMut>>>);

impl Cache {
    pub fn with_capacity(size: usize) -> Cache {
        Cache(Arc::new(Mutex::new(LruCache::new(NonZeroUsize::new(size).unwrap()))))
    }
}

impl Default for Cache {
    fn default() -> Self {
        Cache::with_capacity(1024)
    }
}

impl Deref for Cache {
    type Target = Arc<Mutex<LruCache<u64, BytesMut>>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for Cache {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
