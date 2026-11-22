//! Partition storage on huge pages. All partition records live in fixed-size chunks carved from
//! one anonymous region that is 2 MB aligned and advised MADV_HUGEPAGE, so they sit on huge
//! pages whatever glibc does. Chunks are 64-byte aligned, so K-record non-temporal flushes
//! write whole cache lines. A consumed partition hands its chunks back to the pool and later
//! writers reuse them (no fresh page faults or kernel zeroing for, e.g., events B output).

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};


pub const CHUNK: usize = 64 << 10;
const BLOCK: usize = 2 << 20;

#[cfg(target_os = "linux")]
mod sys {
    use std::ffi::c_void;
    unsafe extern "C" {
        fn mmap(addr: *mut c_void, len: usize, prot: i32, flags: i32, fd: i32, off: i64) -> *mut c_void;
        fn madvise(addr: *mut c_void, len: usize, advice: i32) -> i32;
    }
    const PROT_RW: i32 = 0x1 | 0x2;
    const MAP_PRIVATE_ANON_NORESERVE: i32 = 0x02 | 0x20 | 0x4000;
    const MADV_HUGEPAGE: i32 = 14;
    const MADV_POPULATE_WRITE: i32 = 23;

    /// Maps `len` bytes (plus one block for alignment) and returns the 2 MB aligned start.
    pub fn map(len: usize, huge: bool) -> Option<usize> {
        let p = unsafe { mmap(std::ptr::null_mut(), len + super::BLOCK, PROT_RW, MAP_PRIVATE_ANON_NORESERVE, -1, 0) };
        if p as isize == -1 {
            return None;
        }
        let a = (p as usize + super::BLOCK - 1) & !(super::BLOCK - 1);
        if huge {
            unsafe { madvise(a as *mut c_void, len, MADV_HUGEPAGE) };
        }
        Some(a)
    }

    /// Faults a block in ahead of its writes (EINVAL on kernels before 5.14, then a no-op).
    pub fn populate(a: usize, len: usize) {
        unsafe { madvise(a as *mut c_void, len, MADV_POPULATE_WRITE) };
    }
}

#[cfg(not(target_os = "linux"))]
mod sys {
    pub fn map(len: usize, _huge: bool) -> Option<usize> {
        let l = std::alloc::Layout::from_size_align(len, super::BLOCK).ok()?;
        let p = unsafe { std::alloc::alloc(l) };
        if p.is_null() { None } else { Some(p as usize) }
    }
    pub fn populate(_a: usize, _len: usize) {}
}

fn env_on(var: &str, default: bool) -> bool {
    match std::env::var(var) {
        Ok(v) => v != "0",
        Err(_) => default,
    }
}

pub struct Pool {
    /// Bump cursor and end of the current region.
    bump: Mutex<(usize, usize)>,
    free: Mutex<Vec<usize>>,
    nfree: AtomicUsize,
    huge: bool,
    popw: bool,
}

impl Pool {
    /// Reserves address space for `bytes` of chunks (virtual only; pages are touched on use).
    pub fn new(bytes: usize) -> Pool {
        let huge = env_on("SS_HUGE", true);
        let popw = env_on("SS_POPW", true);
        let len = (bytes.max(BLOCK) + BLOCK - 1) & !(BLOCK - 1);
        let a = sys::map(len, huge).expect("mmap partition region");
        Pool { bump: Mutex::new((a, a + len)), free: Mutex::new(Vec::new()), nfree: AtomicUsize::new(0), huge, popw }
    }

    /// One CHUNK-sized, CHUNK-aligned piece of memory: a recycled chunk if any, else fresh.
    #[cold]
    #[inline(never)]
    pub fn chunk(&self) -> usize {
        if self.nfree.load(Ordering::Relaxed) > 0 {
            let mut f = self.free.lock().unwrap();
            if let Some(c) = f.pop() {
                self.nfree.store(f.len(), Ordering::Relaxed);
                return c;
            }
        }
        let (c, pop) = {
            let mut b = self.bump.lock().unwrap();
            if b.0 + CHUNK > b.1 {
                // Region exhausted (estimate too small): map another.
                let len = 256 * BLOCK;
                let a = sys::map(len, self.huge).expect("mmap partition region");
                *b = (a, a + len);
            }
            let c = b.0;
            b.0 += CHUNK;
            (c, self.popw && c % BLOCK == 0 && c + BLOCK <= b.1)
        };
        if pop {
            sys::populate(c, BLOCK);
        }
        c
    }

    pub fn give(&self, chunks: &[usize]) {
        if chunks.is_empty() {
            return;
        }
        let mut f = self.free.lock().unwrap();
        f.extend_from_slice(chunks);
        self.nfree.store(f.len(), Ordering::Relaxed);
    }
}

/// An append-only list of records stored in pool chunks. A chunk holds up to `cap()` records,
/// a multiple of 8, so aligned 8-record flushes never straddle chunks.
pub struct List<T> {
    /// (chunk address, records in it); the last entry's count is kept in `cur_len`.
    chunks: Vec<(usize, usize)>,
    cur: *mut T,
    cur_len: usize,
    len: usize,
}

unsafe impl<T: Send> Send for List<T> {}

impl<T: Copy> Default for List<T> {
    fn default() -> Self {
        List { chunks: Vec::new(), cur: std::ptr::null_mut(), cur_len: List::<T>::cap(), len: 0 }
    }
}

impl<T: Copy> List<T> {
    #[inline(always)]
    pub const fn cap() -> usize {
        CHUNK / std::mem::size_of::<T>() / 8 * 8
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.len
    }

    #[cold]
    #[inline(never)]
    fn grow(&mut self, pool: &Pool) {
        let c = pool.chunk();
        if let Some(l) = self.chunks.last_mut() {
            l.1 = self.cur_len;
        }
        self.chunks.push((c, 0));
        self.cur = c as *mut T;
        self.cur_len = 0;
    }

    /// Room for `k` more records in the current chunk (k <= 8, and the chunk fill is a
    /// multiple of k when only k-sized reservations are made), returned as a raw pointer.
    #[inline(always)]
    pub fn reserve(&mut self, pool: &Pool, k: usize) -> *mut T {
        if self.cur_len + k > Self::cap() {
            self.grow(pool);
        }
        let p = unsafe { self.cur.add(self.cur_len) };
        self.cur_len += k;
        self.len += k;
        p
    }

    #[inline(always)]
    pub fn push(&mut self, pool: &Pool, v: T) {
        let p = self.reserve(pool, 1);
        unsafe { p.write(v) };
    }

    /// Calls `f` on each chunk's records in insertion order.
    #[inline(always)]
    pub fn for_slices(&self, mut f: impl FnMut(&[T])) {
        let n = self.chunks.len();
        for (i, &(c, l)) in self.chunks.iter().enumerate() {
            let l = if i + 1 == n { self.cur_len } else { l };
            f(unsafe { std::slice::from_raw_parts(c as *const T, l) });
        }
    }

    /// Calls `f` on each chunk's records as a raw (pointer, length) pair, in insertion order.
    /// The chunks belong to this list, so the caller may write through the pointer.
    #[inline(always)]
    pub fn for_slices_raw(&self, mut f: impl FnMut(*mut T, usize)) {
        let n = self.chunks.len();
        for (i, &(c, l)) in self.chunks.iter().enumerate() {
            let l = if i + 1 == n { self.cur_len } else { l };
            f(c as *mut T, l);
        }
    }

    /// Hands the chunks back to the pool for reuse.
    pub fn release(self, pool: &Pool) {
        let v: Vec<usize> = self.chunks.iter().map(|c| c.0).collect();
        pool.give(&v);
    }
}
