//! Exact-or-hashed 128-bit keys for byte strings (8-byte aligned), and a single-array open-addressing map.

const P0: u64 = 0xa076_1d64_78bd_642f;
const P1: u64 = 0xe703_7ed1_a0b4_28db;
const P2: u64 = 0x8ebc_6af0_9c88_c6e3;
const P3: u64 = 0x5899_65cc_7537_4cc3;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
#[repr(C)]
pub struct Key(pub [u64; 2]);

impl Key {
    #[inline(always)]
    pub fn is_zero(&self) -> bool {
        (self.0[0] | self.0[1]) == 0
    }
}

#[inline(always)]
pub fn mum(a: u64, b: u64) -> u64 {
    let r = (a as u128).wrapping_mul(b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}

#[inline(always)]
fn rd8(b: &[u8], i: usize) -> u64 {
    unsafe { (b.as_ptr().add(i) as *const u64).read_unaligned() }
}

#[inline(always)]
fn rd4(b: &[u8], i: usize) -> u64 {
    unsafe { (b.as_ptr().add(i) as *const u32).read_unaligned() as u64 }
}

#[inline(always)]
pub fn key128(b: &[u8]) -> Key {
    let n = b.len();
    if n <= 15 {
        let w1_len = (n as u64) << 56;
        if n > 8 {
            let w0 = rd8(b, 0);
            let w1 = (rd8(b, n - 8) >> ((16 - n) * 8)) | w1_len;
            return Key([w0, w1]);
        } else if n == 8 {
            return Key([rd8(b, 0), w1_len]);
        } else if n >= 4 {
            let w0 = rd4(b, 0) | ((rd4(b, n - 4) >> ((8 - n) * 8)) << 32);
            return Key([w0, w1_len]);
        } else if n > 0 {
            let b0 = unsafe { *b.get_unchecked(0) } as u64;
            let b1 = unsafe { *b.get_unchecked(n >> 1) } as u64;
            let b2 = unsafe { *b.get_unchecked(n - 1) } as u64;
            let w0 = b0 | (b1 << ((n >> 1) * 8)) | (b2 << ((n - 1) * 8));
            return Key([w0, w1_len]);
        } else {
            return Key([0, 0]);
        }
    }
    let mut h1 = P0 ^ (n as u64);
    let mut h2 = P1.wrapping_add(n as u64);
    let mut i = 0;
    while i + 8 <= n {
        let w = rd8(b, i);
        h1 = mum(h1 ^ w, P2);
        h2 = mum(h2 ^ w.rotate_left(29), P3) ^ h1.rotate_left(17);
        i += 8;
    }
    if i < n {
        let w = rd8(b, n - 8);
        h1 = mum(h1 ^ w, P1);
        h2 = mum(h2 ^ w.rotate_left(29), P0) ^ h1.rotate_left(17);
    }
    h1 = mum(h1 ^ P3, h2 ^ P2) ^ h1;
    h2 = mum(h2 ^ P0, h1 ^ P1) ^ h2;
    Key([h1, h2 | (0xFFu64 << 56)])
}

#[inline(always)]
pub fn hash64(k: Key) -> u64 {
    mum(k.0[0] ^ P0, k.0[1] ^ P1)
}

#[inline(always)]
pub fn part_of(k: Key) -> usize {
    (hash64(k) >> 56) as usize
}

#[derive(Clone, Copy, Default)]
#[repr(C)]
pub struct Entry {
    pub k: Key,
    pub v: u64,
}

pub struct KeyMap {
    pub entries: Vec<Entry>,
    pub mask: usize,
    pub len: usize,
}

impl KeyMap {
    pub fn with_capacity(n: usize) -> KeyMap {
        let cap = (n * 2).next_power_of_two().max(16);
        KeyMap {
            entries: vec![Entry::default(); cap],
            mask: cap - 1,
            len: 0,
        }
    }

    #[inline(always)]
    pub fn get(&self, k: Key) -> Option<u64> {
        self.get_with_hash(k, hash64(k))
    }

    #[inline(always)]
    pub fn get_with_hash(&self, k: Key, h: u64) -> Option<u64> {
        if k.is_zero() {
            return None;
        }
        let mut i = h as usize & self.mask;
        loop {
            let e = unsafe { self.entries.get_unchecked(i) };
            if e.k == k {
                return Some(e.v);
            }
            if e.k.is_zero() {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }

    /// Inserts k -> v unless k is present. Returns (existing_or_new_v, was_present).
    #[inline(always)]
    pub fn get_or_insert(&mut self, k: Key, v: u64) -> (u64, bool) {
        if (self.len + 1) * 2 > self.entries.len() {
            self.grow();
        }
        let mut i = hash64(k) as usize & self.mask;
        loop {
            let e = unsafe { self.entries.get_unchecked_mut(i) };
            if e.k == k {
                return (e.v, true);
            }
            if e.k.is_zero() {
                e.k = k;
                e.v = v;
                self.len += 1;
                return (v, false);
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline(always)]
    pub fn insert_if_absent(&mut self, k: Key, v: u64) -> Option<u64> {
        let (old, present) = self.get_or_insert(k, v);
        if present { Some(old) } else { None }
    }

    fn grow(&mut self) {
        let mut n = KeyMap::with_capacity(self.entries.len());
        for e in &self.entries {
            if !e.k.is_zero() {
                n.get_or_insert(e.k, e.v);
            }
        }
        *self = n;
    }
}

const fn masks() -> [[u64; 2]; 17] {
    let mut m = [[0u64; 2]; 17];
    let mut n = 0;
    while n <= 16 {
        let lo = if n >= 8 { u64::MAX } else { (1u64 << (n * 8)) - 1 };
        let hi = if n >= 16 { u64::MAX } else if n <= 8 { 0 } else { (1u64 << ((n - 8) * 8)) - 1 };
        m[n] = [lo, hi];
        n += 1;
    }
    m
}
static IDMASK: [[u64; 2]; 17] = masks();

#[inline(always)]
fn rd8p(p: *const u8) -> u64 {
    unsafe { (p as *const u64).read_unaligned() }
}

/// 64-bit hash of an ID's bytes (user and event IDs). Equal strings give equal hashes.
/// IDs of up to 16 bytes take two masked 8-byte loads and one multiply, with no branch on
/// the length; the loads may run past the slice but never past its 4 KB page (memory is
/// mapped by whole pages), and a field near a page end is copied out first. An empty slice
/// may have a dangling pointer, so it is never loaded.
#[inline(always)]
pub fn idhash(b: &[u8]) -> u64 {
    let n = b.len();
    let p = b.as_ptr();
    if n <= 16 {
        let (lo, hi) = if (n != 0) & ((p as usize & 0xFFF) <= 0xFF0) {
            (rd8p(p), rd8p(unsafe { p.add(8) }))
        } else {
            idhash_copy(b)
        };
        let m = unsafe { IDMASK.get_unchecked(n) };
        return mum((lo & m[0]) ^ P0, (hi & m[1]) ^ P1.wrapping_add((n as u64).wrapping_mul(P3)));
    }
    idhash_long(b)
}

#[cold]
#[inline(never)]
fn idhash_copy(b: &[u8]) -> (u64, u64) {
    let mut buf = [0u8; 16];
    buf[..b.len()].copy_from_slice(b);
    (u64::from_le_bytes(buf[..8].try_into().unwrap()), u64::from_le_bytes(buf[8..].try_into().unwrap()))
}

#[inline(never)]
fn idhash_long(b: &[u8]) -> u64 {
    let n = b.len();
    let mut h = P0 ^ (n as u64);
    let mut i = 0;
    while i + 16 < n {
        h = mum(rd8(b, i) ^ P1 ^ h, rd8(b, i + 8) ^ P2);
        i += 16;
    }
    mum(rd8(b, n - 16) ^ P3 ^ h, rd8(b, n - 8) ^ P0.wrapping_add(n as u64))
}
