//! Exact-or-hashed 128-bit keys for byte strings, and a small open-addressing map.
//!
//! Strings of up to 15 bytes are packed losslessly (bytes + length in the top byte), so
//! equal keys mean equal strings. Longer strings get a 120-bit hash with the top byte
//! set to 0xFF, so they never collide with packed keys.

const P0: u64 = 0xa076_1d64_78bd_642f;
const P1: u64 = 0xe703_7ed1_a0b4_28db;
const P2: u64 = 0x8ebc_6af0_9c88_c6e3;
const P3: u64 = 0x5899_65cc_7537_4cc3;

#[inline(always)]
fn mum(a: u64, b: u64) -> u64 {
    let r = (a as u128) * (b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}

#[inline(always)]
fn rd8(b: &[u8], i: usize) -> u64 {
    u64::from_le_bytes(b[i..i + 8].try_into().unwrap())
}

#[inline]
pub fn key128(b: &[u8]) -> u128 {
    let n = b.len();
    if n <= 15 {
        let mut buf = [0u8; 16];
        buf[..n].copy_from_slice(b);
        buf[15] = n as u8;
        return u128::from_le_bytes(buf);
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
    let k = ((h2 as u128) << 64) | (h1 as u128);
    k | (0xFFu128 << 120)
}

#[inline(always)]
pub fn hash64(k: u128) -> u64 {
    mum((k as u64) ^ P0, ((k >> 64) as u64) ^ P1)
}

#[inline(always)]
pub fn part_of(k: u128) -> usize {
    (hash64(k) >> 56) as usize
}

pub struct KeyMap {
    keys: Vec<u128>,
    vals: Vec<u32>,
    mask: usize,
    len: usize,
}

impl KeyMap {
    pub fn with_capacity(n: usize) -> KeyMap {
        let cap = (n * 2).next_power_of_two().max(16);
        KeyMap { keys: vec![0; cap], vals: vec![0; cap], mask: cap - 1, len: 0 }
    }
    #[inline]
    pub fn get(&self, k: u128) -> Option<u32> {
        if k == 0 {
            return None;
        }
        let mut i = hash64(k) as usize & self.mask;
        loop {
            let x = self.keys[i];
            if x == k {
                return Some(self.vals[i]);
            }
            if x == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    /// Inserts k -> v unless k is present. Returns the existing value if present.
    #[inline]
    pub fn insert_if_absent(&mut self, k: u128, v: u32) -> Option<u32> {
        debug_assert!(k != 0);
        if (self.len + 1) * 2 > self.keys.len() {
            self.grow();
        }
        let mut i = hash64(k) as usize & self.mask;
        loop {
            let x = self.keys[i];
            if x == k {
                return Some(self.vals[i]);
            }
            if x == 0 {
                self.keys[i] = k;
                self.vals[i] = v;
                self.len += 1;
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    fn grow(&mut self) {
        let mut n = KeyMap::with_capacity(self.keys.len());
        for (i, &k) in self.keys.iter().enumerate() {
            if k != 0 {
                n.insert_if_absent(k, self.vals[i]);
            }
        }
        *self = n;
    }
}
