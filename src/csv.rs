//! The CSV dialect of rules.md: `\n` ends a line, fields split on every comma, a field may
//! be wrapped in double quotes, and any other quote makes the line malformed.

/// Calls `f` for every line of `d` (a chunk of whole lines). A final line without `\n` counts.
/// Reference line splitter (kept for tests); the pipeline uses `for_rows`.
#[allow(dead_code)]
#[inline]
pub fn for_lines<F: FnMut(&[u8])>(d: &[u8], mut f: F) {
    let mut s = 0;
    let len = d.len();
    while s < len {
        let e = match memchr_nl(&d[s..]) {
            Some(p) => s + p,
            None => len,
        };
        f(&d[s..e]);
        s = e + 1;
    }
}

#[inline(always)]
pub fn memchr_nl(b: &[u8]) -> Option<usize> {
    unsafe extern "C" {
        fn memchr(s: *const u8, c: i32, n: usize) -> *const u8;
    }
    let p = unsafe { memchr(b.as_ptr(), b'\n' as i32, b.len()) };
    if p.is_null() {
        None
    } else {
        Some(unsafe { p.offset_from(b.as_ptr()) as usize })
    }
}

/// Splits a line into exactly N unquoted fields in a single pass; false if malformed.
#[inline(always)]
pub fn split<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    let len = line.len();
    let mut n = 0;
    let mut i = 0;
    loop {
        if n == N {
            return false;
        }
        if i < len && unsafe { *line.get_unchecked(i) } == b'"' {
            i += 1;
            let start = i;
            while i < len && unsafe { *line.get_unchecked(i) } != b'"' {
                i += 1;
            }
            if i == len {
                return false;
            }
            out[n] = unsafe { line.get_unchecked(start..i) };
            i += 1;
            n += 1;
            if i == len {
                break;
            }
            if unsafe { *line.get_unchecked(i) } != b',' {
                return false;
            }
            i += 1;
        } else {
            let start = i;
            while i < len {
                let c = unsafe { *line.get_unchecked(i) };
                if c == b',' {
                    break;
                }
                if c == b'"' {
                    return false;
                }
                i += 1;
            }
            out[n] = unsafe { line.get_unchecked(start..i) };
            n += 1;
            if i == len {
                break;
            }
            i += 1;
        }
    }
    n == N
}

/// A purchase price: ASCII digits only, value 1 to 10,000,000.
#[inline(always)]
pub fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        let d = c.wrapping_sub(b'0');
        if d > 9 {
            return None;
        }
        v = v * 10 + d as u64;
        if v > 10_000_000 {
            return None;
        }
    }
    if v >= 1 { Some(v as u32) } else { None }
}

// ---------------------------------------------------------------------------------------
// Block scanner: one SIMD pass per 64 bytes gives a bitmask of separators (`,` and `\n`)
// and a bitmask of quotes. Rows are then cut by popping separator bits, and quotes are
// checked once per line with a running popcount.
// ---------------------------------------------------------------------------------------

/// (separator mask, quote mask) for the 64 bytes at `p`. Bit i is byte i.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64) {
    use core::arch::aarch64::*;
    unsafe {
        #[inline(always)]
        unsafe fn bits(a: uint8x16_t, b: uint8x16_t, c: uint8x16_t, d: uint8x16_t) -> u64 {
            unsafe {
                const W: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
                let w = vld1q_u8(W.as_ptr());
                let s0 = vpaddq_u8(vandq_u8(a, w), vandq_u8(b, w));
                let s1 = vpaddq_u8(vandq_u8(c, w), vandq_u8(d, w));
                let s = vpaddq_u8(s0, s1);
                let s = vpaddq_u8(s, s);
                vgetq_lane_u64(vreinterpretq_u64_u8(s), 0)
            }
        }
        let a = vld1q_u8(p);
        let b = vld1q_u8(p.add(16));
        let c = vld1q_u8(p.add(32));
        let d = vld1q_u8(p.add(48));
        let cm = vdupq_n_u8(b',');
        let nl = vdupq_n_u8(b'\n');
        let qt = vdupq_n_u8(b'"');
        let sa = vorrq_u8(vceqq_u8(a, cm), vceqq_u8(a, nl));
        let sb = vorrq_u8(vceqq_u8(b, cm), vceqq_u8(b, nl));
        let sc = vorrq_u8(vceqq_u8(c, cm), vceqq_u8(c, nl));
        let sd = vorrq_u8(vceqq_u8(d, cm), vceqq_u8(d, nl));
        let sep = bits(sa, sb, sc, sd);
        let quo = bits(vceqq_u8(a, qt), vceqq_u8(b, qt), vceqq_u8(c, qt), vceqq_u8(d, qt));
        (sep, quo)
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx512bw"))]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64) {
    use core::arch::x86_64::*;
    unsafe {
        let v = _mm512_loadu_si512(p as *const _);
        let sep = _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b',' as i8))
            | _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'\n' as i8));
        let quo = _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(b'"' as i8));
        (sep, quo)
    }
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2", not(target_feature = "avx512bw")))]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64) {
    use core::arch::x86_64::*;
    unsafe {
        let lo = _mm256_loadu_si256(p as *const __m256i);
        let hi = _mm256_loadu_si256(p.add(32) as *const __m256i);
        let cm = _mm256_set1_epi8(b',' as i8);
        let nl = _mm256_set1_epi8(b'\n' as i8);
        let qt = _mm256_set1_epi8(b'"' as i8);
        let slo = _mm256_or_si256(_mm256_cmpeq_epi8(lo, cm), _mm256_cmpeq_epi8(lo, nl));
        let shi = _mm256_or_si256(_mm256_cmpeq_epi8(hi, cm), _mm256_cmpeq_epi8(hi, nl));
        let sep = (_mm256_movemask_epi8(slo) as u32 as u64) | ((_mm256_movemask_epi8(shi) as u32 as u64) << 32);
        let qlo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, qt)) as u32 as u64;
        let qhi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, qt)) as u32 as u64;
        (sep, qlo | (qhi << 32))
    }
}

#[cfg(not(any(target_arch = "aarch64", all(target_arch = "x86_64", target_feature = "avx2"))))]
#[inline(always)]
unsafe fn masks64(p: *const u8) -> (u64, u64) {
    let mut sep = 0u64;
    let mut quo = 0u64;
    for i in 0..64 {
        let c = unsafe { *p.add(i) };
        sep |= ((c == b',' || c == b'\n') as u64) << i;
        quo |= ((c == b'"') as u64) << i;
    }
    (sep, quo)
}

/// Portable reference for `masks64` (used by tests to check the SIMD versions).
#[allow(dead_code)]
pub fn masks64_ref(b: &[u8; 64]) -> (u64, u64) {
    let mut sep = 0u64;
    let mut quo = 0u64;
    for i in 0..64 {
        let c = b[i];
        sep |= ((c == b',' || c == b'\n') as u64) << i;
        quo |= ((c == b'"') as u64) << i;
    }
    (sep, quo)
}

#[allow(dead_code)]
pub fn masks64_simd(b: &[u8; 64]) -> (u64, u64) {
    unsafe { masks64(b.as_ptr()) }
}

struct Scan<'a> {
    d: &'a [u8],
    base: usize, // start of the current block (== len once exhausted)
    sep: u64,    // separator bits of the current block not yet popped
    qblk: u64,   // quote bits of the whole current block
    qbefore: u32, // quotes in all blocks before `base`
}

#[cold]
#[inline(never)]
fn tail_masks(rest: &[u8]) -> (u64, u64) {
    let mut t = [0u8; 64];
    t[..rest.len()].copy_from_slice(rest);
    unsafe { masks64(t.as_ptr()) }
}

impl<'a> Scan<'a> {
    #[inline(always)]
    fn new(d: &'a [u8]) -> Scan<'a> {
        let mut s = Scan { d, base: 0, sep: 0, qblk: 0, qbefore: 0 };
        s.load();
        s
    }

    #[inline(always)]
    fn load(&mut self) {
        let len = self.d.len();
        let (sep, quo) = if self.base + 64 <= len {
            unsafe { masks64(self.d.as_ptr().add(self.base)) }
        } else {
            tail_masks(&self.d[self.base..])
        };
        self.sep = sep;
        self.qblk = quo;
    }

    /// Next separator position, or `len` (a virtual line end) once the data is exhausted.
    #[inline(always)]
    fn next(&mut self) -> usize {
        loop {
            if self.sep != 0 {
                let tz = self.sep.trailing_zeros() as usize;
                self.sep &= self.sep - 1;
                return self.base + tz;
            }
            self.qbefore = self.qbefore.wrapping_add(self.qblk.count_ones());
            let len = self.d.len();
            if self.base + 64 >= len {
                self.base = len;
                self.qblk = 0;
                return len;
            }
            self.base += 64;
            self.load();
        }
    }

    /// Number of quotes before position `p` (p must come from the last `next()`).
    #[inline(always)]
    fn quotes_before(&self, p: usize) -> u32 {
        let o = p - self.base; // < 64, or 0 for the virtual end
        self.qbefore.wrapping_add((self.qblk & ((1u64 << o) - 1)).count_ones())
    }
}

/// Calls `f` with the N fields (quotes stripped) of every well-formed line of `d`, a chunk
/// of whole lines (a final line without `\n` counts). A line is well-formed when it has
/// exactly N comma-separated fields and every quote wraps a whole field (`"x"`, `""`).
/// Every comma separates fields, quoted or not. Empty lines are dropped (N >= 2).
#[inline(always)]
pub fn for_rows_scan<'a, const N: usize, F: FnMut(&[&'a [u8]; N])>(d: &'a [u8], mut f: F) {
    let len = d.len();
    if len == 0 {
        return;
    }
    let mut sc = Scan::new(d);
    let mut start = 0usize;
    let mut qprev = 0u32;
    let ptr = d.as_ptr();
    while start < len {
        let mut ends = [0usize; N];
        let mut k = 0usize;
        let p = loop {
            let p = sc.next();
            if p >= len || unsafe { *ptr.add(p) } == b'\n' {
                if k == N - 1 {
                    ends[k] = p;
                    k = N;
                } else {
                    k = usize::MAX;
                }
                break p;
            }
            if k < N - 1 {
                ends[k] = p;
            }
            k += 1;
        };
        let q = sc.quotes_before(p);
        let nq = q.wrapping_sub(qprev);
        qprev = q;
        if k == N {
            // Branchless quote stripping: a field is stripped when it is `"…"` (len >= 2).
            // The line is valid iff its quote count is exactly 2 per stripped field.
            let mut fields: [&'a [u8]; N] = [&[]; N];
            let mut s = start;
            let mut stripped = 0usize;
            for i in 0..N {
                let e = ends[i];
                let n = e - s;
                let first = unsafe { *ptr.add(s.min(len - 1)) };
                let last = unsafe { *ptr.add(e.max(1) - 1) };
                let q = ((n >= 2) & (first == b'"') & (last == b'"')) as usize;
                fields[i] = unsafe { std::slice::from_raw_parts(ptr.add(s + q), n - 2 * q) };
                stripped += q;
                s = e + 1;
            }
            if nq as usize == 2 * stripped {
                f(&fields);
            }
        }
        start = p + 1;
    }
}

/// Masks of `,`, `\n` and `"` for the 64 bytes at `p`. Bit i is byte i.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
unsafe fn masks3(p: *const u8) -> (u64, u64, u64) {
    use core::arch::x86_64::*;
    unsafe {
        let lo = _mm256_loadu_si256(p as *const __m256i);
        let hi = _mm256_loadu_si256(p.add(32) as *const __m256i);
        #[inline(always)]
        unsafe fn m(lo: __m256i, hi: __m256i, c: u8) -> u64 {
            unsafe {
                let v = _mm256_set1_epi8(c as i8);
                (_mm256_movemask_epi8(_mm256_cmpeq_epi8(lo, v)) as u32 as u64)
                    | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(hi, v)) as u32 as u64) << 32)
            }
        }
        (m(lo, hi, b','), m(lo, hi, b'\n'), m(lo, hi, b'"'))
    }
}

#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn masks3(p: *const u8) -> (u64, u64, u64) {
    use core::arch::aarch64::*;
    unsafe {
        #[inline(always)]
        unsafe fn bits(a: uint8x16_t, b: uint8x16_t, c: uint8x16_t, d: uint8x16_t) -> u64 {
            unsafe {
                const W: [u8; 16] = [1, 2, 4, 8, 16, 32, 64, 128, 1, 2, 4, 8, 16, 32, 64, 128];
                let w = vld1q_u8(W.as_ptr());
                let s0 = vpaddq_u8(vandq_u8(a, w), vandq_u8(b, w));
                let s1 = vpaddq_u8(vandq_u8(c, w), vandq_u8(d, w));
                let s = vpaddq_u8(s0, s1);
                let s = vpaddq_u8(s, s);
                vgetq_lane_u64(vreinterpretq_u64_u8(s), 0)
            }
        }
        let a = vld1q_u8(p);
        let b = vld1q_u8(p.add(16));
        let c = vld1q_u8(p.add(32));
        let d = vld1q_u8(p.add(48));
        let m = |ch: u8| {
            let v = vdupq_n_u8(ch);
            bits(vceqq_u8(a, v), vceqq_u8(b, v), vceqq_u8(c, v), vceqq_u8(d, v))
        };
        (m(b','), m(b'\n'), m(b'"'))
    }
}

#[cfg(not(any(target_arch = "aarch64", all(target_arch = "x86_64", target_feature = "avx2"))))]
#[inline(always)]
unsafe fn masks3(p: *const u8) -> (u64, u64, u64) {
    let (mut c, mut n, mut q) = (0u64, 0u64, 0u64);
    for i in 0..64 {
        let x = unsafe { *p.add(i) };
        c |= ((x == b',') as u64) << i;
        n |= ((x == b'\n') as u64) << i;
        q |= ((x == b'"') as u64) << i;
    }
    (c, n, q)
}

/// Calls `f` with the N fields (quotes stripped) of every well-formed line of `d`, a chunk
/// of whole lines (a final line without `\n` counts). A line is well-formed when it has
/// exactly N comma-separated fields and every quote wraps a whole field (`"x"`, `""`).
/// Every comma separates fields, quoted or not. Empty lines are dropped (N >= 2).
///
/// Line at a time: while 128 bytes remain, the comma, newline and quote masks of the 128
/// bytes at the line start give the line end (first newline), the comma count and the quote
/// count with no branch per field. Lines of 128+ bytes and the last 128 bytes of the chunk
/// go through `for_rows_scan`.
#[inline(always)]
pub fn for_rows<'a, const N: usize, F: FnMut(&[&'a [u8]; N])>(d: &'a [u8], mut f: F) {
    let len = d.len();
    let ptr = d.as_ptr();
    let mut start = 0usize;
    while start + 128 <= len {
        let (c0, n0, q0, c1, n1, q1) = unsafe {
            let p = ptr.add(start);
            let (c0, n0, q0) = masks3(p);
            let (c1, n1, q1) = masks3(p.add(64));
            (c0, n0, q0, c1, n1, q1)
        };
        let nl = (n0 as u128) | ((n1 as u128) << 64);
        if nl == 0 {
            // A line of 128+ bytes.
            let end = match memchr_nl(&d[start + 128..]) {
                Some(x) => start + 128 + x + 1,
                None => len,
            };
            for_rows_scan::<N, _>(&d[start..end], &mut f);
            start = end;
            continue;
        }
        let e = nl.trailing_zeros() as usize; // line end, relative to start
        let below = (1u128 << e) - 1;
        let mut cm = ((c0 as u128) | ((c1 as u128) << 64)) & below;
        let nq = (((q0 as u128) | ((q1 as u128) << 64)) & below).count_ones() as usize;
        if cm.count_ones() as usize == N - 1 {
            let mut fields: [&'a [u8]; N] = [&[]; N];
            let mut s = start;
            let mut stripped = 0usize;
            for i in 0..N {
                let fe = if i + 1 < N {
                    let c = cm.trailing_zeros() as usize;
                    cm &= cm.wrapping_sub(1);
                    start + c
                } else {
                    start + e
                };
                let n = fe - s;
                let first = unsafe { *ptr.add(s) };
                let last = unsafe { *ptr.add(fe.max(1) - 1) };
                let q = ((n >= 2) & (first == b'"') & (last == b'"')) as usize;
                fields[i] = unsafe { std::slice::from_raw_parts(ptr.add(s + q), n - 2 * q) };
                stripped += q;
                s = fe + 1;
            }
            if nq == 2 * stripped {
                f(&fields);
            }
        }
        start += e + 1;
    }
    if start < len {
        for_rows_scan::<N, _>(&d[start..], &mut f);
    }
}

/// Hint the CPU to pull the cache line at `p` into L1. Never faults.
#[inline(always)]
pub fn prefetch(p: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch(p as *const i8, core::arch::x86_64::_MM_HINT_T0);
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!("prfm pldl1keep, [{0}]", in(reg) p, options(nostack, readonly, preserves_flags));
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    let _ = p;
}
