//! The CSV dialect of rules.md: `\n` ends a line, fields split on every comma, a field may
//! be wrapped in double quotes, and any other quote makes the line malformed.

/// Calls `f` for every line of `d` (a chunk of whole lines). A final line without `\n` counts.
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
