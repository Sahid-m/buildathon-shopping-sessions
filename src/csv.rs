//! The CSV dialect of rules.md: `\n` ends a line, fields split on every comma, a field may
//! be wrapped in double quotes, and any other quote makes the line malformed.

/// Calls `f` for every line of `d` (a chunk of whole lines). A final line without `\n` counts.
#[inline]
pub fn for_lines<F: FnMut(&[u8])>(d: &[u8], mut f: F) {
    let mut s = 0;
    while s < d.len() {
        let e = match d[s..].iter().position(|&c| c == b'\n') {
            Some(p) => s + p,
            None => d.len(),
        };
        f(&d[s..e]);
        s = e + 1;
    }
}

#[inline]
fn unquote(f: &[u8]) -> Option<&[u8]> {
    if f.is_empty() {
        return Some(f);
    }
    if f[0] == b'"' {
        let l = f.len();
        if l >= 2 && f[l - 1] == b'"' && !f[1..l - 1].contains(&b'"') {
            Some(&f[1..l - 1])
        } else {
            None
        }
    } else if f.contains(&b'"') {
        None
    } else {
        Some(f)
    }
}

/// Splits a line into exactly N unquoted fields; false if the line is malformed.
#[inline]
pub fn split<'a, const N: usize>(line: &'a [u8], out: &mut [&'a [u8]; N]) -> bool {
    let mut n = 0;
    let mut start = 0;
    let len = line.len();
    let mut i = 0;
    loop {
        if i == len || line[i] == b',' {
            if n == N {
                return false;
            }
            match unquote(&line[start..i]) {
                Some(v) => out[n] = v,
                None => return false,
            }
            n += 1;
            start = i + 1;
            if i == len {
                break;
            }
        }
        i += 1;
    }
    n == N
}

/// A purchase price: ASCII digits only, value 1 to 10,000,000.
#[inline]
pub fn parse_price(b: &[u8]) -> Option<u32> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (c - b'0') as u64;
        if v > 10_000_000 {
            return None;
        }
    }
    if v >= 1 { Some(v as u32) } else { None }
}
