//! Differential fuzz tests: `csv::for_rows` against a spec oracle and the old `split`,
//! `masks64` SIMD against scalar, `ts::parse_ts` against `ts::parse_ts_ref`.
//! Run with `cargo test --release --offline`.
#![allow(dead_code)]
use crate::{csv, ts};

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 { self.0 ^= self.0 << 13; self.0 ^= self.0 >> 7; self.0 ^= self.0 << 17; self.0 }
    fn below(&mut self, n: u64) -> u64 { self.next() % n }
}

fn spec_rows<const N: usize>(d: &[u8]) -> Vec<Vec<Vec<u8>>> {
    let mut out = vec![];
    let mut lines: Vec<&[u8]> = d.split(|&c| c == b'\n').collect();
    if d.last() == Some(&b'\n') { lines.pop(); }
    if d.is_empty() { lines.clear(); }
    'l: for line in lines {
        let parts: Vec<&[u8]> = line.split(|&c| c == b',').collect();
        if parts.len() != N { continue; }
        let mut row = vec![];
        for p in parts {
            let nq = p.iter().filter(|&&c| c == b'"').count();
            if nq == 0 { row.push(p.to_vec()); }
            else if nq == 2 && p.len() >= 2 && p[0] == b'"' && p[p.len()-1] == b'"' { row.push(p[1..p.len()-1].to_vec()); }
            else { continue 'l; }
        }
        out.push(row);
    }
    out
}

fn old_rows<const N: usize>(d: &[u8]) -> Vec<Vec<Vec<u8>>> {
    let mut out = vec![];
    csv::for_lines(d, |line| { let mut f: [&[u8]; N] = [&[]; N]; if csv::split::<N>(line, &mut f) { out.push(f.iter().map(|x| x.to_vec()).collect()); } });
    out
}
fn new_rows<const N: usize>(d: &[u8]) -> Vec<Vec<Vec<u8>>> {
    let mut out = vec![];
    csv::for_rows::<N, _>(d, |f| { out.push(f.iter().map(|x| x.to_vec()).collect()); });
    out
}

fn gen_field(r: &mut Rng, out: &mut Vec<u8>) {
    const ALPHA: &[u8] = b"ab09\r -:TZ+\"\"\",,\n\x00\xff";
    match r.below(10) {
        0..=4 => { let n = r.below(12); for _ in 0..n { out.push(b'a' + r.below(26) as u8); } }
        5 => { out.push(b'"'); let n = r.below(10); for _ in 0..n { out.push(b'0' + r.below(10) as u8); } out.push(b'"'); }
        6 => { let n = r.below(80); for _ in 0..n { out.push(b'x'); } }
        _ => { let n = r.below(8); for _ in 0..n { out.push(ALPHA[r.below(ALPHA.len() as u64) as usize]); } }
    }
}

pub fn check_csv<const N: usize>(r: &mut Rng, iters: usize, maxl: u64) -> (usize, usize, usize) {
    let (mut rows, mut div, mut lines) = (0, 0, 0);
    for _ in 0..iters {
        let mut d = vec![];
        let nl = r.below(maxl);
        for _ in 0..nl {
            let nf = if r.below(4) == 0 { r.below(N as u64 + 3) } else { N as u64 };
            for j in 0..nf { if j > 0 { d.push(b','); } gen_field(r, &mut d); }
            if r.below(10) == 0 { d.push(b'\r'); }
            d.push(b'\n');
            lines += 1;
        }
        if r.below(3) == 0 && d.last() == Some(&b'\n') { d.pop(); }
        let s = spec_rows::<N>(&d);
        let n = new_rows::<N>(&d);
        assert_eq!(s, n, "spec vs new mismatch on {:?}", String::from_utf8_lossy(&d));
        let o = old_rows::<N>(&d);
        if o != n {
            // only allowed divergence: old accepted a quoted field that contains a comma
            // (spec: every comma separates fields).
            for row in o.iter().filter(|row| !n.contains(row)) {
                assert!(row.iter().any(|f| f.contains(&b',')), "old-only row without comma: {:?} in {:?}", row, String::from_utf8_lossy(&d));
            }
            assert!(n.iter().all(|row| o.contains(row)), "new-only row in {:?}", String::from_utf8_lossy(&d));
            div += 1;
        }
        rows += n.len();
    }
    (rows, div, lines)
}

fn check_masks(r: &mut Rng) {
    for _ in 0..200000 {
        let mut b = [0u8; 64];
        for x in b.iter_mut() { *x = match r.below(4) { 0 => b",\n\"\r"[r.below(4) as usize], 1 => r.next() as u8, _ => b'a' }; }
        assert_eq!(csv::masks64_ref(&b), csv::masks64_simd(&b));
    }
    for v in 0..=255u8 { for pos in 0..64 { let mut b = [b'a'; 64]; b[pos] = v; assert_eq!(csv::masks64_ref(&b), csv::masks64_simd(&b)); } }
}

fn cmp_ts(b: &[u8], cnt: &mut [usize; 2]) {
    let a = ts::parse_ts(b);
    let e = ts::parse_ts_ref(b);
    assert_eq!(a, e, "ts mismatch on {:?}", String::from_utf8_lossy(b));
    cnt[a.is_some() as usize] += 1;
}

fn check_ts(r: &mut Rng) -> [usize; 2] {
    let mut cnt = [0usize; 2];
    let bases: Vec<&[u8]> = vec![b"2026-10-03T23:58:10Z", b"2026-10-03T19:58:10-04:00", b"2024-02-29T00:00:00+23:59", b"1791071890000",
        b"0001791071890000", b"00000000000000000001791071890000", b"99999999999999999999", b"9223372036854775807", b"9223372036854775808",
        b"0000-01-01T00:00:00Z", b"9999-12-31T23:59:59Z", b"1900-02-29T00:00:00Z", b"2000-02-29T00:00:00Z", b"2100-02-28T12:00:00-00:00",
        b"1970-01-01T00:00:00Z", b"1969-12-31T23:59:59Z", b"0000000000000", b"86400000", b"1", b"2026-10-03T23:58:10.123Z", b"00000000000000000000"];
    // every single-byte mutation at every position, plus truncations/extensions
    for base in &bases {
        cmp_ts(base, &mut cnt);
        for pos in 0..base.len() { for v in 0..=255u8 { let mut b = base.to_vec(); b[pos] = v; cmp_ts(&b, &mut cnt); } }
        for l in 0..base.len() { cmp_ts(&base[..l], &mut cnt); }
        for v in 0..=255u8 { let mut b = base.to_vec(); b.push(v); cmp_ts(&b, &mut cnt); let mut b2 = vec![v]; b2.extend_from_slice(base); cmp_ts(&b2, &mut cnt); }
    }
    // all calendar dates in years 0..=9999 x a few times, F1 and F2
    for y in 0..=9999u32 { for mo in 0..=13u32 { for d in [0u32, 1, 15, 28, 29, 30, 31, 32] {
        let hh = r.below(26) as u32; let mm = r.below(62) as u32; let ss = r.below(62) as u32;
        let s = format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, mo, d, hh, mm, ss);
        cmp_ts(s.as_bytes(), &mut cnt);
        let s = format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}{}{:02}:{:02}", y, mo, d, hh, mm, ss, if r.below(2)==0 {'+'} else {'-'}, r.below(26), r.below(62));
        cmp_ts(s.as_bytes(), &mut cnt);
    } } }
    // random strings over a timestamp-ish alphabet
    const AL: &[u8] = b"0123456789-:TZ+tz .\r\"";
    for _ in 0..3_000_000 {
        let n = match r.below(4) { 0 => 13, 1 => 20, 2 => 25, _ => r.below(34) } as usize;
        let mut b = Vec::with_capacity(n);
        let base = bases[r.below(bases.len() as u64) as usize];
        for i in 0..n { b.push(if i < base.len() && r.below(6) != 0 { base[i] } else { AL[r.below(AL.len() as u64) as usize] }); }
        cmp_ts(&b, &mut cnt);
        // random digit strings of length 0..=40
        let n = r.below(41) as usize;
        let b: Vec<u8> = (0..n).map(|_| if r.below(3) == 0 { b'0' } else { b'0' + r.below(10) as u8 }).collect();
        cmp_ts(&b, &mut cnt);
    }
    cnt
}


#[test]
fn fuzz_masks() {
    check_masks(&mut Rng(0x1234_5678_9abc_def1));
}

#[test]
fn fuzz_csv() {
    let mut r = Rng(0x9E37_79B9_7F4A_7C15);
    check_csv::<7>(&mut r, 100_000, 12);
    check_csv::<4>(&mut r, 100_000, 12);
    check_csv::<2>(&mut r, 100_000, 12);
    // Long chunks exercise the 128-byte line path of `for_rows` and its hand-off to the
    // block scanner (lines of 128+ bytes, the chunk tail).
    check_csv::<7>(&mut r, 30_000, 80);
    check_csv::<4>(&mut r, 30_000, 80);
    check_csv::<2>(&mut r, 30_000, 80);
}

#[test]
fn fuzz_ts() {
    check_ts(&mut Rng(0xD1B5_4A32_D192_ED03));
}

/// `ts::parse_ingest` must equal `ts::parse_ts_ref` on every valid F1 string: every day of
/// years 1..=9999 in steps, every day of 1960..=2110, with random times and all edge times.
#[test]
fn ingest_fast_path() {
    let mut r = Rng(0x0123_4567_89AB_CDEF);
    let mut n = 0usize;
    let years: Vec<i64> = (1960..=2110).chain((1..=9999).step_by(37)).chain([1, 1600, 1900, 2000, 2100, 2400, 9999]).collect();
    for y in years {
        for mo in 1..=12 {
            for d in 1..=31 {
                let mut times = vec![(0, 0, 0), (23, 59, 59), (12, 0, 0)];
                for _ in 0..3 {
                    times.push((r.below(24) as i64, r.below(60) as i64, r.below(60) as i64));
                }
                for (h, mi, s) in times {
                    let st = format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}Z");
                    let e = ts::parse_ts_ref(st.as_bytes());
                    if let Some(v) = e {
                        assert_eq!(ts::parse_ingest(st.as_bytes()), Some(v), "{st}");
                        n += 1;
                    }
                }
            }
        }
    }
    // Non-F1 inputs fall back to parse_ts.
    for st in ["1829203731000", "2027-12-18T07:05:50-09:30", "", "2027-12-18 07:05:50Z", "x"] {
        assert_eq!(ts::parse_ingest(st.as_bytes()), ts::parse_ts(st.as_bytes()), "{st}");
    }
    assert!(n > 500_000);
}
