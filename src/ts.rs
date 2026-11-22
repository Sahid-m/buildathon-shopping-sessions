//! Timestamp forms F1 (`YYYY-MM-DDTHH:MM:SSZ`), F2 (`...+HH:MM`) and F3 (epoch ms digits).

#[inline(always)]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let a = unsafe { *b.get_unchecked(i) }.wrapping_sub(b'0');
    let c = unsafe { *b.get_unchecked(i + 1) }.wrapping_sub(b'0');
    if a > 9 || c > 9 { None } else { Some((a * 10 + c) as i64) }
}

const DOY_BASE: [i64; 13] = [
    0,
    305, // Jan (mp=10: (1530+2)/5 - 1 = 305)
    336, // Feb (mp=11: (1683+2)/5 - 1 = 336)
    -1,  // Mar (mp=0: 0 - 1 = -1)
    30,  // Apr (mp=1: 30 - 1 = 29? Wait: (153*1+2)/5 - 1 = 30)
    60,  // May (mp=2: (306+2)/5 - 1 = 60)
    91,  // Jun (mp=3: (459+2)/5 - 1 = 91)
    121, // Jul (mp=4: (612+2)/5 - 1 = 121)
    152, // Aug (mp=5: (765+2)/5 - 1 = 152)
    183, // Sep (mp=6: (918+2)/5 - 1 = 183)
    213, // Oct (mp=7: (1071+2)/5 - 1 = 213)
    244, // Nov (mp=8: (1224+2)/5 - 1 = 244)
    274, // Dec (mp=9: (1377+2)/5 - 1 = 274)
];

#[inline(always)]
pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let doy = unsafe { *DOY_BASE.get_unchecked(m as usize) } + d;
    if (2000..2100).contains(&y) {
        let yoe = y - 2000;
        return 11017 + yoe * 365 + (yoe >> 2) + doy;
    }
    days_general(y, doy)
}

#[cold]
#[inline(never)]
fn days_general(y: i64, doy: i64) -> i64 {
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Reference (byte-at-a-time) parser; `parse_ts` must agree with it on every input.
#[allow(dead_code)]
pub fn parse_ts_ref(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 0 {
        return None;
    }
    if (n == 20 || n == 25) && b[4] == b'-' {
        if b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
            return None;
        }
        let y = d2(b, 0)? * 100 + d2(b, 2)?;
        let mo = d2(b, 5)?;
        let d = d2(b, 8)?;
        let h = d2(b, 11)?;
        let mi = d2(b, 14)?;
        let s = d2(b, 17)?;
        if !(1..=12).contains(&mo) || h > 23 || mi > 59 || s > 59 || d < 1 || y < 1 {
            return None;
        }
        let md = match mo {
            2 => {
                if y % 4 == 0 && (y % 100 != 0 || y % 400 == 0) {
                    29
                } else {
                    28
                }
            }
            4 | 6 | 9 | 11 => 30,
            _ => 31,
        };
        if d > md {
            return None;
        }
        let off = if n == 20 {
            if b[19] != b'Z' {
                return None;
            }
            0
        } else {
            let sg = b[19];
            if (sg != b'+' && sg != b'-') || b[22] != b':' {
                return None;
            }
            let oh = d2(b, 20)?;
            let om = d2(b, 23)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let o = (oh * 60 + om) * 60_000;
            if sg == b'-' { -o } else { o }
        };
        let days = days_from_civil(y, mo, d);
        return Some((days * 86_400 + h * 3600 + mi * 60 + s) * 1000 - off);
    }
    parse_f3(b)
}

#[inline(always)]
fn parse_f3(b: &[u8]) -> Option<i64> {
    let mut v: u64 = 0;
    let mut sig = 0usize;
    for &c in b {
        let x = c.wrapping_sub(b'0');
        if x > 9 {
            return None;
        }
        if sig > 0 || x != 0 {
            sig += 1;
        }
        if sig > 19 {
            return if b.iter().all(|c| c.is_ascii_digit()) { Some(i64::MAX) } else { None };
        }
        v = v * 10 + x as u64;
    }
    Some(if v > i64::MAX as u64 { i64::MAX } else { v as i64 })
}

// ---------------------------------------------------------------------------------------
// Fast parser: fixed-layout SWAR for F1/F2 and 13-digit F3, one validity branch each.
// ---------------------------------------------------------------------------------------

#[inline(always)]
fn rd8(b: &[u8], i: usize) -> u64 {
    debug_assert!(i + 8 <= b.len());
    unsafe { (b.as_ptr().add(i) as *const u64).read_unaligned() }
}

/// Byte-wise template check: `x = w ^ tmpl` turns digit bytes into 0..9 and fixed bytes
/// into 0. `dig` has 0xFF on digit bytes, `fix` has 0xFF on fixed bytes. True iff every
/// digit byte is an ASCII digit and every fixed byte equals the template.
#[inline(always)]
fn tmpl_ok(x: u64, dig: u64, fix: u64) -> bool {
    // digit byte valid iff x <= 9: high nibble zero and x+6 does not reach 0x10.
    // A carry out of a byte only happens when that byte already fails.
    let add6 = dig & 0x0606_0606_0606_0606;
    let bad = ((x | x.wrapping_add(add6)) & dig & 0xF0F0_F0F0_F0F0_F0F0) | (x & fix);
    bad == 0
}

#[inline(always)]
fn byte(x: u64, i: u32) -> i64 {
    ((x >> (i * 8)) & 0xFF) as i64
}

const MDAYS: [u8; 16] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0, 0, 0];

/// `YYYY-MM-DDTHH:MM:SS` at b[0..19] (b.len() >= 20). Returns ms since epoch (UTC).
#[inline(always)]
fn civil19(b: &[u8]) -> Option<i64> {
    // w0 = "YYYY-MM-", w1 = "DDTHH:MM", w2 = b[12..20] = "H:MM:SS?" (byte 7 ignored)
    const T0: u64 = u64::from_le_bytes(*b"0000-00-");
    const T1: u64 = u64::from_le_bytes(*b"00T00:00");
    const T2: u64 = u64::from_le_bytes(*b"0:00:00\0");
    const D0: u64 = u64::from_le_bytes([0xFF, 0xFF, 0xFF, 0xFF, 0, 0xFF, 0xFF, 0]);
    const F0: u64 = u64::from_le_bytes([0, 0, 0, 0, 0xFF, 0, 0, 0xFF]);
    const D1: u64 = u64::from_le_bytes([0xFF, 0xFF, 0, 0xFF, 0xFF, 0, 0xFF, 0xFF]);
    const F1: u64 = u64::from_le_bytes([0, 0, 0xFF, 0, 0, 0xFF, 0, 0]);
    const D2: u64 = u64::from_le_bytes([0, 0, 0, 0, 0, 0xFF, 0xFF, 0]);
    const F2: u64 = u64::from_le_bytes([0, 0xFF, 0, 0, 0xFF, 0, 0, 0]);
    let x0 = rd8(b, 0) ^ T0;
    let x1 = rd8(b, 8) ^ T1;
    let x2 = rd8(b, 12) ^ T2;
    if !(tmpl_ok(x0, D0, F0) & tmpl_ok(x1, D1, F1) & tmpl_ok(x2, D2, F2)) {
        return None;
    }
    let y = byte(x0, 0) * 1000 + byte(x0, 1) * 100 + byte(x0, 2) * 10 + byte(x0, 3);
    let mo = byte(x0, 5) * 10 + byte(x0, 6);
    let d = byte(x1, 0) * 10 + byte(x1, 1);
    let h = byte(x1, 3) * 10 + byte(x1, 4);
    let mi = byte(x1, 6) * 10 + byte(x1, 7);
    let s = byte(x2, 5) * 10 + byte(x2, 6);
    let yu = y as u32; // 0..=9999
    let leap = (yu % 4 == 0) & ((yu % 100 != 0) | (yu % 400 == 0));
    // mo <= 99 here; MDAYS lookup is masked to 16 entries and out-of-range months fail.
    let md = MDAYS[(mo & 15) as usize] as i64 + ((mo == 2) & leap) as i64;
    let ok = (mo >= 1) & (mo <= 12) & (d >= 1) & (d <= md) & (h <= 23) & (mi <= 59) & (s <= 59) & (y >= 1);
    if !ok {
        return None;
    }
    let days = days_from_civil(y, mo, d);
    Some((days * 86_400 + h * 3600 + mi * 60 + s) * 1000)
}

/// Eight ASCII digits (already validated, little-endian load) to their value.
#[inline(always)]
fn eight_digits(w: u64) -> u64 {
    let w = w.wrapping_sub(0x3030_3030_3030_3030);
    let w = w.wrapping_mul(10).wrapping_add(w >> 8);
    let w = ((w & 0x0000_00FF_0000_00FF).wrapping_mul(100 + (1_000_000u64 << 32))
        .wrapping_add(((w >> 16) & 0x0000_00FF_0000_00FF).wrapping_mul(1 + (10_000u64 << 32))))
        >> 32;
    w & 0xFFFF_FFFF
}

#[inline(always)]
fn all_digits8(w: u64) -> bool {
    let x = w ^ 0x3030_3030_3030_3030;
    ((x | x.wrapping_add(0x0606_0606_0606_0606)) & 0xF0F0_F0F0_F0F0_F0F0) == 0
}

/// Epoch milliseconds, or None for a bad timestamp. An F3 value too large for i64 gives
/// i64::MAX (it is outside every window anyway).
#[inline(always)]
pub fn parse_ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 20 && b[4] == b'-' {
        if b[19] != b'Z' {
            return None;
        }
        return civil19(b);
    }
    if n == 13 {
        let w0 = rd8(b, 0);
        let w1 = rd8(b, 5);
        if !(all_digits8(w0) & all_digits8(w1)) {
            return None;
        }
        let hi = eight_digits(w0);
        let lo = eight_digits(w1) % 100_000;
        return Some((hi * 100_000 + lo) as i64);
    }
    if n == 25 && b[4] == b'-' {
        // w = b[17..25] = "SS+HH:MM": check HH, ':' and MM here (SS is checked by civil19)
        const T4: u64 = u64::from_le_bytes(*b"\0\0\x0000:00");
        const DIG: u64 = u64::from_le_bytes([0, 0, 0, 0xFF, 0xFF, 0, 0xFF, 0xFF]);
        const FIX: u64 = u64::from_le_bytes([0, 0, 0, 0, 0, 0xFF, 0, 0]);
        let x = rd8(b, 17) ^ T4;
        let sg = b[19];
        if !tmpl_ok(x, DIG, FIX) | ((sg != b'+') & (sg != b'-')) {
            return None;
        }
        let oh = byte(x, 3) * 10 + byte(x, 4);
        let om = byte(x, 6) * 10 + byte(x, 7);
        if (oh > 23) | (om > 59) {
            return None;
        }
        let t = civil19(b)?;
        let o = (oh * 60 + om) * 60_000;
        return Some(if sg == b'-' { t + o } else { t - o });
    }
    if n == 0 {
        return None;
    }
    parse_f3(b)
}

/// `ingest_ts`, which rules.md guarantees is valid F1: a 20-byte `..T..Z` field is converted
/// without calendar or range checks; anything else goes through `parse_ts`.
#[inline(always)]
pub fn parse_ingest(b: &[u8]) -> Option<i64> {
    if b.len() == 20 && (b[10] == b'T') & (b[19] == b'Z') {
        const LO: u64 = 0x0F0F_0F0F_0F0F_0F0F;
        // Each byte becomes (digit_i * 10 + digit_{i+1}); no carries since nibbles <= 15.
        let pair = |w: u64| {
            let x = w & LO;
            x.wrapping_mul(10).wrapping_add(x >> 8)
        };
        let a = pair(rd8(b, 0)); // "YYYY-MM-": byte0 = YY(hi), byte2 = YY(lo)
        let m = pair(rd8(b, 4)); // "-MM-DDTH": byte1 = MM, byte4 = DD
        let c = pair(rd8(b, 11)); // "HH:MM:SS": byte0 = HH, byte3 = MM, byte6 = SS
        let y = byte(a, 0) * 100 + byte(a, 2);
        let mo = byte(m, 1);
        let d = byte(m, 4);
        let sod = byte(c, 0) * 3600 + byte(c, 3) * 60 + byte(c, 6);
        let days = days_from_civil(y, mo, d);
        return Some((days * 86_400 + sod) * 1000);
    }
    parse_ts(b)
}
