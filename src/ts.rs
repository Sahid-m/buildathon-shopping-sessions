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
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Epoch milliseconds, or None for a bad timestamp. An F3 value too large for i64 gives
/// i64::MAX (it is outside every window anyway).
#[inline(always)]
pub fn parse_ts(b: &[u8]) -> Option<i64> {
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
