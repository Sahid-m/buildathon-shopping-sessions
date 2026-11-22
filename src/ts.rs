//! Timestamp forms F1 (`YYYY-MM-DDTHH:MM:SSZ`), F2 (`...+HH:MM`) and F3 (epoch ms digits).

#[inline(always)]
fn d2(b: &[u8], i: usize) -> Option<i64> {
    let a = b[i].wrapping_sub(b'0');
    let c = b[i + 1].wrapping_sub(b'0');
    if a > 9 || c > 9 { None } else { Some((a * 10 + c) as i64) }
}

pub fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// Epoch milliseconds, or None for a bad timestamp. An F3 value too large for i64 gives
/// i64::MAX (it is outside every window anyway).
#[inline]
pub fn parse_ts(b: &[u8]) -> Option<i64> {
    let n = b.len();
    if n == 0 {
        return None;
    }
    if b.iter().all(|c| c.is_ascii_digit()) {
        return parse_f3(b);
    }
    if n != 20 && n != 25 {
        return None;
    }
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let y = d2(b, 0)? * 100 + d2(b, 2)?;
    let mo = d2(b, 5)?;
    let d = d2(b, 8)?;
    let h = d2(b, 11)?;
    let mi = d2(b, 14)?;
    let s = d2(b, 17)?;
    if !(1..=12).contains(&mo) || h > 23 || mi > 59 || s > 59 || d < 1 {
        return None;
    }
    let leap = y % 4 == 0 && (y % 100 != 0 || y % 400 == 0);
    let md = match mo {
        2 => {
            if leap {
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
    Some((days * 86_400 + h * 3600 + mi * 60 + s) * 1000 - off)
}

#[inline]
fn parse_f3(b: &[u8]) -> Option<i64> {
    let mut v: u64 = 0;
    let mut sig = 0;
    for &c in b {
        let x = c.wrapping_sub(b'0');
        if x > 9 {
            return None;
        }
        if sig > 0 || x != 0 {
            sig += 1;
        }
        if sig > 19 {
            // more than 19 significant digits: keep validating digits, value overflows
            return if b.iter().all(|c| c.is_ascii_digit()) { Some(i64::MAX) } else { None };
        }
        v = v * 10 + x as u64;
    }
    Some(if v > i64::MAX as u64 { i64::MAX } else { v as i64 })
}
