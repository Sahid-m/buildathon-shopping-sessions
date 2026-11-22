//! A minimal Parquet writer: one row group, one PLAIN uncompressed data page (v1) per
//! column, Thrift compact metadata written by hand. Enough for flat INT32/INT64/BYTE_ARRAY
//! columns, REQUIRED or OPTIONAL.

use std::fs::File;
use std::io::Write;
use std::path::Path;

use crate::UdRow;

pub struct CdRow<'a> {
    pub campaign: &'a [u8],
    pub channel: &'a [u8],
    pub day: i32,
    pub sessions: i64,
    pub purchasing: i64,
    pub revenue: i64,
    pub assisted: i64,
}

// ---- Thrift compact protocol ----
struct T {
    b: Vec<u8>,
    last: Vec<i16>,
}
const I32: u8 = 5;
const I64: u8 = 6;
const BIN: u8 = 8;
const LIST: u8 = 9;
const STRUCT: u8 = 12;

impl T {
    fn new() -> T {
        T { b: vec![], last: vec![0] }
    }
    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push((v as u8) | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }
    fn zz(&mut self, v: i64) {
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }
    fn field(&mut self, id: i16, ty: u8) {
        let d = id - *self.last.last().unwrap();
        if d > 0 && d <= 15 {
            self.b.push(((d as u8) << 4) | ty);
        } else {
            self.b.push(ty);
            self.zz(id as i64);
        }
        *self.last.last_mut().unwrap() = id;
    }
    fn i32f(&mut self, id: i16, v: i32) {
        self.field(id, I32);
        self.zz(v as i64);
    }
    fn i64f(&mut self, id: i16, v: i64) {
        self.field(id, I64);
        self.zz(v);
    }
    fn binf(&mut self, id: i16, v: &[u8]) {
        self.field(id, BIN);
        self.varint(v.len() as u64);
        self.b.extend_from_slice(v);
    }
    fn begin(&mut self, id: i16) {
        self.field(id, STRUCT);
        self.last.push(0);
    }
    fn begin_elem(&mut self) {
        self.last.push(0);
    }
    fn end(&mut self) {
        self.b.push(0);
        self.last.pop();
    }
    fn list(&mut self, id: i16, ety: u8, n: usize) {
        self.field(id, LIST);
        if n < 15 {
            self.b.push(((n as u8) << 4) | ety);
        } else {
            self.b.push(0xF0 | ety);
            self.varint(n as u64);
        }
    }
}

pub enum Col<'a> {
    Str(&'a str, Vec<&'a [u8]>, Option<Vec<bool>>),
    I32Date(&'a str, Vec<i32>),
    I64(&'a str, Vec<i64>),
}

fn write(path: &Path, cols: Vec<Col>, nrows: usize) {
    let mut out: Vec<u8> = Vec::with_capacity(1 << 20);
    out.extend_from_slice(b"PAR1");
    // (type, optional, offset, size, name, logical)
    let mut metas: Vec<(i32, bool, i64, i64, &str, u8)> = vec![];
    for c in &cols {
        let mut page: Vec<u8> = vec![];
        let (ty, opt, name, logical) = match c {
            Col::Str(name, vals, def) => {
                if let Some(def) = def {
                    // RLE/bit-packed hybrid, bit width 1, all as bit-packed groups of 8
                    let groups = nrows.div_ceil(8);
                    let mut lv: Vec<u8> = vec![];
                    let mut h = ((groups as u64) << 1) | 1;
                    while h >= 0x80 {
                        lv.push((h as u8) | 0x80);
                        h >>= 7;
                    }
                    lv.push(h as u8);
                    let mut bits = vec![0u8; groups];
                    for (i, &d) in def.iter().enumerate() {
                        if d {
                            bits[i >> 3] |= 1 << (i & 7);
                        }
                    }
                    lv.extend_from_slice(&bits);
                    page.extend_from_slice(&(lv.len() as u32).to_le_bytes());
                    page.extend_from_slice(&lv);
                }
                let total: usize = vals.iter().map(|v| v.len() + 4).sum();
                page.reserve(total);
                for v in vals {
                    page.extend_from_slice(&(v.len() as u32).to_le_bytes());
                    page.extend_from_slice(v);
                }
                (6, def.is_some(), *name, 1u8)
            }
            Col::I32Date(name, vals) => {
                page.reserve(vals.len() * 4);
                for v in vals {
                    page.extend_from_slice(&v.to_le_bytes());
                }
                (1, false, *name, 6u8)
            }
            Col::I64(name, vals) => {
                page.reserve(vals.len() * 8);
                for v in vals {
                    page.extend_from_slice(&v.to_le_bytes());
                }
                (2, false, *name, 0u8)
            }
        };
        let mut h = T::new();
        h.i32f(1, 0); // DATA_PAGE
        h.i32f(2, page.len() as i32);
        h.i32f(3, page.len() as i32);
        h.begin(5);
        h.i32f(1, nrows as i32);
        h.i32f(2, 0); // PLAIN
        h.i32f(3, 3); // RLE
        h.i32f(4, 3); // RLE
        h.end();
        h.b.push(0);
        let off = out.len() as i64;
        out.extend_from_slice(&h.b);
        out.extend_from_slice(&page);
        let size = out.len() as i64 - off;
        metas.push((ty, opt, off, size, name, logical));
    }
    // FileMetaData
    let mut m = T::new();
    m.i32f(1, 1);
    m.list(2, STRUCT, metas.len() + 1);
    m.begin_elem();
    m.binf(4, b"schema");
    m.i32f(5, metas.len() as i32);
    m.end();
    for &(ty, opt, _, _, name, logical) in &metas {
        m.begin_elem();
        m.i32f(1, ty);
        m.i32f(3, opt as i32);
        m.binf(4, name.as_bytes());
        match logical {
            1 => m.i32f(6, 0), // UTF8
            6 => m.i32f(6, 6), // DATE
            _ => {}
        }
        if logical != 0 {
            m.begin(10);
            m.begin(logical as i16);
            m.end();
            m.end();
        }
        m.end();
    }
    m.i64f(3, nrows as i64);
    m.list(4, STRUCT, 1);
    m.begin_elem();
    m.list(1, STRUCT, metas.len());
    let mut total = 0i64;
    for &(ty, _, off, size, name, _) in &metas {
        total += size;
        m.begin_elem();
        m.i64f(2, off);
        m.begin(3);
        m.i32f(1, ty);
        m.list(2, I32, 2);
        m.zz(0);
        m.zz(3);
        m.list(3, BIN, 1);
        m.varint(name.len() as u64);
        m.b.extend_from_slice(name.as_bytes());
        m.i32f(4, 0);
        m.i64f(5, nrows as i64);
        m.i64f(6, size);
        m.i64f(7, size);
        m.i64f(9, off);
        m.end();
        m.end();
    }
    m.i64f(2, total);
    m.i64f(3, nrows as i64);
    m.end();
    m.b.push(0);
    out.extend_from_slice(&m.b);
    out.extend_from_slice(&(m.b.len() as u32).to_le_bytes());
    out.extend_from_slice(b"PAR1");
    let mut f = File::create(path).unwrap();
    f.write_all(&out).unwrap();
}

pub fn write_user_daily(path: &Path, rows: &[UdRow], cats: &[&[u8]]) {
    let top: Vec<&[u8]> = rows.iter().filter_map(|r| r.top.map(|c| cats[c as usize])).collect();
    let def: Vec<bool> = rows.iter().map(|r| r.top.is_some()).collect();
    write(
        path,
        vec![
            Col::Str("user_id", rows.iter().map(|r| r.name.get()).collect(), None),
            Col::I32Date("day", rows.iter().map(|r| r.day).collect()),
            Col::I64("sessions", rows.iter().map(|r| r.sessions).collect()),
            Col::I64("events", rows.iter().map(|r| r.events).collect()),
            Col::I64("revenue_cents", rows.iter().map(|r| r.revenue).collect()),
            Col::Str("top_category", top, Some(def)),
        ],
        rows.len(),
    );
}

pub fn write_campaign_daily(path: &Path, rows: &[CdRow]) {
    write(
        path,
        vec![
            Col::Str("campaign_id", rows.iter().map(|r| r.campaign).collect(), None),
            Col::Str("channel", rows.iter().map(|r| r.channel).collect(), None),
            Col::I32Date("day", rows.iter().map(|r| r.day).collect()),
            Col::I64("sessions", rows.iter().map(|r| r.sessions).collect()),
            Col::I64("purchasing_sessions", rows.iter().map(|r| r.purchasing).collect()),
            Col::I64("revenue_cents", rows.iter().map(|r| r.revenue).collect()),
            Col::I64("assisted_sessions", rows.iter().map(|r| r.assisted).collect()),
        ],
        rows.len(),
    );
}
