//! A minimal Parquet writer: one row group, one PLAIN uncompressed data page (v1) per
//! column, Thrift compact metadata written by hand. Enough for flat INT32/INT64/BYTE_ARRAY
//! columns, REQUIRED or OPTIONAL.

use std::fs::File;
use std::io::{IoSlice, Write};
use std::path::Path;

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

/// One column's data page body, already PLAIN-encoded, as slices written in order.
struct ColOut<'a> {
    name: &'a str,
    ty: i32,      // physical type: 1 INT32, 2 INT64, 6 BYTE_ARRAY
    logical: u8,  // 0 none, 1 UTF8 string, 6 DATE
    opt: bool,
    body: Vec<&'a [u8]>,
}

/// Writes one row group with one page per column. Column bodies are passed through to the
/// file with a vectored write, without being copied into a staging buffer.
fn write(path: &Path, cols: &[ColOut], nrows: usize) {
    let mut heads: Vec<Vec<u8>> = Vec::with_capacity(cols.len());
    let mut metas: Vec<(i64, i64)> = Vec::with_capacity(cols.len());
    let mut off = 4i64; // after "PAR1"
    for c in cols {
        let plen: usize = c.body.iter().map(|b| b.len()).sum();
        let mut h = T::new();
        h.i32f(1, 0); // DATA_PAGE
        h.i32f(2, plen as i32);
        h.i32f(3, plen as i32);
        h.begin(5);
        h.i32f(1, nrows as i32);
        h.i32f(2, 0); // PLAIN
        h.i32f(3, 3); // RLE
        h.i32f(4, 3); // RLE
        h.end();
        h.b.push(0);
        let size = (h.b.len() + plen) as i64;
        metas.push((off, size));
        off += size;
        heads.push(h.b);
    }
    // FileMetaData
    let mut m = T::new();
    m.i32f(1, 1);
    m.list(2, STRUCT, cols.len() + 1);
    m.begin_elem();
    m.binf(4, b"schema");
    m.i32f(5, cols.len() as i32);
    m.end();
    for c in cols {
        m.begin_elem();
        m.i32f(1, c.ty);
        m.i32f(3, c.opt as i32);
        m.binf(4, c.name.as_bytes());
        match c.logical {
            1 => m.i32f(6, 0), // UTF8
            6 => m.i32f(6, 6), // DATE
            _ => {}
        }
        if c.logical != 0 {
            m.begin(10);
            m.begin(c.logical as i16);
            m.end();
            m.end();
        }
        m.end();
    }
    m.i64f(3, nrows as i64);
    m.list(4, STRUCT, 1);
    m.begin_elem();
    m.list(1, STRUCT, cols.len());
    let mut total = 0i64;
    for (c, &(off, size)) in cols.iter().zip(&metas) {
        total += size;
        m.begin_elem();
        m.i64f(2, off);
        m.begin(3);
        m.i32f(1, c.ty);
        m.list(2, I32, 2);
        m.zz(0);
        m.zz(3);
        m.list(3, BIN, 1);
        m.varint(c.name.len() as u64);
        m.b.extend_from_slice(c.name.as_bytes());
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
    let mlen = (m.b.len() as u32).to_le_bytes();
    m.b.extend_from_slice(&mlen);
    m.b.extend_from_slice(b"PAR1");

    let mut iov: Vec<IoSlice> = vec![IoSlice::new(b"PAR1")];
    for (c, h) in cols.iter().zip(&heads) {
        iov.push(IoSlice::new(h));
        for b in &c.body {
            if !b.is_empty() {
                iov.push(IoSlice::new(b));
            }
        }
    }
    iov.push(IoSlice::new(&m.b));
    let mut f = File::create(path).unwrap();
    let mut s = &mut iov[..];
    while !s.is_empty() {
        let n = f.write_vectored(s).unwrap();
        assert!(n > 0, "short write");
        IoSlice::advance_slices(&mut s, n);
    }
}

/// user_daily columns, PLAIN-encoded as rows are produced, so the file write passes them
/// through unchanged. `sessions` and `events` are INT32 (any integer width is accepted).
#[derive(Default)]
pub struct UdCols {
    uid: Vec<u8>,
    day: Vec<u8>,
    ses: Vec<u8>,
    evn: Vec<u8>,
    rev: Vec<u8>,
    top: Vec<u8>,
    def: Vec<u8>,
    acc: u8,
    n: usize,
}

impl UdCols {
    pub fn with_capacity(rows: usize) -> UdCols {
        UdCols {
            uid: Vec::with_capacity(rows * 16),
            day: Vec::with_capacity(rows * 4),
            ses: Vec::with_capacity(rows * 4),
            evn: Vec::with_capacity(rows * 4),
            rev: Vec::with_capacity(rows * 8),
            top: Vec::with_capacity(rows * 12),
            def: Vec::with_capacity(rows / 8 + 1),
            acc: 0,
            n: 0,
        }
    }

    #[inline(always)]
    pub fn push(&mut self, name: &[u8], day: i32, sessions: i64, events: i64, revenue: i64, top: Option<&[u8]>) {
        self.uid.extend_from_slice(&(name.len() as u32).to_le_bytes());
        self.uid.extend_from_slice(name);
        self.day.extend_from_slice(&day.to_le_bytes());
        self.ses.extend_from_slice(&(sessions as i32).to_le_bytes());
        self.evn.extend_from_slice(&(events as i32).to_le_bytes());
        self.rev.extend_from_slice(&revenue.to_le_bytes());
        if let Some(t) = top {
            self.top.extend_from_slice(&(t.len() as u32).to_le_bytes());
            self.top.extend_from_slice(t);
            self.acc |= 1 << (self.n & 7);
        }
        self.n += 1;
        if self.n & 7 == 0 {
            self.def.push(self.acc);
            self.acc = 0;
        }
    }
}

pub fn write_user_daily(path: &Path, ud: &UdCols) {
    let n = ud.n;
    let mut def = ud.def.clone();
    if n & 7 != 0 {
        def.push(ud.acc);
    }
    // Definition levels: RLE/bit-packed hybrid, bit width 1, all as bit-packed groups of 8.
    let groups = n.div_ceil(8);
    let mut lh: Vec<u8> = vec![0; 4];
    let mut h = ((groups as u64) << 1) | 1;
    while h >= 0x80 {
        lh.push((h as u8) | 0x80);
        h >>= 7;
    }
    lh.push(h as u8);
    let lvlen = (lh.len() - 4 + def.len()) as u32;
    lh[..4].copy_from_slice(&lvlen.to_le_bytes());
    let cols = [
        ColOut { name: "user_id", ty: 6, logical: 1, opt: false, body: vec![&ud.uid] },
        ColOut { name: "day", ty: 1, logical: 6, opt: false, body: vec![&ud.day] },
        ColOut { name: "sessions", ty: 1, logical: 0, opt: false, body: vec![&ud.ses] },
        ColOut { name: "events", ty: 1, logical: 0, opt: false, body: vec![&ud.evn] },
        ColOut { name: "revenue_cents", ty: 2, logical: 0, opt: false, body: vec![&ud.rev] },
        ColOut { name: "top_category", ty: 6, logical: 1, opt: true, body: vec![&lh, &def, &ud.top] },
    ];
    write(path, &cols, n);
}

pub fn write_campaign_daily(path: &Path, rows: &[CdRow]) {
    fn strs<'a>(it: impl Iterator<Item = &'a [u8]>) -> Vec<u8> {
        let mut v = vec![];
        for s in it {
            v.extend_from_slice(&(s.len() as u32).to_le_bytes());
            v.extend_from_slice(s);
        }
        v
    }
    let i64s = |f: &dyn Fn(&CdRow) -> i64| -> Vec<u8> { rows.iter().flat_map(|r| f(r).to_le_bytes()).collect() };
    let camp = strs(rows.iter().map(|r| r.campaign));
    let chan = strs(rows.iter().map(|r| r.channel));
    let day: Vec<u8> = rows.iter().flat_map(|r| r.day.to_le_bytes()).collect();
    let ses = i64s(&|r| r.sessions);
    let pur = i64s(&|r| r.purchasing);
    let rev = i64s(&|r| r.revenue);
    let ast = i64s(&|r| r.assisted);
    let cols = [
        ColOut { name: "campaign_id", ty: 6, logical: 1, opt: false, body: vec![&camp] },
        ColOut { name: "channel", ty: 6, logical: 1, opt: false, body: vec![&chan] },
        ColOut { name: "day", ty: 1, logical: 6, opt: false, body: vec![&day] },
        ColOut { name: "sessions", ty: 2, logical: 0, opt: false, body: vec![&ses] },
        ColOut { name: "purchasing_sessions", ty: 2, logical: 0, opt: false, body: vec![&pur] },
        ColOut { name: "revenue_cents", ty: 2, logical: 0, opt: false, body: vec![&rev] },
        ColOut { name: "assisted_sessions", ty: 2, logical: 0, opt: false, body: vec![&ast] },
    ];
    write(path, &cols, rows.len());
}
