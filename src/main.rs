//! Shopping Sessions pipeline. See README.md for the approach and rules.md for the spec.

mod csv;
mod keys;
mod pq;
mod ts;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use keys::{KeyMap, key128, part_of};
use memmap2::Mmap;

pub const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const NP: usize = 256;

#[derive(Clone, Copy)]
pub struct Str {
    ptr: usize,
    len: u32,
}
impl Str {
    fn of(s: &[u8]) -> Str {
        Str { ptr: s.as_ptr() as usize, len: s.len() as u32 }
    }
    #[inline]
    fn get<'a>(&self) -> &'a [u8] {
        unsafe { std::slice::from_raw_parts(self.ptr as *const u8, self.len as usize) }
    }
}

#[derive(Clone, Copy)]
struct EvA {
    ekey: u128,
    ingest: i64,
    ts: i64,
    user: Str,
    price: u32,
    cat: u16,
    flags: u8, // bit0 valid, bit1 purchase
}

#[derive(Clone, Copy)]
struct EvC {
    pkey: u128,
    ts: i64,
    name: Str,
    price: u32,
    cat: u16,
    purchase: bool,
}

#[derive(Clone, Copy)]
struct ClC {
    pkey: u128,
    ckey: u128,
    ts: i64,
    cid: Str,
    camp: u16,
}

pub struct UdRow {
    pub name: Str,
    pub day: i32,
    pub sessions: i64,
    pub events: i64,
    pub revenue: i64,
    pub top: Option<u16>,
}

fn log(t0: &Instant, msg: &str) {
    if std::env::var_os("SS_QUIET").is_none() {
        eprintln!("[{:8.3} ms] {}", t0.elapsed().as_secs_f64() * 1000.0, msg);
    }
}

fn list_csv(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match std::fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().map(|x| x == "csv").unwrap_or(false))
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

fn map_file(p: &Path) -> Option<Mmap> {
    let f = File::open(p).ok()?;
    let len = f.metadata().ok()?.len();
    if len == 0 {
        return None;
    }
    unsafe { Mmap::map(&f).ok() }
}

/// Splits every file's data region (after the header line) into chunks of whole lines.
fn chunks(maps: &[Mmap], target: usize) -> Vec<(usize, usize, usize)> {
    let mut out = vec![];
    for (fi, m) in maps.iter().enumerate() {
        let d: &[u8] = m;
        let Some(h) = memchr(b'\n', d, 0) else { continue };
        let mut s = h + 1;
        while s < d.len() {
            let mut e = s + target;
            if e >= d.len() {
                e = d.len();
            } else {
                e = match memchr(b'\n', d, e) {
                    Some(p) => p + 1,
                    None => d.len(),
                };
            }
            out.push((fi, s, e));
            s = e;
        }
    }
    out
}

#[inline]
fn memchr(c: u8, d: &[u8], from: usize) -> Option<usize> {
    d[from..].iter().position(|&x| x == c).map(|p| p + from)
}

/// Runs `f(i)` for i in 0..n across `nt` threads with dynamic scheduling.
fn par_for<F: Fn(usize, usize) + Sync>(nt: usize, n: usize, f: F) {
    let next = AtomicUsize::new(0);
    std::thread::scope(|s| {
        for t in 0..nt {
            let f = &f;
            let next = &next;
            s.spawn(move || {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= n {
                        break;
                    }
                    f(t, i);
                }
            });
        }
    });
}

fn read_small(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_default()
}

fn small_lines(d: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut lines: Vec<&[u8]> = d.split(|&c| c == b'\n').collect();
    if lines.last().map(|l| l.is_empty()).unwrap_or(false) {
        lines.pop();
    }
    lines.into_iter().skip(1)
}

fn main() {
    let t0 = Instant::now();
    let args: Vec<String> = std::env::args().collect();
    let (data, out) = if args.len() >= 3 {
        (PathBuf::from(&args[1]), PathBuf::from(&args[2]))
    } else {
        (
            PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into())),
            PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into())),
        )
    };
    let nt: usize = std::env::var("NCPU")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4))
        .max(1);

    // manifest
    let man = String::from_utf8_lossy(&read_small(&data.join("manifest.json"))).to_string();
    let get = |k: &str| -> i64 {
        let i = man.find(k).expect("manifest key");
        let rest = &man[i + k.len()..];
        let a = rest.find('"').unwrap() + 1; // closing quote of key
        let rest2 = &rest[a..];
        let b = rest2.find('"').unwrap() + 1;
        let rest3 = &rest2[b..];
        let c = rest3.find('"').unwrap();
        ts::parse_ts(rest3[..c].as_bytes()).expect("manifest ts")
    };
    let ws = get("\"window_start");
    let we = get("\"window_end");
    let cws = ws - WEEK;
    let day0 = ws.div_euclid(DAY);
    let ndays = ((we - 1).div_euclid(DAY) - day0 + 1) as usize;

    // products and campaigns
    let prod_raw = read_small(&data.join("products.csv"));
    let camp_raw = read_small(&data.join("campaigns.csv"));
    let mut cat_names: Vec<&[u8]> = vec![b"unknown"];
    let mut prod_rows: Vec<(&[u8], &[u8])> = vec![];
    for line in small_lines(&prod_raw) {
        let mut f: [&[u8]; 2] = [&[]; 2];
        if csv::split::<2>(line, &mut f) && !f[0].is_empty() {
            prod_rows.push((f[0], f[1]));
            cat_names.push(f[1]);
        }
    }
    cat_names.sort();
    cat_names.dedup();
    let cat_id = |n: &[u8]| cat_names.binary_search(&n).unwrap() as u16;
    let unknown_cat = cat_id(b"unknown");
    let mut products = KeyMap::with_capacity(prod_rows.len());
    for (p, c) in &prod_rows {
        products.insert_if_absent(key128(p), cat_id(c) as u32);
    }
    let mut camp_names: Vec<&[u8]> = vec![];
    let mut camp_chan: Vec<&[u8]> = vec![];
    let mut campaigns = KeyMap::with_capacity(4096);
    for line in small_lines(&camp_raw) {
        let mut f: [&[u8]; 2] = [&[]; 2];
        if csv::split::<2>(line, &mut f) && !f[0].is_empty() {
            if campaigns.get(key128(f[0])).is_none() {
                campaigns.insert_if_absent(key128(f[0]), camp_names.len() as u32);
                camp_names.push(f[0]);
                camp_chan.push(f[1]);
            }
        }
    }
    let ncamp = camp_names.len();
    log(&t0, "small files");

    // map all big files
    let ev_maps: Vec<Mmap> = list_csv(&data.join("events")).iter().filter_map(|p| map_file(p)).collect();
    let cl_maps: Vec<Mmap> = list_csv(&data.join("clicks")).iter().filter_map(|p| map_file(p)).collect();
    let ln_maps: Vec<Mmap> = list_csv(&data.join("links")).iter().filter_map(|p| map_file(p)).collect();
    const CH: usize = 4 << 20;
    let ln_chunks = chunks(&ln_maps, CH);

    // ---- links ----
    let ln_out: Vec<Mutex<Vec<(Str, Str)>>> = (0..ln_chunks.len()).map(|_| Mutex::new(vec![])).collect();
    par_for(nt, ln_chunks.len(), |_, ci| {
        let (fi, s, e) = ln_chunks[ci];
        let d: &[u8] = &ln_maps[fi][s..e];
        let mut v = vec![];
        csv::for_lines(d, |line| {
            let mut f: [&[u8]; 2] = [&[]; 2];
            if csv::split::<2>(line, &mut f) && !f[0].is_empty() && !f[1].is_empty() {
                v.push((Str::of(f[0]), Str::of(f[1])));
            }
        });
        *ln_out[ci].lock().unwrap() = v;
    });
    let mut idmap = KeyMap::with_capacity(1 << 16);
    let mut ids: Vec<Str> = vec![];
    let mut parent: Vec<u32> = vec![];
    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        let mut r = x;
        while parent[r as usize] != r {
            r = parent[r as usize];
        }
        while parent[x as usize] != r {
            let n = parent[x as usize];
            parent[x as usize] = r;
            x = n;
        }
        r
    }
    for m in &ln_out {
        let v = m.lock().unwrap();
        for (a, b) in v.iter() {
            let mut idx = |s: &Str| -> u32 {
                let k = key128(s.get());
                match idmap.get(k) {
                    Some(i) => i,
                    None => {
                        let i = ids.len() as u32;
                        idmap.insert_if_absent(k, i);
                        ids.push(*s);
                        parent.push(i);
                        i
                    }
                }
            };
            let ia = idx(a);
            let ib = idx(b);
            let ra = find(&mut parent, ia);
            let rb = find(&mut parent, ib);
            if ra != rb {
                // keep the root as the byte-wise smallest id
                if ids[ra as usize].get() < ids[rb as usize].get() {
                    parent[rb as usize] = ra;
                } else {
                    parent[ra as usize] = rb;
                }
            }
        }
    }
    // person of each linked id: (name key, name)
    let mut link_person: Vec<(u128, Str)> = Vec::with_capacity(ids.len());
    for i in 0..ids.len() {
        let r = find(&mut parent, i as u32) as usize;
        link_person.push((key128(ids[r].get()), ids[r]));
    }
    let person_of = |user: &[u8]| -> (u128, Str) {
        let k = key128(user);
        match idmap.get(k) {
            Some(i) => link_person[i as usize],
            None => (k, Str::of(user)),
        }
    };
    log(&t0, &format!("links: {} ids", ids.len()));

    // ---- events phase A ----
    let ev_chunks = chunks(&ev_maps, CH);
    let ev_a: Vec<Mutex<Vec<Vec<EvA>>>> = (0..nt).map(|_| Mutex::new((0..NP).map(|_| vec![]).collect())).collect();
    par_for(nt, ev_chunks.len(), |t, ci| {
        let (fi, s, e) = ev_chunks[ci];
        let d: &[u8] = &ev_maps[fi][s..e];
        let mut g = ev_a[t].lock().unwrap();
        csv::for_lines(d, |line| {
            let mut f: [&[u8]; 7] = [&[]; 7];
            if !csv::split::<7>(line, &mut f) || f[0].is_empty() {
                return;
            }
            let ekey = key128(f[0]);
            let ingest = ts::parse_ts(f[3]).unwrap_or(i64::MAX);
            let mut flags = 0u8;
            let mut price = 0u32;
            let mut cat = 0u16;
            let mut tsv = 0i64;
            'v: {
                if f[1].is_empty() || f[5].is_empty() {
                    break 'v;
                }
                let Some(t) = ts::parse_ts(f[2]) else { break 'v };
                if t < ws || t >= we {
                    break 'v;
                }
                let ty = f[4];
                if ty == b"purchase" {
                    let Some(p) = csv::parse_price(f[6]) else { break 'v };
                    price = p;
                    cat = match products.get(key128(f[5])) {
                        Some(c) => c as u16,
                        None => unknown_cat,
                    };
                    flags |= 2;
                } else if ty != b"view" && ty != b"cart" {
                    break 'v;
                }
                if ingest - t > DAY {
                    break 'v;
                }
                tsv = t;
                flags |= 1;
            }
            let p = part_of(ekey);
            g[p].push(EvA { ekey, ingest, ts: tsv, user: Str::of(f[1]), price, cat, flags });
        });
    });
    log(&t0, "events A");

    // ---- clicks ----
    let cl_chunks = chunks(&cl_maps, CH);
    let cl_c: Vec<Mutex<Vec<Vec<ClC>>>> = (0..nt).map(|_| Mutex::new((0..NP).map(|_| vec![]).collect())).collect();
    par_for(nt, cl_chunks.len(), |t, ci| {
        let (fi, s, e) = cl_chunks[ci];
        let d: &[u8] = &cl_maps[fi][s..e];
        let mut g = cl_c[t].lock().unwrap();
        csv::for_lines(d, |line| {
            let mut f: [&[u8]; 4] = [&[]; 4];
            if !csv::split::<4>(line, &mut f) || f[0].is_empty() || f[1].is_empty() {
                return;
            }
            let Some(t) = ts::parse_ts(f[3]) else { return };
            if t < cws || t >= we {
                return;
            }
            let Some(camp) = campaigns.get(key128(f[2])) else { return };
            let (pkey, _) = person_of(f[1]);
            g[part_of(pkey)].push(ClC { pkey, ckey: key128(f[0]), ts: t, cid: Str::of(f[0]), camp: camp as u16 });
        });
    });
    log(&t0, "clicks");

    // ---- events phase B: dedup ----
    let ev_a: Vec<Vec<Vec<EvA>>> = ev_a.into_iter().map(|m| m.into_inner().unwrap()).collect();
    let ev_c: Vec<Mutex<Vec<Vec<EvC>>>> = (0..nt).map(|_| Mutex::new((0..NP).map(|_| vec![]).collect())).collect();
    par_for(nt, NP, |t, p| {
        let n: usize = ev_a.iter().map(|v| v[p].len()).sum();
        let mut tab = KeyMap::with_capacity(n);
        let mut best: Vec<&EvA> = Vec::with_capacity(n);
        for tv in &ev_a {
            for r in &tv[p] {
                match tab.get(r.ekey) {
                    None => {
                        tab.insert_if_absent(r.ekey, best.len() as u32);
                        best.push(r);
                    }
                    Some(i) => {
                        if r.ingest < best[i as usize].ingest {
                            best[i as usize] = r;
                        }
                    }
                }
            }
        }
        let mut g = ev_c[t].lock().unwrap();
        for r in best {
            if r.flags & 1 == 0 {
                continue;
            }
            let (pkey, name) = person_of(r.user.get());
            g[part_of(pkey)].push(EvC {
                pkey,
                ts: r.ts,
                name,
                price: r.price,
                cat: r.cat,
                purchase: r.flags & 2 != 0,
            });
        }
    });
    drop(ev_a);
    log(&t0, "events B");

    // ---- phase C: sessions + attribution ----
    let ev_c: Vec<Vec<Vec<EvC>>> = ev_c.into_iter().map(|m| m.into_inner().unwrap()).collect();
    let cl_c: Vec<Vec<Vec<ClC>>> = cl_c.into_iter().map(|m| m.into_inner().unwrap()).collect();
    let ncat = cat_names.len();
    struct TOut {
        ud: Vec<UdRow>,
        camp: Vec<[i64; 4]>,
    }
    let touts: Vec<Mutex<TOut>> = (0..nt)
        .map(|_| Mutex::new(TOut { ud: vec![], camp: vec![[0; 4]; (ncamp + 1) * ndays] }))
        .collect();
    par_for(nt, NP, |t, p| {
        let mut evs: Vec<EvC> = Vec::with_capacity(ev_c.iter().map(|v| v[p].len()).sum());
        for tv in &ev_c {
            evs.extend_from_slice(&tv[p]);
        }
        evs.sort_unstable_by_key(|e| (e.pkey, e.ts));
        let mut cls: Vec<ClC> = Vec::with_capacity(cl_c.iter().map(|v| v[p].len()).sum());
        for tv in &cl_c {
            cls.extend_from_slice(&tv[p]);
        }
        cls.sort_unstable_by_key(|c| (c.pkey, c.ts, c.ckey));
        cls.dedup_by(|a, b| a.pkey == b.pkey && a.ts == b.ts && a.ckey == b.ckey);

        let mut g = touts[t].lock().unwrap();
        let TOut { ud, camp } = &mut *g;
        let mut cnt = vec![0u32; ncamp];
        let mut pos = vec![0u32; ncamp];
        let mut active: Vec<u16> = vec![];
        let mut catsum = vec![0i64; ncat];
        let mut cat_touched: Vec<u16> = vec![];
        let mut ci = 0usize;
        let mut i = 0usize;
        while i < evs.len() {
            let pk = evs[i].pkey;
            let mut j = i;
            while j < evs.len() && evs[j].pkey == pk {
                j += 1;
            }
            while ci < cls.len() && cls[ci].pkey < pk {
                ci += 1;
            }
            let mut cj = ci;
            while cj < cls.len() && cls[cj].pkey == pk {
                cj += 1;
            }
            let pc = &cls[ci..cj];
            let name = evs[i].name;
            let (mut lo, mut hi) = (0usize, 0usize);
            // current (person, day) row
            let mut cur: Option<UdRow> = None;
            let mut k = i;
            while k < j {
                let start = evs[k].ts;
                let day = start.div_euclid(DAY) as i32;
                let di = (start.div_euclid(DAY) - day0) as usize;
                if cur.as_ref().map(|c| c.day != day).unwrap_or(false) {
                    let mut row = cur.take().unwrap();
                    row.top = top_cat(&mut catsum, &mut cat_touched);
                    ud.push(row);
                }
                let mut last = start;
                let mut m = k;
                let mut rev = 0i64;
                let mut purchased = false;
                while m < j && evs[m].ts - last <= GAP {
                    let e = &evs[m];
                    last = e.ts;
                    if e.purchase {
                        purchased = true;
                        rev += e.price as i64;
                        if catsum[e.cat as usize] == 0 {
                            cat_touched.push(e.cat);
                        }
                        catsum[e.cat as usize] += e.price as i64;
                    }
                    m += 1;
                }
                let nev = (m - k) as i64;
                let row = cur.get_or_insert(UdRow { name, day, sessions: 0, events: 0, revenue: 0, top: None });
                row.sessions += 1;
                row.events += nev;
                row.revenue += rev;
                // attribution
                while hi < pc.len() && pc[hi].ts <= start {
                    let c = pc[hi].camp as usize;
                    if cnt[c] == 0 {
                        pos[c] = active.len() as u32;
                        active.push(c as u16);
                    }
                    cnt[c] += 1;
                    hi += 1;
                }
                while lo < hi && pc[lo].ts < start - WEEK {
                    let c = pc[lo].camp as usize;
                    cnt[c] -= 1;
                    if cnt[c] == 0 {
                        let p = pos[c] as usize;
                        let lastc = *active.last().unwrap();
                        active[p] = lastc;
                        pos[lastc as usize] = p as u32;
                        active.pop();
                    }
                    lo += 1;
                }
                let credited = if hi > lo {
                    let mt = pc[hi - 1].ts;
                    let mut b = hi - 1;
                    let mut q = hi - 1;
                    while q > lo && pc[q - 1].ts == mt {
                        q -= 1;
                        if pc[q].cid.get() < pc[b].cid.get() {
                            b = q;
                        }
                    }
                    for &c in &active {
                        camp[c as usize * ndays + di][3] += 1;
                    }
                    pc[b].camp as usize
                } else {
                    ncamp
                };
                let r = &mut camp[credited * ndays + di];
                r[0] += 1;
                r[1] += purchased as i64;
                r[2] += rev;
                k = m;
            }
            if let Some(mut row) = cur.take() {
                row.top = top_cat(&mut catsum, &mut cat_touched);
                ud.push(row);
            }
            // reset window state
            for q in lo..hi {
                cnt[pc[q].camp as usize] = 0;
            }
            active.clear();
            i = j;
        }
    });
    log(&t0, "phase C");

    // ---- output ----
    let touts: Vec<TOut> = touts.into_iter().map(|m| m.into_inner().unwrap()).collect();
    let mut camp_tot = vec![[0i64; 4]; (ncamp + 1) * ndays];
    for to in &touts {
        for (a, b) in camp_tot.iter_mut().zip(to.camp.iter()) {
            for x in 0..4 {
                a[x] += b[x];
            }
        }
    }
    let ud_dir = out.join("user_daily");
    let cd_dir = out.join("campaign_daily");
    std::fs::create_dir_all(&ud_dir).unwrap();
    std::fs::create_dir_all(&cd_dir).unwrap();
    let cat_strs: Vec<&[u8]> = cat_names.clone();
    std::thread::scope(|s| {
        for (ti, to) in touts.iter().enumerate() {
            let path = ud_dir.join(format!("part-{ti}.parquet"));
            let cat_strs = &cat_strs;
            s.spawn(move || pq::write_user_daily(&path, &to.ud, cat_strs));
        }
        let mut rows: Vec<pq::CdRow> = vec![];
        for c in 0..=ncamp {
            for d in 0..ndays {
                let r = camp_tot[c * ndays + d];
                if r[0] > 0 || r[3] > 0 {
                    let (id, ch): (&[u8], &[u8]) =
                        if c == ncamp { (b"organic", b"organic") } else { (camp_names[c], camp_chan[c]) };
                    rows.push(pq::CdRow {
                        campaign: id,
                        channel: ch,
                        day: (day0 + d as i64) as i32,
                        sessions: r[0],
                        purchasing: r[1],
                        revenue: r[2],
                        assisted: if c == ncamp { 0 } else { r[3] },
                    });
                }
            }
        }
        pq::write_campaign_daily(&cd_dir.join("part-0.parquet"), &rows);
    });
    log(&t0, "done");
}

fn top_cat(catsum: &mut [i64], touched: &mut Vec<u16>) -> Option<u16> {
    let mut best: Option<u16> = None;
    let mut bv = 0i64;
    for &c in touched.iter() {
        let v = catsum[c as usize];
        if v > bv || (v == bv && best.map(|b| c < b).unwrap_or(true)) {
            bv = v;
            best = Some(c);
        }
        catsum[c as usize] = 0;
    }
    touched.clear();
    best
}
