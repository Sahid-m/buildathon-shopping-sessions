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

use keys::{Key, KeyMap, hash64, key128, mum};
use memmap2::Mmap;

pub const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const NP: usize = 256;
const ING_MASK: u64 = 0x000F_FFFF_FFFF_FFFF;
const INVALID_DT: u64 = 0x0000_00FF_FFFF_FFFF;

#[derive(Clone, Copy, Default)]
#[repr(transparent)]
pub struct Str(pub u64);

impl Str {
    #[inline(always)]
    fn of(s: &[u8]) -> Str {
        Str((s.as_ptr() as u64 & 0x0000_FFFF_FFFF_FFFF) | ((s.len() as u64) << 48))
    }
    #[inline(always)]
    fn get<'a>(&self) -> &'a [u8] {
        unsafe {
            std::slice::from_raw_parts(
                (self.0 & 0x0000_FFFF_FFFF_FFFF) as *const u8,
                (self.0 >> 48) as usize,
            )
        }
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
struct EvA {
    ehash: u64,        // 8B: hash64(key128(event_id))
    ingest_cat: u64,   // 8B: low 52b = biased ingest, high 12b = cat
    user: Str,         // 8B
    ts_lo: u32,        // 4B: low 32b of (ts - ws)
    price_ts_hi: u32,  // 4B: low 24b = price (0 if not purchase), high 8b = high 8b of (ts - ws) -> 32B total
}

impl EvA {
    #[inline(always)]
    fn ingest(&self) -> u64 {
        self.ingest_cat & ING_MASK
    }
    #[inline(always)]
    fn cat(&self) -> u16 {
        (self.ingest_cat >> 52) as u16
    }
    #[inline(always)]
    fn dt(&self) -> u64 {
        (self.ts_lo as u64) | (((self.price_ts_hi >> 24) as u64) << 32)
    }
    #[inline(always)]
    fn price(&self) -> u32 {
        self.price_ts_hi & 0x00FF_FFFF
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
struct EvC {
    pkey: u64,      // 8B
    ts: i64,        // 8B
    name: Str,      // 8B
    price: u32,     // 4B
    cat: u16,       // 2B
    purchase: bool, // 1B + 1B pad -> 32B total
}

#[derive(Clone, Copy)]
#[repr(C)]
struct ClC {
    pkey: u64, // 8B
    ts: i64,   // 8B
    cid: Str,  // 8B
    camp: u16, // 2B + 6B pad -> 32B total
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
        let Some(h) = csv::memchr_nl(d) else { continue };
        let mut s = h + 1;
        while s < d.len() {
            let mut e = s + target;
            if e >= d.len() {
                e = d.len();
            } else {
                e = match csv::memchr_nl(&d[e..]) {
                    Some(p) => e + p + 1,
                    None => d.len(),
                };
            }
            out.push((fi, s, e));
            s = e;
        }
    }
    out
}

/// Runs `f(t, i)` for i in 0..n across `nt` threads with dynamic scheduling.
fn par_for<F: Fn(usize, usize) + Sync>(nt: usize, n: usize, f: F) {
    if n == 0 {
        return;
    }
    let next = AtomicUsize::new(0);
    let threads = nt.min(n);
    std::thread::scope(|s| {
        for t in 0..threads {
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
        let a = rest.find('"').unwrap() + 1;
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
        products.insert_if_absent(key128(p), cat_id(c) as u64);
    }
    let mut camp_names: Vec<&[u8]> = vec![];
    let mut camp_chan: Vec<&[u8]> = vec![];
    let mut campaigns = KeyMap::with_capacity(4096);
    for line in small_lines(&camp_raw) {
        let mut f: [&[u8]; 2] = [&[]; 2];
        if csv::split::<2>(line, &mut f) && !f[0].is_empty() {
            let k = key128(f[0]);
            let (_, present) = campaigns.get_or_insert(k, camp_names.len() as u64);
            if !present {
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

    // ---- links: parallel parse + pre-hash ----
    let ln_out: Vec<Mutex<Vec<(Key, Str, Key, Str)>>> = (0..ln_chunks.len()).map(|_| Mutex::new(vec![])).collect();
    par_for(nt, ln_chunks.len(), |_, ci| {
        let (fi, s, e) = ln_chunks[ci];
        let d: &[u8] = &ln_maps[fi][s..e];
        let mut v = Vec::with_capacity((e - s) / 22);
        csv::for_lines(d, |line| {
            let mut f: [&[u8]; 2] = [&[]; 2];
            if csv::split::<2>(line, &mut f) && !f[0].is_empty() && !f[1].is_empty() {
                v.push((key128(f[0]), Str::of(f[0]), key128(f[1]), Str::of(f[1])));
            }
        });
        *ln_out[ci].lock().unwrap() = v;
    });

    // ---- overlap union-find with events phase A ----
    let ev_chunks = chunks(&ev_maps, CH);
    let total_ev_bytes: usize = ev_maps.iter().map(|m| m.len()).sum();
    let est_ev_per_part = (total_ev_bytes / 85 / nt.max(1) / NP).max(64);
    let ev_a: Vec<Mutex<Vec<Vec<EvA>>>> = (0..nt)
        .map(|_| Mutex::new((0..NP).map(|_| Vec::with_capacity(est_ev_per_part)).collect()))
        .collect();

    let total_edges: usize = ln_out.iter().map(|m| m.lock().unwrap().len()).sum();
    let pmap_cap = ((total_edges * 2 + 64) * 2).next_power_of_two().max(16);
    let mut pmap: Vec<(u64, u64)> = Vec::new();

    let next_ev = AtomicUsize::new(0);
    std::thread::scope(|s| {
        let ln_out_ref = &ln_out;
        let pmap_ptr = &mut pmap;
        let ev_a_ref = &ev_a;
        let ev_chunks_ref = &ev_chunks;
        let ev_maps_ref = &ev_maps;
        let products_ref = &products;
        let next_ev_ref = &next_ev;

        // Thread 0 builds union-find + compact 16B/slot pmap, then helps with events A
        s.spawn(move || {
            let mut idmap = KeyMap::with_capacity(total_edges * 2 + 64);
            let mut ids: Vec<Str> = Vec::with_capacity(total_edges * 2);
            let mut parent: Vec<u32> = Vec::with_capacity(total_edges * 2);
            #[inline(always)]
            fn find(parent: &mut [u32], mut x: u32) -> u32 {
                let mut r = x;
                while unsafe { *parent.get_unchecked(r as usize) } != r {
                    r = unsafe { *parent.get_unchecked(r as usize) };
                }
                while unsafe { *parent.get_unchecked(x as usize) } != r {
                    let n = unsafe { *parent.get_unchecked(x as usize) };
                    unsafe { *parent.get_unchecked_mut(x as usize) = r };
                    x = n;
                }
                r
            }
            for m in ln_out_ref {
                let v = m.lock().unwrap();
                for &(ka, sa, kb, sb) in v.iter() {
                    let next_a = ids.len() as u64;
                    let (ia, pres_a) = idmap.get_or_insert(ka, next_a);
                    if !pres_a {
                        ids.push(sa);
                        parent.push(ia as u32);
                    }
                    let next_b = ids.len() as u64;
                    let (ib, pres_b) = idmap.get_or_insert(kb, next_b);
                    if !pres_b {
                        ids.push(sb);
                        parent.push(ib as u32);
                    }
                    let ra = find(&mut parent, ia as u32);
                    let rb = find(&mut parent, ib as u32);
                    if ra != rb {
                        if ids[ra as usize].get() < ids[rb as usize].get() {
                            parent[rb as usize] = ra;
                        } else {
                            parent[ra as usize] = rb;
                        }
                    }
                }
            }
            // Pack into 16B/slot (hash_nz, root_str) table for fast L3-friendly lookups
            let cap = idmap.entries.len().max(pmap_cap);
            let mask = cap - 1;
            pmap_ptr.resize(cap, (0, 0));
            for e in &idmap.entries {
                if !e.k.is_zero() {
                    let r = find(&mut parent, e.v as u32) as usize;
                    let raw = ids[r].0;
                    let h_nz = hash64(e.k) | 1;
                    let mut i = (h_nz as usize) & mask;
                    while unsafe { pmap_ptr.get_unchecked(i).0 } != 0 {
                        i = (i + 1) & mask;
                    }
                    unsafe { *pmap_ptr.get_unchecked_mut(i) = (h_nz, raw) };
                }
            }
            drop(idmap);
            drop(ids);
            drop(parent);

            // Help process remaining event chunks
            let mut g = ev_a_ref[0].lock().unwrap();
            loop {
                let ci = next_ev_ref.fetch_add(1, Ordering::Relaxed);
                if ci >= ev_chunks_ref.len() {
                    break;
                }
                process_ev_chunk(
                    ci,
                    ev_chunks_ref,
                    ev_maps_ref,
                    products_ref,
                    ws,
                    we,
                    unknown_cat,
                    &mut g,
                );
            }
        });

        for t in 1..nt {
            s.spawn(move || {
                let mut g = ev_a_ref[t].lock().unwrap();
                loop {
                    let ci = next_ev_ref.fetch_add(1, Ordering::Relaxed);
                    if ci >= ev_chunks_ref.len() {
                        break;
                    }
                    process_ev_chunk(
                        ci,
                        ev_chunks_ref,
                        ev_maps_ref,
                        products_ref,
                        ws,
                        we,
                        unknown_cat,
                        &mut g,
                    );
                }
            });
        }
    });
    drop(ln_out);
    log(&t0, "links + events A");

    let pmask = pmap.len() - 1;
    let person_of = |user: &[u8]| -> (u64, Str) {
        let h = hash64(key128(user));
        let h_nz = h | 1;
        let mut i = (h_nz as usize) & pmask;
        loop {
            let (k, raw) = unsafe { *pmap.get_unchecked(i) };
            if k == h_nz {
                let ph = mum(raw ^ 0x9E37_79B9_7F4A_7C15, 0xBF58_476D_1CE4_E5B9);
                return (ph, Str(raw));
            }
            if k == 0 {
                return (h, Str::of(user));
            }
            i = (i + 1) & pmask;
        }
    };

    // ---- clicks ----
    let cl_chunks = chunks(&cl_maps, CH);
    let total_cl_bytes: usize = cl_maps.iter().map(|m| m.len()).sum();
    let est_cl_per_part = (total_cl_bytes / 55 / nt.max(1) / NP).max(32);
    let cl_c: Vec<Mutex<Vec<Vec<ClC>>>> = (0..nt)
        .map(|_| Mutex::new((0..NP).map(|_| Vec::with_capacity(est_cl_per_part)).collect()))
        .collect();
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
            g[(pkey >> 56) as usize].push(ClC { pkey, ts: t, cid: Str::of(f[0]), camp: camp as u16 });
        });
    });
    log(&t0, "clicks");

    // ---- events phase B: dedup with 8-byte-per-slot L2-resident table reused per thread ----
    let ev_a: Vec<Vec<Vec<EvA>>> = ev_a.into_iter().map(|m| m.into_inner().unwrap()).collect();
    let ev_c: Vec<Mutex<Vec<Vec<EvC>>>> = (0..nt)
        .map(|_| Mutex::new((0..NP).map(|_| Vec::with_capacity(est_ev_per_part)).collect()))
        .collect();
    let next_p = AtomicUsize::new(0);
    std::thread::scope(|s| {
        let ev_a_ref = &ev_a;
        let ev_c_ref = &ev_c;
        let next_p_ref = &next_p;
        let person_of_ref = &person_of;
        for t in 0..nt {
            s.spawn(move || {
                let mut tab: Vec<u64> = Vec::new();
                let mut best: Vec<EvA> = Vec::new();
                let mut g = ev_c_ref[t].lock().unwrap();
                loop {
                    let p = next_p_ref.fetch_add(1, Ordering::Relaxed);
                    if p >= NP {
                        break;
                    }
                    let n: usize = ev_a_ref.iter().map(|v| v[p].len()).sum();
                    if n == 0 {
                        continue;
                    }
                    let cap = (n * 2).next_power_of_two().max(16);
                    let mask = cap - 1;
                    if tab.len() < cap {
                        tab.resize(cap, 0);
                    }
                    tab[..cap].fill(0);
                    best.clear();
                    if best.capacity() < n {
                        best.reserve(n - best.capacity());
                    }

                    for tv in ev_a_ref {
                        for r in &tv[p] {
                            let h = r.ehash;
                            let tag = (h as u32) | 1;
                            let mut i = (h >> 16) as usize & mask;
                            loop {
                                let slot = unsafe { *tab.get_unchecked(i) };
                                if slot == 0 {
                                    let idx = best.len() as u64;
                                    unsafe { *tab.get_unchecked_mut(i) = (tag as u64) | (idx << 32) };
                                    best.push(*r);
                                    break;
                                }
                                if (slot as u32) == tag {
                                    let idx = (slot >> 32) as usize;
                                    let b = unsafe { best.get_unchecked_mut(idx) };
                                    if b.ehash == h {
                                        if r.ingest() < b.ingest() {
                                            *b = *r;
                                        }
                                        break;
                                    }
                                }
                                i = (i + 1) & mask;
                            }
                        }
                    }

                    for r in &best {
                        let dt = r.dt();
                        if dt == INVALID_DT {
                            continue;
                        }
                        let (pkey, name) = person_of_ref(r.user.get());
                        let price = r.price();
                        g[(pkey >> 56) as usize].push(EvC {
                            pkey,
                            ts: ws + dt as i64,
                            name,
                            price,
                            cat: r.cat(),
                            purchase: price > 0,
                        });
                    }
                }
            });
        }
    });
    drop(ev_a);
    drop(pmap);
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
        .map(|_| Mutex::new(TOut { ud: Vec::with_capacity(65536), camp: vec![[0; 4]; (ncamp + 1) * ndays] }))
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
        cls.sort_unstable_by(|a, b| {
            a.pkey
                .cmp(&b.pkey)
                .then_with(|| a.ts.cmp(&b.ts))
                .then_with(|| a.cid.get().cmp(b.cid.get()))
        });
        cls.dedup_by(|a, b| a.pkey == b.pkey && a.ts == b.ts && a.cid.get() == b.cid.get());

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

#[inline(always)]
fn process_ev_chunk(
    ci: usize,
    ev_chunks: &[(usize, usize, usize)],
    ev_maps: &[Mmap],
    products: &KeyMap,
    ws: i64,
    we: i64,
    unknown_cat: u16,
    g: &mut [Vec<EvA>],
) {
    let (fi, s, e) = ev_chunks[ci];
    let d: &[u8] = &ev_maps[fi][s..e];
    csv::for_lines(d, |line| {
        let mut f: [&[u8]; 7] = [&[]; 7];
        if !csv::split::<7>(line, &mut f) || f[0].is_empty() {
            return;
        }
        let ehash = hash64(key128(f[0]));
        let ingest_raw = ts::parse_ts(f[3]).unwrap_or(i64::MAX);
        let ing_u = if ingest_raw == i64::MAX {
            ING_MASK
        } else {
            ((ingest_raw as i128 + (1i128 << 50)).clamp(0, (ING_MASK - 1) as i128)) as u64
        };
        let mut dt = INVALID_DT;
        let mut price = 0u32;
        let mut cat = 0u16;
        'v: {
            if f[1].is_empty() || f[5].is_empty() {
                break 'v;
            }
            let ty = f[4];
            if ty == b"purchase" {
                let Some(p) = csv::parse_price(f[6]) else { break 'v };
                let Some(t) = ts::parse_ts(f[2]) else { break 'v };
                if t < ws || t >= we || ingest_raw - t > DAY {
                    break 'v;
                }
                price = p;
                cat = match products.get(key128(f[5])) {
                    Some(c) => c as u16,
                    None => unknown_cat,
                };
                dt = (t - ws) as u64;
            } else if ty == b"view" || ty == b"cart" {
                let Some(t) = ts::parse_ts(f[2]) else { break 'v };
                if t < ws || t >= we || ingest_raw - t > DAY {
                    break 'v;
                }
                dt = (t - ws) as u64;
            }
        }
        let p = (ehash >> 56) as usize;
        g[p].push(EvA {
            ehash,
            ingest_cat: ing_u | ((cat as u64) << 52),
            user: Str::of(f[1]),
            ts_lo: dt as u32,
            price_ts_hi: price | (((dt >> 32) as u32) << 24),
        });
    });
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
