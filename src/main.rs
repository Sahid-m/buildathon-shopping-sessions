//! Shopping Sessions pipeline. See README.md for the approach and rules.md for the spec.

mod csv;
mod keys;
#[cfg(test)]
mod parse_fuzz;
mod pq;
mod ts;
mod wc;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use keys::{Key, KeyMap, hash64, key128, mum};
use memmap2::Mmap;

pub const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
const NP: usize = 256;
/// Event-id partitions for dedup candidates (more of them keep each dedup table in L2).
const NPK_BITS: u32 = 10;
const NPK: usize = 1 << NPK_BITS;

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

/// Dedup candidate for one event row (16B): the low 56 bits of the event hash (the top 8 are
/// the partition), biased ingest seconds, and the location of its payload (LOC_NONE if invalid).
#[derive(Clone, Copy)]
#[repr(C)]
struct Cand {
    a: u64, // low 56b = event hash low bits, high 8b = ingest bits 24..32
    b: u64, // high 24b = ingest bits 0..24, low 40b = location
}

const K56: u64 = (1 << 56) - 1;
const LOC_NONE: u64 = (1 << 40) - 1;

impl Cand {
    #[inline(always)]
    fn new(eh: u64, ing: u32, loc: u64) -> Cand {
        Cand { a: (eh & K56) | (((ing >> 24) as u64) << 56), b: (((ing & 0xFF_FFFF) as u64) << 40) | loc }
    }
    #[inline(always)]
    fn key(&self) -> u64 {
        self.a & K56
    }
    #[inline(always)]
    fn ing(&self) -> u32 {
        (((self.a >> 56) as u32) << 24) | ((self.b >> 40) as u32)
    }
    #[inline(always)]
    fn loc(&self) -> u64 {
        self.b & LOC_NONE
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
struct EvC {
    pkey: u64, // 8B
    name: Str, // 8B
    w: u64,    // 8B: low 31b = ts - ws, next 24b = price (0 unless purchase), top 9b = category
}

impl EvC {
    #[inline(always)]
    fn dt(&self) -> i64 {
        (self.w & 0x7FFF_FFFF) as i64
    }
    #[inline(always)]
    fn ts(&self, ws: i64) -> i64 {
        ws + self.dt()
    }
    #[inline(always)]
    fn price(&self) -> u32 {
        ((self.w >> 31) & 0xFF_FFFF) as u32
    }
    #[inline(always)]
    fn cat(&self) -> u16 {
        (self.w >> 55) as u16
    }
    #[inline(always)]
    fn purchase(&self) -> bool {
        self.price() > 0
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
struct ClC {
    pkey: u64, // 8B
    cid: Str,  // 8B
    w: u64,    // 8B: low 32b = ts - (window_start - 7 days), high bits = campaign
}

impl ClC {
    #[inline(always)]
    fn new(pkey: u64, cid: Str, dt: u64, camp: u16) -> ClC {
        ClC { pkey, cid, w: dt | ((camp as u64) << 32) }
    }
    #[inline(always)]
    fn dt(&self) -> i64 {
        (self.w & 0xFFFF_FFFF) as i64
    }
    #[inline(always)]
    fn camp(&self) -> u16 {
        (self.w >> 32) as u16
    }
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
    if std::env::var_os("SS_POP").is_some() {
        unsafe { memmap2::MmapOptions::new().populate().map(&f).ok() }
    } else {
        unsafe { Mmap::map(&f).ok() }
    }
}

/// Maps files on `nt` threads, keeping order (with SS_POP each map pre-faults its pages).
fn map_all(paths: &[PathBuf], nt: usize) -> Vec<Mmap> {
    let slots: Vec<Mutex<Option<Mmap>>> = paths.iter().map(|_| Mutex::new(None)).collect();
    par_for(nt, paths.len(), |_, i| *slots[i].lock().unwrap() = map_file(&paths[i]));
    slots.into_iter().filter_map(|m| m.into_inner().unwrap()).collect()
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

/// The scorer's kernel only gives huge pages to memory that asks for them (THP "madvise").
/// glibc only madvises its arena heaps, so the mmap threshold is raised to its maximum to keep
/// the ~300 KB partition buffers in those heaps (otherwise each gets its own 4 KB-page mapping).
/// glibc's allocator asks when this tunable is set, which cuts page faults while the big
/// buffers are first touched and page freeing at exit. Re-exec once with it set.
const TUNABLES: &str = "glibc.malloc.hugetlb=1:glibc.malloc.mmap_threshold=33554432";

fn ensure_tunables() {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt;
        if std::env::var("GLIBC_TUNABLES").map(|v| v.contains("hugetlb")).unwrap_or(false) {
            return;
        }
        if let Ok(exe) = std::env::current_exe() {
            let mut args = std::env::args_os();
            args.next();
            // exec only returns on failure; then just carry on without the tunable.
            let _ = std::process::Command::new(exe).args(args).env("GLIBC_TUNABLES", TUNABLES).exec();
        }
    }
}

fn main() {
    ensure_tunables();
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
    // EvC packs ts - window_start into 31 bits and the category into 9 bits.
    assert!(we - ws < (1i64 << 31), "window longer than 24 days is not supported");
    assert!(cat_names.len() <= 512, "more than 512 categories is not supported");
    log(&t0, "small files");

    // map all big files
    let ev_maps: Vec<Mmap> = map_all(&list_csv(&data.join("events")), nt);
    let cl_maps: Vec<Mmap> = map_all(&list_csv(&data.join("clicks")), nt);
    let ln_maps: Vec<Mmap> = map_all(&list_csv(&data.join("links")), nt);
    const CH: usize = 4 << 20;
    let ln_chunks = chunks(&ln_maps, CH);

    // ---- links: parallel parse + pre-hash ----
    let ln_out: Vec<Mutex<Vec<(Key, Str, Key, Str)>>> = (0..ln_chunks.len()).map(|_| Mutex::new(vec![])).collect();
    par_for(nt, ln_chunks.len(), |_, ci| {
        let (fi, s, e) = ln_chunks[ci];
        let d: &[u8] = &ln_maps[fi][s..e];
        let mut v = Vec::with_capacity((e - s) / 22);
        csv::for_rows::<2, _>(d, |f| {
            if !f[0].is_empty() && !f[1].is_empty() {
                v.push((key128(f[0]), Str::of(f[0]), key128(f[1]), Str::of(f[1])));
            }
        });
        *ln_out[ci].lock().unwrap() = v;
    });

    // ---- events A (one pass) + clicks; union-find on thread 0 first ----
    // Each event row gives a payload (EvC) and a dedup candidate (in event partitions) holding
    // the payload's location. Once the person table is ready, payloads are keyed by person
    // (resolved set); rows parsed before that are keyed by raw user (unresolved set) and
    // resolved after dedup.
    let ev_chunks = chunks(&ev_maps, CH);
    let cl_chunks = chunks(&cl_maps, CH);
    let total_ev_bytes: usize = ev_maps.iter().map(|m| m.len()).sum();
    let est_ev_per_part = (total_ev_bytes / 85 / nt.max(1) / NP).max(64);
    let total_cl_bytes: usize = cl_maps.iter().map(|m| m.len()).sum();
    let est_cl_per_part = (total_cl_bytes / 55 / nt.max(1) / NP).max(32);
    let total_edges: usize = ln_out.iter().map(|m| m.lock().unwrap().len()).sum();
    let pmap_cap = ((total_edges * 2 + 64) * 2).next_power_of_two().max(1024);
    let pm_cell: OnceLock<Vec<(u64, u64)>> = OnceLock::new();

    // Payload locations name one of 2*nt worker lists: t (resolved) or nt + t (unresolved).
    let thb = ((2 * nt).next_power_of_two().trailing_zeros()) as u32;
    let mk = |n: usize, cap: usize| -> Vec<Mutex<Vec<Vec<EvC>>>> {
        (0..n).map(|_| Mutex::new((0..NP).map(|_| Vec::with_capacity(cap)).collect())).collect()
    };
    let mkc = |n: usize, cap: usize| -> Vec<Mutex<Vec<Vec<ClC>>>> {
        (0..n).map(|_| Mutex::new((0..NP).map(|_| Vec::with_capacity(cap)).collect())).collect()
    };
    let cl_c = mkc(nt, est_cl_per_part);
    let cl_u = mkc(nt, est_cl_per_part / 2);
    let ev_c = mk(nt, est_ev_per_part);
    let ev_u = mk(nt, est_ev_per_part / 2);
    let ev_k: Vec<Mutex<Vec<Vec<Cand>>>> = (0..nt)
        .map(|_| Mutex::new((0..NPK).map(|_| Vec::with_capacity(est_ev_per_part * NP / NPK + 16)).collect()))
        .collect();
    let next_ev = AtomicUsize::new(0);
    let next_cl = AtomicUsize::new(0);
    std::thread::scope(|s| {
        for t in 0..nt {
            let (cl_c, cl_u, ev_c, ev_u, ev_k, next_ev, next_cl) = (&cl_c, &cl_u, &ev_c, &ev_u, &ev_k, &next_ev, &next_cl);
            let (ev_chunks, cl_chunks, ev_maps, cl_maps, products, campaigns) =
                (&ev_chunks, &cl_chunks, &ev_maps, &cl_maps, &products, &campaigns);
            let (ln_out, pm_cell) = (&ln_out, &pm_cell);
            s.spawn(move || {
                if t == 0 {
                    let _ = pm_cell.set(build_pmap(ln_out, total_edges, pmap_cap));
                    log(&t0, "  pmap built");
                }
                let mut ge = ev_c[t].lock().unwrap();
                let mut gu = ev_u[t].lock().unwrap();
                let mut gk = ev_k[t].lock().unwrap();
                let mut ste = wc::Stager::<EvC, 8>::new(NP);
                let mut stu = wc::Stager::<EvC, 8>::new(NP);
                let mut stk = wc::Stager::<Cand, 8>::new(NPK);
                let mut batch = [Row::default(); BATCH];
                loop {
                    let ci = next_ev.fetch_add(1, Ordering::Relaxed);
                    if ci >= ev_chunks.len() {
                        break;
                    }
                    let (fi, s, e) = ev_chunks[ci];
                    let d: &[u8] = &ev_maps[fi][s..e];
                    if let Some(pm) = pm_cell.get() {
                        let pl = PLook::new(pm);
                        let mut n = 0usize;
                        let mut flush = |batch: &[Row], ge: &mut [Vec<EvC>], gk: &mut [Vec<Cand>]| {
                            for r in batch {
                                let loc = if r.valid {
                                    let (pk, name) = pl.person(r.uh, r.user);
                                    let p = (pk >> 56) as usize;
                                    let idx = (ge[p].len() + ste.pending(p)) as u64;
                                    ste.push(ge, p, EvC { pkey: pk, name, w: r.w });
                                    (idx << (8 + thb)) | ((t as u64) << 8) | p as u64
                                } else {
                                    LOC_NONE
                                };
                                stk.push(gk, (r.eh >> (64 - NPK_BITS)) as usize, Cand::new(r.eh, r.ing, loc));
                            }
                        };
                        csv::for_rows::<7, _>(d, |f| {
                            if f[0].is_empty() {
                                return;
                            }
                            let r = parse_event(f, products, ws, we, unknown_cat);
                            if r.valid {
                                pl.prefetch(r.uh);
                            }
                            batch[n] = r;
                            n += 1;
                            if n == BATCH {
                                flush(&batch, &mut ge, &mut gk);
                                n = 0;
                            }
                        });
                        flush(&batch[..n], &mut ge, &mut gk);
                    } else {
                        let wi = (nt + t) as u64;
                        csv::for_rows::<7, _>(d, |f| {
                            if f[0].is_empty() {
                                return;
                            }
                            let r = parse_event(f, products, ws, we, unknown_cat);
                            let loc = if r.valid {
                                let p = (r.uh >> 56) as usize;
                                let idx = (gu[p].len() + stu.pending(p)) as u64;
                                stu.push(&mut gu, p, EvC { pkey: r.uh, name: r.user, w: r.w });
                                (idx << (8 + thb)) | (wi << 8) | p as u64
                            } else {
                                LOC_NONE
                            };
                            stk.push(&mut gk, (r.eh >> (64 - NPK_BITS)) as usize, Cand::new(r.eh, r.ing, loc));
                        });
                    }
                }
                ste.finish(&mut ge);
                stu.finish(&mut gu);
                stk.finish(&mut gk);
                drop((ge, gu, gk));
                let mut g = cl_c[t].lock().unwrap();
                let mut gcu = cl_u[t].lock().unwrap();
                let mut cb = [(0u64, ClC { pkey: 0, cid: Str(0), w: 0 }); BATCH];
                loop {
                    let ci = next_cl.fetch_add(1, Ordering::Relaxed);
                    if ci >= cl_chunks.len() {
                        break;
                    }
                    let (fi, s, e) = cl_chunks[ci];
                    let d: &[u8] = &cl_maps[fi][s..e];
                    let pl = pm_cell.get().map(|pm| PLook::new(pm));
                    let mut n = 0usize;
                    let flush = |b: &[(u64, ClC)], g: &mut [Vec<ClC>]| {
                        let pl = pl.as_ref().unwrap();
                        for &(uh, c) in b {
                            let (pk, _) = pl.person(uh, Str(0));
                            g[(pk >> 56) as usize].push(ClC { pkey: pk, ..c });
                        }
                    };
                    csv::for_rows::<4, _>(d, |f| {
                        if f[0].is_empty() || f[1].is_empty() {
                            return;
                        }
                        let Some(t) = ts::parse_ts(f[3]) else { return };
                        if t < cws || t >= we {
                            return;
                        }
                        let Some(camp) = campaigns.get(key128(f[2])) else { return };
                        let uh = hash64(key128(f[1]));
                        let c = ClC::new(uh, Str::of(f[0]), (t - cws) as u64, camp as u16);
                        match &pl {
                            Some(pl) => {
                                pl.prefetch(uh);
                                cb[n] = (uh, c);
                                n += 1;
                                if n == BATCH {
                                    flush(&cb, &mut g);
                                    n = 0;
                                }
                            }
                            None => gcu[(uh >> 56) as usize].push(c),
                        }
                    });
                    if n > 0 {
                        flush(&cb[..n], &mut g);
                    }
                }
            });
        }
    });
    drop(ln_out);
    let pmap = pm_cell.into_inner().unwrap();
    log(&t0, "links + events A + clicks");

    // ---- events B: dedup candidates, mark every losing copy's payload dead ----
    let bases: Vec<usize> = ev_c
        .iter()
        .chain(ev_u.iter())
        .flat_map(|m| m.lock().unwrap().iter_mut().map(|v| v.as_mut_ptr() as usize).collect::<Vec<_>>())
        .collect();
    let tmask = (1u64 << thb) - 1;
    let kill = |loc: u64| {
        if loc != LOC_NONE {
            let p = (loc & 255) as usize;
            let t = ((loc >> 8) & tmask) as usize;
            let idx = (loc >> (8 + thb)) as usize;
            // Each payload has one candidate, and each candidate is handled by one thread.
            unsafe { std::ptr::write(&mut (*(bases[t * NP + p] as *mut EvC).add(idx)).name.0, 0) };
        }
    };
    let ev_k = split_parts(ev_k);
    let dd_tabs: Vec<Mutex<(Vec<u64>, Vec<Cand>)>> = (0..nt).map(|_| Mutex::new((vec![], vec![]))).collect();
    par_for(nt, NPK, |t, p| {
        let mut g = dd_tabs[t].lock().unwrap();
        let (tab, best) = &mut *g;
        let parts = take_part(&ev_k, p);
        let n: usize = parts.iter().map(|v| v.len()).sum();
        if n == 0 {
            return;
        }
        let cap = (n * 2).next_power_of_two().max(16);
        let mask = cap - 1;
        if tab.len() < cap {
            tab.resize(cap, 0);
        }
        tab[..cap].fill(0);
        best.clear();
        best.reserve(n);
        for tv in &parts {
            for r in tv {
                let h = r.key();
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
                        if b.key() == h {
                            if r.ing() < b.ing() {
                                kill(b.loc());
                                *b = *r;
                            } else {
                                kill(r.loc());
                            }
                            break;
                        }
                    }
                    i = (i + 1) & mask;
                }
            }
        }
    });
    drop(ev_k);
    drop(dd_tabs);
    log(&t0, "events B");

    // ---- person resolution: move linked users' records to their person's partition ----
    // pmap is indexed by the top bits of the user hash, so user partition p only probes its
    // own 1/NP slice of the table, which stays in L2.
    let pl = PLook::new(&pmap);
    let linked = |h: u64| pl.linked(h);
    let pkey_of = |raw: u64| mum(raw ^ 0x9E37_79B9_7F4A_7C15, 0xBF58_476D_1CE4_E5B9);
    let mut ev_c = split_parts(ev_c);
    let mut cl_c = split_parts(cl_c);
    let ev_u = split_parts(ev_u);
    let cl_u = split_parts(cl_u);
    // Moved buffers are sized for the worst case (every record moves) so they never regrow;
    // untouched capacity is never faulted in.
    let n_ev: usize = ev_u.iter().map(|w| w.iter().map(|v| v.lock().unwrap().len()).sum::<usize>()).sum();
    let n_cl: usize = cl_u.iter().map(|w| w.iter().map(|v| v.lock().unwrap().len()).sum::<usize>()).sum();
    let cap_e = n_ev / (nt * NP) + 64;
    let cap_c = n_cl / (nt * NP) + 64;
    let mv_e: Vec<Mutex<Vec<Vec<EvC>>>> =
        (0..nt).map(|_| Mutex::new((0..NP).map(|_| Vec::with_capacity(cap_e)).collect())).collect();
    let mv_c: Vec<Mutex<Vec<Vec<ClC>>>> =
        (0..nt).map(|_| Mutex::new((0..NP).map(|_| Vec::with_capacity(cap_c)).collect())).collect();
    let next_p = AtomicUsize::new(0);
    std::thread::scope(|s| {
        for t in 0..nt {
            let (mv_e, mv_c, ev_c, cl_c, next_p, linked, pkey_of) =
                (&mv_e, &mv_c, &ev_u, &cl_u, &next_p, &linked, &pkey_of);
            s.spawn(move || {
                let mut me = mv_e[t].lock().unwrap();
                let mut mc = mv_c[t].lock().unwrap();
                let mut ste = wc::Stager::<EvC, 8>::new(NP);
                let mut stc = wc::Stager::<ClC, 8>::new(NP);
                loop {
                    let p = next_p.fetch_add(1, Ordering::Relaxed);
                    if p >= NP {
                        break;
                    }
                    // Keeps unlinked live records in place (compacted), moves linked ones.
                    for w in ev_c {
                        let mut v = w[p].lock().unwrap();
                        let mut k = 0;
                        for i in 0..v.len() {
                            let e = unsafe { *v.get_unchecked(i) };
                            if e.name.0 == 0 {
                                continue;
                            }
                            if let Some(raw) = linked(e.pkey) {
                                let pk = pkey_of(raw);
                                ste.push(&mut me, (pk >> 56) as usize, EvC { pkey: pk, name: Str(raw), w: e.w });
                            } else {
                                unsafe { *v.get_unchecked_mut(k) = e };
                                k += 1;
                            }
                        }
                        v.truncate(k);
                    }
                    for w in cl_c {
                        let mut v = w[p].lock().unwrap();
                        let mut k = 0;
                        for i in 0..v.len() {
                            let c = unsafe { *v.get_unchecked(i) };
                            if let Some(raw) = linked(c.pkey) {
                                let pk = pkey_of(raw);
                                stc.push(&mut mc, (pk >> 56) as usize, ClC { pkey: pk, cid: c.cid, w: c.w });
                            } else {
                                unsafe { *v.get_unchecked_mut(k) = c };
                                k += 1;
                            }
                        }
                        v.truncate(k);
                    }
                }
                ste.finish(&mut me);
                stc.finish(&mut mc);
            });
        }
    });
    if std::env::var_os("SS_STATS").is_some() {
        let a: usize = mv_e.iter().map(|m| m.lock().unwrap().iter().map(|v| v.len()).sum::<usize>()).sum();
        let b: usize = ev_c.iter().map(|w| w.iter().map(|v| v.lock().unwrap().len()).sum::<usize>()).sum();
        let c: usize = mv_c.iter().map(|m| m.lock().unwrap().iter().map(|v| v.len()).sum::<usize>()).sum();
        eprintln!("moved ev {a} of {b}, moved clicks {c}");
    }
    ev_c.extend(ev_u);
    cl_c.extend(cl_u);
    ev_c.extend(split_parts(mv_e));
    cl_c.extend(split_parts(mv_c));
    log(&t0, "resolve");

    drop(pmap);

    // ---- phase C: sessions + attribution ----
    let ncat = cat_names.len();
    // Bits for a timestamp offset from the click window start (covers events too).
    let tb = 64 - ((we - cws - 1).max(1) as u64).leading_zeros();
    struct TOut {
        ud: Vec<UdRow>,
        camp: Vec<[i64; 4]>,
    }
    let touts: Vec<Mutex<TOut>> = (0..nt)
        .map(|_| Mutex::new(TOut { ud: Vec::with_capacity(65536), camp: vec![[0; 4]; (ncamp + 1) * ndays] }))
        .collect();
    let scratch: Vec<Mutex<(Scratch<EvC>, Scratch<ClC>)>> =
        (0..nt).map(|_| Mutex::new((Scratch::new(), Scratch::new()))).collect();
    let order = lpt_order(&ev_c, &cl_c);
    par_for(nt, NP, |t, pi| {
        let p = order[pi];
        let mut scr = scratch[t].lock().unwrap();
        let (esc, csc) = &mut *scr;
        bucket_sort(&ev_c, p, |e| e.name.0 != 0, |e| e.pkey, |e| (e.ts(ws) - cws) as u64, tb, |a, b| {
            (a.pkey, a.dt()).cmp(&(b.pkey, b.dt()))
        }, esc);
        let evs = &esc.out;
        bucket_sort(&cl_c, p, |_| true, |c| c.pkey, |c| c.dt() as u64, tb, |a, b| {
            a.pkey
                .cmp(&b.pkey)
                .then_with(|| a.dt().cmp(&b.dt()))
                .then_with(|| a.cid.get().cmp(b.cid.get()))
        }, csc);
        let cls = &mut csc.out;
        // The packed sort keeps equal (pkey, ts) clicks in input order; order those runs by
        // click_id so the dedup below sees copies side by side.
        let mut a = 0;
        while a < cls.len() {
            let mut z = a + 1;
            while z < cls.len() && cls[z].pkey == cls[a].pkey && cls[z].dt() == cls[a].dt() {
                z += 1;
            }
            if z - a > 1 {
                cls[a..z].sort_unstable_by(|x, y| x.cid.get().cmp(y.cid.get()));
            }
            a = z;
        }
        cls.dedup_by(|a, b| a.pkey == b.pkey && a.dt() == b.dt() && a.cid.get() == b.cid.get());

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
                let start = evs[k].ts(ws);
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
                while m < j && evs[m].ts(ws) - last <= GAP {
                    let e = &evs[m];
                    last = e.ts(ws);
                    if e.purchase() {
                        let (price, cat) = (e.price() as i64, e.cat());
                        purchased = true;
                        rev += price;
                        if catsum[cat as usize] == 0 {
                            cat_touched.push(cat);
                        }
                        catsum[cat as usize] += price;
                    }
                    m += 1;
                }
                let nev = (m - k) as i64;
                let row = cur.get_or_insert(UdRow { name, day, sessions: 0, events: 0, revenue: 0, top: None });
                row.sessions += 1;
                row.events += nev;
                row.revenue += rev;
                // attribution
                while hi < pc.len() && (cws + pc[hi].dt()) <= start {
                    let c = pc[hi].camp() as usize;
                    if cnt[c] == 0 {
                        pos[c] = active.len() as u32;
                        active.push(c as u16);
                    }
                    cnt[c] += 1;
                    hi += 1;
                }
                while lo < hi && (cws + pc[lo].dt()) < start - WEEK {
                    let c = pc[lo].camp() as usize;
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
                    let mt = cws + pc[hi - 1].dt();
                    let mut b = hi - 1;
                    let mut q = hi - 1;
                    while q > lo && (cws + pc[q - 1].dt()) == mt {
                        q -= 1;
                        if pc[q].cid.get() < pc[b].cid.get() {
                            b = q;
                        }
                    }
                    for &c in &active {
                        camp[c as usize * ndays + di][3] += 1;
                    }
                    pc[b].camp() as usize
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
                cnt[pc[q].camp() as usize] = 0;
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
    // Skip freeing the large buffers; the OS reclaims them at exit.
    std::process::exit(0);
}

/// Partition indices, largest first (sizes summed over threads), so whale partitions start
/// early and do not leave one thread running alone at the end.
fn lpt_order<T, U>(a: &Parts<T>, b: &Parts<U>) -> Vec<usize> {
    let size = |p: usize| -> usize {
        a.iter().map(|w| w[p].lock().unwrap().len()).sum::<usize>()
            + b.iter().map(|w| w[p].lock().unwrap().len()).sum::<usize>()
    };
    let mut v: Vec<(usize, usize)> = (0..NP).map(|p| (size(p), p)).collect();
    v.sort_unstable_by(|x, y| y.cmp(x));
    v.into_iter().map(|(_, p)| p).collect()
}

/// Per-thread, per-partition buffers; each partition is taken (and freed) by its one consumer.
type Parts<T> = Vec<Vec<Mutex<Vec<T>>>>;

fn split_parts<T>(v: Vec<Mutex<Vec<Vec<T>>>>) -> Parts<T> {
    v.into_iter().map(|m| m.into_inner().unwrap().into_iter().map(Mutex::new).collect()).collect()
}

/// Takes partition `p` out of every thread's buffers, so it is freed once consumed.
fn take_part<T>(parts: &Parts<T>, p: usize) -> Vec<Vec<T>> {
    parts.iter().map(|w| std::mem::take(&mut *w[p].lock().unwrap())).collect()
}

/// Gathers partition `p` from every thread's output and sorts it by (pkey, ts). The partition
/// already shares the top 8 key bits, so a counting pass on the next 12 bits leaves buckets of
/// one or a few persons. Each bucket is then sorted as packed u64 keys
/// `rank(pkey) | ts offset | index` (u64 sorts use branchless networks), and the records are
/// gathered in key order. Records with equal (pkey, ts) come out in input order. A bucket whose
/// keys do not fit 64 bits falls back to a comparison sort with `cmp`.
fn bucket_sort<T: Copy>(
    parts: &Parts<T>,
    p: usize,
    alive: impl Fn(&T) -> bool,
    key: impl Fn(&T) -> u64,
    tsoff: impl Fn(&T) -> u64,
    tb: u32,
    cmp: impl Fn(&T, &T) -> std::cmp::Ordering + Copy,
    sc: &mut Scratch<T>,
) {
    let bucket = |v: &T| ((key(v) >> 44) & 4095) as usize;
    let workers = take_part(parts, p);
    let mut ends = vec![0usize; 4096];
    for w in &workers {
        for v in w {
            ends[bucket(v)] += alive(v) as usize;
        }
    }
    let mut n = 0;
    for e in ends.iter_mut() {
        let c = *e;
        *e = n;
        n += c;
    }
    let Scratch { scat, out, keys, pks } = sc;
    scat.clear();
    scat.reserve(n);
    let dst = scat.as_mut_ptr();
    for w in &workers {
        for v in w {
            if !alive(v) {
                continue;
            }
            let o = &mut ends[bucket(v)];
            // Prefix sums give each bucket a disjoint range with one slot per input.
            unsafe { dst.add(*o).write(*v) };
            *o += 1;
        }
    }
    unsafe { scat.set_len(n) };
    drop(workers);
    out.clear();
    out.reserve(n);
    let mut start = 0;
    for &end in ends.iter() {
        let b = &scat[start..end];
        start = end;
        if b.len() <= SMALL {
            // Stable insertion sort by (key, tsoff): ties keep input order, as the packed
            // keys do.
            let o = out.len();
            out.extend_from_slice(b);
            let t = &mut out[o..];
            for i in 1..t.len() {
                let v = t[i];
                let kv = (key(&v), tsoff(&v));
                let mut j = i;
                while j > 0 && (key(&t[j - 1]), tsoff(&t[j - 1])) > kv {
                    t[j] = t[j - 1];
                    j -= 1;
                }
                t[j] = v;
            }
            continue;
        }
        if !sort_bucket(b, &key, &tsoff, tb, keys, pks, out) {
            let o = out.len();
            out.extend_from_slice(b);
            out[o..].sort_unstable_by(cmp);
        }
    }
}

/// Per-thread buffers for `bucket_sort`, reused across partitions to avoid fresh page faults.
struct Scratch<T> {
    scat: Vec<T>,
    out: Vec<T>,
    keys: Vec<u64>,
    pks: Vec<u64>,
}

impl<T> Scratch<T> {
    fn new() -> Self {
        Scratch { scat: vec![], out: vec![], keys: vec![], pks: vec![] }
    }
}

/// Buckets up to this size are insertion-sorted in place instead of via packed keys.
const SMALL: usize = 8;

/// Appends `b` sorted by (key, tsoff, input index) to `out`; false (nothing appended) if the
/// packed keys would not fit in 64 bits.
#[inline(always)]
fn sort_bucket<T: Copy>(
    b: &[T],
    key: &impl Fn(&T) -> u64,
    tsoff: &impl Fn(&T) -> u64,
    tb: u32,
    keys: &mut Vec<u64>,
    pks: &mut Vec<u64>,
    out: &mut Vec<T>,
) -> bool {
    let n = b.len();
    let ib = 64 - ((n - 1) as u64).leading_zeros();
    if tb + ib > 64 {
        return false;
    }
    let rb = 64 - tb - ib;
    let k0 = key(&b[0]);
    let single = b.iter().all(|v| key(v) == k0);
    keys.clear();
    if single {
        keys.extend(b.iter().enumerate().map(|(i, v)| (tsoff(v) << ib) | i as u64));
    } else {
        if rb == 0 {
            return false;
        }
        pks.clear();
        pks.extend(b.iter().map(key));
        pks.sort_unstable();
        pks.dedup();
        if rb < 64 && pks.len() as u64 > (1u64 << rb) {
            return false;
        }
        let sh = tb + ib;
        keys.extend(b.iter().enumerate().map(|(i, v)| {
            let r = match pks.binary_search(&key(v)) {
                Ok(r) => r as u64,
                Err(_) => unreachable!(),
            };
            (r << sh) | (tsoff(v) << ib) | i as u64
        }));
    }
    keys.sort_unstable();
    let mask = if ib == 64 { u64::MAX } else { (1u64 << ib) - 1 };
    out.extend(keys.iter().map(|&k| unsafe { *b.get_unchecked((k & mask) as usize) }));
    true
}

const BATCH: usize = 16;

/// Lookups in the person table: (hash | 1, root id string) slots, indexed by the top bits.
struct PLook<'a> {
    pm: &'a [(u64, u64)],
    mask: usize,
    shift: u32,
}

impl<'a> PLook<'a> {
    fn new(pm: &'a [(u64, u64)]) -> Self {
        PLook { pm, mask: pm.len() - 1, shift: 64 - pm.len().trailing_zeros() }
    }
    #[inline(always)]
    fn linked(&self, h: u64) -> Option<u64> {
        let h_nz = h | 1;
        let mut i = (h_nz >> self.shift) as usize;
        loop {
            let (k, raw) = unsafe { *self.pm.get_unchecked(i) };
            if k == h_nz {
                return Some(raw);
            }
            if k == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
    /// (person key, person name) of a user: its link root, or itself if unlinked.
    #[inline(always)]
    fn person(&self, h: u64, user: Str) -> (u64, Str) {
        match self.linked(h) {
            Some(raw) => (mum(raw ^ 0x9E37_79B9_7F4A_7C15, 0xBF58_476D_1CE4_E5B9), Str(raw)),
            None => (h, user),
        }
    }
    #[inline(always)]
    fn prefetch(&self, h: u64) {
        csv::prefetch(unsafe { self.pm.as_ptr().add(((h | 1) >> self.shift) as usize) } as *const u8);
    }
}

/// A parsed event row.
#[derive(Clone, Copy, Default)]
struct Row {
    eh: u64,
    uh: u64,
    user: Str,
    w: u64,
    ing: u32,
    valid: bool,
}

/// Parses one event row: event hash, ingest seconds (biased), and if valid the user hash and
/// the packed EvC word (dt, price, category).
#[inline(always)]
fn parse_event(f: &[&[u8]; 7], products: &KeyMap, ws: i64, we: i64, unknown_cat: u16) -> Row {
    let eh = hash64(key128(f[0]));
    let ingest_raw = ts::parse_ingest(f[3]);
    let ing = match ingest_raw {
        Some(v) => (v.div_euclid(1000) - ws.div_euclid(1000) + (1i64 << 31)).clamp(0, u32::MAX as i64 - 1) as u32,
        None => u32::MAX,
    };
    let mut r = Row { eh, ing, ..Row::default() };
    if f[1].is_empty() || f[5].is_empty() {
        return r;
    }
    let ty = f[4];
    let is_p = ty == b"purchase";
    if !(is_p || ty == b"view" || ty == b"cart") {
        return r;
    }
    let mut price = 0u32;
    if is_p {
        let Some(v) = csv::parse_price(f[6]) else { return r };
        price = v;
    }
    let Some(t) = ts::parse_ts(f[2]) else { return r };
    if t < ws || t >= we || ingest_raw.unwrap_or(i64::MAX) - t > DAY {
        return r;
    }
    let mut cat = 0u16;
    if is_p {
        cat = match products.get(key128(f[5])) {
            Some(c) => c as u16,
            None => unknown_cat,
        };
    }
    r.w = ((t - ws) as u64) | ((price as u64) << 31) | ((cat as u64) << 55);
    r.uh = hash64(key128(f[1]));
    r.user = Str::of(f[1]);
    r.valid = true;
    r
}

/// Union-find over all link edges, packed into a 16B/slot (hash_nz, root_str) table.
fn build_pmap(ln_out: &[Mutex<Vec<(Key, Str, Key, Str)>>], total_edges: usize, cap: usize) -> Vec<(u64, u64)> {
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
    let tuf = Instant::now();
    const LPF: usize = 12;
    for m in ln_out {
        let v = m.lock().unwrap();
        for (ei, &(ka, sa, kb, sb)) in v.iter().enumerate() {
            if let Some(nx) = v.get(ei + LPF) {
                let em = idmap.mask;
                let base = idmap.entries.as_ptr();
                csv::prefetch(unsafe { base.add(hash64(nx.0) as usize & em) } as *const u8);
                csv::prefetch(unsafe { base.add(hash64(nx.2) as usize & em) } as *const u8);
            }
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
    let tloop = tuf.elapsed();
    // Root string of every id, then the table fill with the home slots prefetched ahead.
    let n = ids.len();
    let mut root: Vec<u64> = Vec::with_capacity(n);
    for i in 0..n {
        let r = find(&mut parent, i as u32) as usize;
        root.push(unsafe { ids.get_unchecked(r).0 });
    }
    let mask = cap - 1;
    let shift = 64 - cap.trailing_zeros();
    let mut pmap = vec![(0u64, 0u64); cap];
    let mut batch = [(0u64, 0u32); 16];
    let mut nb = 0usize;
    let flush = |pmap: &mut Vec<(u64, u64)>, b: &[(u64, u32)]| {
        for &(h_nz, id) in b {
            let raw = unsafe { *root.get_unchecked(id as usize) };
            let mut i = (h_nz >> shift) as usize;
            while unsafe { pmap.get_unchecked(i).0 } != 0 {
                i = (i + 1) & mask;
            }
            unsafe { *pmap.get_unchecked_mut(i) = (h_nz, raw) };
        }
    };
    for e in &idmap.entries {
        if !e.k.is_zero() {
            let h_nz = hash64(e.k) | 1;
            csv::prefetch(unsafe { pmap.as_ptr().add((h_nz >> shift) as usize) } as *const u8);
            csv::prefetch(unsafe { root.as_ptr().add(e.v as usize) } as *const u8);
            batch[nb] = (h_nz, e.v as u32);
            nb += 1;
            if nb == 16 {
                flush(&mut pmap, &batch);
                nb = 0;
            }
        }
    }
    flush(&mut pmap, &batch[..nb]);
    if std::env::var_os("SS_QUIET").is_none() {
        eprintln!("  uf loop {:?}, fill {:?}, ids={}", tloop, tuf.elapsed() - tloop, n);
    }
    pmap
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
