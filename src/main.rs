//! Shopping Sessions pipeline. See README.md for the approach and rules.md for the spec.

mod csv;
mod keys;
#[cfg(test)]
mod parse_fuzz;
mod pq;
mod slab;
mod ts;
mod wc;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use keys::{Key, KeyMap, hash64, key128, mum};
use memmap2::Mmap;
use slab::{List, Pool};

pub const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;
/// log2 of the event-id partition count (events A scatter, events B dedup). Measured on x86
/// runners, more parts here cost events A more than they save in events B, at 20M and 60M.
const NPA_BITS: u32 = 8;
/// Person partition count bounds (log2) for phase C. The count scales with input so a
/// partition stays near EV_PER_PART events and its 4096 buckets hold under one person each.
const NPC_BITS_MIN: u32 = 8;
const NPC_BITS_MAX: u32 = 8; // 1024 person partitions scored slower on the 1 MB-L2 scorer
const EV_PER_PART: usize = 20_000;
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
    uh: u64,           // 8B: hash64(key128(user_id)), computed while the line is hot
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

/// A valid click before person resolution, bucketed by user hash.
#[derive(Clone, Copy)]
#[repr(C)]
struct ClB {
    uh: u64,
    ts: i64,
    cid: Str,
    camp: u16,
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
    fn new(pkey: u64, name: Str, dt: u64, price: u32, cat: u16) -> EvC {
        EvC { pkey, name, w: dt | ((price as u64) << 31) | ((cat as u64) << 55) }
    }
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

struct UdRow {
    name: Str,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
}

#[derive(Clone, Copy, Default)]
#[repr(C, align(16))]
struct CampCell {
    rev: u64,
    ses: u32,
    pur: u32,
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

/// Reads a partition-bits override (for benchmarking) from env var `var`.
fn np_bits_env(var: &str) -> Option<u32> {
    std::env::var(var).ok().and_then(|v| v.parse::<u32>().ok()).map(|b| b.clamp(4, 12))
}

/// log2 of the person partition count: about one partition per EV_PER_PART events
/// (estimated at ~85 bytes per event line), rounded to the nearest power of two and clamped.
fn npc_bits_for(total_ev_bytes: usize) -> u32 {
    let target = (total_ev_bytes / 85 / EV_PER_PART).max(1) as u64;
    let lo = 63 - target.leading_zeros();
    // Round up when target >= 1.5 * 2^lo.
    let b = if target * 2 >= 3u64 << lo { lo + 1 } else { lo };
    b.clamp(NPC_BITS_MIN, NPC_BITS_MAX)
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

/// Like `par_for`, on exactly `nt` threads; each thread runs `fin(t)` once it finds no more
/// work, while the others may still be running.
fn par_for_fin<F: Fn(usize, usize) + Sync, G: Fn(usize) + Sync>(nt: usize, n: usize, f: F, fin: G) {
    let next = AtomicUsize::new(0);
    std::thread::scope(|s| {
        for t in 0..nt {
            let (f, fin, next) = (&f, &fin, &next);
            s.spawn(move || {
                loop {
                    let i = next.fetch_add(1, Ordering::Relaxed);
                    if i >= n {
                        break;
                    }
                    f(t, i);
                }
                fin(t);
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
        if std::env::var_os("SS_NOTUNE").is_some()
            || std::env::var("GLIBC_TUNABLES").map(|v| v.contains("hugetlb")).unwrap_or(false)
        {
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

    // ---- overlap union-find with events phase A ----
    let ev_chunks = chunks(&ev_maps, CH);
    let total_ev_bytes: usize = ev_maps.iter().map(|m| m.len()).sum();
    // Two independent partitionings, each indexed by the top bits of a 64-bit hash: events A
    // scatters by event id for the events-B dedup (npa parts), and events B and clicks
    // scatter by person for phase C (npc parts).
    let npa_bits = np_bits_env("SS_NPABITS").unwrap_or(NPA_BITS);
    let npc_bits = np_bits_env("SS_NPCBITS").unwrap_or_else(|| npc_bits_for(total_ev_bytes));
    let (npa, npc) = (1usize << npa_bits, 1usize << npc_bits);
    let (psha, pshc) = (64 - npa_bits, 64 - npc_bits);
    let est_ev_per_part = (total_ev_bytes / 85 / nt.max(1) / npa).max(64);
    let est_evc_per_part = (total_ev_bytes / 85 / nt.max(1) / npc).max(64);
    let _ = (est_ev_per_part, est_evc_per_part);
    // Partition records live in pool chunks; events B output reuses events A chunks.
    let total_cl_bytes: usize = cl_maps.iter().map(|m| m.len()).sum();
    let pool = Pool::new(total_ev_bytes / 2 + total_cl_bytes / 2 + 3 * nt * (npa + npc) * slab::CHUNK + (64 << 20));
    let pool = &pool;
    let ev_a: Vec<Mutex<Vec<List<EvA>>>> =
        (0..nt).map(|_| Mutex::new((0..npa).map(|_| List::default()).collect())).collect();

    let total_edges: usize = ln_out.iter().map(|m| m.lock().unwrap().len()).sum();
    let pmap_cap = ((total_edges * 2 + 64) * 2).next_power_of_two().max(1024);
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
            let cap = pmap_cap;
            let mask = cap - 1;
            let shift = 64 - cap.trailing_zeros();
            pmap_ptr.resize(cap, (0, 0));
            for e in &idmap.entries {
                if !e.k.is_zero() {
                    let r = find(&mut parent, e.v as u32) as usize;
                    let raw = ids[r].0;
                    let h_nz = hash64(e.k) | 1;
                    let mut i = (h_nz >> shift) as usize;
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
                let mut st = wc::Stager::<EvA, 8>::new(npa);
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
                    psha,
                    &mut g,
                    &mut st,
                    pool,
                );
            }
            st.finish(&mut g, pool);
        });

        for t in 1..nt {
            s.spawn(move || {
                let mut g = ev_a_ref[t].lock().unwrap();
                let mut st = wc::Stager::<EvA, 8>::new(npa);
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
                        psha,
                        &mut g,
                        &mut st,
                        pool,
                    );
                }
                st.finish(&mut g, pool);
            });
        }
    });
    drop(ln_out);
    log(&t0, "links + events A");

    let pmask = pmap.len() - 1;
    let pshift = 64 - pmap.len().trailing_zeros();
    let person_of = |h: u64, user: Str| -> (u64, Str) {
        let h_nz = h | 1;
        let mut i = (h_nz >> pshift) as usize;
        loop {
            let (k, raw) = unsafe { *pmap.get_unchecked(i) };
            if k == h_nz {
                let ph = mum(raw ^ 0x9E37_79B9_7F4A_7C15, 0xBF58_476D_1CE4_E5B9);
                return (ph, Str(raw));
            }
            if k == 0 {
                return (h, user);
            }
            i = (i + 1) & pmask;
        }
    };
    // Lookups come in random order over a table bigger than cache, so callers prefetch the
    // home slot PF lookups ahead.
    const PF: usize = 16;
    let prefetch_person =
        |h: u64| csv::prefetch(unsafe { pmap.as_ptr().add(((h | 1) >> pshift) as usize) } as *const u8);

    // ---- clicks ----
    let cl_chunks = chunks(&cl_maps, CH);
    let cl_c: Vec<Mutex<Vec<List<ClC>>>> =
        (0..nt).map(|_| Mutex::new((0..npc).map(|_| List::default()).collect())).collect();
    let cl_tmp: Vec<Mutex<Vec<ClB>>> = (0..nt).map(|_| Mutex::new(Vec::new())).collect();
    par_for(nt, cl_chunks.len(), |t, ci| {
        let (fi, s, e) = cl_chunks[ci];
        let d: &[u8] = &cl_maps[fi][s..e];
        let mut tmp = cl_tmp[t].lock().unwrap();
        tmp.clear();
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
            tmp.push(ClB { uh, ts: t, cid: Str::of(f[0]), camp: camp as u16 });
        });
        let mut g = cl_c[t].lock().unwrap();
        for i in 0..tmp.len() {
            if i + PF < tmp.len() {
                prefetch_person(tmp[i + PF].uh);
            }
            let r = &tmp[i];
            let (pkey, _) = person_of(r.uh, Str(0));
            g[(pkey >> pshc) as usize].push(pool, ClC::new(pkey, r.cid, (r.ts - cws) as u64, r.camp));
        }
    });
    drop(cl_tmp);
    log(&t0, "clicks");

    // ---- events phase B: dedup with 8-byte-per-slot L2-resident table reused per thread ----
    let ev_a = split_parts(ev_a);
    let ev_c: Vec<Mutex<Vec<List<EvC>>>> =
        (0..nt).map(|_| Mutex::new((0..npc).map(|_| List::default()).collect())).collect();
    let next_p = AtomicUsize::new(0);
    std::thread::scope(|s| {
        let ev_a_ref = &ev_a;
        let ev_c_ref = &ev_c;
        let next_p_ref = &next_p;
        let person_of = &person_of;
        let prefetch_person = &prefetch_person;
        for t in 0..nt {
            s.spawn(move || {
                let mut tab: Vec<u64> = Vec::new();
                let mut best: Vec<EvA> = Vec::new();
                let mut g = ev_c_ref[t].lock().unwrap();
                let mut st = wc::Stager::<EvC, 8>::new(npc);
                loop {
                    let p = next_p_ref.fetch_add(1, Ordering::Relaxed);
                    if p >= npa {
                        break;
                    }
                    let parts = take_part(ev_a_ref, p);
                    let n: usize = parts.iter().map(|v| v.len()).sum();
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

                    for tv in &parts {
                        tv.for_slices(|sl| for r in sl {
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
                        });
                    }
                    for tv in parts {
                        tv.release(pool);
                    }

                    for i in 0..best.len() {
                        if i + PF < best.len() {
                            prefetch_person(best[i + PF].uh);
                        }
                        let r = &best[i];
                        let dt = r.dt();
                        if dt == INVALID_DT {
                            continue;
                        }
                        let (pkey, name) = person_of(r.uh, r.user);
                        let price = r.price();
                        st.push(&mut g, pool, (pkey >> pshc) as usize, EvC::new(pkey, name, dt, price, r.cat()));
                    }
                }
                st.finish(&mut g, pool);
            });
        }
    });
    drop(ev_a);
    log(&t0, "events B");

    drop(pmap);

    // ---- phase C: sessions + attribution ----
    let ev_c = split_parts(ev_c);
    let cl_c = split_parts(cl_c);
    let ncat = cat_names.len();
    // Bits for a timestamp offset from the click window start (covers events too).
    let tb = 64 - ((we - cws - 1).max(1) as u64).leading_zeros();
    struct TOut {
        ud: pq::UdCols,
        // Per (campaign, day): revenue, sessions, purchasing sessions. 16 bytes per cell
        // (instead of 32) keeps this randomly-hit table at half the L2 footprint.
        camp: Vec<CampCell>,
        assisted: Vec<u32>,
    }
    // Rough upper estimate of rows per thread (one row per person-day, ~85 B per event line);
    // the vectors still grow if it is short.
    let est_rows = total_ev_bytes / 85 / nt.max(1) / 3 + 1024;
    let touts: Vec<Mutex<TOut>> = (0..nt)
        .map(|_| {
            Mutex::new(TOut {
                ud: pq::UdCols::with_capacity(est_rows),
                camp: vec![CampCell::default(); (ncamp + 1) * ndays],
                assisted: vec![0; (ncamp + 1) * ndays],
            })
        })
        .collect();
    let cat_strs: &[&[u8]] = &cat_names;
    let scratch: Vec<Mutex<(Scratch<EvC>, Scratch<ClC>)>> =
        (0..nt).map(|_| Mutex::new((Scratch::new(), Scratch::new()))).collect();
    let order = lpt_order(&ev_c, &cl_c, npc);
    // Phase C buckets: the 12 hash bits just below the partition bits.
    let bsh = pshc - 12;
    let ud_dir = out.join("user_daily");
    let cd_dir = out.join("campaign_daily");
    std::fs::create_dir_all(&ud_dir).unwrap();
    std::fs::create_dir_all(&cd_dir).unwrap();
    par_for_fin(nt, npc, |t, pi| {
        let p = order[pi];
        let mut scr = scratch[t].lock().unwrap();
        let (esc, csc) = &mut *scr;
        bucket_sort(&ev_c, p, bsh, |e| e.pkey, |e| (e.ts(ws) - cws) as u64, tb, |a, b| {
            (a.pkey, a.dt()).cmp(&(b.pkey, b.dt()))
        }, esc, pool);
        let evs = &esc.scat;
        bucket_sort(&cl_c, p, bsh, |c| c.pkey, |c| c.dt() as u64, tb, |a, b| {
            a.pkey
                .cmp(&b.pkey)
                .then_with(|| a.dt().cmp(&b.dt()))
                .then_with(|| a.cid.get().cmp(b.cid.get()))
        }, csc, pool);
        let cls = &mut csc.scat;
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
        let TOut { ud, camp, assisted } = &mut *g;
        let mut cnt = vec![0u32; ncamp];
        let mut pos = vec![0u32; ncamp];
        let mut active: Vec<u16> = vec![];
        let mut catsum = vec![0i64; ncat];
        let mut cat_touched: Vec<u16> = vec![];
        let mut ci = 0usize;
        let mut i = 0usize;
        // User names are random reads into the input; prefetch each person's name once, about
        // NAME_PF events ahead of where it is copied to the output.
        let mut pf = 0usize;
        while i < evs.len() {
            let pk = evs[i].pkey;
            let pf_end = (i + NAME_PF).min(evs.len());
            while pf < pf_end {
                if pf == 0 || evs[pf].pkey != evs[pf - 1].pkey {
                    let nm = evs[pf].name.get();
                    csv::prefetch(nm.as_ptr());
                    csv::prefetch(nm.as_ptr().wrapping_add(nm.len().saturating_sub(1)));
                }
                pf += 1;
            }
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
                    let row = cur.take().unwrap();
                    let top = top_cat(&mut catsum, &mut cat_touched).map(|c| cat_strs[c as usize]);
                    ud.push(row.name.get(), row.day, row.sessions, row.events, row.revenue, top);
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
                let row = cur.get_or_insert(UdRow { name, day, sessions: 0, events: 0, revenue: 0 });
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
                        assisted[c as usize * ndays + di] += 1;
                    }
                    pc[b].camp() as usize
                } else {
                    ncamp
                };
                let r = &mut camp[credited * ndays + di];
                r.rev += rev as u64;
                r.ses += 1;
                r.pur += purchased as u32;
                k = m;
            }
            if let Some(row) = cur.take() {
                let top = top_cat(&mut catsum, &mut cat_touched).map(|c| cat_strs[c as usize]);
                ud.push(row.name.get(), row.day, row.sessions, row.events, row.revenue, top);
            }
            for q in lo..hi {
                cnt[pc[q].camp() as usize] = 0;
            }
            active.clear();
            i = j;
        }
    }, |t| {
        // This thread's user_daily rows are complete: write them while others finish.
        let g = touts[t].lock().unwrap();
        pq::write_user_daily(&ud_dir.join(format!("part-{t}.parquet")), &g.ud);
    });
    log(&t0, "phase C"); // includes the user_daily writes

    // ---- output ----
    let touts: Vec<TOut> = touts.into_iter().map(|m| m.into_inner().unwrap()).collect();
    let mut camp_tot = vec![[0i64; 4]; (ncamp + 1) * ndays];
    for to in &touts {
        for ((a, b), &x) in camp_tot.iter_mut().zip(to.camp.iter()).zip(to.assisted.iter()) {
            a[0] += b.ses as i64;
            a[1] += b.pur as i64;
            a[2] += b.rev as i64;
            a[3] += x as i64;
        }
    }
    {
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
    }
    log(&t0, "done");
    // Skip freeing the large buffers; the OS reclaims them at exit.
    std::process::exit(0);
}

/// Partition indices, largest first (sizes summed over threads), so whale partitions start
/// early and do not leave one thread running alone at the end.
fn lpt_order<T: Copy, U: Copy>(a: &Parts<T>, b: &Parts<U>, np: usize) -> Vec<usize> {
    let size = |p: usize| -> usize {
        a.iter().map(|w| w[p].lock().unwrap().len()).sum::<usize>()
            + b.iter().map(|w| w[p].lock().unwrap().len()).sum::<usize>()
    };
    let mut v: Vec<(usize, usize)> = (0..np).map(|p| (size(p), p)).collect();
    v.sort_unstable_by(|x, y| y.cmp(x));
    v.into_iter().map(|(_, p)| p).collect()
}

/// Per-thread, per-partition buffers; each partition is taken (and freed) by its one consumer.
type Parts<T> = Vec<Vec<Mutex<List<T>>>>;

fn split_parts<T>(v: Vec<Mutex<Vec<List<T>>>>) -> Parts<T> {
    v.into_iter().map(|m| m.into_inner().unwrap().into_iter().map(Mutex::new).collect()).collect()
}

/// Takes partition `p` out of every thread's buffers; the consumer releases it to the pool.
fn take_part<T: Copy>(parts: &Parts<T>, p: usize) -> Vec<List<T>> {
    parts.iter().map(|w| std::mem::take(&mut *w[p].lock().unwrap())).collect()
}

/// Gathers partition `p` from every thread's output and sorts it by (pkey, ts). The partition
/// already shares the top partition bits, so a counting pass on the next 12 bits (from `bsh`) leaves buckets of
/// one or a few persons. Each bucket is then sorted as packed u64 keys
/// `rank(pkey) | ts offset | index` (u64 sorts use branchless networks), and the records are
/// gathered in key order. Records with equal (pkey, ts) come out in input order. A bucket whose
/// keys do not fit 64 bits falls back to a comparison sort with `cmp`.
fn bucket_sort<T: Copy>(
    parts: &Parts<T>,
    p: usize,
    bsh: u32,
    key: impl Fn(&T) -> u64,
    tsoff: impl Fn(&T) -> u64,
    tb: u32,
    cmp: impl Fn(&T, &T) -> std::cmp::Ordering + Copy,
    sc: &mut Scratch<T>,
    pool: &Pool,
) {
    let bucket = |v: &T| ((key(v) >> bsh) & 4095) as usize;
    let workers = take_part(parts, p);
    let mut ends = vec![0usize; 4096];
    for w in &workers {
        w.for_slices(|sl| {
            for v in sl {
                ends[bucket(v)] += 1;
            }
        });
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
        w.for_slices(|sl| {
            for v in sl {
                let o = &mut ends[bucket(v)];
                // Prefix sums give each bucket a disjoint range with one slot per input.
                unsafe { dst.add(*o).write(*v) };
                *o += 1;
            }
        });
    }
    unsafe { scat.set_len(n) };
    for w in workers {
        w.release(pool);
    }
    // Each bucket is sorted in place (sorted buckets stay where they are in `scat`), so no
    // second partition-sized buffer is written.
    let mut start = 0;
    for &end in ends.iter() {
        let b = &mut scat[start..end];
        start = end;
        if b.len() <= SMALL {
            // Stable insertion sort by (key, tsoff): ties keep input order, as the packed
            // keys do.
            for i in 1..b.len() {
                let v = b[i];
                let kv = (key(&v), tsoff(&v));
                let mut j = i;
                while j > 0 && (key(&b[j - 1]), tsoff(&b[j - 1])) > kv {
                    b[j] = b[j - 1];
                    j -= 1;
                }
                b[j] = v;
            }
            continue;
        }
        out.clear();
        if sort_bucket(b, &key, &tsoff, tb, bsh, keys, pks, out) {
            b.copy_from_slice(out);
        } else {
            b.sort_unstable_by(cmp);
        }
    }
}

/// Per-thread buffers for `bucket_sort`, reused across partitions to avoid fresh page faults.
/// `scat` holds the sorted partition; `out` is one bucket's gather buffer.
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

/// How many events ahead phase C prefetches user names.
const NAME_PF: usize = 24;

/// Appends `b` sorted by (key, tsoff, input index) to `out`; false (nothing appended) if the
/// packed keys would not fit in 64 bits.
#[inline(always)]
fn sort_bucket<T: Copy>(
    b: &[T],
    key: &impl Fn(&T) -> u64,
    tsoff: &impl Fn(&T) -> u64,
    tb: u32,
    bsh: u32,
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
        let sh = tb + ib;
        let mask = (1u64 << ib) - 1;
        // Bucket members share every key bit above `bsh`, so the key bits just below it order
        // the persons as the full key does, as long as no two persons share them. Use those
        // bits as the rank and check the sorted runs; on a clash, rank by sort + dedup below.
        let rbits = rb.min(bsh);
        if rbits >= 8 {
            let lsh = bsh - rbits;
            let rmask = (1u64 << rbits) - 1;
            keys.extend(
                b.iter().enumerate().map(|(i, v)| (((key(v) >> lsh) & rmask) << sh) | (tsoff(v) << ib) | i as u64),
            );
            keys.sort_unstable();
            let mut ok = true;
            for w in keys.windows(2) {
                let same_rank = (w[0] >> sh) == (w[1] >> sh);
                let (x, y) = unsafe { (b.get_unchecked((w[0] & mask) as usize), b.get_unchecked((w[1] & mask) as usize)) };
                ok &= !same_rank | (key(x) == key(y));
            }
            if ok {
                out.extend(keys.iter().map(|&k| unsafe { *b.get_unchecked((k & mask) as usize) }));
                return true;
            }
            keys.clear();
        }
        pks.clear();
        pks.extend(b.iter().map(key));
        pks.sort_unstable();
        pks.dedup();
        if rb < 64 && pks.len() as u64 > (1u64 << rb) {
            return false;
        }
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

#[inline(always)]
fn process_ev_chunk(
    ci: usize,
    ev_chunks: &[(usize, usize, usize)],
    ev_maps: &[Mmap],
    products: &KeyMap,
    ws: i64,
    we: i64,
    unknown_cat: u16,
    psh: u32,
    g: &mut [List<EvA>],
    st: &mut wc::Stager<EvA, 8>,
    pool: &Pool,
) {
    let (fi, s, e) = ev_chunks[ci];
    let d: &[u8] = &ev_maps[fi][s..e];
    csv::for_rows::<7, _>(d, |f| {
        if f[0].is_empty() {
            return;
        }
        let ehash = hash64(key128(f[0]));
        let ingest_raw = ts::parse_ingest(f[3]).unwrap_or(i64::MAX);
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
            let is_p = ty == b"purchase";
            if !(is_p || ty == b"view" || ty == b"cart") {
                break 'v;
            }
            let mut p = 0;
            if is_p {
                let Some(v) = csv::parse_price(f[6]) else { break 'v };
                p = v;
            }
            let Some(t) = ts::parse_ts(f[2]) else { break 'v };
            if t < ws || t >= we || ingest_raw - t > DAY {
                break 'v;
            }
            if is_p {
                price = p;
                cat = match products.get(key128(f[5])) {
                    Some(c) => c as u16,
                    None => unknown_cat,
                };
            }
            dt = (t - ws) as u64;
        }
        let p = (ehash >> psh) as usize;
        let uh = hash64(key128(f[1]));
        st.push(g, pool, p, EvA {
            ehash,
            uh,
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
