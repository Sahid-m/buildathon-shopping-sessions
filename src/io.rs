//! Input loading. By default every input file is read into memory with large parallel
//! `pread` calls, instead of being memory-mapped and faulted in one page at a time.
//! `SS_MMAP=1` switches back to plain mmap.

use std::alloc::{Layout, alloc, dealloc};
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use memmap2::Mmap;

const ALIGN: usize = 2 << 20;
const JOB: usize = 8 << 20;

/// One file's bytes, either read into an owned buffer or memory-mapped.
pub enum Data {
    Buf { ptr: *mut u8, len: usize, cap: usize },
    Map(Mmap),
}

unsafe impl Send for Data {}
unsafe impl Sync for Data {}

impl std::ops::Deref for Data {
    type Target = [u8];
    #[inline(always)]
    fn deref(&self) -> &[u8] {
        match self {
            Data::Buf { ptr, len, .. } => unsafe { std::slice::from_raw_parts(*ptr, *len) },
            Data::Map(m) => m,
        }
    }
}

impl Drop for Data {
    fn drop(&mut self) {
        if let Data::Buf { ptr, cap, .. } = self {
            unsafe { dealloc(*ptr, Layout::from_size_align_unchecked(*cap, ALIGN)) };
        }
    }
}

#[cfg(target_os = "linux")]
fn advise_huge(ptr: *mut u8, len: usize) {
    unsafe extern "C" {
        fn madvise(addr: *mut u8, len: usize, advice: i32) -> i32;
    }
    const MADV_HUGEPAGE: i32 = 14;
    unsafe { madvise(ptr, len, MADV_HUGEPAGE) };
}

#[cfg(not(target_os = "linux"))]
fn advise_huge(_ptr: *mut u8, _len: usize) {}

/// Loads every non-empty file in `paths`, in order. Empty files are skipped.
pub fn load_all(paths: &[PathBuf], nt: usize) -> Vec<Data> {
    let files: Vec<(File, usize)> = paths
        .iter()
        .filter_map(|p| {
            let f = File::open(p).ok()?;
            let len = f.metadata().ok()?.len() as usize;
            if len == 0 { None } else { Some((f, len)) }
        })
        .collect();
    if std::env::var_os("SS_MMAP").is_some() {
        return files.iter().filter_map(|(f, _)| unsafe { Mmap::map(f).ok() }.map(Data::Map)).collect();
    }
    let out: Vec<Data> = files
        .iter()
        .map(|&(_, len)| {
            let cap = len.div_ceil(ALIGN) * ALIGN;
            let ptr = unsafe { alloc(Layout::from_size_align(cap, ALIGN).unwrap()) };
            assert!(!ptr.is_null(), "out of memory reading input");
            advise_huge(ptr, cap);
            Data::Buf { ptr, len, cap }
        })
        .collect();
    let mut jobs: Vec<(usize, usize, usize)> = vec![];
    for (fi, &(_, len)) in files.iter().enumerate() {
        let mut s = 0;
        while s < len {
            let e = (s + JOB).min(len);
            jobs.push((fi, s, e));
            s = e;
        }
    }
    let next = AtomicUsize::new(0);
    let threads = nt.max(1).min(jobs.len().max(1));
    std::thread::scope(|sc| {
        for _ in 0..threads {
            sc.spawn(|| {
                loop {
                    let j = next.fetch_add(1, Ordering::Relaxed);
                    if j >= jobs.len() {
                        break;
                    }
                    let (fi, s, e) = jobs[j];
                    let Data::Buf { ptr, .. } = &out[fi] else { unreachable!() };
                    // Jobs cover disjoint byte ranges of the buffer.
                    let dst = unsafe { std::slice::from_raw_parts_mut(ptr.add(s), e - s) };
                    let mut done = 0;
                    while done < dst.len() {
                        match files[fi].0.read_at(&mut dst[done..], (s + done) as u64) {
                            Ok(0) => panic!("input file shrank while reading"),
                            Ok(n) => done += n,
                            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                            Err(e) => panic!("read failed: {e}"),
                        }
                    }
                }
            });
        }
    });
    out
}

/// `sessions --diag DATA`: measures the machine (cold read speed, page-fault and copy cost).
pub fn diag(data: &std::path::Path, nt: usize) {
    use std::time::Instant;
    let list = |d: &str| -> Vec<PathBuf> {
        let mut v: Vec<PathBuf> = std::fs::read_dir(data.join(d))
            .map(|rd| rd.filter_map(|e| e.ok()).map(|e| e.path()).collect())
            .unwrap_or_default();
        v.sort();
        v
    };
    let ev = list("events");
    let t = Instant::now();
    let bufs = load_all(&ev, nt);
    let bytes: usize = bufs.iter().map(|b| b.len()).sum();
    let s = t.elapsed().as_secs_f64();
    eprintln!("DIAG first parallel pread of events: {} MB in {:.3} s = {:.0} MB/s", bytes >> 20, s, bytes as f64 / s / 1e6);
    drop(bufs);
    let t = Instant::now();
    let bufs = load_all(&ev, nt);
    let s = t.elapsed().as_secs_f64();
    eprintln!("DIAG second parallel pread: {:.3} s = {:.0} MB/s", s, bytes as f64 / s / 1e6);
    drop(bufs);
    let t = Instant::now();
    let bufs = load_all(&ev, 1);
    eprintln!("DIAG single-thread pread: {:.3} s", t.elapsed().as_secs_f64());
    drop(bufs);
    // mmap + touch, single thread and parallel
    let maps: Vec<Mmap> = ev.iter().filter_map(|p| unsafe { Mmap::map(&File::open(p).ok()?).ok() }).collect();
    let t = Instant::now();
    let mut acc = 0u64;
    for m in &maps {
        let mut i = 0;
        while i < m.len() {
            acc = acc.wrapping_add(m[i] as u64);
            i += 4096;
        }
    }
    eprintln!("DIAG mmap touch every page, 1 thread: {:.3} s ({} pages) acc={}", t.elapsed().as_secs_f64(), bytes / 4096, acc & 1);
    drop(maps);
    let maps: Vec<Mmap> = ev.iter().filter_map(|p| unsafe { Mmap::map(&File::open(p).ok()?).ok() }).collect();
    let t = Instant::now();
    let next = AtomicUsize::new(0);
    std::thread::scope(|sc| {
        for _ in 0..nt {
            sc.spawn(|| {
                let mut acc = 0u64;
                loop {
                    let j = next.fetch_add(1, Ordering::Relaxed);
                    if j >= maps.len() {
                        break;
                    }
                    let m = &maps[j];
                    let mut i = 0;
                    while i < m.len() {
                        acc = acc.wrapping_add(m[i] as u64);
                        i += 4096;
                    }
                }
                std::hint::black_box(acc);
            });
        }
    });
    eprintln!("DIAG mmap touch every page, {nt} threads: {:.3} s", t.elapsed().as_secs_f64());
    drop(maps);
    // anonymous memory faults
    for huge in [false, true] {
        let n = 1usize << 30;
        let ptr = unsafe { alloc(Layout::from_size_align(n, ALIGN).unwrap()) };
        if huge {
            advise_huge(ptr, n);
        }
        let t = Instant::now();
        let mut i = 0;
        while i < n {
            unsafe { std::ptr::write_volatile(ptr.add(i), 1) };
            i += 4096;
        }
        let s = t.elapsed().as_secs_f64();
        eprintln!("DIAG touch 1 GiB anon (hugepage advice {huge}), 1 thread: {:.3} s = {:.2} us/4K page", s, s * 1e6 / (n / 4096) as f64);
        let t = Instant::now();
        unsafe { std::ptr::write_bytes(ptr, 7, n) }; std::hint::black_box(unsafe { std::ptr::read_volatile(ptr.add(n / 2)) });
        eprintln!("DIAG memset 1 GiB after touch: {:.3} s", t.elapsed().as_secs_f64());
        unsafe { dealloc(ptr, Layout::from_size_align_unchecked(n, ALIGN)) };
    }
    // parallel anon faults
    let t = Instant::now();
    std::thread::scope(|sc| {
        for _ in 0..nt {
            sc.spawn(|| {
                let n = 256usize << 20;
                let ptr = unsafe { alloc(Layout::from_size_align(n, ALIGN).unwrap()) };
                let mut i = 0;
                while i < n {
                    unsafe { std::ptr::write_volatile(ptr.add(i), 1) };
                    i += 4096;
                }
                unsafe { dealloc(ptr, Layout::from_size_align_unchecked(n, ALIGN)) };
            });
        }
    });
    eprintln!("DIAG {nt} threads x 256 MiB anon touch: {:.3} s", t.elapsed().as_secs_f64());
    let t = Instant::now();
    for _ in 0..20000 {
        std::hint::black_box(std::fs::metadata(data.join("manifest.json")).ok());
    }
    eprintln!("DIAG 20000 stat syscalls: {:.3} s", t.elapsed().as_secs_f64());
}
