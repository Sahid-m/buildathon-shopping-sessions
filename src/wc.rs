//! Software write-combining for radix scatters. Records for each partition are staged in a
//! small buffer and flushed K at a time with non-temporal stores, so the destination lines
//! are not first read from memory (no read-for-ownership) and do not evict hot data.

use std::mem::MaybeUninit;

pub struct Stager<T: Copy, const K: usize> {
    buf: Vec<[MaybeUninit<T>; K]>,
    cnt: Vec<u32>,
}

impl<T: Copy, const K: usize> Stager<T, K> {
    pub fn new(np: usize) -> Self {
        Stager { buf: (0..np).map(|_| [MaybeUninit::uninit(); K]).collect(), cnt: vec![0; np] }
    }

    #[inline(always)]
    pub fn push(&mut self, out: &mut [Vec<T>], p: usize, v: T) {
        let c = unsafe { self.cnt.get_unchecked_mut(p) };
        let b = unsafe { self.buf.get_unchecked_mut(p) };
        unsafe { b.get_unchecked_mut(*c as usize).write(v) };
        *c += 1;
        if *c as usize == K {
            *c = 0;
            let dst = unsafe { out.get_unchecked_mut(p) };
            dst.reserve(K);
            unsafe {
                stream_copy(
                    dst.as_mut_ptr().add(dst.len()) as *mut u8,
                    b.as_ptr() as *const u8,
                    K * std::mem::size_of::<T>(),
                );
                dst.set_len(dst.len() + K);
            }
        }
    }

    /// Flushes the partial buffers and orders the streamed stores before later reads.
    pub fn finish(&mut self, out: &mut [Vec<T>]) {
        for p in 0..self.cnt.len() {
            let c = self.cnt[p] as usize;
            let b = &self.buf[p];
            for i in 0..c {
                out[p].push(unsafe { b[i].assume_init() });
            }
            self.cnt[p] = 0;
        }
        fence();
    }
}

#[inline(always)]
unsafe fn stream_copy(dst: *mut u8, src: *const u8, n: usize) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::_mm_stream_si64;
        if n % 8 == 0 && (dst as usize) % 8 == 0 {
            let mut i = 0;
            while i < n {
                _mm_stream_si64(dst.add(i) as *mut i64, (src.add(i) as *const i64).read_unaligned());
                i += 8;
            }
            return;
        }
        std::ptr::copy_nonoverlapping(src, dst, n);
    }
    #[cfg(not(target_arch = "x86_64"))]
    unsafe {
        std::ptr::copy_nonoverlapping(src, dst, n);
    }
}

#[inline(always)]
fn fence() {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_sfence();
    }
}
