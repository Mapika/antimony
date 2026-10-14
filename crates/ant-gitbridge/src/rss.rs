//! Keeps the resident size flat while reading big packs.
//!
//! The git object database memory-maps whole pack files. Every page the
//! importer touches stays in this process's resident set until the kernel
//! needs the memory, so reading a multi-gigabyte history makes the process
//! look like it uses gigabytes (and a memory-limited container would pay for
//! it). The pages are clean, file-backed and re-readable, so dropping them
//! from our page tables is always safe; they stay in the page cache.

/// Drop the resident pages of mapped pack/index files once more than
/// `limit_bytes` of file-backed memory is resident.
#[cfg(target_os = "linux")]
pub(crate) fn trim_mapped_packs(limit_bytes: usize) {
    let Ok(statm) = std::fs::read_to_string("/proc/self/statm") else { return };
    // size resident shared text lib data dt (pages); `shared` = file-backed.
    let shared_pages: usize =
        statm.split_whitespace().nth(2).and_then(|v| v.parse().ok()).unwrap_or(0);
    // SAFETY: sysconf has no preconditions.
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) }.max(4096) as usize;
    if shared_pages * page < limit_bytes {
        return;
    }
    let Ok(maps) = std::fs::read_to_string("/proc/self/maps") else { return };
    for line in maps.lines() {
        let mut parts = line.split_whitespace();
        let (Some(range), Some(perms)) = (parts.next(), parts.next()) else { continue };
        let path = parts.nth(3).unwrap_or("");
        if !perms.starts_with("r") || perms.as_bytes().get(1) == Some(&b'w') {
            continue;
        }
        if !(path.contains("/objects/pack/") && (path.ends_with(".pack") || path.ends_with(".idx")))
        {
            continue;
        }
        let Some((start, end)) = range.split_once('-') else { continue };
        let (Ok(start), Ok(end)) =
            (usize::from_str_radix(start, 16), usize::from_str_radix(end, 16))
        else {
            continue;
        };
        // SAFETY: the range is a read-only private file mapping of this
        // process; MADV_DONTNEED on such a mapping only discards clean pages,
        // which are faulted back in from the file on the next access.
        unsafe {
            libc::madvise(start as *mut libc::c_void, end - start, libc::MADV_DONTNEED);
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn trim_mapped_packs(_limit_bytes: usize) {}

/// Limit allocator arenas (one per thread by default, each holding freed
/// memory back) and hand freed heap memory back to the OS.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub(crate) fn limit_arenas() {
    // SAFETY: mallopt only sets allocator tunables.
    unsafe {
        libc::mallopt(libc::M_ARENA_MAX, 4);
        // Fixed thresholds: big buffers are mapped and returned to the OS when
        // freed instead of fragmenting the heap (the dynamic threshold grows
        // to 32 MiB after the first large free).
        libc::mallopt(libc::M_MMAP_THRESHOLD, 1 << 20);
        libc::mallopt(libc::M_TRIM_THRESHOLD, 16 << 20);
    }
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub(crate) fn release_heap() {
    // SAFETY: malloc_trim only returns free memory to the OS.
    unsafe {
        libc::malloc_trim(0);
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
pub(crate) fn limit_arenas() {}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
pub(crate) fn release_heap() {}
