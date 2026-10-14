//! Batch parallelism shared by the disk cache, read-through verification,
//! remote fetches and workspace scans: [`par_map`] runs on the calling
//! thread plus helper threads drawn from one process-wide budget.

use std::sync::atomic::{AtomicUsize, Ordering};

/// Threads to use for CPU/syscall-bound batch work.
fn parallelism() -> usize {
    static N: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *N.get_or_init(|| std::thread::available_parallelism().map_or(4, |n| n.get()).min(32))
}

/// Helper threads still available process-wide. Concurrent `par_map` callers
/// share this budget, so many simultaneous batches cannot oversubscribe the
/// machine (which shows up as lock spinning in the filesystem, not speed).
fn helper_budget() -> &'static AtomicUsize {
    static BUDGET: std::sync::OnceLock<AtomicUsize> = std::sync::OnceLock::new();
    BUDGET.get_or_init(|| AtomicUsize::new(parallelism().saturating_sub(1)))
}

/// Take up to `want` helper threads from the shared budget.
fn take_helpers(want: usize) -> usize {
    let budget = helper_budget();
    let mut cur = budget.load(Ordering::Relaxed);
    loop {
        let take = cur.min(want);
        if take == 0 {
            return 0;
        }
        match budget.compare_exchange_weak(cur, cur - take, Ordering::AcqRel, Ordering::Relaxed) {
            Ok(_) => return take,
            Err(now) => cur = now,
        }
    }
}

/// Map `f` over `items` on the calling thread plus helper threads drawn from
/// a process-wide budget of [`parallelism`], keeping at least `min_per_thread`
/// items per thread (small inputs run inline). Results are in input order.
pub(crate) fn par_map<T: Sync, R: Send>(
    items: &[T],
    min_per_thread: usize,
    f: impl Fn(&T) -> R + Sync,
) -> Vec<R> {
    let wanted = items.len().div_ceil(min_per_thread.max(1)).min(parallelism());
    let helpers = if wanted > 1 { take_helpers(wanted - 1) } else { 0 };
    if helpers == 0 {
        return items.iter().map(f).collect();
    }
    const CHUNK: usize = 8;
    let next = AtomicUsize::new(0);
    let work = || {
        let mut mine: Vec<(usize, Vec<R>)> = Vec::new();
        loop {
            let start = next.fetch_add(CHUNK, Ordering::Relaxed);
            if start >= items.len() {
                break;
            }
            let end = (start + CHUNK).min(items.len());
            mine.push((start, items[start..end].iter().map(&f).collect()));
        }
        mine
    };
    let parts: Vec<Vec<(usize, Vec<R>)>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..helpers).map(|_| scope.spawn(work)).collect();
        let mut parts = vec![work()];
        for h in handles {
            parts.push(h.join().unwrap_or_else(|p| std::panic::resume_unwind(p)));
        }
        parts
    });
    helper_budget().fetch_add(helpers, Ordering::AcqRel);
    let mut flat: Vec<(usize, Vec<R>)> = parts.into_iter().flatten().collect();
    flat.sort_unstable_by_key(|(start, _)| *start);
    flat.into_iter().flat_map(|(_, v)| v).collect()
}
