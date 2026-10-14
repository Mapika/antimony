//! Forward-mode read authorization state: short-lived per-principal
//! decisions and the pending batch of permission checks (see the crate docs,
//! "Permission checks").
//!
//! A decision is keyed by a SHA-256 of the caller's `Authorization` header
//! (never the header itself: the raw credential is not kept here, logged or
//! exported in metrics) and the object id. Only *allowed* decisions are kept,
//! for at most the configured TTL; a denial is asked again each time, so a
//! grant takes effect at once and only a revocation can lag, by the TTL.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ant_core::ObjectId;
use parking_lot::Mutex;
use tokio::sync::oneshot;

use crate::serve::Reply;

/// Stable key of a caller's credentials.
pub(crate) type CredKey = [u8; 32];

/// Hash of the `Authorization` header value (`None`: no header).
pub(crate) fn cred_key(auth: Option<&str>) -> CredKey {
    match auth {
        Some(a) => ant_auth::hash_token(&format!("a{a}")),
        None => ant_auth::hash_token("n"),
    }
}

/// The origin's decision about one object for one caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// `GET /o/{id}` would serve it; `private`: not to be stored by shared caches.
    Allowed { private: bool },
    /// It would not (hidden or missing: the origin does not say which).
    Denied,
}

/// The answer of one check for one object.
#[derive(Debug, Clone)]
pub(crate) enum Outcome {
    Verdict(Verdict),
    /// The origin refused the request itself (bad credentials, rate limit,
    /// ...): its response is passed through.
    Refused(Arc<Reply>),
    /// The origin has no `POST /o/check`.
    Unsupported,
}

pub(crate) type Slot = Result<Outcome, Arc<String>>;

/// Most decisions kept; beyond it expired ones, then all, are dropped.
const MAX_DECISIONS: usize = 1 << 19;
/// How long a missing `/o/check` is remembered before probing again.
pub(crate) const UNSUPPORTED_FOR: Duration = Duration::from_secs(60);

pub(crate) struct Pending {
    pub creds: Option<String>,
    pub waiters: Vec<(ObjectId, oneshot::Sender<Slot>)>,
}

pub(crate) struct AuthzState {
    pub ttl: Duration,
    pub window: Duration,
    decisions: Mutex<HashMap<(CredKey, ObjectId), (Instant, bool)>>,
    pub pending: Mutex<HashMap<CredKey, Pending>>,
    unsupported_until: Mutex<Option<Instant>>,
}

impl AuthzState {
    pub fn new(ttl: Duration, window: Duration) -> Self {
        Self {
            ttl,
            window,
            decisions: Mutex::default(),
            pending: Mutex::default(),
            unsupported_until: Mutex::new(None),
        }
    }

    /// The still-valid allowed decision for `(key, id)`, if any.
    pub fn lookup(&self, key: &CredKey, id: &ObjectId, now: Instant) -> Option<Verdict> {
        if self.ttl.is_zero() {
            return None;
        }
        let map = self.decisions.lock();
        let (at, private) = *map.get(&(*key, *id))?;
        (now.saturating_duration_since(at) < self.ttl).then_some(Verdict::Allowed { private })
    }

    /// Remember allowed decisions taken at `asked_at` (the time the question
    /// was sent, so a slow answer is not credited with extra lifetime).
    pub fn remember(
        &self,
        key: &CredKey,
        asked_at: Instant,
        allowed: impl Iterator<Item = (ObjectId, bool)>,
    ) {
        if self.ttl.is_zero() {
            return;
        }
        let mut map = self.decisions.lock();
        if map.len() >= MAX_DECISIONS {
            let now = Instant::now();
            let ttl = self.ttl;
            map.retain(|_, (at, _)| now.saturating_duration_since(*at) < ttl);
            if map.len() >= MAX_DECISIONS {
                map.clear();
            }
        }
        for (id, private) in allowed {
            map.insert((*key, id), (asked_at, private));
        }
    }

    pub fn is_unsupported(&self, now: Instant) -> bool {
        self.unsupported_until.lock().is_some_and(|t| now < t)
    }

    pub fn mark_unsupported(&self, now: Instant) {
        *self.unsupported_until.lock() = Some(now + UNSUPPORTED_FOR);
    }

    /// Never use the batch endpoint (as against an origin without it).
    pub fn disable(&self) {
        *self.unsupported_until.lock() = Some(Instant::now() + Duration::from_secs(86_400 * 3650));
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.decisions.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: u8) -> ObjectId {
        ObjectId([n; 32])
    }

    #[test]
    fn keys_separate_principals_and_never_equal_the_header() {
        let a = cred_key(Some("Bearer a"));
        assert_ne!(a, cred_key(Some("Bearer b")));
        assert_ne!(a, cred_key(None));
        assert_ne!(cred_key(Some("")), cred_key(None));
        assert_eq!(a, cred_key(Some("Bearer a")));
    }

    #[test]
    fn decisions_expire_and_are_per_principal() {
        let s = AuthzState::new(Duration::from_millis(100), Duration::ZERO);
        let (a, b) = (cred_key(Some("a")), cred_key(Some("b")));
        let t0 = Instant::now();
        s.remember(&a, t0, [(id(1), false), (id(2), true)].into_iter());
        assert_eq!(s.lookup(&a, &id(1), t0), Some(Verdict::Allowed { private: false }));
        assert_eq!(s.lookup(&a, &id(2), t0), Some(Verdict::Allowed { private: true }));
        assert_eq!(s.lookup(&b, &id(1), t0), None, "another principal");
        assert_eq!(s.lookup(&a, &id(3), t0), None);
        assert!(s.lookup(&a, &id(1), t0 + Duration::from_millis(99)).is_some());
        assert!(
            s.lookup(&a, &id(1), t0 + Duration::from_millis(100)).is_none(),
            "TTL is exclusive"
        );
    }

    #[test]
    fn zero_ttl_keeps_nothing() {
        let s = AuthzState::new(Duration::ZERO, Duration::ZERO);
        let k = cred_key(Some("a"));
        s.remember(&k, Instant::now(), [(id(1), false)].into_iter());
        assert_eq!(s.len(), 0);
        assert_eq!(s.lookup(&k, &id(1), Instant::now()), None);
    }

    #[test]
    fn unsupported_is_remembered_for_a_while() {
        let s = AuthzState::new(Duration::from_secs(1), Duration::ZERO);
        let t0 = Instant::now();
        assert!(!s.is_unsupported(t0));
        s.mark_unsupported(t0);
        assert!(s.is_unsupported(t0 + Duration::from_secs(59)));
        assert!(!s.is_unsupported(t0 + UNSUPPORTED_FOR));
    }
}
