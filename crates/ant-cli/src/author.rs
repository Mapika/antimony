//! Commit author identity resolution.

use std::time::{SystemTime, UNIX_EPOCH};

use ant_core::Signature;

/// Resolve the author: `ANT_AUTHOR_NAME`/`ANT_AUTHOR_EMAIL`, else git config,
/// else `$USER` / `$USER@localhost`. `env` and `git` are lookups so the logic
/// is testable.
pub fn resolve_author(
    env: &dyn Fn(&str) -> Option<String>,
    git: &dyn Fn(&str) -> Option<String>,
    time: i64,
) -> Signature {
    let non_empty = |v: Option<String>| v.filter(|s| !s.trim().is_empty());
    let user = non_empty(env("USER")).unwrap_or_else(|| "unknown".into());
    let name = non_empty(env("ANT_AUTHOR_NAME"))
        .or_else(|| non_empty(git("user.name")))
        .unwrap_or_else(|| user.clone());
    let email = non_empty(env("ANT_AUTHOR_EMAIL"))
        .or_else(|| non_empty(git("user.email")))
        .unwrap_or_else(|| format!("{user}@localhost"));
    Signature { name: name.into_bytes(), email: email.into_bytes(), time, tz_offset_minutes: 0 }
}

/// Resolve from the real environment, `git config` and the clock.
pub fn current_author() -> Signature {
    let env = |k: &str| std::env::var(k).ok();
    let git = |k: &str| {
        let out = std::process::Command::new("git").args(["config", "--get", k]).output().ok()?;
        out.status.success().then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
    };
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0);
    resolve_author(&env, &git, now)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lookup<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |k| pairs.iter().find(|(a, _)| *a == k).map(|(_, v)| v.to_string())
    }

    #[test]
    fn env_wins() {
        let env = lookup(&[("ANT_AUTHOR_NAME", "E"), ("ANT_AUTHOR_EMAIL", "e@x"), ("USER", "u")]);
        let git = lookup(&[("user.name", "G"), ("user.email", "g@x")]);
        let s = resolve_author(&env, &git, 5);
        assert_eq!(
            (&s.name[..], &s.email[..], s.time, s.tz_offset_minutes),
            (&b"E"[..], &b"e@x"[..], 5, 0)
        );
    }

    #[test]
    fn git_then_user() {
        let env = lookup(&[("USER", "u")]);
        let git = lookup(&[("user.name", "G")]);
        let s = resolve_author(&env, &git, 0);
        assert_eq!((&s.name[..], &s.email[..]), (&b"G"[..], &b"u@localhost"[..]));
        let s = resolve_author(&env, &lookup(&[]), 0);
        assert_eq!((&s.name[..], &s.email[..]), (&b"u"[..], &b"u@localhost"[..]));
    }
}
