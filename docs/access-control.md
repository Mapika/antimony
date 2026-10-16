# Access control and threat model

`ant serve --policy policy.toml` (or `ANT_POLICY`) turns on access control;
without it the server runs in open mode and logs a warning. The policy
format (principals with SHA-256-hashed bearer tokens, longest-prefix path
rules, ref rules, admins) is documented in `crates/ant-auth`. Clients send
`ANT_TOKEN`: as `authorization: Bearer` on gRPC and HTTP, and as the Basic
auth password (any user name) for `git clone`. Missing or unknown tokens get
`unauthenticated` / 401, denials `permission_denied` / 403.

A minimal working policy is in [usage.md](usage.md#access-control-in-two-minutes);
operating one (reindexing, rate limits, tokens per agent) is in
[operations.md](operations.md#access-policy) and the
[operator guide](operator-guide.md#tokens-principals-and-ref-naming).

## What is protected

- *File content under unreadable paths*: `ReadPath`, `Blame`, `Diff`
  (including patches and path prefixes) and `Log` / `LogRange` with a path
  behave as if those paths did not exist.
- *Objects by id*: every blob and tree written under a restricted path rule
  (a *zone*) is tagged with that zone, so `GetObjects`, `HasObjects` and
  `GET /o/<id>` report it exactly like a missing object (same code, body and
  headers). Content is readable by id if *any* place it is stored is
  readable: the same bytes at a public path are not secret. Objects outside
  the root zone are served with `Cache-Control: private`. Tree bundles
  (`GET /t/<tree-id>`) never carry such objects in a `public` response, and
  bundles that depend on the caller are `private, no-store`
  ([architecture.md](architecture.md#tree-bundles-get-ttree-id)).
- *Read by copy*: `CreateChangeset` may reference a blob by id only if the
  caller can read it or uploaded it; otherwise it is "not found". Fresh
  uploads stay hidden from everyone until a changeset places them.
- *Writes*: every changed path must be writable — for `CreateChangeset` ops,
  for every commit `Land` replays (or, for a fast-forward, the net tree diff,
  which also stops reverts disguised as merges), and for `UpdateRef` moves
  (tree diff between old and new target). `Land`/`UpdateRef` also need the
  ref rule. Checks run per request before a land joins a group-commit batch,
  so a denied request never affects others. Only admins may upload trees or
  commits.
- *git export*: needs read access to everything (403 otherwise), because the
  export cannot filter trees without changing commit ids.

## Known v1 limitations

- Structure is not secret: readable trees (e.g. the root) list restricted
  entry *names and ids*; clients need them to sparse-skip. Commit metadata
  (messages, authors, ids, parents) is visible to every authenticated
  principal, including `Log` without a path.
- Objects written before the policy existed, by `ant import`, or by an admin
  through `PutObjects` (trees/commits) are untagged, i.e. public, until
  `ant admin reindex-acl <rev> --history` (`ant import --policy` runs it).
  Path-rule changes need a reindex; read/write list changes do not.
- Creating a ref (land onto a missing ref, `UpdateRef` with no expected
  value) needs only the ref rule: there is no previous content to protect.
- Tagging races can only over-restrict: concurrent first writes of identical
  bytes to a public and a restricted path may leave the object hidden by id
  (path reads still work). Error codes are uniform, but response timing
  still differs slightly between missing and hidden objects.
- Workspaces of principals who cannot read everything must be sparse around
  restricted paths. The client disk cache is shared per machine: principals
  sharing a cache directory share whatever it holds.
- Tokens are bearer secrets: run behind TLS.
