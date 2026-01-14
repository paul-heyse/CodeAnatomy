## PyGithub deep dive 1 — Client config + auth

This section is about getting a **production-grade `Github(...)` client**: correct auth, correct base URL (incl. GHES), and the knobs that matter for **performance + reliability** (retry, pool sizing, throttling).

---

# 1) The `Github(...)` client: what you can configure (and why it matters)

PyGithub’s main entry point is `github.Github(...)`. The current docs show its constructor signature and defaults, including:

* `base_url` (defaults to `https://api.github.com`)
* `timeout`
* `user_agent`
* `per_page`
* `verify` (bool or str)
* `retry` (int or `urllib3.util.retry.Retry`, can be set to `None` to disable)
* `pool_size`
* `seconds_between_requests` (default `0.25`)
* `seconds_between_writes` (default `1.0`)
* `auth` (preferred way to pass auth; older parameters like `login_or_token`, `password`, `jwt`, `app_auth` are deprecated)
* `lazy` (object laziness mode) ([PyGithub][1])

It also supports **connection cleanup** either via `.close()` or by using the client as a **context manager** (`with Github(...) as gh:`). ([PyGithub][1])

### Best practice

* **Create one client and reuse it** (don’t instantiate per request).
* Use the **context manager** if your program has a clear “session lifetime” (CLI tool, short job).

---

# 2) Base URL and GitHub Enterprise Server (GHES)

### GitHub.com

Default is `https://api.github.com` (PyGithub’s default `base_url`). ([PyGithub][1])

### GitHub Enterprise Server

GitHub’s GHES REST API base URL convention is:

* `http(s)://HOSTNAME/api/v3` ([GitHub Docs][2])

So for PyGithub you typically do:

* `Github(base_url="https://HOSTNAME/api/v3", ...)`

**Practical gotcha:** GHES almost always requires the `/api/v3` suffix (don’t just pass the web UI hostname). The GHES quickstart explicitly uses that `/api/v3` base. ([GitHub Docs][2])

---

# 3) TLS verification (`verify`) and enterprise certificates

PyGithub exposes `verify` as “boolean or string”. ([PyGithub][1])

How to use this safely in practice:

* `verify=True` (default) for normal TLS validation
* `verify="/path/to/ca-bundle.pem"` if your GHES uses a private CA
* Avoid `verify=False` except for local debugging (it disables certificate validation)

---

# 4) Page sizing (`per_page`) and the real max

PyGithub defaults `per_page=30`. ([PyGithub][1])

For list-heavy workloads (repo inventory, issues/PR enumeration, etc.), you almost always want:

* `per_page=100`

Because GitHub’s REST endpoints that support `per_page` commonly cap it at **max 100** (example shown on the Issues REST docs). ([GitHub Docs][3])

**Tradeoff:** bigger pages = fewer HTTP round trips (faster), but larger payloads (more memory + latency per request). In practice, `100` is the usual “throughput default”.

---

# 5) Auth deep dive: PAT vs GitHub App Installation

PyGithub’s docs explicitly recommend using `auth=...` objects (and mark old-style `login_or_token`, `password`, `jwt`, etc. as deprecated). ([PyGithub][1])

## A) Personal Access Token (PAT) — simplest

This is the “standard” for dev scripts and personal tooling.

```python
from github import Auth, Github

auth = Auth.Token("access_token")
with Github(auth=auth, per_page=100) as gh:
    me = gh.get_user().login
```

PyGithub shows exactly this `Auth.Token(...)` pattern in its authentication examples. ([PyGithub][4])

**When PAT is best**

* single-user tool
* low operational overhead
* you don’t need fine-grained org-wide install permissions

## B) GitHub App Installation auth — best for distributed software / automation

PyGithub’s docs describe two distinct things:

### 1) “App authentication” (as the App itself)

* Authenticate with `Auth.AppAuth(app_id, private_key)`
* Use `GithubIntegration(...)` to list installations, etc.
* Important: docs note the **App itself** can call only a limited set of endpoints; use `GithubIntegration` when authenticated as a GitHub App. ([PyGithub][4])

```python
from github import Auth, GithubIntegration

auth = Auth.AppAuth(123456, private_key)
gi = GithubIntegration(auth=auth)

for inst in gi.get_installations():
    print(inst.id)
```

([PyGithub][4])

### 2) “App installation authentication” (as an installation token)

This is the money mode for bots/services.

PyGithub states:

* `AppInstallationAuth` **fetches an access token**
* handles expiration
* **refreshes automatically** ([PyGithub][4])

Two documented ways:

**(a) Derive installation auth directly:**

```python
from github import Auth, Github

auth = Auth.AppAuth(123456, private_key).get_installation_auth(
    installation_id,
    token_permissions,   # optional permissions
)
gh = Github(auth=auth)
```

([PyGithub][4])

**(b) Go through `GithubIntegration`:**

```python
from github import Auth, GithubIntegration

auth = Auth.AppAuth(123456, private_key)
gi = GithubIntegration(auth=auth)

gh = gi.get_github_for_installation(installation_id, token_permissions)
```

([PyGithub][4])

**When App Installation auth is best**

* distributed tooling (avoid users generating PATs)
* least-privilege permissions via installation + `token_permissions`
* better operational security posture than “everyone brings a PAT”

---

# 6) Reliability + performance knobs: retry, pooling, throttling

## A) Throttling defaults align with GitHub best practices

PyGithub defaults:

* `seconds_between_requests = 0.25`
* `seconds_between_writes = 1.0` ([PyGithub][1])

GitHub’s REST best practices explicitly recommend:

* **avoid concurrent requests** (queue/serialize)
* **wait at least one second between mutative requests** (POST/PATCH/PUT/DELETE) ([GitHub Docs][5])

So PyGithub’s defaults are already tuned in that spirit.

### What to do

* If you’re mostly **GET-only** and not running concurrency, you can often reduce `seconds_between_requests` modestly.
* If you’re doing **write-heavy automation**, keep `seconds_between_writes >= 1.0` (or increase it).

## B) Secondary rate limits: concurrency ceilings + what happens

GitHub explicitly calls out secondary limits such as:

* no more than **100 concurrent requests** (shared across REST + GraphQL)
* exceeding secondary limits can yield **403 or 429**
* if `retry-after` is present, wait that many seconds before retrying ([GitHub Docs][6])

This matters because:

* even with a big pool, you **should not** blast parallel threads beyond those limits
* PyGithub’s built-in delays help, but if you add your own concurrency, you must add your own queue/throttle too

## C) Retries (`retry`)

PyGithub exposes `retry` as:

* `int` or `urllib3.util.retry.Retry`
* defaulting to a built-in retry policy
* set `retry=None` to disable retries ([PyGithub][1])

### Best practice

* Keep retries enabled.
* For long-running jobs, pass an explicit `Retry` tuned for your environment (transient 502/503/504, plus respectful backoff).

## D) Connection pooling (`pool_size`)

PyGithub exposes `pool_size` directly. ([PyGithub][1])

### Best practice

* Single-threaded: defaults are usually fine.
* If you use threads: set `pool_size` ≈ your max in-flight requests (but keep concurrency conservative per GitHub guidance). ([GitHub Docs][6])

---

# 7) Recommended “profiles” you can bake into your project

### Profile 1 — Local scripts / CLI (safe default)

* `per_page=100`
* keep default throttles
* keep default retry

```python
from github import Auth, Github

def make_gh(token: str, *, base_url: str = "https://api.github.com") -> Github:
    return Github(
        auth=Auth.Token(token),
        base_url=base_url,
        per_page=100,
        # leave retry/throttles at defaults initially
    )
```

Defaults + knobs are as documented by PyGithub. ([PyGithub][1])

### Profile 2 — Read-heavy enumerator (repo inventory, “file scope”)

* `per_page=100`
* consider reducing `seconds_between_requests` slightly (but watch secondary limits)
* **do not** add high concurrency without a queue

GitHub explicitly warns about secondary limits and recommends serializing requests. ([GitHub Docs][5])

### Profile 3 — Bot / automation (writes, CI, labeling, PR comments)

* keep `seconds_between_writes >= 1.0`
* use **App Installation auth** rather than PAT when distributing to others

PyGithub describes installation auth + auto-refreshing tokens. ([PyGithub][4])

---

If you want the next deep dive, the natural continuation is **“Pagination + rate-limit discipline in practice”** (PaginatedList patterns, `totalCount`, when to prefer headers over `/rate_limit`, and how to structure a safe enumerator for “total file scope” without tripping secondary limits).

[1]: https://pygithub.readthedocs.io/en/latest/github.html "Main class: Github — PyGithub 0.1.dev1+g19e1c5032 documentation"
[2]: https://docs.github.com/en/enterprise-server%403.17/rest/quickstart "Quickstart for GitHub REST API - GitHub Enterprise Server 3.17 Docs"
[3]: https://docs.github.com/en/rest/issues/issues "REST API endpoints for issues - GitHub Docs"
[4]: https://pygithub.readthedocs.io/en/stable/examples/Authentication.html "Authentication — PyGithub 0.1.dev50+gecd47649e documentation"
[5]: https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api "Best practices for using the REST API - GitHub Docs"
[6]: https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api "Rate limits for the REST API - GitHub Docs"

## PyGithub deep dive 2 — Pagination + rate-limit discipline in practice

This is the “how to not get surprised” layer: what PyGithub’s `PaginatedList` is *really* doing, when `totalCount` is useful vs misleading, and how to build a **safe enumerator** (esp. “total file scope”) without tripping GitHub’s **secondary limits**.

---

# 1) GitHub pagination mechanics (what PyGithub abstracts)

For REST endpoints that paginate, GitHub uses the HTTP **`link` header** to advertise `rel="next"`, `rel="prev"`, `rel="first"`, `rel="last"` URLs. If results fit on one page (or the endpoint doesn’t paginate), `link` is omitted. Sometimes `last` is omitted when it **can’t be calculated**. ([GitHub Docs][1])

Also: `per_page` changes both response size *and* the `link` URLs (GitHub will propagate your `per_page` into the next/last links). ([GitHub Docs][1])

**Implication:** anything that depends on “knowing the last page” (including some “count” heuristics) can become unreliable when `last` isn’t present. ([GitHub Docs][1])

---

# 2) PyGithub `PaginatedList`: the user-facing patterns

PyGithub documents `PaginatedList` as the abstraction over REST + GraphQL pagination, with these supported patterns: ([pygithub.readthedocs.io][2])

### The “normal” way: iterate

```python
for repo in user.get_repos():
    ...
```

PyGithub transparently fetches pages as you iterate. ([pygithub.readthedocs.io][2])

### `totalCount`

```python
print(user.get_repos().totalCount)
```

Convenient, but you should treat it as a *hint* unless you’re in a context where it’s known to be accurate (more on this below). ([pygithub.readthedocs.io][2])

### Indexing / slicing / reversed

PyGithub documents:

```python
second_repo = user.get_repos()[1]
first_repos = user.get_repos()[:10]

for repo in reversed(user.get_repos()):
    ...
```

([pygithub.readthedocs.io][2])

### Page-level access: `get_page(n)` (REST-only)

If you truly need page access, PyGithub exposes `get_page`, but explicitly notes it’s **not supported for GraphQL lists**: ([pygithub.readthedocs.io][2])

```python
repos = user.get_repos()
assert repos.is_rest, "get_page not supported by the GraphQL API"
page0 = repos.get_page(0)
page3 = repos.get_page(3)
```

---

# 3) The big PaginatedList “gotcha”: slicing doesn’t necessarily save API calls

Even though slicing is supported, there’s a known pain point: slicing can still require fetching earlier pages you intended to skip. A PyGithub issue specifically calls out that slicing a large list at a high index can still make requests for earlier items. ([GitHub][3])

**Practical rule:**

* Use slicing for *small front slices* (`[:10]`) or convenience.
* For “start at page N”, use **`get_page(N)`** and then iterate forward yourself.

A safe “page streaming” helper:

```python
def iter_rest_paginated(plist):
    assert plist.is_rest, "Use direct iteration for GraphQL-backed lists"
    page = 0
    while True:
        chunk = plist.get_page(page)
        if not chunk:
            return
        yield from chunk
        page += 1
```

---

# 4) `totalCount`: when it’s meaningful vs when it lies to you

## A) Why `totalCount` can be tricky

GitHub pagination itself can omit `rel="last"` when it can’t be calculated. ([GitHub Docs][1])
So any logic that infers “how many pages exist” from `last` can break down in those cases.

## B) Search is special: hard caps + incomplete results

GitHub’s **Search REST API** is explicitly designed to help you find “the one result (or few results)” and has two constraints that matter here:

* **Up to 1,000 results per search** ([GitHub Docs][4])
* Responses can indicate **`incomplete_results=true`** when a query times out (i.e., you didn’t even get all matches it found so far). ([GitHub Docs][4])

So if you’re using `Github.search_*` or repo discussion-like search flows, treat `totalCount` as “UI-ish” and don’t build correctness logic around it.

---

# 5) Rate-limit discipline: header-first, `/rate_limit` sparingly

GitHub recommends checking rate limit status using the **rate limit response headers on *every* response**, and only calling `GET /rate_limit` when needed. ([GitHub Docs][5])

Key headers include: `x-ratelimit-limit`, `x-ratelimit-remaining`, `x-ratelimit-used`, `x-ratelimit-reset`, and `x-ratelimit-resource`. ([GitHub Docs][5])

Critically:

* `GET /rate_limit` **does not count against your primary rate limit**, but **can count against your secondary rate limit**, so don’t poll it. ([GitHub Docs][5])

### Error handling guidance (GitHub’s)

If you get rate limited (primary or secondary), GitHub recommends: ([GitHub Docs][6])

* If `retry-after` is present → wait that many seconds.
* Else if `x-ratelimit-remaining == 0` → wait until `x-ratelimit-reset`.
* Else → wait at least a minute; if it persists, exponential backoff.

And: **avoid concurrency** (queue/serialize), and **pause ≥1 second between mutative requests** to avoid secondary limits. ([GitHub Docs][6])

---

# 6) How to *actually access* headers / budgets in PyGithub

You have three useful surfaces:

### A) Client-level “last seen” budget

PyGithub exposes:

* `Github.rate_limiting -> (remaining, limit)`
* `Github.rate_limiting_resettime -> unix timestamp`
* `Github.get_rate_limit()` (calls `GET /rate_limit`) ([pygithub.readthedocs.io][7])

### B) Per-object raw headers (best for “what did this request cost?”)

Every `GithubObject` includes:

* `raw_headers` (dict)
* `etag`, `last_modified` (helpful for conditional requests/caching) ([pygithub.readthedocs.io][8])

### C) Bucket awareness (core vs search vs code_search)

PyGithub’s rate limit model includes buckets like `core`, `search`, `code_search`, etc. ([pygithub.readthedocs.io][9])
And GitHub Search has its own stricter limits:

* authenticated: **30 req/min** for search endpoints *except code search*
* code search: **10 req/min**
  ([GitHub Docs][4])

---

# 7) “Total file scope” enumerator without tripping limits

## Don’t use Search for this

Search is capped (1,000 results) and is rate-limited tightly (30/min; code search 10/min). ([GitHub Docs][4])
For “all files in a repo”, **use the Git Trees API**.

## Prefer Git Trees API over Contents API for enumeration

GitHub’s Contents API has an upper limit of **1,000 files per directory** and explicitly recommends using **Git Trees API** for more. ([GitHub Docs][10])

## Best approach: Git Trees `recursive=1` fast path

GitHub’s Trees endpoint:

* accepts a **tree SHA or a ref name (branch/tag)** ([GitHub Docs][11])
* supports `recursive` (any value enables recursion; omit to disable recursion) ([GitHub Docs][11])
* has a limit: **100,000 entries / 7 MB** for the recursive response; if exceeded you’ll see `truncated=true` and must walk subtrees non-recursively. ([GitHub Docs][11])

### PyGithub mapping you’ll use

`Repository.get_git_tree(sha: str, recursive=NotSet)` maps to `GET /repos/{owner}/{repo}/git/trees/{sha}`. ([pygithub.readthedocs.io][12])

### A safe, production-style implementation

This does:

1. Try the **single-call recursive tree**.
2. If `truncated`, fall back to **BFS over subtrees** (non-recursive tree calls).
3. Stay “rate-limit polite” by reading headers and sleeping when needed.

```python
from __future__ import annotations

import time
from collections import deque
from typing import Iterable

from github import Auth, Github
from github.Repository import Repository
from github.GithubException import RateLimitExceededException, GithubException


def _header(headers: dict, key: str) -> str | None:
    # PyGithub raw_headers keys are typically lowercased, but be tolerant.
    return (
        headers.get(key)
        or headers.get(key.lower())
        or headers.get(key.upper())
    )


def _sleep_from_headers(headers: dict) -> None:
    # Follow GitHub guidance: retry-after > x-ratelimit-reset > 60s fallback. :contentReference[oaicite:27]{index=27}
    ra = _header(headers, "retry-after")
    if ra:
        time.sleep(int(ra) + 1)
        return

    remaining = _header(headers, "x-ratelimit-remaining")
    reset = _header(headers, "x-ratelimit-reset")
    if remaining == "0" and reset:
        reset_ts = int(reset)
        time.sleep(max(0, reset_ts - int(time.time())) + 1)
        return

    time.sleep(60)


def list_repo_file_paths(repo: Repository, ref: str | None = None) -> list[str]:
    """
    Returns file paths (blobs) at `ref` (branch/tag). Uses Git Trees API.
    - Fast path: recursive tree
    - Fallback: subtree BFS if truncated
    """
    ref = ref or repo.default_branch

    # 1) Fast path: recursive tree
    tree = repo.get_git_tree(ref, recursive=True)  # recursion enabled by presence of param :contentReference[oaicite:28]{index=28}
    data = tree.raw_data  # available on all GithubObjects :contentReference[oaicite:29]{index=29}

    if not data.get("truncated"):
        return sorted(
            entry["path"]
            for entry in data.get("tree", [])
            if entry.get("type") == "blob"
        )

    # 2) Fallback: BFS over subtrees (non-recursive). Docs say omit `recursive` to disable. :contentReference[oaicite:30]{index=30}
    out: list[str] = []
    q: deque[tuple[str, str]] = deque()

    root = repo.get_git_tree(ref)  # non-recursive
    for entry in root.raw_data.get("tree", []):
        if entry.get("type") == "blob":
            out.append(entry["path"])
        elif entry.get("type") == "tree" and entry.get("sha"):
            q.append((entry["sha"], entry["path"] + "/"))

    while q:
        sha, prefix = q.popleft()
        try:
            sub = repo.get_git_tree(sha)  # sha is a tree sha
        except RateLimitExceededException as e:
            # When PyGithub throws, we may still have headers on the exception in some cases;
            # easiest is to sleep conservatively and retry.
            time.sleep(60)
            sub = repo.get_git_tree(sha)

        # If we have raw_headers, obey them (retry-after / reset / etc.)
        _sleep_from_headers(getattr(sub, "raw_headers", {}) or {})

        for entry in sub.raw_data.get("tree", []):
            p = prefix + entry["path"]
            if entry.get("type") == "blob":
                out.append(p)
            elif entry.get("type") == "tree" and entry.get("sha"):
                q.append((entry["sha"], p + "/"))

    return sorted(out)
```

**Why this is “safe”**

* Uses Trees API (meant for repository-wide structure). ([GitHub Docs][11])
* Avoids Search (1,000 cap + strict RPM). ([GitHub Docs][4])
* Serial traversal (aligns with “avoid concurrent requests”). ([GitHub Docs][6])
* Handles `truncated=true` exactly as GitHub recommends (fetch subtrees one at a time). ([GitHub Docs][11])

---

# 8) When to *actually* call `gh.get_rate_limit()` in PyGithub

Use it:

* at startup to log baseline bucket budgets (`core/search/code_search/graphql`) ([pygithub.readthedocs.io][9])
* when debugging confusing throttling behavior
* when you need cross-bucket visibility in one shot

Avoid calling it inside tight loops; GitHub explicitly warns it can still count toward secondary limits. ([GitHub Docs][5])

---

If you want the next deep dive, the natural one is **“Repo inventory & file scope”** (trees vs contents vs archives, how to pin to a commit SHA for determinism, and a “manifest contract” shape you can cache + diff across runs).

[1]: https://docs.github.com/rest/using-the-rest-api/using-pagination-in-the-rest-api "Using pagination in the REST API - GitHub Docs"
[2]: https://pygithub.readthedocs.io/en/latest/utilities.html "Utilities — PyGithub 0.1.dev1+g19e1c5032 documentation"
[3]: https://github.com/PyGithub/PyGithub/issues/2029 "Slicing long paginated lists does not reduce API calls · Issue #2029 · PyGithub/PyGithub · GitHub"
[4]: https://docs.github.com/en/rest/search/search "REST API endpoints for search - GitHub Docs"
[5]: https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api "Rate limits for the REST API - GitHub Docs"
[6]: https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api "Best practices for using the REST API - GitHub Docs"
[7]: https://pygithub.readthedocs.io/en/latest/github.html "Main class: Github — PyGithub 0.1.dev1+g19e1c5032 documentation"
[8]: https://pygithub.readthedocs.io/en/latest/github_objects.html "Github objects — PyGithub 0.1.dev1+g19e1c5032 documentation"
[9]: https://pygithub.readthedocs.io/en/latest/github_objects/RateLimit.html "RateLimit — PyGithub 0.1.dev1+g19e1c5032 documentation"
[10]: https://docs.github.com/en/rest/repos/contents "REST API endpoints for repository contents - GitHub Docs"
[11]: https://docs.github.com/rest/git/trees "REST API endpoints for Git trees - GitHub Docs"
[12]: https://pygithub.readthedocs.io/en/v2.4.0/github_objects/Repository.html "Repository — PyGithub 2.4.0 documentation"

## PyGithub deep dive 3 — Repo inventory & file scope

You can think of “repo inventory” as choosing **one of three primitives** depending on what you need:

1. **Git Trees**: fastest way to get the full file list (+ blob SHAs)
2. **Contents**: best for “read this file / list this directory” (not whole repo)
3. **Archives (zip/tar)**: best for “give me a whole snapshot I can process locally”

Below is the “best-in-class” way to use each, plus how to **pin to a commit SHA for determinism** and a **manifest contract** you can cache + diff.

---

# 1) Trees vs Contents vs Archives: what each is *for*

## A) Git Trees API: repo-wide file inventory (best default)

**What you get:** a tree of entries with `path`, `mode`, `type`, `sha`, and (for blobs) `size`, and you can request recursion. The API supports `type` values `blob`, `tree`, or `commit`, and file modes include normal files, executables, symlinks, and submodules. ([GitHub Docs][1])

**How recursion works + limits:**

* “Get a tree” accepts **a tree SHA1 or a ref name** (branch/tag name) for the tree. ([GitHub Docs][1])
* Setting `recursive` to *any value* enables recursion; omit it to disable recursion. ([GitHub Docs][1])
* If the response has `truncated: true`, GitHub tells you to **fetch non-recursively and walk subtrees one at a time**. ([GitHub Docs][1])
* Recursive responses are capped: **100,000 entries / 7 MB**, and exceeding that yields `truncated=true`. ([GitHub Docs][1])

**PyGithub surface:** `Repository.get_git_tree(sha, recursive=...)` calls `GET /repos/{owner}/{repo}/git/trees/{sha}` and returns a `GitTree`. ([pygithub.readthedocs.io][2])

✅ Use Trees when you need: **“total file scope”**, fast path/sha manifests, diffing inventories, selecting subsets by path rules.

---

## B) Contents API: directory listing + file reads (good, but not for whole-repo inventory)

**What you get:** for a directory, an array of items; for a file, metadata + base64 content (for small files) and a `download_url`. The Contents API is explicitly “Base64 encoded content.” ([GitHub Docs][3])

**Hard constraints that matter for inventory:**

* Directory listing has an **upper limit of 1,000 files per directory**; GitHub explicitly says: *if you need more, use the Git Trees API*. ([GitHub Docs][3])
* It’s not a recursive listing API; GitHub notes: “to get recursively, you can recursively get the tree.” ([GitHub Docs][3])
* `download_url` links **expire** and are meant to be used once; use Contents to fetch a fresh one. ([GitHub Docs][3])
* File size behavior:

  * ≤1 MB: fully supported
  * 1–100 MB: only certain media types; `content` may be empty for object format; use raw
  * > 100 MB: endpoint not supported ([GitHub Docs][3])
* Submodules have quirks: when listing a directory, submodules may come back with type `"file"` for backwards compatibility (with plans to change in a future major API version). ([GitHub Docs][3])

**PyGithub surface:** `Repository.get_contents(path, ref=...)` calls `GET /repos/{owner}/{repo}/contents/{path}` and returns a `ContentFile` or list of them. ([pygithub.readthedocs.io][2])
There’s also `get_dir_contents(path, ref=...)` which calls the same endpoint and returns a list. ([pygithub.readthedocs.io][2])

✅ Use Contents when you need: **read a small number of files**, list a specific directory, or get a fresh `download_url` for a file.

🚫 Avoid Contents as your “whole repo inventory” mechanism.

---

## C) Archives (zipball/tarball): “give me a whole snapshot”

**What you get:** a tar/zip snapshot of the code (not git history).

GitHub’s “Source code archives” doc is very explicit:

* You can download a snapshot of a **branch, tag, or specific commit**.
* Snapshots are generated by `git archive` and **do not contain full repository history**. ([GitHub Docs][4])

**REST archive endpoints (redirect behavior):**

* `GET /repos/{owner}/{repo}/tarball/{ref}` and `/zipball/{ref}` return a **redirect URL**; you must follow redirects or use the `Location` header to fetch the archive. ([GitHub Docs][3])
* For private repos, the redirected links **expire after five minutes**. ([GitHub Docs][3])
* If you omit `ref`, GitHub uses the repo’s default branch (usually `main`). ([GitHub Docs][3])

**PyGithub surface:** `Repository.get_archive_link(archive_format, ref=...)` calls `GET /repos/{owner}/{repo}/{archive_format}/{ref}` and returns a string. ([pygithub.readthedocs.io][2])

✅ Use Archives when you need: **bulk local processing** (linters, scanners), or you want to avoid N API calls for content.

---

# 2) Determinism: how to pin to a commit SHA (and why)

Branches/tags can move; commit IDs are stable. GitHub explicitly warns:

* An archive of a **commit ID** will always have the same file contents (assuming commit still exists and repo name hasn’t changed).
* Branches/tags can move, so future downloads may differ.
* Compression settings can change (bytes may differ even if extracted contents match).
* For reproducibility, GitHub recommends using the **archives REST API with a commit ID for `:ref`**. ([GitHub Docs][4])

So the “deterministic pattern” is:

1. Start from an input ref (branch/tag)
2. Resolve it to a **commit SHA**
3. Resolve that commit to a **tree SHA**
4. Inventory against the tree SHA (Trees) or pass commit SHA (Contents/Archives)
5. Store the resolved SHAs in your manifest

### PyGithub building blocks

* Resolve a git ref: `repo.get_git_ref(ref)` → calls `GET /repos/{owner}/{repo}/git/refs/{ref}` ([pygithub.readthedocs.io][2])
* Read the git commit object: `repo.get_git_commit(sha)` → calls `GET /repos/{owner}/{repo}/git/commits/{sha}` ([pygithub.readthedocs.io][2])
* Get the tree: `repo.get_git_tree(tree_sha, recursive=...)` ([pygithub.readthedocs.io][2])
* For Contents, the `ref` query parameter is explicitly “commit/branch/tag” and defaults to default branch if omitted. ([GitHub Docs][3])

---

# 3) Best-in-class “inventory pipeline” with Trees (fast, diffable, cacheable)

## Step 0: Resolve `ref → commit_sha → tree_sha`

```python
def resolve_ref(repo, ref: str) -> tuple[str, str]:
    # ref string for GitHub “git refs” is typically like "heads/main" or "tags/v1.2.3"
    git_ref = repo.get_git_ref(ref)  # GET /repos/{owner}/{repo}/git/refs/{ref} :contentReference[oaicite:24]{index=24}
    commit_sha = git_ref.object.sha

    git_commit = repo.get_git_commit(commit_sha)  # GET /repos/{owner}/{repo}/git/commits/{sha} :contentReference[oaicite:25]{index=25}
    tree_sha = git_commit.tree.sha

    return commit_sha, tree_sha
```

## Step 1: Fetch tree recursively (single call) and handle truncation

* Recursive tree is ideal, but may return `truncated=true` if you exceed **100k entries / 7MB**; if truncated, walk subtrees non-recursively. ([GitHub Docs][1])

This is the same fallback strategy we used in the prior deep dive; the point here is: **Trees are your canonical inventory.**

---

# 4) “Manifest contract” you can cache + diff (recommended shape)

A good contract has two goals:

1. **Deterministic identity** of “what repo snapshot did we inventory?”
2. A **stable, sortable file table** that can be hashed and diffed

### Proposed JSON schema (v1)

```json
{
  "schema_version": 1,
  "repo": {
    "full_name": "org/name",
    "id": 123456
  },
  "requested": {
    "ref": "heads/main" 
  },
  "resolved": {
    "commit_sha": "…",
    "tree_sha": "…"
  },
  "inventory": {
    "method": "git_trees_recursive|git_trees_bfs",
    "truncated": false
  },
  "entries": [
    {"path":"src/a.py","type":"blob","mode":"100644","sha":"…","size":1234},
    {"path":"subdir","type":"tree","mode":"040000","sha":"…"},
    {"path":"vendor/lib","type":"commit","mode":"160000","sha":"…"}  // submodule gitlink
  ],
  "summary": {
    "blobs": 12034,
    "trees": 532,
    "submodules": 3
  },
  "digest": {
    "algo": "sha256",
    "hex": "…" 
  }
}
```

### Why these fields

* `type`/`mode` matter because trees can represent **symlinks and submodules** (mode `120000` symlink-blob, mode `160000` submodule commit). ([GitHub Docs][1])
* `sha` on blobs is the **content identity**; it’s the lever for caching/dedup and “did this file change?” diffs. (Trees include these SHAs in responses. ([GitHub Docs][1]))

### Canonicalization rules (so your `digest` is stable)

* Sort `entries` by `path` (byte/lex order)
* Emit only stable fields: `path,type,mode,sha,size`
* Serialize with stable key ordering (e.g., `json.dumps(..., sort_keys=True, separators=(",", ":"))`)

### Diffing across runs

Given two manifests:

* `added = paths_new - paths_old`
* `removed = paths_old - paths_new`
* `modified = {p | p in both and sha changed}`
* Optional rename detection: match by `sha` for blobs (same sha appears at a new path)

---

# 5) Content retrieval: the “right” way once you have a manifest

Once you have a tree manifest, you should **not** use Contents recursively. You already have blob SHAs.

Two typical strategies:

## A) Fetch specific files by path (Contents) — good for small reads

* `repo.get_contents(path, ref=commit_sha)` is simplest, and the API supports raw/object media types for larger files. ([GitHub Docs][3])
* Keep in mind the Contents size rules (1–100MB: raw/object; >100MB unsupported). ([GitHub Docs][3])

## B) Fetch blobs by SHA (Git Blobs) — best for caching/dedup

PyGithub exposes `repo.get_git_blob(sha)` → `GET /repos/{owner}/{repo}/git/blobs/{sha}`. ([pygithub.readthedocs.io][2])
This is ideal because your cache key is literally the content SHA.

---

# 6) When to choose which approach (rule of thumb)

* **Need “all file paths + diff over time”** → Trees manifest (store commit_sha + tree_sha)
* **Need “read a few small files”** → Contents (`get_contents`)
* **Need “process whole repo locally”** → Archives pinned to **commit SHA** for reproducibility ([GitHub Docs][4])
* **Need “incremental content cache”** → Trees manifest + blob cache keyed by blob SHA

---

If you want the next deep dive, the natural continuation is: **“Blob/content retrieval strategies + caching”** (Contents raw vs object, blobs-by-sha caches, ETag/If-None-Match patterns, and a clean “RepoSnapshot” abstraction that exposes `list_paths() / open_bytes(path) / diff(other)` with deterministic behavior).

[1]: https://docs.github.com/rest/git/trees "REST API endpoints for Git trees - GitHub Docs"
[2]: https://pygithub.readthedocs.io/en/v1.57/github_objects/Repository.html "Repository — PyGithub 1.57 documentation"
[3]: https://docs.github.com/en/rest/repos/contents "REST API endpoints for repository contents - GitHub Docs"
[4]: https://docs.github.com/en/repositories/working-with-files/using-files/downloading-source-code-archives "Downloading source code archives - GitHub Docs"

## PyGithub deep dive 4 — Blob/content retrieval strategies + caching

At this point you already have the “north-star” inventory primitive: **Trees → a deterministic manifest with `path → blob_sha`**.

Now the goal is:

* **fetch bytes cheaply**
* **cache aggressively**
* **stay rate-limit polite**
* **keep everything deterministic** (commit SHA pinned)

---

# 1) Retrieval primitives for bytes: pick the right one per use case

## A) Git Blobs API (best when you already have blob SHAs)

If your inventory step used Trees, you already have **blob SHAs**, which makes content retrieval naturally **content-addressed**.

GitHub’s “Get a blob” endpoint:

* default response is JSON with `content` **Base64 encoded**
* supports `application/vnd.github.raw+json` to return **raw blob data**
* supports blobs up to **100 MB** ([GitHub Docs][1])

**Why it’s the best cache key:** the blob SHA is the *identity* of the content. You can store bytes once and reuse across:

* multiple paths (renames)
* multiple commits (unchanged files)
* multiple repos (forks / duplicated vendored files), if you want cross-repo dedupe

## B) Contents API (best for path-based reads, directories, symlinks)

GitHub’s “Get repository content” endpoint supports custom media types: ([GitHub Docs][2])

* `application/vnd.github.raw+json` → raw file contents (files + symlinks)
* `application/vnd.github.object+json` → consistent object format (dir listing becomes `{entries: [...]}`)
* HTML rendering media type

Important constraints: ([GitHub Docs][2])

* directory listing capped at **1,000 files**; for recursion use Trees
* **download_url expires** (meant to be used once)
* file size:

  * ≤ 1 MB: all features supported
  * 1–100 MB: only `raw` or `object` are supported; in `object` mode `content` is empty and `encoding` is `"none"`
  * > 100 MB: not supported

So for “open_bytes(path)”:

* **prefer blob-by-sha**, but
* Contents + `raw` is the clean fallback when you only know a path (or you need symlink semantics).

## C) Archives (zip/tar) (best for bulk local processing)

Archives are great when you plan to read *many* files locally (static analysis, scanning, indexing). (You already have this in the prior deep dive.)

---

# 2) Rate-limit posture: the single biggest lever is conditional requests

GitHub recommends using **ETag / Last-Modified** values to make conditional `GET` requests; if unchanged you get `304 Not Modified`, and **that does not count against your primary rate limit** (when correctly authorized). ([GitHub Docs][3])

Also:

* conditional requests for unsafe methods (`POST/PUT/PATCH/DELETE`) are generally not supported unless specifically noted. ([GitHub Docs][3])

PyGithub exposes the raw materials you need:

* every `GithubObject` has `raw_headers`, `etag`, `last_modified` (and `last_modified_datetime`) ([pygithub.readthedocs.io][4])
* you can drop down to the underlying `Requester` to send requests with custom headers ([pygithub.readthedocs.io][4])

---

# 3) Two caching styles that work well together

## Style 1: Content-addressed cache (blob SHA → bytes)

This is your “workhorse cache” if you’re doing repo scanning.

**Cache key:** `blob_sha`
**Cache value:** raw bytes (+ optional metadata like size)

Pros:

* deterministic, immutable
* no staleness problems
* extremely diff-friendly

You mostly avoid conditional requests entirely because **if the blob SHA is unchanged, the bytes are unchanged**.

## Style 2: HTTP conditional cache (URL → ETag/Last-Modified → body)

This is best for:

* list endpoints (issues, PRs, workflows)
* path endpoints (contents by path)
* anything you can’t key by blob SHA

**Cache key:** normalized request identity (method + URL + query + selected headers like Accept)
**Cache value:** response body + ETag/Last-Modified + relevant headers

⚠️ Practical gotcha: authenticated `304` responses may omit pagination `Link` headers in some cases, which complicates “recomputing paging” from a 304 response. If you do conditional caching of paginated lists, plan to reuse the previously cached `Link` headers or avoid conditional requests for those list pages. ([GitHub][5])

---

# 4) Doing conditional requests in PyGithub (the “Requester” pattern)

High-level PyGithub methods generally don’t let you pass `If-None-Match` directly (this has been a long-standing pain point). ([GitHub][6])
So the “best-in-class” pattern is:

* use normal PyGithub calls when you don’t care about conditional caching
* use `obj.requester.requestJsonAndCheck(..., headers=...)` when you *do*

PyGithub’s `Requester.requestJsonAndCheck()` explicitly supports a `headers` argument and returns `(headers, JSON_response)`. ([pygithub.readthedocs.io][7])

Example (conditional GET):

```python
def conditional_get_json(requester, url: str, *, etag: str | None):
    headers = {}
    if etag:
        headers["If-None-Match"] = etag

    resp_headers, data = requester.requestJsonAndCheck("GET", url, headers=headers)
    # If unchanged, GitHub returns 304 with empty body; you must handle that at your call site.
    return resp_headers, data
```

And for **streaming raw bytes**, `Requester.getStream()` is the cleanest primitive (returns status + headers + an iterator of chunks). ([pygithub.readthedocs.io][7])

---

# 5) Best-in-class `RepoSnapshot` abstraction (deterministic + cacheable)

You want an object that represents “a repo at a commit SHA” and exposes:

* `list_paths() -> list[str]` (sorted)
* `open_bytes(path) -> bytes`
* `diff(other) -> RepoDiff`

### Contract assumptions

* snapshot is pinned to a **commit SHA**
* snapshot owns an immutable manifest: `path -> entry` where entry includes blob SHA and file mode/type

### Data structures

```python
from dataclasses import dataclass
from typing import Dict, Iterable, Optional

@dataclass(frozen=True)
class ManifestEntry:
    path: str
    type: str        # "blob" | "tree" | "commit" (submodule)
    mode: str        # e.g. "100644", "100755", "120000" (symlink), "160000" (submodule)
    sha: str         # blob/tree sha
    size: int | None

@dataclass(frozen=True)
class RepoDiff:
    added: list[str]
    removed: list[str]
    modified: list[str]   # same path, different blob sha
    # optional: renamed detection by matching sha moved paths
```

### Cache interfaces

Keep it simple and explicit:

* `ManifestStore.get(repo_full_name, commit_sha) -> dict[path, ManifestEntry] | None`
* `ManifestStore.put(...)`
* `BlobStore.get(blob_sha) -> bytes | None`
* `BlobStore.put(blob_sha, bytes)`

### RepoSnapshot core logic

```python
import base64

class RepoSnapshot:
    def __init__(self, repo, *, commit_sha: str, manifest: Dict[str, ManifestEntry], blob_store):
        self.repo = repo
        self.commit_sha = commit_sha
        self.manifest = manifest
        self.blob_store = blob_store

    def list_paths(self) -> list[str]:
        return sorted([p for p, e in self.manifest.items() if e.type == "blob"])

    def open_bytes(self, path: str) -> bytes:
        e = self.manifest.get(path)
        if not e:
            raise KeyError(path)
        if e.type != "blob":
            raise TypeError(f"{path} is {e.type}, not a file blob (mode={e.mode})")

        cached = self.blob_store.get(e.sha)
        if cached is not None:
            return cached

        # Default path: Git Blob API (base64 JSON) → decode.
        blob = self.repo.get_git_blob(e.sha)  # wrapper around Git Blobs endpoint
        if getattr(blob, "encoding", None) == "base64":
            data = base64.b64decode(blob.content)
        else:
            # defensive; in practice GitHub blob endpoint’s JSON uses base64 content by default :contentReference[oaicite:11]{index=11}
            data = blob.content if isinstance(blob.content, (bytes, bytearray)) else blob.content.encode()

        self.blob_store.put(e.sha, data)
        return data

    def diff(self, other: "RepoSnapshot") -> RepoDiff:
        a = self.manifest
        b = other.manifest

        pa = {p: e.sha for p, e in a.items() if e.type == "blob"}
        pb = {p: e.sha for p, e in b.items() if e.type == "blob"}

        added = sorted(set(pb) - set(pa))
        removed = sorted(set(pa) - set(pb))
        modified = sorted([p for p in set(pa) & set(pb) if pa[p] != pb[p]])

        return RepoDiff(added=added, removed=removed, modified=modified)
```

### When to use “raw blob” or “contents raw”

If you want to avoid base64 decode overhead or stream large blobs:

* Git Blobs API supports `application/vnd.github.raw+json` for raw blob data ([GitHub Docs][1])
* Contents API supports `application/vnd.github.raw+json` for raw file contents and is the recommended approach for 1–100MB files ([GitHub Docs][2])
* `Requester.getStream()` is the “download bytes” primitive in PyGithub ([pygithub.readthedocs.io][7])

In practice for code repos:

* use blob base64 for simplicity for small/medium files
* use streaming raw for “rare big file” edge cases

---

# 6) A pragmatic “best-in-class” policy set

**Default (fast + deterministic):**

* Inventory via Trees
* Fetch bytes via blob SHA (content-addressed cache)
* Avoid Contents except for “path-only” situations

**Add conditional requests where it pays:**

* for repo metadata endpoints you poll repeatedly (repo view, refs, releases, workflows)
* for directory listings / “is this file unchanged?” checks when you don’t already have a blob SHA

GitHub explicitly encourages conditional requests and notes 304s don’t count against your primary limit (when authorized). ([GitHub Docs][3])

---

If you want the next deep dive, the natural continuation is **“Higher-level collaboration APIs with caching discipline”**: issues/PRs/discussions where (a) pagination + conditional requests interact, (b) search endpoints have hard caps, and (c) a “delta sync” pattern avoids rescanning everything.

[1]: https://docs.github.com/enterprise-cloud%40latest/rest/git/blobs "REST API endpoints for Git blobs - GitHub Enterprise Cloud Docs"
[2]: https://docs.github.com/en/rest/repos/contents "REST API endpoints for repository contents - GitHub Docs"
[3]: https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api "Best practices for using the REST API - GitHub Docs"
[4]: https://pygithub.readthedocs.io/en/latest/github_objects.html "Github objects — PyGithub 0.1.dev1+g19e1c5032 documentation"
[5]: https://github.com/orgs/community/discussions/75348 "REST API: Pagination Headers in Conditional Requests · community · Discussion #75348 · GitHub"
[6]: https://github.com/PyGithub/PyGithub/issues/536?utm_source=chatgpt.com "ETag not supported to make conditional requests through ..."
[7]: https://pygithub.readthedocs.io/en/latest/utilities.html "Utilities — PyGithub 0.1.dev1+g19e1c5032 documentation"

## PyGithub deep dive 5 — Higher-level collaboration APIs with caching discipline

This layer is about syncing **issues / PRs / discussions + their comment streams** in a way that:

* doesn’t rescan the whole repo every run (**delta sync**)
* avoids getting wrecked by **pagination churn**
* uses **conditional requests** where they actually help
* treats **Search** as a scalpel, not a primary data plane

---

# 1) Core constraints you design around

## Pagination is “moving ground”

GitHub paginates list endpoints and advertises next/prev/last via the `link` header; if everything fits in one page, the `link` header is omitted, and `rel="last"` might be omitted if it can’t be calculated. ([GitHub Docs][1])
So your sync logic should **not** assume stable page boundaries or “known last page.”

## Conditional requests are real and worth it

GitHub recommends using `etag` / `last-modified` for conditional `GET`s; if unchanged you get `304 Not Modified`, and that **does not count against primary rate limit** when you’re correctly authorized. ([GitHub Docs][2])

## Search has hard caps + low RPM

Search is explicitly capped at **up to 1,000 results per search**, and it has a custom rate limit: **30 req/min** for most search endpoints, and **10 req/min** for code search (and code search requires auth). Timeouts can yield `incomplete_results=true`. ([GitHub Docs][3])
So search is great for “find me X,” but it’s a poor substrate for “sync everything.”

---

# 2) The object model reality: PRs are issues (and comments are shared)

Two important “model facts” that shape your API choices:

* GitHub’s REST API considers **every pull request an issue**, but not every issue is a PR; Issues endpoints may return both, and you can identify PRs by a `pull_request` key. ([GitHub Docs][4])
* Many “shared” PR actions (labels, assignees, milestones) are managed via **Issues endpoints**, not Pull Requests endpoints. ([GitHub Docs][5])
* Likewise, “issue comments” cover comments on issues *and* PRs; review comments are separate. ([GitHub Docs][6])

**Design implication:** For a unified “conversation stream” you usually sync:

* Issues (includes PR-issues) + issue comments
* Pull requests (PR-specific fields) + review comments
* Discussions + discussion comments (GraphQL)

---

# 3) Delta sync patterns that avoid rescanning everything

## Pattern A — Use `since` where it exists (best)

The repo issues list supports `since` meaning “only show results last updated after this time” (ISO8601). ([GitHub Docs][4])
Issue comments and PR review comments also support `since`. ([GitHub Docs][6])

### Issues delta (REST)

List repo issues:

* `sort`: `created|updated|comments`
* `direction`: `asc|desc`
* `since`: “last updated after …” ([GitHub Docs][4])

A practical delta loop in PyGithub:

```python
from datetime import datetime, timezone, timedelta

def sync_issues(repo, *, since_dt: datetime):
    # overlap guards against same-timestamp boundary effects
    since_dt = since_dt - timedelta(seconds=30)

    for issue in repo.get_issues(
        state="all",
        sort="updated",
        direction="asc",
        since=since_dt,
    ):
        # issue.pull_request is present for PRs returned from Issues endpoints
        is_pr = issue.pull_request is not None
        upsert_issue(issue, is_pr=is_pr)

        # advance watermark based on issue.updated_at
        yield issue.updated_at
```

PyGithub exposes `Repository.get_issues(... since: datetime ...)` and calls `GET /repos/{owner}/{repo}/issues`. ([pygithub.readthedocs.io][7])

**Why `direction="asc"` for delta?**
You can safely move your watermark forward as you process, and if the job is interrupted you restart near the last checkpoint.

## Pattern B — “Sort updated desc and stop early” when `since` is missing

The Pull Requests list endpoint supports sorting (including `updated`) but does **not** have a `since` filter. ([GitHub Docs][5])

So you do:

1. list PRs ordered by most-recently-updated
2. stop paging once `updated_at < watermark`

```python
def sync_pulls(repo, *, since_dt: datetime):
    for pr in repo.get_pulls(state="all", sort="updated", direction="desc"):
        if pr.updated_at < since_dt:
            break
        upsert_pull(pr)
```

PyGithub exposes `Repository.get_pulls(state, sort, direction, base, head)` calling `GET /repos/{owner}/{repo}/pulls`. ([pygithub.readthedocs.io][7])
The REST endpoint’s sort options include `created`, `updated`, `popularity`, `long-running`. ([GitHub Docs][5])

## Pattern C — Repo-level comment streams (huge win vs per-issue crawling)

### Issue comments (issues + PR conversation comments)

“List issue comments for a repository”:

* default ordering is ascending ID
* supports `sort` (`created|updated`), `direction`, and `since` (“last updated after …”) ([GitHub Docs][6])

PyGithub gives you `repo.get_issues_comments(sort, direction, since)`. ([pygithub.readthedocs.io][7])

```python
def sync_issue_comments(repo, *, since_dt: datetime):
    for c in repo.get_issues_comments(sort="updated", direction="asc", since=since_dt):
        upsert_issue_comment(c)
        yield c.updated_at
```

### Review comments (diff-line comments)

“List review comments in a repository”:

* default ordering ascending by ID
* supports `sort`, `direction`, `since` ([GitHub Docs][8])

PyGithub gives you `repo.get_pulls_review_comments(sort, direction, since)` (and related helpers). ([pygithub.readthedocs.io][7])

```python
def sync_review_comments(repo, *, since_dt: datetime):
    for c in repo.get_pulls_review_comments(sort="updated", direction="asc", since=since_dt):
        upsert_review_comment(c)
        yield c.updated_at
```

**Why repo-level streams are “best-in-class”:**
You avoid N×M traversal (“for each issue, list comments”), and you get a single monotonic watermark per stream.

---

# 4) Discussions delta sync (GraphQL) with cursor paging + stop-early

Discussions are first-class in GitHub’s GraphQL API. The docs show:

* `Repository.discussions(... orderBy: {field: UPDATED_AT, direction: DESC})`
* cursor pagination via `pageInfo { endCursor hasNextPage }`
* each discussion has `updatedAt` ([GitHub Docs][9])

### Strategy

* Query discussions ordered by `UPDATED_AT DESC`
* Process until `updatedAt < watermark`, then stop (no need to read older pages)
* For each discussion whose `updatedAt` moved, fetch its comments (also cursor-paged) only if you need them

### PyGithub execution surface

PyGithub supports GraphQL via `Requester.graphql_query()` (and related helpers). ([pygithub.readthedocs.io][10])

Example query loop:

```python
DISCUSSIONS_Q = """
query($owner:String!, $name:String!, $first:Int!, $after:String) {
  repository(owner:$owner, name:$name) {
    discussions(first:$first, after:$after, orderBy:{field:UPDATED_AT, direction:DESC}) {
      pageInfo { endCursor hasNextPage }
      nodes { id number title updatedAt url }
    }
  }
}
"""

def sync_discussions(repo, *, since_dt):
    owner = repo.owner.login
    name = repo.name
    after = None
    first = 50

    while True:
        headers, data = repo.requester.graphql_query(DISCUSSIONS_Q, {
            "owner": owner, "name": name, "first": first, "after": after
        })
        conn = data["data"]["repository"]["discussions"]
        for d in conn["nodes"]:
            updated = d["updatedAt"]  # ISO8601 string
            if updated < since_dt.isoformat().replace("+00:00","Z"):
                return
            upsert_discussion(d)

        if not conn["pageInfo"]["hasNextPage"]:
            return
        after = conn["pageInfo"]["endCursor"]
```

The shape of `Repository.discussions`, its `orderBy` default, and pagination via `pageInfo`/cursors are in GitHub’s Discussions GraphQL guide. ([GitHub Docs][9])

---

# 5) Where conditional requests actually help (and where they don’t)

## Works great for:

* “single resource” reads (one issue, one PR, one discussion node)
* “first page” probes (e.g., “anything changed since last run?”)

GitHub recommends conditional requests broadly and explains the 304/primary-limit benefit. ([GitHub Docs][2])

## Often *not* worth it for deep pagination on “newest-first” lists

If you page through `sort=updated&direction=desc`, new items arriving at the top can shift page boundaries, meaning ETags for page 2/3/4 churn even if the underlying older items didn’t change. The win is usually smaller than just doing **stop-early delta sync**.

**Best practice combo:**

* Use **delta** (`since` or stop-early) as the primary reducer
* Use **ETag** as a “cheap no-op detector” before you do any work

---

# 6) “Do we ever use Search?”

Yes—but only when the question is inherently “searchy” and bounded.

GitHub is explicit that search is designed to find a specific item and provides up to 1,000 results per search. It also documents the custom rate limits and `incomplete_results` timeout behavior. ([GitHub Docs][3])

**Good uses**

* find a small set of PRs/issues matching a qualifier (`label:`, `author:`, etc.)
* targeted code search for a handful of queries (accepting the 10/min limit)

**Bad uses**

* “sync all issues/PRs in a large org” (you’ll hit 1,000 caps, timeouts, and low RPM)

---

# 7) A clean “SyncState contract” (what you persist)

For each repo, persist:

* `issues_updated_watermark`
* `pulls_updated_watermark`
* `issue_comments_updated_watermark`
* `review_comments_updated_watermark`
* `discussions_updated_watermark`
* optional: `etag_first_page_{stream}` for “cheap no-op detector”

And adopt two safety rules:

1. **overlap window** (rewind `since` by e.g. 30–120s)
2. **idempotent upserts** keyed by stable IDs

---

If you want the next deep dive, the natural continuation is: **“Write paths + mutation safety”** (creating/updating issues, commenting, labeling, closing PRs, discussion mutations) including (a) secondary-limit-safe write pacing, (b) idempotency keys / retry semantics, and (c) how to structure “work queues” so you never spam endpoints. GitHub’s best practices explicitly call out webhook-first designs and careful retry/backoff. ([GitHub Docs][2])

[1]: https://docs.github.com/rest/using-the-rest-api/using-pagination-in-the-rest-api "Using pagination in the REST API - GitHub Docs"
[2]: https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api "Best practices for using the REST API - GitHub Docs"
[3]: https://docs.github.com/en/rest/search/search "REST API endpoints for search - GitHub Docs"
[4]: https://docs.github.com/en/rest/issues/issues "REST API endpoints for issues - GitHub Docs"
[5]: https://docs.github.com/en/rest/pulls/pulls "REST API endpoints for pull requests - GitHub Docs"
[6]: https://docs.github.com/rest/issues/comments "REST API endpoints for issue comments - GitHub Docs"
[7]: https://pygithub.readthedocs.io/en/latest/github_objects/Repository.html "Repository — PyGithub 0.1.dev1+g19e1c5032 documentation"
[8]: https://docs.github.com/en/rest/pulls/comments "REST API endpoints for pull request review comments - GitHub Docs"
[9]: https://docs.github.com/en/graphql/guides/using-the-graphql-api-for-discussions "Using the GraphQL API for Discussions - GitHub Docs"
[10]: https://pygithub.readthedocs.io/en/stable/utilities.html "Utilities — PyGithub 0.1.dev50+gecd47649e documentation"

## PyGithub deep dive 6 — Write paths + mutation safety

When you start *creating/updating* issues/PRs/discussions, you’re in the danger zone where (a) **secondary rate limits** matter more than primary limits, (b) naive retries can create **duplicate side effects**, and (c) you want a **single-writer queue** that makes “exactly-once-ish” semantics realistic.

---

# 1) The rules of the road (GitHub’s write guidance)

GitHub’s REST best-practices are blunt:

* **Avoid polling**; prefer **webhooks**. ([GitHub Docs][1])
* **Avoid concurrent requests**; make requests **serially** and “implement a queue system.” ([GitHub Docs][1])
* For lots of mutations (`POST/PATCH/PUT/DELETE`), **wait at least 1 second between each request** to avoid secondary limits. ([GitHub Docs][1])
* If you hit rate limiting, follow a strict backoff ladder using `retry-after` / `x-ratelimit-reset`, then exponential backoff; and **continuing while rate limited can get your integration banned.** ([GitHub Docs][1])

Secondary limits are also quantified:

* **≤ 100 concurrent requests** total (shared across REST + GraphQL). ([GitHub Docs][2])
* Point system: most REST `GET` cost 1 point; most REST writes cost **5 points**; GraphQL mutations cost **5 points**. ([GitHub Docs][2])
* Content creation limits: roughly **≤ 80 content-generating requests/min** and **≤ 500/hour** (plus endpoint-specific lower caps). ([GitHub Docs][2])
* There’s **no way to check secondary-limit status** preemptively—you only discover it by being throttled. ([GitHub Docs][2])

---

# 2) PyGithub configuration that aligns with the rules (don’t fight the library)

PyGithub’s `Github(...)` already defaults to:

* `seconds_between_requests=0.25`
* `seconds_between_writes=1.0`
* a default retry policy (`GithubRetry(total=10, ...)`) ([pygithub.readthedocs.io][3])

Those defaults line up with GitHub’s “pause between mutative requests” guidance. ([GitHub Docs][1])

**Recommendation for write-heavy automation:** keep `seconds_between_writes >= 1.0` and *still* enforce “single writer” at the application level (queue), because secondary limits are also about concurrency. ([GitHub Docs][1])

---

# 3) Retry semantics: what you should retry (and how)

GitHub’s official backoff ladder for rate-limit errors is:

1. If `retry-after` is present → wait that many seconds
2. Else if `x-ratelimit-remaining == 0` → wait until `x-ratelimit-reset`
3. Else wait at least **1 minute**, then exponential backoff, and stop after N retries ([GitHub Docs][1])

And again: don’t keep hammering while limited. ([GitHub Docs][1])

PyGithub gives you:

* `RateLimitExceededException` (raised when rate limit exceeded) ([pygithub.readthedocs.io][4])
* Exceptions carry `headers` (so you can read `retry-after`, `x-ratelimit-reset`, etc.). ([pygithub.readthedocs.io][4])

**What to retry safely**

* **GET / list** operations: safe to retry (idempotent reads)
* **Writes**: only safe to retry if *you* make them idempotent (next section)

---

# 4) Idempotency “for real” with GitHub: don’t assume the server will dedupe

GitHub’s REST best-practices don’t provide a universal “Idempotency-Key” mechanism; so the safe approach is:

### A) Prefer idempotent *shapes* of writes

Use “set to desired state” APIs where possible:

* Labels: prefer `set_labels()` (PUT exact set) over repeated `add_to_labels()` calls when you want deterministic outcomes. ([pygithub.readthedocs.io][5])
* Close an issue/PR: use `.edit(state="closed")` (PATCH) only if it’s not already closed. ([pygithub.readthedocs.io][5])
* Merge PR: check `.is_merged()` before attempting `.merge()` to avoid duplicate attempts. ([pygithub.readthedocs.io][5])

### B) For non-idempotent writes (comments), make them idempotent yourself

Creating comments is a classic “double-post risk” (network failure after server accepted it).

PyGithub exposes comment creation:

* `Issue.create_comment()` via `POST /repos/{owner}/{repo}/issues/{issue_number}/comments` ([pygithub.readthedocs.io][5])

**Idempotency playbook for comments**

* Generate a stable operation id (UUID or deterministic hash)
* Embed it in the comment body as an invisible marker (e.g., `<!-- op:... -->`)
* On retry, **list the last N comments** and check for that marker before posting again

  * (You’ll usually do this with a small bounded read, not Search)

### C) For GraphQL mutations (Discussions), use `clientMutationId` + your own marker

GitHub’s Discussions API is primarily accessed via GraphQL; it can “get, create, edit, and delete discussion posts.” ([GitHub Docs][6])
GraphQL mutations commonly accept/return `clientMutationId` (“a unique identifier for the client performing the mutation”), including `addDiscussionComment`. ([GitHub Docs][7])

That helps you **correlate** attempts; it does not magically guarantee dedupe, so still use the same “marker in body + reconcile” approach for comment-like mutations.

---

# 5) The “single writer + outbox” work queue (so you never spam endpoints)

GitHub explicitly recommends serial requests and a queue system to avoid secondary limits. ([GitHub Docs][1])
So structure your write path like this:

### Operation model

Each queued write should include:

* `op_id` (your idempotency key)
* `kind` (create_issue / edit_issue / set_labels / comment / close_pr / merge_pr / discussion_mutation)
* `dedupe_key` (e.g., `("comment", repo, issue_number, op_id)`)
* `precondition` (optional state checks)
* `payload`
* `attempts`, `next_run_at`

### Worker discipline

* **Concurrency = 1** for writes (or *very* small, but 1 is safest)
* Enforce **>= 1s delay** between writes (PyGithub defaults to 1s, but your queue should also avoid bursts) ([pygithub.readthedocs.io][3])
* On 403/429: follow GitHub’s backoff ladder (`retry-after` / reset / 60s + exponential) ([GitHub Docs][1])
* Persist successes so re-runs don’t reapply

This queue becomes your “mutation firewall”: reads can be parallelized (carefully), writes are serialized and deduped.

---

# 6) Concrete PyGithub write recipes (the exact surfaces you’ll use)

## Issues

* Create issue: `repo.create_issue(...)` ([pygithub.readthedocs.io][8])
* Update issue: `Issue.edit(...)` via `PATCH /repos/{owner}/{repo}/issues/{issue_number}` ([pygithub.readthedocs.io][5])
* Comment: `Issue.create_comment(...)` ([pygithub.readthedocs.io][5])
* Labels:

  * Add: `Issue.add_to_labels(...)` (POST) ([pygithub.readthedocs.io][5])
  * Set exact: `Issue.set_labels(...)` (PUT) ([pygithub.readthedocs.io][5])
  * Remove one: `Issue.remove_from_labels(name)` (DELETE) ([pygithub.readthedocs.io][5])

## Pull requests

* Close PR: `PullRequest.edit(state="closed")` via `PATCH /repos/{owner}/{repo}/pulls/{pull_number}` ([pygithub.readthedocs.io][5])
* Merge PR:

  * Check: `PullRequest.is_merged()` (GET `/merge`) ([pygithub.readthedocs.io][5])
  * Merge: `PullRequest.merge()` (PUT `/merge`) ([pygithub.readthedocs.io][5])
* Review comments: `create_review_comment`, `create_review_comment_reply` etc. ([pygithub.readthedocs.io][5])

## Discussions

* Use GraphQL; supports create/edit/delete discussions and comments. ([GitHub Docs][6])

---

## A minimal “mutation-safe” wrapper pattern (illustrative)

```python
import time
from github import GithubException

def sleep_for_rate_limit(headers: dict[str, str]) -> None:
    # GitHub’s recommended ladder: retry-after -> x-ratelimit-reset -> >=60s fallback :contentReference[oaicite:35]{index=35}
    ra = headers.get("retry-after") or headers.get("Retry-After")
    if ra:
        time.sleep(int(ra) + 1)
        return
    remaining = headers.get("x-ratelimit-remaining") or headers.get("X-RateLimit-Remaining")
    reset = headers.get("x-ratelimit-reset") or headers.get("X-RateLimit-Reset")
    if remaining == "0" and reset:
        time.sleep(max(0, int(reset) - int(time.time())) + 1)
        return
    time.sleep(60)

def run_mutation(op_id: str, mutate_fn, reconcile_fn, max_attempts: int = 5):
    """
    mutate_fn: does the PyGithub write
    reconcile_fn: checks if op already applied (idempotency) when uncertain
    """
    for attempt in range(max_attempts):
        if reconcile_fn(op_id):
            return "already-applied"

        try:
            return mutate_fn(op_id)
        except GithubException as e:
            # For rate limit-ish errors, back off per GitHub guidance. :contentReference[oaicite:36]{index=36}
            hdrs = getattr(e, "headers", None) or {}
            sleep_for_rate_limit(hdrs)
    raise RuntimeError("mutation failed after retries")
```

This implements the key principle: **retry writes only with a reconciliation step**.

---

# Practical “DoD” checklist for safe writes

* [ ] Webhook-first where possible (don’t poll) ([GitHub Docs][1])
* [ ] One write worker (queue), no concurrent mutations ([GitHub Docs][1])
* [ ] ≥1s pacing for POST/PATCH/PUT/DELETE ([GitHub Docs][1])
* [ ] Backoff uses `retry-after` / `x-ratelimit-reset`, then exponential; stop hammering ([GitHub Docs][1])
* [ ] App-level idempotency for non-idempotent writes (especially comments)
* [ ] Prefer “set” mutations (`set_labels`, `edit(state=...)`) over “additive” when you want deterministic outcomes ([pygithub.readthedocs.io][5])
* [ ] Treat GraphQL mutations as “writes” in the same throttle budget (they cost more points) ([GitHub Docs][2])

If you want the next deep dive, the natural continuation is **“Webhook-driven mutation pipelines”**: how to structure a webhook intake → dedupe → outbox write queue so your system becomes event-driven and almost never polls (and how to test it with replay fixtures). ([GitHub Docs][1])

[1]: https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api "Best practices for using the REST API - GitHub Docs"
[2]: https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api "Rate limits for the REST API - GitHub Docs"
[3]: https://pygithub.readthedocs.io/en/latest/github.html "Main class: Github — PyGithub 0.1.dev1+g19e1c5032 documentation"
[4]: https://pygithub.readthedocs.io/en/stable/utilities.html "Utilities — PyGithub 0.1.dev50+gecd47649e documentation"
[5]: https://pygithub.readthedocs.io/en/latest/apis.html "APIs — PyGithub 0.1.dev1+g19e1c5032 documentation"
[6]: https://docs.github.com/en/graphql/guides/using-the-graphql-api-for-discussions "Using the GraphQL API for Discussions - GitHub Docs"
[7]: https://docs.github.com/en/enterprise-server%403.15/graphql/reference/mutations "Mutations - GitHub Enterprise Server 3.15 Docs"
[8]: https://pygithub.readthedocs.io/en/stable/examples/Issue.html "Issues — PyGithub 0.1.dev50+gecd47649e documentation"

## Deep dive 7 — Repository core surfaces (the “code + git data” plane)

At the repo level, there are **two distinct “code planes”** you use in PyGithub:

1. **Git database (aka Git Data) API** — raw Git objects (**blobs, trees, commits, refs, tags**). GitHub explicitly frames this as “read and write raw Git objects … and list/update references.” ([GitHub Docs][1])
2. **Contents API** — path-based “files + directories” operations (create/update/delete Base64-encoded content; read file/directory contents). ([GitHub Docs][2])

That split is the key to designing “total file scope”, deterministic snapshots, and robust read/write workflows.

---

# 7.1 PyGithub method map (what to reach for)

PyGithub’s own endpoint→method index shows the Git database surface mapped under `/repos/{owner}/{repo}/git/...`: create/get blobs, commits, refs, tags, trees; and ref edits/deletes live on `GitRef`. ([pygithub.readthedocs.io][3])

### Git database primitives (raw Git objects)

From `Repository` (read side is also clearly documented):

* **Blobs:** `get_git_blob()` (and `create_git_blob()` exists in the API index) ([pygithub.readthedocs.io][4])
* **Trees:** `get_git_tree(recursive=...)` (and `create_git_tree()` exists) ([pygithub.readthedocs.io][4])
* **Commits:** `get_git_commit()` (and `create_git_commit()` exists) ([pygithub.readthedocs.io][4])
* **Refs:** `get_git_ref()`, `get_git_refs()`, `get_git_matching_refs()`, and `create_git_ref()` exists; plus `GitRef.edit()`/`GitRef.delete()` ([pygithub.readthedocs.io][4])
* **Tags:** `get_git_tag()` (and `create_git_tag()` exists) ([pygithub.readthedocs.io][4])

### Contents primitives (path-based)

* `create_file(...)`, `update_file(...)`, `delete_file(...)` all call the Contents endpoints and accept branch + author/committer metadata. ([pygithub.readthedocs.io][4])
* `get_dir_contents(...)` is documented as calling `GET /contents/{path}`. (Docs/examples also show `repo.get_contents(...)` usage.) ([pygithub.readthedocs.io][4])

---

# 7.2 Total file scope (inventory): **Trees**, not Contents

### Why Trees is the canonical “repo inventory” API

GitHub explicitly points you to Trees for recursive inventory and warns Contents has a **1,000 files per directory** cap. ([GitHub Docs][2])

### The “one request” fast path

“Get a tree” accepts a SHA1 *or ref name* and supports `recursive`; but if the response is `truncated: true`, GitHub instructs you to switch to non-recursive and fetch subtrees one at a time. There’s also a hard cap: **100,000 entries / 7 MB** for the recursive response. ([GitHub Docs][5])

**Practical meaning:** your best-in-class inventory implementation is:

* Try `repo.get_git_tree(<ref_or_tree_sha>, recursive=True)`
* If `truncated`, do a BFS/DFS that repeatedly calls non-recursive `get_git_tree(tree_sha)` per subtree

### Tree entries are rich enough to be a cache contract

When *creating* trees, GitHub spells out the canonical semantics of tree entries:

* `mode` encodes file type semantics, including submodules and symlinks (`100644`, `100755`, `040000`, `160000`, `120000`)
* `type` is `blob` / `tree` / `commit`
* setting `sha: null` deletes a file
* use either `sha` **or** `content` (not both) ([GitHub Docs][6])

Even if you never create trees, these semantics explain what you’ll see in inventories and why **“path → blob sha (+ mode/type)”** is the right manifest schema.

---

# 7.3 Reading file contents: Contents vs Blobs (and when to go raw)

## A) Contents API: simplest for “read this path”

GitHub’s Contents endpoint:

* returns file or directory contents; omitting `path` returns the root directory
* supports custom media types including:

  * `application/vnd.github.raw+json` (raw file contents for files and symlinks)
  * `application/vnd.github.object+json` (consistent object form; directory becomes `{entries: [...]}`)
* supports `ref` as **commit/branch/tag** (default is repo default branch) ([GitHub Docs][2])

**Size tiers matter (this bites scanners):**

* ≤ 1 MB: all features supported
* 1–100 MB: only `raw` or `object` media types supported (and object returns empty `content`)
* > 100 MB: not supported ([GitHub Docs][2])

**Submodules + symlinks are special:**

* Directory listings may label submodules as `"file"` for backwards compatibility (expected to become `"submodule"` in a future major version)
* Symlinks can return either the target file’s contents or a descriptor object depending on the target ([GitHub Docs][2])

### “Raw bytes” retrieval via PyGithub Requester (best practice)

PyGithub exposes a low-level `Requester` on objects for raw requests; notably:

* `requestJsonAndCheck(..., headers=...)`
* `getStream(url, ..., headers=...) → (status, headers, stream_chunk_iterator)` ([pygithub.readthedocs.io][7])

So for a **large file** where you want **raw bytes**, use the Contents endpoint with the raw media type, via `getStream`:

```python
status, headers, chunks = repo.requester.getStream(
    f"/repos/{repo.full_name}/contents/{path}",
    parameters={"ref": commit_sha},
    headers={"Accept": "application/vnd.github.raw+json"},
)
data = b"".join(chunks)
```

This directly aligns with GitHub’s documented “raw” media type and size tier rules. ([GitHub Docs][2])

## B) Git Blobs API: best when you already have blob SHAs (from Trees)

GitHub’s blob docs are explicit:

* blobs store file contents; the file’s SHA-1 is computed and stored in the blob
* “Get a blob” default returns base64 `content`, but `application/vnd.github.raw+json` returns raw blob data
* supports blobs up to **100 MB** ([GitHub Docs][8])

**Best-in-class read strategy for scanners:**

* Inventory via Trees
* Read file bytes by blob SHA (content-addressed caching keyed by SHA)
* Use Requester+raw media type only when you want to skip base64 decode or stream

---

# 7.4 Writing file changes: two “right” workflows

## Workflow 1 — Simple file writes via Contents (single-file, ergonomic)

PyGithub documents:

* `create_file(path, message, content, branch=..., committer=..., author=...)` → `PUT /contents/{path}` ([pygithub.readthedocs.io][4])
* `update_file(path, message, content, sha, branch=..., committer=..., author=...)` (requires the prior blob `sha`) ([pygithub.readthedocs.io][4])
* `delete_file(path, message, sha, ...)` → `DELETE /contents/{path}` ([pygithub.readthedocs.io][4])

The PyGithub examples show the canonical “get sha then update/delete” loop. ([pygithub.readthedocs.io][9])

## Workflow 2 — Best-in-class multi-file commits via Git database (blob → tree → commit → ref)

This is the workflow you use when you want:

* one commit that touches N files
* deterministic tree construction
* more control over object identity + caching

GitHub’s docs basically describe the pipeline:

* Create blob: content + encoding (`utf-8` / `base64`) ([GitHub Docs][8])
* Create tree:

  * specify entries with `path/mode/type/sha` (or inline `content`)
  * set `base_tree` to the current tree to avoid accidentally “deleting everything else” ([GitHub Docs][6])
* Create commit: `message`, `tree`, optional `parents`, plus author/committer metadata ([GitHub Docs][10])
* Update reference to point to new commit (fast-forward by default; `force` optional) ([GitHub Docs][11])

### Reference formatting (this trips people)

GitHub is explicit:

* “List matching references” uses `heads/<branch>` and `tags/<tag>` ([GitHub Docs][11])
* “Create a reference” requires a **fully qualified** `ref` like `refs/heads/master` and rejects refs that don’t start with `refs` and have at least two slashes ([GitHub Docs][11])

### Tags: annotated vs lightweight

The **Git tags API supports annotated tag objects only**; and creating a tag object does **not** create the tag reference. To create an annotated tag, you create the tag object *then* create `refs/tags/<tag>`; for a lightweight tag, just create the ref. ([GitHub Docs][12])

---

# 7.5 “Manifest contract” for code-plane caching + diffs

If your goal is “total file scope” + deterministic diffs, your repo snapshot manifest should be built from Trees and include:

* `resolved_commit_sha`
* `tree_sha`
* sorted entries: `{path, type, mode, sha, size?}`

That’s enough to:

* diff paths (added/removed)
* diff contents (sha changes)
* detect renames by matching blob SHA appearing under a new path

The tree entry semantics (mode/type/sha, including submodules and symlinks) are stable and explicitly defined by GitHub. ([GitHub Docs][6])

---

# 7.6 When to drop down to `requester` (even if PyGithub has a method)

Even with broad coverage, `requester` is the “power tool” for repo-core work because it lets you:

* set custom `Accept` headers (raw/object media types, diff formats, etc.)
* stream bytes (`getStream`)
* still reuse PyGithub auth + base_url + pooling

PyGithub documents both `requestJsonAndCheck(... headers=...)` and `getStream(... headers=...)` on `Requester`. ([pygithub.readthedocs.io][7])

---

If you want the next deep dive after this repo-core section, the two most natural continuations are:

1. **Branches + refs as “control plane”** (branch enumeration, safe ref updates, tag+release workflows, and determinism around default branch changes)
2. **Archives + bulk export** (zip/tar snapshots pinned to commit SHAs; when that beats API reads for scanners)

[1]: https://docs.github.com/en/rest/git "REST API endpoints for Git database - GitHub Docs"
[2]: https://docs.github.com/en/rest/repos/contents "REST API endpoints for repository contents - GitHub Docs"
[3]: https://pygithub.readthedocs.io/en/latest/apis.html "APIs — PyGithub 0.1.dev1+g19e1c5032 documentation"
[4]: https://pygithub.readthedocs.io/en/v1.57/github_objects/Repository.html "Repository — PyGithub 1.57 documentation"
[5]: https://docs.github.com/rest/git/trees "REST API endpoints for Git trees - GitHub Docs"
[6]: https://docs.github.com/en/rest/git/trees "REST API endpoints for Git trees - GitHub Docs"
[7]: https://pygithub.readthedocs.io/en/latest/utilities.html "Utilities — PyGithub 0.1.dev1+g19e1c5032 documentation"
[8]: https://docs.github.com/en/rest/git/blobs "REST API endpoints for Git blobs - GitHub Docs"
[9]: https://pygithub.readthedocs.io/en/latest/examples/Repository.html?utm_source=chatgpt.com "Repository — PyGithub 2.8.1.dev27+g61dcf49d3 documentation"
[10]: https://docs.github.com/en/rest/git/commits "REST API endpoints for Git commits - GitHub Docs"
[11]: https://docs.github.com/rest/git/refs "REST API endpoints for Git references - GitHub Docs"
[12]: https://docs.github.com/en/rest/git/tags "REST API endpoints for Git tags - GitHub Docs"

## Deep dive 8 — Collaboration plane (issues, PRs, reviews, comments, projects, discussions)

This “plane” is everything humans *do together* around code: tracking work (issues/projects), proposing changes (PRs), reviewing changes (reviews + review comments), and communicating (issue comments + review comments + discussions).

---

# 8.1 The two big modeling facts you must internalize

### 1) PRs are *also* issues (for “shared actions”)

GitHub’s REST docs repeat this everywhere: **every pull request is an issue, but not every issue is a pull request**—so shared operations like **assignees/labels/milestones** live under Issues endpoints. ([GitHub Docs][1])

### 2) “Comments” are *three different things*

* **Issue comments** = the main conversation thread on issues *and PRs* (top-level discussion). ([GitHub Docs][1])
* **Pull request review comments** = diff-hunk / line-level comments made during review (different from issue comments and commit comments). ([GitHub Docs][2])
* **Commit comments** = separate API family (relevant if you’re doing commit-centric tooling). ([GitHub Docs][2])

PyGithub mirrors this distinction—and even warns you about it in the `PullRequest` object (see §8.3). ([pygithub.readthedocs.io][3])

---

# 8.2 Issues: lifecycle + “shared actions” surface

## What PyGithub gives you

PyGithub’s endpoint→method index is the quickest “capability map.” For issues it includes, among others:

* comments: `Issue.get_comments()` / `Issue.create_comment()` (also surfaced via `PullRequest.create_issue_comment()` for PRs)
* labels: `Issue.get_labels()`, `add_to_labels()`, `set_labels()`, `remove_from_labels()`, `delete_labels()`
* lock/unlock, reactions, timeline, sub-issues, events, etc. ([pygithub.readthedocs.io][4])

That’s the “everything you need for a real issue workflow” layer.

## GitHub-side semantics you should design around

* Issue comments apply to both issues and PRs; creating comments triggers notifications and GitHub warns that creating content too quickly can result in secondary rate limiting. ([GitHub Docs][1])
* Labels and assignees are explicitly described as shared across issues and PRs (via Issues endpoints). ([GitHub Docs][5])

---

# 8.3 Pull requests: review-centric objects, plus the “PR is an issue” bridge

## Core PR object (PyGithub)

The `PullRequest` class wraps REST Pulls endpoints. ([pygithub.readthedocs.io][6])

### Critical PyGithub warning: `get_comments()` is **review comments only**

PyGithub explicitly states:

* `PullRequest.get_comments()` returns **review comments** only
* for normal conversation comments use `PullRequest.get_issue_comments()` ([pygithub.readthedocs.io][3])

So, the common “safe trio” is:

* `pr.get_issue_comments()` → conversation thread on the PR (issue comments) ([pygithub.readthedocs.io][3])
* `pr.get_review_comments()` / `pr.get_comments()` → diff comments (review comments) ([pygithub.readthedocs.io][3])
* `pr.get_reviews()` → review objects (approve / request changes / comment) ([pygithub.readthedocs.io][3])

### Review comment semantics (GitHub)

GitHub defines review comments as diff-hunk comments and explicitly distinguishes them from issue comments and commit comments. ([GitHub Docs][2])

---

# 8.4 Reviews: “review objects” vs “review comments” (and the PENDING workflow)

## Review objects (groups of review comments + state)

GitHub describes PR reviews as **groups of review comments** with a **state** and optional body comment. ([GitHub Docs][7])

PyGithub has a `PullRequestReview` object; it maps to the REST review endpoints and includes operations like `dismiss(...)`. ([pygithub.readthedocs.io][8])

## Creating reviews safely (the PENDING pattern)

GitHub’s REST review docs spell out an important lifecycle detail:

* If you create a review with **no `event`**, it becomes **`PENDING`** (draft) and isn’t submitted
* To submit, you later call “submit a review”
* Creating reviews triggers notifications and can trip secondary limits if you create content too quickly ([GitHub Docs][7])

This matters because “post a bunch of inline comments” is usually done by:

1. create a **PENDING** review with many draft comments in `comments=[...]`
2. submit it once the set is complete ([GitHub Docs][7])

---

# 8.5 Comments: “repo-level streams” are the best-in-class sync primitive

If you’re building tooling that needs to *sync* collaboration data, don’t crawl “issue → comments” N times.

Instead, use repository-level streams with `since`:

* **Issue comments in a repository**: includes comments on issues and PRs; ordered by ascending ID by default. ([GitHub Docs][1])
* **Review comments in a repository**: lists review comments for all PRs; supports `sort`, `direction`, `since`. ([GitHub Docs][2])

This pairs perfectly with the “delta sync” approach you already asked for earlier.

---

# 8.6 Projects: Classic vs the new Projects experience

## Classic Projects (boards: projects → columns → cards)

PyGithub includes first-class objects for classic projects:

* `Project` (edit/delete, get/create columns)
* `ProjectColumn` (get/create cards, move/edit/delete)
* `ProjectCard` (get_content/move/edit/delete) ([pygithub.readthedocs.io][9])

**But** GitHub is actively deprecating Projects (classic) in favor of the new Projects experience. ([GitHub Docs][10])

So: use these wrappers when you must support legacy boards, but treat them as a compatibility surface.

## New Projects (Projects v2) — now has a REST API

As of **Sep 11, 2025**, GitHub announced a REST API for GitHub Projects that can:

* list projects and get project/field/item info
* add/delete issues and PRs from a project
* update field values ([The GitHub Blog][11])

GitHub’s REST Projects docs show endpoints like `GET /orgs/{org}/projectsV2`, require the API version header (`X-GitHub-Api-Version: 2022-11-28`), and use cursor-style pagination (`before`/`after` in Link headers). ([GitHub Docs][12])

**PyGithub status (practically):**

* PyGithub has rich wrappers for **classic** Projects objects (above).
* For **Projects v2 REST** (and/or GraphQL), you should plan to go through `repo.requester` (raw REST calls with the required headers) until PyGithub grows native wrappers.

---

# 8.7 Discussions: PyGithub’s GraphQL-backed wrappers

PyGithub’s GraphQL docs explain the design:

* PyGithub is primarily REST v3, but exposes GraphQL because GitHub ships functionality there
* It provides GraphQL access while still returning “usual” PyGithub objects
* It has GraphQL-native classes (e.g., `RepositoryDiscussion`) where there’s no REST equivalent, and it maps GraphQL attributes into REST-style objects ([pygithub.readthedocs.io][13])

Concrete surfaces:

* `Repository.get_discussion` is a canonical example for GraphQL queries. ([pygithub.readthedocs.io][13])
* Mutations: `RepositoryDiscussion.add_comment`, and `RepositoryDiscussionComment.edit/delete` are cited examples. ([pygithub.readthedocs.io][13])
* Pagination: PyGithub can paginate GraphQL results via `PaginatedList` if your query includes `totalCount` + `pageInfo{…}`. ([pygithub.readthedocs.io][13])

---

# 8.8 Practical “best-in-class” decomposition (how I’d structure your code)

If you’re turning this into a clean internal API, split into four modules:

1. **issues.py**

   * issue CRUD + “shared actions”: labels/assignees/milestones + issue comments + timeline/events
2. **pulls.py**

   * PR CRUD + `get_issue_comments()` + `get_reviews()` + `get_review_comments()` (never confuse comment types) ([pygithub.readthedocs.io][3])
3. **reviews.py**

   * PENDING review creation + submit + dismiss + inline comment batching ([GitHub Docs][7])
4. **projects.py**

   * classic projects via `Project/Column/Card` wrappers
   * projects v2 via REST (requester) with explicit API-version header ([pygithub.readthedocs.io][14])

---

If you want the next deep dive after this: I’d go one of two directions depending on what your tooling is doing most:

* **“Projects v2 in practice”** (fields/items/views + update field values + add/remove issue/PR items, with a clean wrapper over the REST endpoints), or
* **“PR review automation”** (diff positioning, batching inline comments into a single PENDING review + submit, and how to keep it mutation-safe).

[1]: https://docs.github.com/rest/issues/comments "REST API endpoints for issue comments - GitHub Docs"
[2]: https://docs.github.com/en/rest/pulls/comments "REST API endpoints for pull request review comments - GitHub Docs"
[3]: https://pygithub.readthedocs.io/en/v2.1.1/github_objects/PullRequest.html "PullRequest — PyGithub 2.1.1 documentation"
[4]: https://pygithub.readthedocs.io/en/latest/apis.html "APIs — PyGithub 0.1.dev1+g19e1c5032 documentation"
[5]: https://docs.github.com/en/rest/issues/labels "REST API endpoints for labels - GitHub Docs"
[6]: https://pygithub.readthedocs.io/en/latest/github_objects/PullRequest.html?utm_source=chatgpt.com "PullRequest — PyGithub 2.8.1.dev27+g61dcf49d3 ..."
[7]: https://docs.github.com/rest/pulls/reviews "REST API endpoints for pull request reviews - GitHub Docs"
[8]: https://pygithub.readthedocs.io/en/latest/github_objects/PullRequestReview.html "PullRequestReview — PyGithub 0.1.dev1+g19e1c5032 documentation"
[9]: https://pygithub.readthedocs.io/en/latest/github_objects.html "Github objects — PyGithub 0.1.dev1+g19e1c5032 documentation"
[10]: https://docs.github.com/en/enterprise-server%403.16/rest/projects-classic/projects "REST API endpoints for Projects (classic) - GitHub Enterprise Server 3.16 Docs"
[11]: https://github.blog/changelog/2025-09-11-a-rest-api-for-github-projects-sub-issues-improvements-and-more/ "A REST API for GitHub Projects, sub-issues improvements, and more - GitHub Changelog"
[12]: https://docs.github.com/en/rest/projects/projects "REST API endpoints for Projects - GitHub Docs"
[13]: https://pygithub.readthedocs.io/en/latest/graphql.html "Github GraphQL — PyGithub 0.1.dev1+g19e1c5032 documentation"
[14]: https://pygithub.readthedocs.io/en/latest/github_objects/ProjectColumn.html "ProjectColumn — PyGithub 0.1.dev1+g19e1c5032 documentation"

## Deep dive 9 — Automation & CI/CD plane (Actions, deployments, environments, webhooks)

This plane is “how you make GitHub *do things* (run CI, gate deploys, notify systems)”, and PyGithub gives you a pretty complete **Actions + Deployments + Webhooks** control surface—plus a few places where you’ll drop to `repo.requester` for endpoints that return redirects (logs/artifacts) or aren’t wrapped.

---

# 9.1 GitHub Actions via PyGithub: workflows → runs → jobs → artifacts

### A) Workflows (the definitions)

PyGithub maps the core workflow endpoints cleanly:

* `repo.get_workflows()`
* `repo.get_workflow(workflow_id)`
* `workflow.enable()` / `workflow.disable()`
* `workflow.create_dispatch(ref, inputs=...)`
* `workflow.get_runs(...)` (filters: actor/branch/event/status/created/etc.)
  All visible in the API mapping, and the `Workflow` object documents the calls directly. ([pygithub.readthedocs.io][1])

Two practical notes:

* GitHub lets you use REST to list workflows and paginate (max `per_page=100`). ([GitHub Docs][2])
* The workflow “dispatch” endpoint can be triggered with `workflow_id` as the workflow file name (GitHub’s docs explicitly mention this pattern). ([GitHub Docs][3])

**PyGithub gotcha:** `Workflow.create_dispatch(...)` returns `bool` and the docs note it may raise or return `False` without details depending on `throw`. In automation code, set `throw=True` so failures are actionable. ([pygithub.readthedocs.io][4])

---

### B) Workflow runs (instances of execution)

PyGithub wraps the “repo runs” and “run operations” well:

* `repo.get_workflow_runs()` and `repo.get_workflow_run(run_id)` ([pygithub.readthedocs.io][1])
* `run.cancel()`, `run.rerun()`, `run.rerun_failed_jobs()`, `run.timing()`, `run.jobs()` ([pygithub.readthedocs.io][5])

GitHub’s own REST docs emphasize that workflow runs can be viewed/re-run/canceled and that logs are available via API. ([GitHub Docs][6])

**Delta/polling watchout:** the “List workflow runs” endpoint warns it will return **up to 1,000 results** for each “search” when using certain filter parameters (actor/branch/check_suite_id/created/event/head_sha/status). So treat it like a *filtered query surface*, not a full history loader. ([GitHub Docs][6])

---

### C) Jobs (step-level visibility + job logs)

PyGithub gives you run→jobs via `WorkflowRun.jobs(...)`. ([pygithub.readthedocs.io][5])
GitHub REST defines workflow jobs as “a set of steps that execute on the same runner,” and explicitly says the REST API can be used to view logs + job info. ([GitHub Docs][7])

**Why you care:** if you want logs *while a run is still executing*, job-level logs are often more reliable than run-level logs (run logs are a single archive). (GitHub documents job/log capabilities; you’ll choose which endpoint at runtime.) ([GitHub Docs][7])

---

### D) Artifacts (metadata + delete in PyGithub; download via redirect)

PyGithub maps repo-level artifacts:

* `repo.get_artifacts()` and `repo.get_artifact(artifact_id)` ([pygithub.readthedocs.io][1])
* `artifact.delete()` ([pygithub.readthedocs.io][1])

GitHub’s REST artifacts docs explain artifacts are for “share data between jobs” and can be downloaded/deleted via API. ([GitHub Docs][8])
For downloading: GitHub returns a **redirect URL** that **expires after 1 minute**; you must read the `Location` header and then fetch the zip. ([GitHub Docs][8])

**Implication:** even if PyGithub doesn’t have a “download()” helper, you can download artifacts via `repo.requester` + follow redirect.

---

### E) Runners + Actions secrets/variables (nice-to-have ops surface)

PyGithub also maps:

* **Self-hosted runners** list/get/remove at the repo level ([pygithub.readthedocs.io][1])
* **Repo Actions secrets** public key + get/create/delete ([pygithub.readthedocs.io][1])
* **Repo Actions variables** get/create/edit/delete ([pygithub.readthedocs.io][1])

---

# 9.2 Run logs + artifacts: the “redirect download” pattern (PyGithub requester)

Two key endpoints return expiring redirects:

* **Download workflow run logs**: redirect URL, expires after 1 minute, look for `Location`. ([GitHub Docs][6])
* **Download an artifact**: redirect URL, expires after 1 minute, `archive_format` must be `zip`, look for `Location`. ([GitHub Docs][8])

A clean pattern is:

1. call the API endpoint (authenticated)
2. read `Location`
3. fetch bytes from that Location (still authenticated as needed)

In PyGithub terms: `repo.requester.requestRaw(...)` / `getStream(...)` (exact helper depends on version), but the principle is stable.

---

# 9.3 Deployments: deployment objects + statuses (and why they still matter)

PyGithub maps classic deployments:

* `repo.get_deployments()`, `repo.create_deployment(...)`, `repo.get_deployment(deployment_id)` ([pygithub.readthedocs.io][1])
* `deployment.get_statuses()`, `deployment.create_status(state=..., target_url=..., description=..., environment=...)` ([pygithub.readthedocs.io][1])

GitHub’s deployments docs define:

* a deployment = request to deploy a specific ref (branch/SHA/tag)
* deployment statuses have states like `error`, `failure`, `pending`, `in_progress`, `queued`, `success`
* `description` and `log_url` are “highly recommended”
* GitHub emits `deployment` and `deployment_status` events for integrations to react and update status. ([GitHub Docs][9])

**Best-in-class use:** treat Deployments as your “external deploy orchestrator handshake” even if Actions runs the actual steps—because deployment statuses give a standard place to publish rollout progress + links.

---

# 9.4 Environments: gating + config + secrets/variables

PyGithub maps environments at the repo level:

* `repo.get_environments()`, `repo.get_environment(name)`, `repo.create_environment(name, ...)`, `repo.delete_environment(name)` ([pygithub.readthedocs.io][1])

And the `Environment` object supports environment-scoped secrets/variables, including:

* `env.get_public_key()`
* `env.create_secret(...)`, `env.get_secrets()`, `env.delete_secret(...)`
* `env.create_variable(...)`, `env.get_variables()`, `env.delete_variable(...)` ([pygithub.readthedocs.io][10])

GitHub’s “deployment environments” REST docs frame environments as configurable deployment targets, and note environment secrets are managed via the Actions secrets APIs; they also document plan/feature availability constraints. ([GitHub Docs][11])

---

# 9.5 Webhooks: create/manage hooks + deliveries + ping/test + redelivery

### A) Repository webhooks (server receives POST events)

GitHub’s REST docs summarize the model: repository webhooks deliver HTTP `POST` payloads when certain events happen. ([GitHub Docs][12])
They also note `last response` may be null if no deliveries happened in 30 days. ([GitHub Docs][12])

PyGithub maps the core CRUD + observability:

* `repo.get_hooks()`, `repo.create_hook(...)`, `repo.get_hook(hook_id)` ([pygithub.readthedocs.io][1])
* `hook.edit(...)`, `hook.delete()`, `hook.ping()`, `hook.test()` ([pygithub.readthedocs.io][1])
* deliveries: `repo.get_hook_deliveries(hook_id)` and `repo.get_hook_delivery(hook_id, delivery_id)` ([pygithub.readthedocs.io][1])

Testing behavior:

* GitHub can trigger `ping` events via REST, and can trigger a test `push` event for repo webhooks subscribed to `push`. ([GitHub Docs][13])
* After you create a webhook, GitHub sends a `ping` event automatically. ([GitHub Docs][14])

### B) Redelivery: not fully wrapped in PyGithub (use requester)

GitHub’s REST docs include “Redeliver a delivery” at:
`POST /repos/{owner}/{repo}/hooks/{hook_id}/deliveries/{delivery_id}/attempts` (returns 202; can be “spammed” / 422). ([GitHub Docs][15])

PyGithub’s API mapping exposes listing deliveries + getting a delivery, but does **not** list a dedicated redeliver helper—so for “redeliver failed delivery” automation, drop to `repo.requester`. ([pygithub.readthedocs.io][1])

GitHub’s webhook troubleshooting docs explicitly describe using the REST API to view and redeliver failed deliveries and even provide scripts/workflows for automatic redelivery. ([GitHub Docs][16])

---

# 9.6 A “golden” minimal control-loop (dispatch → watch → logs/artifacts)

Here’s a compact shape that matches how you’ll actually build automation tools:

```python
from __future__ import annotations
import time
from github import Github

def dispatch_and_wait(repo, workflow_id_or_file: str, ref: str, inputs: dict | None = None, timeout_s: int = 1800):
    wf = repo.get_workflow(workflow_id_or_file)
    ok = wf.create_dispatch(ref=ref, inputs=inputs or {}, throw=True)  # may raise if throw=True
    assert ok is True

    deadline = time.time() + timeout_s
    while time.time() < deadline:
        # newest-first; tune filters (branch/event/head_sha) to reduce scan
        runs = wf.get_runs(branch=ref if "/" not in ref else None, event="workflow_dispatch")
        run = runs[0] if runs.totalCount else None
        if run and run.status == "completed":
            return run
        time.sleep(5)

    raise TimeoutError("workflow run did not complete in time")

def download_run_logs_zip(repo, run_id: int) -> bytes:
    # GitHub returns a redirect that expires after 1 minute; read Location and fetch. :contentReference[oaicite:36]{index=36}
    status, headers, _ = repo.requester.requestRaw(
        "GET",
        f"/repos/{repo.owner.login}/{repo.name}/actions/runs/{run_id}/logs",
        headers={"Accept": "application/vnd.github+json"},
    )
    loc = headers.get("location") or headers.get("Location")
    if not loc:
        raise RuntimeError(f"Expected redirect Location header, got status={status}")

    # Fetch the redirected URL (exact helper may vary by PyGithub version)
    status2, headers2, chunks = repo.requester.getStream(loc)
    return b"".join(chunks)
```

Why this exact shape works well:

* you use PyGithub for high-level typed objects (workflow/run) ([pygithub.readthedocs.io][4])
* you use `requester` for the “redirect download” endpoints (logs/artifacts) ([GitHub Docs][6])

---

## Next deep dive options

If you want to keep following the original outline, the next natural sections are:

* **10) Security & compliance plane** (Dependabot alerts, code scanning, secret scanning, advisories), or
* **11) Checks API plane** (check runs/suites) to unify CI status reporting + annotations (this connects tightly to Actions logs and job structure).

[1]: https://pygithub.readthedocs.io/en/latest/apis.html "APIs — PyGithub 0.1.dev1+g19e1c5032 documentation"
[2]: https://docs.github.com/rest/actions/workflows "REST API endpoints for workflows - GitHub Docs"
[3]: https://docs.github.com/en/enterprise-server%403.14/rest/actions/workflows?utm_source=chatgpt.com "REST API endpoints for workflows"
[4]: https://pygithub.readthedocs.io/en/latest/github_objects/Workflow.html "Workflow — PyGithub 0.1.dev1+g19e1c5032 documentation"
[5]: https://pygithub.readthedocs.io/en/latest/github_objects/WorkflowRun.html "WorkflowRun — PyGithub 0.1.dev1+g19e1c5032 documentation"
[6]: https://docs.github.com/rest/actions/workflow-runs "REST API endpoints for workflow runs - GitHub Docs"
[7]: https://docs.github.com/en/rest/actions/workflow-jobs "REST API endpoints for workflow jobs - GitHub Docs"
[8]: https://docs.github.com/en/rest/actions/artifacts "REST API endpoints for GitHub Actions artifacts - GitHub Docs"
[9]: https://docs.github.com/en/rest/deployments/deployments?apiVersion=2022-11-28 "REST API endpoints for deployments - GitHub Docs"
[10]: https://pygithub.readthedocs.io/en/v2.3.0/github_objects/Environment.html "Environment — PyGithub 2.3.0 documentation"
[11]: https://docs.github.com/en/rest/deployments/environments "REST API endpoints for deployment environments - GitHub Docs"
[12]: https://docs.github.com/rest/webhooks/repos "REST API endpoints for repository webhooks - GitHub Docs"
[13]: https://docs.github.com/en/webhooks/testing-and-troubleshooting-webhooks/testing-webhooks?utm_source=chatgpt.com "Testing webhooks"
[14]: https://docs.github.com/en/webhooks/using-webhooks/creating-webhooks?utm_source=chatgpt.com "Creating webhooks - GitHub Docs"
[15]: https://docs.github.com/rest/webhooks/repos?utm_source=chatgpt.com "REST API endpoints for repository webhooks"
[16]: https://docs.github.com/en/webhooks/using-webhooks/handling-failed-webhook-deliveries?utm_source=chatgpt.com "Handling failed webhook deliveries"

## Deep dive 10 — Security & compliance plane

This plane is “security signals as first-class data”: **Dependabot alerts**, **code scanning alerts**, **secret scanning alerts**, and **security advisories**—plus the repo/org “control plane” knobs that tell you what’s enabled.

### 10.1 PyGithub coverage map (what’s wrapped vs “use requester”)

PyGithub’s API index shows direct wrappers for:

* **Dependabot alerts**

  * `Repository.get_dependabot_alerts()`, `get_dependabot_alert()`, `update_dependabot_alert()` (and org-level equivalents) ([pygithub.readthedocs.io][1])
* **Code scanning alerts**

  * `Repository.get_codescan_alerts()`, `get_codescan_alert()`, and `CodeScanAlert.get_instances()` (and org-level `Organization.get_codescan_alerts()`) ([pygithub.readthedocs.io][1])
* **Secret scanning alerts**

  * `Repository.get_secret_scanning_alerts()`, `get_secret_scanning_alert()` (and org-level `Organization.get_secret_scanning_alerts()`) ([pygithub.readthedocs.io][1])
* **Repository security advisories**

  * `Repository.get_repository_advisories()`, `create_repository_advisory()`, `report_security_vulnerability()`, `get_repository_advisory()` plus many `RepositoryAdvisory.*` mutation helpers (publish/close/etc.) ([pygithub.readthedocs.io][1])
* **Repo control-plane visibility** (`security_and_analysis`)

  * GitHub’s repos API returns a `security_and_analysis` block (advanced security / secret scanning / push protection / non-provider patterns), but you only see it with admin or org owner/security-manager privileges. ([GitHub Docs][2])

Anything beyond that (e.g., “latest platform additions”) is usually still reachable via `repo.requester`.

---

### 10.2 Dependabot alerts (dependency vulnerabilities)

**What it is:** a stream of vulnerable dependency findings with a clear triage state machine.

#### Listing (repo or org)

The REST API supports substantial filtering:

* `state` supports: `auto_dismissed`, `dismissed`, `fixed`, `open` ([GitHub Docs][3])
* filter by `severity`, `ecosystem`, `package`, `manifest`, plus EPSS-based filters like `epss_percentage` and more ([GitHub Docs][3])
* sort by `created`, `updated`, or `epss_percentage` ([GitHub Docs][3])

Auth/permissions:

* classic PAT/OAuth typically needs `security_events` (or `public_repo` for public-only). ([GitHub Docs][3])
* fine-grained tokens need **“Dependabot alerts” repo permissions (read)** for listing. ([GitHub Docs][3])

#### Getting a single alert + caching win

The “get alert” endpoint explicitly supports `304 Not modified`, which is perfect for ETag/conditional caching (low cost, high payoff). ([GitHub Docs][3])

#### Updating/dismissing (mutation safety)

Updating a Dependabot alert is: set `state` to `dismissed` or `open`. If you dismiss, you **must** provide `dismissed_reason` (e.g. `tolerable_risk`, `not_used`, etc.). ([GitHub Docs][3])
It can return `422` for “validation failed, or the endpoint has been spammed” — treat this exactly like a write-path backpressure signal and queue/retry politely (don’t hammer). ([GitHub Docs][3])

#### Dependabot secrets (CI/CD + registries)

PyGithub also maps Dependabot secrets endpoints (public key + create/delete secret). ([pygithub.readthedocs.io][1])
GitHub Apps need the `dependabot_secrets` permission for these endpoints. ([GitHub Docs][4])

---

### 10.3 Code scanning alerts (static analysis / CodeQL / SARIF)

**What it is:** findings from code scanning tools, with rich “most recent instance” details per alert.

#### Listing alerts (repo)

Key behaviors:

* Response includes a `most_recent_instance` for the default branch (or a specified `ref`). ([GitHub Docs][5])
* Filters include:

  * `tool_name` or `tool_guid` (mutually exclusive) ([GitHub Docs][5])
  * `pr` number (list results scoped to a PR) ([GitHub Docs][5])
  * `sort` by `created` or `updated` ([GitHub Docs][5])
  * `state` can be `open`, `closed`, `dismissed`, `fixed` and you can filter by `severity` ([GitHub Docs][5])
  * pagination supports cursor-style `before` / `after` from the `Link` header ([GitHub Docs][5])

Auth/permissions:

* classic PAT/OAuth needs `security_events` (or `public_repo`). ([GitHub Docs][5])
* fine-grained tokens require “Code scanning alerts” permission (read/write depending on endpoint). ([GitHub Docs][5])

#### Alert instances

PyGithub maps `/code-scanning/alerts/{alert_number}/instances` to `CodeScanAlert.get_instances()` for “where does this occur?” workflows. ([pygithub.readthedocs.io][1])

#### Beyond “alerts”: SARIF uploads + analyses

GitHub’s code scanning REST surface explicitly supports uploading offline tool results as **SARIF**, and then polling upload status. ([GitHub Docs][5])
If PyGithub doesn’t wrap SARIF endpoints in your version, this is a prime `repo.requester` use case.

---

### 10.4 Secret scanning alerts (secrets + push protection)

**What it is:** detections of leaked secrets with:

* alert state (`open` / `resolved`)
* optional assignment
* locations where the secret appears

#### Listing alerts (org or repo)

* Org list requires org admin or security manager. ([GitHub Docs][6])
* Repo list requires repo admin (or org admin for owning org). ([GitHub Docs][6])
* Filter by `state` (`open` / `resolved`). ([GitHub Docs][6])

Permissions:

* fine-grained tokens need **“Secret scanning alerts” repo permissions (read)**. ([GitHub Docs][6])

#### “Non-provider patterns / passwords” are opt-in on the REST API

GitHub added a `secret_type` query param so you can request alerts for non-provider patterns and `password`, and notes they are **not returned by default** in the list endpoints. ([The GitHub Blog][7])
This is an easy “why is my alert count low?” gotcha.

#### Update alert (resolve + assign)

You can resolve/unresolve and assign/unassign:

* When setting `state="resolved"`, you must provide a `resolution`. ([GitHub Docs][6])
* `404` can mean “repo is public, or secret scanning is disabled, or not found” — treat it as “not eligible / not enabled” as much as “missing.” ([GitHub Docs][6])
* `422` can mean mismatched state/resolution or invalid assignee (no write access). ([GitHub Docs][6])

#### Locations

Listing locations for an alert is a first-class endpoint (and also returns `404` when secret scanning is disabled). ([GitHub Docs][6])

#### Push protection bypass (write-path with strict identity)

There’s a dedicated endpoint to create a push protection bypass:

* caller must be the original author of the committed secret ([GitHub Docs][6])
* requires a `placeholder_id` and a reason (`false_positive`, `used_in_tests`, `will_fix_later`) ([GitHub Docs][6])

#### Scan history (requires GitHub Advanced Security)

Secret scanning scan history is gated: it explicitly requires GitHub Advanced Security and returns `404` if GHAS or secret scanning isn’t enabled. ([GitHub Docs][6])

---

### 10.5 Security advisories (global + repository)

#### Global advisories (GitHub Advisory DB)

* Lists advisories; **defaults exclude malware**, and you must set `type=malware` to include them. ([GitHub Docs][8])
* Can be used without auth for public resources, and fine-grained tokens require no permissions. ([GitHub Docs][8])

This is your best “ecosystem-wide” ingest (CVE/GHSA intelligence) surface.

#### Repository security advisories (maintainer disclosure workflows)

* Create a repo advisory requires repo security manager or admin, and requires structured fields like `summary`, `description`, and `vulnerabilities`. ([GitHub Docs][9])
* “Request a CVE” is supported, but **you can request a CVE for public repos only**. ([GitHub Docs][9])
* Several endpoints can return `422` for “validation failed, or the endpoint has been spammed” — again, treat this as queue/backpressure. ([GitHub Docs][9])

PyGithub maps the repository advisories endpoints + many advisory mutations (accept report, publish, close, request CVE, etc.) in its API index. ([pygithub.readthedocs.io][1])

---

### 10.6 Best-in-class implementation pattern (what to build in your codebase)

**Unify everything as “security signals” with stable IDs + watermarks:**

* **Dependabot**: key by `(repo, alert_number)`; watermark on `updated_at`; filter on `state` and `severity` for incremental sync. ([GitHub Docs][3])
* **Code scanning**: key by `(repo, alert_number)`; use `sort=updated` and cursor pagination; store `most_recent_instance.commit_sha` to anchor to code snapshots. ([GitHub Docs][5])
* **Secret scanning**: key by `(repo, alert_number)`; store `state/resolution/assigned_to`; load locations lazily for open alerts. ([GitHub Docs][6])
* **Advisories**: global key by `ghsa_id`; repo advisories also keyed by `ghsa_id`. ([GitHub Docs][8])

**Control-plane gating:** check `security_and_analysis` to know which repos can even produce certain streams (and remember you may not see it without the right org/repo privileges). ([GitHub Docs][2])

---

## Deep dive 11 — Checks API plane (check runs/suites, annotations, requested actions)

This plane is “publish rich CI results back to GitHub” (with line-level annotations in PR UI).

### 11.1 Mental model (how GitHub thinks about checks)

* A **check suite** is a collection of check runs created by a single GitHub App for a specific commit. ([GitHub Docs][10])
* A **check run** is one individual test in that suite, with a status + conclusion, and optional rich output + annotations. ([GitHub Docs][10])
* Only GitHub Actions can set certain statuses like `requested`, `waiting`, or `pending`. ([GitHub Docs][10])
* GitHub retains checks data for **400 days**, then archives and deletes it shortly after. ([GitHub Docs][10])

### 11.2 Permissions and auth (the non-negotiable part)

GitHub’s check run docs are explicit:

* **Write permission for checks is only available to GitHub Apps**; OAuth apps and authenticated users can view but not create. ([GitHub Docs][11])
* If you aren’t building a GitHub App, consider commit statuses instead. ([GitHub Docs][11])

### 11.3 PyGithub surfaces you’ll use

From the PyGithub API index:

* `Repository.create_check_run()`, `Repository.get_check_run()`, `CheckRun.edit()`, `CheckRun.get_annotations()` ([pygithub.readthedocs.io][1])
* `Repository.create_check_suite()`, `Repository.update_check_suites_preferences()`, `Repository.get_check_suite()`, `CheckSuite.get_check_runs()`, `CheckSuite.rerequest()` ([pygithub.readthedocs.io][1])
* commit-scoped: `Commit.get_check_runs()` ([pygithub.readthedocs.io][1])

### 11.4 Check run lifecycle (golden pattern)

1. Receive `check_suite` webhook (or your own trigger), create a run as `queued` or `in_progress`
2. Update status over time; complete with `status="completed"` and a `conclusion`
3. Attach output (`title`, `summary`, optional `text`) and annotations

The docs explicitly endorse creating early, then updating as details become available. ([GitHub Docs][10])

### 11.5 Annotations: limits + batching strategy

GitHub’s check runs API has the core rule you build around:

* `output.annotations` max **50 annotations per API request**; to exceed, make multiple **Update a check run** calls and annotations are appended. ([GitHub Docs][11])
* GitHub Actions UI annotations are limited to **10 warnings and 10 errors per step**, which is why publishing via Checks API is often the escape hatch. ([GitHub Docs][11])

So the best-in-class approach is:

* compute all findings
* chunk annotations into batches of 50
* call `CheckRun.edit(...)` repeatedly to append

### 11.6 Requested actions + reruns

Checks can expose “buttons” (requested actions):

* include an `actions` array (max 3) in the check run output; user clicks → GitHub sends `check_run.requested_action` webhook. ([GitHub Docs][11])
* reruns: users can re-run checks; you’ll get webhooks requesting a new run. ([GitHub Docs][10])

### 11.7 Check suites: when you ever create them manually

By default, suites are auto-created when you create check runs. You only create check suites manually if you disabled automatic suite creation via “update preferences.” ([GitHub Docs][12])
Also: a GitHub App usually receives one `check_suite` event per commit SHA even if pushed to multiple branches. ([GitHub Docs][12])

---

If you want to continue in the same style, the next natural deep-dive after these two is: **“Security plane ↔ Checks plane integration”**: turning security findings (Dependabot/code scanning/secret scanning) into **PR check runs** with annotations + summaries, while keeping it mutation-safe and rate-limit-safe.

[1]: https://pygithub.readthedocs.io/en/latest/apis.html "APIs — PyGithub 0.1.dev1+g19e1c5032 documentation"
[2]: https://docs.github.com/rest/repos/repos "REST API endpoints for repositories - GitHub Docs"
[3]: https://docs.github.com/en/rest/dependabot/alerts "REST API endpoints for Dependabot alerts - GitHub Docs"
[4]: https://docs.github.com/enterprise-cloud%40latest/rest/dependabot/secrets?utm_source=chatgpt.com "REST API endpoints for Dependabot secrets"
[5]: https://docs.github.com/en/rest/code-scanning/code-scanning "REST API endpoints for code scanning - GitHub Docs"
[6]: https://docs.github.com/en/rest/secret-scanning/secret-scanning "REST API endpoints for secret scanning - GitHub Docs"
[7]: https://github.blog/changelog/2024-08-06-secret-scanning-alerts-for-non-provider-patterns-and-passwords-are-retrievable-with-the-rest-api/ "Secret scanning alerts for non-provider patterns and passwords are retrievable with the REST API - GitHub Changelog"
[8]: https://docs.github.com/en/rest/security-advisories/global-advisories "REST API endpoints for global security advisories - GitHub Docs"
[9]: https://docs.github.com/en/rest/security-advisories/repository-advisories "REST API endpoints for repository security advisories - GitHub Docs"
[10]: https://docs.github.com/en/rest/guides/using-the-rest-api-to-interact-with-checks "Using the REST API to interact with checks - GitHub Docs"
[11]: https://docs.github.com/rest/checks/runs "REST API endpoints for check runs - GitHub Docs"
[12]: https://docs.github.com/en/rest/checks/suites "REST API endpoints for check suites - GitHub Docs"

## Deep dive 12 — GraphQL support (selective, via `Requester`)

### 12.1 What “selective GraphQL support” means in PyGithub

PyGithub is primarily a **GitHub REST API v3** client, but it exposes GraphQL because GitHub ships some functionality “behind” GraphQL and because GraphQL can **reduce round-trips and response size** by letting you choose exactly what fields you want. PyGithub’s design goal is: *use GraphQL when needed, but still return the usual object-oriented PyGithub objects when possible*. ([pygithub.readthedocs.io][1])

A key pattern is **GraphQL-only schemas** with no REST equivalent. PyGithub implements these as GraphQL classes that **translate GraphQL attributes into REST-style attributes** (so downstream code feels like normal PyGithub). The docs call out `RepositoryDiscussion` as the canonical example. ([pygithub.readthedocs.io][1])

---

### 12.2 The entrypoint: `object.requester` (your “escape hatch”)

PyGithub exposes a low-level `Requester` on *most* objects (e.g., repo, issue, PR): if an endpoint isn’t wrapped (or GraphQL is more ergonomic), you call GraphQL through that requester. ([pygithub.readthedocs.io][2])

The GraphQL surface on `Requester` is:

* `graphql_query()` / `graphql_query_class()`
* `graphql_node()` / `graphql_node_class()`
* `graphql_named_mutation()` / `graphql_named_mutation_class()` ([pygithub.readthedocs.io][1])

---

## 12.3 Query methods: when to use which

### A) `graphql_query(query, variables) -> (headers, json)`

This is the “raw dict” API: you send a query string + variables dict, and you get back response headers plus the parsed JSON response. It raises `GithubException` for error status codes. ([pygithub.readthedocs.io][3])

**Use when:** you want full control, and you’re fine handling the JSON payload yourself.

```python
QUERY = """
query($owner:String!, $name:String!, $number:Int!) {
  repository(owner:$owner, name:$name) {
    discussion(number:$number) {
      id
      number
      title
      url
      updatedAt
    }
  }
}
"""

headers, data = repo.requester.graphql_query(
    QUERY,
    {"owner": repo.owner.login, "name": repo.name, "number": 123},
)
disc = data["data"]["repository"]["discussion"]
```

### B) `graphql_query_class(query, variables, data_path, klass) -> klass instance`

Same idea, but PyGithub will **extract a nested object** from the JSON response (via `data_path`) and **populate a PyGithub class instance** with those properties. ([pygithub.readthedocs.io][3])

**Use when:** you want to stay in “PyGithub objects”, but you’re sourcing those objects from GraphQL.

> Think of `data_path` as the JSON pointer for where the object lives, e.g. `["data", "repository", "discussion"]`. ([pygithub.readthedocs.io][3])

---

## 12.4 Node lookup: `graphql_node*` (the “fetch by global ID” pattern)

### A) `graphql_node(node_id, output_schema, node_type) -> (headers, json)`

This is a convenience wrapper around the canonical GraphQL pattern:

* query `node(id: $id) { __typename ... on <Type> { <fields> } }`

PyGithub literally documents the shape it sends, and it requires:

* a node id
* a selection set (`output_schema`)
* a GraphQL node type (`node_type`) ([pygithub.readthedocs.io][3])

(Their doc snippet shows `$id. ID!` which is clearly intended to be `$id: ID!`—GraphQL uses `:`.)

**Use when:** you already have a GraphQL global node id (common in discussions/projects v2), and you want a consistent retrieval surface.

### B) `graphql_node_class(node_id, output_schema, klass, node_type=None) -> klass instance`

Same as above, but returns a populated PyGithub instance of the requested class; `node_type` defaults to the class name. ([pygithub.readthedocs.io][3])

---

## 12.5 Mutations: `graphql_named_mutation*` (a structured way to write)

### A) `graphql_named_mutation(mutation_name, mutation_input, output_schema) -> (headers, json)`

PyGithub provides a “named mutation” helper that builds the standard shape:

```graphql
mutation Mutation($input: MutationNameInput!) {
  mutationName(input: $input) { <output_schema> }
}
```

…and it sends this using `graphql_query()`. ([pygithub.readthedocs.io][2])

### B) `graphql_named_mutation_class(...) -> klass instance`

This one extracts a named `item` from the mutation result and populates a PyGithub class instance. ([pygithub.readthedocs.io][2])

**Use when:** you want typed PyGithub objects back from GraphQL writes (e.g., discussion comment objects).

**Version landmine:** in PyGithub **2.5.0**, parameters of `Requester.graphql_named_mutation` were renamed (`variables` → `mutation_input`, `output` → `output_schema`, and the default output was removed). If you’re copying older snippets, they may be stale. ([pygithub.readthedocs.io][4])

---

## 12.6 Pagination: two layers (GitHub cursors + PyGithub `PaginatedList` expectations)

### GitHub GraphQL pagination basics

GitHub GraphQL uses **cursor-based pagination** on “connections”:

* you must provide `first` or `last` for any connection
* values must be **1–100**
* you page using `pageInfo { endCursor hasNextPage }` + an `after:` argument ([GitHub Docs][5])

### PyGithub’s `PaginatedList` + GraphQL

PyGithub can paginate GraphQL results via `PaginatedList`, but it requires that the GraphQL response includes:

* `totalCount`
* `pageInfo { startCursor endCursor hasNextPage hasPreviousPage }` ([pygithub.readthedocs.io][1])

This is why, when you author GraphQL queries meant to be paged, you **must include those fields** in the response selection set (even if you don’t personally care about them).

Also: PyGithub documents that `PaginatedList` abstracts both REST and GraphQL pagination, but `get_page()` is not supported for GraphQL-backed lists. ([pygithub.readthedocs.io][2])

---

## 12.7 Rate limits + “cost discipline” for GraphQL

GitHub GraphQL uses a **point-based primary rate limit**, with limits depending on auth type (e.g., users typically 5,000 points/hour; GitHub App installations have their own rules; `GITHUB_TOKEN` in Actions is lower). ([GitHub Docs][6])

Best practice:

* Use **response headers** (`x-ratelimit-*`) to track remaining budget.
* GitHub notes you *can* query the `rateLimit` object, but says: **prefer headers when possible**. ([GitHub Docs][6])

Secondary limits still apply to GraphQL too (including **≤ 100 concurrent requests** shared across REST+GraphQL, and per-minute point ceilings). ([GitHub Docs][6])

---

## 12.8 GHES and “it works on GitHub.com but not Enterprise” gotchas

PyGithub added logic to **derive the GraphQL URL from `base_url`** (important for GitHub Enterprise setups). ([pygithub.readthedocs.io][4])
So: if you’re on GHES, ensure you’re setting `Github(base_url=...)` correctly (as you did in the client-config deep dive), and PyGithub should route GraphQL appropriately.

Another real-world footgun: older versions had bugs around GraphQL variables; PyGithub’s changelog includes a fix for “GraphQL Queries with Variables.” ([pygithub.readthedocs.io][4])

---

## 12.9 When GraphQL is the right lever (vs REST)

Use GraphQL in PyGithub when:

* the REST API is missing/awkward for what you need (classic examples: **Discussions**, and sometimes newer features)
* you want to pull a “wide” object graph in one call (e.g., discussion + author + labels + comments summary)
* you’re syncing “delta views” where you only need a handful of scalar fields per item

Discussions are the textbook case: GitHub explicitly documents Discussions operations through the GraphQL API (get/create/edit/delete), and they require appropriate token scopes. ([GitHub Docs][7])

---

If you want the next deep dive after this, the most natural “build on GraphQL support” is: **a Repo Discussions end-to-end cookbook** (list discussions with cursor paging + watermarking, fetch comments lazily, add/edit/delete comments via `graphql_named_mutation`, and return everything as a deterministic snapshot you can diff).

[1]: https://pygithub.readthedocs.io/en/stable/graphql.html "Github GraphQL — PyGithub 0.1.dev50+gecd47649e documentation"
[2]: https://pygithub.readthedocs.io/en/latest/utilities.html "Utilities — PyGithub 0.1.dev1+g19e1c5032 documentation"
[3]: https://pygithub.readthedocs.io/en/v2.8.1/utilities.html "Utilities — PyGithub 2.8.1 documentation"
[4]: https://pygithub.readthedocs.io/en/v2.7.0/changes.html "Change log — PyGithub 2.7.0 documentation"
[5]: https://docs.github.com/en/enterprise-server%403.14/graphql/guides/using-pagination-in-the-graphql-api "Using pagination in the GraphQL API - GitHub Enterprise Server 3.14 Docs"
[6]: https://docs.github.com/en/graphql/overview/rate-limits-and-query-limits-for-the-graphql-api "Rate limits and query limits for the GraphQL API - GitHub Docs"
[7]: https://docs.github.com/en/graphql/guides/using-the-graphql-api-for-discussions "Using the GraphQL API for Discussions - GitHub Docs"

## Deep dive 13 — Error model & exceptions (what you catch)

### 13.1 Exception taxonomy (PyGithub’s “contract”)

PyGithub centralizes API failures into `GithubException(status, data, headers, message)` where:

* `status` = HTTP status code
* `data` = decoded response payload (often a dict with `message`, `documentation_url`, etc.)
* `headers` = response headers (important for `x-ratelimit-*`, `retry-after`, etc.) ([PyGithub][1])

It then exposes specific subclasses for common cases (all carry the same `status/data/headers/message` shape): ([PyGithub][1])

* `BadCredentialsException` (bad credentials; API replies 401 or 403 HTML status)
* `UnknownObjectException` (404 “not found”)
* `BadUserAgentException` (403 bad user-agent)
* `RateLimitExceededException` (403 rate limit exceeded HTML status)
* `TwoFactorException` (OTP required)
* `IncompletableObject` (object cannot be completed because API data lacks a URL)
* `BadAttributeException` (parsing/type mismatch) with extra fields: `actual_value`, `expected_type`, `transformation_exception` ([PyGithub][1])

Also note: network/TLS/etc. failures can bubble up from underlying libs and won’t necessarily be `GithubException`. ([PyGithub][1])

---

### 13.2 “Best-in-class” catching strategy (practical and non-fragile)

**Rule 1: Catch the semantic exceptions first, then fall back to `GithubException`.**

```python
from github.GithubException import (
    GithubException,
    UnknownObjectException,
    BadCredentialsException,
    RateLimitExceededException,
)

def safe_get_repo(gh, full_name: str):
    try:
        return gh.get_repo(full_name)
    except UnknownObjectException:
        return None  # 404: repo not found / no access in some cases
    except BadCredentialsException as e:
        raise RuntimeError("GitHub auth failed") from e
    except RateLimitExceededException as e:
        # handle backoff using headers (see below)
        raise
    except GithubException as e:
        # everything else: permissions, validation, abuse/secondary limits, etc.
        raise
```

**Rule 2: Don’t assume `RateLimitExceededException` is the only “rate-limit” shape.**
GitHub can throttle with secondary limits using **403 or 429**; the recommended handling is driven by `retry-after` / `x-ratelimit-reset` / “wait at least a minute then exponential backoff.” ([GitHub Docs][2])

So for robust handling, treat *any* `GithubException` with:

* `status in (403, 429)` and a `retry-after` header as “back off”
* `x-ratelimit-remaining == 0` as “sleep until reset”
* otherwise follow GitHub’s “wait ≥ 60s, then exponential” guidance ([GitHub Docs][3])

---

### 13.3 Decoding error payloads into actionable signals

Because `GithubException.data` is the decoded body, you can reliably extract:

* `message` (human-readable reason)
* `documentation_url` (often points to the relevant rule/limit)
* plus endpoint-specific keys (validation errors, etc.) ([PyGithub][1])

A tiny “normalize” helper you can standardize across your codebase:

```python
def classify_github_error(e) -> str:
    status = getattr(e, "status", None)
    data = getattr(e, "data", None) or {}
    msg = (data.get("message") or "").lower()

    if status == 404:
        return "not_found"
    if status in (401, 403) and "bad credentials" in msg:
        return "auth"
    if status in (403, 429) and ("rate limit" in msg or "secondary rate limit" in msg):
        return "rate_limit"
    if status == 422:
        return "validation"
    if status == 403:
        return "forbidden"
    if status and status >= 500:
        return "server_error"
    return "other"
```

---

### 13.4 Retry/backoff: align with GitHub’s rules + PyGithub’s retry engine

**GitHub’s rule-of-thumb ladder (REST):**

* if `retry-after` exists → wait that many seconds
* else if `x-ratelimit-remaining == 0` → wait until `x-ratelimit-reset` (epoch seconds)
* else wait ≥ 60 seconds; if secondary limit persists, exponential backoff and stop after N retries ([GitHub Docs][3])

**PyGithub’s `GithubRetry`** (a GitHub-tuned `urllib3.Retry`) helps here:

* retries 403 when retryable (has `Retry-After` or content indicates a rate-limit error)
* retries 403 and 500–599 by default (configurable via `status_forcelist`)
* retries default allowed methods + GET and POST (configurable via `allowed_methods`)
* includes `secondary_rate_wait` knob ([PyGithub][4])

That means “best default” is:

* keep retries enabled
* add your own *outer* backoff for write-heavy tasks (so you never spam)

---

## Deep dive 14 — Extensibility & “power user” patterns

### 14.1 Raw endpoint access via `requester` (the escape hatch)

PyGithub explicitly supports calling endpoints it doesn’t yet wrap via:

* `Github.requester` (“make requests to API endpoints not yet supported”) ([PyGithub][5])
* and more generally `object.requester` on most PyGithub objects ([PyGithub][1])

The key “power” methods are: ([PyGithub][1])

* `requestJsonAndCheck(verb, url, parameters=None, headers=None, input=None, follow_302_redirect=False) -> (headers, json)`
* `requestJson(...) -> (status, headers, body)` (raw body string; optional `follow_302_redirect`)
* `getStream(url, ...) -> (status, headers, stream_iterator)` for large downloads
* `requestMultipartAndCheck`, `requestBlobAndCheck`, `requestMemoryBlobAndCheck` for uploads

**Why this matters:** you can add support for any new GitHub endpoint *without* adding a new dependency or waiting for PyGithub to ship wrappers, while still reusing auth/base_url/retry/pooling.

Example: call a not-yet-wrapped REST endpoint:

```python
def gh_api_json(gh, verb: str, path: str, *, params=None, headers=None, body=None):
    # path can be like "/repos/{owner}/{repo}/something"
    resp_headers, data = gh.requester.requestJsonAndCheck(
        verb, path, parameters=params, headers=headers, input=body
    )
    return resp_headers, data
```

Two “best practice” notes:

* Use `headers=` to set custom `Accept` media types or the API version header when GitHub requires it.
* Use `follow_302_redirect=True` (supported on `requestJsonAndCheck` / `requestJson`) for endpoints that respond with redirects. ([PyGithub][1])

---

### 14.2 GraphQL as a backstop (still inside PyGithub)

All GraphQL entrypoints live on `Requester` (`graphql_query`, `graphql_node`, `graphql_named_mutation`, plus “_class” helpers) and raise `GithubException` on error status codes. ([PyGithub][1])
So your standardization pattern is:

* REST wrapper when available
* `requester.requestJsonAndCheck` when REST endpoint exists but PyGithub doesn’t wrap it
* GraphQL when REST is missing/awkward (e.g., some Discussions/Projects v2 workflows)

---

### 14.3 Operational tuning you should standardize (one “client factory”)

PyGithub exposes the knobs you typically want in a production distribution:

* `per_page`, `timeout`, `verify`
* `retry` (int or `urllib3.Retry` / `GithubRetry`, or `None` to disable)
* `pool_size`
* `seconds_between_requests`, `seconds_between_writes`
* plus `close()` / context manager lifetime management ([PyGithub][5])

A clean “one place” factory:

```python
from github import Github, Auth
from github.GithubRetry import GithubRetry

def make_github(token: str, *, base_url: str = "https://api.github.com") -> Github:
    return Github(
        auth=Auth.Token(token),
        base_url=base_url,
        per_page=100,
        retry=GithubRetry(total=10, secondary_rate_wait=60),
        seconds_between_requests=0.25,
        seconds_between_writes=1.0,
    )
```

This aligns with PyGithub’s documented configuration surface and its GitHub-tuned retry behavior. ([PyGithub][5])

---

### 14.4 A “golden” power-user wrapper (recommended internal contract)

If you want to make this library-like and consistent:

* **`gh.rest_json(path, …)`**: uses `requestJsonAndCheck`, returns `(headers, data)`
* **`gh.stream(path, …)`**: uses `getStream`, returns bytes or iterator
* **`gh.graphql(query, vars)`**: uses `graphql_query`
* **`gh.handle_error(e)`**: central classifier + backoff rules

That gives you a stable internal API even as PyGithub adds/removes wrappers over time.


[1]: https://pygithub.readthedocs.io/en/latest/utilities.html "Utilities — PyGithub 0.1.dev1+g19e1c5032 documentation"
[2]: https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api?utm_source=chatgpt.com "Rate limits for the REST API"
[3]: https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api?utm_source=chatgpt.com "Best practices for using the REST API"
[4]: https://pygithub.readthedocs.io/en/v2.1.0/github_objects/GithubRetry.html "GithubRetry — PyGithub 2.1.0 documentation"
[5]: https://pygithub.readthedocs.io/en/latest/github.html "Main class: Github — PyGithub 0.1.dev1+g19e1c5032 documentation"
