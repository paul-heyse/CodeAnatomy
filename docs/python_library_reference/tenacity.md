Below is a practical, **end‑to‑end reference to the `tenacity` Python library**—aimed at replacing home‑grown retry code in an AI/automation stack. It covers the mental model, core API, sync/async usage, composition patterns, logging/observability, migration tips, and footguns.

> **What is Tenacity?**
> Tenacity is a general‑purpose retrying library (Apache‑2.0) that adds robust retry behavior to *anything*—functions, coroutines, or even arbitrary code blocks—using a small set of composable strategies: **when to stop**, **how long to wait**, **what to retry on**, and **what to do before/after attempts**. It supports sync and async natively. ([Tenacity][1])

---

## Installation & Compatibility

```bash
pip install tenacity
```

* As of **Oct 20, 2025**, latest on PyPI is **9.1.2** (released Apr 2 2025).
  **Requires Python ≥ 3.9** and has classifiers up to 3.13. ([PyPI][2])
* Recent release notes indicate minor API nips/tucks (e.g., `BaseRetrying.copy` return type) and dropped Python 3.8 support in the 9.1.x line. ([GitHub][3])

---

## Core mental model

A Tenacity controller wraps a call and keeps trying until a **stop** condition is met. Between attempts it uses a **wait** strategy. It decides to retry based on a **retry** predicate (exceptions and/or results). On each attempt, it can run **before/after/before_sleep** hooks, and on final failure it raises `RetryError` (or re‑raises the last exception if `reraise=True`). ([Tenacity][4])

---

## The two ways to use Tenacity

### 1) Decorator (most common)

```python
from tenacity import retry, stop_after_attempt, wait_random_exponential

@retry(
    stop=stop_after_attempt(6),                # up to 6 tries
    wait=wait_random_exponential(0.5, max=60),# "full-jitter" backoff
    reraise=True
)
def call_api():
    ...
```

The decorator builds a `Retrying` (or async equivalent) controller for you. Defaults are “retry forever, no wait, on any `Exception`,” which you should almost always override. ([Tenacity][1])

### 2) “Code block” style (great for complex flows)

```python
from tenacity import Retrying, RetryError, stop_after_attempt

try:
    for attempt in Retrying(stop=stop_after_attempt(3)):
        with attempt:
            # any statements here are retried together
            do_one_step()
            do_another_step()
except RetryError:
    handle_failure()
```

This style lets you **retry arbitrary blocks** without extracting them into a helper function. There’s also **`AsyncRetrying`** with `async for` for coroutines. ([Tenacity][1])

---

## The building blocks (API reference)

> Each knob below is a constructor/function you pass into `@retry(...)` or to a `Retrying/AsyncRetrying` controller. Full taxonomy lives in the official API reference. ([Tenacity][4])

### Stop strategies (`stop=...`)

* `stop_after_attempt(n)` – give up after *n* tries.
* `stop_after_delay(t)` – give up after *t* seconds from first attempt (also accepts `datetime.timedelta`).
* `stop_any(a, b, ...)` / `stop_all(a, b, ...)` – logical composition; you can also use `|` (“any”) in simple cases.
* `stop_when_event_set(event)` – externally cancel when a `threading.Event` is set (great for deadline budgets from a parent workflow). ([Tenacity][4])

### Wait strategies (`wait=...`)

* `wait_none()` – no delay.
* `wait_fixed(s)` – constant delay.
* `wait_incrementing(start, increment, max)` – linear backoff.
* `wait_exponential(multiplier=1, min=0, max=...)` – deterministic exponential backoff, **no jitter**.
* `wait_random(min, max)` – random within a range.
* `wait_random_exponential(multiplier=1, min=0, max=...)` – **full‑jitter exponential** (preferred to reduce thundering herds). In **9.0.0**, the `min` argument behavior was corrected; upgrade if you rely on it. ([Tenacity][4])
* `wait_exponential_jitter(initial=1, max=..., exp_base=2, jitter=1)` – Google‑style backoff with additive jitter. ([Tenacity][4])
* Advanced: `wait_chain(...)` (piecewise waits), `wait_combine(...)` (run multiple waiters), or **add** waits (`wait_fixed(3) + wait_random(0, 2)`) to apply both. ([Tenacity][4])

> **Tip:** `wait=` can also be a **callable** `(retry_state) -> seconds` if you want to implement “respect `Retry-After` header” or other custom pacing. ([Tenacity][5])

### Retry conditions (`retry=...`)

* **By exception type/message/causes**

  * `retry_if_exception_type(exc_types)` – *positive allow‑list* of exceptions.
  * `retry_unless_exception_type(exc_types)` / `retry_if_not_exception_type(exc_types)` – retry everything **except** these types.
  * `retry_if_exception(predicate)` – custom check on a raised exception.
  * `retry_if_exception_message(message=..., match=...)` – retry if message equals or matches; since **9.1.1** `match` accepts `re.Pattern`. ([Tenacity][4])
  * `retry_if_exception_cause_type(exc_types)` – retry if any `__cause__` in the exception chain matches. ([Tenacity][4])
* **By return value**

  * `retry_if_result(predicate)` / `retry_if_not_result(predicate)` – drive retries by the function’s output (e.g., incomplete JSON, status code, sentinel). ([Tenacity][1])
* **Compose** with `retry_any(...)`, `retry_all(...)` or the `|` operator for simple “or” combinations. ([Tenacity][4])

### Hooks, errors & introspection

* `before=...`, `after=...` – run callbacks around **every** attempt (e.g., start/stop spans).
* `before_sleep=...` – run **only** before a delayed retry (useful for logging warnings); built‑ins: `before_log`, `after_log`, `before_sleep_log(exc_info=True)` for stack traces. ([Tenacity][1])
* `TryAgain` – raise from inside your function to force a retry **now**. ([Tenacity][1])
* `reraise=True` – on final failure, re‑raise the last exception instead of `RetryError`. ([Tenacity][1])
* `retry_error_callback` – return a fallback value on final failure (no exception). ([Tenacity][1])
* `RetryCallState` – available to hooks and internal logic: attempt number, elapsed time, and `outcome` (either a value or an exception). In block style you can set the outcome to a computed **result** so `retry_if_result(...)` can see it. ([Tenacity][1])
* **Statistics** – access runtime stats collected by the wrapper.
  ⚠️ **Breaking change note:** around **8.5.0 → 9.x** the location/shape of the statistics exposed on wrapped functions changed (a change significant enough that 9.0.0 was tagged “to warn API breakage on statistics attribute”). If your code inspects `wrapped.retry.statistics`, check your version and prefer a defensive accessor (see below). ([GitHub][6])

---

## Sync/Async parity (and how to do it safely)

* The `@retry` decorator works for **sync and `async def`** functions; sleeps are awaited for coroutines. You can also use **`AsyncRetrying`** in “code block” style (`async for attempt in AsyncRetrying(...)`). ([Tenacity][1])
* For non‑`asyncio` event loops, pass `sleep=trio.sleep` (etc.) to integrate properly. ([Tenacity][1])

> **Async footgun:** Avoid patterns that might swallow `asyncio.CancelledError`. Prefer **allow‑listing** retryable exceptions (e.g., `aiohttp.ClientError`, `asyncio.TimeoutError`) with `retry_if_exception_type(...)` instead of “retry unless X” when working with cancellations; there’s an open issue about `retry_if_not_exception_type` and `CancelledError`. ([GitHub][7])

---

## Practical patterns (drop‑in replacements for typical custom logic)

### 1) Resilient HTTP (sync: `requests`)

```python
import logging, requests, re
from tenacity import (
    retry, stop_after_delay, wait_random_exponential, retry_if_exception_type,
    before_sleep_log, reraise
)

log = logging.getLogger("http")

@retry(
    stop=stop_after_delay(30),
    wait=wait_random_exponential(multiplier=0.5, max=20),
    retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
    before_sleep=before_sleep_log(log, logging.WARNING, exc_info=True),
    reraise=True,
)
def get_json(url):
    r = requests.get(url, timeout=10)
    # Treat 429/5xx as retryable transient failures
    if r.status_code in {429, 500, 502, 503, 504}:
        r.raise_for_status()  # triggers retry via exception
    return r.json()
```

Uses **full‑jitter exponential** backoff (best practice in distributed systems), a **deadline**, and explicit **retryable exception types**. ([Tenacity][4])

### 2) Resilient HTTP (async: `aiohttp`), result‑driven retry

```python
import asyncio, aiohttp, logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_result, reraise

def retry_on_status(resp):
    return getattr(resp, "status", 0) in {429, 500, 502, 503, 504}

@retry(
    stop=stop_after_attempt(6),
    wait=wait_exponential(multiplier=1, min=1, max=10),  # deterministic backoff
    retry=retry_if_result(retry_on_status),               # uses function result
    reraise=True,
)
async def fetch(session, url):
    return await session.get(url, timeout=10)

async def fetch_json(url):
    async with aiohttp.ClientSession() as session:
        resp = await fetch(session, url)
        if retry_on_status(resp):
            # Should never reach: Tenacity will retry above when predicate is True.
            ...
        return await resp.json()
```

Here we **base retries on the returned response** rather than exceptions. ([Tenacity][4])

### 3) Respect `Retry-After` (custom `wait=` function)

```python
import email.utils, time
from tenacity import retry, stop_after_delay

def wait_on_retry_after(retry_state):
    exc = retry_state.outcome.exception()
    # adapt to your HTTP client; example assumes exc.response.headers
    retry_after = None
    if hasattr(exc, "response"):
        ra = exc.response.headers.get("Retry-After")
        if ra:
            try:
                retry_after = int(ra)
            except ValueError:
                # HTTP-date: parse to seconds from now
                dt = email.utils.parsedate_to_datetime(ra)
                retry_after = max(0, (dt - dt.now(dt.tzinfo)).total_seconds())
    return retry_after if retry_after is not None else 0

@retry(stop=stop_after_delay(60), wait=wait_on_retry_after, reraise=True)
def call_with_retry_after():
    ...
```

Tenacity explicitly allows `wait=` to be a callable taking `RetryCallState`. ([Tenacity][5])

### 4) Block‑style retries for multi‑step flows

```python
from tenacity import Retrying, stop_after_attempt

def workflow():
    for attempt in Retrying(stop=stop_after_attempt(3), reraise=True):
        with attempt:
            validate_inputs()
            save_to_db()          # transient DB/network errors retried
            emit_event()          # part of the same atomic “attempt”
```

This mirrors “try the **whole** subroutine” patterns from custom frameworks. ([Tenacity][1])

---

## Logging & observability

* Plug `before_sleep_log(logger, level, exc_info=True)` to record **only** attempts that will sleep & retry (less noise than logging every attempt). `before_log`/`after_log` log around every try, respectively. ([Tenacity][1])
* Custom hooks receive a `RetryCallState` with `attempt_number`, `seconds_since_start`, and `outcome` (use `outcome.result()` or `outcome.exception()`). ([Tenacity][1])

---

## Introspection & statistics (and how not to get bit)

If you’ve written custom retry counters, you’ll likely want to read the **statistics** exposed by Tenacity’s wrapper:

```python
def get_stats(fn):
    # works across Tenacity 8.4.x and >= 8.5/9.x
    stats = getattr(fn, "statistics", None)
    if stats is None and hasattr(fn, "retry"):
        stats = getattr(fn.retry, "statistics", None)
    return stats or {}
```

> **Version notes:** In **8.5.0** the `statistics` object moved in a way that was considered API‑breaking by users; **9.0.0** was then released “to warn API breakage on statistics attribute”. There are also recent issues around empty or missing stats when functions are wrapped. If you rely on stats for behavior, prefer **hooks** (e.g., increment your own counters in `after=`) over direct access. ([GitHub][6])

---

## Advanced composition cheatsheet

* **Combine waits:**
  `wait_fixed(3) + wait_random(0, 2)` → “at least 3s plus up to 2s jitter.” ([Tenacity][1])
* **Chain waits:**
  `wait_chain(wait_fixed(1), wait_fixed(2), wait_fixed(5))` → 1s, 1s, 2s, 2s, 5s, 5s… ([Tenacity][4])
* **Combine retry conditions:**
  `(retry_if_result(is_empty) | retry_if_exception_type(MyTransientErr))` → retry on either condition. ([Tenacity][1])
* **External cancellation:**
  `stop_when_event_set(event)` and/or `sleep_using_event(event)` to wake early. ([Tenacity][4])
* **Force a retry from inside:**
  `raise TryAgain` (use sparingly; predicates are clearer). ([Tenacity][1])
* **Change policy at call time:**
  `fn.retry_with(stop=..., wait=...)()` to override decorator arguments dynamically. ([Tenacity][1])

---

## Safe defaults for AI/LLM agents & API clients

When refactoring agent code that talks to APIs or tools, these **defaults** map well to production heuristics:

```python
from tenacity import retry, stop_after_delay, wait_random_exponential
from tenacity import retry_if_exception_type, before_sleep_log
import logging, httpx, asyncio

log = logging.getLogger("agent")

RetryOn = (httpx.TimeoutException, httpx.TransportError)

@retry(
    stop=stop_after_delay(30),                           # overall budget
    wait=wait_random_exponential(multiplier=0.5, max=8),# full jitter
    retry=retry_if_exception_type(RetryOn),              # allow-list exceptions
    before_sleep=before_sleep_log(log, logging.WARNING, exc_info=True),
    reraise=True,
)
def tool_call(...):
    ...
```

* **Full‑jitter exponential backoff** reduces synchronized retries across many agents.
* **Overall deadline** (not just attempts) avoids runaway latency.
* **Allow‑list** retryable exceptions (timeouts, transient connection failures).
* Use **result‑based** predicates for *semantically transient* conditions (e.g., incomplete search results, 429/rate limits). ([Tenacity][4])

> If you want an opinionated wrapper with “do‑the‑right‑thing” defaults on top of Tenacity, see **`stamina`** (open‑source wrapper by Hynek). It can be a lightweight drop‑in for many agents. ([GitHub][8])

---

## Migration guide: mapping common custom features to Tenacity

| If your custom code does this…                | Tenacity equivalent                                                                                     |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| “Retry N times then give up”                  | `stop_after_attempt(N)`                                                                                 |
| “Give up after T seconds total”               | `stop_after_delay(T)`                                                                                   |
| “Exponential backoff with jitter”             | `wait_random_exponential(...)` (full jitter), or `wait_exponential_jitter(...)` (Google style)          |
| “Retry on timeout/connection only”            | `retry_if_exception_type((TimeoutError, ConnectionError, ...))`                                         |
| “Retry on 429/5xx responses”                  | Use `retry_if_result` on the returned response, or raise in your code and use `retry_if_exception_type` |
| “Log only when actually sleeping”             | `before_sleep_log(logger, level, exc_info=True)`                                                        |
| “Run an action before/after every attempt”    | `before=` / `after=` hooks                                                                              |
| “Retry a multi‑step block together”           | `for attempt in Retrying(...): with attempt: ...`                                                       |
| “Force retry based on internal condition”     | `raise TryAgain`                                                                                        |
| “Cancel retries when an outside event occurs” | `stop_when_event_set(event)` and/or `sleep_using_event(event)`                                          |
| “Inspect attempts/elapsed time”               | `RetryCallState` in hooks; defensive access to `statistics` (see snippet above)                         |

(See docs for all named strategies and hooks.) ([Tenacity][4])

---

## Footguns & best practices

* **Never** rely on Tenacity’s decorator defaults (infinite, no wait). Always set `stop=` and `wait=`. ([Tenacity][1])
* Prefer **allow‑listing** exceptions with `retry_if_exception_type` over “retry everything except X”, especially in **async** code to avoid swallowing cancellations. See the `CancelledError` discussion in open issues. ([GitHub][7])
* **Idempotency:** only auto‑retry **idempotent** operations (GETs, queries). For non‑idempotent actions, ensure **at‑least‑once safety** in the callee or add idempotency keys.
* If you depend on `statistics`, **pin a version** or use hooks to maintain your own counters; 8.5.0→9.x changed “statistics attribute” behavior. ([GitHub][3])
* Use **`reraise=True`** in libraries/services so callers see the *real* exception rather than `RetryError`. ([Tenacity][1])
* For *contention* scenarios (many clients hitting one resource), prefer **full‑jitter** (`wait_random_exponential`). For *unavailable resource* scenarios (a single thing becoming available), deterministic `wait_exponential` is OK. ([Tenacity][4])

---

## Version highlights (recent)

* **9.1.x** (latest series): dropped Python 3.8, added small typing and matching improvements (e.g., `re.Pattern` allowed for message `match`). ([GitHub][3])
* **9.0.0**: bumped major “to warn API breakage on statistics attribute”; fixed `wait_random_exponential(min=...)` to actually respect `min`. ([GitHub][3])
* **8.4.x**: shipped `tenacity.asyncio` subpackage in wheels and added async strategies; Trio support out of the box. ([GitHub][3])
* **8.2+**: `before_sleep_log(exc_info=True)` and `timedelta` support for `stop_after_delay`. ([Tenacity][5])

---

## Quick API crib

* **Main:** `retry(...)`, `Retrying(...)`, `AsyncRetrying(...)`, `RetryError`, `TryAgain`. ([Tenacity][4])
* **Stop:** `stop_after_attempt`, `stop_after_delay`, `stop_any`, `stop_all`, `stop_when_event_set`. ([Tenacity][4])
* **Wait:** `wait_none`, `wait_fixed`, `wait_random`, `wait_incrementing`, `wait_exponential`, `wait_random_exponential`, `wait_exponential_jitter`, `wait_chain`, `wait_combine`, `+` composition. ([Tenacity][4])
* **Retry:** `retry_if_exception_type`, `retry_unless_exception_type`, `retry_if_exception`, `retry_if_exception_message`, `retry_if_not_exception_message`, `retry_if_exception_cause_type`, `retry_if_result`, `retry_if_not_result`, `retry_any`, `retry_all`, `|` composition. ([Tenacity][4])
* **Hooks:** `before_log`, `after_log`, `before_sleep_log`, plus custom callables receiving `RetryCallState`. ([Tenacity][1])
* **Nap:** `sleep` (default), `sleep_using_event(event)`. ([Tenacity][4])

---

## Final refactor tips for an AI agent codebase

1. **Centralize policy**: define one or two `@retry` policy decorators for “network I/O” and “compute/polling” and reuse them across tools.
2. **Separate exception policy per integration** (OpenAI, vector DB, Slack, etc.) by allow‑listing that client’s *transient* errors.
3. **Prefer result‑based predicates** for semantically transient states (e.g., “LLM returned incomplete JSON, retry parse step”).
4. **Use block style** (`Retrying(...)`) where you want a *transactional* retry of a multi‑step sequence. ([Tenacity][1])
5. **Add `before_sleep_log`** everywhere for observability and stick `reraise=True` in library code so callers see the real failure on give‑up. ([Tenacity][1])
6. **Guard cancellations** in async flows (see footgun above). ([GitHub][7])

---

### Authoritative sources & further reading

* **Docs (overview + examples + code‑block & async):** Tenacity docs home. ([Tenacity][1])
* **API reference (complete list of strategies & hooks):** Tenacity API. ([Tenacity][4])
* **Changelog (selected entries; async iterator fix; timedelta support; logging options):** Tenacity changelog. ([Tenacity][5])
* **Latest package & Python version support:** PyPI page. ([PyPI][2])
* **Releases (9.x notes incl. statistics attribute and Python 3.8 drop):** GitHub releases. ([GitHub][3])
* **Statistics change context:** GitHub issue #486. ([GitHub][6])

If you want, tell me what your current custom retry utilities look like (exception lists, backoff math, logging), and I’ll translate them 1‑for‑1 into a pair of reusable Tenacity policies for your agent stack.

[1]: https://tenacity.readthedocs.io/ "Tenacity — Tenacity  documentation"
[2]: https://pypi.org/project/tenacity/ "tenacity · PyPI"
[3]: https://github.com/jd/tenacity/releases "Releases · jd/tenacity · GitHub"
[4]: https://tenacity.readthedocs.io/en/latest/api.html "API Reference — Tenacity  documentation"
[5]: https://tenacity.readthedocs.io/en/latest/changelog.html "Changelog — Tenacity  documentation"
[6]: https://github.com/jd/tenacity/issues/486?utm_source=chatgpt.com "8.5.0 includes breaking changes on the statistics object #486"
[7]: https://github.com/jd/tenacity/issues "GitHub · Where software is built"
[8]: https://github.com/hynek/stamina?utm_source=chatgpt.com "hynek/stamina: Production-grade retries for Python"
