Below is a comprehensive, “use-in-production” overview of **Starlette** for Python—covering core concepts, full feature surface, advanced patterns, and deployment tips. I’ve cross‑checked details against the latest official docs and release notes (current train is **0.50.0, Nov 1, 2025**). I also include pragmatic snippets you can adapt directly. ([Starlette][1])

> **Note about your earlier uploads**
> I can’t access the previously uploaded architecture files in this chat state. If you want me to *tailor* Starlette patterns to that architecture (or to your FAISS+cuVS setup), re‑upload those files and I’ll fold them in. For now, this guide is self‑contained and sourced from up‑to‑date, high‑quality references on the web.

---

## 1) Starlette at a glance

**Starlette** is a lightweight **ASGI** framework/toolkit for building high‑performance web services in Python. It provides HTTP routing, WebSockets, background tasks, sessions/cookies, templating, static file serving, test utilities (on **httpx**), and a focused set of middlewares (CORS, GZip, HTTPS-only, Trusted Hosts). It’s “ASGI all the way down,” so every component can run alone or be composed with others. ([Starlette][1])

**Version & currency.** The release line as of today is **0.50.0 (Nov 1, 2025)** (0.49.x shipped a FileResponse range‑parsing security fix). If you’re on older 0.4x, consult the release notes for migration items (e.g., more formal lifespan usage, Python version support). ([Starlette][2])

**ASGI background.** Starlette implements the **ASGI** 3.0 interface: `async app(scope, receive, send)`. Requests come with a **scope** (connection metadata) and asynchronous **receive/send** channels. That’s what makes HTTP, WebSocket, and other protocols first‑class. ([ASGI Documentation][3])

---

## 2) Applications & lifespan (startup/shutdown)

Create the app with `Starlette(...)`. Key parameters include `routes`, `middleware`, `exception_handlers`, and **either** classic `on_startup`/`on_shutdown` **or** modern **`lifespan=`** (use one or the other—prefer `lifespan` today). You can also stash global objects on `app.state`. ([Starlette][4])

```python
import contextlib
from typing import AsyncIterator

from starlette.applications import Starlette
from starlette.responses import PlainTextResponse
from starlette.routing import Route

@contextlib.asynccontextmanager
async def lifespan(app: Starlette) -> AsyncIterator[dict]:
    # e.g., open/close a single HTTP client or DB connection pool
    import httpx
    async with httpx.AsyncClient() as client:
        yield {"http": client}  # becomes request.state.http

async def ping(request):
    client = request.state.http
    r = await client.get("https://example.org")
    return PlainTextResponse(r.text[:60])

app = Starlette(lifespan=lifespan, routes=[Route("/ping", ping)])
```

**Testing with lifespan.** Use `TestClient` as a **context manager** so startup/shutdown handlers run during tests. ([Starlette][5])

---

## 3) Routing & endpoints

Starlette’s router supports **HTTP**, **WebSockets**, **mounting sub‑apps**, host‑based routing, path parameters, reverse URL lookups, and both function‑based and class‑based endpoints. Common route classes: `Route`, `WebSocketRoute`, `Mount`. For class‑based endpoints use `HTTPEndpoint`/`WebSocketEndpoint`. You can `url_path_for(...)` (router) or `request.url_for(...)` (app/request) to reverse URLs. ([Starlette][6])

```python
from starlette.responses import JSONResponse
from starlette.routing import Route, WebSocketRoute, Mount
from starlette.endpoints import HTTPEndpoint
from starlette.staticfiles import StaticFiles

class User(HTTPEndpoint):
    async def get(self, request):
        return JSONResponse({"id": int(request.path_params["user_id"])})

async def ws(ws):
    await ws.accept()
    await ws.send_text("hello")
    await ws.close()

routes = [
    Route("/users/{user_id:int}", User),
    WebSocketRoute("/ws", ws),
    Mount("/static", StaticFiles(directory="static"), name="static"),
]
```

---

## 4) Requests

`Request(scope, receive)` wraps the raw ASGI channels with a friendly API:

* **URL, headers, query, path params, cookies, client address**: `request.url`, `request.headers[...]`, `request.query_params[...]`, `request.path_params[...]`, `request.cookies`, `request.client`.
* **Body access**: `await request.body()`, `await request.json()`, `async with request.form() as form:` (limits via `max_files`, `max_fields`, `max_part_size`) and **streaming** via `async for chunk in request.stream(): ...`
* **Connection state**: `await request.is_disconnected()` for long‑polling/streaming checks.
* Share app‑global objects via `request.app` and `request.state`. ([Starlette][7])

---

## 5) Responses

Built‑ins include `Response`, `HTMLResponse`, `PlainTextResponse`, `JSONResponse` (customize serialization by subclassing), `RedirectResponse`, `StreamingResponse`, and `FileResponse` (supports **ETag**, **Last‑Modified**, **Range** requests). For **SSE**, use a third‑party `EventSourceResponse`. `set_cookie`/`delete_cookie` exist on `Response` (note new `partitioned=` flag on recent Python). ([Starlette][8])

```python
from typing import Any
import orjson
from starlette.responses import JSONResponse, StreamingResponse, FileResponse

class ORJSONResponse(JSONResponse):
    def render(self, content: Any) -> bytes:
        return orjson.dumps(content)

async def numbers(request):
    async def gen():
        for i in range(5):
            yield f"data: {i}\n\n"  # SSE-compatible body
    return StreamingResponse(gen(), media_type="text/event-stream")

async def download(request):
    return FileResponse("assets/report.pdf", filename="report.pdf")
```

---

## 6) Background tasks

Attach one or many background tasks with `BackgroundTask` / `BackgroundTasks`. Tasks run **after** the response is sent—great for fire‑and‑forget work like logging, notifications, or small post‑commit jobs. Important nuance: if a background task raises, the response has already gone out; handle/report those errors separately. ([Starlette][9])

---

## 7) Middleware: what’s included & how to use it well

Starlette ships these first‑class middlewares:

* **CORS** (`CORSMiddleware`) — set allow lists for origins/methods/headers; for consistent behavior even on error pages, **wrap the whole app** (outermost), not just insert into `middleware=[...]`. ([Starlette][10])
* **Sessions** (`SessionMiddleware`) — signed cookie‑based sessions via `request.session[...]`. Configure `secret_key`, `https_only`, `same_site`, etc. ([Starlette][10])
* **HTTPS redirect** (`HTTPSRedirectMiddleware`) — enforce HTTPS/WSS. ([Starlette][10])
* **Trusted hosts** (`TrustedHostMiddleware`) — allowlisted `Host` header to mitigate host‑header attacks. ([Starlette][10])
* **GZip** (`GZipMiddleware`) — compression. ([Starlette][10])

Two always‑present **internal** layers: `ServerErrorMiddleware` (outermost, 500/debug pages) and `ExceptionMiddleware` (innermost, for handled exceptions like `HTTPException`). ([Starlette][4])

**Which style to write?**

* **BaseHTTPMiddleware** gives a neat `dispatch(request, call_next)` API (easy, but a little overhead).
* **Pure ASGI** middleware keeps it zero‑cost and exposes ASGI messages—best for cross‑protocol concerns. The docs show patterns for inspecting/modifying request/response, passing info to endpoints, etc. ([Starlette][10])

**Behind proxies (real client IP & scheme).** If you’re behind NGINX/ALB/etc., enable **Uvicorn’s** `--proxy-headers` or use its `ProxyHeadersMiddleware` to honor `X‑Forwarded‑Proto` / `X‑Forwarded‑For` (and restrict trusted IPs via `--forwarded-allow-ips`). This is a server concern but critical for correct `request.url`/`request.client`. ([Uvicorn][11])

---

## 8) WebSockets

WebSocket endpoints work just like HTTP: function‑based (`async def ws(websocket): ...`) or class‑based via `WebSocketEndpoint`. Accept/receive/send/close with the `WebSocket` object. Starlette can also deny with proper close codes (requires server support for the *WebSocket Denial Response* extension; otherwise raise `HTTPException` from a WebSocket route). ([Starlette][12])

---

## 9) Static files & templating

* **Static files.** `StaticFiles(directory=..., html=True)` serves assets and auto‑loads `index.html` for directories; it supports ETags, `If-Modified-Since`, and will show `404.html` if present (in HTML mode). Mount it under `/static` (or anywhere). ([Starlette][13])
* **Templates.** Use `Jinja2Templates` (`return templates.TemplateResponse("page.html", {"request": request, ...})`). This is opt‑in (templates are not bound to the app instance). ([Starlette][1])

---

## 10) Authentication & permissions

Install `AuthenticationMiddleware` with a **custom backend** that returns `AuthCredentials([...scopes...])` and a `User`. Then use `@requires(...)` on endpoints to enforce scopes, optionally altering the status code or redirect target. This keeps “who the user is” separate from “what they can do.” Works for HTTP and WebSockets. ([Starlette][14])

---

## 11) Exceptions & error handling

Use `raise HTTPException(status_code, detail, headers)` for handled cases; mount custom exception handlers **by status** or **by exception class**. Remember: unhandled exceptions bubble to `ServerErrorMiddleware`; handled ones flow through `ExceptionMiddleware`. Background task failures occur after the response—log/report them out‑of‑band. ([Starlette][15])

---

## 12) Configuration (env, secrets) & app settings

Starlette’s `Config(".env")`, `Secret`, and `CommaSeparatedStrings` types help separate config from code (12‑factor). You can also read prefixed variables and control `.env` **encoding** (added in 0.49.0). The docs include a complete example of app settings, DB lifecycle in tests, and using `TestClient` context manager. ([Starlette][16])

---

## 13) Concurrency & thread‑pool offloading

Starlette uses **AnyIO**’s thread offloading for sync endpoints, file I/O, uploads, and synchronous background tasks (default **40 tokens**). If you do heavy blocking work, increase the token count via `anyio.to_thread.current_default_thread_limiter().total_tokens = N` or, better, move that work to a job runner. ([Starlette][17])

---

## 14) Testing

`starlette.testclient.TestClient` (powered by **httpx**) allows sync tests, WebSocket testing, and choosing an async backend (asyncio or Trio). To ensure `lifespan` runs, always use `with TestClient(app) as client:`. You can suppress server exceptions with `raise_server_exceptions=False` to assert on 500 bodies. ([Starlette][18])

---

## 15) GraphQL (status & options)

Built‑in GraphQL (**`GraphQLApp`**) was **deprecated in 0.15.0** and **removed in 0.17.0**. Use third‑party ASGI GraphQL packages such as **Ariadne**, **Strawberry**, or **starlette‑graphene3**—each ships a Starlette‑compatible ASGI app you can mount. ([Starlette][19])

---

## 16) Server push & streaming

Starlette supports **streaming responses** out of the box, and its docs cover HTTP server‑push patterns; in practice, you’ll more often use **SSE** (via `EventSourceResponse`) or WebSockets for real‑time updates. ([Starlette][8])

---

## 17) Production deployment

Use a modern **ASGI server**:

* **Uvicorn** for performance & simplicity, optionally under **Gunicorn** with **uvicorn‑worker** (the built‑in `uvicorn.workers` has been deprecated—use the separate package). Configure `--proxy-headers`/`--forwarded-allow-ips` when behind proxies. ([Uvicorn][20])
* **Hypercorn** for robust HTTP/2 support and Trio compatibility. ([hypercorn.readthedocs.io][21])
* **Daphne** (from Django/Channels) if you’re aligning with Channels. ([GitHub][22])

When serving **static files** at scale, prefer a CDN or the front proxy; keep `StaticFiles` for dev or small deployments.

---

## 18) Practical patterns (copy‑ready)

### A. Health checks & readiness

```python
from starlette.responses import JSONResponse
from starlette.routing import Route

async def live(_):      return JSONResponse({"status": "ok"})
async def ready(request): 
    # e.g., ping a pool on request.state/db
    return JSONResponse({"ready": True})

routes = [Route("/health/live", live), Route("/health/ready", ready)]
```

### B. Background tasks (durable pattern)

```python
from starlette.background import BackgroundTask, BackgroundTasks
from starlette.responses import JSONResponse

def write_audit(record: dict):
    # Do not block event loop; safe CPU/disk work here
    ...

async def create_order(request):
    body = await request.json()
    tasks = BackgroundTasks()
    tasks.add_task(write_audit, {"actor": "user", "op": "create", "id": body["id"]})
    return JSONResponse({"ok": True}, background=tasks)
```

### C. CORS done right (outer wrapping)

```python
from starlette.middleware.cors import CORSMiddleware
app = CORSMiddleware(app, allow_origins=["https://app.example.com"], allow_credentials=True,
                     allow_methods=["GET","POST","OPTIONS"], allow_headers=["*"])
```

This guarantees CORS headers also appear on **error** responses (outermost layer). ([Starlette][10])

### D. Pure ASGI middleware skeleton (super low overhead)

```python
class MetricsMiddleware:
    def __init__(self, app): self.app = app
    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        start = __import__("time").perf_counter()
        async def _send(message):
            if message.get("type") == "http.response.start":
                duration = __import__("time").perf_counter() - start
                # export duration + route pattern via scope.get("path")
            await send(message)
        return await self.app(scope, receive, _send)
```

See Starlette’s docs for patterns like inspecting/modifying requests, eager responses, passing data to endpoints, and cleanup. ([Starlette][10])

---

## 19) Security checklist (Starlette specifics)

* **HTTPS only** and **HSTS** (via proxy or custom header middleware). Use `HTTPSRedirectMiddleware` to redirect. ([Starlette][10])
* **Trusted hosts** to prevent host‑header attacks. ([Starlette][10])
* **CORS** with explicit allow lists; wrap app to enforce on errors. ([Starlette][10])
* **Sessions**: use `https_only=True`, proper `same_site`, and rotate `secret_key`. ([Starlette][10])

---

## 20) Starlette + your stack (where it fits)

* **Routing layer** for your vector/search services: expose endpoints for indexing, query, hybrid search, and streaming results (SSE/WebSockets).
* **BackgroundTasks** for light post‑response indexing triggers or audit logs; for heavier jobs (e.g., large batch embeddings), hand off to your task runner/orchestrator. ([Starlette][9])
* **Lifespan** to create/tear down shared clients (e.g., HTTPX, DB pools, search clients) and place them on `request.state`. ([Starlette][5])
* **CORS/TrustedHosts** when fronted by your API gateway/CDN; **Uvicorn’s proxy settings** if terminating TLS at the edge. ([Uvicorn][11])

If you re‑upload the high‑level architecture and FAISS/cuVS doc, I’ll map concrete Starlette modules and middleware to each plane (ingest, query, control) and propose code scaffolds per service.

---

## 21) Quick reference (what to reach for)

* **HTTP**: `Route`, `Request`, `JSONResponse`, `StreamingResponse`, `FileResponse`. ([Starlette][7])
* **WebSockets**: `WebSocketRoute`, `WebSocketEndpoint`. ([Starlette][6])
* **Background work**: `BackgroundTask(s)`. ([Starlette][9])
* **Static & templates**: `StaticFiles(..., html=True)`, `Jinja2Templates`. ([Starlette][13])
* **Middleware**: CORS, Sessions, HTTPSRedirect, TrustedHost, GZip. ([Starlette][10])
* **Auth**: `AuthenticationMiddleware`, `@requires`. ([Starlette][14])
* **Exceptions**: `HTTPException`, `app.add_exception_handler(...)`. ([Starlette][15])
* **Config**: `Config(".env")`, `Secret`, `CommaSeparatedStrings`. ([Starlette][16])
* **Tests**: `TestClient` (context manager!), backends (`asyncio`/`trio`). ([Starlette][18])
* **Deploy**: Uvicorn (+ gunicorn via uvicorn‑worker), Hypercorn, or Daphne. ([Uvicorn][20])

---

### Sources

* Official docs and home: features, install, overview. ([Starlette][1])
* Applications API & built‑in exception layers. ([Starlette][4])
* Lifespan patterns and testing. ([Starlette][5])
* Routing, reverse URL lookups, host routing. ([Starlette][6])
* Requests (body, stream, form limits, cookies, disconnection). ([Starlette][7])
* Responses (streaming, files, cookies, SSE via third‑party). ([Starlette][8])
* Background tasks behavior. ([Starlette][9])
* Middleware catalog & best practices (CORS outer wrapping, sessions, HTTPS, trusted hosts, gzip; pure ASGI tips). ([Starlette][10])
* Static files (html mode, 404.html); Templates. ([Starlette][13])
* Authentication middleware & permissions. ([Starlette][14])
* WebSockets + denial response behavior. ([Starlette][12])
* Test client & backends. ([Starlette][18])
* Thread‑pool offloading & AnyIO tokens. ([Starlette][17])
* GraphQL removal & recommended libraries. ([Starlette][19])
* Release notes (currency, 0.49–0.50). ([Starlette][2])
* Uvicorn proxy headers and uvicorn‑worker (deployment). ([Uvicorn][11])
* Hypercorn & Daphne references. ([hypercorn.readthedocs.io][21])

---

If you’d like, I can now **generate a project skeleton** (routers, middleware, lifespan state, tests) aligned to your services and data flow, or **layer Starlette** onto your ingestion/query planes once I can see the architecture/FAISS notes again.

[1]: https://starlette.dev/?utm_source=chatgpt.com "Starlette"
[2]: https://starlette.dev/release-notes/ "Release Notes - Starlette"
[3]: https://asgi.readthedocs.io/en/latest/specs/main.html?utm_source=chatgpt.com "ASGI (Asynchronous Server Gateway Interface) Specification"
[4]: https://starlette.dev/applications/ "Applications - Starlette"
[5]: https://www.starlette.dev/lifespan/ "Lifespan - Starlette"
[6]: https://starlette.dev/routing/ "Routing - Starlette"
[7]: https://www.starlette.dev/requests/ "Requests - Starlette"
[8]: https://starlette.dev/responses/ "Responses - Starlette"
[9]: https://starlette.dev/release-notes/?utm_source=chatgpt.com "Release Notes"
[10]: https://starlette.dev/middleware/ "Middleware - Starlette"
[11]: https://www.uvicorn.org/settings/?utm_source=chatgpt.com "Settings - Uvicorn"
[12]: https://starlette.dev/websockets/?utm_source=chatgpt.com "WebSockets"
[13]: https://starlette.dev/staticfiles/ "Static Files - Starlette"
[14]: https://starlette.dev/authentication/ "Authentication - Starlette"
[15]: https://www.starlette.dev/exceptions/?utm_source=chatgpt.com "Exceptions"
[16]: https://starlette.dev/config/ "Configuration - Starlette"
[17]: https://starlette.dev/threadpool/ "Thread Pool - Starlette"
[18]: https://starlette.dev/testclient/?utm_source=chatgpt.com "Test Client"
[19]: https://starlette.dev/graphql/?utm_source=chatgpt.com "GraphQL"
[20]: https://uvicorn.dev/deployment/?utm_source=chatgpt.com "Deployment"
[21]: https://hypercorn.readthedocs.io/?utm_source=chatgpt.com "Hypercorn documentation — Hypercorn 0.17.3 documentation"
[22]: https://github.com/django/daphne?utm_source=chatgpt.com "django/daphne: Django Channels HTTP/WebSocket server"
