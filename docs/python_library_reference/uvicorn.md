Below is a **production‑grade, example‑driven overview of Uvicorn**—covering what it is, how it works, every knob you’ll actually tune, and “best‑in‑class” deployment patterns (CLI, programmatic, Gunicorn worker, behind Nginx, Docker, systemd). I’ve grounded the guidance in the **official Uvicorn documentation** plus a few high‑signal references for worker/process management changes introduced recently. ([Uvicorn][1])

---

## 1) What Uvicorn is (and isn’t)

**Uvicorn is an ASGI web server** for Python. It speaks **HTTP/1.1** and **WebSockets** (no HTTP/2 or HTTP/3 as of today). You run any ASGI app (FastAPI, Starlette, Django ASGI, raw ASGI functions) on it. ASGI is the async successor to WSGI and adds websockets and background tasks. ([Uvicorn][1])

Uvicorn can use:

* **Event loop**: `uvloop` (fastest; default when installed) or `asyncio` (fallback; used on Windows/PyPy). You choose with `--loop`. ([Uvicorn][2])
* **HTTP parser**: `httptools` (fast C‑based) or `h11` (pure Python). You choose with `--http`. ([Uvicorn][2])
* **WebSocket protocol**: `websockets` (default if installed), `websockets‑sansio` (new), or `wsproto`. Select with `--ws`. ([Uvicorn][3])

> Install the “batteries‑included” extras with `pip install "uvicorn[standard]"` to pull in `uvloop`, `httptools`, `websockets`, `watchfiles`, `python-dotenv`, etc. Uvicorn will auto‑prefer these where present. ([Uvicorn][2])

---

## 2) Three ways to run Uvicorn (choose one per context)

### A) Command line (ideal for dev and simple prod)

```bash
# Dev, auto-reload (watchfiles if installed)
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Key flags to know (see full list further below):

* **Reloader**: `--reload`, `--reload-include`, `--reload-exclude`, `--reload-delay` (watchfiles adds richer include/exclude). ([Uvicorn][4])
* **Workers**: `--workers N` (multiprocess, mutually exclusive with `--reload`). On POSIX, Uvicorn’s built‑in supervisor uses `spawn` and supports signals like **`SIGHUP` for graceful restart**, **`SIGTTIN` to scale up**, **`SIGTTOU` to scale down**. ([Uvicorn][5])
* **Loop/HTTP/WS engines**: `--loop`, `--http`, `--ws` to pick implementations. ([Uvicorn][1])
* **Lifespan**: `--lifespan [auto|on|off]` to enable app startup/shutdown events. ([Uvicorn][1])
* **Proxy awareness**: `--proxy-headers` and `--forwarded-allow-ips` (trusted IPs/networks or `*`). Defaults: proxy support **enabled** but only **trusted** per `forwarded-allow-ips`. ([Uvicorn][4])
* **TLS**: `--ssl-keyfile`, `--ssl-certfile`, `--ssl-version`, `--ssl-ciphers`, etc. ([Uvicorn][4])

Full CLI roster (abbrev’d here) is on the docs’ **Command line options** and **Settings** pages. ([Uvicorn][1])

### B) Programmatic (embed server in your code)

```python
# quick equivalent to the CLI
import uvicorn

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, log_level="info")
```

For maximum control (and to run inside an existing event loop), use `Config` + `Server` and `await server.serve()`:

```python
import asyncio, uvicorn

async def main():
    config = uvicorn.Config("main:app", host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())
```

Note: `reload=True` or `workers>1` require the import‑string style and an `if __name__ == "__main__"` guard. ([Uvicorn][1])

### C) Gunicorn with the **external** Uvicorn worker (recommended pattern)

Uvicorn’s **built‑in** Gunicorn worker module is **deprecated**; use the **`uvicorn-worker`** package moving forward:

```bash
pip install uvicorn-worker gunicorn
gunicorn -w 4 -k uvicorn_worker.UvicornWorker "main:app"
# PyPy: -k uvicorn_worker.UvicornH11Worker
```

This gives you Gunicorn’s mature process management + Uvicorn’s fast I/O stack. (The docs now explicitly recommend the external worker.) ([Uvicorn][5])

---

## 3) Fundamental ASGI concepts you’ll actually use

**Lifespan**: Uvicorn emits `lifespan.startup` / `lifespan.shutdown` once per worker—use that to open DB pools, caches, etc. Disable with `--lifespan off` if your app framework handles boot independently. ([Uvicorn][6])

**WebSockets**: Uvicorn supports `wsproto`, `websockets`, and the new `websockets‑sansio` (June 2025). Pick with `--ws`. Your app receives `websocket.connect/receive/disconnect` events. ([Uvicorn][3])

**Event loop**: `--loop auto` prefers `uvloop` (not on Windows/PyPy; Windows uses asyncio—Proactor when multi‑worker, Selector when single worker). You can even supply a **custom loop** (`--loop module:function`), e.g. experimental **rloop** or **Winloop**. ([Uvicorn][7])

---

## 4) Server behavior that impacts reliability & performance

* **Flow control**: Uvicorn throttles read/write to avoid buffering floods (important for big uploads/downloads). ([Uvicorn][8])
* **HTTP semantics**: Adds `Server` and `Date` headers by default; honors `Content-Length`, falls back to chunked encoding, strips bodies on `HEAD`. You can disable default headers (`--no-server-header`, `--no-date-header`). ([Uvicorn][8])
* **Timeouts**: Keep‑alive default is 5s (`--timeout-keep-alive`); graceful shutdown wait (`--timeout-graceful-shutdown`). ([Uvicorn][8])
* **Resource limits**: `--limit-concurrency` (cap concurrent conns/tasks; 503 once exceeded), `--limit-max-requests` (recycle processes to mitigate leaks). ([Uvicorn][8])
* **Pipelining**: Supported pragmatically—requests queued and read paused until prior responses finish. ([Uvicorn][8])
* **Graceful restarts**: With Uvicorn’s multiprocess manager, SIGHUP restarts workers one‑by‑one without dropping connections. ([Uvicorn][5])

---

## 5) “Best‑in‑class” configurations by scenario

### 5.1 Local development (fast feedback, safe defaults)

```bash
pip install "uvicorn[standard]"
uvicorn main:app --reload --reload-include "*.py,*.jinja2" --log-level debug
```

* `watchfiles` gives precise change filters; include non‑py assets when templating. ([Uvicorn][4])
* Keep `workers=1` during dev; reloader and workers are mutually exclusive. ([Uvicorn][5])

### 5.2 Single host (Linux), **systemd** + Nginx (no container)

**Systemd unit** (illustrative—adapt paths/users per your box; see systemd man page for options):

```ini
# /etc/systemd/system/myapp.service
[Unit]
Description=My ASGI app (Uvicorn)
After=network-online.target

[Service]
User=www-data
Group=www-data
WorkingDirectory=/srv/myapp
Environment="UVICORN_WORKERS=4"
ExecStart=/srv/myapp/.venv/bin/uvicorn "main:app" \
  --host 127.0.0.1 --port 8001 \
  --workers ${UVICORN_WORKERS} \
  --loop uvloop --http httptools \
  --proxy-headers --forwarded-allow-ips=127.0.0.1 \
  --limit-concurrency 2048 --timeout-keep-alive 5
Restart=on-failure
RestartSec=3

[Install]
WantedBy=multi-user.target
```

* Rely on **Uvicorn’s built‑in supervisor** for workers, or use **Gunicorn + uvicorn‑worker** if you need Gunicorn features. ([Uvicorn][5])
* systemd service unit design is governed by `systemd.service(5)`; tailor `Restart=`, `Environment=`, etc. to your needs. ([Freedesktop][9])

**Nginx** (Unix socket + proxy headers + WebSocket upgrade):

```nginx
upstream uvicorn {
  server unix:/tmp/uvicorn.sock;
}

server {
  listen 80;
  server_name example.com;

  location / {
    proxy_set_header Host $http_host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection $connection_upgrade;
    proxy_redirect off;
    proxy_buffering off;
    proxy_pass http://uvicorn;
  }

  location /static {
    root /srv/myapp/static;
  }
}
```

* This mirrors the docs’ example and ensures correct client IP/scheme plus WebSocket upgrades. ([Uvicorn][5])

> **Security**: Don’t set `--forwarded-allow-ips="*"` unless your proxies **strip and set** the forwarded headers themselves; otherwise clients can spoof addresses. ([Uvicorn][5])

### 5.3 Gunicorn + **uvicorn‑worker** (POSIX process manager features)

```bash
pip install uvicorn-worker gunicorn
# Typical: 1–2 workers per CPU core; start conservative and load-test.
gunicorn "main:app" -w 4 -k uvicorn_worker.UvicornWorker \
  --graceful-timeout 30 --timeout 30 --keep-alive 5
```

* The **built‑in** `uvicorn.workers` module is **deprecated**; use `uvicorn-worker` going forward. ([Uvicorn][1])

### 5.4 Containers (Docker, K8s)

Minimal, cache‑aware **Dockerfile** (docs pattern):

```dockerfile
FROM python:3.12-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
WORKDIR /app
# dependency layers cached via uv lockfile
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project
ADD . /app
RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen
CMD ["uv", "run", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
```

* Prefer **1 worker per container** and scale replicas with the orchestrator; don’t build a mini‑process manager inside the container. ([Uvicorn][10])

---

## 6) Application loading patterns

* **Plain app**: `uvicorn main:app` where `app` is an ASGI callable. ([Uvicorn][1])
* **App factory**: `uvicorn --factory main:create_app` calls a no‑arg function returning an ASGI app—great for dependency injection. ([Uvicorn][1])
* **Interface modes**: `--interface [asgi3|asgi2|wsgi]`; WSGI mode is **deprecated**—use `a2wsgi` if you must serve WSGI code. ([Uvicorn][4])
* **`--app-dir`**: add a directory to `PYTHONPATH` (e.g., monorepos). ([Uvicorn][1])

---

## 7) Logging, env, and observability

* **Log level**: `--log-level [critical|error|warning|info|debug|trace]` and `--access-log/--no-access-log`. ([Uvicorn][1])
* **Structured config**: `--log-config` supports `.ini`, JSON, YAML (YAML requires PyYAML or install `uvicorn[standard]`). ([Uvicorn][4])
* **`.env` files**: `--env-file` loads environment variables (via `python‑dotenv` if using `[standard]`). Intended for **your app’s** configuration, not Uvicorn’s own `UVICORN_*` env variables (CLI args override). ([Uvicorn][4])

Minimal JSON logging config:

```json
{
  "version": 1,
  "formatters": {"default": {"format": "%(levelname)s %(name)s %(message)s"}},
  "handlers": {"default": {"class": "logging.StreamHandler", "formatter": "default"}},
  "root": {"level": "INFO", "handlers": ["default"]}
}
```

Run with: `uvicorn main:app --log-config logging.json`. ([Uvicorn][4])

---

## 8) Proxy and header correctness (critical in production)

When behind one or more proxies/load balancers:

* Start Uvicorn with **`--proxy-headers`** and a **safe** **`--forwarded-allow-ips`** list (IPs, CIDRs, or UDS literal). This controls trust of `X-Forwarded-For` and `X-Forwarded-Proto` to reconstruct client IP and scheme. Don’t over‑trust (`*`) unless proxies sanitize headers first. ([Uvicorn][5])
* If your app is **sub‑mounted** (e.g., behind `example.com/api`), set `--root-path /api` so routing and URLs are correct. ([Uvicorn][1])

---

## 9) TLS/HTTPS quickly and correctly

* Local/dev: generate a cert with **mkcert**. Prod: use Let’s Encrypt. Run:
  `uvicorn main:app --ssl-keyfile key.pem --ssl-certfile cert.pem`. ([Uvicorn][5])
* For Gunicorn worker: `gunicorn --keyfile=key.pem --certfile=cert.pem -k uvicorn_worker.UvicornWorker main:app`. ([Uvicorn][5])

---

## 10) Tuning cheatsheet (knobs that actually move the needle)

* **Loop/HTTP engine**: `--loop uvloop --http httptools` on Linux/CPython (fallback to asyncio/h11 on PyPy/Windows). ([Uvicorn][2])
* **Concurrency caps**: Set `--limit-concurrency` to a level your service can handle under load to protect memory. ([Uvicorn][4])
* **Keep‑alive**: Default 5s is conservative; tune with `--timeout-keep-alive` for your client mix and proxy timeouts. ([Uvicorn][4])
* **Max‑requests**: If you suspect leaks, set `--limit-max-requests` (with a process manager—Uvicorn supervisor or Gunicorn). ([Uvicorn][8])
* **Workers**: Start at **`cpu_count`** or at most **2×** and load‑test; measure p95/p99 and error budgets before increasing. Use built‑in workers or Gunicorn + uvicorn‑worker depending on operational needs. (Docs show both patterns; built‑in workers and signals are available.) ([Uvicorn][5])

---

## 11) Minimal, correct examples you can copy‑paste

### A) Raw ASGI app (no framework)

```python
# main.py
async def app(scope, receive, send):
    assert scope["type"] == "http"
    await send({"type": "http.response.start", "status": 200,
                "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": b"Hello, world!"})
```

Run: `uvicorn main:app`. ([Uvicorn][1])

### B) Lifespan done right

```python
# opens resources on startup; closes them on shutdown (per worker)
async def app(scope, receive, send):
    if scope["type"] == "lifespan":
        while True:
            msg = await receive()
            if msg["type"] == "lifespan.startup":
                # connect pools, warm caches...
                await send({"type": "lifespan.startup.complete"})
            elif msg["type"] == "lifespan.shutdown":
                # close pools...
                await send({"type": "lifespan.shutdown.complete"})
                return
    elif scope["type"] == "http":
        ...
```

Turn off if the framework owns startup/shutdown: `uvicorn main:app --lifespan off`. ([Uvicorn][6])

### C) WebSockets with protocol selection

```bash
# default (if websockets installed)
uvicorn main:app --ws websockets
# alternative engines
uvicorn main:app --ws wsproto
uvicorn main:app --ws websockets-sansio
```

Choose based on your dependency and performance preferences. ([Uvicorn][3])

### D) Gunicorn + uvicorn‑worker (current path)

```bash
gunicorn "main:app" -w 4 -k uvicorn_worker.UvicornWorker --graceful-timeout 30
```

Use `uvicorn_worker.UvicornH11Worker` for PyPy. ([PyPI][11])

### E) Nginx + Uvicorn (with UDS, proxy headers, WS upgrade)

Use the Nginx snippet from §5.2; it aligns with the docs’ guidance and preserves client IP/scheme. ([Uvicorn][5])

---

## 12) Complete CLI option map (curated)

You’ll most often touch:

* **Binding**: `--host`, `--port`, `--uds`, `--fd`. ([Uvicorn][1])
* **Dev**: `--reload`, `--reload-include`, `--reload-exclude`. (Enhanced when `watchfiles` is present.) ([Uvicorn][4])
* **Prod**: `--workers`, `--env-file`, `--timeout-worker-healthcheck`. (Workers and reload are mutually exclusive.) ([Uvicorn][4])
* **Impl**: `--loop [auto|asyncio|uvloop]`, `--http [auto|h11|httptools]`, `--ws [auto|websockets|websockets-sansio|wsproto]`, `--lifespan [auto|on|off]`. ([Uvicorn][1])
* **Interface**: `--interface [auto|asgi3|asgi2|wsgi]` (WSGI is deprecated; use `a2wsgi` instead). ([Uvicorn][4])
* **Logging**: `--log-config`, `--log-level`, `--access-log/--no-access-log`, `--use-colors/--no-use-colors`. ([Uvicorn][4])
* **HTTP headers**: `--server-header/--no-server-header`, `--date-header/--no-date-header`, `--header Name:Value`. ([Uvicorn][1])
* **Proxy**: `--proxy-headers`, `--forwarded-allow-ips`, `--root-path`. ([Uvicorn][4])
* **Limits/timeouts**: `--limit-concurrency`, `--limit-max-requests`, `--backlog`, `--timeout-keep-alive`, `--timeout-graceful-shutdown`. ([Uvicorn][1])
* **TLS**: `--ssl-keyfile`, `--ssl-certfile`, `--ssl-version`, `--ssl-cert-reqs`, `--ssl-ca-certs`, `--ssl-ciphers`. ([Uvicorn][4])

The full, authoritative list is in **Command line options** and **Settings**. ([Uvicorn][1])

---

## 13) Common pitfalls (and how to avoid them)

* **Worker vs. reload**: They are mutually exclusive. Use `--reload` locally; use `--workers` in prod. ([Uvicorn][5])
* **Windows/PyPy surprise**: No `uvloop`; performance may differ. Consider Winloop (experimental) if you need more speed on Windows. ([Uvicorn][7])
* **Forwarded headers**: Don’t set `--forwarded-allow-ips="*"` unless you control every proxy and it sanitizes headers. Misconfigurations leak spoofed client IPs. ([Uvicorn][5])
* **Gunicorn worker class**: Switch to `uvicorn-worker`; the in‑package worker is deprecated. ([Uvicorn][1])
* **WSGI mode**: Deprecated—use `a2wsgi` if you must bridge, or migrate to an ASGI framework. ([Uvicorn][4])

---

## 14) Copy‑ready deployment recipes

### A) **Production with Uvicorn’s supervisor** (no Gunicorn), behind Nginx

```bash
# choose engines explicitly; tune limits/timeouts to your SLOs
uvicorn "main:app" \
  --host 127.0.0.1 --port 8001 \
  --workers 4 \
  --loop uvloop --http httptools --ws websockets \
  --proxy-headers --forwarded-allow-ips=127.0.0.1 \
  --limit-concurrency 2048 --limit-max-requests 20000 \
  --timeout-keep-alive 5 --timeout-graceful-shutdown 30 \
  --log-level info
```

Graceful rolling restart from the outside (POSIX): send **`SIGHUP`** to the parent process. ([Uvicorn][5])

### B) **Production with Gunicorn + uvicorn‑worker** (feature‑rich process manager)

```bash
gunicorn "main:app" -w 4 -k uvicorn_worker.UvicornWorker \
  --graceful-timeout 30 --timeout 30 --keep-alive 5
```

Scale workers with Gunicorn and keep Uvicorn on the fastest engines under the hood. ([PyPI][11])

### C) **Docker** (single worker; scale with orchestrator)

Use the **Dockerfile** in §5.4 and run:

```bash
docker build -t myapp .
docker run -p 8000:8000 myapp
```

The official docs recommend letting the orchestrator manage parallelism (replicas), not piling multiple process managers into one container. ([Uvicorn][10])

---

## 15) One‑file sanity checklist for agents

1. **Install extras**: `pip install "uvicorn[standard]"` (Linux/CPython). ([Uvicorn][2])
2. **Run**: `uvicorn main:app --loop uvloop --http httptools`. ([Uvicorn][2])
3. **Behind proxy**: add `--proxy-headers --forwarded-allow-ips=<lb-ip-or-cidr> --root-path /subpath`. ([Uvicorn][5])
4. **TLS**: `--ssl-keyfile ... --ssl-certfile ...` (mkcert for dev, Let’s Encrypt for prod). ([Uvicorn][5])
5. **Limits/Timeouts**: set `--limit-concurrency`, `--limit-max-requests`, `--timeout-keep-alive`, `--timeout-graceful-shutdown`. ([Uvicorn][4])
6. **Workers**: use `--workers` **or** `--reload` (never both). For Gunicorn, use **`uvicorn_worker.UvicornWorker`**. ([Uvicorn][5])

---

### Sources

* Official docs: **Command line options, Settings, Server Behavior, Deployment, Docker, Concepts (Lifespan/WebSockets/Event loop)**. These pages include all flags, defaults, and recommended patterns shown above. ([Uvicorn][1])
* **Installation & Extras** (`uvicorn[standard]` behavior and optional deps). ([Uvicorn][2])
* **Gunicorn worker deprecation** → use **uvicorn‑worker**. (Official docs + project pages.) ([Uvicorn][5])

If you want, I can turn this into a **starter template repo** (with `pyproject.toml`, logging config, systemd unit, Nginx conf, Dockerfile, and a smoke‑test script) so agents can deploy with a single command.

[1]: https://uvicorn.dev/ "Uvicorn"
[2]: https://uvicorn.dev/installation/ "Installation - Uvicorn"
[3]: https://uvicorn.dev/concepts/websockets/ "WebSockets - Uvicorn"
[4]: https://uvicorn.dev/settings/ "Settings - Uvicorn"
[5]: https://uvicorn.dev/deployment/ "Deployment - Uvicorn"
[6]: https://uvicorn.dev/concepts/lifespan/ "Lifespan - Uvicorn"
[7]: https://uvicorn.dev/concepts/event-loop/ "Event Loop - Uvicorn"
[8]: https://uvicorn.dev/server-behavior/ "Server Behavior - Uvicorn"
[9]: https://www.freedesktop.org/software/systemd/man/systemd.service.html?utm_source=chatgpt.com "systemd.service"
[10]: https://uvicorn.dev/deployment/docker/ "Docker - Uvicorn"
[11]: https://pypi.org/project/uvicorn-worker/?utm_source=chatgpt.com "uvicorn-worker"
