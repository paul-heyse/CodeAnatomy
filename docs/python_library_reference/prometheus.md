Below is a **comprehensive, implementation‑grade reference** to the official Prometheus Python client library (`prometheus_client`). It’s structured so an AI agent—or any senior engineer—can copy/paste, design, and run **production‑ready, best‑in‑class** implementations across sync/async servers, with multi‑process (Gunicorn) support, Pushgateway usage, TLS/mTLS, custom collectors, exemplars, and more. Every API surface is linked to its authoritative source.

---

## 0) Install & “hello, metrics”

```bash
pip install prometheus-client
```

Minimal example (exposes metrics over HTTP on port 8000):

```python
from prometheus_client import start_http_server, Summary
import random, time

REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request')

@REQUEST_TIME.time()
def process_request(t):
    time.sleep(t)

if __name__ == "__main__":
    start_http_server(8000)  # http://localhost:8000/ (root path)
    while True:
        process_request(random.random())
```

`start_http_server()` starts a daemon thread serving the registry; the function returns `(server, thread)` so you can shut it down gracefully. ([prometheus.github.io][1])

---

## 1) Core metric types (Counter, Gauge, Summary, Histogram, Info, Enum)

### 1.1 Counter

Monotonically increasing; resets on process restart. Helpers: `inc(value=1.0)`, `count_exceptions()` (decorator/context manager).

```python
from prometheus_client import Counter
errors = Counter('my_failures_total', 'Number of failures')
errors.inc()
@errors.count_exceptions(ValueError)
def parse(): ...
```

OpenMetrics compatibility: if you name `my_failures_total`, the client strips `_total` internally and adds it on exposition. ([prometheus.github.io][2])

### 1.2 Gauge

Arbitrary up/down value. Helpers: `set`, `inc`, `dec`, `set_to_current_time()`, `track_inprogress()` context manager/decorator, and `set_function(fn)` to compute a value at collect time.

```python
from prometheus_client import Gauge
inflight = Gauge('inprogress_requests', 'In‑flight requests')
inflight.inc(); inflight.dec(2)
queue_depth = Gauge('queue_depth', 'Items in queue')
queue = {}
queue_depth.set_function(lambda: len(queue))
```

([prometheus.github.io][3])

### 1.3 Summary

Tracks **count and sum** (e.g., request durations). The Python client **does not expose quantiles**; use Histograms for quantiles via PromQL. Convenient `time()` decorator/context manager.

```python
from prometheus_client import Summary
latency = Summary('request_latency_seconds', 'Request latency (s)')
@latency.time()
def handle(): ...
```

([prometheus.github.io][4])

### 1.4 Histogram

Samples observations into **buckets**, plus `_sum` and `_count`. Default buckets target web/RPC latencies; pass `buckets=[...]` to customize. Has `time()` helper.

```python
from prometheus_client import Histogram
h = Histogram('request_latency_seconds', 'Request latency (s)', buckets=[0.05,0.1,0.25,0.5,1,2,5])
@h.time()
def handle(): ...
```

Use PromQL `histogram_quantile()` for percentiles from histogram buckets. ([prometheus.github.io][5])

### 1.5 Info

A special metric that exposes **key‑value** metadata as labels (value is always `1`).

```python
from prometheus_client import Info
build = Info('build_info', 'Build metadata')
build.info({'version': '1.2.3', 'commit': 'abc123'})
```

([prometheus.github.io][6])

### 1.6 Enum

Tracks **which state** an entity is in among predefined states.

```python
from prometheus_client import Enum
task_state = Enum('task_state', 'Task state', states=['starting','running','stopped'])
task_state.state('running')
```

([prometheus.github.io][7])

---

## 2) Labels: how to add dimensionality safely

```python
from prometheus_client import Counter
requests = Counter('http_requests_total', 'Total HTTP requests', ['method','path'])
requests.labels('GET', '/').inc()
```

Initialize expected label values early (call `.labels(...)` with values) to avoid scrape‑time surprises. Follow Prometheus **naming & label best practices**: include a **single base unit** (e.g., `_seconds`, `_bytes`), keep names lowercase with underscores, and use labels to differentiate characteristics—**not** metric names. ([prometheus.github.io][8])

> **Histogram vs Summary**: Prefer **Histogram** when you need quantiles, aggregation, or SLOs across instances; Python `Summary` has count/sum only (no quantiles). ([prometheus.github.io][4])

---

## 3) Exemplars (trace IDs, span IDs)

Attach an exemplar (e.g., `trace_id`) to **Counters** and **Histograms**—rendered only in **OpenMetrics** format.

```python
c.labels(method='GET', path='/').inc(exemplar={'trace_id': 'abc123'})
h.observe(0.123, exemplar={'trace_id': 'abc123'})
```

Prometheus must be started with `--enable-feature=exemplar-storage`. If you generate output yourself, use `prometheus_client.openmetrics.exposition.generate_latest`. The built‑in servers do content negotiation and will return OpenMetrics when Prometheus asks. ([prometheus.github.io][9])

---

## 4) Default collectors (auto‑exported metrics)

The client automatically exports:

* **Process metrics** (CPU, RSS, fds, start time) – Linux only – prefixed `process_*`.
* **Platform/Python metadata** on `python_info{...} 1`.
* **GC metrics** (e.g., `python_gc_objects_collected_total{generation="0|1|2"}`).

Disable any of these if not helpful:

```python
import prometheus_client as pc
pc.REGISTRY.unregister(pc.GC_COLLECTOR)
pc.REGISTRY.unregister(pc.PLATFORM_COLLECTOR)
pc.REGISTRY.unregister(pc.PROCESS_COLLECTOR)
```

You can also set `disable_created_metrics()` or env `PROMETHEUS_DISABLE_CREATED_SERIES=True` to turn off `_created` series. ([prometheus.github.io][10])

---

## 5) Registries, restricted output, textfile export & parsing

* **Default registry**: `prometheus_client.REGISTRY`.
* **Custom registry** for targeted exposure or Pushgateway/textfile use:

  ```python
  from prometheus_client import CollectorRegistry, Gauge, write_to_textfile
  reg = CollectorRegistry()
  Gauge('raid_status', '1 if OK', registry=reg).set(1)
  write_to_textfile('/var/lib/node_exporter/textfile/raid.prom', reg)
  ```

  (Node exporter textfile collector). ([prometheus.github.io][11])
* **Restricted registry**: serve only certain metric names (via `?name[]=...` on the built‑in HTTP server, or programmatically):

  ```python
  from prometheus_client import generate_latest, REGISTRY
  generate_latest(REGISTRY.restricted_registry(['python_info','python_gc_objects_collected_total']))
  ```

  ([prometheus.github.io][12])
* **Parse text exposition** (advanced ingestion to other systems):

  ```python
  from prometheus_client.parser import text_string_to_metric_families
  for fam in text_string_to_metric_families("my_gauge 1.0\n"): ...
  ```

  ([prometheus.github.io][13])

---

## 6) Exporting: HTTP/HTTPS, WSGI/ASGI/AIOHTTP/Twisted, Flask, FastAPI+Gunicorn

### 6.1 Built‑in HTTP server (optionally TLS/mTLS)

```python
from prometheus_client import start_http_server
server, thread = start_http_server(8000)             # HTTP on :8000
server, thread = start_http_server(8000,             # HTTPS
                                   certfile="server.crt",
                                   keyfile="server.key")
# Optional mTLS: add client_auth_required=True and client CA (client_cafile / client_capath)
```

Only `GET` and `OPTIONS` are supported; `/favicon.ico` returns 200 empty. You may also reuse **`MetricsHandler`** if you need to integrate with an existing HTTP server. ([prometheus.github.io][14])

### 6.2 WSGI (universal) & Flask

```python
# WSGI
from prometheus_client import make_wsgi_app, start_wsgi_server
app = make_wsgi_app(disable_compression=False)  # gzip if client supports it
# Or: start_wsgi_server(8000) in a background thread
```

To integrate with Flask, mount the WSGI app at `/metrics` using `DispatcherMiddleware`. ([prometheus.github.io][15])

### 6.3 ASGI (FastAPI, Starlette, etc.)

```python
from fastapi import FastAPI
from prometheus_client import make_asgi_app

app = FastAPI()
app.mount("/metrics", make_asgi_app(disable_compression=False))
```

The ASGI app negotiates gzip automatically; turn off via `disable_compression=True`. ([prometheus.github.io][16])

### 6.4 FastAPI **with Gunicorn** (multi‑process‑safe snippet)

```python
from fastapi import FastAPI
from prometheus_client import make_asgi_app, CollectorRegistry, multiprocess

app = FastAPI()
def make_metrics_app():
    registry = CollectorRegistry()
    multiprocess.MultiProcessCollector(registry)
    return make_asgi_app(registry=registry)

app.mount("/metrics", make_metrics_app())
# Run: gunicorn -b 127.0.0.1:8000 myapp:app -k uvicorn.workers.UvicornWorker
```

([prometheus.github.io][17])

### 6.5 AIOHTTP

```python
from aiohttp import web
from prometheus_client.aiohttp import make_aiohttp_handler

app = web.Application()
app.router.add_get("/metrics", make_aiohttp_handler(disable_compression=False))
```

([prometheus.github.io][18])

### 6.6 Twisted

```python
from prometheus_client.twisted import MetricsResource
from twisted.web.server import Site
from twisted.web.resource import Resource
from twisted.internet import reactor

root = Resource()
root.putChild(b"metrics", MetricsResource())
reactor.listenTCP(8000, Site(root))
reactor.run()
```

([prometheus.github.io][19])

---

## 7) Multiprocess mode (Gunicorn, uWSGI with workers)

**Why**: Python apps often scale by processes, not threads. The client provides **multiprocess mode** that aggregates per‑process shard files from `PROMETHEUS_MULTIPROC_DIR`. **Clean the directory before startup.** ([prometheus.github.io][20])

**Limitations (important)**:

* Registries can’t be used “as normal”; *all instantiated metrics are exported*.
* **Custom collectors** don’t work.
* `Gauge.set_function` not supported.
* **Info** and **Enum** don’t work.
* **Pushgateway** cannot be used.
* **Exemplars** unsupported.
* `remove`/`clear` label operations unsupported. ([prometheus.github.io][20])

**Gunicorn hook** (removes stale per‑PID shards on worker exit):

```python
# gunicorn.conf.py
from prometheus_client import multiprocess
def child_exit(server, worker):
    multiprocess.mark_process_dead(worker.pid)
```

**Gauge aggregation modes** in multiprocess: `'all'` (default; per‑PID), `'sum'`, `'max'`, `'min'`, `'mostrecent'`, and “live” variants (e.g., `'livesum'`).

```python
from prometheus_client import Gauge
inflight = Gauge('inprogress_requests', 'Help', multiprocess_mode='livesum')
```

([prometheus.github.io][20])

> For FastAPI+Gunicorn, mount a **per‑request** multiprocess registry to avoid duplicate metrics being exported from both the serving process and the multiprocess collector. ([prometheus.github.io][17])

---

## 8) Pushgateway (batch/ephemeral jobs)

Use a separate registry and **grouping keys**.

```python
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway, pushadd_to_gateway, delete_from_gateway, instance_ip_grouping_key
reg = CollectorRegistry()
g = Gauge('job_last_success_unixtime', 'Last success', registry=reg); g.set_to_current_time()
push_to_gateway('localhost:9091', job='batchA', registry=reg)
# pushadd_to_gateway(...) merges per metric name+grouping key, delete_from_gateway(...), instance_ip_grouping_key()
```

Auth helpers: `basic_auth_handler` and `tls_auth_handler`. ([prometheus.github.io][21])

---

## 9) Bridges (Graphite) & text generation

If you must send metrics to **Graphite**, the client provides a **Graphite bridge** (push over TCP).

```python
from prometheus_client.bridge.graphite import GraphiteBridge
gb = GraphiteBridge(('graphite.your.org', 2003), tags=True)
gb.push()   # or gb.start(10.0) to push every 10s
```

([prometheus.github.io][22])

---

## 10) Custom collectors (proxying other systems)

Implement `collect()` and optionally `describe()` to yield `*MetricFamily` objects:

```python
from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily, REGISTRY
from prometheus_client.registry import Collector
class MyCollector(Collector):
    def collect(self):
        yield GaugeMetricFamily('my_gauge', 'Help', value=7)
        c = CounterMetricFamily('my_counter_total', 'Help', labels=['foo'])
        c.add_metric(['bar'], 1.7)
        yield c
REGISTRY.register(MyCollector())
```

([prometheus.github.io][23])

---

## 11) Production‑grade patterns & guardrails

### 11.1 Naming, labels, and units

* **Use base units** in metric names (`_seconds`, `_bytes`), keep names lowercase with underscores, and **do not** encode label semantics into the metric name.
* Aim for label sets where `sum(...)` or `avg(...)` across all labels is meaningful.
* Avoid unbounded cardinality (e.g., user IDs, random strings) in labels.
  See official practices. ([Prometheus][24])

### 11.2 Histogram vs Summary (Python)

* For latencies, **prefer Histograms**, pick buckets that match your SLOs, and compute quantiles in PromQL with `histogram_quantile()`.
* Python `Summary` **does not** expose quantiles; it only provides `_count` and `_sum`. ([prometheus.github.io][5])

### 11.3 Default metrics hygiene

* Keep `process_*`, `python_info`, and GC metrics unless they’re noisy; unregister selectively if needed.
* Consider disabling `_created` across the app (`disable_created_metrics()` or env flag). ([prometheus.github.io][10])

### 11.4 Securing your endpoint

* Use `start_http_server(..., certfile=..., keyfile=...)` for **TLS**; add `client_auth_required=True` and **client CA** (`client_cafile` / `client_capath`) for **mTLS**.
* Built‑in server only responds to `GET`/`OPTIONS`. For custom routing or auth, integrate via `MetricsHandler`, or mount a WSGI/ASGI app behind your web stack. ([prometheus.github.io][14])

### 11.5 ASGI/WSGI integration notes

* ASGI/WSGI servers **negotiate gzip**; you can disable it via `disable_compression=True`.
* In Flask, mount the **WSGI** app at `/metrics`; in FastAPI, mount the **ASGI** app at `/metrics`. ([prometheus.github.io][15])

### 11.6 Multi‑process (Gunicorn/uWSGI)

* Set `PROMETHEUS_MULTIPROC_DIR` from the **bootstrap shell**, not in Python, and **wipe** it before each run.
* Add the **`child_exit`** hook; use **multiprocess gauge modes** (`'livesum'` etc.) to control aggregation.
* Do **not** use Info/Enum, `set_function`, Pushgateway, or exemplars in multiprocess mode. ([prometheus.github.io][20])

---

## 12) Worked examples (copy‑paste)

### 12.1 HTTP server with TLS/mTLS

```python
from prometheus_client import start_http_server
server, thread = start_http_server(9100, certfile="server.crt", keyfile="server.key")
# for mTLS:
# start_http_server(9100, certfile="server.crt", keyfile="server.key",
#                   client_auth_required=True, client_cafile="ca-chain.pem")
```

([prometheus.github.io][14])

### 12.2 Flask app with `/metrics`

```python
from flask import Flask
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from prometheus_client import make_wsgi_app

app = Flask(__name__)
app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {'/metrics': make_wsgi_app()})
```

([prometheus.github.io][25])

### 12.3 FastAPI + Gunicorn (multiprocess‑safe)

```python
from fastapi import FastAPI
from prometheus_client import make_asgi_app, CollectorRegistry, multiprocess

app = FastAPI()
def make_metrics_app():
    reg = CollectorRegistry()
    multiprocess.MultiProcessCollector(reg)
    return make_asgi_app(registry=reg)

app.mount("/metrics", make_metrics_app())
# Run: gunicorn -b 127.0.0.1:8000 app:app -k uvicorn.workers.UvicornWorker
```

([prometheus.github.io][17])

### 12.4 AIOHTTP server with `/metrics`

```python
from aiohttp import web
from prometheus_client.aiohttp import make_aiohttp_handler
app = web.Application()
app.router.add_get("/metrics", make_aiohttp_handler())
```

([prometheus.github.io][18])

### 12.5 Pushgateway (with Basic or TLS auth)

```python
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from prometheus_client.exposition import basic_auth_handler, tls_auth_handler

reg = CollectorRegistry()
Gauge('job_last_success_unixtime', 'Last success', registry=reg).set_to_current_time()

def basic(url, method, timeout, headers, data):
    return basic_auth_handler(url, method, timeout, headers, data, 'user', 'pass')

def mtls(url, method, timeout, headers, data):
    return tls_auth_handler(url, method, timeout, headers, data, 'client.crt', 'client.key')

push_to_gateway('localhost:9091', job='batchA', registry=reg, handler=basic)
```

([prometheus.github.io][21])

### 12.6 Custom collector shim

```python
from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily, REGISTRY
from prometheus_client.registry import Collector

class UpstreamCollector(Collector):
    def collect(self):
        yield GaugeMetricFamily('upstream_foo', 'Upstream foo', value=42)
        c = CounterMetricFamily('upstream_ops_total', 'Ops', labels=['kind'])
        c.add_metric(['read'], 10.0); c.add_metric(['write'], 5.0)
        yield c

REGISTRY.register(UpstreamCollector())
```

([prometheus.github.io][23])

---

## 13) Operational checklists

**Rollout**

* Expose `/metrics` via ASGI/WSGI middleware or the built‑in server; secure with TLS/mTLS if needed. ([prometheus.github.io][14])
* Keep **default collectors** unless there’s a strong reason to disable. ([prometheus.github.io][10])
* In Gunicorn: set **`PROMETHEUS_MULTIPROC_DIR`**, clean it, wire `child_exit`, and prefer **ASGI mount with a per‑request multiprocess registry**. ([prometheus.github.io][20])

**Instrumentation**

* Use **Counters** for totals, **Gauges** for current state, **Histograms** for latency/size distributions and quantiles. Avoid `Summary` for percentiles in Python. ([prometheus.github.io][5])
* Choose bucket boundaries that reflect SLOs (e.g., `0.05, 0.1, 0.25, 0.5, 1, 2, 5`). ([prometheus.github.io][5])
* Use **labels** judiciously; keep cardinality bounded; follow naming/units best practices. ([Prometheus][24])

**Troubleshooting**

* Seeing duplicate metrics under Gunicorn? Revisit the **per‑request registry** guidance for multiprocess mode. ([prometheus.github.io][20])
* Need to reduce series churn? Pre‑initialize labels (`metric.labels(...);`) and consider disabling `_created`. ([prometheus.github.io][8])

---

## 14) API quick map (Python objects & helpers)

* **Metric constructors**:
  `Counter(name, help, labelnames=None, registry=REGISTRY)`
  `Gauge(...)`, `Summary(...)`, `Histogram(..., buckets=[...])`, `Info(...)`, `Enum(..., states=[...])`
  Label binding with `.labels(**kwargs or *values)` and observation via `inc/dec/set/observe`.

* **Helpers**:
  `Counter.count_exceptions`, `Gauge.track_inprogress`, `*.time()`, `Gauge.set_function`, `disable_created_metrics()`.

* **Collectors**:
  `ProcessCollector`, `PlatformCollector`, `GCCollector` (auto‑registered; can unregister). ([prometheus.github.io][10])

* **Registries**:
  `CollectorRegistry`, `REGISTRY`, `generate_latest(registry)`, `REGISTRY.restricted_registry([...])`. ([prometheus.github.io][12])

* **Exporters**:
  `start_http_server(...)` (**TLS/mTLS**), `make_wsgi_app`, `start_wsgi_server`, `make_asgi_app`, `aiohttp.make_aiohttp_handler`, `twisted.MetricsResource`. ([prometheus.github.io][14])

* **Multiprocess**:
  `multiprocess.MultiProcessCollector(registry)`, `multiprocess.mark_process_dead(pid)`, `Gauge(..., multiprocess_mode='livesum')`. ([prometheus.github.io][20])

* **Pushgateway**:
  `push_to_gateway`, `pushadd_to_gateway`, `delete_from_gateway`, `instance_ip_grouping_key`, auth handlers. ([prometheus.github.io][21])

* **Bridges & parsers**:
  `bridge.graphite.GraphiteBridge`, `parser.text_string_to_metric_families`, `write_to_textfile`. ([prometheus.github.io][22])

---

## 15) Two “golden” patterns you can reuse

### Pattern A — HTTP request metrics (language/framework‑agnostic)

```python
from prometheus_client import Counter, Histogram

REQS = Counter('http_requests_total', 'Total HTTP requests', ['method','path','status'])
LAT  = Histogram('http_request_duration_seconds', 'HTTP request latency (s)',
                 buckets=[0.05,0.1,0.25,0.5,1,2,5])  # adjust to SLO

def on_request(method, path):
    timer = LAT.labels(method, path).time()
    try:
        # ... handle request
        status = "200"
        return ...
    except Exception:
        status = "500"
        raise
    finally:
        timer.observe_duration()  # recorded by context
        REQS.labels(method, path, status).inc()
```

Rationale: histogram buckets enable robust SLOs via `histogram_quantile()`; counters slice volume & errors. ([prometheus.github.io][5])

### Pattern B — Multiprocess‑safe FastAPI with `/metrics`

(see §6.3; ensures correct aggregation across workers, no duplicate metrics). ([prometheus.github.io][17])

---

## 16) Reference index (what to read when you need X)

* **All docs / quickstart:** client_python site. ([prometheus.github.io][1])
* **Metric types & best practices:** official Prometheus docs on metric types, histograms vs summaries, naming/labels. ([Prometheus][26])
* **Instrumenting (per metric type):** Counter, Gauge, Summary, Histogram, Info, Enum. ([prometheus.github.io][2])
* **Exemplars:** how to add and view (OpenMetrics). ([prometheus.github.io][9])
* **Export & servers:** HTTP/HTTPS (TLS/mTLS, allowed methods), WSGI/ASGI/AIOHTTP/Twisted, Flask, FastAPI+Gunicorn. ([prometheus.github.io][14])
* **Multiprocess mode:** env, limitations, registry & Gunicorn hook, gauge modes. ([prometheus.github.io][20])
* **Pushgateway:** functions & auth handlers. ([prometheus.github.io][21])
* **Textfile / Parser / Restricted registry / Graphite:** export alternatives. ([prometheus.github.io][11])

---

### Final tips

* Keep **labels bounded** (no IDs, UUIDs, or raw paths with parameters).
* Design **buckets** from your SLOs backward; too few → poor percentiles; too many → heavy cardinality.
* Prefer **Histograms** over Summaries for percentiles and fleet‑wide aggregation (Python client has no quantiles for `Summary`). ([Prometheus][24])

If you want, I can tailor these patterns to your stack (Flask/Django/FastAPI, sync/async, on‑prem/Kubernetes), or generate a drop‑in `metrics.py` module that wires everything (including TLS/mTLS and Gunicorn multiprocess setup).

[1]: https://prometheus.github.io/client_python/ "client_python"
[2]: https://prometheus.github.io/client_python/instrumenting/counter/ "Counter | client_python"
[3]: https://prometheus.github.io/client_python/instrumenting/gauge/ "Gauge | client_python"
[4]: https://prometheus.github.io/client_python/instrumenting/summary/ "Summary | client_python"
[5]: https://prometheus.github.io/client_python/instrumenting/histogram/ "Histogram | client_python"
[6]: https://prometheus.github.io/client_python/instrumenting/info/ "Info | client_python"
[7]: https://prometheus.github.io/client_python/instrumenting/enum/ "Enum | client_python"
[8]: https://prometheus.github.io/client_python/instrumenting/labels/ "Labels | client_python"
[9]: https://prometheus.github.io/client_python/instrumenting/exemplars/ "Exemplars | client_python"
[10]: https://prometheus.github.io/client_python/collector/ "Collector | client_python"
[11]: https://prometheus.github.io/client_python/exporting/textfile/ "Node exporter textfile collector | client_python"
[12]: https://prometheus.github.io/client_python/restricted-registry/ "Restricted registry | client_python"
[13]: https://prometheus.github.io/client_python/parser/ "Parser | client_python"
[14]: https://prometheus.github.io/client_python/exporting/http/ "HTTP/HTTPS | client_python"
[15]: https://prometheus.github.io/client_python/exporting/http/wsgi/ "WSGI | client_python"
[16]: https://prometheus.github.io/client_python/exporting/http/asgi/ "ASGI | client_python"
[17]: https://prometheus.github.io/client_python/exporting/http/fastapi-gunicorn/ "FastAPI + Gunicorn | client_python"
[18]: https://prometheus.github.io/client_python/exporting/http/aiohttp/ "AIOHTTP | client_python"
[19]: https://prometheus.github.io/client_python/exporting/http/twisted/ "Twisted | client_python"
[20]: https://prometheus.github.io/client_python/multiprocess/ "Multiprocess Mode | client_python"
[21]: https://prometheus.github.io/client_python/exporting/pushgateway/ "Pushgateway | client_python"
[22]: https://prometheus.github.io/client_python/bridges/graphite/ "Graphite | client_python"
[23]: https://prometheus.github.io/client_python/collector/custom/ "Custom Collectors | client_python"
[24]: https://prometheus.io/docs/practices/naming/?utm_source=chatgpt.com "Metric and label naming"
[25]: https://prometheus.github.io/client_python/exporting/http/flask/ "Flask | client_python"
[26]: https://prometheus.io/docs/concepts/metric_types/?utm_source=chatgpt.com "Metric types"
