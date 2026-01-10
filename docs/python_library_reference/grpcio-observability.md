
## grpcio-observability — functionality catalog (what exists)

### 1) What the package *is* (scope + runtime model)

* **Purpose:** a **native gRPC Python observability package** that exposes **OpenTelemetry *metrics*** collected in **gRPC Core (C/C++)** up into Python. ([PyPI][1])
* **Data path / performance model:** telemetry is **collected + buffered in Core** and **exported to Python in batches** (reduces GIL churn; introduces a small reporting delay). ([PyPI][1])
* **Dependencies:** requires `grpcio` and **`opentelemetry-api`** (not the SDK as a hard dependency). ([PyPI][1])
* **Platform:** PyPI wheels are **currently Linux-only**. ([PyPI][1])

---

### 2) Public API surface (Python-level entry points)

The public-facing API is essentially: **“create an OpenTelemetry metrics plugin and register it globally.”** ([grpc.github.io][2])

#### 2.1 `grpc_observability.OpenTelemetryPlugin`

* **Constructor knobs** (current gRPC Python docs + source):

  * `plugin_options: Iterable[OpenTelemetryPluginOption] | None`
  * `meter_provider: opentelemetry.metrics.MeterProvider | None` (if `None`, metrics collection is effectively disabled) ([grpc.github.io][3])
  * `target_attribute_filter: Callable[[str], bool] | None` (**deprecated**) — cardinality control for `grpc.target` (“keep original target” vs replace with `"other"`). ([grpc.github.io][3])
  * `generic_method_attribute_filter: Callable[[str], bool] | None` — cardinality control for **generic/unregistered** method names (`grpc.method`), “keep method name” vs replace with `"other"` (pre-registered methods always recorded). ([grpc.github.io][3])

* **Lifecycle methods**

  * `register_global()` — install a **global plugin** that acts on all channels/servers in-process; raises `RuntimeError` if already registered. ([grpc.github.io][2])
  * `deregister_global()` — remove the global plugin; raises `RuntimeError` if none was registered. ([grpc.github.io][2])
  * **Context manager support:** `with OpenTelemetryPlugin(...): ...` calls “start” on enter and “end” on exit. ([grpc.github.io][3])

#### 2.2 “Hidden-in-plain-sight” advanced interfaces (in module source)

These are *real* extension points that are easy to miss because they’re not front-and-center in the high-level docs.

* **`OpenTelemetryLabelInjector` (interface)**

  * `get_labels_for_exchange() -> Dict[str, str|bytes]` — labels intended for **metadata exchange** across the wire. ([grpc.github.io][3])
  * `get_additional_labels(include_exchange_labels: bool) -> Dict[str, str]` — labels added directly to metric points. ([grpc.github.io][3])
  * `deserialize_labels(labels: Dict[str, str|bytes]) -> Dict[str, str|bytes]` — hook to decode any exchanged labels into final label set. ([grpc.github.io][3])
  * **Implication:** there is an explicit design for **“label propagation / enrichment”** via metadata, not just local tagging. ([grpc.github.io][3])

* **`OpenTelemetryPluginOption` (marker/extension interface)**

  * A placeholder “option object” type passed via `plugin_options` to extend plugin behavior. ([grpc.github.io][3])
  * In practice, the *spec* and the gRPC metrics docs imply options are used to enable/disable **additional instrument groups** and **optional attributes** (esp. experimental metrics). ([gRPC][4])

* **Key label constants** (useful when you’re building downstream views/dashboards):

  * `grpc.method`, `grpc.target`, and the fallback `"other"` value used by the cardinality filters. ([grpc.github.io][3])

---

### 3) Metrics catalog (what the plugin can emit)

grpcio-observability is the Python packaging for gRPC’s **OpenTelemetry metrics plugin**; the canonical metric inventory is maintained in gRPC’s OpenTelemetry metrics guide and gRFCs. ([gRPC][4])

#### 3.1 Per-call metrics (core RPC metrics)

**Client per-call (stable / on by default in gRPC’s guidance):**

* `grpc.client.call.duration` (Histogram, seconds) — labels: `grpc.method`, `grpc.target`, `grpc.status`. ([gRPC][4])

**Client per-attempt (stable / on by default):**

* `grpc.client.attempt.started` (Counter `{attempt}`) — `grpc.method`, `grpc.target`. ([gRPC][4])
* `grpc.client.attempt.duration` (Histogram, seconds) — `grpc.method`, `grpc.target`, `grpc.status`; optional `grpc.lb.locality`, `grpc.lb.backend_service`. ([gRPC][4])
* `grpc.client.attempt.sent_total_compressed_message_size` (Histogram, bytes) — same label set (incl. optional locality/backend_service). ([gRPC][4])
* `grpc.client.attempt.rcvd_total_compressed_message_size` (Histogram, bytes) — same label set. ([gRPC][4])

**Server instruments:**

* `grpc.server.call.started` (Counter `{call}`) — label: `grpc.method`. ([gRPC][4])
* `grpc.server.call.duration` (Histogram, seconds) — `grpc.method`, `grpc.status`. ([gRPC][4])
* `grpc.server.call.sent_total_compressed_message_size` (Histogram, bytes) — `grpc.method`, `grpc.status`. ([gRPC][4])
* `grpc.server.call.rcvd_total_compressed_message_size` (Histogram, bytes) — `grpc.method`, `grpc.status`. ([gRPC][4])

#### 3.2 Retry / hedging metrics (spec’d; typically experimental/off-by-default)

* `grpc.client.call.retries`
* `grpc.client.call.transparent_retries`
* `grpc.client.call.hedges`
* `grpc.client.call.retry_delay` ([gRPC][4])
  A96 explicitly notes these begin **experimental/off-by-default**, and (at least as of that doc) Python implementation status was “TBD.” ([Gitea: Git with a cup of tea][5])

#### 3.3 Load-balancer policy metrics (experimental)

**Weighted Round Robin (WRR):**

* `grpc.lb.wrr.rr_fallback`
* `grpc.lb.wrr.endpoint_weight_not_yet_usable`
* `grpc.lb.wrr.endpoint_weight_stale`
* `grpc.lb.wrr.endpoint_weights` ([gRPC][4])

**Pick First:**

* `grpc.lb.pick_first.disconnections`
* `grpc.lb.pick_first.connection_attempts_succeeded`
* `grpc.lb.pick_first.connection_attempts_failed` ([gRPC][4])

#### 3.4 xDS client metrics (experimental)

* `grpc.xds_client.connected`
* `grpc.xds_client.server_failure`
* `grpc.xds_client.resource_updates_valid`
* `grpc.xds_client.resource_updates_invalid`
* `grpc.xds_client.resources` ([gRPC][4])

#### 3.5 Attribute/label catalog (what you’ll see on points)

* Required/common: `grpc.method`, `grpc.status`, `grpc.target` ([gRPC][4])
* Optional / conditional: `grpc.lb.backend_service`, `grpc.lb.locality` ([gRPC][4])
* xDS-related: `grpc.xds.server`, `grpc.xds.authority`, `grpc.xds.cache_state`, `grpc.xds.resource_type` ([gRPC][4])
* **Important enablement note:** gRPC’s guide explicitly calls out that some attributes are *optional* and must be enabled via plugin API. ([gRPC][4])

---

### 4) Cardinality controls (advanced behavior that affects dashboards)

* **Generic method name filtering**: the plugin can intentionally record `grpc.method="other"` for generic/unregistered methods unless your `generic_method_attribute_filter` says otherwise. ([grpc.github.io][3])
* **Target filtering** (deprecated): same concept for `grpc.target` (replace with `"other"` to keep series counts bounded). ([grpc.github.io][3])

---

### 5) Core→Python export tuning (environment variables)

These are package-level knobs that affect buffering/export behavior in gRPC Core (and are easy to miss because they’re not OpenTelemetry settings):

* `GRPC_PYTHON_CENSUS_EXPORT_BATCH_INTERVAL` — export batch interval (default 0.5s). ([PyPI][1])
* `GRPC_PYTHON_CENSUS_MAX_EXPORT_BUFFER_SIZE` — max buffer items before export (default 10,000). ([PyPI][1])
* `GRPC_PYTHON_CENSUS_EXPORT_THRESHOLD` — trigger threshold (% full) to export (default 0.7). ([PyPI][1])
* `GRPC_PYTHON_CENSUS_EXPORT_THREAD_TIMEOUT` — exporting-thread timeout (default 10s). ([PyPI][1])

---

### 6) Compatibility + known gaps (important “catalog” items)

* **Linux-only distribution** (as of current PyPI guidance). ([PyPI][1])
* **AsyncIO (`grpc.aio`) support gap:** tracked issue indicates grpcio-observability “does not properly add metrics” when using `grpc.aio` (and mentions the interceptor-based OTel contrib alternative has shortcomings for stream-stream). ([GitHub][6])
* **Method attribution surprises:** users have observed metrics reporting `grpc.method="other"` when they expected per-method breakdown; this is consistent with the documented “generic method → other unless allowed” pattern. ([GitHub][7])

---

### 7) Adjacent packages in the same “grpc-packages” ecosystem (relevant when cataloging “what’s available”)

* **`grpcio-csm-observability`** exists as a sibling package under the same maintainer; it’s distinct from (and complementary to) `grpcio-observability` when you’re doing Cloud Service Mesh / CSM-specific observability. ([PyPI][8])

---

If you want, next I can turn this catalog into a **“module-by-module deep dive map”** (API signatures + behaviors + gotchas + what’s experimental vs stable), starting with: plugin lifecycle, label injection/metadata exchange, and the precise mapping from gRPC events → each metric instrument.

[1]: https://pypi.org/project/grpcio-observability/ "grpcio-observability · PyPI"
[2]: https://grpc.github.io/grpc/python/grpc_observability.html "gRPC Python Observability — gRPC Python 1.76.0 documentation"
[3]: https://grpc.github.io/grpc/python/_modules/grpc_observability/_open_telemetry_plugin.html "grpc_observability._open_telemetry_plugin — gRPC Python 1.76.0 documentation"
[4]: https://grpc.io/docs/guides/opentelemetry-metrics/ "OpenTelemetry Metrics | gRPC"
[5]: https://gitea.cncfstack.com/grpc/proposal/src/branch/master/A96-retry-otel-stats.md "proposal/A96-retry-otel-stats.md at master - proposal - Gitea: Git with a cup of tea"
[6]: https://github.com/grpc/grpc/issues/39061 "Add support for grpc.aio in grcio-observability · Issue #39061 · grpc/grpc · GitHub"
[7]: https://github.com/grpc/grpc/issues/38265 "grpcio-observability cannot get instruments each grpc.method  · Issue #38265 · grpc/grpc · GitHub"
[8]: https://pypi.org/user/grpc-packages/?utm_source=chatgpt.com "Profile of grpc-packages"


## grpcio-observability deep-dive map (Python) — modules, lifecycle, label injection, event→metric wiring

### Mental model (what actually runs)

* **Collection point:** metrics are primarily collected inside **gRPC Core** via per-call/per-attempt tracer callouts (“CallTracer / CallAttemptTracer / ServerCallTracer”), then surfaced to language runtimes. The OpenTelemetry plugin configures tracer factories on channels/servers. ([Gitea: Git with a cup of tea][1])
* **Python packaging behavior:** telemetry is **buffered in Core and exported to Python in batches** (intentional GIL-avoidance; implies visibility lag). ([PyPI][2])
* **OTel binding contract:** plugin depends on **OpenTelemetry API**; the application provides an **SDK-backed MeterProvider** (or you get a no-op / no metrics). gRPC implementations should **not** silently fall back to a global provider. ([Gitea: Git with a cup of tea][1])

---

## 1) `grpc_observability` / `grpc_observability._open_telemetry_plugin`

### 1.1 Public class: `OpenTelemetryPlugin`

**Signature (current gRPC Python docs / module code):**

```python
class grpc_observability.OpenTelemetryPlugin(
    *,
    plugin_options: Optional[Iterable[OpenTelemetryPluginOption]] = None,
    meter_provider: Optional[MeterProvider] = None,
    target_attribute_filter: Optional[Callable[[str], bool]] = None,   # deprecated
    generic_method_attribute_filter: Optional[Callable[[str], bool]] = None,
)
```

([grpc.github.io][3])

**Internal structure / bridge objects**

* Constructor stores config and creates a native-side plugin wrapper list:

  * `self._plugins = [_open_telemetry_observability._OpenTelemetryPlugin(self)]` ([grpc.github.io][3])
* Defaults matter (cardinality):

  * `target_attribute_filter` defaults to `lambda _target: True` (keep target; deprecated) ([grpc.github.io][3])
  * `generic_method_attribute_filter` defaults to `lambda _target: False` ⇒ **generic/unregistered methods become `grpc.method="other"`** by default. ([grpc.github.io][3])

### 1.2 Lifecycle API: global install + context manager

* `register_global()` → calls `start_open_telemetry_observability(plugins=self._plugins)`; documented to raise `RuntimeError` if already registered. ([grpc.github.io][3])
* `deregister_global()` → calls `end_open_telemetry_observability()`; documented to raise `RuntimeError` if none registered. ([grpc.github.io][3])
* `with OpenTelemetryPlugin(...):` uses the same start/end pair. ([grpc.github.io][3])

**Timing invariant (high-impact gotcha)**

* The lower-level grpcio observability hook explicitly states initialization must happen **“at the start of a program, before any channels/servers are built”**. While grpcio-observability wraps different symbols (`start_open_telemetry_observability`), it is architecturally the same “install tracer factories into core before object construction” pattern. Treat late registration as “may not retrofit existing channels/servers.” ([Fuchsia Git Repositories][4])

### 1.3 Stability notes (API vs metrics)

* The **metrics themselves** are categorized by gRPC as **stable/on-by-default** (per-call, per-attempt, server), with **experimental/off-by-default** for retry/hedging, LB policy, xDS client. ([gRPC][5])
* The *Python API* is described in the gRFC as **EXPERIMENTAL** (historical doc; check your pinned grpcio version if you care about surface stability). ([Gitea: Git with a cup of tea][1])

---

## 2) `grpc_observability._open_telemetry_observability` (native binding boundary)

You don’t see implementation in the Python wrapper, but the observable semantics are:

### 2.1 Entry points

* `start_open_telemetry_observability(plugins: List[_OpenTelemetryPlugin])` (global install) ([grpc.github.io][3])
* `end_open_telemetry_observability()` (global uninstall) ([grpc.github.io][3])
* `_OpenTelemetryPlugin(self)` native wrapper created from the Python `OpenTelemetryPlugin` instance (bridges config + callback hooks). ([grpc.github.io][3])

### 2.2 Export batching knobs (Core→Python)

Environment variables (affect flush latency + memory + overhead):

* `GRPC_PYTHON_CENSUS_EXPORT_BATCH_INTERVAL` (default 0.5s)
* `GRPC_PYTHON_CENSUS_MAX_EXPORT_BUFFER_SIZE` (default 10,000)
* `GRPC_PYTHON_CENSUS_EXPORT_THRESHOLD` (default 0.7)
* `GRPC_PYTHON_CENSUS_EXPORT_THREAD_TIMEOUT` (default 10s)
  ([PyPI][2])

(Names say “CENSUS” for legacy reasons; behavior is still the Core→Python export path.) ([PyPI][2])

---

## 3) Label injection & metadata exchange (advanced / easy-to-miss)

### 3.1 Interface: `OpenTelemetryLabelInjector`

Defined in `grpc_observability._open_telemetry_plugin`:

* `get_labels_for_exchange() -> Dict[str, str|bytes]`

  * Intended for **wire-level metadata exchange** labels. ([grpc.github.io][3])
* `get_additional_labels(include_exchange_labels: bool) -> Dict[str, str]`

  * Labels added **directly onto metric points**; can optionally include the exchange labels. ([grpc.github.io][3])
* `deserialize_labels(labels: Dict[str, str|bytes]) -> Dict[str, str|bytes]`

  * Post-receipt decode step; example in docstring shows an exchange payload (bytes) being expanded into multiple final labels. ([grpc.github.io][3])

**What’s “actually wired” in Python?**

* The Python surface defines the injector interface, but the public `OpenTelemetryPlugin` wrapper does not expose a first-class parameter like `label_injector=...`. The most plausible attachment point is `plugin_options`, but the option protocol is intentionally abstract in the wrapper. So: **interface exists; wiring is native/option-driven** (verify in your pinned grpcio-observability build if you plan to rely on it). ([grpc.github.io][3])

### 3.2 Security/cardinality context (why injection is constrained)

* gRFC explicitly calls out **unregistered/generic method names** as a potential cardinality attack vector; default is to emit `"other"` unless overridden. ([Gitea: Git with a cup of tea][1])

---

## 4) Precise gRPC event → metric instrument mapping (A66 + current gRPC metrics guide)

### 4.1 Common labels (where values come from)

* `grpc.method`: full method name; generic/unregistered methods default to `"other"` unless allowed by filter. ([Gitea: Git with a cup of tea][1])
* `grpc.target`: canonicalized target URI used to create the channel. ([gRPC][5])
* `grpc.status`: gRPC status code string (e.g., `OK`, `CANCELLED`). ([gRPC][5])
* Optional attempt-scoped attributes (must be enabled via plugin API; experimental metrics always off by default):

  * `grpc.lb.locality`, `grpc.lb.backend_service` ([gRPC][5])

### 4.2 Client-side: CallTracer + CallAttemptTracer callouts → instruments

#### `grpc.client.call.duration` (stable, on-by-default)

* **Start timestamp:** after the application starts the RPC; “before payload serialization” is the intended inclusion point. ([Gitea: Git with a cup of tea][1])
* **End timestamp:** before status is delivered to the application. ([Gitea: Git with a cup of tea][1])
* **Labels:** `grpc.method`, `grpc.target`, `grpc.status`. ([Gitea: Git with a cup of tea][1])

#### Attempt lifecycle (stable, on-by-default)

A66 defines the attempt creation boundary explicitly:

* Attempts are created **after name resolution and any xDS HTTP filters, but before the LB pick**. ([Gitea: Git with a cup of tea][1])

**(A) `grpc.client.attempt.started`**

* **Trigger:** on “new attempt created” event. ([Gitea: Git with a cup of tea][1])
* **Labels:** `grpc.method`, `grpc.target`. ([Gitea: Git with a cup of tea][1])

**(B) `grpc.client.attempt.duration`**

* **Start:** attempt creation event (same boundary above; includes LB pick time by definition). ([Gitea: Git with a cup of tea][1])
* **End:** when trailing metadata/status is received for the attempt (receipt implies attempt end). ([Gitea: Git with a cup of tea][1])
* **Labels:** required `grpc.method`, `grpc.target`, `grpc.status`; optional `grpc.lb.locality`, `grpc.lb.backend_service` (if enabled). ([gRPC][5])

**(C) `grpc.client.attempt.sent_total_compressed_message_size`**

* **Accumulation:** sum of **compressed** request message sizes (metadata excluded; framing excluded) across the attempt; A66 requires callouts for “message sent” with message in **compressed form**. ([Gitea: Git with a cup of tea][1])
* **Record point:** logically recorded once per attempt (histogram) at attempt completion (when trailing metadata/status received). ([Gitea: Git with a cup of tea][1])

**(D) `grpc.client.attempt.rcvd_total_compressed_message_size`**

* Symmetric to sent: sum compressed response message sizes; recorded per attempt. ([Gitea: Git with a cup of tea][1])

### 4.3 Server-side: ServerCallTracer callouts → instruments

A66’s server boundary is transport-centric (not app handler centric):

* Call start: when initial metadata is received by the transport (new stream recognized). ([Gitea: Git with a cup of tea][1])
* Call end: first point transport considers stream “done” (e.g., scheduling trailing headers with END_STREAM, RST_STREAM, abort). ([Gitea: Git with a cup of tea][1])

**(A) `grpc.server.call.started` (stable, on-by-default)**

* **Trigger:** when transport receives initial metadata for a call (start of stream). ([Gitea: Git with a cup of tea][1])
* **Labels:** `grpc.method`. ([Gitea: Git with a cup of tea][1])

**(B) `grpc.server.call.duration` (stable, on-by-default)**

* **Start:** after transport knows it has a new stream (HTTP/2: after first header frame decode; HPACK timing is impl-defined). ([Gitea: Git with a cup of tea][1])
* **End:** when trailing metadata/status is sent / stream completion is scheduled (transport perspective). ([Gitea: Git with a cup of tea][1])
* **Labels:** `grpc.method`, `grpc.status`. ([Gitea: Git with a cup of tea][1])

**(C) `grpc.server.call.sent_total_compressed_message_size`**

* Sum of compressed response message sizes (metadata excluded; framing excluded); callouts require message sent/received in compressed form. ([Gitea: Git with a cup of tea][1])

**(D) `grpc.server.call.rcvd_total_compressed_message_size`**

* Sum of compressed request message sizes (metadata excluded; framing excluded). ([Gitea: Git with a cup of tea][1])

### 4.4 Retry / LB / xDS metrics (experimental, always off-by-default)

gRPC defines additional instrument groups:

* Client per-call retry instruments (`grpc.client.call.retries`, `transparent_retries`, `hedges`, `retry_delay`) are **experimental**. ([gRPC][5])
* LB policy + xDS client metrics are **experimental**. ([gRPC][5])
  Whether your Python build can enable these depends on what native plugin options are exposed/implemented for your grpcio/grpcio-observability version. (The gRPC guide notes some instruments/attributes require explicit enablement via plugin API.) ([gRPC][5])

---

## 5) Gotchas & drift points (operationally expensive if you miss them)

### 5.1 `grpc.method == "other"` surprises

* Default `generic_method_attribute_filter` returns `False`, explicitly driving `"other"` for generic methods. ([grpc.github.io][3])
* This shows up in practice as “all metrics attributed to `other`” reports. ([GitHub][6])

### 5.2 `grpc.aio` support gap

* Tracked issue: grpcio-observability “does not properly add metrics” for `grpc.aio` (AsyncIO). ([GitHub][7])

### 5.3 “Tracing exists in code, not in product”

* Maintainer response: grpcio-observability **currently only works for metrics**; tracing hooks exist but are not fully implemented (as of Apr 2024). ([Google Groups][8])

### 5.4 Interpretation nuance: compressed vs uncompressed message bytes

* A66 calls out historical OpenCensus cross-language disagreement; the OpenTelemetry spec here is **compressed bytes** (matching C++/Java; not Go’s legacy OpenCensus behavior). ([Gitea: Git with a cup of tea][1])

### 5.5 Histogram boundary expectations

* A66 specifies recommended bucket boundaries (latency/size/count) for compatibility; in OpenTelemetry you typically enforce these via SDK Views (the API “advice” mechanism is experimental and not universally implemented). ([Gitea: Git with a cup of tea][1])

---

If you want the *next* slice in the same “module-by-module” style, the most leverage is: (a) enumerate the concrete `plugin_options` supported by your pinned grpcio-observability build (that’s where optional labels + experimental instruments usually get surfaced), and (b) document the exact semantics of `grpc.target` canonicalization + any target-filter replacement behavior.

[1]: https://gitea.cncfstack.com/grpc/proposal/src/branch/master/A66-otel-stats.md "proposal/A66-otel-stats.md at master - proposal - Gitea: Git with a cup of tea"
[2]: https://pypi.org/project/grpcio-observability/?utm_source=chatgpt.com "grpcio-observability"
[3]: https://grpc.github.io/grpc/python/_modules/grpc_observability/_open_telemetry_plugin.html "grpc_observability._open_telemetry_plugin — gRPC Python 1.76.0 documentation"
[4]: https://fuchsia.googlesource.com/third_party/grpc/%2B/refs/tags/v1.57.0/src/python/grpcio/grpc/_observability.py "src/python/grpcio/grpc/_observability.py - third_party/grpc - Git at Google"
[5]: https://grpc.io/docs/guides/opentelemetry-metrics/ "OpenTelemetry Metrics | gRPC"
[6]: https://github.com/grpc/grpc/issues/38265?utm_source=chatgpt.com "grpcio-observability cannot get instruments each grpc. ..."
[7]: https://github.com/grpc/grpc/issues/39061?utm_source=chatgpt.com "Add support for grpc.aio in grcio-observability · Issue #39061"
[8]: https://groups.google.com/g/grpc-io/c/PoTO2PddBBE "[gRPC-Python] tracing for retries"


## (a) `plugin_options` in grpcio-observability (what concrete options exist, how they’re applied)

### Where the hook is declared

`grpc_observability._open_telemetry_plugin` defines:

* `class OpenTelemetryPluginOption:` (empty marker/interface; no methods) ([grpc.github.io][1])
* `OpenTelemetryPlugin.__init__(..., plugin_options: Optional[Iterable[OpenTelemetryPluginOption]] = None, ...)` which merely stores `self.plugin_options = plugin_options or []`. ([grpc.github.io][1])

### What the hook *does today* (in the current public Python implementation)

In the published Python surface (gRPC Python docs for 1.76.0 / grpcio-observability 1.76.0), `plugin_options` are **not consumed** by any visible Python-side logic:

* The only “downstream” method that looks like an enablement point is `OpenTelemetryPlugin._get_enabled_optional_labels(self) -> List[OptionalLabelType]`, but it is hardcoded to return `[]`. ([grpc.github.io][1])
* No concrete `OpenTelemetryPluginOption` subclasses are defined in that module, and the base `OpenTelemetryPluginOption` has no API surface to implement. ([grpc.github.io][1])

**Net:** for the “vanilla” `grpc_observability.OpenTelemetryPlugin`, `plugin_options` are currently an **inert extension slot** (placeholder for future / native-side wiring), not a catalog of toggles you can enumerate and use from Python in the way C++/Java builders expose “enable optional labels/instruments”.

This matters because gRPC’s OpenTelemetry metrics guide explicitly notes that **some instruments are off-by-default and must be explicitly enabled via the gRPC OpenTelemetry plugin API**, and that **experimental metrics are always off-by-default**. ([gRPC][2])
The Python `OpenTelemetryPlugin` as published (constructor + filters + global register) does not expose an obvious “enable X” surface beyond those filters. ([grpc.github.io][3])

### Practical “enumerate your pinned build” audit (fast, mechanical)

If you want an LLM agent to *prove* what your pinned wheel exposes (vs relying on docs), the check is:

```python
import inspect
import grpc_observability._open_telemetry_plugin as otel

opt_base = otel.OpenTelemetryPluginOption
concrete = [
    obj for obj in vars(otel).values()
    if inspect.isclass(obj) and issubclass(obj, opt_base) and obj is not opt_base
]
print(concrete)
```

On the documented 1.76.0 surface, this should be empty because no subclasses are defined there. ([grpc.github.io][1])

And for version pin confirmation (wheel-level truth):

```python
from importlib.metadata import version
print(version("grpcio-observability"))
```

(PyPI shows grpcio-observability 1.76.0 released Oct 21, 2025.) ([PyPI][4])

---

## (b) `grpc.target` semantics: canonicalization + target-filter replacement (“other”)

### Canonicalization (definition)

gRFC **A66** defines `grpc.target` as:

* **Canonicalized target URI used when creating the gRPC Channel** (examples: `dns:///pubsub.googleapis.com:443`, `xds:///helloworld-gke:8000`). ([Gitea: Git with a cup of tea][5])
* Canonicalized form includes the **scheme even if the user omitted it**: `scheme://[authority]/path`. ([Gitea: Git with a cup of tea][5])
* If a “target URI is not available” (e.g., certain in-process channels), implementations may **synthesize** a target URI. ([Gitea: Git with a cup of tea][5])

So, for a metrics consumer: `grpc.target` is intended to be a **channel-scoped identifier** (“what endpoint/resolver target did this channel represent?”), normalized into URI form (scheme present). ([Gitea: Git with a cup of tea][5])

### Target cardinality control: `target_attribute_filter` → replace with `"other"` (deprecated, but implemented)

The current Python API includes `target_attribute_filter` (explicitly marked **[DEPRECATED]** in docs/source). Its semantics are:

* Called **per channel** to decide whether to record the actual `grpc.target` string or replace it with `"other"`. ([grpc.github.io][1])
* Return `True` → keep original target; return `False` → emit `"other"`. ([grpc.github.io][1])
* Rationale: reduce metric label cardinality when many channels have distinct targets (e.g., IP-literal targets). ([grpc.github.io][1])

The module also hard-defines the label key/value constants:

* `GRPC_TARGET_LABEL = "grpc.target"`
* `GRPC_OTHER_LABEL_VALUE = "other"` ([grpc.github.io][1])

### Spec-side note on target cardinality (why the filter exists at all)

A66 explicitly flags that some channels may use IP addresses as targets and that this may “blow up” cardinality; it notes a future possibility to “override recorded target names” for this reason. ([Gitea: Git with a cup of tea][5])

### Operationally-relevant consequences (what an agent should assume)

* The replacement is **channel-wide**, not call-wide: once a channel is classified as “keep” vs “other”, every per-call/per-attempt metric emitted through that channel will carry the chosen `grpc.target` value. (That’s implied by “called per channel” and the fact that `grpc.target` is defined as the channel creation target.) ([grpc.github.io][1])
* `grpc.target` is **not** “peer address” and not “chosen backend”; it’s the *logical target URI* used to create the channel (resolver/LB/xDS input), hence why optional LB labels like `grpc.lb.locality` exist separately in the broader metrics spec. ([Gitea: Git with a cup of tea][5])

If you want the next slice after this: the highest-leverage continuation is to map “which instruments/attributes are *in principle* enableable per the gRPC metrics guide” vs “which are *actually reachable* from Python’s published API (today)”, since the guide explicitly references off-by-default instruments but Python’s surface doesn’t currently expose an enablement catalog beyond filters. ([gRPC][2])

[1]: https://grpc.github.io/grpc/python/_modules/grpc_observability/_open_telemetry_plugin.html "grpc_observability._open_telemetry_plugin — gRPC Python 1.76.0 documentation"
[2]: https://grpc.io/docs/guides/opentelemetry-metrics/ "OpenTelemetry Metrics | gRPC"
[3]: https://grpc.github.io/grpc/python/grpc_observability.html "gRPC Python Observability — gRPC Python 1.76.0 documentation"
[4]: https://pypi.org/project/grpcio-observability/?utm_source=chatgpt.com "grpcio-observability"
[5]: https://gitea.cncfstack.com/grpc/proposal/src/branch/master/A66-otel-stats.md "proposal/A66-otel-stats.md at master - proposal - Gitea: Git with a cup of tea"


## Enablement surface: spec vs Python (`grpcio-observability` / `grpc_observability`) as shipped today

### 0) What the gRPC metrics guide says is *enableable in principle* (cross-language)

* Instrument groups + defaults:

  * **Client per-call** + **client per-attempt**: **stable, on by default**. ([gRPC][1])
  * **Client per-call retry**, **LB policy (WRR / Pick First)**, **XdsClient**: **experimental**. ([gRPC][1])
  * Global note: **some instruments are off by default and require explicit enablement via the gRPC OpenTelemetry plugin API; experimental metrics are always off-by-default**. ([gRPC][1])
* Attribute defaults:

  * Global note: **optional attributes on instruments must be explicitly enabled from the gRPC OpenTelemetry plugin API**. ([gRPC][1])
* View/export customization is *not* gRPC enablement; it’s **OTel SDK Views** (“customize the views exported by OpenTelemetry”). ([gRPC][1])

### 1) What the Python published API *actually exposes today*

`grpc_observability.OpenTelemetryPlugin` exposes:

* **hard on/off**: `meter_provider: MeterProvider | None` (None ⇒ “no metrics collected”) ([grpc.github.io][2])
* **cardinality filters only**:

  * `generic_method_attribute_filter`: controls whether **generic/unregistered methods** keep their name vs replaced with `"other"` (default is `False` ⇒ `"other"`). ([grpc.github.io][2])
  * `target_attribute_filter` (**deprecated**): controls whether `grpc.target` is kept vs replaced with `"other"` (default is keep). ([grpc.github.io][2])
* **placeholder extension slot**: `plugin_options: Iterable[OpenTelemetryPluginOption]` exists, but:

  * `_get_enabled_optional_labels()` is hardcoded to `return []` (no optional labels enabled). ([grpc.github.io][2])
  * no concrete option classes are defined in the published Python module surface. ([grpc.github.io][2])

**Implication:** from Python’s documented surface, you can (1) turn metrics on/off, (2) bound method/target cardinality, but you **cannot** explicitly enable: optional labels, experimental instruments, or any “off-by-default” instrument sets referenced by the guide’s plugin-API notes. ([gRPC][1])

---

## 2) Instrument reachability matrix (guide enablement vs Python surface)

Legend:

* **Spec default** = what the guide states (on-by-default vs experimental/off-by-default).
* **Needs plugin enable?** = per the guide’s notes (explicit enablement via gRPC plugin API).
* **Python knob** = something you can actually do via `OpenTelemetryPlugin(...)`.
* **Python-effective** = reachable assuming normal defaults.

| Instrument set         | Instruments (guide)                                                                                                                        |       Spec default |                                           Needs plugin enable? | Python knob(s)                                                                 |                              Python-effective today |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ | -----------------: | -------------------------------------------------------------: | ------------------------------------------------------------------------------ | --------------------------------------------------: |
| Client per-call        | `grpc.client.call.duration` ([gRPC][1])                                                                                                    |        stable / on |                                                             no | `meter_provider`                                                               |               **Yes** (if `meter_provider != None`) |
| Client per-attempt     | `grpc.client.attempt.started`, `...duration`, `...sent_total_compressed_message_size`, `...rcvd_total_compressed_message_size` ([gRPC][1]) |        stable / on | no (for instruments), **yes (for optional attrs)** ([gRPC][1]) | `meter_provider`; *no optional-label enablement surface* ([grpc.github.io][2]) | **Yes (base instruments)**; **No (optional attrs)** |
| Server instruments     | `grpc.server.call.started`, `...duration`, `...sent_total_compressed_message_size`, `...rcvd_total_compressed_message_size` ([gRPC][1])    |        stable / on |                                                             no | `meter_provider`                                                               |               **Yes** (if `meter_provider != None`) |
| Client per-call retry  | `grpc.client.call.retries`, `...transparent_retries`, `...hedges`, `...retry_delay` ([gRPC][1])                                            | experimental / off |       **yes** (experimental always off-by-default) ([gRPC][1]) | none exposed                                                                   |                                              **No** |
| LB policy (WRR)        | `grpc.lb.wrr.*` ([gRPC][1])                                                                                                                | experimental / off |                                            **yes** ([gRPC][1]) | none exposed                                                                   |                                              **No** |
| LB policy (Pick First) | `grpc.lb.pick_first.*` ([gRPC][1])                                                                                                         | experimental / off |                                            **yes** ([gRPC][1]) | none exposed                                                                   |                                              **No** |
| XdsClient              | `grpc.xds_client.*` ([gRPC][1])                                                                                                            | experimental / off |                                            **yes** ([gRPC][1]) | none exposed                                                                   |                                              **No** |

**OTel SDK Views (export-time):** If an instrument exists (e.g., stable per-call/per-attempt/server), Python can still *suppress/reshape* it via the SDK Views mechanism (aggregation/buckets/drop-by-name), but that does **not** substitute for gRPC plugin “enable instrument” knobs. ([gRPC][1])

---

## 3) Attribute/label reachability matrix (guide dispositions vs Python control)

| Attribute                                                                        | Where it appears (guide)                                           |         Disposition (guide) | Spec enablement note                                        | Python-effective today                                           | Python control                                                                                            |
| -------------------------------------------------------------------------------- | ------------------------------------------------------------------ | --------------------------: | ----------------------------------------------------------- | ---------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `grpc.method` ([gRPC][1])                                                        | required on most per-call/per-attempt/server metrics ([gRPC][1])   |                    required | (none)                                                      | **Yes**, but **may be `"other"`** for generic/unregistered calls | `generic_method_attribute_filter` (default `False` ⇒ `"other"` for generic methods) ([grpc.github.io][2]) |
| `grpc.target` ([gRPC][1])                                                        | required on client metrics + many experimental metrics ([gRPC][1]) |                    required | (none)                                                      | **Yes** on client metrics, unless replaced                       | `target_attribute_filter` (deprecated; can force `"other"`) ([grpc.github.io][2])                         |
| `grpc.status` ([gRPC][1])                                                        | required on duration + size histograms ([gRPC][1])                 |                    required | (none)                                                      | **Yes**                                                          | none                                                                                                      |
| `grpc.lb.locality` ([gRPC][1])                                                   | optional on attempt metrics; optional on WRR metrics ([gRPC][1])   |                **optional** | optional attrs must be enabled via plugin API ([gRPC][1])   | **No** (not enabled)                                             | no public enablement surface (`_get_enabled_optional_labels() -> []`) ([grpc.github.io][2])               |
| `grpc.lb.backend_service` ([gRPC][1])                                            | optional on attempt metrics; optional on WRR metrics ([gRPC][1])   |                **optional** | optional attrs must be enabled via plugin API ([gRPC][1])   | **No** (not enabled)                                             | same as above ([grpc.github.io][2])                                                                       |
| `grpc.xds.*` (`server`, `authority`, `cache_state`, `resource_type`) ([gRPC][1]) | required labels on xDS instruments ([gRPC][1])                     | required (within xDS group) | xDS instruments are experimental/off-by-default ([gRPC][1]) | **No** (because xDS instruments not reachable)                   | no public enablement surface                                                                              |

---

### Bottom line (dense)

* **Reachable from Python API today:** stable per-call/per-attempt/server instruments + required attrs (`grpc.method`, `grpc.target`, `grpc.status`) with **method/target cardinality controls** and **OTel SDK View shaping**. ([gRPC][1])
* **Not reachable from Python API today:** any “explicit enable” for **optional attrs** (`grpc.lb.locality`, `grpc.lb.backend_service`) and any **experimental / off-by-default instrument groups** (retry/LB/xDS), because Python exposes no concrete plugin options and enables no optional labels. ([gRPC][1])

[1]: https://grpc.io/docs/guides/opentelemetry-metrics/ "OpenTelemetry Metrics | gRPC"
[2]: https://grpc.github.io/grpc/python/_modules/grpc_observability/_open_telemetry_plugin.html "grpc_observability._open_telemetry_plugin — gRPC Python 1.76.0 documentation"
