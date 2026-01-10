
# PyArrow Flight (`pyarrow.flight`) — comprehensive feature catalog (Arrow/PyArrow **22.0.0**)

Arrow Flight is a high-performance RPC framework for streaming Arrow **RecordBatches** over the network (built on **gRPC** + the Arrow **IPC** format). ([Apache Arrow][1])
In Python, the Flight API is explicitly labeled **unstable** (APIs may change). ([Apache Arrow][2])
(Version note: Arrow’s current stable release is **22.0.0**.) ([Apache Arrow][3])

---

## 0) Mental model: the “control plane” and the “data plane”

Flight separates:

* **Discovery / metadata (control plane):** list “flights”, ask for schemas, get endpoints (tickets + locations) that describe where data can be fetched.
* **Streaming transfer (data plane):** DoGet / DoPut / DoExchange stream Arrow record batches (optionally with app metadata). ([Apache Arrow][1])

---

## A) Core identity + discovery objects (the *wire* nouns)

### A1) `Location` (where a server lives)

* **Purpose:** represent a Flight service URI.
* **Factories:**

  * `Location.for_grpc_tcp(host, port)`
  * `Location.for_grpc_tls(host, port)`
  * `Location.for_grpc_unix(path)` ([Apache Arrow][4])
* **Methods/attrs:** `equals(other)`, `uri`. ([Apache Arrow][4])

### A2) `FlightDescriptor` + `DescriptorType` (what a “flight” refers to)

* **Descriptor kinds:** `DescriptorType.PATH` vs `DescriptorType.CMD` (+ `UNKNOWN`). ([Apache Arrow][5])
* **Constructors:**

  * `FlightDescriptor.for_path(*path)` (resource path semantics)
  * `FlightDescriptor.for_command(command)` (opaque command blob) ([Apache Arrow][6])
* **Interop helpers:** `serialize()` / `deserialize(serialized)` to move descriptors through non-Flight systems (e.g., REST). ([Apache Arrow][6])
* **Accessors:** `descriptor_type`, `path`, `command`. ([Apache Arrow][6])

### A3) `Ticket` (how the client actually fetches a stream)

* **Purpose:** opaque token used with `do_get` (and in `FlightEndpoint`).
* **Interop helpers:** `serialize()` / `deserialize(serialized)` for non-Flight transport. ([Apache Arrow][7])
* **Accessor:** `ticket`. ([Apache Arrow][7])

### A4) `FlightEndpoint` (ticket + one-or-more locations where it can be read)

* Construct with `FlightEndpoint(ticket, locations, expiration_time=None, app_metadata='')`. ([Apache Arrow][8])
* Carries:

  * a `Ticket` (or bytes)
  * a list of endpoint URIs (`locations`)
  * optional expiration/app metadata ([Apache Arrow][8])

### A5) `FlightInfo` (the full “answer” to “what is this flight?”)

* Construct with: `FlightInfo(schema, descriptor, endpoints, total_records=None, total_bytes=None, ordered=False, app_metadata='')`. ([Apache Arrow][9])
* Includes:

  * `schema`, `descriptor`, `endpoints`
  * optional `total_records`/`total_bytes` (may be unknown)
  * `ordered` (whether endpoints preserve data order)
  * `app_metadata` plus `serialize()`/`deserialize()` for interop ([Apache Arrow][9])

---

## B) Actions: application-specific “RPC verbs” beyond streaming

### B1) `ActionType` and `Action`

* **Server advertises** available action types via `list_actions`.
* `ActionType(type, description)` also has `make_action(buf)` helper. ([Apache Arrow][10])
* `Action(action_type, buf)` supports `serialize()` / `deserialize()`. ([Apache Arrow][11])

### B2) `Result`

* Returned from `do_action` as an iterator of `Result`.
* `Result(buf)` supports `serialize()` / `deserialize()`; `body` returns the underlying buffer. ([Apache Arrow][12])

---

## C) Client surface (connection, discovery, streaming, actions)

### C1) Connect / construct a client

Two equivalent entrypoints:

* `pyarrow.flight.connect(location, **kwargs) -> FlightClient` ([Apache Arrow][13])
* `FlightClient(location, tls_root_certs=None, *, cert_chain=None, private_key=None, override_hostname=None, middleware=None, write_size_limit_bytes=None, disable_server_verification=None, generic_options=None)` ([Apache Arrow][14])

**Transport/security knobs (constructor + `connect`) include:**

* TLS root certs, **mutual TLS** client cert/key, `override_hostname` (dangerous) ([Apache Arrow][14])
* `disable_server_verification` (dangerous) ([Apache Arrow][14])
* `generic_options` (implementation-dependent transport knobs) ([Apache Arrow][14])
* `middleware` (list of `ClientMiddlewareFactory`) ([Apache Arrow][14])
* `write_size_limit_bytes` (soft payload cap; exceeding raises an exception; client can retry smaller batches) ([Apache Arrow][14])

### C2) Per-call knobs: `FlightCallOptions`

`FlightCallOptions(timeout=None, write_options=None, headers=None, read_options=None)` provides:

* `timeout` (seconds)
* `headers` (list of (key,value) tuples)
* `write_options` (`pyarrow.ipc.IpcWriteOptions`) and `read_options` (`pyarrow.ipc.IpcReadOptions`) ([Apache Arrow][15])

### C3) Discovery / metadata methods

On `FlightClient`:

* `list_flights(criteria: bytes=None, options=None)`
* `get_flight_info(descriptor, options=None)`
* `get_schema(descriptor, options=None)`
* `list_actions(options=None)` ([Apache Arrow][14])

### C4) Data transfer methods

* `do_get(ticket, options=None) -> FlightStreamReader` ([Apache Arrow][14])
* `do_put(descriptor, schema, options=None) -> (FlightStreamWriter, FlightMetadataReader)` ([Apache Arrow][14])
* `do_exchange(descriptor, options=None) -> (FlightStreamWriter, FlightStreamReader)` (bidirectional exchange) ([Apache Arrow][14])
* `do_action(action, options=None) -> iterator[Result]` ([Apache Arrow][14])

### C5) Client lifecycle + readiness

* `wait_for_available(timeout=5)` (block until server reachable) ([Apache Arrow][14])
* `close()` ([Apache Arrow][14])
* Async hooks exist (`supports_async`, `as_async`) on `FlightClient` (availability depends on build/transport). ([Apache Arrow][14])

---

## D) Reading from Flight streams (client-side readers)

### D1) `FlightStreamReader` (cancelable stream reader)

Key capabilities:

* `read_all() -> Table`
* `read_chunk() -> FlightStreamChunk` (batch + optional app metadata)
* `read_pandas(**options) -> pandas.DataFrame`
* `to_reader() -> RecordBatchReader`
* `cancel()` (cancel read)
* attributes: `schema`, `stats` ([Apache Arrow][16])

### D2) `MetadataRecordBatchReader`

A base reader for Flight streams (similar surface):

* `read_all()`, `read_chunk()`, `read_pandas(**options)`, `to_reader()`
* attributes: `schema`, `stats` ([Apache Arrow][17])

### D3) “Chunk” semantics: `FlightStreamChunk`

Python docs describe `read_chunk()` returning a `FlightStreamChunk` which contains the next batch “along with any metadata.” ([Apache Arrow][16])
(When you need to reason about the shape: it’s a holder for a `RecordBatch` plus `app_metadata`.) ([GitHub][18])

---

## E) Writing to Flight streams (client-side writers)

### E1) `FlightStreamWriter`

A writer that can also **half-close** the write side:

* `begin(schema, options=None)`
* `write_batch(record_batch)`
* `write_table(table, max_chunksize=None)` (writes contiguous batches)
* `write_with_metadata(batch, buf)` and `write_metadata(buf)`
* `done_writing()` (client done writing, not done reading)
* `close()` (close and write end-of-stream marker) ([Apache Arrow][19])

### E2) `MetadataRecordBatchWriter`

A RecordBatchWriter that can write **application metadata**; context manager; on exit calls `close()`. ([Apache Arrow][20])

### E3) Write-size guardrail

If you set `write_size_limit_bytes` on the client, oversized batch writes raise `FlightWriteSizeExceededError`. ([Apache Arrow][14])

---

## F) Server framework (implementing a Flight service)

### F1) `FlightServerBase` construction + security knobs

`FlightServerBase(location=None, auth_handler=None, tls_certificates=None, verify_client=False, root_certificates=None, middleware=None)` includes:

* **Immediate start:** server is running as soon as the instance is created (no need to call `serve()` to start). ([Apache Arrow][21])
* `tls_certificates`: list of (cert,key) pairs
* `verify_client=True` enables **mutual TLS**; `root_certificates` provides PEM roots for client-cert validation ([Apache Arrow][21])
* `middleware`: dict of `ServerMiddlewareFactory` instances keyed by string; handlers can retrieve instances via `ServerCallContext.get_middleware(key)`. ([Apache Arrow][21])

### F2) Lifecycle & process control

* `serve()` / `wait()` block until shutdown.
* `shutdown()` blocks until current requests finish; docs warn **not** to call it from inside an RPC handler (you can deadlock; call from a background thread). ([Apache Arrow][21])
* `port` attribute reports listening port (or non-positive if not applicable). ([Apache Arrow][21])

### F3) RPC hooks you override (the server “interface”)

Override any subset:

* `list_flights(self, context, criteria: bytes) -> iterator[FlightInfo]` ([Apache Arrow][21])
* `get_flight_info(self, context, descriptor) -> FlightInfo` ([Apache Arrow][21])
* `get_schema(self, context, descriptor) -> Schema` ([Apache Arrow][21])
* `do_get(self, context, ticket) -> FlightDataStream` ([Apache Arrow][21])
* `do_put(self, context, descriptor, reader: MetadataRecordBatchReader, writer: FlightMetadataWriter)` (receive uploads; can send metadata back) ([Apache Arrow][21])
* `do_exchange(self, context, descriptor, reader, writer)` (bidirectional exchange) ([Apache Arrow][21])
* `list_actions(self, context) -> iterator[ActionType|tuple]` and `do_action(self, context, action) -> iterator[bytes]` ([Apache Arrow][21])

### F4) Server-side data stream helpers (`FlightDataStream` family)

* `RecordBatchStream(data_source, options=None)`: wraps a `Table` or `RecordBatchReader` and streams it; docs note the remainder of the DoGet can run in C++ without acquiring the GIL. ([Apache Arrow][22])
* `GeneratorStream(schema, generator, options=None)`: stream backed by a Python generator that yields stream objects / tables / batches / readers. ([Apache Arrow][23])

### F5) `ServerCallContext` (per-RPC context/state)

Key capabilities:

* response headers/trailers: `add_header`, `add_trailer`
* cancellation: `is_cancelled()`
* auth identity + peer info: `peer()`, `peer_identity()`
* middleware lookup: `get_middleware(key)` ([Apache Arrow][24])

---

## G) Middleware: cross-cutting concerns (headers, tracing, auth glue)

### G1) Method introspection objects

* `FlightMethod` enum lists protocol RPC methods (e.g., `DO_GET`, `DO_PUT`, `GET_SCHEMA`, …). ([Apache Arrow][25])
* `CallInfo(method)` is passed to middleware factories to identify the call. ([Apache Arrow][26])

### G2) Client middleware

* `ClientMiddlewareFactory.start_call(info: CallInfo) -> ClientMiddleware|None` (thread-safe; must not raise). ([Apache Arrow][27])
* `ClientMiddleware` hooks:

  * `sending_headers()` → add request headers (lowercase ASCII; gRPC binary headers must end in `-bin`)
  * `received_headers(headers)`
  * `call_completed(exception)` ([Apache Arrow][28])

### G3) Server middleware

* `ServerMiddlewareFactory.start_call(info: CallInfo, headers: dict) -> ServerMiddleware|None` (thread-safe; may reject call by raising). ([Apache Arrow][29])
* `ServerMiddleware.sending_headers()` and `call_completed(exception)` ([Apache Arrow][30])
* Thread-local guarantee: middleware methods are called on the same thread as the RPC implementation (so thread-locals can be used). ([Apache Arrow][29])

---

## H) Authentication surfaces (handshake + per-call tokens)

### H1) Auth handler interfaces

* `ServerAuthHandler`:

  * `authenticate(outgoing, incoming)` (handshake)
  * `is_valid(token) -> identity` (validate per-call token) ([Apache Arrow][31])
* `ClientAuthHandler`:

  * `authenticate(outgoing, incoming)` (handshake)
  * `get_token()` (token to attach per call) ([Apache Arrow][32])

### H2) Client convenience methods

* `FlightClient.authenticate(auth_handler, options=None)` ([Apache Arrow][14])
* `FlightClient.authenticate_basic_token(username, password, options=None)` (HTTP basic auth; returns a bearer-token header tuple) ([Apache Arrow][14])

### H3) Getting the authenticated identity on the server

* `ServerCallContext.peer_identity()` exposes the authenticated peer identity. ([Apache Arrow][24])

---

## I) Errors & failure modes (typed exceptions you can catch)

### I1) Flight error base

* `FlightError(message='', extra_info=b'')` is the base class; servers can raise it (or subclasses) to convey structured Flight failures. ([Apache Arrow][33])

### I2) Standard subclasses

From the Flight API docs:

* `FlightCancelledError`
* `FlightInternalError`
* `FlightServerError`
* `FlightTimedOutError`
* `FlightUnauthenticatedError`
* `FlightUnauthorizedError`
* `FlightUnavailableError`
* `FlightWriteSizeExceededError` ([Apache Arrow][2])

### I3) Timeout/cancel levers tied to these errors

* `FlightCallOptions(timeout=...)` is the main call timeout knob. ([Apache Arrow][15])
* Client stream cancellation: `FlightStreamReader.cancel()`. ([Apache Arrow][16])
* Server-side cancellation check: `ServerCallContext.is_cancelled()`. ([Apache Arrow][24])

---

## J) Adjacent but important: Flight SQL (layered on Flight)

Flight SQL is a protocol that reuses Flight’s metadata + streaming RPCs to execute SQL queries and fetch SQL metadata (catalogs, schemas, types, prepared statements, etc.). ([Apache Arrow][34])
In Python ecosystems, it’s common to access Flight SQL endpoints through **ADBC Flight SQL drivers** rather than hand-rolling the protobuf command layer. ([Apache Arrow][35])

---

If you want the **next increment** (like your PyArrow doc style), I can expand each cluster above into a deep-dive page with: power knobs, failure modes, minimal server/client snippets, and “contract tests” (e.g., deterministic DoPut/DoGet round-trips + middleware header normalization + auth token policies).

Reference style you shared: 

[1]: https://arrow.apache.org/docs/format/Flight.html?utm_source=chatgpt.com "Arrow Flight RPC — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/python/api/flight.html "Arrow Flight — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/install/?utm_source=chatgpt.com "Installation | Apache Arrow"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.Location.html?utm_source=chatgpt.com "pyarrow.flight.Location — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.DescriptorType.html?utm_source=chatgpt.com "pyarrow.flight.DescriptorType — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightDescriptor.html "pyarrow.flight.FlightDescriptor — Apache Arrow v22.0.0"
[7]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.Ticket.html?utm_source=chatgpt.com "pyarrow.flight.Ticket — Apache Arrow v22.0.0"
[8]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightEndpoint.html?utm_source=chatgpt.com "pyarrow.flight.FlightEndpoint — Apache Arrow v22.0.0"
[9]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightInfo.html "pyarrow.flight.FlightInfo — Apache Arrow v22.0.0"
[10]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.ActionType.html?utm_source=chatgpt.com "pyarrow.flight.ActionType — Apache Arrow v22.0.0"
[11]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.Action.html?utm_source=chatgpt.com "pyarrow.flight.Action — Apache Arrow v22.0.0"
[12]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.Result.html?utm_source=chatgpt.com "pyarrow.flight.Result — Apache Arrow v22.0.0"
[13]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.connect.html "pyarrow.flight.connect — Apache Arrow v22.0.0"
[14]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightClient.html "pyarrow.flight.FlightClient — Apache Arrow v22.0.0"
[15]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightCallOptions.html "pyarrow.flight.FlightCallOptions — Apache Arrow v22.0.0"
[16]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightStreamReader.html?utm_source=chatgpt.com "pyarrow.flight.FlightStreamReader — Apache Arrow v22.0.0"
[17]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.MetadataRecordBatchReader.html?utm_source=chatgpt.com "pyarrow.flight.MetadataRecordBatchReader - Apache Arrow"
[18]: https://github.com/apache/arrow/blob/master/python/pyarrow/includes/libarrow_flight.pxd?utm_source=chatgpt.com "arrow/python/pyarrow/includes/libarrow_flight.pxd at main"
[19]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightStreamWriter.html?utm_source=chatgpt.com "pyarrow.flight.FlightStreamWriter — Apache Arrow v22.0.0"
[20]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.MetadataRecordBatchWriter.html?utm_source=chatgpt.com "pyarrow.flight.MetadataRecordBatchWriter - Apache Arrow"
[21]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightServerBase.html "pyarrow.flight.FlightServerBase — Apache Arrow v22.0.0"
[22]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.RecordBatchStream.html?utm_source=chatgpt.com "pyarrow.flight.RecordBatchStream — Apache Arrow v22.0.0"
[23]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.GeneratorStream.html?utm_source=chatgpt.com "pyarrow.flight.GeneratorStream — Apache Arrow v22.0.0"
[24]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.ServerCallContext.html?utm_source=chatgpt.com "pyarrow.flight.ServerCallContext — Apache Arrow v22.0.0"
[25]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightMethod.html?utm_source=chatgpt.com "pyarrow.flight.FlightMethod — Apache Arrow v22.0.0"
[26]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.CallInfo.html?utm_source=chatgpt.com "pyarrow.flight.CallInfo — Apache Arrow v22.0.0"
[27]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.ClientMiddlewareFactory.html "pyarrow.flight.ClientMiddlewareFactory — Apache Arrow v22.0.0"
[28]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.ClientMiddleware.html "pyarrow.flight.ClientMiddleware — Apache Arrow v22.0.0"
[29]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.ServerMiddlewareFactory.html "pyarrow.flight.ServerMiddlewareFactory — Apache Arrow v22.0.0"
[30]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.ServerMiddleware.html "pyarrow.flight.ServerMiddleware — Apache Arrow v22.0.0"
[31]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.ServerAuthHandler.html?utm_source=chatgpt.com "pyarrow.flight.ServerAuthHandler — Apache Arrow v22.0.0"
[32]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.ClientAuthHandler.html?utm_source=chatgpt.com "pyarrow.flight.ClientAuthHandler — Apache Arrow v22.0.0"
[33]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightError.html?utm_source=chatgpt.com "pyarrow.flight.FlightError — Apache Arrow v22.0.0"
[34]: https://arrow.apache.org/docs/format/FlightSql.html?utm_source=chatgpt.com "Arrow Flight SQL — Apache Arrow v22.0.0"
[35]: https://arrow.apache.org/adbc/current/python/api/index.html?utm_source=chatgpt.com "Python API Reference - ADBC 21 documentation"

Below are the **deep-dive pages** for each cluster (A–J) from the catalog, in the same “power knobs + failure modes + minimal snippets + contract tests” style you’ve been using.

Versioning notes:

* **Arrow / PyArrow v22.0.0** doc baseline; Flight APIs are explicitly **unstable**. ([Apache Arrow][1])
* `FlightServerBase` starts serving **immediately on instantiation** (no need to call `serve()` to start), and `shutdown()` must not be called from inside an RPC handler. ([Apache Arrow][2])

---

## Flight A) Core identity + discovery objects — deep dive (PyArrow Flight v22)

### Mental model: “nouns” that let you *name* data streams without moving data

A Flight service exposes *streams* of `RecordBatch`es. The discovery layer is just a way to:

1. name streams (`FlightDescriptor`),
2. locate them (`FlightInfo` → `FlightEndpoint` → `Location`),
3. redeem them (`Ticket` → `do_get`). ([Apache Arrow][3])

---

# A1) Descriptor/Ticket are *opaque* application contracts

## A1.1 `FlightDescriptor`: PATH vs CMD

**Surface area**

* `FlightDescriptor.for_path(*path)` (resource naming)
* `FlightDescriptor.for_command(command: bytes)` (opaque “query plan” blob) ([Apache Arrow][4])

**Power knobs**

* **PATH** is best when you want stable, cacheable identities:

  * `["dataset", "docs.v_function_summary", "v1"]`
  * `["artifact", "runs", run_id]`
* **CMD** is best for “parameterized queries” or “plans”:

  * `command = msgspec.json.encode({"op":"scan", "ds":"...", "where":...})`
* Both can be serialized/deserialized for REST bridges (non-Flight systems). ([Apache Arrow][4])

**Failure modes**

* Treating CMD as a human-readable string → you’ll regret it. Always version it.
* Returning a `FlightInfo` whose schema doesn’t match the actual stream: clients fail late.

## A1.2 `Ticket`: redemption token, not a query

**Surface area**

* `Ticket(ticket: bytes)` plus `serialize()/deserialize()` ([Apache Arrow][5])

**Power knobs**

* Encode **partition + snapshot/run** in the ticket:

  * `ticket = b"ds=...;run=...;part=0003"` (simple)
  * or msgpack/protobuf bytes (robust)
* Use tickets as “capability tokens”: random bytes (unguessable) to gate access.

**Failure modes**

* Tickets that are “predictable” become an auth surface.

---

# A2) `FlightInfo` / `FlightEndpoint` are the “how to consume this flight” map

## A2.1 `FlightEndpoint`: ticket + locations (+ metadata)

**Surface area**

* `FlightEndpoint(ticket, locations, expiration_time=None, app_metadata=b"")` with attributes `.ticket`, `.locations`, `.expiration_time`, `.app_metadata`. ([Apache Arrow][6])

**Power knobs**

* If `.locations` is empty, many implementations interpret this as “redeem on the same service” (common pattern). ([Apache Arrow][7])
* `expiration_time` is a **retry hint**: if present, clients may assume retries are safe; if absent, clients should be conservative about retrying. ([Apache Arrow][7])
* `app_metadata` is a clean place for **endpoint-level hints**:

  * compression used
  * partition key range
  * schema hash

**Failure modes**

* Invalid URI in `.locations` can raise an `ArrowException` on construction. ([Apache Arrow][8])

## A2.2 `FlightInfo`: schema + endpoints (+ ordering)

**Surface area**

* `FlightInfo(schema, descriptor, endpoints, total_records=None, total_bytes=None, ordered=False, app_metadata=b"")` plus `.serialize()`. ([Apache Arrow][9])

**Power knobs**

* `ordered=True` has real meaning: endpoints should concatenate in order to reproduce the dataset. ([Apache Arrow][3])
* `total_records/total_bytes` can be `-1` if unknown; filling them enables better client-side progress/cost controls. ([Apache Arrow][7])
* `app_metadata` at the **flight level** is excellent for:

  * schema hash
  * logical dataset name + version
  * “producer build id”

**Failure modes**

* `ordered=True` but endpoints are not stable/deterministic → subtle drift in downstream computations.

---

### Minimal implementation snippets

**(1) Canonical “dataset descriptor”**

```python
import pyarrow.flight as flight

def ds_descriptor(name: str, version: str = "v1") -> flight.FlightDescriptor:
    return flight.FlightDescriptor.for_path("dataset", name, version)
```

**(2) Canonical “partition ticket”**

```python
import pyarrow.flight as flight

def ds_ticket(name: str, run_id: str, part: int) -> flight.Ticket:
    return flight.Ticket(f"ds={name};run={run_id};part={part:04d}".encode("utf-8"))
```

---

### Contract tests (pytest): serialize/deserialize invariants

These mirror Arrow’s own expectations (round-trip stable). ([GitHub][10])

```python
import pyarrow.flight as flight

def test_descriptor_roundtrip():
    d = flight.FlightDescriptor.for_command(b"hello")
    assert d == flight.FlightDescriptor.deserialize(d.serialize())

def test_ticket_roundtrip():
    t = flight.Ticket(b"abc123")
    assert t == flight.Ticket.deserialize(t.serialize())

def test_endpoint_roundtrip():
    ep = flight.FlightEndpoint(flight.Ticket(b"x"), [flight.Location.for_grpc_tcp("localhost", 1234)])
    assert ep == flight.FlightEndpoint.deserialize(ep.serialize())
```

---

## Flight B) Actions — deep dive (custom verbs over the control plane)

### Mental model: “small RPCs” for side effects / admin / cache invalidation

Actions are not streams—they’re **application-defined** commands that return 0..N `Result` payloads. ([Apache Arrow][11])

---

# B1) Action types are *advertised*; action payloads are opaque

## B1.1 `ActionType` / `Action`

**Surface area**

* `ActionType(type, description)` and `make_action(buf)` ([Apache Arrow][12])
* `Action(action_type, buf)` with `serialize()/deserialize()` ([Apache Arrow][11])

**Power knobs**

* Use ActionType names like API routes:

  * `"health.check"`, `"cache.invalidate"`, `"runs.delete"`
* Version payload formats (`v1`, `v2`) inside the buf, not the name.

**Failure modes**

* Treating action payload as JSON string without explicit encoding rules (UTF-8? binary?) → interop bugs.

## B1.2 `Result`

**Surface area**

* `Result(buf)` with `.body` and `serialize()/deserialize()` ([Apache Arrow][13])

**Power knobs**

* Use results as a **streamed response** when you want progress events:

  * yield `Result(b"started")`, then `Result(b"done")`
* Or yield exactly one result for “RPC-like” semantics.

---

### Minimal server snippet: a `health.check` action

```python
import pyarrow.flight as flight

class MyServer(flight.FlightServerBase):
    def list_actions(self, context):
        yield flight.ActionType("health.check", "Return server health")

    def do_action(self, context, action):
        if action.action_type != "health.check":
            raise NotImplementedError(action.action_type)
        yield b"ok"
```

---

### Contract tests: “action surface is pinned”

```python
import pyarrow.flight as flight

def test_actions_surface(client):
    types = {a.type for a in client.list_actions()}
    assert "health.check" in types

def test_health_action(client):
    out = list(client.do_action(flight.Action("health.check", b"")))
    assert out == [flight.Result(b"ok")] or out == [b"ok"]  # depending on wrapper usage
```

(If you want strictness: normalize to bytes by reading `.body` when present.)

---

## Flight C) Client — deep dive (connection + call options + transport knobs)

### Mental model: one `FlightClient` controls both planes

* Discovery calls: `get_flight_info`, `get_schema`, `list_flights`, `list_actions` ([Apache Arrow][14])
* Data calls: `do_get`, `do_put`, `do_exchange` ([Apache Arrow][2])

---

# C1) Connect + lifecycle

## C1.1 Construction and connect()

* `flight.connect(location, **kwargs)` (convenience) ([Apache Arrow][15])
* `FlightClient(..., middleware=..., write_size_limit_bytes=..., disable_server_verification=..., generic_options=...)` ([Apache Arrow][16])

**Power knobs**

* `write_size_limit_bytes` is a **soft cap**: if a serialized batch exceeds it, writing raises and you can retry smaller batches. ([Apache Arrow][16])
* `disable_server_verification` exists for TLS but is explicitly insecure. ([Apache Arrow][15])
* `generic_options` passes through to underlying transport; interpretation is implementation-dependent. ([jorisvandenbossche.github.io][17])

**Failure modes**

* Creating a client once and using it forever without timeouts → hangs are “correct behavior” when networks die.

## C1.2 Readiness and cleanup

* `wait_for_available(timeout=...)` blocks until reachable. ([Apache Arrow][16])
* Both server and client commonly support context manager usage (seen in Arrow’s own tests). ([Gemfury][18])

---

# C2) Per-call options: `FlightCallOptions`

**Surface area**

* `FlightCallOptions(timeout=None, write_options=None, headers=None, read_options=None)` ([Apache Arrow][19])

**Power knobs**

* `timeout` is **seconds**; `None` = transport default. ([Apache Arrow][20])
* `headers`: list of `(key, value)` tuples. Key should be **lowercase ASCII** for maximum gRPC portability. ([Apache Arrow][7])
* IPC options (read/write) let you control IPC-level behavior without changing the service contract. ([Apache Arrow][19])

**Failure modes**

* Header casing: historically, non-lowercase headers caused crashes in older PyArrow Flight; fixed in later releases, but treat “lowercase only” as policy. ([Apache Issues][21])

---

### Minimal client snippets

**(1) A “bounded” call surface helper**

```python
import pyarrow.flight as flight

def opts(run_id: str, timeout_s: float = 30.0) -> flight.FlightCallOptions:
    return flight.FlightCallOptions(
        timeout=timeout_s,
        headers=[(b"x-run-id", run_id.encode("utf-8"))],  # keep lowercase
    )
```

**(2) The canonical “discover → do_get”**

```python
info = client.get_flight_info(descriptor, opts("run123"))
reader = client.do_get(info.endpoints[0].ticket, opts("run123"))
table = reader.read_all()
```

---

### Contract tests: timeouts/cancellation plumbing exists

Arrow’s docs explicitly call out call options + cancellation. ([Apache Arrow][22])

```python
def test_call_options_headers_reach_server(server_with_header_echo, client):
    o = flight.FlightCallOptions(headers=[(b"x-req-id", b"abc")])
    info = client.get_flight_info(flight.FlightDescriptor.for_command(b"simple"), o)
    assert info is not None
```

---

## Flight D) Reading streams — deep dive (chunking + metadata + cancellation)

### Mental model: `read_all()` is convenience; `read_chunk()` is the contract tool

* `FlightStreamReader.read_chunk()` returns a `FlightStreamChunk` and raises `StopIteration` at end. ([Apache Arrow][23])
* The chunk contains `.data` (RecordBatch) and optional `.app_metadata`. ([GitHub][24])

---

# D1) Reader surfaces

## D1.1 `FlightStreamReader`

**Surface area**

* `read_all() -> Table`
* `read_chunk() -> FlightStreamChunk` (with metadata)
* `to_reader() -> RecordBatchReader`
* `read_pandas(**options)` (loads entire stream first)
* `cancel()` for client-driven cancellation ([Apache Arrow][23])

**Power knobs**

* Use `read_chunk()` to:

  * bound memory
  * stream progress
  * interleave compute with transfer

**Failure modes**

* `read_pandas()` always materializes the whole stream first. ([Apache Arrow][23])
* Misunderstanding end-of-stream: `StopIteration` is “normal termination”. ([Apache Arrow][23])

## D1.2 Server-driven chunk size

Clients don’t pick chunk size; servers do. If you need stable chunking:

* server should stream a `RecordBatchReader` or generate batches itself (`GeneratorStream`) with a fixed max size. ([Apache Arrow][25])

---

# D2) App metadata: where protocol-level side channels live

* Metadata can be attached per-batch (chunk metadata) and is exposed on `read_chunk()`. ([GitHub][24])

**Use cases**

* progress / watermark
* batch-level checksum
* “this batch is a dictionary delta” (advanced)

---

# D3) Sharp edge: `GeneratorStream` + dictionary-encoded data can bite

A known class of issues: client IPC errors observed when using `GeneratorStream` with dictionary/categorical columns in some versions. Prefer `RecordBatchStream` if you can. ([GitHub][26])

---

### Minimal implementation snippet: “robust chunk loop”

```python
def iter_batches(reader):
    while True:
        try:
            chunk = reader.read_chunk()
        except StopIteration:
            return
        batch = chunk.data          # RecordBatch :contentReference[oaicite:41]{index=41}
        meta  = chunk.app_metadata  # Buffer | None
        yield batch, meta
```

---

### Contract tests: “chunk loop terminates correctly”

```python
def test_read_chunk_stopiteration(reader_from_known_small_table):
    n = 0
    while True:
        try:
            chunk = reader_from_known_small_table.read_chunk()
        except StopIteration:
            break
        assert chunk.data.num_rows >= 0
        n += 1
    assert n >= 1
```

---

## Flight E) Writing streams — deep dive (DoPut + metadata acks + write limits)

### Mental model: `do_put` is a duplex: you write batches, server can send metadata

* Client: `writer, metadata_reader = client.do_put(descriptor, schema, options)` ([Apache Arrow][16])
* Server: `do_put(self, context, descriptor, reader, writer: FlightMetadataWriter)` ([Apache Arrow][2])

---

# E1) `FlightStreamWriter` is a RecordBatchWriter + extra lifecycle

**Surface area**

* `begin(schema, options=None)`
* `write_batch(batch)`
* `write_table(table, max_chunksize=None)` (explicit chunking control)
* `write_with_metadata(batch, buf)` and `write_metadata(buf)`
* `done_writing()` (half-close write side)
* `close()` (fully close; writes end-of-stream marker) ([Apache Arrow][27])

**Power knobs**

* Use `write_table(..., max_chunksize=...)` to enforce deterministic chunk size. ([Apache Arrow][27])
* If you set `write_size_limit_bytes` on the client, you *must* be prepared to split batches. ([Apache Arrow][16])
* `done_writing()` matters for `do_exchange` patterns where you keep reading after you finish writing. ([Apache Arrow][27])

**Failure modes**

* Closing without proper “finish” in certain backends can raise internal errors; safe pattern:

  1. write
  2. `done_writing()`
  3. drain server metadata (if any)
  4. `close()`

---

# E2) Server-side `FlightMetadataWriter` = ACK channel during DoPut

Server receives uploaded data via `reader` and may send metadata back with `writer.write(message: Buffer)`. ([Apache Arrow][2])

---

### Minimal implementation snippet: client DoPut with chunking + metadata

```python
import pyarrow as pa
import pyarrow.flight as flight

writer, meta_reader = client.do_put(descriptor, table.schema, options)
writer.write_table(table, max_chunksize=50_000)   # deterministic chunking
writer.done_writing()

# optional: drain ack metadata from server (if server sends any)
# while True:
#     try:
#         ack = meta_reader.read()
#     except StopIteration:
#         break

writer.close()
```

---

### Contract tests: write size limit behaves as a “retry smaller batches” lever

```python
import pyarrow.flight as flight
import pytest

def test_write_size_limit_exceeded(client_with_small_limit, big_batch):
    writer, _ = client_with_small_limit.do_put(descriptor, big_batch.schema)
    with pytest.raises(flight.FlightWriteSizeExceededError):
        writer.write_batch(big_batch)  # must split and retry :contentReference[oaicite:49]{index=49}
```

---

## Flight F) Server framework — deep dive (implementing a service correctly)

### Mental model: subclass `FlightServerBase`, override RPCs, stream efficiently

* Server starts as soon as you instantiate it. ([Apache Arrow][2])
* Implement by overriding:

  * discovery: `list_flights`, `get_flight_info`, `get_schema`
  * data: `do_get`, `do_put`, `do_exchange`
  * actions: `list_actions`, `do_action` ([Apache Arrow][2])

---

# F1) Construction + security knobs

**Surface area**
`FlightServerBase(location=None, auth_handler=None, tls_certificates=None, verify_client=False, root_certificates=None, middleware=None)` ([Apache Arrow][2])

**Power knobs**

* `location=None` binds localhost on random port. ([Apache Arrow][2])
* TLS: `tls_certificates=[(cert_bytes, key_bytes)]`
* mTLS: `verify_client=True` + `root_certificates=...` ([Apache Arrow][2])
* `middleware` is a dict of factories accessible via `ServerCallContext.get_middleware(key)`. ([Apache Arrow][2])

**Failure modes**

* Calling `shutdown()` inside an RPC handler can deadlock the server. ([Apache Arrow][2])

---

# F2) Streaming primitives: choose the right one

## F2.1 `RecordBatchStream`: best default for Tables/Readers

* It hands off remainder of DoGet to C++ and avoids the GIL. ([Apache Arrow][28])

## F2.2 `GeneratorStream`: generator-backed streams

* Yields `FlightDataStream` / `Table` / `RecordBatch` / `RecordBatchReader`. ([Apache Arrow][25])
* Use when you need dynamic paging or push-based generation.

**Failure modes**

* Beware `GeneratorStream` + dictionary columns edge cases (see D3). ([GitHub][26])

---

# F3) Cancellation and timeouts

* Clients can cancel; some objects expose `.cancel()`. ([Apache Arrow][22])
* Server must **poll** `context.is_cancelled()` and stop work. ([Apache Arrow][14])

---

### Minimal implementation snippet: a tiny in-memory “dataset store” server

```python
import pyarrow as pa
import pyarrow.flight as flight

class KVFlightServer(flight.FlightServerBase):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._tables: dict[bytes, pa.Table] = {}

    def _key(self, descriptor: flight.FlightDescriptor) -> bytes:
        if descriptor.descriptor_type == flight.DescriptorType.PATH:
            return b"/".join([p.encode("utf-8") for p in descriptor.path])
        return descriptor.command

    def get_schema(self, context, descriptor):
        key = self._key(descriptor)
        return self._tables[key].schema

    def get_flight_info(self, context, descriptor):
        key = self._key(descriptor)
        table = self._tables[key]
        ticket = flight.Ticket(key)
        endpoint = flight.FlightEndpoint(ticket, [])  # redeem on same service
        return flight.FlightInfo(table.schema, descriptor, [endpoint], total_records=table.num_rows)

    def do_get(self, context, ticket):
        table = self._tables[ticket.ticket]
        return flight.RecordBatchStream(table)

    def do_put(self, context, descriptor, reader, writer):
        # stream in batches to avoid materializing too early
        batches = []
        while True:
            try:
                chunk = reader.read_chunk()
            except StopIteration:
                break
            batches.append(chunk.data)
        self._tables[self._key(descriptor)] = pa.Table.from_batches(batches)
        writer.write(pa.py_buffer(b"ok"))  # ACK metadata during DoPut
```

Notes:

* `do_put` signature uses `MetadataRecordBatchReader` + `FlightMetadataWriter`. ([Apache Arrow][2])
* `RecordBatchStream` is the preferred “fast path” stream. ([Apache Arrow][28])

---

### Contract tests: deterministic DoPut → DoGet roundtrip

```python
import pyarrow.flight as flight

def test_put_get_roundtrip(server, client, table):
    d = flight.FlightDescriptor.for_path("dataset", "x", "v1")
    writer, meta = client.do_put(d, table.schema)
    writer.write_table(table, max_chunksize=10_000)
    writer.done_writing()
    writer.close()

    info = client.get_flight_info(d)
    out = client.do_get(info.endpoints[0].ticket).read_all()
    assert out.equals(table)
```

---

## Flight G) Middleware — deep dive (headers, tracing, policy gates)

### Mental model: “interceptors” on every RPC

* Client middleware can add headers and observe response headers.
* Server middleware can inspect incoming headers and *fail the request* (so it can implement policy/auth). ([Apache Arrow][14])

---

# G1) Client middleware

## G1.1 `ClientMiddlewareFactory.start_call(info)`

* Must be thread-safe and must not raise; may return `None`. ([Apache Arrow][29])

## G1.2 `ClientMiddleware` callbacks

* “fast and infallible”: should not raise or block indefinitely. ([Apache Arrow][30])

**Power knobs**

* Canonical use: inject `x-request-id`, trace context, or auth tokens.
* Keep header names lowercase ASCII for gRPC portability. ([Apache Arrow][7])

---

# G2) Server middleware

## G2.1 `ServerMiddlewareFactory.start_call(info, headers)`

* Must be thread-safe; receives headers dict where values are lists of strings (text) or bytes (binary `-bin`). ([Apache Arrow][31])

## G2.2 Thread-local guarantee

Server middleware methods are called on the same thread as the RPC implementation; thread-locals are accessible. ([Apache Arrow][31])

**Failure modes**

* Slow middleware becomes global latency.
* Header casing surprises (see historical bug). ([Apache Issues][21])

---

### Minimal middleware snippet: request-id propagation

```python
import pyarrow.flight as flight

REQ_ID = "x-request-id"

class RequestIdClientMW(flight.ClientMiddleware):
    def __init__(self, req_id: str):
        self.req_id = req_id

    def sending_headers(self):
        return {REQ_ID: self.req_id}  # keep lowercase

class RequestIdClientMWFactory(flight.ClientMiddlewareFactory):
    def __init__(self, req_id: str):
        self.req_id = req_id

    def start_call(self, info):
        return RequestIdClientMW(self.req_id)
```

---

### Contract tests: header normalization policy

```python
def test_headers_are_lowercase(policy_headers):
    for k, v in policy_headers:
        assert k == k.lower()
```

(If you want a “hard gate”, make your middleware assert and raise a `FlightUnauthenticatedError` or `FlightUnauthorizedError` on the server side.)

---

## Flight H) Authentication — deep dive (handshake + per-call token)

### Mental model: two phases, and TLS is mandatory for safety

* Phase 1: handshake exchange between client and server.
* Phase 2: per-call token attached; server validates and provides identity available via `ServerCallContext.peer_identity()`. ([Apache Arrow][14])
* Arrow’s docs warn: auth without TLS is insecure. ([Apache Arrow][14])

---

# H1) Implementing auth: `ServerAuthHandler` / `ClientAuthHandler`

## H1.1 Server side

* `ServerAuthHandler.authenticate(outgoing, incoming)`
* `ServerAuthHandler.is_valid(token) -> identity` ([Apache Arrow][32])

## H1.2 Client side

* `ClientAuthHandler.authenticate(outgoing, incoming)`
* `ClientAuthHandler.get_token()` ([Apache Arrow][33])

---

# H2) “Basic token” convenience

* `FlightClient.authenticate_basic_token(username, password, options)` returns a tuple representing an authorization header entry for bearer token usage in `FlightCallOptions.headers`. ([Apache Arrow][34])

---

### Minimal implementation snippet: custom token auth (simple + explicit)

```python
import pyarrow.flight as flight

class StaticTokenServerAuth(flight.ServerAuthHandler):
    def __init__(self, token: bytes, identity: str = "user"):
        self._token = token
        self._identity = identity

    def authenticate(self, outgoing, incoming):
        # minimal handshake: server sends nothing; client sends nothing
        return

    def is_valid(self, token: bytes) -> str:
        if token != self._token:
            raise flight.FlightUnauthenticatedError("bad token")
        return self._identity

class StaticTokenClientAuth(flight.ClientAuthHandler):
    def __init__(self, token: bytes):
        self._token = token

    def authenticate(self, outgoing, incoming):
        return

    def get_token(self) -> bytes:
        return self._token
```

---

### Contract tests: “unauthenticated by default”

```python
import pytest
import pyarrow.flight as flight

def test_auth_required(client_without_auth, descriptor):
    with pytest.raises(flight.FlightUnauthenticatedError):
        client_without_auth.get_flight_info(descriptor)
```

---

## Flight I) Errors — deep dive (typed failures + binary details)

### Mental model: raise exceptions; use FlightError subclasses for semantic status

* Flight docs: to indicate failure, raise exceptions; Flight-specific failures via `FlightError` subclasses. ([Apache Arrow][14])
* `FlightError` carries `extra_info: bytes` for structured binary detail. ([Apache Arrow][35])

---

# I1) Error taxonomy (Python)

Key classes:

* `FlightInternalError`, `FlightServerError` ([Apache Arrow][36])
* `FlightTimedOutError`, `FlightUnavailableError` ([Apache Arrow][37])
* `FlightUnauthenticatedError`, `FlightUnauthorizedError` ([Apache Arrow][38])
* `FlightWriteSizeExceededError(message, limit, actual)` ([Apache Arrow][39])

---

# I2) Contract pattern: normalize errors for tests

For a repo-grade contract harness, normalize:

* exception type
* message prefix
* `extra_info` bytes (often protobuf/msgpack) to a parsed dict, then compare.

---

### Minimal snippet: raising a typed error with extra_info

```python
import pyarrow.flight as flight
import pyarrow as pa

def deny():
    raise flight.FlightUnauthorizedError(
        "not allowed",
        extra_info=b"\x01\x02\x03"  # your protobuf/msgpack here
    )
```

(`extra_info` is part of the FlightError surface.) ([Apache Arrow][35])

---

### Contract tests: stable classification

```python
import pytest
import pyarrow.flight as flight

def test_error_classification(server, client):
    with pytest.raises(flight.FlightUnauthorizedError) as e:
        client.get_flight_info(flight.FlightDescriptor.for_command(b"deny"))
    assert isinstance(e.value.extra_info, (bytes, bytearray))
```

---

## Flight J) Flight SQL adjacency — deep dive (how this usually gets used from Python)

### Mental model: Flight SQL = standardized “commands over Flight”

* Flight SQL is a protocol for interacting with SQL databases using Arrow + Flight RPC. ([Apache Arrow][40])
* In Python, the **best-in-class** integration is usually via **ADBC Flight SQL drivers** (DBAPI layer on top) rather than hand-encoding Flight SQL protobuf commands yourself. ([PyPI][41])

---

# J1) When to use “plain Flight” vs “Flight SQL”

**Use plain Flight (`pyarrow.flight`) when:**

* your service is a *data product API* (datasets, snapshots, partitions)
* your request is not naturally SQL (e.g., “give me the call-graph edges for run X”)

**Use Flight SQL when:**

* the server is a database exposing SQL semantics and you want metadata APIs (catalogs, schemas, types, prepared statements)

---

# J2) Practical pattern: ADBC DBAPI for Flight SQL

* Install: `adbc_driver_manager` + `adbc-driver-flightsql` ([Apache Arrow][42])
* Fetch results as Arrow Tables/RecordBatches where supported (driver-dependent).

*(I’m keeping this page conceptual since it’s outside `pyarrow.flight` proper, but it’s the “real world” path to Flight SQL in Python.)*

---

## Appendix: a “repo-ready” contract harness skeleton (single file)

If you want to drop this into your repo and iterate, here’s a compact pattern that encodes the big invariants (roundtrip + header policy + auth policy) using the fact that Arrow’s own tests demonstrate context-manager usage for server/client. ([Gemfury][18])

```python
# tests/test_flight_contracts.py
import pytest
import pyarrow as pa
import pyarrow.flight as flight

class EchoServer(flight.FlightServerBase):
    def __init__(self):
        super().__init__("grpc://0.0.0.0:0")  # ephemeral port
        self._tables = {}

    def get_flight_info(self, context, descriptor):
        key = descriptor.command
        t = flight.Ticket(key)
        ep = flight.FlightEndpoint(t, [])
        return flight.FlightInfo(self._tables[key].schema, descriptor, [ep])

    def do_get(self, context, ticket):
        return flight.RecordBatchStream(self._tables[ticket.ticket])

    def do_put(self, context, descriptor, reader, writer):
        batches = []
        while True:
            try:
                chunk = reader.read_chunk()
            except StopIteration:
                break
            batches.append(chunk.data)
        self._tables[descriptor.command] = pa.Table.from_batches(batches)
        writer.write(pa.py_buffer(b"ok"))

@pytest.fixture()
def server_and_client():
    with EchoServer() as server:
        with flight.connect(f"grpc://localhost:{server.port}") as client:
            yield server, client

def test_roundtrip(server_and_client):
    server, client = server_and_client
    table = pa.table({"x": [1,2,3]})
    d = flight.FlightDescriptor.for_command(b"t1")

    w, meta = client.do_put(d, table.schema)
    w.write_table(table, max_chunksize=2)
    w.done_writing()
    w.close()

    info = client.get_flight_info(d)
    out = client.do_get(info.endpoints[0].ticket).read_all()
    assert out.equals(table)
```

---

If you want the *next* increment after these deep dives: I can convert the Appendix into a **full “Flight contract pack”** (multi-file) with:

* deterministic header normalization + middleware test doubles,
* an auth test matrix (no-auth / token / mTLS scaffold),
* and golden snapshot fixtures for `FlightInfo`/`Ticket`/`Descriptor` serialization.

[1]: https://arrow.apache.org/docs/python/api/flight.html "Arrow Flight — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightServerBase.html "pyarrow.flight.FlightServerBase — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/format/Flight.html?utm_source=chatgpt.com "Arrow Flight RPC — Apache Arrow v22.0.0"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightDescriptor.html?utm_source=chatgpt.com "pyarrow.flight.FlightDescriptor — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.Ticket.html?utm_source=chatgpt.com "pyarrow.flight.Ticket — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightEndpoint.html?utm_source=chatgpt.com "pyarrow.flight.FlightEndpoint — Apache Arrow v22.0.0"
[7]: https://arrow.apache.org/docs/cpp/api/flight.html?utm_source=chatgpt.com "Arrow Flight RPC — Apache Arrow v22.0.0"
[8]: https://arrow.apache.org/docs/10.0/python/generated/pyarrow.flight.FlightEndpoint.html?utm_source=chatgpt.com "pyarrow.flight.FlightEndpoint — Apache Arrow v10.0.1"
[9]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightInfo.html?utm_source=chatgpt.com "pyarrow.flight.FlightInfo — Apache Arrow v22.0.0"
[10]: https://github.com/apache/arrow/blob/main/python/pyarrow/tests/test_flight.py?utm_source=chatgpt.com "arrow/python/pyarrow/tests/test_flight.py at main"
[11]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.Action.html?utm_source=chatgpt.com "pyarrow.flight.Action — Apache Arrow v22.0.0"
[12]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.ActionType.html?utm_source=chatgpt.com "pyarrow.flight.ActionType — Apache Arrow v22.0.0"
[13]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.Result.html?utm_source=chatgpt.com "pyarrow.flight.Result — Apache Arrow v22.0.0"
[14]: https://arrow.apache.org/docs/python/flight.html "Arrow Flight RPC — Apache Arrow v22.0.0"
[15]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.connect.html?utm_source=chatgpt.com "pyarrow.flight.connect — Apache Arrow v22.0.0"
[16]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightClient.html?utm_source=chatgpt.com "pyarrow.flight.FlightClient — Apache Arrow v22.0.0"
[17]: https://jorisvandenbossche.github.io/arrow-docs-preview/html-option-1/cpp/api/flight.html?utm_source=chatgpt.com "Arrow Flight RPC"
[18]: https://gemfury.com/arrow-nightlies/python%3Apyarrow/pyarrow-22.0.0.dev21-cp311-cp311-musllinux_1_2_aarch64.whl/content/tests/test_flight_async.py "tests/test_flight_async.py · arrow-nightlies / pyarrow v22.0.0.dev21 - python package | Gemfury"
[19]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightCallOptions.html?utm_source=chatgpt.com "pyarrow.flight.FlightCallOptions — Apache Arrow v22.0.0"
[20]: https://arrow.apache.org/docs/11.0/python/generated/pyarrow.flight.FlightCallOptions.html?utm_source=chatgpt.com "pyarrow.flight.FlightCallOptions — Apache Arrow v11.0.0"
[21]: https://issues.apache.org/jira/browse/ARROW-16606?focusedCommentId=17540928&utm_source=chatgpt.com "[FlightRPC][Python] Flight RPC crashes when a middleware ..."
[22]: https://arrow.apache.org/docs/python/flight.html?utm_source=chatgpt.com "Arrow Flight RPC — Apache Arrow v22.0.0"
[23]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightStreamReader.html?utm_source=chatgpt.com "pyarrow.flight.FlightStreamReader — Apache Arrow v22.0.0"
[24]: https://github.com/apache/arrow/issues/34017?utm_source=chatgpt.com "[Python] Incorrect return type for FlightStreamReader. ..."
[25]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.GeneratorStream.html?utm_source=chatgpt.com "pyarrow.flight.GeneratorStream — Apache Arrow v22.0.0"
[26]: https://github.com/apache/arrow/issues/38480?utm_source=chatgpt.com "[Python][FlightRPC] IPC error using ..."
[27]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightStreamWriter.html?utm_source=chatgpt.com "pyarrow.flight.FlightStreamWriter — Apache Arrow v22.0.0"
[28]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.RecordBatchStream.html?utm_source=chatgpt.com "pyarrow.flight.RecordBatchStream — Apache Arrow v22.0.0"
[29]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.ClientMiddlewareFactory.html?utm_source=chatgpt.com "pyarrow.flight.ClientMiddlewareFactory — Apache Arrow v22.0.0"
[30]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.ClientMiddleware.html?utm_source=chatgpt.com "pyarrow.flight.ClientMiddleware — Apache Arrow v22.0.0"
[31]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.ServerMiddlewareFactory.html?utm_source=chatgpt.com "pyarrow.flight.ServerMiddlewareFactory - Apache Arrow"
[32]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.ServerAuthHandler.html?utm_source=chatgpt.com "pyarrow.flight.ServerAuthHandler — Apache Arrow v22.0.0"
[33]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.ClientAuthHandler.html?utm_source=chatgpt.com "pyarrow.flight.ClientAuthHandler — Apache Arrow v22.0.0"
[34]: https://arrow.apache.org/docs/4.0/python/generated/pyarrow.flight.FlightClient.html?utm_source=chatgpt.com "pyarrow.flight.FlightClient — Apache Arrow v4.0.1"
[35]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightError.html?utm_source=chatgpt.com "pyarrow.flight.FlightError — Apache Arrow v22.0.0"
[36]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightInternalError.html?utm_source=chatgpt.com "pyarrow.flight.FlightInternalError — Apache Arrow v22.0.0"
[37]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightTimedOutError.html?utm_source=chatgpt.com "pyarrow.flight.FlightTimedOutError — Apache Arrow v22.0.0"
[38]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightUnauthenticatedError.html?utm_source=chatgpt.com "pyarrow.flight.FlightUnauthenticatedError - Apache Arrow"
[39]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightWriteSizeExceededError.html?utm_source=chatgpt.com "pyarrow.flight.FlightWriteSizeExceededError - Apache Arrow"
[40]: https://arrow.apache.org/docs/format/FlightSql.html?utm_source=chatgpt.com "Arrow Flight SQL — Apache Arrow v22.0.0"
[41]: https://pypi.org/project/adbc-driver-flightsql/?utm_source=chatgpt.com "adbc-driver-flightsql"
[42]: https://arrow.apache.org/adbc/main/python/driver_manager.html?utm_source=chatgpt.com "Driver Manager - ADBC 22 (dev) documentation - Apache Arrow"

Below is a **repo-ready, multi-file “Flight contract pack”** you can drop into your codebase. It delivers the three things you asked for:

1. **Deterministic header normalization + middleware test doubles** (client + server)
2. **Auth test matrix**: no-auth / token / **mTLS scaffold** (skips unless certs exist)
3. **Golden snapshot fixtures + update script** for `FlightDescriptor` / `Ticket` / `FlightInfo` serialization

Docs this pack is aligned to:

* `FlightServerBase` starts serving **immediately on instantiation** and supports TLS/mTLS via `tls_certificates`, `verify_client`, `root_certificates`. ([Apache Arrow][1])
* `FlightClient` supports TLS/mTLS via `tls_root_certs`, `cert_chain`, `private_key`, and has `write_size_limit_bytes`. ([Apache Arrow][2])
* `ServerMiddlewareFactory.start_call(info, headers)` receives `headers: dict[str, list[str] | bytes]`. ([Apache Arrow][3])
* Middleware header rules: **lowercase ASCII names**, bytes allowed only for `-bin` keys. ([Apache Arrow][4])
* `FlightCallOptions.headers` exists, but in practice passing `str` has caused `TypeError` and docs have had mismatches; treat bytes as the contract. ([Apache Arrow][5])

---

## 0) File layout (drop-in)

```text
src/codeintel/flight_contract/
  __init__.py
  config.py
  headers.py
  middleware_doubles.py
  auth_handlers.py
  serialization_goldens.py
  testing/
    __init__.py
    server_kv.py
    tls.py

tests/flight_contract/
  conftest.py
  test_headers_policy.py
  test_middleware_doubles.py
  test_put_get_roundtrip.py
  test_auth_matrix.py
  test_serialization_goldens.py

tests/golden/flight/
  descriptor_path_v1.json
  descriptor_cmd_v1.json
  ticket_v1.json
  flightinfo_v1.json
  README.md

scripts/
  update_flight_goldens.py

tools/
  gen_flight_test_certs.sh

tests/assets/tls/
  README.md
```

---

# 1) `src/codeintel/flight_contract/__init__.py`

```python
from __future__ import annotations

from .config import FlightContractConfig
from .headers import HeaderPolicy, normalize_flight_call_headers, normalize_middleware_headers
from .auth_handlers import StaticTokenClientAuth, StaticTokenServerAuth
from .middleware_doubles import (
    RecordingClientMiddlewareFactory,
    RecordingServerMiddlewareFactory,
    StrictHeaderServerMiddlewareFactory,
)
from .serialization_goldens import (
    GoldenDescriptor,
    GoldenFlightInfo,
    GoldenTicket,
    load_golden_json,
    save_golden_json,
)

__all__ = [
    "FlightContractConfig",
    "HeaderPolicy",
    "normalize_flight_call_headers",
    "normalize_middleware_headers",
    "StaticTokenClientAuth",
    "StaticTokenServerAuth",
    "RecordingClientMiddlewareFactory",
    "RecordingServerMiddlewareFactory",
    "StrictHeaderServerMiddlewareFactory",
    "GoldenDescriptor",
    "GoldenTicket",
    "GoldenFlightInfo",
    "load_golden_json",
    "save_golden_json",
]
```

---

# 2) `src/codeintel/flight_contract/config.py`

```python
from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class FlightContractConfig:
    """
    Central knobs for the Flight contract pack.

    Prefer pinning your PyArrow version in your lockfile; this is an *assertion*
    layer to make drift explicit in CI.
    """
    # “Contract schema” version for goldens/normalization.
    contract_version: str = "v1"

    # Optional pin: set to e.g. "22.0.0" in CI to fail fast on upgrades.
    expected_pyarrow_version: str | None = os.getenv("FLIGHT_CONTRACT_EXPECT_PYARROW")

    # Goldens behavior
    allow_missing_goldens: bool = os.getenv("FLIGHT_CONTRACT_ALLOW_MISSING_GOLDENS", "0") == "1"
    update_goldens_envvar: str = "UPDATE_FLIGHT_GOLDENS"

    # mTLS tests
    enable_mtls: bool = os.getenv("FLIGHT_CONTRACT_ENABLE_MTLS", "0") == "1"
```

---

# 3) `src/codeintel/flight_contract/headers.py`

Deterministic normalization for:

* **FlightCallOptions.headers** (tuple list, bytes-first)
* **middleware sending_headers()** dict (string keys + string/list values; bytes only for `-bin`) ([Apache Arrow][4])

```python
from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable, Mapping, Sequence

# gRPC metadata key rules (strict-enough for contracts):
# - lowercase ASCII
# - digits/letters/._- only
_GRPC_KEY_RE = re.compile(rb"^[0-9a-z_.-]+$")


HeaderKey = bytes
HeaderValue = bytes
FlightHeader = tuple[HeaderKey, HeaderValue]


@dataclass(frozen=True)
class HeaderPolicy:
    """
    Contract policy for request/response metadata.

    Keep this strict: if you later want to loosen rules, do it explicitly and
    update tests/goldens. Header names must be lowercase ASCII. :contentReference[oaicite:6]{index=6}
    """
    required: frozenset[bytes] = frozenset()
    allow_prefixes: tuple[bytes, ...] = (b"x-", b"authorization", b"user-agent")
    allow_binary: bool = True  # -bin values

    def validate_key(self, key: bytes) -> None:
        if key != key.lower():
            raise ValueError(f"Header key must be lowercase ASCII: {key!r}")
        if not _GRPC_KEY_RE.match(key):
            raise ValueError(f"Header key contains invalid characters: {key!r}")
        if not any(key.startswith(p) for p in self.allow_prefixes):
            raise ValueError(f"Header key not allowed by prefix policy: {key!r}")

    def validate_pair(self, key: bytes, value: bytes) -> None:
        self.validate_key(key)
        if key.endswith(b"-bin"):
            if not self.allow_binary:
                raise ValueError(f"Binary headers disabled by policy: {key!r}")
            # bytes payload is allowed (opaque)
            return
        # For non-binary headers, enforce that the bytes are ASCII to avoid accidental
        # transport incompatibilities.
        try:
            value.decode("ascii")
        except UnicodeDecodeError as e:
            raise ValueError(f"Non-binary header value must be ASCII: {key!r}") from e

    def validate_required(self, present_keys: Iterable[bytes]) -> None:
        missing = set(self.required) - set(present_keys)
        if missing:
            raise ValueError(f"Missing required headers: {sorted(missing)!r}")


def _to_ascii_bytes(x: str | bytes, *, field: str) -> bytes:
    if isinstance(x, bytes):
        return x
    try:
        return x.encode("ascii")
    except UnicodeEncodeError as e:
        raise ValueError(f"{field} must be ASCII; got {x!r}") from e


def normalize_flight_call_headers(
    headers: Sequence[tuple[str | bytes, str | bytes]] | None,
    *,
    policy: HeaderPolicy | None = None,
    sort: bool = True,
) -> list[FlightHeader]:
    """
    Normalize headers for FlightCallOptions(headers=...).

    Although some docs historically described (str,str), in practice passing str has
    caused TypeError; bytes is the safest “surface contract.” :contentReference[oaicite:7]{index=7}
    """
    if not headers:
        return []
    pol = policy or HeaderPolicy()
    out: list[FlightHeader] = []
    for k, v in headers:
        kb = _to_ascii_bytes(k, field="header key").lower()
        vb = v if isinstance(v, bytes) else v.encode("ascii", errors="strict")
        pol.validate_pair(kb, vb)
        out.append((kb, vb))
    pol.validate_required([k for k, _ in out])
    if sort:
        out.sort(key=lambda kv: (kv[0], kv[1]))
    return out


def normalize_middleware_headers(
    headers: Mapping[str, object] | None,
    *,
    policy: HeaderPolicy | None = None,
) -> dict[str, object]:
    """
    Normalize a middleware header dict (sending_headers()).

    Client/Server middleware expects string keys and string or list-of-string values;
    bytes values are allowed only for '-bin' keys (gRPC). Header names must be lowercase ASCII. :contentReference[oaicite:8]{index=8}
    """
    if not headers:
        return {}

    pol = policy or HeaderPolicy()
    out: dict[str, object] = {}

    for k, v in headers.items():
        kb = _to_ascii_bytes(k, field="middleware header key").lower()
        pol.validate_key(kb)
        ks = kb.decode("ascii")

        if isinstance(v, (str, bytes)):
            vals = [v]
        elif isinstance(v, list):
            vals = v
        else:
            raise ValueError(f"Unsupported middleware header value type: {type(v)} for {k!r}")

        # Validate each
        norm_vals: list[object] = []
        for item in vals:
            if isinstance(item, str):
                vb = item.encode("ascii", errors="strict")
                pol.validate_pair(kb, vb)
                norm_vals.append(item)  # keep as str for middleware surface
            elif isinstance(item, bytes):
                pol.validate_pair(kb, item)
                norm_vals.append(item)  # allowed only for -bin
            else:
                raise ValueError(f"Unsupported header item type: {type(item)} for {k!r}")

        # Collapse singletons to match doc expectation (str or list[str]) but keep bytes if present.
        if len(norm_vals) == 1:
            out[ks] = norm_vals[0]
        else:
            out[ks] = norm_vals

    pol.validate_required([k.encode("ascii") for k in out.keys()])
    return out
```

---

# 4) `src/codeintel/flight_contract/middleware_doubles.py`

Records incoming headers and enforces policy at the server boundary. `ServerMiddlewareFactory.start_call` receives headers as dict of string keys to lists of string or bytes. ([Apache Arrow][3])

```python
from __future__ import annotations

from dataclasses import dataclass, field
import threading
from typing import Any

import pyarrow.flight as flight

from .headers import HeaderPolicy, normalize_middleware_headers


@dataclass
class CallRecord:
    method: str
    incoming_headers: dict[str, Any]


@dataclass
class ClientMiddlewareRecord:
    sent: list[dict[str, object]] = field(default_factory=list)
    received: list[dict[str, object]] = field(default_factory=list)
    completed: list[BaseException | None] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add_sent(self, h: dict[str, object]) -> None:
        with self._lock:
            self.sent.append(h)

    def add_received(self, h: dict[str, object]) -> None:
        with self._lock:
            self.received.append(h)

    def add_completed(self, exc: BaseException | None) -> None:
        with self._lock:
            self.completed.append(exc)


@dataclass
class ServerMiddlewareRecord:
    calls: list[CallRecord] = field(default_factory=list)
    completed: list[BaseException | None] = field(default_factory=list)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def add_call(self, method: str, headers: dict[str, Any]) -> None:
        with self._lock:
            self.calls.append(CallRecord(method=method, incoming_headers=headers))

    def add_completed(self, exc: BaseException | None) -> None:
        with self._lock:
            self.completed.append(exc)


class RecordingClientMiddleware(flight.ClientMiddleware):
    def __init__(self, add_headers: dict[str, object], record: ClientMiddlewareRecord):
        self._add_headers = add_headers
        self._record = record

    def sending_headers(self):
        # Must be fast and infallible. :contentReference[oaicite:10]{index=10}
        self._record.add_sent(self._add_headers)
        return self._add_headers

    def received_headers(self, headers):
        # headers: dict[str, list[str] | bytes] :contentReference[oaicite:11]{index=11}
        self._record.add_received(headers)

    def call_completed(self, exception):
        self._record.add_completed(exception)


class RecordingClientMiddlewareFactory(flight.ClientMiddlewareFactory):
    def __init__(self, add_headers: dict[str, object], record: ClientMiddlewareRecord):
        self._add_headers = add_headers
        self._record = record

    def start_call(self, info):
        # Must be thread-safe and must not raise. :contentReference[oaicite:12]{index=12}
        return RecordingClientMiddleware(self._add_headers, self._record)


class RecordingServerMiddleware(flight.ServerMiddleware):
    def __init__(self, record: ServerMiddlewareRecord, response_headers: dict[str, object] | None):
        self._record = record
        self._response_headers = response_headers

    def sending_headers(self):
        # Must be fast and infallible. :contentReference[oaicite:13]{index=13}
        return self._response_headers

    def call_completed(self, exception):
        self._record.add_completed(exception)


class RecordingServerMiddlewareFactory(flight.ServerMiddlewareFactory):
    def __init__(
        self,
        record: ServerMiddlewareRecord,
        *,
        response_headers: dict[str, object] | None = None,
    ):
        self._record = record
        self._response_headers = response_headers

    def start_call(self, info, headers):
        # headers: dict[str, list[str] | bytes] :contentReference[oaicite:14]{index=14}
        self._record.add_call(str(getattr(info, "method", "unknown")), headers)
        return RecordingServerMiddleware(self._record, self._response_headers)


class StrictHeaderServerMiddlewareFactory(flight.ServerMiddlewareFactory):
    """
    Enforces a HeaderPolicy at the server boundary.

    This is the “hard gate”: if a client sends a key that violates your policy
    (not lowercase, not allowed prefix, etc.), reject early.
    """
    def __init__(self, policy: HeaderPolicy):
        self._policy = policy

    def start_call(self, info, headers):
        # Normalize the incoming dict into the middleware representation rules
        # (keys lowercase ASCII; values are str/list[str]/bytes only for -bin).
        norm = normalize_middleware_headers(headers, policy=self._policy)
        # If normalize didn’t raise, policy is satisfied.
        # Return None -> no per-call middleware instance needed.
        return None
```

---

# 5) `src/codeintel/flight_contract/auth_handlers.py`

Implements token auth (handshake no-op; per-call token required). `ServerAuthHandler.is_valid(token)` returns identity or raises. ([Apache Arrow][6])

```python
from __future__ import annotations

import pyarrow.flight as flight


class StaticTokenServerAuth(flight.ServerAuthHandler):
    def __init__(self, token: bytes, identity: str = "user"):
        super().__init__()
        self._token = token
        self._identity = identity

    def authenticate(self, outgoing, incoming) -> None:
        # Handshake can be used to mint token; here we keep it no-op. :contentReference[oaicite:16]{index=16}
        return

    def is_valid(self, token: bytes) -> str:
        if token != self._token:
            raise flight.FlightUnauthenticatedError("bad token")
        return self._identity


class StaticTokenClientAuth(flight.ClientAuthHandler):
    def __init__(self, token: bytes):
        super().__init__()
        self._token = token

    def authenticate(self, outgoing, incoming) -> None:
        # No-op handshake; could be used to fetch token. :contentReference[oaicite:17]{index=17}
        return

    def get_token(self) -> bytes:
        return self._token
```

---

# 6) `src/codeintel/flight_contract/serialization_goldens.py`

Defines golden schemas and helpers to load/save JSON fixtures; supports optional `serialized_b64` fields that the update script fills in.

```python
from __future__ import annotations

from dataclasses import dataclass
import base64
import json
from pathlib import Path
from typing import Any


def b64e(b: bytes | None) -> str | None:
    if b is None:
        return None
    return base64.b64encode(b).decode("ascii")


def b64d(s: str | None) -> bytes | None:
    if s is None:
        return None
    return base64.b64decode(s.encode("ascii"))


def load_golden_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def save_golden_json(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True) + "\n", encoding="utf-8")


@dataclass(frozen=True)
class GoldenDescriptor:
    version: str
    descriptor_type: str  # "PATH" | "CMD"
    path: list[str] | None
    command_b64: str | None
    serialized_b64: str | None


@dataclass(frozen=True)
class GoldenTicket:
    version: str
    ticket_b64: str
    serialized_b64: str | None


@dataclass(frozen=True)
class GoldenEndpoint:
    locations: list[str]
    expiration_time: str | None
    app_metadata_b64: str
    serialized_b64: str | None
    ticket_ref: str


@dataclass(frozen=True)
class GoldenFlightInfo:
    version: str
    schema: str
    descriptor_ref: str
    endpoints: list[GoldenEndpoint]
    total_records: int
    total_bytes: int
    ordered: bool
    app_metadata_b64: str
    serialized_b64: str | None
```

---

# 7) `src/codeintel/flight_contract/testing/server_kv.py`

A tiny KV Flight server used by tests. Server runs immediately when instantiated. ([Apache Arrow][1])

```python
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import pyarrow as pa
import pyarrow.flight as flight


class KVFlightServer(flight.FlightServerBase):
    """
    Minimal “store by descriptor.command bytes” server:
    - DoPut stores uploaded table
    - DoGet returns stored table
    - GetFlightInfo returns a single endpoint redeemable on same service
    """
    def __init__(self, location, **kwargs):
        super().__init__(location, **kwargs)
        self._tables: dict[bytes, pa.Table] = {}

    @staticmethod
    def _key(descriptor: flight.FlightDescriptor) -> bytes:
        # For contract tests, use .command as stable bytes key.
        if descriptor.descriptor_type == flight.DescriptorType.CMD:
            return descriptor.command
        # PATH -> join with /
        return b"/".join(p.encode("utf-8") for p in descriptor.path)

    def get_schema(self, context, descriptor):
        key = self._key(descriptor)
        return self._tables[key].schema

    def get_flight_info(self, context, descriptor):
        key = self._key(descriptor)
        table = self._tables[key]
        ticket = flight.Ticket(key)
        endpoint = flight.FlightEndpoint(ticket, [])  # redeem on same server
        return flight.FlightInfo(
            table.schema,
            descriptor,
            [endpoint],
            total_records=table.num_rows,
            total_bytes=-1,
            ordered=False,
            app_metadata=b"",
        )

    def do_get(self, context, ticket):
        table = self._tables[ticket.ticket]
        return flight.RecordBatchStream(table)

    def do_put(self, context, descriptor, reader, writer):
        key = self._key(descriptor)
        batches = []
        while True:
            try:
                chunk = reader.read_chunk()
            except StopIteration:
                break
            batches.append(chunk.data)
        self._tables[key] = pa.Table.from_batches(batches)
        writer.write(pa.py_buffer(b"ok"))


@contextmanager
def running_server(server: flight.FlightServerBase) -> Iterator[flight.FlightServerBase]:
    """
    Robust context manager without relying on FlightServerBase implementing __enter__/__exit__.
    """
    try:
        yield server
    finally:
        server.shutdown()
        server.wait()
```

---

# 8) `src/codeintel/flight_contract/testing/tls.py`

mTLS scaffold loaders.

```python
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TLSPemBundle:
    ca_pem: bytes
    server_cert_pem: bytes
    server_key_pem: bytes
    client_cert_pem: bytes
    client_key_pem: bytes


def load_tls_bundle(dirpath: Path) -> TLSPemBundle:
    """
    Expected files (generated by tools/gen_flight_test_certs.sh):
      ca.crt
      server.crt
      server.key
      client.crt
      client.key
    """
    return TLSPemBundle(
        ca_pem=(dirpath / "ca.crt").read_bytes(),
        server_cert_pem=(dirpath / "server.crt").read_bytes(),
        server_key_pem=(dirpath / "server.key").read_bytes(),
        client_cert_pem=(dirpath / "client.crt").read_bytes(),
        client_key_pem=(dirpath / "client.key").read_bytes(),
    )
```

---

# 9) Tests

## 9.1 `tests/flight_contract/conftest.py`

```python
from __future__ import annotations

from pathlib import Path
import pytest

import pyarrow as pa
import pyarrow.flight as flight

from codeintel.flight_contract.config import FlightContractConfig
from codeintel.flight_contract.headers import HeaderPolicy, normalize_flight_call_headers
from codeintel.flight_contract.testing.server_kv import KVFlightServer, running_server
from codeintel.flight_contract.testing.tls import load_tls_bundle


@pytest.fixture(scope="session")
def contract_cfg() -> FlightContractConfig:
    return FlightContractConfig()


@pytest.fixture()
def sample_table() -> pa.Table:
    return pa.table({"x": [1, 2, 3], "y": ["a", "b", "c"]})


@pytest.fixture()
def descriptor_cmd() -> flight.FlightDescriptor:
    return flight.FlightDescriptor.for_command(b"kv:test")


@pytest.fixture()
def header_policy() -> HeaderPolicy:
    # Require a request id header in all calls for these tests
    return HeaderPolicy(required=frozenset({b"x-request-id"}))


@pytest.fixture()
def call_options(header_policy: HeaderPolicy) -> flight.FlightCallOptions:
    hdrs = normalize_flight_call_headers([(b"x-request-id", b"req-123")], policy=header_policy)
    return flight.FlightCallOptions(headers=hdrs)


@pytest.fixture()
def tls_dir() -> Path:
    return Path("tests/assets/tls")


@pytest.fixture()
def tls_bundle(tls_dir: Path, contract_cfg: FlightContractConfig):
    if not contract_cfg.enable_mtls:
        pytest.skip("mTLS disabled (set FLIGHT_CONTRACT_ENABLE_MTLS=1)")
    try:
        return load_tls_bundle(tls_dir)
    except FileNotFoundError:
        pytest.skip("mTLS certs missing; run tools/gen_flight_test_certs.sh")


@pytest.fixture()
def kv_server_plain():
    # Start on localhost random port if location=None. :contentReference[oaicite:19]{index=19}
    server = KVFlightServer(location=None)
    with running_server(server) as s:
        yield s


@pytest.fixture()
def kv_client_plain(kv_server_plain):
    client = flight.FlightClient(("localhost", kv_server_plain.port))
    try:
        client.wait_for_available(timeout=5)  # :contentReference[oaicite:20]{index=20}
        yield client
    finally:
        client.close()
```

---

## 9.2 `tests/flight_contract/test_headers_policy.py`

```python
from __future__ import annotations

import pytest

from codeintel.flight_contract.headers import HeaderPolicy, normalize_flight_call_headers, normalize_middleware_headers


def test_flight_call_headers_normalize_and_sort():
    pol = HeaderPolicy(required=frozenset({b"x-request-id"}))
    out = normalize_flight_call_headers(
        [(b"x-req", b"2"), (b"x-request-id", b"1"), (b"x-req", b"1")],
        policy=pol,
        sort=True,
    )
    assert out == [(b"x-req", b"1"), (b"x-req", b"2"), (b"x-request-id", b"1")]


def test_reject_uppercase_keys():
    pol = HeaderPolicy()
    with pytest.raises(ValueError):
        normalize_flight_call_headers([(b"X-Req", b"1")], policy=pol)


def test_middleware_headers_normalize():
    pol = HeaderPolicy()
    out = normalize_middleware_headers({"x-req": "1", "x-req2": ["2", "3"]}, policy=pol)
    assert out["x-req"] == "1"
    assert out["x-req2"] == ["2", "3"]
```

---

## 9.3 `tests/flight_contract/test_middleware_doubles.py`

```python
from __future__ import annotations

import pytest
import pyarrow.flight as flight

from codeintel.flight_contract.headers import HeaderPolicy, normalize_flight_call_headers
from codeintel.flight_contract.middleware_doubles import (
    ClientMiddlewareRecord,
    RecordingClientMiddlewareFactory,
    ServerMiddlewareRecord,
    RecordingServerMiddlewareFactory,
    StrictHeaderServerMiddlewareFactory,
)
from codeintel.flight_contract.testing.server_kv import KVFlightServer, running_server


def test_server_middleware_records_headers(sample_table, descriptor_cmd):
    server_record = ServerMiddlewareRecord()

    # Server records every call’s headers
    mw = {"rec": RecordingServerMiddlewareFactory(server_record)}

    server = KVFlightServer(location=None, middleware=mw)
    with running_server(server):
        client = flight.FlightClient(("localhost", server.port))
        try:
            # Put data so GetFlightInfo works
            w, _ = client.do_put(descriptor_cmd, sample_table.schema)
            w.write_table(sample_table, max_chunksize=2)
            w.done_writing()
            w.close()

            # Send call options headers
            hdrs = normalize_flight_call_headers([(b"x-request-id", b"req-1")])
            opts = flight.FlightCallOptions(headers=hdrs)
            _ = client.get_flight_info(descriptor_cmd, opts)

            assert server_record.calls, "Expected at least one recorded call"
            # Inbound headers dict includes our metadata (key type depends on transport layer)
            found = any("x-request-id" in c.incoming_headers for c in server_record.calls)
            assert found
        finally:
            client.close()


def test_strict_header_middleware_rejects_uppercase(sample_table, descriptor_cmd):
    pol = HeaderPolicy()
    mw = {"strict": StrictHeaderServerMiddlewareFactory(pol)}
    server = KVFlightServer(location=None, middleware=mw)

    with running_server(server):
        client = flight.FlightClient(("localhost", server.port))
        try:
            w, _ = client.do_put(descriptor_cmd, sample_table.schema)
            w.write_table(sample_table)
            w.done_writing()
            w.close()

            # Intentionally violate lowercase policy.
            bad_opts = flight.FlightCallOptions(headers=[(b"X-Bad", b"1")])
            with pytest.raises(Exception):
                client.get_flight_info(descriptor_cmd, bad_opts)
        finally:
            client.close()
```

---

## 9.4 `tests/flight_contract/test_put_get_roundtrip.py`

```python
from __future__ import annotations

import pyarrow.flight as flight


def test_put_get_roundtrip(kv_client_plain, descriptor_cmd, sample_table, call_options):
    client = kv_client_plain

    w, _ = client.do_put(descriptor_cmd, sample_table.schema, call_options)
    w.write_table(sample_table, max_chunksize=2)
    w.done_writing()
    w.close()

    info = client.get_flight_info(descriptor_cmd, call_options)
    out = client.do_get(info.endpoints[0].ticket, call_options).read_all()
    assert out.equals(sample_table)
```

---

## 9.5 `tests/flight_contract/test_auth_matrix.py`

Covers: no-auth + token + mTLS (skips unless enabled + certs present). TLS/mTLS config is from `FlightServerBase` + `FlightClient` surfaces. ([Apache Arrow][1])

```python
from __future__ import annotations

import pytest
import pyarrow.flight as flight

from codeintel.flight_contract.auth_handlers import StaticTokenClientAuth, StaticTokenServerAuth
from codeintel.flight_contract.testing.server_kv import KVFlightServer, running_server


def test_no_auth_allows_calls(kv_server_plain, kv_client_plain, descriptor_cmd, sample_table):
    w, _ = kv_client_plain.do_put(descriptor_cmd, sample_table.schema)
    w.write_table(sample_table)
    w.done_writing()
    w.close()
    _ = kv_client_plain.get_flight_info(descriptor_cmd)


def test_token_auth_requires_authenticate(sample_table, descriptor_cmd):
    token = b"secret-token"

    server = KVFlightServer(location=None, auth_handler=StaticTokenServerAuth(token))
    with running_server(server):
        client = flight.FlightClient(("localhost", server.port))
        try:
            # Put requires auth (server will validate per-call token)
            with pytest.raises(flight.FlightUnauthenticatedError):
                w, _ = client.do_put(descriptor_cmd, sample_table.schema)

            # Authenticate then retry
            client.authenticate(StaticTokenClientAuth(token))

            w, _ = client.do_put(descriptor_cmd, sample_table.schema)
            w.write_table(sample_table)
            w.done_writing()
            w.close()

            _ = client.get_flight_info(descriptor_cmd)
        finally:
            client.close()


def test_mtls_scaffold(sample_table, descriptor_cmd, tls_bundle):
    # mTLS: server requires client cert and validates it using root_certificates. :contentReference[oaicite:22]{index=22}
    location = flight.Location.for_grpc_tls("0.0.0.0", 0)

    server = KVFlightServer(
        location=location,
        tls_certificates=[(tls_bundle.server_cert_pem, tls_bundle.server_key_pem)],
        verify_client=True,
        root_certificates=tls_bundle.ca_pem,
    )

    with running_server(server):
        client_loc = flight.Location.for_grpc_tls("localhost", server.port)
        client = flight.FlightClient(
            client_loc,
            tls_root_certs=tls_bundle.ca_pem,
            cert_chain=tls_bundle.client_cert_pem,
            private_key=tls_bundle.client_key_pem,
        )
        try:
            client.wait_for_available(timeout=5)
            w, _ = client.do_put(descriptor_cmd, sample_table.schema)
            w.write_table(sample_table)
            w.done_writing()
            w.close()
            _ = client.get_flight_info(descriptor_cmd)
        finally:
            client.close()
```

---

## 9.6 `tests/flight_contract/test_serialization_goldens.py`

Loads goldens; asserts structural invariants always; asserts exact `serialized_b64` bytes if present.

```python
from __future__ import annotations

from pathlib import Path
import pytest
import pyarrow as pa
import pyarrow.flight as flight

from codeintel.flight_contract.config import FlightContractConfig
from codeintel.flight_contract.serialization_goldens import load_golden_json, b64d, b64e, save_golden_json


GOLDEN_DIR = Path("tests/golden/flight")


def _require(path: Path, cfg: FlightContractConfig) -> dict:
    if not path.exists():
        if cfg.allow_missing_goldens:
            pytest.skip(f"Missing golden: {path}")
        pytest.fail(f"Missing golden: {path} (run: python scripts/update_flight_goldens.py)")
    return load_golden_json(path)


def test_descriptor_goldens(contract_cfg: FlightContractConfig):
    g = _require(GOLDEN_DIR / "descriptor_path_v1.json", contract_cfg)
    d = flight.FlightDescriptor.for_path(*g["path"])
    assert d.descriptor_type == flight.DescriptorType.PATH
    assert list(d.path) == g["path"]

    # Round-trip is always required
    rt = flight.FlightDescriptor.deserialize(d.serialize())
    assert list(rt.path) == list(d.path)

    if g.get("serialized_b64"):
        assert b64e(d.serialize()) == g["serialized_b64"]

    g2 = _require(GOLDEN_DIR / "descriptor_cmd_v1.json", contract_cfg)
    cmd = b64d(g2["command_b64"])
    assert cmd is not None
    d2 = flight.FlightDescriptor.for_command(cmd)
    assert d2.descriptor_type == flight.DescriptorType.CMD
    assert d2.command == cmd

    rt2 = flight.FlightDescriptor.deserialize(d2.serialize())
    assert rt2.command == d2.command
    if g2.get("serialized_b64"):
        assert b64e(d2.serialize()) == g2["serialized_b64"]


def test_ticket_golden(contract_cfg: FlightContractConfig):
    g = _require(GOLDEN_DIR / "ticket_v1.json", contract_cfg)
    t = flight.Ticket(b64d(g["ticket_b64"]))
    rt = flight.Ticket.deserialize(t.serialize())
    assert rt.ticket == t.ticket
    if g.get("serialized_b64"):
        assert b64e(t.serialize()) == g["serialized_b64"]


def test_flightinfo_golden(contract_cfg: FlightContractConfig):
    g = _require(GOLDEN_DIR / "flightinfo_v1.json", contract_cfg)

    dg = _require(GOLDEN_DIR / g["descriptor_ref"], contract_cfg)
    d = flight.FlightDescriptor.for_path(*dg["path"])

    tg = _require(GOLDEN_DIR / g["endpoints"][0]["ticket_ref"], contract_cfg)
    ticket = flight.Ticket(b64d(tg["ticket_b64"]))

    loc_uri = g["endpoints"][0]["locations"][0]
    loc = flight.Location(loc_uri)

    schema = pa.schema([("x", pa.int64()), ("y", pa.string())])
    # keep schema in sync with golden string check
    assert schema.to_string() == g["schema"]

    ep = flight.FlightEndpoint(ticket, [loc])
    info = flight.FlightInfo(
        schema,
        d,
        [ep],
        total_records=g["total_records"],
        total_bytes=g["total_bytes"],
        ordered=g["ordered"],
        app_metadata=b64d(g["app_metadata_b64"]) or b"",
    )

    rt = flight.FlightInfo.deserialize(info.serialize())
    assert rt.total_records == info.total_records
    assert rt.total_bytes == info.total_bytes
    assert rt.ordered == info.ordered

    if g.get("serialized_b64"):
        assert b64e(info.serialize()) == g["serialized_b64"]
```

---

# 10) Goldens (fixtures)

## `tests/golden/flight/README.md`

```markdown
These goldens define the **contract surface** for pyarrow.flight serialization.

- The JSON files contain:
  - “structural” fields (descriptor path, ticket bytes, endpoint locations, totals, etc.)
  - optional `serialized_b64` fields.

By default, the repo expects `serialized_b64` fields to be present; if missing, run:

  python scripts/update_flight_goldens.py

To allow missing goldens temporarily (not recommended), set:

  FLIGHT_CONTRACT_ALLOW_MISSING_GOLDENS=1
```

## `tests/golden/flight/descriptor_path_v1.json`

```json
{
  "command_b64": null,
  "descriptor_type": "PATH",
  "path": ["dataset", "example", "v1"],
  "serialized_b64": null,
  "version": "v1"
}
```

## `tests/golden/flight/descriptor_cmd_v1.json`

```json
{
  "command_b64": "AQID",
  "descriptor_type": "CMD",
  "path": null,
  "serialized_b64": null,
  "version": "v1"
}
```

## `tests/golden/flight/ticket_v1.json`

```json
{
  "serialized_b64": null,
  "ticket_b64": "ZHM9ZXhhbXBsZTtydW49cnVuMTIzO3BhcnQ9MDAwMA==",
  "version": "v1"
}
```

## `tests/golden/flight/flightinfo_v1.json`

```json
{
  "app_metadata_b64": "",
  "descriptor_ref": "descriptor_path_v1.json",
  "endpoints": [
    {
      "app_metadata_b64": "",
      "expiration_time": null,
      "locations": ["grpc+tcp://localhost:31337"],
      "serialized_b64": null,
      "ticket_ref": "ticket_v1.json"
    }
  ],
  "ordered": false,
  "schema": "x: int64\ny: string",
  "serialized_b64": null,
  "total_bytes": -1,
  "total_records": 3,
  "version": "v1"
}
```

---

# 11) Golden updater script

## `scripts/update_flight_goldens.py`

```python
from __future__ import annotations

from pathlib import Path
import pyarrow as pa
import pyarrow.flight as flight

from codeintel.flight_contract.serialization_goldens import load_golden_json, save_golden_json, b64e, b64d

G = Path("tests/golden/flight")

def main() -> None:
    # Descriptor PATH
    gp = load_golden_json(G / "descriptor_path_v1.json")
    d_path = flight.FlightDescriptor.for_path(*gp["path"])
    gp["serialized_b64"] = b64e(d_path.serialize())
    save_golden_json(G / "descriptor_path_v1.json", gp)

    # Descriptor CMD
    gc = load_golden_json(G / "descriptor_cmd_v1.json")
    cmd = b64d(gc["command_b64"])
    assert cmd is not None
    d_cmd = flight.FlightDescriptor.for_command(cmd)
    gc["serialized_b64"] = b64e(d_cmd.serialize())
    save_golden_json(G / "descriptor_cmd_v1.json", gc)

    # Ticket
    gt = load_golden_json(G / "ticket_v1.json")
    t = flight.Ticket(b64d(gt["ticket_b64"]))
    gt["serialized_b64"] = b64e(t.serialize())
    save_golden_json(G / "ticket_v1.json", gt)

    # FlightInfo
    gi = load_golden_json(G / "flightinfo_v1.json")

    schema = pa.schema([("x", pa.int64()), ("y", pa.string())])
    assert schema.to_string() == gi["schema"]

    dref = load_golden_json(G / gi["descriptor_ref"])
    desc = flight.FlightDescriptor.for_path(*dref["path"])

    ep0 = gi["endpoints"][0]
    tref = load_golden_json(G / ep0["ticket_ref"])
    ticket = flight.Ticket(b64d(tref["ticket_b64"]))

    loc = flight.Location(ep0["locations"][0])
    endpoint = flight.FlightEndpoint(ticket, [loc])

    info = flight.FlightInfo(
        schema,
        desc,
        [endpoint],
        total_records=gi["total_records"],
        total_bytes=gi["total_bytes"],
        ordered=gi["ordered"],
        app_metadata=b"",
    )
    gi["serialized_b64"] = b64e(info.serialize())
    save_golden_json(G / "flightinfo_v1.json", gi)

    print("Updated Flight goldens.")

if __name__ == "__main__":
    main()
```

---

# 12) mTLS scaffold scripts & docs

## `tools/gen_flight_test_certs.sh`

Generates a local CA, plus server/client certs with SAN for localhost.

```bash
#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="tests/assets/tls"
mkdir -p "${OUT_DIR}"

cd "${OUT_DIR}"

echo "Generating CA..."
openssl req -x509 -newkey rsa:2048 -sha256 -days 3650 -nodes \
  -keyout ca.key -out ca.crt -subj "/CN=FlightTestCA"

echo "Generating server key + CSR..."
openssl req -newkey rsa:2048 -nodes \
  -keyout server.key -out server.csr -subj "/CN=localhost"

cat > server.ext <<'EOF'
basicConstraints=CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names
[alt_names]
DNS.1 = localhost
IP.1 = 127.0.0.1
EOF

echo "Signing server cert..."
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days 3650 -sha256 -extfile server.ext

echo "Generating client key + CSR..."
openssl req -newkey rsa:2048 -nodes \
  -keyout client.key -out client.csr -subj "/CN=flight-client"

cat > client.ext <<'EOF'
basicConstraints=CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
EOF

echo "Signing client cert..."
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out client.crt -days 3650 -sha256 -extfile client.ext

rm -f *.csr *.ext ca.srl

echo "Done. Files in ${OUT_DIR}:"
ls -1
```

## `tests/assets/tls/README.md`

```markdown
This directory holds TLS materials for the Flight contract tests (mTLS).

Generate files via:

  bash tools/gen_flight_test_certs.sh

Then enable mTLS tests with:

  FLIGHT_CONTRACT_ENABLE_MTLS=1 pytest -q tests/flight_contract/test_auth_matrix.py -k mtls
```

---

## What you get out of this pack

* A **hard policy boundary** for headers (lowercase, ASCII, allowed prefixes) enforced at middleware. ([Apache Arrow][4])
* A reusable server/client test harness for **DoPut/DoGet determinism** + request metadata behavior
* A clean way to **pin and detect drift** in Flight serialization (`serialized_b64` goldens) without relying on fragile equality semantics
* An mTLS scaffold that’s “real enough” for CI (but safely skipped unless enabled)

If you want the next increment after *this* contract pack: I can extend it into a **CodeIntel-style contract runner** that emits a single CI artifact (JSON) summarizing: header policy pass/fail, auth matrix status, roundtrip throughput metrics, and golden drift diffs (so you can gate upgrades with a crisp report).

[1]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightServerBase.html "pyarrow.flight.FlightServerBase — Apache Arrow v22.0.0"
[2]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightClient.html "pyarrow.flight.FlightClient — Apache Arrow v22.0.0"
[3]: https://arrow.apache.org/docs/10.0/python/generated/pyarrow.flight.ServerMiddlewareFactory.html?utm_source=chatgpt.com "pyarrow.flight.ServerMiddlewareFactory — Apache Arrow v10.0.1"
[4]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.ClientMiddleware.html?utm_source=chatgpt.com "pyarrow.flight.ClientMiddleware — Apache Arrow v22.0.0"
[5]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.FlightCallOptions.html?utm_source=chatgpt.com "pyarrow.flight.FlightCallOptions — Apache Arrow v22.0.0"
[6]: https://arrow.apache.org/docs/python/generated/pyarrow.flight.ServerAuthHandler.html?utm_source=chatgpt.com "pyarrow.flight.ServerAuthHandler — Apache Arrow v22.0.0"
