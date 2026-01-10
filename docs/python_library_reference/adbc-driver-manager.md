# `adbc-driver-manager` / `adbc_driver_manager` (Python) — comprehensive feature catalog

**Mental model:** `adbc_driver_manager` is the **ADBC “driver manager” binding** for Python: it can **dynamically load** database drivers (shared libraries) and exposes (1) a **low-level, C-API-shaped** interface and (2) a **DB-API 2.0 (PEP 249)** wrapper (with Arrow/pandas/polars extensions when available). ([Apache Arrow][1])

---

## A) Packaging, install surface, and “what you actually import”

* **PyPI package name:** `adbc-driver-manager`
  **Import name:** `adbc_driver_manager` ([PyPI][2])
* **Install:**

  * `pip install adbc-driver-manager` (package provides bindings + DBAPI layer). ([PyPI][2])
* **Python version floor:** current PyPI release indicates **Python ≥ 3.10**. ([PyPI][2])
* **Extras:** `dbapi`, `test` (PyPI “Provides-Extra”). ([PyPI][2])
* **DBAPI dependency:** the DBAPI layer “requires PyArrow” for full functionality; without PyArrow, “data read/write” APIs are missing (you can still execute and receive a PyCapsule/Arrow handle, but convenience methods will raise). ([Apache Arrow][3])

---

## B) Driver loading & discovery (shared libraries + manifests)

### B1) Two driver-loading modes

1. **Direct library path** (e.g., `/path/to/libadbc_driver.so`) passed as the `driver` option. ([Apache Arrow][4])
2. **Driver manifest name** (TOML) that resolves to a driver shared library (more portable, supports metadata + platform-specific paths). ([Apache Arrow][4])

### B2) Manifest structure + entrypoints

* Manifest is a **TOML** file; minimally must specify `Driver.shared` (single path or per-platform mapping). ([Apache Arrow][4])
* Manifest can specify **`Driver.entrypoint`** (non-default initialization symbol). ([Apache Arrow][4])
* In Python, `entrypoint=` is also a first-class parameter on `dbapi.connect(...)`. ([Apache Arrow][3])

### B3) Search paths & “load_flags”

* Manifest/libraries can be discovered through ordered search paths, gated by `load_flags` (bitmask). In Python, pass `load_flags` as an option to `AdbcDatabase` or via `db_kwargs` to `dbapi.connect`. ([Apache Arrow][4])
* Key search behavior (Unix-like):

  * `ADBC_DRIVER_PATH` (colon-separated) when `LOAD_FLAG_SEARCH_ENV` is enabled
  * auto-added venv path: `$VIRTUAL_ENV/etc/adbc/drivers`
  * conda path: `$CONDA_PREFIX/etc/adbc/drivers` (when installed/built with conda + env-search enabled)
  * user config dir (when `LOAD_FLAG_SEARCH_USER`): e.g. `~/.config/adbc/drivers` (or `$XDG_CONFIG_HOME/adbc/drivers`)
  * system config dir (when `LOAD_FLAG_SEARCH_SYSTEM`) ([Apache Arrow][4])

### B4) DBAPI connect “driver” resolution rules (very practical)

`adbc_driver_manager.dbapi.connect(driver=...)` accepts:

* a **driver/manifest name** (e.g. `adbc_driver_sqlite` → tries `adbc_driver_sqlite.toml`, then OS-specific library name),
* a **path to a shared library**, or
* a **URI-like string** containing `://` (scheme treated as driver name; URI passed through unchanged—only works if driver expects such URIs). ([Apache Arrow][3])

---

## C) Low-level API (C API–shaped): handles, options, Arrow C Data interface

**Core promise:** “fairly direct 1:1 mapping” to the ADBC C API. ([Apache Arrow][3])

### C1) Constants & enums (standardized knobs)

* **Status codes:** `AdbcStatusCode` (e.g., `OK`, `NOT_IMPLEMENTED`, `INVALID_ARGUMENT`, `INTEGRITY`, `IO`, `TIMEOUT`, `UNAUTHENTICATED`, …). ([Apache Arrow][3])
* **Object discovery depth:** `GetObjectsDepth` (`ALL`, `CATALOGS`, `DB_SCHEMAS`, `TABLES`, `COLUMNS`). ([Apache Arrow][3])
* **Standard option name enums:**

  * `DatabaseOptions`: `URI`, `USERNAME`, `PASSWORD` ([Apache Arrow][3])
  * `ConnectionOptions`: `CURRENT_CATALOG`, `CURRENT_DB_SCHEMA`, `ISOLATION_LEVEL` ([Apache Arrow][3])
  * `StatementOptions`: `BIND_BY_NAME`, `INCREMENTAL`, `INGEST_*`, `PROGRESS` (several ingest options are explicitly marked experimental). ([Apache Arrow][3])
* **Legacy ingest constants:** `INGEST_OPTION_MODE(_APPEND/_CREATE)`, `INGEST_OPTION_TARGET_TABLE`. ([Apache Arrow][3])

### C2) Handle classes (context-manageable resources)

All ADBC handles are context managers via the `_AdbcHandle` base class. ([Apache Arrow][3])

#### `AdbcDatabase(**kwargs)`

* Must include at least `driver` (to identify what to load).
* Options surface: `set_options(**kwargs)` + typed getters `get_option*`. ([Apache Arrow][3])

#### `AdbcConnection(database: AdbcDatabase, **kwargs)`

* **Not thread-safe**; clients must serialize access. ([Apache Arrow][3])
* Transaction control: `commit()`, `rollback()`, plus `set_autocommit(enabled)`. ([Apache Arrow][3])
* Metadata/catalog: `get_info(info_codes=None)`, `get_objects(depth, …)`, `get_table_schema(catalog, db_schema, table_name)`, `get_table_types()`. ([Apache Arrow][3])
* Distributed results: `read_partition(partition)` for reading partitions returned by statement execution. ([Apache Arrow][3])
* Options: `set_options(**kwargs)` + typed getters `get_option*`. ([Apache Arrow][3])
* Cancellation: `cancel()`. ([Apache Arrow][3])

#### `AdbcStatement(connection: AdbcConnection)`

* **Not thread-safe**; serialize access. ([Apache Arrow][3])
* Query definition:

  * `set_sql_query(query)`
  * `set_substrait_plan(plan)` (execute Substrait plans, not just SQL) ([Apache Arrow][3])
* Parameter binding:

  * `bind(data, schema=None)` (ArrowArray + optional schema)
  * `bind_stream(stream)` (ArrowArrayStream) ([Apache Arrow][3])
* Prepared statements + parameter introspection:

  * `prepare()`
  * `get_parameter_schema()` (schema of parameters; call after prepare; may raise `NotSupportedError`). ([Apache Arrow][3])
* Execution:

  * `execute_query() -> (ArrowArrayStreamHandle, rowcount)`
  * `execute_update() -> affected_rows`
  * `execute_schema() -> ArrowSchemaHandle`
  * `execute_partitions() -> (list[bytes] partitions, ArrowSchemaHandle|None, rowcount)`; schema may be `None` if incremental execution is enabled and server doesn’t return a schema. ([Apache Arrow][3])
* Statement-local options: `set_options(**kwargs)` + typed getters `get_option*` (includes `StatementOptions.PROGRESS`, `BIND_BY_NAME`, `INCREMENTAL`, ingest knobs). ([Apache Arrow][3])
* Cancellation: `cancel()`; cleanup: `close()`. ([Apache Arrow][3])

### C3) Arrow C Data “handle wrappers” (PyCapsule interface)

These are for advanced/interop use without importing PyArrow in the low-level layer:

* `ArrowArrayHandle`, `ArrowArrayStreamHandle`, `ArrowSchemaHandle`

  * properties: `address`, `is_valid`
  * method: `release()` (idempotent; invalidates handle) ([Apache Arrow][3])

---

## D) DBAPI 2.0 layer (`adbc_driver_manager.dbapi`) + ADBC/Arrow extensions

### D1) DBAPI module constants/types

* `apilevel = "2.0"`, `paramstyle = "qmark"` (hardcoded but “actually depends on the driver”), `threadsafety = 1` (“connections may not be shared”). ([Apache Arrow][3])
* DBAPI datetime constructors: `Date`, `Time`, `Timestamp`, plus `DateFromTicks`, `TimeFromTicks`, `TimestampFromTicks`. ([Apache Arrow][3])
* DBAPI “type objects” (`BINARY`, `DATETIME`, `NUMBER`, `ROWID`, `STRING`) are present but *invalid* without PyArrow installed. ([Apache Arrow][3])

### D2) `dbapi.connect(...)` (the universal entrypoint)

Signature includes:

* `driver`, optional `uri`, `entrypoint`, `db_kwargs`, `conn_kwargs`, and `autocommit` (disabled by default for DBAPI compliance; warns if can’t be disabled). ([Apache Arrow][3])

### D3) `dbapi.Connection` (PEP 249 + ADBC extensions)

**Standard-ish DBAPI:**

* `cursor(adbc_stmt_kwargs=...)`, `commit()`, `rollback()`, `close()` (close warns about leaking resources if not closed; closes associated cursors). ([Apache Arrow][3])

**Convenience/extension methods (not DBAPI standard):**

* `execute(operation, parameters=None, *, adbc_stmt_kwargs=None)` convenience: creates a new cursor and runs. ([Apache Arrow][3])
* `adbc_cancel()`, `adbc_clone()` (clone shares underlying database). ([Apache Arrow][3])
* Metadata helpers:

  * `adbc_get_info() -> dict`
  * `adbc_get_objects(depth=..., *_filters) -> RecordBatchReader`
  * `adbc_get_table_schema(table_name, catalog_filter=None, db_schema_filter=None) -> pyarrow.Schema`
  * `adbc_get_table_types() -> list[str]` ([Apache Arrow][3])
* “Escape hatch” properties:

  * `adbc_connection` (underlying `AdbcConnection`)
  * `adbc_database` (underlying `AdbcDatabase`)
  * `adbc_current_catalog`, `adbc_current_db_schema` ([Apache Arrow][3])

### D4) `dbapi.Cursor` (PEP 249 + Arrow/pandas/polars + distributed results + ingestion)

**Core DBAPI attributes:**

* `arraysize`, `description`, `rowcount`, `rownumber`, and `connection`. ([Apache Arrow][3])
* Underlying statement access: `adbc_statement`. ([Apache Arrow][3])

**Standard DBAPI-ish methods:**

* `execute(operation, parameters=None)`, `executemany(operation, seq_of_parameters)`, `fetchone`, `fetchmany`, `fetchall`, iterator `next()` / `StopIteration`. ([Apache Arrow][3])
* `callproc` and `nextset` exist but are **not supported**; `setinputsizes`/`setoutputsize` are no-ops. ([Apache Arrow][3])

**Operation/parameter semantics (important):**

* `operation` may be **SQL as str** or a **serialized Substrait plan as bytes**.
* `seq_of_parameters` may be:

  * Python sequences (row-wise parameter sets), **or**
  * Arrow data (`pyarrow.RecordBatch/Table/RecordBatchReader`), etc.
* `parameters=None` is allowed (outside DBAPI spec); empty parameter sequence implies “execute zero times”. ([Apache Arrow][3])

**Arrow-first fetch extensions (not DBAPI standard):**

* `fetch_arrow() -> ArrowArrayStreamHandle` (PyCapsule interface; **can only be called once** and must be called before other consumption methods like `fetchone`, `fetch_arrow_table`, etc.). ([Apache Arrow][3])
* `fetch_arrow_table() -> pyarrow.Table` (DuckDB-like API). ([Apache Arrow][3])
* `fetch_record_batch() -> pyarrow.RecordBatchReader` (DuckDB-like API). ([Apache Arrow][3])
* `fetchallarrow() -> pyarrow.Table` (turbodbc-like API). ([Apache Arrow][3])
* DataFrame fetch extensions:

  * `fetch_df() -> pandas.DataFrame`
  * `fetch_polars() -> polars.DataFrame` ([Apache Arrow][3])

**Prepare/parameter schema extensions (key “power feature”):**

* `adbc_prepare(operation)` prepares without executing and can return parameter schema (recipe emphasizes using this to introspect parameter schema + avoid re-prepare). ([Apache Arrow][5])

**Bulk ingestion (Arrow → table, not DBAPI standard):**

* `adbc_ingest(table_name, data, mode="create"|"append"|"replace"|"create_append", *, catalog_name=None, db_schema_name=None, temporary=False) -> int`

  * accepts `pyarrow.RecordBatch`, `pyarrow.Table`, `pyarrow.RecordBatchReader`, **or any Arrow PyCapsule-compatible object** (`__arrow_c_array__` / `__arrow_c_stream__`)
  * explicitly described as avoiding per-row overhead of prepare/bind/insert loops. ([Apache Arrow][3])

**Distributed results / partitioned execution extensions:**

* `adbc_execute_partitions(...) -> (list[bytes] partitions, pyarrow.Schema|None)` (schema may be `None` if incremental execution enabled and server didn’t return schema). ([Apache Arrow][3])
* `adbc_read_partition(partition)` reads one partition. ([Apache Arrow][3])
* `adbc_execute_schema(...) -> pyarrow.Schema` (schema without execution). ([Apache Arrow][3])

**Statement-level cancellation:**

* `adbc_cancel()` cancels ongoing operations on the statement. ([Apache Arrow][3])

---

## E) Error model, status codes, and exception taxonomy

### E1) Status codes (driver-level)

* `AdbcStatusCode` enumerates canonical error categories (e.g., `INVALID_DATA`, `INTEGRITY`, `NOT_FOUND`, `UNAUTHORIZED`, …). ([Apache Arrow][3])

### E2) DBAPI exception hierarchy (+ rich metadata)

* Base DBAPI exception: `Error`, with structured attributes:

  * `status_code` (original ADBC status), optional `vendor_code`, optional `sqlstate`, optional `details` list. ([Apache Arrow][3])
* DBAPI-aligned subclasses: `DatabaseError`, `DataError`, `IntegrityError`, `InterfaceError`, `InternalError`, `NotSupportedError`, `OperationalError`, `ProgrammingError`, plus `Warning`. ([Apache Arrow][3])

---

## F) Resource lifecycle, safety, and concurrency constraints

* **Low-level handles are context managers** (`_AdbcHandle`). ([Apache Arrow][3])
* **DBAPI Connection/Cursor MUST be closed** or driver resources may leak; `__del__` exists but timing is not guaranteed. In dev, `__del__` raises a `ResourceWarning` under pytest or when `_ADBC_DRIVER_MANAGER_WARN_UNCLOSED_RESOURCE=1`. ([Apache Arrow][3])
* **Thread safety:**

  * Low-level `AdbcConnection` and `AdbcStatement` are explicitly “not thread-safe; serialize access.” ([Apache Arrow][3])
  * DBAPI declares `threadsafety = 1` (connections may not be shared). ([Apache Arrow][3])

---

## G) Cookbook/recipes: “intended” usage patterns you should treat as supported

* **Direct low-level use:** create `AdbcDatabase` → `AdbcConnection` → `AdbcStatement`; set query; execute → receive Arrow stream handle; import with `pyarrow.RecordBatchReader.from_stream(handle)`; then close all (or use context managers). ([Apache Arrow][5])
* **Manual prepare:** DBAPI auto-prepares statements for compliance, but you can call `Cursor.adbc_prepare(...)` to fetch parameter schema and ensure repeated executions reuse the prepared statement. ([Apache Arrow][5])

---

## H) “Scope boundaries” with driver-specific packages (important for architecture)

* `adbc_driver_manager` is generic and requires separately installed driver shared libraries (or manifests). ([Apache Arrow][1])
* Driver-specific packages (e.g., `adbc_driver_postgresql`, `adbc_driver_sqlite`) typically bundle and load the driver library “invisibly” and expose `connect()` convenience APIs. ([Apache Arrow][1])

---

If you want the *next increment* (like your PyArrow doc): I can expand each section into a “deep dive page” (power knobs + failure modes + minimal snippets), **especially** for: `load_flags`/manifest resolution, partitioned execution (`execute_partitions` + incremental mode), and the Arrow-native ingestion path (`adbc_ingest` + statement ingest options). 

[1]: https://arrow.apache.org/adbc/main/python/driver_manager.html "Driver Manager - ADBC 22 (dev) documentation"
[2]: https://pypi.org/project/adbc-driver-manager/ "adbc-driver-manager · PyPI"
[3]: https://arrow.apache.org/adbc/main/python/api/adbc_driver_manager.html "adbc_driver_manager - ADBC 22 (dev) documentation"
[4]: https://arrow.apache.org/adbc/main/format/driver_manifests.html "ADBC Driver Manager and Manifests - ADBC 22 (dev) documentation"
[5]: https://arrow.apache.org/adbc/main/python/recipe/driver_manager.html "DBAPI/Driver Manager Recipes - ADBC 22 (dev) documentation"

Below is the **“deep dive pages” expansion** of the catalog, in the same spirit as your `pyarrow-advanced.md` template (mental model → full surface area → power knobs → failure modes → minimal snippets). The three “heavy” pages you called out are **B (load_flags/manifest resolution)**, **E (partitioned execution + incremental)**, and **F (Arrow-native ingestion)**.

---

## A) Packaging, import surface, and “what this package is” — deep dive

### Mental model

`adbc-driver-manager` (import: `adbc_driver_manager`) is a **generic runtime loader** for ADBC drivers plus a Python façade:

* **Low-level**: near 1:1 bindings to the ADBC C API.
* **High-level**: a **DB-API 2.0 (PEP 249)** interface (and Arrow/pandas/polars convenience methods) layered on top. ([Apache Arrow][1])

### Full surface area (public module topology)

* `import adbc_driver_manager` → low-level handles/enums (`AdbcDatabase`, `AdbcConnection`, `AdbcStatement`, `AdbcStatusCode`, option enums). ([Apache Arrow][2])
* `import adbc_driver_manager.dbapi` → PEP-249-ish `connect()`, `Connection`, `Cursor`, DB-API constants, DB-API exceptions. ([Apache Arrow][2])
* Real-world usage: you typically install **a driver package** (e.g. `adbc_driver_postgresql`) or provide **a shared library/manifest** for the driver manager to load. ([PyPI][3])

### Power knobs

* “DBAPI enabled” is a *feature gate* on having Arrow ecosystem installed: DBAPI exposes Arrow-native methods (`fetch_arrow_table`, `fetch_record_batch`, etc.) and DataFrame adapters. ([Apache Arrow][2])
* Dev-time leak detection: unclosed objects can trigger `ResourceWarning` under pytest or when `_ADBC_DRIVER_MANAGER_WARN_UNCLOSED_RESOURCE=1`. ([Apache Arrow][2])

### Failure modes

* **Driver not found**: you installed the manager but not the actual driver artifact (manifest / shared library / driver package). ([Apache Arrow][4])
* **Resource leaks**: failing to close `Connection`/`Cursor`/low-level handles can leak native resources; DBAPI explicitly warns about this. ([Apache Arrow][2])

### Minimal snippets

```python
import adbc_driver_manager.dbapi as dbapi

# driver can be a name/manifest, or a path to a .so/.dylib/.dll
with dbapi.connect(driver="adbc_driver_sqlite", uri="file::memory:") as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT 1")
        print(cur.fetchone())
```

Driver argument resolution rules (name → manifest → platform library name; URI scheme detection) are defined by `connect()`. ([Apache Arrow][2])

---

## B) Driver loading & manifest resolution (`load_flags`, search paths, reproducibility) — deep dive

### Mental model

The driver manager resolves `driver=...` into **either**:

1. a **shared library** to dlopen/LoadLibrary, **or**
2. a **TOML manifest** that points to a shared library (portable installs, metadata, safer than `LD_LIBRARY_PATH`). ([Apache Arrow][4])

### B1) Manifest structure (TOML contract)

A driver manifest is TOML with:

* Required: `Driver.shared` (either a string path or a per-platform mapping). ([Apache Arrow][4])
* Optional metadata: `name`, `version`, `publisher`, `url`, etc. ([Apache Arrow][4])
* Optional ADBC capability metadata: `[ADBC] version`, `[ADBC.features] supported/unsupported` lists. ([Apache Arrow][4])
* Optional non-default entrypoint: `Driver.entrypoint`. ([Apache Arrow][4])
* `manifest_version` defaults to 1; driver managers must error on versions > 1. ([Apache Arrow][4])
* Platform tuple keys use `<os>_<arch>` (e.g. `linux_amd64`) with an authoritative tuple list. ([Apache Arrow][4])

**Minimal manifest example (single shared library):**

```toml
manifest_version = 1
name = "My Driver"
version = "1.0.0"

[Driver]
shared = "/opt/adbc/lib/libadbc_driver_mydb.so"
# entrypoint = "AdbcDriverInit"  # only if non-default
```

Manifest requirements and examples are standardized. ([Apache Arrow][4])

### B2) Resolution algorithm (what happens when you pass `driver="foo"`)

**Key resolution rules:**

* If `driver` is a *path*, load that file directly. ([Apache Arrow][4])
* If the name has **no extension**:

  * try `<name>.toml` first; if missing, try platform library extension (`.so/.dylib/.dll`). ([Apache Arrow][4])
* **Relative paths** are rejected unless explicitly allowed (security hardening). ([Apache Arrow][4])
* If the name is a “bare” driver name (not a file path), the manager can:

  1. search for a manifest (`<name>.toml`) in configured directories, then
  2. fall back to loading the shared library via platform loader search semantics. ([Apache Arrow][4])

### B3) `load_flags` bitmask (the knobs)

The canonical load flags are numeric bitflags (C driver manager header):

* `ADBC_LOAD_FLAG_SEARCH_ENV = 1`
* `ADBC_LOAD_FLAG_SEARCH_USER = 2`
* `ADBC_LOAD_FLAG_SEARCH_SYSTEM = 4`
* `ADBC_LOAD_FLAG_ALLOW_RELATIVE_PATHS = 8`
* `ADBC_LOAD_FLAG_DEFAULT` is the OR of all of the above. ([Apache Arrow][5])

In Python, you pass the bitmask as the `load_flags` option (e.g., `db_kwargs={"load_flags": "..."}` or low-level `AdbcDatabase(..., load_flags=...)`). ([Apache Arrow][4])

**Security posture (recommended):**

* Prefer **manifests + controlled search paths** over `LD_LIBRARY_PATH`. ([Apache Arrow][4])
* Disable relative paths in production unless you explicitly need them (avoid CWD injection). ([Apache Arrow][4])

### B4) Search path order (Unix-like vs Windows)

**Unix-like platforms** (Linux/macOS): manifest search order is defined as:

1. `ADBC_DRIVER_PATH` if `SEARCH_ENV` set (colon-separated). ([Apache Arrow][4])
2. additional search paths (and Python auto-adds `$VIRTUAL_ENV/etc/adbc/drivers` in venv). ([Apache Arrow][4])
3. conda env path `$CONDA_PREFIX/etc/adbc/drivers` (if built/installed with conda and `SEARCH_ENV` set). ([Apache Arrow][4])
4. user config dir if `SEARCH_USER` set (`$XDG_CONFIG_HOME/adbc/drivers` or `~/.config/adbc/drivers`; macOS uses `~/Library/Application Support/ADBC/Drivers`). ([Apache Arrow][4])
5. system config dir if `SEARCH_SYSTEM` set (`/etc/adbc/drivers`; macOS `/Library/Application Support/ADBC/Drivers`). ([Apache Arrow][4])

**Windows**: similar, but:

* `ADBC_DRIVER_PATH` is **semicolon-separated**, and venv auto-adds `$VIRTUAL_ENV\etc\adbc\drivers`. ([Apache Arrow][4])
* user/system search includes **registry keys** under `HKEY_CURRENT_USER\SOFTWARE\ADBC\Drivers\${name}` and `HKEY_LOCAL_MACHINE\SOFTWARE\ADBC\Drivers\${name}` (with subkeys like `driver`, `entrypoint`, etc.). ([Apache Arrow][4])

### B5) Minimal snippets (manifest discovery + `load_flags`)

```python
import os
import adbc_driver_manager.dbapi as dbapi

# Put "mydb.toml" in one of these directories (or set ADBC_DRIVER_PATH explicitly):
os.environ["ADBC_DRIVER_PATH"] = "/opt/adbc/manifests"

# Search only env paths (1) + disallow relative paths (omit ALLOW_RELATIVE_PATHS)
LOAD_ENV_ONLY = 1  # ADBC_LOAD_FLAG_SEARCH_ENV

with dbapi.connect(
    driver="mydb",  # resolves to mydb.toml (if found), else mydb.(so/dylib/dll)
    db_kwargs={"load_flags": str(LOAD_ENV_ONLY)},
) as conn:
    ...
```

Flag meanings and search order are standardized. ([Apache Arrow][4])

---

## C) Low-level API: handles, options, and Arrow C Data interface — deep dive

### Mental model

Low-level is **handle-oriented** and Arrow-native:

* `AdbcDatabase` owns a loaded driver + database configuration.
* `AdbcConnection` is a session (transactions, metadata).
* `AdbcStatement` is an executable/query object (SQL or Substrait, bind params, execute).
  Everything is a context-manageable native handle. ([Apache Arrow][2])

### Full surface area (core classes/methods)

**Enums / standardized option keys**

* `AdbcStatusCode` covers canonical error categories. ([Apache Arrow][2])
* `DatabaseOptions.URI/USERNAME/PASSWORD`, `ConnectionOptions.CURRENT_CATALOG/CURRENT_DB_SCHEMA/ISOLATION_LEVEL`. ([Apache Arrow][2])
* `StatementOptions` includes:

  * `BIND_BY_NAME = "adbc.statement.bind_by_name"`
  * `INCREMENTAL = "adbc.statement.exec.incremental"`
  * ingest keys: `adbc.ingest.mode`, `adbc.ingest.target_*`, `adbc.ingest.temporary` (some explicitly experimental)
  * `PROGRESS = "adbc.statement.exec.progress"` ([Apache Arrow][2])

**`AdbcDatabase(**kwargs)`**

* Requires `driver` at minimum; supports `set_options(**kwargs)`; note “not all drivers support setting options after creation.” ([Apache Arrow][2])

**`AdbcConnection(database, **kwargs)`**

* Not thread-safe (serialize access). ([Apache Arrow][2])
* Metadata: `get_info`, `get_objects`, `get_table_schema`, `get_table_types`. ([Apache Arrow][2])
* Distributed result read: `read_partition(partition) -> ArrowArrayStreamHandle`. ([Apache Arrow][2])
* Transactions: `commit`, `rollback`, `set_autocommit`. ([Apache Arrow][2])
* Cancel: `cancel()`. ([Apache Arrow][2])

**`AdbcStatement(connection)`**

* Not thread-safe. ([Apache Arrow][2])
* Query: `set_sql_query(str)` / `set_substrait_plan(bytes)`. ([Apache Arrow][2])
* Bind: `bind(data, schema=None)` / `bind_stream(stream)` accept PyCapsule/int/handle forms. ([Apache Arrow][2])
* Prepare: `prepare()`, `get_parameter_schema()`. ([Apache Arrow][2])
* Execute:

  * `execute_query() -> (ArrowArrayStreamHandle, rowcount)` ([Apache Arrow][2])
  * `execute_update() -> affected_rows` ([Apache Arrow][2])
  * `execute_schema() -> ArrowSchemaHandle` ([Apache Arrow][2])
  * `execute_partitions() -> (partitions: list[bytes], schema: ArrowSchemaHandle|None, rowcount)`; schema may be `None` in incremental mode; not all drivers support it. ([Apache Arrow][2])

### Power knobs

* **StatementOptions.BIND_BY_NAME** for name-based parameter binding. ([Apache Arrow][2])
* **StatementOptions.PROGRESS** for driver-defined progress reporting (read via `get_option*`). ([Apache Arrow][2])
* **Incremental partitions**: enable with `StatementOptions.INCREMENTAL`. ([Apache Arrow][2])

### Failure modes

* **Not supported**: partitioned execution is explicitly optional (“Not all drivers will support this”). ([Apache Arrow][2])
* **Schema is None** under incremental execution if the server doesn’t return schema. ([Apache Arrow][2])
* **Option setting after creation** may be rejected by some drivers. ([Apache Arrow][2])

### Minimal snippet (low-level execute → Arrow stream)

```python
import adbc_driver_manager as dm
import pyarrow as pa

with dm.AdbcDatabase(driver="adbc_driver_sqlite") as db:
    with dm.AdbcConnection(db, uri="file::memory:") as conn:
        with dm.AdbcStatement(conn) as stmt:
            stmt.set_sql_query("SELECT 1 AS x")
            stream_handle, rowcount = stmt.execute_query()
            reader = pa.RecordBatchReader.from_stream(stream_handle)
            print(reader.read_all())
```

Low-level statement execution returns an Arrow stream handle + rowcount. ([Apache Arrow][2])

---

## D) DBAPI layer: PEP 249 core + Arrow/pandas/polars extensions — deep dive

### Mental model

DBAPI is a **convenience façade** that still routes through ADBC statements and Arrow. It’s “PEP 249 compliant” where feasible, plus many extensions that are explicitly *not* DBAPI standard. ([Apache Arrow][2])

### Full surface area

**Resource management (non-negotiable)**

* You must `close()` `Connection` and `Cursor` or you may leak driver resources; fallback `__del__` has nondeterministic timing and can `ResourceWarning` under pytest / env var. ([Apache Arrow][2])

**DBAPI constants**

* `apilevel='2.0'`, `paramstyle='qmark'` (hardcoded but driver-dependent), `threadsafety=1` (connections not shareable). ([Apache Arrow][2])

**`connect(...)`**

* Signature includes: `driver`, optional `uri`, `entrypoint`, `db_kwargs`, `conn_kwargs`, `autocommit`. ([Apache Arrow][2])
* Driver resolution rules:

  * name/manifest → tries `<name>.toml` then platform library naming,
  * shared library path,
  * URI-like strings containing `://` treated as driver name by scheme (URI passed through unchanged),
  * `uri` param takes precedence over `db_kwargs["uri"]`. ([Apache Arrow][2])
* Autocommit default is disabled for DBAPI compliance; warns if driver can’t disable it. ([Apache Arrow][2])

**Cursor parameter semantics**

* `execute(operation, parameters=None)` + `executemany(operation, seq_of_parameters)` accept:

  * SQL as `str`, Substrait plans as `bytes`,
  * parameters as Python sequences **or** Arrow record batch/table/reader,
  * `None` parameters is explicitly allowed (outside DB-API spec). ([Apache Arrow][2])

**Arrow / DataFrame fetch extensions**

* `fetch_arrow()` returns a PyCapsule Arrow stream; **only callable once**, must happen before any other data-consuming fetch. ([Apache Arrow][2])
* Convenience fetch:

  * `fetch_arrow_table()` (DuckDB-like)
  * `fetch_record_batch()` (DuckDB-like RecordBatchReader)
  * `fetchallarrow()` (turbodbc-like)
  * `fetch_df()` (pandas)
  * `fetch_polars()` (polars) ([Apache Arrow][2])

**Prepare + schema introspection (extension)**

* `adbc_prepare(operation) -> pyarrow.Schema|None` (bind parameter schema); executing later via `execute/executemany` won’t prepare again. ([Apache Arrow][2])

---

## E) Partitioned execution + incremental mode (`execute_partitions`, `INCREMENTAL`) — deep dive

### Mental model

Some drivers can execute a query and return **partition descriptors** (opaque `bytes`) for a **distributed result set**. You then “hydrate” each partition into Arrow data. Incremental mode trades “schema upfront” for “start streaming partitions ASAP.” ([Apache Arrow][2])

### E1) Surfaces & return contracts

**Low-level**

* `AdbcStatement.execute_partitions() -> (partitions: list[bytes], schema: ArrowSchemaHandle|None, rowcount)`; schema may be `None` if incremental is enabled and server does not return schema; not all drivers support it. ([Apache Arrow][2])
* `AdbcConnection.read_partition(partition: bytes) -> ArrowArrayStreamHandle`. ([Apache Arrow][2])

**DBAPI extensions (Cursor)**

* `Cursor.adbc_execute_partitions(...) -> (partitions: list[bytes], schema: pyarrow.Schema|None)`; schema may be `None` if incremental execution is enabled and schema hasn’t been returned. ([Apache Arrow][2])
* `Cursor.adbc_read_partition(partition: bytes) -> None` (sets the cursor up to read that partition; you then fetch via `fetch_*`). ([Apache Arrow][2])

### E2) Incremental execution knob

* `StatementOptions.INCREMENTAL = "adbc.statement.exec.incremental"`: “Enable incremental execution on ExecutePartitions.” ([Apache Arrow][2])
* Operational consequence: schema can be `None` until the server provides it (or never, depending on driver). ([Apache Arrow][2])

### E3) Recommended consumption patterns (robust)

1. **Schema-first (deterministic)**

   * If you require schema to allocate downstream buffers, call `adbc_execute_schema` first. ([Apache Arrow][2])
2. **Incremental-first (latency-first)**

   * Enable incremental mode; accept `schema is None`; start processing partitions as they arrive; treat schema discovery as optional/out-of-band.
3. **Rowcount discipline**

   * Treat `rowcount == -1` as “unknown” (common) and never rely on it for correctness. ([Apache Arrow][2])
4. **Driver capability probing**

   * Be prepared for “not supported” behavior on `execute_partitions`. ([Apache Arrow][2])

### E4) Failure modes

* **Not supported** (`execute_partitions` optional): surface as `NotSupportedError` at DBAPI layer or `NOT_IMPLEMENTED`/`UNKNOWN`-ish status at low-level. ([Apache Arrow][2])
* **Schema None breaks consumers**: many Arrow consumers expect a schema; mitigate via schema-first call or a fallback query execution path. ([Apache Arrow][2])
* **Partition descriptor portability**: descriptors are opaque; assume they’re only valid with the same driver/version and compatible server context.

### E5) Minimal snippets

**Low-level: execute partitions + read each partition as Arrow**

```python
import adbc_driver_manager as dm
import pyarrow as pa

with dm.AdbcDatabase(driver="...") as db:
    with dm.AdbcConnection(db, uri="...") as conn:
        with dm.AdbcStatement(conn) as stmt:
            stmt.set_options(**{dm.StatementOptions.INCREMENTAL.value: "true"})
            stmt.set_sql_query("SELECT ...")
            parts, schema_handle, _ = stmt.execute_partitions()

            tables = []
            for p in parts:
                stream = conn.read_partition(p)
                tables.append(pa.RecordBatchReader.from_stream(stream).read_all())
            full = pa.concat_tables(tables)
```

Low-level partition contracts and incremental schema behavior are defined. ([Apache Arrow][2])

**DBAPI: execute partitions → read partition → fetch**

```python
import adbc_driver_manager.dbapi as dbapi

with dbapi.connect(driver="...", uri="...") as conn:
    cur = conn.cursor()
    parts, schema = cur.adbc_execute_partitions("SELECT ...")
    for p in parts:
        cur.adbc_read_partition(p)
        batch_reader = cur.fetch_record_batch()
        ...
```

DBAPI partition APIs are extensions (not standard DBAPI). ([Apache Arrow][2])

---

## F) Arrow-native ingestion (`adbc_ingest`, ingest options) — deep dive

### Mental model

`adbc_ingest` is the “bulk load” fast-path: instead of row-by-row insert loops, you stream Arrow data directly into the driver’s ingestion implementation. ([Apache Arrow][2])

### F1) Surface area (DBAPI Cursor)

`Cursor.adbc_ingest(...)` signature:

* `table_name: str`
* `data`: `pyarrow.RecordBatch | pyarrow.Table | pyarrow.RecordBatchReader | CapsuleType`
* `mode: Literal['append','create','replace','create_append'] = 'create'`
* optional: `catalog_name`, `db_schema_name` (explicitly EXPERIMENTAL)
* optional: `temporary: bool = False` (explicitly EXPERIMENTAL; most drivers won’t support with catalog/schema) ([Apache Arrow][2])

Data can be any Arrow-compatible object that implements the PyCapsule protocol (`__arrow_c_array__` / `__arrow_c_stream__`). ([Apache Arrow][2])

Return value:

* `int` inserted rows, or `-1` if driver can’t provide it. ([Apache Arrow][2])

### F2) Ingestion mode semantics (exact)

* `append`: append to existing table (error if missing)
* `create`: create + insert (error if exists)
* `create_append`: create if not exists, then insert
* `replace`: drop existing if any, then behave like `create` ([Apache Arrow][2])

### F3) Statement-level ingest knobs (low-level option keys)

The standardized ingest option keys (set via `AdbcStatement.set_options(...)` or driver-specific wrappers) include:

* `adbc.ingest.mode`
* `adbc.ingest.target_table`
* `adbc.ingest.target_catalog` / `adbc.ingest.target_db_schema` (EXPERIMENTAL)
* `adbc.ingest.temporary` (EXPERIMENTAL) ([Apache Arrow][2])

There are also legacy module-level ingest constants (`INGEST_OPTION_MODE`, `INGEST_OPTION_TARGET_TABLE`, etc.). ([Apache Arrow][2])

### F4) Failure modes (practical)

* **Driver rejects temporary + catalog/schema** (explicitly called out). ([Apache Arrow][2])
* **Arrow object compatibility**: some Arrow-y objects may not be accepted as-is by a given driver’s ingestion path; in practice you may need to materialize into a `pyarrow.Table` or `RecordBatchReader` depending on driver. (Example observed in Arrow ADBC issue reports.) ([GitHub][6])
* **Mode mismatch**: `append` errors if table missing; `create` errors if exists; treat these as contract assertions rather than “maybe” operations. ([Apache Arrow][2])

### F5) Minimal snippets

**Ingest a PyArrow table (create/replace patterns)**

```python
import pyarrow as pa
import adbc_driver_manager.dbapi as dbapi

data = pa.table({"a": [1, 2, 3]})

with dbapi.connect(driver="...", uri="...") as conn:
    with conn.cursor() as cur:
        cur.adbc_ingest("my_table", data, mode="replace")
```

Ingest signature + mode semantics are defined. ([Apache Arrow][2])

**Temporary ingest (best-effort, driver-dependent)**

```python
with dbapi.connect(driver="...", uri="...") as conn:
    with conn.cursor() as cur:
        cur.adbc_ingest("stage_tmp", data, mode="create", temporary=True)
```

Temporary ingest constraints are documented as experimental and often unsupported with catalog/schema. ([Apache Arrow][2])

---

## G) Errors, status codes, and diagnostics — deep dive

### Mental model

ADBC errors are **structured**: a canonical status category + optional vendor code/SQLSTATE + optional details payload. DBAPI exceptions preserve that structure as attributes. ([Apache Arrow][2])

### Full surface area

**Status categories**: `AdbcStatusCode` IntEnum includes `INVALID_ARGUMENT`, `INVALID_DATA`, `NOT_FOUND`, `NOT_IMPLEMENTED`, `TIMEOUT`, `UNAUTHENTICATED`, `UNAUTHORIZED`, etc. ([Apache Arrow][2])

**Exception hierarchy** (PEP 249 aligned)

* Base: `adbc_driver_manager.Error(..., status_code, vendor_code=None, sqlstate=None, details=None)` with attributes:

  * `status_code: AdbcStatusCode`
  * `vendor_code: int|None`
  * `sqlstate: str|None`
  * `details: list[tuple[str, bytes]]|None` ([Apache Arrow][2])
* Subclasses: `DatabaseError`, `DataError`, `IntegrityError`, `InterfaceError`, `InternalError`, `NotSupportedError`, `OperationalError`, `ProgrammingError`, plus `Warning`. ([Apache Arrow][2])

### Power knobs (debug posture)

* Turn on dev-time leak warnings via `_ADBC_DRIVER_MANAGER_WARN_UNCLOSED_RESOURCE=1`. ([Apache Arrow][2])
* Treat `details` as driver-defined binary attachments; log them in hex/base64 for forensic value. ([Apache Arrow][2])

### Failure modes

* Misclassifying errors as strings: you lose `status_code/vendor_code/sqlstate/details` which are often the only actionable signal. ([Apache Arrow][2])

### Minimal snippet (structured error handling)

```python
import adbc_driver_manager as dm

try:
    ...
except dm.Error as e:
    print("status:", e.status_code)
    print("vendor:", e.vendor_code, "sqlstate:", e.sqlstate)
    print("details:", e.details)
```

Exception attributes are part of the public contract. ([Apache Arrow][2])

---

## H) Lifecycle, thread-safety, cancellation, and “don’t leak native resources” — deep dive

### Mental model

You are holding **native resources** (driver, database handles, connections, statements, Arrow streams). Python GC is not a safe lifecycle manager here; use context managers and explicit close. ([Apache Arrow][2])

### Full surface area

* Low-level handles are context managers (`_AdbcHandle`). ([Apache Arrow][2])
* `AdbcConnection` and `AdbcStatement` are **not thread-safe** (serialize access). ([Apache Arrow][2])
* DBAPI declares `threadsafety = 1` (“connections may not be shared”). ([Apache Arrow][2])
* DBAPI warns that failure to close connections may leak memory or database connections. ([Apache Arrow][2])
* Cancellation:

  * Connection: `adbc_cancel()` (DBAPI) / `cancel()` (low-level)
  * Statement: `adbc_cancel()` (DBAPI cursor) / `cancel()` (low-level statement) ([Apache Arrow][2])

### Power knobs

* Explicit `autocommit` control in `dbapi.connect(..., autocommit=...)` (default disabled; warns if can’t disable). ([Apache Arrow][2])
* Option setting: `set_options(**{"some.option": "value"})` exists on `AdbcDatabase`/`AdbcConnection`/`AdbcStatement`, but not all drivers support post-creation updates. ([Apache Arrow][2])

### Failure modes

* Shared connection across threads → undefined behavior (your bug, not the driver’s). ([Apache Arrow][2])
* Forgetting to close cursor/connection → leaked resources + dev-time `ResourceWarning`. ([Apache Arrow][2])

### Minimal snippet (idiomatic lifecycle discipline)

```python
import adbc_driver_manager.dbapi as dbapi

with dbapi.connect(driver="...", uri="...") as conn:
    with conn.cursor() as cur:
        cur.execute("...")
        ...
# all native resources deterministically released
```

DBAPI resource management requirements are explicit. ([Apache Arrow][2])

[1]: https://arrow.apache.org/adbc/main/python/driver_manager.html "Driver Manager - ADBC 22 (dev) documentation"
[2]: https://arrow.apache.org/adbc/main/python/api/adbc_driver_manager.html "adbc_driver_manager - ADBC 22 (dev) documentation"
[3]: https://pypi.org/project/adbc-driver-manager/ "adbc-driver-manager · PyPI"
[4]: https://arrow.apache.org/adbc/main/format/driver_manifests.html "ADBC Driver Manager and Manifests - ADBC 22 (dev) documentation"
[5]: https://arrow.apache.org/adbc/21/cpp/api/adbc__driver__manager_8h.html "ADBC: /adbc/c/include/arrow-adbc/adbc_driver_manager.h File Reference"
[6]: https://github.com/apache/arrow-adbc/issues/1310?utm_source=chatgpt.com "python: errors using pyarrow Dataset with adbc_ingest() for ..."


## 1) DuckDB’s ADBC driver: what it is (and what it is *not*)

DuckDB ships an **ADBC driver** that exposes a database-agnostic, C-style API where **Arrow is the transfer format**. DuckDB’s driver is specifically efficient because it builds on DuckDB↔Arrow **zero-copy integration**, so query results can be surfaced as Arrow with only a small constant conversion overhead (vs ODBC/JDBC-style row-wise reshaping). ([DuckDB][1])

**Spec coverage / key limitation:** DuckDB’s ADBC driver implements the full ADBC specification **except**:

* `ConnectionReadPartition`
* `StatementExecutePartitions`

Those functions exist for systems that internally partition result sets; DuckDB doesn’t, so **distributed partition descriptors aren’t a thing for DuckDB ADBC**. ([DuckDB][1])

**How the driver is “addressed”**

* You point the driver manager at a **DuckDB shared library** (the `libduckdb` / `duckdb.dll` you already have via DuckDB installs) and an exported **entrypoint** function called `duckdb_adbc_init`. You can also set a `path` option to persist to a file; otherwise it’s in-memory. ([DuckDB][1])

---

## 2) Python connectivity: two robust ways (wrapper vs “raw” driver-manager)

### A) DuckDB-provided wrapper (most ergonomic when present)

DuckDB’s docs show using a Python module `adbc_driver_duckdb.dbapi`:

```python
import adbc_driver_duckdb.dbapi
import pyarrow as pa

with adbc_driver_duckdb.dbapi.connect("test.db") as conn, conn.cursor() as cur:
    cur.execute("SELECT 42")
    tbl = cur.fetch_arrow_table()
    print(tbl)

    data = pa.record_batch([[1, 2, 3, 4], ["a", "b", "c", "d"]], names=["ints", "strs"])
    cur.adbc_ingest("AnswerToEverything", data)
```

This is the canonical “DuckDB ADBC in Python” demo: Arrow-native fetch (`fetch_arrow_table`) and Arrow-native ingest (`adbc_ingest`). ([DuckDB][1])

> Practical note: there have been packaging regressions where `adbc_driver_duckdb` wasn’t importable in some DuckDB Python releases (e.g., it existed but wasn’t exported). If you ever hit that, use the driver-manager path below. ([GitHub][2])

### B) Driver-manager direct (most stable + portable)

Apache Arrow’s ADBC cookbook demonstrates loading DuckDB’s driver through the generic driver manager by passing:

* `driver=duckdb.__file__`
* `entrypoint="duckdb_adbc_init"` ([Apache Arrow][3])

```python
import duckdb
from adbc_driver_manager import dbapi

conn = dbapi.connect(
    driver=duckdb.__file__,
    entrypoint="duckdb_adbc_init",
    db_kwargs={"path": "test.db"},   # omit for :memory: semantics
)
with conn, conn.cursor() as cur:
    cur.execute("SELECT 1")
    print(cur.fetchone())
```

This route is “best-in-class” if you’re building a system that might swap ADBC backends (DuckDB today, something else tomorrow).

---

## 3) PyArrow ↔ DuckDB over ADBC: the Arrow-native contract

### Outbound (DuckDB → PyArrow)

Prefer **Arrow-returning** fetch methods, not row-wise DBAPI:

* `fetch_arrow_table()` when results fit in memory
* `fetch_record_batch()` / RecordBatchReader-style streaming when they don’t

DuckDB’s own Python docs also emphasize Arrow-centric querying and returning Arrow Tables. ([DuckDB][4])

### Inbound (PyArrow → DuckDB)

Use `adbc_ingest(table_name, data, mode=...)` for bulk loads (Arrow Table / RecordBatch / RecordBatchReader). This avoids per-row overhead from prepare/bind/insert loops. ([Apache Arrow][5])

### What you *don’t* get with DuckDB ADBC

No `execute_partitions` / `read_partition` result partitioning APIs—DuckDB explicitly doesn’t implement those ADBC calls. If you need scalable/streaming reads, do it with Arrow streaming (`fetch_record_batch`/reader), not partition descriptors. ([DuckDB][1])

---

## 4) Best-in-class implementation: “PyArrow → DuckDB database file” (fast, bounded-memory, schema-stable)

This is the pattern I’d treat as “production-grade” for an Arrow-heavy pipeline that persists into DuckDB.

### 4.1 Decide whether you should *even* materialize PyArrow first

If your data source is already **files** (Parquet, dataset directories), the best pipeline is often to let DuckDB scan them directly or via Arrow Datasets/Scanners so it can push down projections/filters. DuckDB explicitly pushes column selection + filters into Arrow Dataset scans. ([DuckDB][4])

If your data is truly **in-memory Arrow** (produced by upstream compute), proceed with ADBC ingest.

### 4.2 Stream ingestion, don’t build giant Tables

Best practice is to ingest as a **RecordBatchReader** (or a sequence of reasonably sized RecordBatches) so memory stays bounded.

### 4.3 Control schema explicitly when correctness matters

`adbc_ingest` can infer table schema from Arrow, but “best in class” usually means:

1. `CREATE TABLE …` with explicit DuckDB types (especially for decimals, timestamps/timezones, categoricals/dictionaries, nested structs/lists)
2. `adbc_ingest(mode="append")` into that table

(You get deterministic typing and avoid silent widen/cast surprises.)

### 4.4 Use transactional boundaries for atomic loads

Wrap multi-step loads in a transaction:

* Stage into a temp/staging table (optional)
* Swap/insert into final table
* Commit

### 4.5 Validate and observe

* If `adbc_ingest` returns `-1` rowcount (allowed by spec), run a `SELECT count(*)` check as your authoritative verification. ([Apache Arrow][5])
* Prefer Arrow fetch APIs for verification queries to keep the whole loop columnar.

---

## 5) Concrete “best-in-class” recipes

### Recipe 1 — Ingest an Arrow stream into a persistent DuckDB table (ADBC, streaming)

```python
import duckdb
import pyarrow as pa
from adbc_driver_manager import dbapi

def ingest_batches(db_path: str, table: str, reader: pa.RecordBatchReader) -> None:
    conn = dbapi.connect(
        driver=duckdb.__file__,
        entrypoint="duckdb_adbc_init",
        db_kwargs={"path": db_path},
    )
    with conn, conn.cursor() as cur:
        # (Optional) explicit schema control:
        # cur.execute(f"CREATE TABLE IF NOT EXISTS {table} (...explicit types...)")

        # Bulk ingest (Arrow-native)
        cur.adbc_ingest(table, reader, mode="create_append")  # or "append"/"replace"/"create"
        conn.commit()

# Example: build a streaming reader from batches
batch = pa.record_batch([[1, 2, 3], ["a", "b", "c"]], names=["i", "s"])
reader = pa.ipc.RecordBatchReader.from_batches(batch.schema, [batch])

ingest_batches("my.duckdb", "t_ingest", reader)
```

`adbc_ingest` is the spec’d bulk Arrow ingestion path. ([Apache Arrow][5])

### Recipe 2 — Query and retrieve results as Arrow (no row-wise conversion)

```python
import duckdb
from adbc_driver_manager import dbapi

conn = dbapi.connect(driver=duckdb.__file__, entrypoint="duckdb_adbc_init", db_kwargs={"path": "my.duckdb"})
with conn, conn.cursor() as cur:
    cur.execute("SELECT * FROM t_ingest WHERE i = 2")
    tbl = cur.fetch_arrow_table()          # Arrow table
    # or: reader = cur.fetch_record_batch()  # stream results
```

DuckDB’s ADBC docs explicitly show `fetch_arrow_table`, and DuckDB’s Arrow guide demonstrates returning Arrow Tables from queries. ([DuckDB][1])

### Recipe 3 — Non-ADBC fallback (still best-in-class when you’re purely in Python)

If you don’t need ADBC portability, DuckDB’s Python client can query Arrow objects directly, and you can persist via `CREATE TABLE AS` / `INSERT INTO` referencing a PyArrow object:

```python
import duckdb
import pyarrow as pa

con = duckdb.connect("my.duckdb")
my_arrow = pa.Table.from_pydict({"a": [42]})
con.execute("CREATE TABLE my_table AS SELECT * FROM my_arrow")
```

DuckDB documents this Arrow import pattern explicitly. ([DuckDB][6])

---

If you tell me **your target shape** (“I have a PyArrow Dataset directory”, “I have a stream of RecordBatches”, “I already have a huge Table in memory”, etc.), I can give you a single **canonical ingestion function** (schema policy + batch sizing + transactional staging + validation) that you can drop into your CodeIntel-style DatasetContract system.

[1]: https://duckdb.org/docs/stable/clients/adbc.html "ADBC Client – DuckDB"
[2]: https://github.com/duckdb/duckdb/issues/19038?utm_source=chatgpt.com "Regression in 1.4.0: adbc_driver_duckdb is not found #74"
[3]: https://arrow.apache.org/adbc/0.5.0/python/recipe/driver_manager.html?utm_source=chatgpt.com "Driver Manager Recipes - ADBC 0.5.0 documentation"
[4]: https://duckdb.org/docs/stable/guides/python/sql_on_arrow.html "SQL on Apache Arrow – DuckDB"
[5]: https://arrow.apache.org/adbc/14/python/api/adbc_driver_manager.html?utm_source=chatgpt.com "adbc_driver_manager - ADBC 14 documentation"
[6]: https://duckdb.org/docs/stable/guides/python/import_arrow.html "Import from Apache Arrow – DuckDB"
