
# FastMCP v2.14+ prompts and context #

Below are the **FastMCP v2.14+ “Prompts”** and **“Context + CurrentContext DI”** surfaces, with enough detail to implement them end-to-end as a Python expert.

---

## 1) Prompts in FastMCP (v2.14+)

### What prompts are (vs tools)

Prompts are **server-exposed, reusable, parameterized message templates**. When a client requests a prompt, FastMCP validates arguments, runs your prompt function, and returns the resulting **message(s)** back to the client/LLM. ([FastMCP][1])

The MCP spec explicitly frames prompts as **user-controlled** (typically UI-selectable), not “side-effectful execution” like tools. ([Model Context Protocol][2])

---

### Server-side: defining prompts

#### Basic definition + message primitives

FastMCP’s standard pattern is `@mcp.prompt` over a Python function; the function name becomes the prompt identifier, and the docstring becomes the description by default. ([FastMCP][1])

Prompts can return:

* `str` → auto-converted into a single “user” message
* `PromptMessage` → used as-is
* `list[PromptMessage | str]` → multi-message prompt (conversation template)
* `PromptResult` → full control over messages + runtime metadata
* other types → coerced to string and wrapped as a message ([FastMCP][1])

FastMCP also provides a friendlier `Message(...)` constructor for multi-message prompts. ([FastMCP][1])

> Note: prompts **cannot use `*args/**kwargs`**, because FastMCP must generate a full argument schema. ([FastMCP][1])

#### Decorator configuration (feature-complete)

`@mcp.prompt(...)` supports:

* `name`, `title`, `description`
* `tags` (categorization)
* `meta` (static metadata passed to the client as `_meta`)
* `enabled` (hide/disable prompt)
* `icons` (optional icon list) ([FastMCP][1])

#### Typed arguments (despite MCP’s “string args” rule)

The MCP spec requires prompt arguments be passed as **strings**; FastMCP lets you type annotate arguments for DX and will deserialize JSON-stringified values for common types (e.g., `list[int]`, `dict[str, str]`) while generating schema guidance to help clients/LLMs format inputs. ([FastMCP][1])

#### PromptResult (new in 2.14.1): runtime metadata per render

If you need per-request metadata (e.g., priority, routing hints), return `PromptResult(messages=..., meta=...)`. This `meta` is **runtime metadata for that render**, distinct from the static `@mcp.prompt(meta=...)` definition metadata. ([FastMCP][1])

#### Enable/disable at runtime

You can disable a prompt at definition-time (`enabled=False`) or later via `.disable()` / `.enable()`. Disabled prompts disappear from listings and calling them yields “Unknown prompt”. ([FastMCP][1])

#### Change notifications + duplicate prompt behavior

* FastMCP can emit `notifications/prompts/list_changed` when prompts are added/enabled/disabled **during an active request context** (not during startup). ([FastMCP][1])
* Duplicate registration behavior is configurable via `FastMCP(..., on_duplicate_prompts=...)` with `"warn"` (default), `"error"`, `"replace"`, `"ignore"`. ([FastMCP][1])

#### Context inside prompts

Prompts can access `Context` (including `request_id`) by accepting a context parameter. ([FastMCP][1])

---

### Client-side: discovering + rendering prompts

From a FastMCP client:

* `await client.list_prompts()` returns prompt definitions (name, description, arguments) ([FastMCP][3])
* Tags/extra metadata live under `_meta._fastmcp` by convention; this behavior can be disabled via `include_fastmcp_meta`. ([FastMCP][3])
* `await client.get_prompt(name, args)` returns a `GetPromptResult` with `.messages`. ([FastMCP][3])
* The client auto-serializes complex argument values to JSON strings (using `pydantic_core.to_json()`), matching the MCP spec’s string-args requirement; servers can deserialize back to typed values. ([FastMCP][3])
* Raw MCP objects are available via `list_prompts_mcp()` / `get_prompt_mcp()`. ([FastMCP][3])

---

### Minimal end-to-end example (server + client)

**Server**

````python
from fastmcp import FastMCP
from fastmcp.dependencies import CurrentContext
from fastmcp.server.context import Context
from fastmcp.prompts import PromptResult, Message
from pydantic import Field

mcp = FastMCP(
    name="PromptServer",
    on_duplicate_prompts="error",  # or warn/replace/ignore
)

@mcp.prompt(
    name="code_review",
    title="Request Code Review",
    description="Ask the LLM to review code and propose improvements",
    tags={"analysis", "code"},
    meta={"version": "1.0"},
)
def code_review(
    code: str = Field(description="The code to review"),
    focus: str = Field(default="readability", description="readability|security|perf"),
) -> PromptResult:
    return PromptResult(
        messages=[
            Message("You are a senior Python reviewer.", role="system"),
            Message(f"Review this code (focus={focus}):\n\n```python\n{code}\n```"),
        ],
        # runtime meta for this render (distinct from decorator meta)
        meta={"focus": focus},
    )

@mcp.prompt(enabled=False)
def experimental_prompt() -> str:
    return "Not ready yet"
````

This uses the full surface: decorator metadata/tags, typed args, `PromptResult` (2.14.1), and enable/disable controls. ([FastMCP][1])

**Client**

```python
from fastmcp import Client

async def run():
    async with Client("http://localhost:8000/mcp") as client:
        prompts = await client.list_prompts()
        result = await client.get_prompt("code_review", {
            "code": "def hello():\n    print('world')",
            "focus": "security",
        })

        # Feed result.messages into your LLM consumer
        llm_messages = [
            {"role": m.role, "content": getattr(m.content, "text", m.content)}
            for m in result.messages
        ]
        return llm_messages
```

This is the canonical `list_prompts()` / `get_prompt()` usage and shows how you’d adapt the returned messages for an LLM client. ([FastMCP][3])

---

## 2) Context + `CurrentContext()` dependency injection (FastMCP v2.14+)

### The big picture

FastMCP’s `Context` is how tools/resources/prompts access MCP “session capabilities”: logging, progress, resource/prompt access, sampling, elicitation, state, request metadata, and server access. ([FastMCP][4])

---

### Accessing context: the v2.14 preferred pattern

#### Preferred: dependency injection (`CurrentContext()`)

In v2.14 the preferred method is to inject context via a defaulted dependency:

```python
from fastmcp.dependencies import CurrentContext
from fastmcp.server.context import Context

@mcp.tool
async def do_work(x: int, ctx: Context = CurrentContext()) -> str:
    ...
```

Key properties of this pattern:

* dependency params are **excluded from the MCP schema** (clients never see them)
* context methods are async (so your handler often is async)
* **context is request-scoped** (new context per operation; not persisted across requests)
* using context outside a request raises errors ([FastMCP][4])

#### Legacy: type-hint injection

Still supported: add a `Context`-typed parameter and FastMCP injects it automatically; parameter name doesn’t matter, and unions/`Annotated[]` work. ([FastMCP][4])

---

### What “DI in 2.14” actually gives you (beyond Context)

FastMCP 2.14’s DI is powered by Docket and supports:

* **custom dependencies** via `Depends(callable)` (sync/async/async context manager)
* nested dependencies
* resource cleanup via async context manager dependencies
* dependency parameters are excluded from the schema ([FastMCP][4])

Also important: dependency resolution **filters out user-provided args that collide with dependency parameter names** (a security feature preventing callers from overriding injected params). ([FastMCP][5])

Built-ins in `fastmcp.server.dependencies` include `CurrentContext`, plus things like `CurrentDocket`, `CurrentWorker`, `CurrentFastMCP`, and HTTP/auth helpers (`get_http_request`, `get_http_headers`, `get_access_token`). ([FastMCP][5])

---

### The full Context surface area (methods + properties you actually get)

From the FastMCP SDK docs, `Context` includes (grouped):

**Logging**

* `ctx.log(...)` (with MCP logging levels)
* convenience: `ctx.debug/info/warning/error(...)` ([FastMCP][6])

**Progress**

* `ctx.report_progress(progress, total=None, message=None)` ([FastMCP][6])

**Resources**

* `ctx.list_resources()`
* `ctx.read_resource(uri)` ([FastMCP][6])

**Prompts (programmatic access)**

* `ctx.list_prompts()`
* `ctx.get_prompt(name, arguments=None)` ([FastMCP][4])

**Request/session metadata**

* `ctx.request_id`, `ctx.client_id`, `ctx.session_id`
* `ctx.request_context` (may be `None` before MCP handshake completes)
* `ctx.session` for advanced usage ([FastMCP][4])

**Client-provided metadata**

* `ctx.request_context.meta` is available for tools/resources/prompts when provided; accessed via attributes (e.g. `meta.user_id`). ([FastMCP][4])

**State management (within a single request)**

* `ctx.set_state(key, value)` / `ctx.get_state(key)`
  State is request-scoped; use external storage for cross-request persistence. ([FastMCP][4])

**Roots**

* `ctx.list_roots()` (roots are provided by the client) ([FastMCP][6])

**List-changed notifications**

* `ctx.send_tool_list_changed()`
* `ctx.send_resource_list_changed()`
* `ctx.send_prompt_list_changed()` ([FastMCP][6])

**Transport helper**

* `ctx.close_sse_stream()` (used with StreamableHTTP + EventStore to force reconnect and avoid long-held connections) ([FastMCP][6])

**LLM sampling**

* `ctx.sample(...)` sends a sampling request to the client’s LLM handler. ([FastMCP][6])

**User elicitation**

* `ctx.elicit(...)` requests structured input from the client/user (with MCP schema constraints). ([FastMCP][6])

**Server access**

* `ctx.fastmcp` gives the underlying server instance when needed. ([FastMCP][4])

---

### Minimal “CurrentContext + Context” implementation pattern

```python
from fastmcp import FastMCP
from fastmcp.dependencies import CurrentContext
from fastmcp.server.context import Context
from fastmcp.server.dependencies import get_http_headers

mcp = FastMCP("ContextDemo")

@mcp.tool
async def do_work(resource_uri: str, ctx: Context = CurrentContext()) -> dict:
    # safe HTTP header access (works even when request_context isn't established)
    headers = get_http_headers()
    await ctx.info(f"UA={headers.get('user-agent','?')} reading {resource_uri}")

    contents = await ctx.read_resource(resource_uri)
    await ctx.report_progress(progress=1, total=1, message="done")

    # request-scoped scratch/state
    ctx.set_state("last_uri", resource_uri)

    return {
        "request_id": ctx.request_id,
        "session_id": ctx.session_id,
        "last_uri": ctx.get_state("last_uri"),
        "bytes_or_text_preview": str(contents[0].content)[:200],
    }
```

This follows the v2.14 preferred injection pattern and uses “safe header” access, request IDs, progress, resource reads, and state. ([FastMCP][4])

---

If you want next: I can do the **same level of “full surface area + minimum implementation info”** for the next two gap items (User Elicitation + MCP middleware), and I’ll tailor the examples specifically to your “single box / few LLM clients / code-intel serving” architecture.

[1]: https://gofastmcp.com/servers/prompts "Prompts - FastMCP"
[2]: https://modelcontextprotocol.io/specification/2025-06-18/server/prompts "Prompts - Model Context Protocol"
[3]: https://gofastmcp.com/clients/prompts "Prompts - FastMCP"
[4]: https://gofastmcp.com/servers/context "MCP Context - FastMCP"
[5]: https://gofastmcp.com/python-sdk/fastmcp-server-dependencies "dependencies - FastMCP"
[6]: https://gofastmcp.com/python-sdk/fastmcp-server-context "context - FastMCP"


# FastMCP 2.14+ user elicitation and middleware #

## 1) User Elicitation (FastMCP v2.14+)

### What you get (full surface area)

**Core API**

* Server calls `await ctx.elicit(message=..., response_type=...)` and receives an `ElicitationResult` with:

  * `action ∈ {"accept","decline","cancel"}`
  * `data` present only when `action=="accept"` ([FastMCP][1])

**Spec constraints you must design around**

* MCP elicitation schemas are intentionally limited to **flat objects with primitive properties** (string/number/integer/boolean/enum). Complex nested objects/arrays are intentionally not supported by the spec. ([Model Context Protocol][2])
* Supported string formats include `email`, `uri`, `date`, `date-time`. ([Model Context Protocol][2])
* **Security requirement:** servers **MUST NOT** use elicitation to request sensitive information. ([Model Context Protocol][2])
* Clients that support elicitation must declare the `elicitation` capability at initialization. ([Model Context Protocol][2])

**FastMCP convenience layers on top of the spec**

* **Scalar types**: `response_type=str|int|bool|...` are automatically wrapped/unwrapped into MCP-compatible object schemas for you. ([FastMCP][1])
* **No response / approval gate**: `response_type=None` requests an empty object schema; on accept, `data` is `None`. ([FastMCP][1])
* **Constrained options**: `Literal[...]`, `Enum`, or the shortcut `response_type=["low","medium","high"]`. ([FastMCP][1])
* **Multi-select (new in 2.14.0)**: wrap choices in an extra list level like `response_type=[["bug","feature"]]`; accepted `data` becomes a list of selected strings. ([FastMCP][1])
* **Titled options (new in 2.14.0)**: pass a dict of values → `{title: ...}`; FastMCP generates SEP-1330 compliant `oneOf` schemas with `const/title` for better UI labels. ([FastMCP][1])
* **Structured responses**: use a dataclass / TypedDict / Pydantic model, but it must stay **shallow** (scalar/enum fields). ([FastMCP][1])
* **Default values (new in 2.14.0)**: use Pydantic `Field(default=...)`; fields become optional and clients can pre-populate defaults. ([FastMCP][1])
* **Multi-turn elicitation**: call `ctx.elicit(...)` multiple times in one tool to gather input progressively. ([FastMCP][1])
* **Pattern matching helpers**: `AcceptedElicitation / DeclinedElicitation / CancelledElicitation` classes for `match` statements. ([FastMCP][1])
* **Hard requirement:** if the client doesn’t support elicitation, `ctx.elicit()` raises an error. ([FastMCP][1])

---

### Minimal server implementation (covers the “important” shapes)

```python
from __future__ import annotations

from enum import Enum
from pydantic import BaseModel, Field
from fastmcp import FastMCP, Context

mcp = FastMCP("ElicitationDemo")

class Priority(str, Enum):
    low = "low"
    medium = "medium"
    high = "high"

class TaskDetails(BaseModel):
    title: str = Field(description="Short task title")
    description: str = Field(default="", description="Optional description")  # default -> optional
    priority: Priority = Field(default=Priority.medium, description="Priority")

@mcp.tool
async def plan_task(ctx: Context) -> dict:
    # 1) Structured object (shallow scalars + enum) with defaults
    details = await ctx.elicit("Enter task details", response_type=TaskDetails)
    if details.action != "accept":
        return {"status": details.action}

    # 2) Multi-select (2.14+): extra list nesting
    tags = await ctx.elicit(
        "Choose tags",
        response_type=[["bug", "feature", "documentation"]],
    )
    if tags.action != "accept":
        return {"status": tags.action}

    # 3) “Approval only” (no response expected)
    approve = await ctx.elicit("Approve creating this task?", response_type=None)
    if approve.action != "accept":
        return {"status": approve.action}

    return {
        "status": "ok",
        "details": details.data.model_dump(),
        "tags": tags.data,
    }
```

This uses FastMCP’s key v2.14 conveniences: shallow structured models + defaults, multi-select, and `None` approval gates. ([FastMCP][1])

---

### Minimal client implementation (the part most people miss)

FastMCP requires a client-side handler. The handler receives:

* `message: str`
* `response_type: type` **dataclass generated from the server’s JSON schema** (or `None` for “no response”)
* `params` (contains the raw schema if needed)
* `context` (request metadata) ([FastMCP][3])

You can return:

* the dataclass instance directly (implicit accept), or
* `ElicitResult(action="accept|decline|cancel", content=...)` for explicit control. ([FastMCP][3])

```python
from fastmcp import Client
from fastmcp.client.elicitation import ElicitResult

async def elicitation_handler(message: str, response_type: type | None, params, context):
    print(f"\nSERVER ASKED: {message}")

    # “No response” (approval gate)
    if response_type is None:
        ok = input("Type 'y' to approve: ").strip().lower() == "y"
        return ElicitResult(action="accept" if ok else "decline")

    # Typical scalar wrappers show up as a dataclass with a `value` field
    # (because FastMCP wraps scalars for MCP compatibility).
    fields = getattr(response_type, "__dataclass_fields__", {})
    if "value" in fields and len(fields) == 1:
        user_input = input("Value: ").strip()
        if not user_input:
            return ElicitResult(action="decline")
        return response_type(value=user_input)

    # For multi-field forms, you can do something UI-driven; here’s a crude CLI fallback:
    kwargs = {}
    for name in fields.keys():
        kwargs[name] = input(f"{name}: ").strip()

    return response_type(**kwargs)

async def main():
    async with Client("http://localhost:8000/mcp", elicitation_handler=elicitation_handler) as client:
        result = await client.call_tool("plan_task", {})
        print(result)
```

This matches the FastMCP client contract: schema → dataclass conversion, plus the `ElicitResult` action model. ([FastMCP][3])

---

## 2) MCP Middleware (FastMCP v2.14+)

### What you get (full surface area)

**Concept**

* MCP middleware is a FastMCP-specific pipeline that can inspect/modify **all MCP JSON-RPC messages** (tools, resources, prompts, listings, notifications). It’s *not* part of the MCP spec and may evolve. ([FastMCP][4])
* Works across transports, but middleware that depends on HTTP headers won’t work on stdio. ([FastMCP][4])

**Adding + ordering**

* Add via `mcp.add_middleware(...)`.
* Order matters: **first added runs first on the way in and last on the way out**. ([FastMCP][4])
* With server composition (`mount`), parent middleware runs first, then child middleware for routes handled by the child. ([FastMCP][4])

**Base class + context**

* Subclass `Middleware` and override either:

  * `__call__(context, call_next)` for *everything*, or
  * specific hook methods. ([FastMCP][4])
* `MiddlewareContext` includes (among other things) `method`, `source`, `type`, `message`, `timestamp`, `fastmcp_context`, and has a `.copy(...)` helper. ([FastMCP][4])

**Hook list (this is the “surface area” you design around)**

* `on_message`, `on_request`, `on_notification` ([FastMCP][4])
* Execution hooks: `on_call_tool`, `on_read_resource`, `on_get_prompt` ([FastMCP][4])
* Listing hooks: `on_list_tools`, `on_list_resources`, `on_list_resource_templates`, `on_list_prompts` ([FastMCP][4])
* `on_initialize` (new in 2.13.0): can reject clients *before* `call_next`; after `call_next`, errors won’t reach the client. ([FastMCP][4])

**Hook hierarchy**

* A tool call typically triggers: `on_message` → `on_request` → `on_call_tool` (and sometimes additional listing calls performed by SDK behavior). ([FastMCP][4])

**Session availability gotcha**

* During initialization, `context.fastmcp_context.request_context` may be `None`; for HTTP transports you can still inspect request data using HTTP helpers (e.g. headers) even when the MCP session isn’t established. ([FastMCP][4])

**Component metadata access**

* Listing hooks see full FastMCP component objects (incl. tags), execution hooks do not; for execution, look up components via `context.fastmcp_context.fastmcp.get_tool(...)` / `.get_resource(...)` / `.get_prompt(...)`. ([FastMCP][4])

**Correct ways to block/shape behavior**

* Deny tool calls by raising `ToolError` (not by returning some custom result). ([FastMCP][4])
* Modify tool arguments before `call_next` and transform results after `call_next`. ([FastMCP][4])
* Share request-scoped state with tools using `Context.set_state/get_state` (middleware can set it; tools can read it). ([FastMCP][4])

---

### Built-in middleware you can use immediately (and why it matters)

These are the “production basics” for a single-box server.

* **Timing**: `TimingMiddleware` + `DetailedTimingMiddleware` (per-operation hooks like `on_call_tool`, `on_read_resource`, etc.). ([FastMCP][5])
* **Logging**: `LoggingMiddleware` and `StructuredLoggingMiddleware` (+ `default_serializer`). ([FastMCP][6])
* **Rate limiting**: `RateLimitingMiddleware` (token bucket; burst + sustained) and `SlidingWindowRateLimitingMiddleware`. ([FastMCP][7])
* **Response caching**: `ResponseCachingMiddleware`

  * caches `tools/call`, `resources/read`, `prompts/get`, plus `tools/list`, `resources/list`, `prompts/list`
  * supports invalidation via notifications
  * note: cached values are returned as no-op objects, so some middleware expecting original subclasses may not be compatible. ([FastMCP][8])
* **Error handling + retries**: `ErrorHandlingMiddleware` (converts exceptions to proper MCP errors and tracks stats) and `RetryMiddleware` (exponential backoff for transient failures). ([FastMCP][9])
* **Tool injection**: `ToolInjectionMiddleware`, `PromptToolMiddleware`, `ResourceToolMiddleware` (handy when you want to expose prompts/resources “as tools” or inject ephemeral/admin tools). ([FastMCP][10])

---

### Minimal “single box” middleware stack + custom auth example

Key ordering rule: if a middleware might **short-circuit** (like caching), put it **inside** middleware you still want to run on cache hits (logging/timing). ([FastMCP][4])

```python
from __future__ import annotations

from fastmcp import FastMCP
from fastmcp.server.middleware import Middleware, MiddlewareContext
from fastmcp.server.middleware.logging import StructuredLoggingMiddleware
from fastmcp.server.middleware.timing import DetailedTimingMiddleware
from fastmcp.server.middleware.error_handling import ErrorHandlingMiddleware
from fastmcp.server.middleware.rate_limiting import RateLimitingMiddleware
from fastmcp.server.middleware.caching import ResponseCachingMiddleware
from fastmcp.server.dependencies import get_http_headers
from mcp import McpError
from mcp.types import ErrorData

class ApiKeyMiddleware(Middleware):
    async def on_request(self, context: MiddlewareContext, call_next):
        headers = get_http_headers()  # works even when MCP session isn't ready
        if headers.get("x-api-key") != "dev-secret":
            raise McpError(ErrorData(code=-32000, message="Unauthorized"))
        return await call_next(context)

mcp = FastMCP("MyServer")

# Outer -> inner (first added runs first on inbound)
mcp.add_middleware(StructuredLoggingMiddleware())
mcp.add_middleware(DetailedTimingMiddleware())
mcp.add_middleware(ErrorHandlingMiddleware())
mcp.add_middleware(RateLimitingMiddleware(max_requests_per_second=10.0, burst_capacity=20))
mcp.add_middleware(ApiKeyMiddleware())
mcp.add_middleware(ResponseCachingMiddleware())
```

This uses:

* ordering semantics ([FastMCP][4])
* session-availability guidance (headers even if request_context is None) ([FastMCP][4])
* built-in middleware modules ([FastMCP][6])

---

If you want next, I can translate these into a “best-practice recipe” for your setup specifically (single box + Streamable HTTP + a few LLM clients): which middleware you should run, in what order, and the few footguns to avoid when combining caching + structured logging + custom auth.

[1]: https://gofastmcp.com/servers/elicitation "User Elicitation - FastMCP"
[2]: https://modelcontextprotocol.io/specification/2025-06-18/client/elicitation "Elicitation - Model Context Protocol"
[3]: https://gofastmcp.com/clients/elicitation "User Elicitation - FastMCP"
[4]: https://gofastmcp.com/servers/middleware "MCP Middleware - FastMCP"
[5]: https://gofastmcp.com/python-sdk/fastmcp-server-middleware-timing "timing - FastMCP"
[6]: https://gofastmcp.com/python-sdk/fastmcp-server-middleware-logging "logging - FastMCP"
[7]: https://gofastmcp.com/python-sdk/fastmcp-server-middleware-rate_limiting "rate_limiting - FastMCP"
[8]: https://gofastmcp.com/python-sdk/fastmcp-server-middleware-caching "caching - FastMCP"
[9]: https://gofastmcp.com/python-sdk/fastmcp-server-middleware-error_handling "error_handling - FastMCP"
[10]: https://gofastmcp.com/python-sdk/fastmcp-server-middleware-tool_injection "tool_injection - FastMCP"



# FastMCP Background Tasks, Streamable HTTP vs legacy SSE + `stateless_http`, Client-side “advanced” surface area, CLI power-user modes: `dev`, `inspect`, `install`, proxying, configs #

## 5) Background Tasks + cancellation (SEP-1686) — FastMCP v2.14+

### What you get (full surface area)

**Protocol-level semantics (what MCP clients/servers negotiate)**

* Tasks are a **protocol-native** way to make component interactions non-blocking: start → get `taskId` immediately → poll/receive updates → retrieve result. ([FastMCP][1])
* Capabilities are negotiated during initialization (`capabilities.tasks...`) and tools additionally advertise per-tool `execution.taskSupport` (`forbidden|optional|required`). ([Model Context Protocol][2])
* Task-augmented requests include a `task` field in request params (optionally with `ttl`); the initial response returns **task data**, and the final operation result is retrieved later via `tasks/result`. ([Model Context Protocol][2])
* Status polling is via `tasks/get` until a terminal status (`completed`, `failed`, or `cancelled`). ([Model Context Protocol][2])
* Cancellation is `tasks/cancel`; receivers *may delete cancelled tasks at any time* and requestors shouldn’t rely on retention. ([Model Context Protocol][2])

**FastMCP server-side surface**

* Enable background execution by adding `task=True` to **tools, resources, resource templates, or prompts**; the handler must be `async` (sync registration raises `ValueError`). ([FastMCP][1])
* Fine-grained execution modes via `TaskConfig(mode=...)`:

  * `forbidden`: never background
  * `optional`: runs sync unless client requests background (this is what `task=True` maps to)
  * `required`: client must request background or server errors ([FastMCP][1])
* Server-wide default: `FastMCP(..., tasks=True)` enables task support by default; sync components must explicitly opt out (`task=False`) to avoid registration errors. ([FastMCP][1])
* Backends (Docket-powered):

  * `memory://` default: zero-config, but ephemeral + no horizontal scaling; also higher pickup latency. ([FastMCP][1])
  * `redis://...` (Redis/Valkey): persistent + fast pickup + scalable workers. ([FastMCP][1])
  * Configure backend via `FASTMCP_DOCKET_URL`. ([FastMCP][1])
* Workers:

  * An embedded worker starts automatically when you have task-enabled components.
  * Add more workers with `fastmcp tasks worker server.py` (requires Redis/Valkey backend). ([FastMCP][1])
  * Concurrency via `FASTMCP_DOCKET_CONCURRENCY`. ([FastMCP][1])
* Progress reporting inside tasks via the `Progress` dependency (`set_total`, `increment`, `set_message`). ([FastMCP][1])
* Advanced task composition: inject `CurrentDocket()` / `CurrentWorker()` to enqueue more work or inspect worker metadata. ([FastMCP][1])

**FastMCP client-side surface**

* Request background execution by passing `task=True` to `call_tool`, `read_resource`, or `get_prompt`; you immediately get a Task object. ([FastMCP][3])
* Task object APIs:

  * `await task.result()` (or `await task`) blocks for completion
  * `await task.status()` returns current state + status message
  * `await task.wait(state=..., timeout=...)` for controlled waiting
  * `await task.cancel()` cancels ([FastMCP][3])
  * `task.on_status_change(callback)` for sync/async status update callbacks ([FastMCP][3])
  * `task.returned_immediately` tells you whether the server ran synchronously (graceful degradation). ([FastMCP][3])
* The underlying `Task.cancel()` sends a `tasks/cancel` request; if the server executed immediately, it’s a no-op (nothing to cancel). ([FastMCP][4])

### Minimal implementation (server + client)

**Server: enable tasks + progress + optional required-mode**

```python
import asyncio
from fastmcp import FastMCP
from fastmcp.dependencies import Progress
from fastmcp.server.tasks import TaskConfig

mcp = FastMCP("MyServer")

@mcp.tool(task=True)  # optional background execution
async def slow_computation(duration: int, progress: Progress = Progress()) -> str:
    await progress.set_total(duration)
    for i in range(duration):
        await asyncio.sleep(1)
        await progress.set_message(f"tick {i+1}/{duration}")
        await progress.increment()
    return f"Completed in {duration}s"

@mcp.tool(task=TaskConfig(mode="required"))  # must run as background task
async def must_be_background() -> str:
    return "ok"
```

This is the documented `task=True` / `TaskConfig(mode=...)` pattern, plus the `Progress` dependency API. ([FastMCP][1])

**Client: run task, subscribe, cancel**

```python
import asyncio
from fastmcp import Client

async def main():
    async with Client("http://localhost:8000/mcp") as client:
        task = await client.call_tool("slow_computation", {"duration": 60}, task=True)

        def on_update(status):
            print(status.status, status.statusMessage)
        task.on_status_change(on_update)

        await asyncio.sleep(3)
        await task.cancel()  # sends tasks/cancel

        # await final result (may be cancelled/failed depending on timing)
        status = await task.status()
        print("final:", status.status)

asyncio.run(main())
```

This is the documented “pass `task=True` → Task object → `status()/wait()/cancel()/result()`” flow. ([FastMCP][3])

---

## 6) Streamable HTTP vs legacy SSE + `stateless_http`

### What you get (full surface area)

**Transports in FastMCP**

* `stdio` (default), `http` (recommended; Streamable HTTP protocol), and `sse` (legacy/deprecated). ([FastMCP][5])
* Streamable HTTP is the MCP-standard HTTP transport (single MCP endpoint supporting POST+GET; may use SSE *within* Streamable HTTP when streaming). ([Model Context Protocol][6])
* MCP explicitly describes backward compatibility guidance for the deprecated HTTP+SSE transport (host the old endpoints alongside the new MCP endpoint if you must support old clients). ([Model Context Protocol][6])

**Streamable HTTP “operational knobs” in FastMCP**

* `mcp.http_app(...)` / `create_streamable_http_app(...)` supports:

  * `transport` (`http` / `streamable-http` / `sse`)
  * `stateless_http` (fresh transport context per request)
  * `event_store` + `retry_interval` (SSE polling/resumability)
  * `json_response`, `auth`, `middleware`, `routes`, etc. ([FastMCP][7])

**Sessions vs stateless mode**

* By default, FastMCP Streamable HTTP maintains **server-side sessions**; sessions enable stateful features like elicitation and sampling, but sessions are stored in-memory per instance. ([FastMCP][8])
* In multi-worker / load-balanced setups, you can get “session doesn’t exist” failures when requests from the same client hit different instances. FastMCP notes that sticky sessions don’t reliably work with many MCP clients because they use `fetch()` and don’t properly forward `Set-Cookie`. ([FastMCP][8])
* Enable stateless mode:

  * `FastMCP(..., stateless_http=True)`
  * `mcp.run(..., stateless_http=True)`
  * `FASTMCP_STATELESS_HTTP=true ...` (also required for `uvicorn --workers N` per docs) ([FastMCP][8])

**Long-running HTTP calls: SSE polling / resumability (2.14)**

* Streamable HTTP supports an “SSE polling” pattern (SEP-1699) to avoid load balancer/proxy timeouts: configure an `EventStore`, periodically call `ctx.close_sse_stream()`, clients reconnect with `Last-Event-ID`, and the server replays missed events. ([FastMCP][8])
* This applies to Streamable HTTP transport and **not** the legacy `transport="sse"`. ([FastMCP][8])
* Event store can be in-memory or backed by Redis (`ttl`, `max_events_per_stream`). ([FastMCP][8])

### Minimal implementation (single-box, few clients)

**Recommended default (single box, simplest):** Streamable HTTP with sessions

```python
from fastmcp import FastMCP

mcp = FastMCP("MyServer")

if __name__ == "__main__":
    mcp.run(transport="http", host="127.0.0.1", port=8000, path="/mcp/")
```

FastMCP recommends HTTP for web services and marks SSE as legacy/deprecated. ([FastMCP][5])

**If your clients/proxies terminate idle connections:** add EventStore + `close_sse_stream()`

```python
from fastmcp import FastMCP, Context
from fastmcp.server.event_store import EventStore

mcp = FastMCP("MyServer")

@mcp.tool
async def long_running(ctx: Context) -> str:
    for i in range(100):
        await ctx.report_progress(i, 100)
        if i % 30 == 0 and i > 0:
            await ctx.close_sse_stream()
        # do work...
    return "Done"

app = mcp.http_app(event_store=EventStore(), retry_interval=2000)
```

This is the documented “SSE polling/resumability” pattern (EventStore + close + replay). ([FastMCP][8])

**If you ever go multi-worker or sit behind a load balancer:** set `stateless_http=True`

```python
from fastmcp import FastMCP

mcp = FastMCP("MyServer", stateless_http=True)
app = mcp.http_app()
```

FastMCP explicitly recommends stateless mode for horizontal scaling and explains why cookie-based affinity can fail with MCP clients. ([FastMCP][8])

---

## 7) Client-side “advanced” surface area (what you actually need)

### Client configuration knobs

FastMCP Client supports:

* `transport` (URL / stdio / in-memory / config-driven, etc.)
* `log_handler`, `progress_handler`, `sampling_handler`, `elicitation_handler`
* `roots` (filesystem roots)
* `timeout` and `init_timeout`
* lower-level `message_handler` (raw protocol messages) ([FastMCP][9])

### Transports (including in-memory)

* **STDIO**: client launches the server subprocess and controls its lifecycle; you must manage env passing (e.g., via `StdioTransport(..., env=...)`). ([FastMCP][10])
* **Remote (URL) transport**: connect to an already-running HTTP server (Streamable HTTP). (This is the typical “few LLM consumers on one box” setup.) ([FastMCP][11])
* **In-memory**: `Client(mcp_server_instance)` runs everything in the same Python process (excellent for tests; no env isolation). ([FastMCP][10])

### Handlers you’ll likely want

**Log handler**

* Receives structured log payloads (`msg`, `extra`) + level; integrates cleanly with `logging`. ([FastMCP][12])

**Progress handler**

* Signature: `(progress: float, total: float | None, message: str | None)`; can be set on the client or overridden per-call. ([FastMCP][13])

**Sampling handler**

* Signature: `(messages, params, context) -> str`
* Receives `SamplingParams` including optional model preferences (hints, cost/speed/intelligence priorities) and optional `systemPrompt`. ([FastMCP][14])

**Elicitation handler**

* FastMCP converts server JSON schema → Python dataclass type; your handler can return the dataclass directly (implicit accept) or an `ElicitResult(action=..., content=...)`. ([FastMCP][15])

### Client task APIs (how you avoid long-held HTTP requests)

* `await client.call_tool(..., task=True)` returns a Task object; you can `await task.status() / wait() / cancel() / result()` and register status callbacks. ([FastMCP][3])
* At the API level, task calls also support `task_id` and `ttl` (default 60s) for tools/resources/prompts. ([FastMCP][11])

---

## 8) CLI power-user modes: `dev`, `inspect`, `install`, proxying, configs

### `fastmcp run` (production-ish local runner + proxy mode)

Entry points you can run:

* **Inferred instance**: file contains `mcp` / `server` / `app`. ([FastMCP][16])
* **Explicit instance**: `server.py:custom_name`. ([FastMCP][16])
* **Factory function**: `server.py:create_server` (async factory supported). ([FastMCP][16])
* **Remote server proxy**: `fastmcp run https://example.com/mcp` starts a local proxy connected to a remote server. ([FastMCP][16])
* **Config-driven**:

  * `fastmcp run` auto-detects `fastmcp.json` (FastMCP declarative config). ([FastMCP][16])
  * `fastmcp run mcp.json` runs as a proxy for servers in MCPConfig. ([FastMCP][17])

Dependency handling: `run` can use your environment directly or run via `uv run` with dependency flags; with `fastmcp.json`, it can manage dependencies from config. ([FastMCP][16])

### `fastmcp dev` (Inspector shortcut)

* Runs a server with the MCP Inspector for testing, always via `uv run`. ([FastMCP][16])
* It’s explicitly a **STDIO-only** testing shortcut; for HTTP/Streamable HTTP testing, you start the server yourself and connect the Inspector separately. ([FastMCP][16])

### `fastmcp install` (client install flows)

* Installs a local server into a target client config (examples show `claude-desktop`).
* Supports local files and `fastmcp.json` (not URLs / remote servers / generic MCP config files). ([FastMCP][16])

### `fastmcp inspect` (manifest + introspection)

* Produces either:

  * **FastMCP format**: richest (tags, enabled status, output schemas, annotations, versions). ([FastMCP][16])
  * **MCP format**: what standard MCP clients see (protocol fields only). ([FastMCP][16])
* Useful for “what will the client see?” debugging and for generating JSON reports (`-o`). ([FastMCP][16])

### `fastmcp project prepare` (fast reproducible envs)

* Turns a `fastmcp.json` environment into a persistent `uv` project dir (`pyproject.toml`, `.venv`, `uv.lock`) so subsequent `fastmcp run ... --project ...` is fast and reproducible. ([FastMCP][16])

[1]: https://gofastmcp.com/servers/tasks "Background Tasks - FastMCP"
[2]: https://modelcontextprotocol.io/specification/draft/basic/utilities/tasks "Tasks - Model Context Protocol"
[3]: https://gofastmcp.com/clients/tasks "Background Tasks - FastMCP"
[4]: https://gofastmcp.com/python-sdk/fastmcp-client-tasks "tasks - FastMCP"
[5]: https://gofastmcp.com/servers/server "The FastMCP Server - FastMCP"
[6]: https://modelcontextprotocol.io/specification/2025-03-26/basic/transports "Transports - Model Context Protocol"
[7]: https://gofastmcp.com/python-sdk/fastmcp-server-http "http - FastMCP"
[8]: https://gofastmcp.com/deployment/http "HTTP Deployment - FastMCP"
[9]: https://gofastmcp.com/clients/client "The FastMCP Client - FastMCP"
[10]: https://gofastmcp.com/clients/transports "Client Transports - FastMCP"
[11]: https://gofastmcp.com/python-sdk/fastmcp-client-client "client - FastMCP"
[12]: https://gofastmcp.com/clients/logging "Server Logging - FastMCP"
[13]: https://gofastmcp.com/clients/progress "Progress Monitoring - FastMCP"
[14]: https://gofastmcp.com/clients/sampling "LLM Sampling - FastMCP"
[15]: https://gofastmcp.com/clients/elicitation "User Elicitation - FastMCP"
[16]: https://gofastmcp.com/patterns/cli "FastMCP CLI - FastMCP"
[17]: https://gofastmcp.com/python-sdk/fastmcp-cli-cli "cli - FastMCP"

