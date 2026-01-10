Below is a **single‑source “operator’s manual”** for the **`mcp` Python library** (the official Model Context Protocol SDK) and its **`mcp[cli]`** extras. It’s written for an AI programming agent that must implement best‑in‑class MCP clients and servers today, aligned with the **MCP specification v2025‑06‑18**. Where the SDK or protocol changed recently (e.g., streamable HTTP, structured tool output, elicitation, OAuth), this guide calls it out explicitly and links to the primary sources.

> **What you’re building, conceptually**
> MCP is a JSON‑RPC based protocol that lets an LLM host (client) talk to an **MCP server** exposing: **Resources** (read/subscribe), **Tools** (call with JSON input → content/structured output), and **Prompts** (named message templates). Clients may also support **Roots**, **Sampling** (server → client LLM calls), and **Elicitation** (server → client asks the human for input). The official Python SDK (“`mcp`”) implements **both client and server stacks**, plus a higher‑level **FastMCP** server framework and a small **CLI** for local dev. ([Model Context Protocol][1])

---

## 0) Install & versioning

### Packages

* **SDK + CLI tools** (recommended for dev):

  ```bash
  uv add "mcp[cli]"    # or:  pip install "mcp[cli]"
  uv run mcp           # launches the CLI entrypoint
  ```

  The `[cli]` extra installs a `mcp` console script with subcommands such as `dev`, `install`, and `run`. ([GitHub][2])

* **Bare SDK only** (if you don’t want the CLI):

  ```bash
  uv add mcp    # or: pip install mcp
  ```

  See the PyPI page for dependencies and versions (e.g., anyio, httpx, starlette, uvicorn). ([PyPI][3])

### Spec alignment (as of **2025‑06‑18**)

The SDK tracks the **MCP 2025‑06‑18** spec (latest), which introduced: **Streamable HTTP** transport as the preferred remote transport, **structured tool output** (`outputSchema` + structured result), **Elicitation**, and **OAuth‑based authorization** guidance. Always check the spec’s “Key Changes”. ([Model Context Protocol][4])

---

## 1) The moving parts (SDK mental model)

**Transports**

* **stdio**: local process → lowest latency, for dev/desktop/CLI. ([Model Context Protocol][5])
* **Streamable HTTP**: single HTTP endpoint (optionally uses SSE for multi‑message responses). **Preferred for remote servers**; supersedes legacy HTTP+SSE. ([Model Context Protocol][6])
* (Legacy) **HTTP+SSE**: supported for backward compatibility. ([Model Context Protocol][7])

**Server features** (what servers expose): **Tools**, **Resources**, **Prompts**. **Servers must declare capabilities** in `initialize`. The spec defines listing operations, pagination, and notifications (`list_changed`). ([Model Context Protocol][8])

**Client features**: **Roots** (client tells a server which file areas are in scope), **Sampling** (server asks the client/host to call the model), **Elicitation** (server asks the user for info). ([Model Context Protocol][9])

**Lifecycle**: Client connects → `initialize` (capability negotiation) → list/use features → close. The Python API maps to these RPCs. ([Model Context Protocol][5])

---

## 2) FastMCP: the high‑level server framework you’ll usually use

**FastMCP** auto‑derives JSON Schemas from type hints and docstrings, and wires everything to the protocol. Minimal server:

```python
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("Demo")

@mcp.tool()
def add(a: int, b: int) -> int:
    """Add two numbers"""
    return a + b

@mcp.resource("greeting://{name}")
def greeting(name: str) -> str:
    """A dynamic greeting resource"""
    return f"Hello, {name}!"

@mcp.prompt()
def greet_user(name: str, style: str = "friendly") -> str:
    """Return a prompt template"""
    styles = {"friendly":"Write a warm, friendly greeting"}
    return f"{styles.get(style, styles['friendly'])} for {name}."

if __name__ == "__main__":
    mcp.run(transport="stdio")  # or "streamable-http"
```

FastMCP turns those decorators into MCP definitions and handlers automatically. ([GitHub][2])

**Context injection**
If your tool/resource/prompt function requests a `Context[...]` parameter, FastMCP injects an object giving you:

* **Logging** back to client (`ctx.debug/info/warning/error`)
* **Progress** (`await ctx.report_progress(cur, total)`)
* **LLM sampling** via client (`await ctx.sample(...)`)
* **Resource access** (`await ctx.read_resource(uri)`)
* **Request metadata** (IDs; access to underlying session)
  These surface the spec’s logging/progress/LLM features cleanly. ([GitHub][10])

> **Don’t print to stdout** in stdio servers: it corrupts JSON‑RPC. Log to stderr (e.g., `logging`) or use the context logging helpers. ([Model Context Protocol][11])

**Run modes**

* **Local dev** (stdio) or **streamable‑http**:

  ```python
  if __name__ == "__main__":
      mcp.run(transport="streamable-http")    # networked endpoint
  ```

  Streamable HTTP supports stateful or stateless modes, JSON or SSE responses, resumability, and Starlette mounting (multiple servers under one app). ([GitHub][2])

**Mounting multiple servers (ASGI)**
You can host several FastMCP servers behind a Starlette app (e.g., `/echo/mcp`, `/math/mcp`) and manage lifespans for each. ([GitHub][2])

---

## 3) Low‑level server: full protocol control

If you need to control **every** RPC and capability (custom pagination, precise output validation, custom notifications), use `mcp.server.lowlevel.Server`. You register handlers (`@server.list_tools`, `@server.call_tool`, etc.) and run via stdio or streamable HTTP.

Key points:

* **Structured tool output**: define `outputSchema` and return structured data (dict). The server validates it and (for backward compatibility) can also serialize to text content. You may also return `(content, structured)` or a full `CallToolResult`. ([GitHub][2])
* **Caveat**: `mcp dev`/`mcp run` only support FastMCP (not low‑level) for launch convenience. ([GitHub][2])

---

## 4) The Client APIs (Python)

Use `ClientSession` with a transport to **call tools**, **read/subscribe resources**, and **use prompts**.

**stdio client** example:

```python
import asyncio
from contextlib import AsyncExitStack
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

async def main():
    params = StdioServerParameters(command="python", args=["server.py"])
    async with AsyncExitStack() as stack:
        stdio = await stack.enter_async_context(stdio_client(params))
        read, write = stdio
        session = await stack.enter_async_context(ClientSession(read, write))
        await session.initialize()

        tools = (await session.list_tools()).tools
        result = await session.call_tool(name=tools[0].name, arguments={})
        print(result.content)

asyncio.run(main())
```

This mirrors the docs “Build a client” tutorial and the SDK’s `ClientSession` API. ([Model Context Protocol][12])

**Multi‑server**: `ClientSessionGroup` lets you connect to several servers and route calls; handy when multiplexing tool catalogs. ([Model Context Protocol][13])

**API surface (mapping to RPCs)**
Below are the **most important** methods you’ll use; each maps 1:1 with spec endpoints:

| Feature                | Client call(s)                                                                                                                | Server callback(s)                                                     | Notes                                                                                                                            |
| ---------------------- | ----------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| **Tools**              | `list_tools()`, `call_tool(name, arguments)`                                                                                  | `@list_tools`, `@call_tool`                                            | Supports **structured output** via `outputSchema`; **list_changed** notifications. ([Model Context Protocol][8])                 |
| **Resources**          | `list_resources()`, `list_resource_templates()`, `read_resource(uri)`, `subscribe_resource(uri)`, `unsubscribe_resource(uri)` | `@list_resources`, `@read_resource`, (server emits `resource/updated`) | Resources are addressable **URIs** with optional **subscribe** and **list_changed** capabilities. ([Model Context Protocol][13]) |
| **Prompts**            | `list_prompts()`, `get_prompt(name, arguments)`                                                                               | `@list_prompts`, `@get_prompt`                                         | Arguments can be **auto‑completed** via `complete(...)`. ([Model Context Protocol][14])                                          |
| **Completions**        | `complete(...)`                                                                                                               | (client capability)                                                    | Used for prompt argument completion. ([Model Context Protocol][13])                                                              |
| **Roots**              | `send_roots_list_changed(...)` (client → server)                                                                              | `ServerSession.list_roots()` (server asks client)                      | Defines filesystem boundaries; servers should request roots if supported. ([Model Context Protocol][9])                          |
| **Sampling**           | (server → client) `ServerSession.create_message(...)`                                                                         | client handles model call; returns `CreateMessageResult`               | Lets tools call the model mid‑flight. ([Model Context Protocol][15])                                                             |
| **Elicitation**        | (server → client) `ServerSession.elicit(...)`                                                                                 | client prompts user, returns values                                    | Structured user input mid‑session. ([Model Context Protocol][16])                                                                |
| **Logging / Progress** | `set_logging_level`, `send_progress_notification`                                                                             | `ServerSession.send_log_message`, `send_progress_notification`         | Surfaces to the host UI. ([Model Context Protocol][13])                                                                          |
| **Errors**             | `McpError`                                                                                                                    |                                                                        | JSON‑RPC error data types documented in API ref. ([Model Context Protocol][13])                                                  |

**Content types**: Tools/Prompts can return **text**, **image**, **audio**, **resource links**, and **embedded resources**; tools may include **structuredContent** (new) validated by `outputSchema`. ([Model Context Protocol][8])

**Pagination**: list operations may return `nextCursor`; call again with `cursor` to continue. ([Model Context Protocol][8])

---

## 5) `mcp[cli]`: development extras

Installing `mcp[cli]` provides a `mcp` command with subcommands:

* **`mcp dev server.py`** — Launch your FastMCP server alongside the **MCP Inspector** web UI for visual testing. Pass extra deps or editable installs with `--with numpy --with pandas --with-editable .`.

  ```bash
  uv run mcp dev server.py --with numpy --with-editable .
  ```

  (Inspector is a **dev tool only**; don’t use in production.) ([GitHub][2])

* **`mcp install server.py`** — Install the server into **Claude Desktop** (or other hosts that read the MCP manifest). Accepts overrides:

  ```bash
  uv run mcp install server.py --name "My Analytics Server" \
    -v API_KEY=abc123 -v DB_URL=postgres://... -f .env
  ```

  ([GitHub][2])

* **`mcp run server.py`** — Run a FastMCP server directly (stdio). (Note: **low‑level** servers are **not** supported by `mcp run/dev`.) ([GitHub][2])

> You can also run the **Inspector** standalone (without the Python CLI) using the Node package:
> `npx @modelcontextprotocol/inspector` and point it at a remote Streamable‑HTTP URL. ([Cloudflare Docs][17])

---

## 6) Transports & deployment

**When to choose what**

* **stdio** → local tools/desktop integrations; lowest latency.
* **streamable‑http** → remote, multi‑tenant, scalable deployments; **recommended** for production and new integrations.
  Streamable HTTP provides bidirectional messaging over a single endpoint, with optional SSE, and supports stateful/stateless modes plus resumability. ([Model Context Protocol][6])

**FastMCP configs you’ll actually use**

* `mcp.run(transport="streamable-http")` for a self‑hosted endpoint.
* `FastMCP(..., stateless_http=True)` for stateless operation (no session persistence).
* `json_response=True` when you want plain JSON responses without SSE (for some browser clients/proxies).
* **CORS**: configure for browser clients (doc section: CORS for browser‑based clients).
* **Mount multiple servers** behind Starlette (host/path routing), e.g., `/echo/mcp` and `/math/mcp`. ([GitHub][2])

Cloudflare and Spring AI document Streamable HTTP deployment patterns (networking, SSE fallback). ([Home][18])

---

## 7) Tools (server) — deep details you’ll need

**Definition**:
`name`, `description`, optional `title`, **`inputSchema`**, optional **`outputSchema`**, **`icons`**, `annotations`, and `meta`. Tools are listed with `tools/list` and invoked with `tools/call`. Servers may notify `notifications/tools/list_changed`. ([Model Context Protocol][8])

**Outputs (2025‑06‑18)**

* Return **content** (text/images/etc.).
* Return **structured data** (dict validated by `outputSchema`).
* Or return **both** (tuple `(content, structured)`), or a full `CallToolResult` to control `_meta`.
  FastMCP/low‑level helpers validate structured output and keep backward compatibility with pre‑2025‑06‑18 hosts. ([GitHub][2])

**Attachments & links**
A tool can return **resource links** (URIs) or **embedded resources** with mime types; clients may fetch/subscribe to those. ([Model Context Protocol][8])

**Robustness patterns**

* Validate input with JSON Schema (out of the box).
* Prefer **deterministic** schemas and **idempotent** operations where possible.
* Emit **progress** for long tasks and **log** meaningful milestones (surface to host UI). ([Model Context Protocol][13])

---

## 8) Resources (server)

Resources are **URI‑addressable**, optionally subscribable. The flow:

* `resources/list` (with optional pagination)
* `resources/read` (returns content)
* `resources/subscribe` / `resources/unsubscribe`
* Server sends `notifications/resources/updated` when content changes
* Optional `notifications/resources/list_changed` if the catalog changes. ([Model Context Protocol][13])

Use dynamic templates (e.g., `greeting://{name}`) in FastMCP to parameterize resources. ([GitHub][2])

---

## 9) Prompts (server)

Prompts are **named templates** exposed by servers for user‑driven workflows.

* `prompts/list`, `prompts/get(name, arguments)`
* Optional `notifications/prompts/list_changed`
* Client may call `complete(...)` to autocomplete arguments. ([Model Context Protocol][14])

---

## 10) Client features your server can rely on

* **Roots**: Clients declare filesystem roots; your server can **request** roots (`ServerSession.list_roots()`), and clients may notify with `roots/list_changed`. Never assume global filesystem access; restrict to roots. ([Model Context Protocol][9])
* **Sampling**: Inside tool logic, request a model completion via the client (`create_message`); the host actually talks to the LLM and returns a `CreateMessageResult`. Use for **agentic sub‑calls**. ([Model Context Protocol][15])
* **Elicitation**: Ask the user for structured input mid‑flow (`elicit`). Provide a schema and helpful prompt text. ([Model Context Protocol][16])

---

## 11) Authorization & security (production guidance)

* **HTTP servers** should follow the spec’s **Authorization** doc (tokens bound via RFC 8707 “resource” parameter, OIDC/OAuth patterns; server acts as an OAuth **resource server**). ([Model Context Protocol][19])
* Review the spec’s **Security Best Practices** (threats, mitigations, isolation). ([Model Context Protocol][20])
* **Inspector is for development only.** Don’t expose it publicly; AWS’s guidance reiterates this. ([AWS Documentation][21])
* **_stdio servers**: **never write to stdout** (use stderr or SDK logging). ([Model Context Protocol][11])
* Prefer **streamable HTTP** with proper CORS and auth for remote access. ([Model Context Protocol][6])

> Security note: recent research and disclosures have highlighted risks when dev tools (e.g., Inspector proxies) are left exposed; always gate dev tooling and rotate credentials. ([Oligo Security][22])

---

## 12) Host & framework integrations (for an AI agent to target)

* **Claude Desktop**: `uv run mcp install server.py` registers your server for use. ([GitHub][2])
* **OpenAI (ChatGPT Connectors / Deep Research)**: when building servers for OpenAI’s hosts, implement the required **`search`** and **`fetch`** tools as documented. ([OpenAI Platform][23])
* **LangChain/LangGraph**: adapters exist to load MCP tools as LangChain tools. ([Socket][24])
* **Cloudflare Agents**: documented patterns for testing & remote Streamable HTTP. ([Cloudflare Docs][17])

---

## 13) End‑to‑end development loop (recommended)

1. **Scaffold server** (FastMCP), define tools/resources/prompts with type‑hints and docstrings. ([GitHub][2])
2. **Run in dev** with **Inspector**:
   `uv run mcp dev server.py --with <deps> --with-editable .` ([GitHub][2])
3. Switch transports: **stdio** locally; **streamable‑http** for remote tests. ([GitHub][2])
4. Add **structured output** where the host benefits from typed data. ([GitHub][2])
5. Add **sampling** and **elicitation** for advanced flows (agentic loops, user prompts). ([Model Context Protocol][15])
6. Harden with **OAuth**, CORS, rate limits; ship behind a standard web stack. ([Model Context Protocol][19])

---

## 14) “Sharp edges” & pitfalls (and how to avoid them)

* **`mcp dev`/`mcp run` don’t launch low‑level servers**—use FastMCP for those commands, or run low‑level servers directly (uvicorn/stdio). ([GitHub][2])
* **stdout** on stdio transport breaks JSON‑RPC; use logging to stderr or SDK logging. ([Model Context Protocol][11])
* **Pagination**: consume `nextCursor` on list endpoints or you’ll miss items. ([Model Context Protocol][8])
* **CORS**: configure explicitly for browser‑based clients with streamable HTTP. ([GitHub][2])
* **Inspector ≠ prod**: never expose dev tools publicly. ([AWS Documentation][21])

---

## 15) Minimal templates you can copy

**A. FastMCP (stdio) with context, progress, and structured output**

```python
from typing import TypedDict
from mcp.server.fastmcp import FastMCP, Context
from mcp.server.session import ServerSession

class SumOut(TypedDict):
    total: int

mcp = FastMCP("Calc")

@mcp.tool(output_schema=SumOut)  # optional; inferred if using TypedDict/PEP 563
async def sum_many(nums: list[int], ctx: Context[ServerSession, None]) -> SumOut:
    total = 0
    for i, n in enumerate(nums):
        total += n
        if (i+1) % 100 == 0:
            await ctx.report_progress(i+1, len(nums))
    await ctx.info(f"Summed {len(nums)} numbers")
    return {"total": total}

if __name__ == "__main__":
    mcp.run(transport="stdio")
```

(Shows context logging/progress + structured output validation.) ([GitHub][2])

**B. FastMCP (streamable HTTP) mounted under Starlette with two servers**

* Mount servers at `/echo/mcp` and `/math/mcp` and run via uvicorn; see repo examples for full code. ([GitHub][2])

**C. Client (stdio) that lists tools and calls one**

* See the earlier `ClientSession` snippet and the “Build a client” tutorial. ([Model Context Protocol][12])

---

## 16) API reference map (what to call when)

**Client side** (`mcp.ClientSession`):
`initialize()`, `get_server_capabilities()`, `list_tools()`, `call_tool()`, `list_resources()`, `list_resource_templates()`, `read_resource()`, `subscribe_resource()`, `unsubscribe_resource()`, `list_prompts()`, `get_prompt()`, `complete()`, `send_roots_list_changed()`, `set_logging_level()`, `send_progress_notification()`. ([Model Context Protocol][13])

**Server side** (`mcp.server.fastmcp` **or** `mcp.server.lowlevel.Server`):
Decorators/handlers for `list_*`, `get_prompt`, `call_tool`; lifecycle via `mcp.run(...)`; server can **emit** `send_log_message`, `send_progress_notification`, `send_resource_updated`, `send_*_list_changed`, and **request** `create_message` (sampling) or `elicit`. ([Model Context Protocol][13])

---

## 17) Testing & debugging

* Use **MCP Inspector** (from `mcp dev` or `npx @modelcontextprotocol/inspector`) to:
  list tools/resources/prompts, run tools with JSON inputs, preview prompt messages, watch logs and notifications. ([Model Context Protocol][25])

---

## 18) Cross‑host compatibility & future‑proofing

* Implement **structured outputs** (helps all hosts/agents). ([GitHub][2])
* Prefer **streamable HTTP** for networked deployments. ([The Cloudflare Blog][26])
* Follow **OAuth guidance** in 2025‑06‑18 spec for remote servers; bind tokens to the **resource** (server) per RFC 8707 resource indicators. ([Model Context Protocol][19])

---

## 19) How this plugs into your architecture

The patterns above fit cleanly with the service boundaries and data‑flow described in your high‑level system design; e.g., running Streamable‑HTTP MCP servers behind your gateway and exposing only the minimal tool/resource surface area per domain. 

---

### Canonical resources (keep these open while implementing)

* **Python SDK README & examples** (everything from install to Streamable HTTP, Starlette mounting, low‑level server, OAuth, pagination, etc.). ([GitHub][2])
* **Python SDK API reference** (method‑level docs for `ClientSession`, `ClientSessionGroup`, `ServerSession`, error types, capabilities). ([Model Context Protocol][13])
* **Spec (2025‑06‑18)** — Tools, Prompts, Resources, Transports, Authorization, Sampling, Elicitation, Roots. ([Model Context Protocol][8])
* **Quickstart tutorials** — Build a server/client; logging do’s and don’ts for stdio. ([Model Context Protocol][11])
* **Inspector** — Dev‑only visual tester. ([Model Context Protocol][25])

---

## 20) Quick checklists (for an autonomous agent)

**Server (prod)**

* [ ] Streamable HTTP, CORS, OAuth (resource‑bound tokens) configured. ([Model Context Protocol][19])
* [ ] Tools define **inputSchema** & **outputSchema**; return structured data where useful. ([Model Context Protocol][8])
* [ ] No `print()` to stdout (stdio). Logs/progress use SDK facilities. ([Model Context Protocol][11])
* [ ] Resources use URIs; subscriptions send `resource/updated` notifications. ([Model Context Protocol][13])
* [ ] Prompts expose arguments; support `complete(...)`. ([Model Context Protocol][14])

**Client**

* [ ] After `initialize`, **always** call `list_tools()` (tool catalogs can change).
* [ ] Respect `nextCursor` for list pagination. ([Model Context Protocol][8])
* [ ] Implement **roots** if file operations are in scope. ([Model Context Protocol][9])
* [ ] Handle sampling/elicitation requests. ([Model Context Protocol][15])

---

If you follow the patterns above, you’ll implement **modern MCP** in Python with the SDK’s happy‑path (FastMCP) and still have the **low‑level APIs** when you need total control. The citations above point to the canonical spec, SDK docs, and examples for each feature and command so you can execute autonomously.

[1]: https://modelcontextprotocol.io/specification/2025-06-18?utm_source=chatgpt.com "Specification - Model Context Protocol"
[2]: https://github.com/modelcontextprotocol/python-sdk "GitHub - modelcontextprotocol/python-sdk: The official Python SDK for Model Context Protocol servers and clients"
[3]: https://pypi.org/project/mcp/?utm_source=chatgpt.com "mcp"
[4]: https://modelcontextprotocol.io/specification/2025-06-18/changelog?utm_source=chatgpt.com "Key Changes - Model Context Protocol"
[5]: https://modelcontextprotocol.io/specification/2025-06-18/basic?utm_source=chatgpt.com "Overview"
[6]: https://modelcontextprotocol.io/specification/2025-03-26/basic/transports?utm_source=chatgpt.com "Transports"
[7]: https://modelcontextprotocol.io/specification/2025-06-18/basic/transports?utm_source=chatgpt.com "Transports"
[8]: https://modelcontextprotocol.io/specification/2025-06-18/server/tools "Tools - Model Context Protocol"
[9]: https://modelcontextprotocol.io/specification/2025-06-18/client/roots?utm_source=chatgpt.com "Roots"
[10]: https://github.com/modelcontextprotocol/python-sdk?utm_source=chatgpt.com "The official Python SDK for Model Context Protocol servers ..."
[11]: https://modelcontextprotocol.io/docs/develop/build-server "Build an MCP server - Model Context Protocol"
[12]: https://modelcontextprotocol.io/docs/develop/build-client "Build an MCP client - Model Context Protocol"
[13]: https://modelcontextprotocol.github.io/python-sdk/api/ "API Reference - MCP Server"
[14]: https://modelcontextprotocol.io/specification/2025-06-18/server/prompts "Prompts - Model Context Protocol"
[15]: https://modelcontextprotocol.io/specification/2025-06-18/client/sampling?utm_source=chatgpt.com "Sampling"
[16]: https://modelcontextprotocol.io/specification/2025-06-18/client/elicitation?utm_source=chatgpt.com "Elicitation"
[17]: https://developers.cloudflare.com/agents/guides/test-remote-mcp-server/?utm_source=chatgpt.com "Test a Remote MCP Server"
[18]: https://docs.spring.io/spring-ai/reference/1.1/api/mcp/mcp-streamable-http-server-boot-starter-docs.html?utm_source=chatgpt.com "Streamable-HTTP MCP Servers - Spring"
[19]: https://modelcontextprotocol.io/specification/2025-06-18/basic/authorization?utm_source=chatgpt.com "Authorization"
[20]: https://modelcontextprotocol.io/specification/draft/basic/security_best_practices?utm_source=chatgpt.com "Security Best Practices"
[21]: https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/gateway-using-inspector.html?utm_source=chatgpt.com "Use the MCP Inspector - Amazon Bedrock AgentCore"
[22]: https://www.oligo.security/blog/critical-rce-vulnerability-in-anthropic-mcp-inspector-cve-2025-49596?utm_source=chatgpt.com "Critical RCE in Anthropic MCP Inspector (CVE-2025-49596 ..."
[23]: https://platform.openai.com/docs/mcp?utm_source=chatgpt.com "Building MCP servers for ChatGPT and API integrations"
[24]: https://socket.dev/pypi/package/langchain-mcp-adapters?utm_source=chatgpt.com "langchain-mcp-adapters - PyPI Package Security Analysis - So..."
[25]: https://modelcontextprotocol.io/docs/tools/inspector?utm_source=chatgpt.com "MCP Inspector"
[26]: https://blog.cloudflare.com/streamable-http-mcp-servers-python/?utm_source=chatgpt.com "Bringing streamable HTTP transport and Python language ..."
