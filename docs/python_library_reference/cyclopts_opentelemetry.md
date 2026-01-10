For a Cyclopts-driven CLI, the cleanest “best practice” pattern is: **treat each CLI invocation like a request**, and put **one observability wrapper around the App’s parse→dispatch→exit lifecycle**, using OpenTelemetry for traces/metrics/log correlation, and Cyclopts’ own seams for introspection + injection.

## 1) Use OpenTelemetry as the telemetry “spine”

OpenTelemetry gives you one coherent model for **traces, metrics, and (increasingly) logs**, and the “collector” pattern keeps your CLI code from caring whether you ship to Jaeger/Tempo/Honeycomb/Datadog/etc. ([OpenTelemetry][1])

For log correlation, the Python OTel logging integration can inject trace/span context into stdlib `logging` records automatically, so logs and traces line up without you manually threading IDs everywhere. ([OpenTelemetry Python Contrib][2])

## 2) Cyclopts gives you the exact seams you want

Cyclopts has two particularly useful entry points for observability:

### A) Parse-time introspection (no monkeypatching)

`App.parse_args(...)` returns **(command_callable, BoundArguments, ignored_parse_false_mapping)**, and you can disable `sys.exit` on parse errors via `exit_on_error=False`. ([Cyclopts][3])

That means you can:

* start a span **before parsing**
* capture command identity + normalized args **after parsing**
* handle parse errors as exceptions (and emit telemetry) instead of dying inside Cyclopts

### B) Context injection into commands (Cyclopts-specific superpower)

`parse_known_args`/`parse_args` surfaces an `ignored` mapping: **python variable name → annotated type** for any parameter annotated with `parse=False` (intended to simplify meta apps). ([Cyclopts][3])

You can exploit this to inject a **RunContext** object (logger, tracer, invocation_id, config, etc.) into *every* command without adding CLI flags.

### C) Config hook (optional but great for “telemetry config”)

`App.config` can be a function (or list) that runs **after parsing tokens/environment variables but before conversion/validation**, and receives `(apps, commands, arguments)` for in-place modification. ([Cyclopts][3])
This is useful when you want “telemetry defaults” to be driven by config files/env (e.g., enable/disable, sampling, endpoints) without plumbing it through every command.

## 3) Concrete “best means” implementation: a Telemetry Runner

Implement **one** wrapper that owns:

* root span + invocation id
* safe argument capture (scrub secrets)
* metrics emission
* consistent exit-code mapping
* flushing exporters on exit

### Minimal skeleton (with RunContext injection)

```python
from __future__ import annotations

from dataclasses import dataclass
import logging
import sys
import time
import uuid
from typing import Any, Mapping

from cyclopts import App, Parameter
from cyclopts.exceptions import CycloptsError
from typing import Annotated

# --- your shared context injected into commands ---
@dataclass(frozen=True)
class RunContext:
    invocation_id: str
    command_chain: tuple[str, ...]
    start_ns: int
    logger: logging.Logger
    # tracer/meter handles could live here (OpenTelemetry, etc.)
    # tracer: trace.Tracer
    # meter: metrics.Meter


def _default_exit_code_from_result(result: Any) -> int:
    """
    Match Cyclopts' default "print_non_int_sys_exit" semantics:
      - str: print, exit 0
      - int: exit that int
      - bool: True->0, False->1
      - None: 0
    (Cyclopts docs describe this default behavior.) 
    """
    # Cyclopts default behavior: print strings, sys.exit with appropriate code. :contentReference[oaicite:5]{index=5}
    if result is None:
        return 0
    if isinstance(result, bool):
        return 0 if result else 1
    if isinstance(result, int):
        return result
    if isinstance(result, str):
        print(result)
        return 0
    # non-int, non-str return: treat as success but make it visible
    print(result)
    return 0


def run_with_telemetry(app: App, argv: list[str] | None = None) -> None:
    """
    Single entrypoint that:
      1) parses
      2) injects RunContext into parse=False params
      3) executes command
      4) emits telemetry + exits
    """
    start_ns = time.time_ns()
    invocation_id = uuid.uuid4().hex

    logger = logging.getLogger("cli")
    logger.info("cli.start", extra={"invocation_id": invocation_id})

    exit_code = 1
    try:
        # Parse without sys.exit on errors so we can emit telemetry.
        command, bound, ignored = app.parse_args(
            argv,
            exit_on_error=False,   # Cyclopts can raise instead of sys.exit(1). :contentReference[oaicite:6]{index=6}
            print_error=True,
        )

        # Determine command chain for tags/attributes (use parse_commands if you want exact tokens/apps).
        command_chain = tuple(getattr(command, "__name__", "<unknown>").split("."))

        ctx = RunContext(
            invocation_id=invocation_id,
            command_chain=command_chain,
            start_ns=start_ns,
            logger=logger,
        )

        # Inject any parse=False parameters (e.g., ctx) into bound.arguments.
        # Cyclopts returns `ignored` for parse=False params for meta-app style injection. :contentReference[oaicite:7]{index=7}
        for name, typ in (ignored or {}).items():
            if typ is RunContext:
                bound.arguments[name] = ctx

        t0 = time.perf_counter()
        result = command(*bound.args, **bound.kwargs)
        dt = time.perf_counter() - t0

        exit_code = _default_exit_code_from_result(result)
        logger.info(
            "cli.finish",
            extra={
                "invocation_id": invocation_id,
                "exit_code": exit_code,
                "duration_s": dt,
                "command": getattr(command, "__name__", "<unknown>"),
            },
        )

    except CycloptsError as e:
        # parse/validation errors (and other cyclopts runtime errors)
        logger.exception("cli.cyclopts_error", extra={"invocation_id": invocation_id})
        exit_code = 2
    except Exception:
        logger.exception("cli.command_error", extra={"invocation_id": invocation_id})
        exit_code = 1
    finally:
        # flush telemetry exporters here if using OpenTelemetry BatchSpanProcessor, etc.
        raise SystemExit(exit_code)


# --- Example command using injected ctx (parse=False) ---
app = App()

@app.command
def build(
    ctx: Annotated[RunContext, Parameter(parse=False)],  # parse=False => injected, not CLI-supplied
    target: str,
) -> int:
    ctx.logger.info("build.begin", extra={"invocation_id": ctx.invocation_id, "target": target})
    # do work...
    return 0
```

Why this pattern is “best” for Cyclopts:

* It uses **official Cyclopts APIs** (`parse_args`, `parse=False` injection) instead of fragile wrapping of internal dispatch. ([Cyclopts][3])
* It guarantees telemetry for **success, command exceptions, and parse/validation failures** (which are otherwise easy to miss when `sys.exit` happens inside the framework). ([Cyclopts][3])
* It can preserve Cyclopts’ default return/exit semantics (documented as default behavior) while still giving you a single place to emit telemetry. ([Cyclopts][4])

## 4) What telemetry to capture (high leverage, low regret)

At minimum, record these as span attributes / log fields / metric labels:

* `cli.invocation_id` (UUID)
* `cli.command` and `cli.command_chain`
* `cli.exit_code`
* `cli.duration_ms` (histogram)
* `cli.error.type` and `cli.error.is_parse_error`
* app version + build SHA (if you have it)

Be careful with arguments:

* **Do not** blindly log `bound.arguments`—CLI args frequently contain tokens/paths/PII.
* Prefer: (a) count of args, (b) names only, (c) allowlist safe args, (d) explicit “sensitive” marking.

## 5) “Other observability” you get almost for free with Cyclopts

* Turn parse errors into exceptions in dev/test by using `exit_on_error=False` (and keep rich formatting via `print_error=True`). ([Cyclopts][5])
* Use `help_on_error` and `verbose` flags strategically for operator friendliness vs developer detail. ([Cyclopts][5])
* Use `App.config` to inject defaults from config/env before conversion/validation (useful for telemetry enablement/sampling endpoints). ([Cyclopts][3])

---

If you tell me where you want telemetry to land (OTLP collector? stdout JSON? a SaaS endpoint?), I can tighten the above into a drop-in `codeintel/observability/cli.py` module with: OTel setup, log correlation, arg scrubbing helpers, and a standard metric set per command.

[1]: https://opentelemetry.io/docs/specs/otel/logs/?utm_source=chatgpt.com "OpenTelemetry Logging"
[2]: https://opentelemetry-python-contrib.readthedocs.io/en/latest/instrumentation/logging/logging.html?utm_source=chatgpt.com "OpenTelemetry Logging Instrumentation"
[3]: https://cyclopts.readthedocs.io/en/v3.14.1/api.html "API — cyclopts"
[4]: https://cyclopts.readthedocs.io/en/latest/packaging.html "Packaging — cyclopts"
[5]: https://cyclopts.readthedocs.io/en/latest/app_calling.html "App Calling & Return Values — cyclopts"
