# Python Quality Rules

Non-obvious enforcement points that complement ruff/pyrefly. These are not duplicated
from `pyproject.toml`; they are policies that require human judgment.

**When to check:** Run quality gates (ruff, pyrefly, pyright) **after task completion**,
not at the beginning or mid-task. Complete your implementation first, then validate.

## Rule Disables

- **No `# noqa` comments** - Fix the issue structurally instead of suppressing
- **No `# type: ignore`** - Fix the type error or improve the type signature
- **No rule disables** - If a rule is wrong, discuss changing the config

## Complexity Caps

| Metric | Limit | Rationale |
|--------|-------|-----------|
| Cyclomatic complexity | ≤ 10 | Forces decomposition |
| Max branches | ≤ 12 | Prevents nested conditionals |
| Max return statements | ≤ 6 | Encourages early returns |

## Import Conventions

- **Absolute imports only** - `ban-relative-imports = "all"` in ruff
- **Type-only imports** - Place in `if TYPE_CHECKING:` blocks
- **Future annotations** - Every module starts with `from __future__ import annotations`

## Docstring Convention

- **NumPy style** - Enforced by ruff `pydocstyle`
- **Imperative mood** - "Return the value" not "Returns the value"
- **Required sections** - Parameters, Returns, Raises (when applicable)

## Type Annotation Rules

- **All functions fully typed** - Both parameters and return types
- **No bare `Any`** - Use `object` or a protocol instead
- **Strict `Optional` handling** - Prefer `X | None` over `Optional[X]`
- **Protocol over ABC** - Prefer structural typing

## Error Handling

- **No bare `except:`** - Always catch specific exceptions
- **No `except Exception:`** - Too broad for most cases
- **Custom exceptions** - Inherit from domain-specific base classes in `src/exceptions/`

## String Conventions

- **Double quotes** - Enforced by ruff
- **f-strings** - Preferred over `.format()` or `%`

## Sources of Truth

- **Ruff rules**: `pyproject.toml` → `[tool.ruff]`, `[tool.ruff.lint]`
- **Pyrefly config**: `pyrefly.toml`
- **Pyright config**: `pyrightconfig.json` (basic mode; pyrefly is the strict gate)
