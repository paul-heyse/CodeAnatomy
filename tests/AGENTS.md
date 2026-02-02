# tests/AGENTS.md

Test policy for the CodeAnatomy codebase.

## Test Categories

| Directory | Purpose | Command |
|-----------|---------|---------|
| `unit/` | Single-module, fast tests | `uv run pytest tests/unit/` |
| `integration/` | Multi-subsystem tests | `uv run pytest tests/integration/` |
| `e2e/` | Full pipeline, slow tests | `uv run pytest tests/e2e/` |
| `msgspec_contract/` | Serialization round-trip | `uv run pytest tests/msgspec_contract/` |
| `cli_golden/` | CLI output snapshots | `uv run pytest tests/cli_golden/` |
| `plan_golden/` | Plan artifact snapshots | `uv run pytest tests/plan_golden/` |

## Markers

```python
@pytest.mark.smoke       # Fast sanity (run first in CI)
@pytest.mark.e2e         # End-to-end (excluded with -m "not e2e")
@pytest.mark.integration # Multi-subsystem
@pytest.mark.benchmark   # Performance (non-gating)
@pytest.mark.serial      # Must run single-threaded
```

## Mocking Policy

**Prefer dependency injection.** `monkeypatch` is allowed for:

- **Environment variables** - `monkeypatch.setenv()`, `monkeypatch.delenv()`
- **Filesystem seams** - Path methods, file operations
- **Tight unit seams** - Functions within the same module

**Avoid patching:**

- Deep internals across module boundaries
- Private methods (`_method`)
- Third-party library internals

## Golden Testing

Update snapshots:
```bash
uv run pytest tests/cli_golden/ --update-golden
uv run pytest tests/plan_golden/ --update-goldens
```

**Always review diffs before committing.**

## Fixture Scopes

- **session** - DataFusion sessions, Delta tables, expensive resources
- **module** - Shared setup within a file
- **function** - Default for everything else
