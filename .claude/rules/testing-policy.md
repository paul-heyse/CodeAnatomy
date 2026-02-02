# Testing Policy

Testing philosophy and allowed patterns, aligned with actual practice in this codebase.

## Mocking Philosophy

**Prefer dependency injection over mocking.** However, `monkeypatch` is allowed for:

| Use Case | Example | Allowed |
|----------|---------|---------|
| Environment variables | `monkeypatch.setenv("DEBUG", "1")` | Yes |
| Filesystem seams | `monkeypatch.setattr(Path, "exists", ...)` | Yes |
| Tight unit test seams | Replacing a single function in the same module | Yes |
| Deep internals | Patching across module boundaries | **Avoid** |
| Third-party libraries | Mocking requests, etc. | Use `responses` or similar |

**Rationale:** DI is cleaner but env vars and filesystem are natural seams that pytest's
`monkeypatch` fixture handles well. Avoid patching deep internals as it couples tests
to implementation details.

## Test Categories

| Directory | Purpose | Speed |
|-----------|---------|-------|
| `tests/unit/` | Single-module tests | Fast |
| `tests/integration/` | Multi-subsystem tests | Medium |
| `tests/e2e/` | Full pipeline tests | Slow |
| `tests/msgspec_contract/` | Serialization round-trip | Fast |
| `tests/cli_golden/` | CLI output snapshots | Fast |
| `tests/plan_golden/` | Plan artifact snapshots | Fast |

## Pytest Markers

```python
@pytest.mark.smoke       # Fast sanity checks (run first)
@pytest.mark.e2e         # End-to-end pipeline tests
@pytest.mark.integration # Multi-subsystem tests
@pytest.mark.benchmark   # Performance tests (non-gating)
@pytest.mark.serial      # Must run single-threaded
```

## Golden Snapshot Testing

Update golden files with flags:
- `--update-golden` - Update a single golden file
- `--update-goldens` - Update all golden files

**Rule:** Never commit updated goldens without reviewing the diff.

## Fixture Conventions

- **Session-scoped** - Expensive resources (DataFusion sessions, Delta tables)
- **Function-scoped** - Everything else (default)
- **Module-scoped** - Shared setup within a test file

## Table-Driven Tests

Use `@pytest.mark.parametrize` for testing multiple inputs:

```python
@pytest.mark.parametrize("input,expected", [
    ("a", 1),
    ("b", 2),
])
def test_lookup(input: str, expected: int) -> None:
    assert lookup(input) == expected
```

## Configuration Injection

Inject configuration via fixtures rather than global state:

```python
@pytest.fixture
def config() -> Config:
    return Config(debug=True)

def test_with_config(config: Config) -> None:
    result = process(config)
    assert result.debug_info is not None
```
