# E2E Test Fixtures

This directory contains golden snapshot files and spec fixtures for CQ E2E tests.

## Golden Snapshot Pattern

Golden snapshots are reference outputs that tests compare against to detect regressions.

### Usage

1. Write a test that generates output
2. Save the expected output as a JSON file in this directory
3. Compare test output against the golden file
4. Use `--update-golden` flag to update snapshots when intentionally changing behavior

### Example

```python
import json
from pathlib import Path

def test_query_output(run_query, update_golden):
    """Test query returns expected structure."""
    result = run_query('entity=function name=build_graph_product')

    golden_path = Path(__file__).parent / "fixtures" / "build_graph_product_query.json"

    if update_golden:
        golden_path.write_text(json.dumps(result.to_dict(), indent=2))
    else:
        expected = json.loads(golden_path.read_text())
        assert result.to_dict() == expected
```

### Updating Snapshots

When you intentionally change output format or behavior:

```bash
uv run pytest tests/e2e/cq/ --update-golden
```

This will regenerate all golden files with current outputs.

For command-matrix snapshots, use:

```bash
bash scripts/update_cq_goldens.sh
```

## Golden Specs

`fixtures/golden_specs/*.json` contains declarative assertions used by command-level
tests (minimum findings, required files/messages/categories, required section titles).

These specs are intentionally less brittle than full snapshots and are used alongside
JSON snapshots.

## Hermetic Workspaces

CQ command goldens rely on stable fixture workspaces under:

- `tests/e2e/cq/_golden_workspace/python_project`
- `tests/e2e/cq/_golden_workspace/rust_workspace`
- `tests/e2e/cq/_golden_workspace/mixed_workspace`

These workspaces are the canonical dataset for Python/Rust command-level goldens and
should be preferred over repository-source-dependent snapshots.

### Best Practices

- Keep golden files small and focused
- Use descriptive filenames that match test names
- Review golden file diffs carefully in PRs
- Don't commit golden files for tests that aren't stable yet
- Use `.gitignore` to exclude temporary/unstable snapshots
