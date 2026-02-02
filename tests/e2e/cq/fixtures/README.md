# E2E Test Fixtures

This directory contains golden snapshot files for E2E tests.

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

### Best Practices

- Keep golden files small and focused
- Use descriptive filenames that match test names
- Review golden file diffs carefully in PRs
- Don't commit golden files for tests that aren't stable yet
- Use `.gitignore` to exclude temporary/unstable snapshots
