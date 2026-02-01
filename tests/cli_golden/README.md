# CLI golden tests

These tests snapshot CLI help and error output to keep formatting stable.

## Update snapshots

```bash
uv run pytest tests/cli_golden --update-golden
```

This regenerates files under `tests/cli_golden/fixtures/` using the Rich console
renderer (width=120, no color). Re-run without `--update-golden` to validate
changes.

## Adding new snapshots

1. Add a new assertion in `tests/cli_golden/test_cli_help_output.py` or another
   golden test.
2. Run the update command above to create the fixture file.
3. Commit the new fixture file alongside the test.

## Notes

- Keep snapshots deterministic (avoid timestamps or random IDs).
- If output includes paths, prefer relative fixtures or stable inputs.
