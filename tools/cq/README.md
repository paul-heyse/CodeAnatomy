# cq tool

The cq tool provides high-signal codebase queries and bundled reports for agent workflows.

## Quick start

Run a declarative query:

```bash
uv run python -m tools.cq.cli q "entity=function name=execute_plan" --format summary
```

Run a target-scoped report bundle:

```bash
uv run python -m tools.cq.cli report refactor-impact --target function:execute_plan --param root
```

## Core commands

- `q`: entity and pattern queries with optional expanders
- `report`: bundled, target-scoped reports (refactor-impact, safety-reliability, change-propagation, dependency-health)
- `index`: manage the ast-grep scan index cache
- `cache`: manage the query result cache

## Caching

Query and report runs use caching by default. Disable with `--no-cache`.

## Artifacts

JSON artifacts are saved by default to `.cq/artifacts`. Use `--no-save-artifact` to skip saving.
