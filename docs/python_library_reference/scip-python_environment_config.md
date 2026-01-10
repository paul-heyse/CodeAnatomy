Here’s the practical “make it work every time” checklist for **scip-python + uv projects**. The two big gotchas are:

1. **scip-python expects to interrogate your environment with `pip`** (unless you give it an explicit `--environment` JSON), and
2. **Pyright needs to know which venv / search paths to use** (especially with `src/` layouts and mono-repos). ([GitHub][1]) 

---

## What files you likely need to add

### 1) `pyrightconfig.json` (recommended)

scip-python is a Pyright-based indexer, so your import/dependency resolution quality is basically “Pyright import resolution quality.” Pyright’s standard way to pin an environment is `venvPath` + `venv`, plus optional `extraPaths` for `src/` layouts. ([GitHub][2])

**Example for a typical uv project where the env lives at `./.venv`:**

```json
{
  "venvPath": ".",
  "venv": ".venv",
  "pythonVersion": "3.12",
  "extraPaths": ["src"],
  "verboseOutput": true
}
```

Why these knobs:

* `venvPath` + `venv` tell Pyright which **site-packages** to use. ([GitHub][2])
* `extraPaths` fixes the common “src layout not importable” problem. ([GitHub][2])
* `verboseOutput` is the fastest way to debug “why can’t you find X?” when indexing. ([GitHub][2])

> If you don’t use `src/`, drop `extraPaths`.

### 2) `.python-version` (strongly recommended if you want deterministic indexing)

uv uses `.python-version` as the project’s default Python version and will create the project venv accordingly. This keeps scip-python’s analysis consistent across machines (and ensures you’re on Python 3.10+ as required). ([Astral Docs][3])

### 3) Optional: `scip_env.json` (only if you *can’t* rely on `pip`)

scip-python can skip `pip` entirely if you pass `--environment` with a JSON list of packages, each including `name`, `version`, and `files`. ([GitHub][1])

---

## Setup actions for uv so scip-python sees the *right* dependencies

### Step A — Ensure the uv environment is real and synced

uv projects normally create a **project venv at `./.venv`** and a **`uv.lock`** the first time you run project commands like `uv run` / `uv sync`. ([Astral Docs][3])

For deterministic installs:

* In CI (or whenever you want “don’t touch the lockfile”): use `--locked` or `--frozen`. ([Astral Docs][4])

  * `uv run --locked …` errors if `uv.lock` is out of date. ([Astral Docs][4])
  * `uv run --frozen …` uses the lockfile without checking it matches project metadata. ([Astral Docs][4])

### Step B — Make sure `pip` is available *inside* the uv environment (unless you’ll use `--environment`)

scip-python “by default” uses `pip` to determine package names/versions/files. 

But uv can create envs without seeding `pip` by default, and uv can also remove “extraneous” packages when syncing exactly to `uv.lock`. ([Astral Docs][5])

**Reliable options:**

1. **Keep `pip` in the lockfile** (my recommendation)
   Add it as a dev dependency so `uv sync` won’t remove it:

* `uv add --dev pip` then `uv sync` (or `uv sync --frozen` in CI). ([Astral Docs][4])

2. **Seed pip when creating the venv**
   Use uv’s seed mechanism (via env var or CLI) to install seed packages like `pip`. ([Astral Docs][5])

* Environment variable: `UV_VENV_SEED=pip` (or `pip,setuptools,wheel` depending on your Python version needs) ([Astral Docs][5])
* CLI equivalent: `uv venv --seed` (behavior referenced by uv’s seed docs) ([Astral Docs][5])

> If you seed pip but don’t lock it, an “exact” sync may still remove it, because uv sync removes packages not in the lockfile by default. ([Astral Docs][4])

### Step C — Ensure your *project itself* is installed or importable

uv decides whether to build/install your project into the environment based on whether you’ve defined a build system (and you can override with `tool.uv.package`). This matters a lot for `src/` layouts and mono-repos. ([Astral Docs][6])

If you **don’t** have a build system and you **do** have `src/` layout, you almost certainly want:

* `extraPaths: ["src"]` in `pyrightconfig.json` (above), **or**
* configure packaging so uv installs your project into the env. ([Astral Docs][6])

---

## How to run scip-python so it uses the uv environment (and not system Python)

scip-python’s README explicitly says to **activate your virtual environment before running**. 

With uv you have two good patterns:

### Pattern 1: Run via uv (usually simplest / most reproducible)

Because `uv run` locks+syncs before executing by default, you can run scip-python “in” the project environment:

* `uv run --locked -- scip-python index . --project-name=…` ([Astral Docs][4])

### Pattern 2: Activate `.venv` then run scip-python

* `source .venv/bin/activate` (or Windows equivalent)
* `scip-python index . --project-name=…` ([GitHub][1])

**Why I prefer `uv run`:** uv project operations don’t necessarily respect `VIRTUAL_ENV` by default (it will warn if you activated something else), and `UV_PROJECT_ENVIRONMENT` can relocate the env path; `uv run` keeps you anchored to the project’s configured env. ([Astral Docs][6])

---

## If you want to avoid `pip` entirely: generate a `--environment` JSON

scip-python supports `--environment` with a JSON list of `{name, version, files}`. ([GitHub][1])

A pragmatic way to generate it (works well with uv because uv still installs standard `.dist-info`
metadata) is via `importlib.metadata`. This repo includes a generator at
`scripts/gen_scip_env.py`:

```python
# scripts/gen_scip_env.py
from __future__ import annotations

import json
import sys
from importlib import metadata

env = []
for dist in metadata.distributions():
    name = dist.metadata.get("Name") or dist.metadata.get("Summary") or dist.name
    version = dist.version
    files = [str(path) for path in (dist.files or ())]
    env.append({"name": name, "version": version, "files": sorted(set(files))})

env.sort(key=lambda entry: str(entry["name"]))
sys.stdout.write(json.dumps(env, indent=2, sort_keys=True))
sys.stdout.write("\n")
```

Then run:

* `uv run -- python scripts/gen_scip_env.py > scip_env.json`
* `uv run -- scip-python index . --project-name=… --environment scip_env.json` ([Astral Docs][4])

CodeIntel also wires this through `ScipIngestOptions.environment_json`, so you can point
the build config at the generated JSON to bypass `pip` entirely.

---

## uv caching / “no fixed python path” considerations (what actually matters)

* **uv’s cache** (wheels/sdists/build artifacts) generally doesn’t matter to scip-python; scip-python cares about what’s **installed into the environment’s site-packages** (or what you list in `--environment`). ([GitHub][1])
* What *does* matter is ensuring scip-python runs against a **stable project env path**:

  * Prefer the default `./.venv` (repo-local, deterministic). ([Astral Docs][3])
  * Be cautious with `UV_PROJECT_ENVIRONMENT` pointing to a shared absolute path (can be overwritten across projects). ([Astral Docs][6])

---

## Quick troubleshooting checklist

1. If external imports are missing: set `verboseOutput: true` in `pyrightconfig.json` and re-run to see import resolution logs. ([GitHub][2])
2. If scip-python complains about `pip`: either add `pip` as a dev dependency (so it stays installed) or switch to `--environment`. ([GitHub][1])
3. If your repo is large and Node OOMs: raise Node heap (e.g., `NODE_OPTIONS=--max-old-space-size=8192`). ([GitHub][1])

If you tell me whether your repo uses **`src/` layout** and whether you run scip-python **locally** or **in CI**, I can give you an exact minimal `pyrightconfig.json` + exact uv commands that keep `pip` present (or avoid it cleanly) with no footguns.

[1]: https://github.com/sourcegraph/scip-python "GitHub - sourcegraph/scip-python: SCIP indexer for Python"
[2]: https://raw.githubusercontent.com/microsoft/pyright/main/docs/configuration.md "raw.githubusercontent.com"
[3]: https://docs.astral.sh/uv/guides/projects/ "Working on projects | uv"
[4]: https://docs.astral.sh/uv/concepts/projects/sync/ "Locking and syncing | uv"
[5]: https://docs.astral.sh/uv/reference/environment/ "Environment variables | uv"
[6]: https://docs.astral.sh/uv/concepts/projects/config/ "Configuring projects | uv"

According to a document from **January 7, 2026**, the cleanest way to make scip-python “see” your **uv** environment in a **src/**-layout repo is:

1. **pin Pyright’s environment + import roots** via `pyrightconfig.json`, and
2. either **(A) ensure `pip` exists inside `.venv`** (so scip-python can enumerate external deps), or **(B) bypass pip** with `--environment` JSON. 

Below is the “exact minimal” setup + commands for **local** runs.

---

## 1) Minimal `pyrightconfig.json` (src/ layout + uv `.venv`)

Create `pyrightconfig.json` at repo root:

```json
{
  "include": ["src"],
  "exclude": ["**/__pycache__", "build", "dist", ".venv"],
  "extraPaths": ["src"],
  "venvPath": ".",
  "venv": ".venv",
  "pythonVersion": "3.12",
  "useLibraryCodeForTypes": true
}
```

Why these exact fields:

* `extraPaths: ["src"]` is the key fix for `src/` imports. 
* `venvPath` + `venv` forces dependency resolution to come from your uv venv. 
* `useLibraryCodeForTypes` improves symbol discovery for libraries without complete stubs. 

> Set `pythonVersion` to your project’s actual version. Quick check: `uv run --frozen -- python -V`. ([Astral Docs][1])

---

## 2) Install scip-python (one-time)

scip-python is installed via **npm**, and requires **Python 3.10+** and **Node 16+**. ([GitHub][2])

```bash
npm install -g @sourcegraph/scip-python
```

([GitHub][2])

---

## 3) Option A (recommended): keep `pip` present in `.venv` and run normally

scip-python’s default dependency detection calls out to **`pip`**; if `pip` isn’t present in your environment, dependency modeling will be degraded unless you use `--environment`. ([GitHub][2])

### A1) Ensure `.venv` matches `uv.lock` *without lockfile churn*

```bash
uv sync --frozen
```

* `uv sync` will create `.venv` if needed and by default does an **exact sync** (removes packages not in the lock). ([Astral Docs][3])
* `--frozen` prevents uv from re-locking before syncing. ([Astral Docs][3])

### A2) Make `pip` non-extraneous (so uv doesn’t delete it)

The “no footguns” move is to add `pip` to your dev group (so it’s in `uv.lock`):

```bash
uv add --dev pip
uv sync --frozen
```

* `uv add --dev …` is the documented way to add to a dev/dependency group. ([Astral Docs][4])
* This avoids the classic footgun where `pip` is present one day and then disappears because `uv sync` is exact-by-default. ([Astral Docs][3])

### A3) Run scip-python *inside* the uv environment

This ensures when scip-python spawns `pip`, it finds `.venv/bin/pip`:

```bash
uv run --frozen -- \
  scip-python index . \
  --project-name "github.com/you/your-repo" \
  --project-version "$(git rev-parse HEAD)" \
  --output "build/scip/index.scip"
```

* `uv run --frozen` uses the lockfile without trying to update it. ([Astral Docs][1])

If you hit Node OOM on larger repos:

```bash
NODE_OPTIONS="--max-old-space-size=8192" uv run --frozen -- scip-python index . --project-name "github.com/you/your-repo"
```

([GitHub][2])

---

## 4) Option B (pipless + hermetic): generate `env.json` and pass `--environment`

scip-python supports a JSON environment manifest `{name, version, files}` to **skip all pip calls**.  ([GitHub][2])

### B1) Keep `pyrightconfig.json` as above, then generate `build/scip/env.json`

Create `scripts/gen_scip_env.py`:

```python
from __future__ import annotations

import json
from importlib import metadata

def main() -> None:
    env = []
    for dist in metadata.distributions():
        name = dist.metadata.get("Name") or dist.name
        version = dist.version
        files = []
        if dist.files:
            files = sorted({str(p) for p in dist.files})
        env.append({"name": name, "version": version, "files": files})

    env.sort(key=lambda x: (x["name"].lower(), x["version"]))
    print(json.dumps(env, indent=2))

if __name__ == "__main__":
    main()
```

Then:

```bash
mkdir -p build/scip
uv run --frozen -- python scripts/gen_scip_env.py > build/scip/env.json
```

### B2) Index with `--environment`

```bash
uv run --frozen -- \
  scip-python index . \
  --project-name "github.com/you/your-repo" \
  --project-version "$(git rev-parse HEAD)" \
  --environment "build/scip/env.json" \
  --output "build/scip/index.scip"
```

This is the cleanest way to avoid any “pip vanished” or “pip not installed” issues while staying fully uv-native.  ([GitHub][2])

---

## Small “no footguns” notes (local + uv + src/)

* If you use extras, remember uv does **not** sync extras unless you request them (e.g. `uv sync --extra foo` / `--all-extras`). ([Astral Docs][1])
* Don’t rely on “seeded” packages that aren’t in `uv.lock`: exact sync will remove them. ([Astral Docs][1])

If you tell me whether your uv venv is the default `./.venv` or you’ve set `UV_PROJECT_ENVIRONMENT` to something else, I can adjust the `venvPath`/`venv` fields to match perfectly.

[1]: https://docs.astral.sh/uv/concepts/projects/sync/ "Locking and syncing | uv"
[2]: https://github.com/sourcegraph/scip-python "GitHub - sourcegraph/scip-python: SCIP indexer for Python"
[3]: https://docs.astral.sh/uv/reference/cli/ "Commands | uv"
[4]: https://docs.astral.sh/uv/concepts/projects/dependencies/?utm_source=chatgpt.com "Managing dependencies | uv - Astral Docs"
