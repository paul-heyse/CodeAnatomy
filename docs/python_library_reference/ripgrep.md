## B) CLI topology, entrypoints, and invocation modes (Linux `rg`)

### B0) The **supported invocation shapes** (what `rg` considers “valid forms”)

The `rg(1)` synopsis is the canonical “topology” for how arguments are parsed and which modes exist: search, multi-pattern search, non-search listings, pipe/stdin search, and introspection (`--help`, `--version`). ([Debian Manpages][1])

```bash
rg [OPTIONS] PATTERN [PATH...]
rg [OPTIONS] -e PATTERN... [PATH...]
rg [OPTIONS] -f PATTERNFILE... [PATH...]
rg [OPTIONS] --files [PATH...]
rg [OPTIONS] --type-list
command | rg [OPTIONS] PATTERN
rg [OPTIONS] --help
rg [OPTIONS] --version
```

([Debian Manpages][1])

Think of these as **mutually defining entrypoints**:

* **Search mode**: there is a *PATTERN* (positional) OR there are one/many `-e/-f` patterns.
* **Listing mode**: `--files` or `--type-list` changes the “program purpose” (no searching).
* **Stdin mode**: if stdin is present (pipe), `rg` can switch to searching stdin unless you force a filesystem PATH. ([Debian Manpages][1])

---

### B1) Primary search entrypoint: `rg [OPTIONS] PATTERN [PATH...]`

#### Syntax

```bash
rg [OPTIONS] PATTERN [PATH...]
```

([Debian Manpages][1])

#### Parsing rules (the part that matters operationally)

* `PATTERN` is the regex (or literal if `-F`, but that’s a later chapter). ([Debian Manpages][1])
* Each `PATH` is a file or directory; directories are searched recursively. ([Debian Manpages][1])
* **Explicit file paths** on the command line override ignore/glob rules (i.e., “I told you to search this file, do it”). ([Debian Manpages][1])

#### High-frequency command shapes

```bash
# Search recursively in the current directory (PATH omitted => CWD is the implicit root)
rg 'TODO'

# Search multiple roots explicitly (common for monorepos)
rg 'TODO' src tests docs

# Search one file (still a PATH; no recursion)
rg 'TODO' pyproject.toml

# Pattern contains spaces/shell metacharacters: quote it
rg 'fast\s+path' .
```

The “recursive in current directory” default behavior is core to ripgrep’s UX and is explicitly described as the baseline behavior. ([Debian Manpages][1])

---

### B2) Multi-pattern entrypoints: `-e PATTERN...` and `-f PATTERNFILE...`

#### B2.1 `-e/--regexp=PATTERN` (repeatable)

**Key semantics:**

* `-e` can be provided multiple times.
* A line is printed if it matches **at least one** of the provided patterns.
* `-e` also solves the “pattern starts with `-`” ambiguity (see below). ([Debian Manpages][1])

```bash
# OR semantics across patterns
rg -e 'TODO' -e 'FIXME' src/

# Common: combine literal-ish & regex-ish patterns
rg -e 'panic!' -e 'unwrap\(' src/
```

([Debian Manpages][1])

**Important parsing rule:** once you use `-e` (or `-f`), *all positional arguments are treated as PATHs*, not “the PATTERN positional.” ([Debian Manpages][1])
That means:

```bash
# Here, there is no positional PATTERN. Everything after options is PATH.
rg -e 'TODO' src/ tests/
```

#### B2.2 `-f/--file=PATTERNFILE` (repeatable; one pattern per line)

**Key semantics:**

* Each line in `PATTERNFILE` is a pattern; newline is not part of the pattern.
* Empty lines match all input lines (**footgun**: a blank line means “match everything”).
* Combine multiple `-f` files and/or `-e` flags: still OR semantics (“match at least one”). ([Debian Manpages][1])

```bash
# patterns.txt:
#   TODO
#   FIXME
#   HACK

rg -f patterns.txt src/
rg -f patterns.txt -e 'panic!' src/
```

([Debian Manpages][1])

**Special case:** if `PATTERNFILE` is `-`, ripgrep reads patterns from stdin. ([Debian Manpages][1])
This is *patterns-from-stdin*, not “search-input-from-stdin”:

```bash
# Feed patterns via stdin (bash)
printf '%s\n' 'TODO' 'FIXME' | rg -f - src/
```

([Debian Manpages][1])

#### B2.3 The “pattern starts with a dash” problem (and the two canonical fixes)

Ripgrep treats leading `-` as option parsing unless you disambiguate. The man page calls out the rule explicitly. ([Debian Manpages][1])

Two correct solutions:

```bash
# 1) Use -e to force “this is a pattern”
rg -e '-foo' .

# 2) Use -- to end option parsing (everything after is positional)
rg -- '-foo' .
```

([Debian Manpages][1])

---

### B3) Non-search modes: “inventory / introspection” entrypoints

These are *purpose switches*: you’re not searching content; you’re asking `rg` to emit metadata about what it *would* do or what it *knows*.

#### B3.1 `--files` (emit the file set that would be searched)

* Prints each file that would be searched **without** searching.
* Primary use: debug “why didn’t rg search X?” and build upstream file lists.
* Overrides `--type-list` if you accidentally pass both. ([Debian Manpages][1])

```bash
# What would rg search, given current ignore/glob settings?
rg --files

# Restrict the listing to a subtree (PATH is still accepted)
rg --files src/

# Debug a “missing file”: does it even appear in the candidate set?
rg --files | rg -F 'suspicious_file.py'
```

([Debian Manpages][1])

#### B3.2 `--type-list` (emit built-in + custom file type definitions)

* Prints all supported file types and their glob mappings (one type per line, `type:glob,glob,...`).
* Includes effects of `--type-add` / `--type-clear` (even though persistence is a separate chapter). ([Debian Manpages][1])

```bash
# View ripgrep’s known type universe (and globs)
rg --type-list

# Grep the type universe for “py”
rg --type-list | rg -n '(^py:|,.*\.py\b)'
```

([Debian Manpages][1])

---

### B4) Pipe mode + stdin auto-detection

#### B4.1 The core behavior

Ripgrep auto-detects whether stdin is readable and, if so, will search stdin (classic pipeline: `ls | rg foo`). ([Debian Manpages][1])

```bash
# Pipeline search (stdin is the “file”)
git diff | rg 'TODO'
journalctl -u myservice | rg 'ERROR|WARN'
```

([Debian Manpages][1])

#### B4.2 “Stdin exists unexpectedly” (common in some shells/environments)

The man page explicitly documents the footgun and the fix:

> In some environments, stdin may exist when it shouldn’t. To turn off stdin detection, explicitly specify the directory to search (e.g., `rg foo ./`). ([Debian Manpages][1])

So the operational rule is:

* If you **intend to search the filesystem**, pass a PATH (at minimum `.` or `./`).
* If you **intend to search stdin**, pipe into `rg` and omit PATH. ([Debian Manpages][1])

```bash
# Force filesystem search even if stdin is present:
rg 'needle' ./

# Force filesystem search in a specific root:
rg 'needle' /var/log
```

([Debian Manpages][1])

#### B4.3 Streaming pipelines: flush behavior (`--line-buffered`)

When you chain multiple commands, buffering can matter. `rg` documents `--line-buffered` as the “I want streaming output through pipes” switch, with an explicit example using `tail -f`. ([Debian Manpages][1])

```bash
tail -f app.log | rg 'ERROR' --line-buffered | rg 'db' --line-buffered
```

([Debian Manpages][1])

---

### B5) Help and version discovery conventions (`--help`, `--version`)

#### B5.1 Help

The synopsis includes the introspection entrypoint:

```bash
rg [OPTIONS] --help
```

([Debian Manpages][1])

Practical Linux usage patterns:

```bash
rg --help | less
man rg
```

#### B5.2 Version

Ripgrep provides:

* `-V, --version` to print version and potentially additional build info. ([Debian Manpages][1])

```bash
rg --version
rg -V
```

([Debian Manpages][1])

**Related (often useful in practice):** `--pcre2-version` prints PCRE2 version info (or errors if not compiled with PCRE2). ([Debian Manpages][1])

```bash
rg --pcre2-version
```

([Debian Manpages][1])

---

### Minimal “mode decision” cheat sheet (what you choose first)

* **Search filesystem (default)**: `rg PATTERN [PATH...]` ([Debian Manpages][1])
* **OR-search multiple patterns**: `rg -e P1 -e P2 [PATH...]` or `rg -f patterns.txt [PATH...]` ([Debian Manpages][1])
* **Pipeline search stdin**: `command | rg PATTERN` ([Debian Manpages][1])
* **Stdin is messing you up**: add `./` (or other PATH) to disable stdin detection ([Debian Manpages][1])
* **Debug “what files are searched”**: `rg --files [PATH...]` ([Debian Manpages][1])
* **Inspect file type universe**: `rg --type-list` ([Debian Manpages][1])



[1]: https://manpages.debian.org/testing/ripgrep/rg.1.en.html "rg(1) — ripgrep — Debian testing — Debian Manpages"

## C) Input + Search option families that change *how `rg` matches* (engine, literals, line terminators, mmap)

### C0) Precedence model you should assume (esp. with configs / aliases)

Many flags have an inverted `--no-*` variant. When you have conflicts (including config file + CLI), **the last flag wins**. ([Ubuntu Manpages][1])

That’s why you’ll see patterns like:

```bash
# Override a config that set fixed-strings:
rg --no-fixed-strings 'foo\w+' .

# Override a config that forced PCRE2:
rg --engine=default 'foo\w+' .

# Override a config that enabled mmap:
rg --no-mmap 'needle' .
```

---

## C1) Regex engine selection: `--engine=…` and `-P/--pcre2`

### C1.1 `--engine=ENGINE` (global engine choice)

**Syntax**

```bash
rg --engine=default PATTERN [PATH...]
rg --engine=pcre2   PATTERN [PATH...]
rg --engine=auto    PATTERN [PATH...]
```

**Hard semantics**

* Engine choice applies to **every regex provided** (positional `PATTERN`, all `-e`, all `-f`). You can’t mix engines per-pattern in a single invocation. ([Debian Manpages][2])
* Accepted values: `default`, `pcre2`, `auto`. ([Debian Manpages][2])
* `default` is “usually fastest” and intended for most use cases. ([Debian Manpages][2])
* `pcre2` is for features like **look-around** and **backreferences**. ([Debian Manpages][2])
* `auto` does “best effort” dynamic selection based on features used in the pattern. ([Debian Manpages][2])
* If your `rg` build lacks PCRE2, `--engine=pcre2` will error and exit. ([Debian Manpages][2])
* `--engine` overrides prior uses of `-P/--pcre2` and the deprecated hybrid flag. ([Debian Manpages][2])

**Practical: when to pick what**

```bash
# Default engine: fastest + linear-time guarantees (no lookaround/backrefs)
rg --engine=default 'foo\w{1,20}bar' src/

# Force PCRE2: needed for lookbehind, backrefs, some advanced constructs
rg --engine=pcre2 '(?<=import\s)Foo' src/

# Auto: keep default semantics normally, but allow PCRE2 when required
rg --engine=auto '(?<=import\s)Foo' src/
```

(Use `--engine=auto` when you want “PCRE2 only when necessary,” but be aware the selection is heuristic and not always obvious.) ([Ubuntu Manpages][1])

### C1.2 `-P, --pcre2` (shorthand for “always use PCRE2”)

**Syntax**

```bash
rg -P 'PATTERN' [PATH...]
```

**Equivalences + conflicts**

* `-P/--pcre2` is the same as `--engine=pcre2`. ([Debian Manpages][2])
* `-P` and `--engine` override one another (last one wins). ([Debian Manpages][2])
* If PCRE2 wasn’t compiled into your `rg`, `-P` errors and exits. ([Debian Manpages][2])

**User-experience footgun (PCRE2 vs default engine)**
PCRE2 can fail “more quietly” in some cases; e.g. using `\n` in a PCRE2 regex without multiline can silently match nothing instead of erroring immediately (default engine would error). ([Debian Manpages][2])

### C1.3 Programmatic detection: “do I have PCRE2?”

Use `--pcre2-version` as a capability probe:

```bash
rg --pcre2-version
```

* Prints PCRE2 version info and exits.
* If PCRE2 is unavailable, it prints an error and exits non-zero. ([Debian Manpages][2])

**Shell guard pattern**

```bash
if rg --pcre2-version >/dev/null 2>&1; then
  rg --engine=pcre2 '(?<=foo)bar' .
else
  # fallback: either bail or rewrite pattern for default engine
  echo "PCRE2 not available" >&2
  exit 2
fi
```

---

## C2) Literal mode: `-F/--fixed-strings` (turn regex parsing OFF)

### C2.1 What it does

**Syntax**

```bash
rg -F 'LITERAL' [PATH...]
rg --fixed-strings 'LITERAL' [PATH...]
```

**Semantics**

* Treat **all patterns as literals** (positional PATTERN + all `-e/-f` patterns). ([Debian Manpages][2])
* Regex metacharacters like `.(){}*+` no longer need escaping. ([Debian Manpages][2])
* Can be disabled with `--no-fixed-strings` (useful when your ripgreprc turns it on). ([Debian Manpages][2])

### C2.2 High-leverage usage patterns

Search for text that is annoying to escape as regex:

```bash
# Find the literal string "foo.bar" (dot is literal, not "any char")
rg -F 'foo.bar' src/

# Find the literal string "(?<=foo)bar" (not treated as lookbehind syntax)
rg -F '(?<=foo)bar' src/

# Search for JSON-ish punctuation without a quoting fiesta
rg -F '"status": "ok"' .
```

**Multi-pattern literal “OR”**

```bash
rg -F -e 'TODO' -e 'FIXME' src/
```

(Still OR semantics; it’s just that each pattern is literal.) ([Debian Manpages][2])

### C2.3 The “can’t mix literal + regex” rule (workarounds)

Since `-F` is global (“treat all patterns as literals”), if you need one literal and one regex, do one of:

* run two `rg` passes, or
* keep regex mode and regex-escape the literal.

---

## C3) Line terminators and anchors: `--crlf` (and its collision with `--null-data`)

### C3.1 `--crlf`: make `^` / `$` behave correctly on CRLF files

**Syntax**

```bash
rg --crlf 'PATTERN' [PATH...]
```

**Semantics**

* Treat CRLF (`\r\n`) as a **single** line terminator (not just `\n`). ([Debian Manpages][2])
* Primary impact: `^` and `$` treat **CRLF, CR, or LF** as line terminators — but they will **never match between** the `\r` and `\n` in CRLF. ([Debian Manpages][2])
* Default engine also supports enabling CRLF inside the pattern with the `R` flag; e.g. `(?R:$)` matches just before CR or LF, never between CR and LF. ([Debian Manpages][2])
* `--crlf` overrides `--null-data`. ([Debian Manpages][2])
* Can be disabled with `--no-crlf`. ([Debian Manpages][2])

**When you reach for it**

* Repos checked out with Windows line endings where `^...$` anchors are producing surprising misses.
* You’re doing strict “whole-line” patterns (`^foo$`, `^\s*$`, etc.) and the target files are CRLF.

**Examples**

```bash
# Strict "whole line equals foo" across CRLF files:
rg --crlf '^foo$' .

# Equivalent: enable CRLF handling inside the pattern (default engine)
rg '(?R:^foo$)' .
```

(That second form only applies to the default regex engine’s inline flags.) ([Debian Manpages][2])

### C3.2 `--null-data` collision: NUL-terminated “lines”

Even if you’re not deep-diving `--null-data` yet, you must know the override relationship because it affects `--crlf`:

* `--null-data` uses NUL (`\0`) as the line terminator; implies `-a/--text`; **overrides `--crlf`**. ([Debian Manpages][2])
* Conversely, `--crlf` **overrides `--null-data`**. ([Debian Manpages][2])

**Operational takeaway:** don’t set both unless you’re intentionally relying on “last flag wins.” ([Ubuntu Manpages][1])

---

## C4) IO strategy: `--mmap` / `--no-mmap` (memory-mapped searching)

### C4.1 `--mmap`: allow memory maps when possible

**Syntax**

```bash
rg --mmap 'PATTERN' [PATH...]
```

**Semantics**

* Enables searching via memory maps **when possible**. ([Debian Manpages][2])
* It’s already used by default when ripgrep heuristically thinks it will be faster; `--mmap` is the “don’t disable it” forcing function. ([Debian Manpages][2])
* Memory maps can’t be used for everything (e.g., stdin / streams / “virtual files”), so even with `--mmap` set, ripgrep may fall back to non-mmap IO. ([Debian Manpages][2])

### C4.2 `--no-mmap`: forcefully disable memory maps (stability knob)

**Syntax**

```bash
rg --no-mmap 'PATTERN' [PATH...]
```

**When you should default to `--no-mmap`**
If you are searching files that may be **simultaneously truncated** (rotating logs, active write/replace workflows), ripgrep can abort unexpectedly when memory maps are used; disabling mmap avoids that failure mode. ([Debian Manpages][2])

**Practical recipes**

```bash
# Safer in /var/log or any directory with active truncation/rotation:
rg --no-mmap 'ERROR|FATAL' /var/log

# CI “stability mode” (avoid rare mmap aborts)
rg --no-mmap 'needle' .
```

---

## C5) “Mode composition” recipes (copy/paste)

These are common real-world combos that surface quickly in agent workflows:

```bash
# 1) Advanced regex + “use default unless PCRE2 is needed”
rg --engine=auto '(\bFoo\b)(?s:.*)\bBar\b' .

# 2) Hard require PCRE2 + CRLF-correct anchors
rg --engine=pcre2 --crlf '^(?=.*foo)(?=.*bar).*$' .

# 3) Literal search + CRLF (anchors are literal because -F)
rg -F --crlf '^foo$' .

# 4) Log directory safety: disable mmap
rg --no-mmap -n 'timeout' /var/log
```

(Engine selection and CRLF semantics are global and affect all patterns you pass.) ([Debian Manpages][2])

---

## C6) Micro-cheat-sheet: conflicts and equivalences

* `-P` ⇔ `--engine=pcre2` (last wins if both). ([Debian Manpages][2])
* `--engine=…` applies to **all** regex inputs (`PATTERN`, `-e`, `-f`). ([Debian Manpages][2])
* `--crlf` changes anchor semantics; CRLF treated as one terminator; never match between `\r` and `\n`. ([Debian Manpages][2])
* `--crlf` overrides `--null-data`; `--null-data` overrides `--crlf` → last wins. ([Debian Manpages][2])
* `--mmap` is “allow mmap when possible”; `--no-mmap` is the stability hammer (avoid aborts on truncation). ([Debian Manpages][2])

If you want the next chapter in the same style, the natural follow-on is **multiline mode and match semantics** (`-U/--multiline`, `--multiline-dotall`, anchor/dot behavior, and the memory implications), since it directly interacts with both **engine choice** and **mmap/streaming IO**.

[1]: https://manpages.ubuntu.com/manpages/jammy/man1/rg.1.html "Ubuntu Manpage:

       rg - recursively search the current directory for lines matching a pattern
    "
[2]: https://manpages.debian.org/testing/ripgrep/rg.1.en.html "rg(1) — ripgrep — Debian testing — Debian Manpages"

## D) Multiline mode + match semantics (`-U/--multiline`, `--multiline-dotall`, anchors, memory/IO)

### D0) Two separate “multiline” concepts you must not conflate

1. **ripgrep multiline search** (`-U/--multiline`): changes the *haystack model* so a match is allowed to include line terminators (e.g., `\n`), and ripgrep will permit newline-matching constructs. ([Debian Manpages][1])
2. **regex “multi-line mode” flag** (`(?m)` in the Rust regex syntax): only changes what `^` / `$` mean (begin/end of *line* vs begin/end of *haystack*). ([Docs.rs][2])
3. **regex “dotall” flag** (`(?s)` or ripgrep’s `--multiline-dotall`): changes whether `.` matches `\n`. ([Debian Manpages][1])

Operationally: `-U` is about **allowing matches to span newline**; `(?m)` is about **anchors**; `(?s)` / `--multiline-dotall` is about **dot matching newline**.

---

## D1) `-U, --multiline`: enabling cross-line matches (and what it actually changes)

### D1.1 Syntax

```bash
rg -U 'PATTERN' [PATH...]
rg --multiline 'PATTERN' [PATH...]
```

([Debian Manpages][1])

### D1.2 Semantics (the “rules” you rely on)

When multiline is **disabled** (default):

* `\n` is **explicitly forbidden** in patterns under ripgrep’s default engine; using it yields an error. ([Debian Manpages][1])
* `\p{any}` behaves like “any codepoint except `\n`” (line terminator excluded). ([Debian Manpages][1])

When multiline is **enabled** (`-U`):

* ripgrep lifts the “no line terminator inside a match” restriction:

  * `\n` becomes permitted in patterns.
  * `\p{any}` now includes `\n`. ([Debian Manpages][1])
* There’s **no limit** on how many lines a single match can span. ([Debian Manpages][1])

### D1.3 Immediate “proof” commands (useful to validate assumptions)

```bash
# Default mode: \n is forbidden (default engine) -> error
rg 'foo\nbar' somefile.txt

# Multiline mode: \n allowed -> match can span line breaks
rg -U 'foo\nbar' somefile.txt
```

(Exactly this “\n forbidden vs permitted” behavior is specified for `-U`.) ([Debian Manpages][1])

### D1.4 PCRE2 interaction: “\n without -U can silently do nothing”

If you switch to PCRE2 (`-P` / `--engine=pcre2`) and use `\n` **without** `-U`, ripgrep may **silently fail to match anything** instead of erroring (contrast with the default engine). ([Debian Manpages][1])
Practical rule: if your pattern includes literal `\n` (or you intend newline participation), **always** pass `-U`—even with PCRE2.

---

## D2) `.` does *not* match newline just because you used `-U`

### D2.1 The key caveat

`-U` does **not** change the semantics of `.`: dot still matches any character **except** `\n` unless you enable dotall. ([Debian Manpages][1])

### D2.2 Your three dotall options (pick one; be explicit)

#### Option A — ripgrep-wide dotall in multiline searches: `--multiline-dotall`

```bash
rg -U --multiline-dotall 'foo.*bar' .
```

* Makes `.` match line terminators *when multiline searching is enabled*.
* Has **no effect** if `-U/--multiline` isn’t enabled. ([Debian Manpages][1])

This is explicitly intended as something you might put in an alias / config if you want dotall semantics “by default.” ([Debian Manpages][1])

#### Option B — inline dotall flag: `(?s)` / `(?s:...)`

```bash
rg -U '(?s)foo.*bar' .
rg -U '(?s:foo.*bar)' .
```

Inline `s` means “allow `.` to match `\n`.” ([Docs.rs][2])

#### Option C — avoid dotall entirely: use `\p{any}` (default engine friendly)

```bash
rg -U 'foo\p{any}*bar' .
```

In multiline mode, `\p{any}` includes `\n`, independent of dotall. ([Debian Manpages][1])

### D2.3 Practical pattern templates (copy/paste)

```bash
# Smallest block from BEGIN to END (non-greedy / lazy)
rg -U '(?s)BEGIN.*?END' .

# Match "foo" then later "bar" across any number of lines, but not greedy
rg -U --multiline-dotall 'foo.*?bar' .

# Safer “any char including newline” without dotall toggles
rg -U 'foo\p{any}*?bar' .
```

Rust regex supports lazy quantifiers like `*?`/`+?`/`{n,m}?`. ([Docs.rs][2])

---

## D3) Anchors (`^`, `$`) in multiline search: when you need `(?m)` (and when you don’t)

### D3.1 Why anchors surprise people in `-U` mode

* In **default** ripgrep mode, you’re matching one line at a time; `^...$` naturally acts like “whole line.” ([Debian Manpages][1])
* In **multiline** mode (`-U`), your haystack is effectively multi-line text; now `^` and `$` become “beginning/end of the haystack” **unless** you enable regex multi-line mode `(?m)` (line anchors). ([Docs.rs][2])

Rust regex docs define this precisely:

* `m`: multi-line mode, `^` and `$` match begin/end of line. ([Docs.rs][2])
* Also: `\A` and `\z` are *always* begin/end of haystack, even with `(?m)`. ([Docs.rs][2])

### D3.2 The canonical anchor recipes

#### A) Match an entire file (true “whole-haystack”)

```bash
rg -U '(?s)\A.*\z' file.txt
```

Use `\A` / `\z` when you mean “string anchors only.” ([Docs.rs][2])

#### B) Match blocks that start at the beginning of a line inside the file

```bash
# Find a block starting with "class X" at line start, then anything until next "def " at line start.
rg -U '(?sm)^class\s+\w+.*?^def\s+' src/
```

* `(?m)` makes `^`/`$` line-aware.
* `(?s)` makes `.` cross newlines. ([Docs.rs][2])

#### C) Line-anchored checks without dotall (still multi-line scanning)

```bash
# Find "TODO" that begins a line anywhere in file
rg -U '(?m)^TODO\b' .
```

`(?m)` is the decisive piece here. ([Docs.rs][2])

### D3.3 CRLF nuance (Windows line endings in multiline mode)

Rust regex docs: `^`/`$` are not Unicode-aware line terminators and, in multi-line mode, recognize `\n` unless CRLF mode is enabled. ([Docs.rs][2])
In ripgrep, you can enforce CRLF-as-one-terminator with `--crlf` (or inline `R` in the default engine), which matters for anchors in mixed-line-ending repos. ([Debian Manpages][1])

---

## D4) Memory + IO implications (why `-U` can change performance characteristics drastically)

### D4.1 The hard constraint: “contiguous in memory”

With multiline enabled, ripgrep may need each file “laid out contiguously in memory” (either **heap read** or **memory map**). This can be slower and use more memory. ([Debian Manpages][1])

### D4.2 Stdin / pipes: multiline can force full buffering (kills streaming)

For things that can’t be memory-mapped (notably **stdin**), ripgrep will consume input **until EOF before searching can begin** when multiline matching across line terminators is actually required. ([Debian Manpages][1])

Practical consequence:

```bash
# This may appear to "hang" (it’s waiting for EOF) if the pattern can match \n:
tail -f app.log | rg -U '(?s)ERROR.*?stacktrace'
```

### D4.3 Optimization you can rely on: `-U` is “lazy” if your regex can’t match `\n`

If you provide `-U` but your regex **does not contain patterns that would match `\n`**, ripgrep will **avoid** reading each file into memory before searching it. ([Debian Manpages][1])

This means the cost cliff is triggered by patterns like:

* literal `\n`
* `(?s)` / `--multiline-dotall` used with `.` quantifiers
* `\p{any}` with quantifiers
* any construct that can include newline once `-U` is active ([Debian Manpages][1])

### D4.4 Interaction with mmap knobs: `--mmap` / `--no-mmap`

* `--mmap` enables memory-map searching when possible; stdin/streams can’t use it anyway. ([Debian Manpages][1])
* `--no-mmap` is your stability knob for searching files that may be truncated while being searched (log rotation), but in multiline mode it can push more work into heap reads. ([Debian Manpages][1])

Actionable patterns:

```bash
# Safer for active log directories (avoid mmap abort on truncation)
rg --no-mmap -U '(?s)ERROR.*?Traceback' /var/log

# If you need multiline + want mmap when possible (large static files)
rg --mmap -U '(?s)BEGIN.*?END' huge_static_file.txt
```

(What mmap does/doesn’t do, and truncation-abort warning, are explicitly documented.) ([Debian Manpages][1])

---

## D5) Flag interactions that change “what counts as a match” (and what gets counted)

### D5.1 `--max-count` counting behavior changes under `-U`

`--max-count` normally limits matching **lines per file**, but with multiline enabled, “a single match that spans multiple lines is only counted once” for this limit. ([Debian Manpages][1])

```bash
# Stop after the first multi-line match per file
rg -U --max-count=1 '(?s)BEGIN.*?END' .
```

### D5.2 `--stop-on-nonmatch` vs `-U`: precedence + why you should avoid mixing

* `-U/--multiline` overrides `--stop-on-nonmatch`. ([Debian Manpages][1])
* `--stop-on-nonmatch` is documented as overriding `-U/--multiline` too. ([Debian Manpages][1])
  Given ripgrep’s “last flag wins” precedence rule, whichever appears later on the CLI will dominate. ([Debian Manpages][1])

Practical rule: don’t combine them unless you are intentionally ordering them for an override.

---

## D6) Minimal “decision matrix” (what to type when)

```bash
# Need cross-line match + dot crosses newline
rg -U --multiline-dotall 'A.*?B' .

# Need cross-line match but prefer explicit “any char” (no dotall)
rg -U 'A\p{any}*?B' .

# Need line anchors to work inside a multiline file scan
rg -U '(?m)^foo.*$' .

# Need both: line anchors + dot crosses newline
rg -U '(?sm)^BEGIN\b.*?^END\b' .
```

(These compose directly from `-U` semantics + regex `m`/`s` flags + dotall rules.) ([Debian Manpages][1])

[1]: https://manpages.debian.org/testing/ripgrep/rg.1.en.html "rg(1) — ripgrep — Debian testing — Debian Manpages"
[2]: https://docs.rs/regex/latest/regex/ "regex - Rust"

## E) Pattern sources + multi-pattern semantics (positional `PATTERN` vs `-e/-f`, dash-leading patterns, `--` delimiter, OR semantics)

### E1) The parsing rule that drives everything: **where does the pattern come from?**

`rg` has exactly one “pattern set” per invocation, sourced from **either**:

* positional `PATTERN`, **or**
* one/more `-e/--regexp` patterns, **and/or**
* one/more `-f/--file` pattern files. ([Debian Manpages][1])

The critical behavior change:

> When `-f/--file` or `-e/--regexp` is used, ripgrep treats **all positional arguments as files or directories to search**. ([Debian Manpages][1])

#### “I meant `bar` as a pattern, why is it treated as a PATH?”

```bash
# WRONG mental model: user thinks `bar` is a second pattern
rg -e 'foo' 'bar'

# ACTUAL meaning: pattern set = {'foo'}, PATH list = {'bar'}
# (so you’ll get “No such file or directory: bar” unless bar exists as a path)
```

This is explicitly defined in the `-e/-f` docs. ([Debian Manpages][1])

**Correct forms**

```bash
# multiple patterns: use multiple -e
rg -e 'foo' -e 'bar' .

# OR: pattern file for bulk patterns
rg -f patterns.txt .
```

([Debian Manpages][1])

---

### E2) Positional `PATTERN` vs `-e/--regexp` (and patterns that begin with `-`)

#### Positional pattern form

```bash
rg [OPTIONS] PATTERN [PATH...]
```

([Debian Manpages][1])

#### Dash-leading pattern (classic “option parsing” trap)

If the pattern begins with `-`, the man page is explicit: use `-e` (or `--`). ([Debian Manpages][1])

Two canonical disambiguations:

```bash
# 1) Use -e/--regexp
rg -e -foo .

# 2) Use -- to stop flag parsing
rg -- -foo .
```

Both are documented as equivalent. ([Debian Manpages][1])

---

### E3) Pattern files: `-f/--file=PATTERNFILE` (and `PATTERNFILE = -`)

#### Syntax + semantics

```bash
rg -f PATTERNFILE [PATH...]
rg -f PATTERNFILE1 -f PATTERNFILE2 [PATH...]
```

Key guarantees:

* **one pattern per line**
* the **newline is not part of the pattern**
* **empty pattern lines match all input lines** (high-impact footgun)
* `PATTERNFILE = -` means **read patterns from stdin** ([Debian Manpages][1])

```bash
# Search for every pattern in patterns.txt (OR semantics)
rg -f patterns.txt src/

# Read patterns from stdin (NOT search input) then search src/
printf '%s\n' 'TODO' 'FIXME' | rg -f - src/
```

([Debian Manpages][1])

#### The “empty line matches everything” footgun (what to do)

Because an empty pattern matches all lines, *any blank line* in your pattern file makes the whole search far less selective. ([Debian Manpages][1])

Practical hardening workflow:

```bash
# sanity-check pattern file before running (shows blank lines)
nl -ba patterns.txt | sed -n 's/^[[:space:]]*[0-9]\+[[:space:]]*$/** BLANK **/p'
```

(That’s a shell-side guard; ripgrep itself does what the man page says.)

---

### E4) “Any-of” semantics across multiple `-e` and/or `-f`

This is not “AND” and not “sequence” — it’s **OR**:

* `-e` can be provided multiple times; **lines matching at least one** are printed. ([Debian Manpages][1])
* `-f` can be provided multiple times (and/or combined with `-e`); **a line is printed iff it matches at least one** of the patterns. ([Debian Manpages][1])

```bash
# any-of across flags and files:
rg -e 'panic!' -e 'unwrap\(' -f patterns.txt src/
```

([Debian Manpages][1])

---

### E5) Stdin ambiguity: patterns-from-stdin vs search-input-from-stdin

Two different things can read stdin:

1. **Search input**: `command | rg PATTERN` (rg reads stdin as the text to search) ([Debian Manpages][1])
2. **Patterns input**: `rg -f - PATH...` (rg reads stdin as patterns) ([Debian Manpages][1])

If you try to do both with the same stdin, you’re fighting physics. The reliable pattern is: **put patterns in a file (or process substitution) if search input is piped**.

```bash
# search stdin, patterns from file:
git diff | rg -f patterns.txt

# bash-only: patterns from process substitution, search input from pipe
git diff | rg -f <(printf '%s\n' 'TODO' 'FIXME')
```

---

## F) Regex engine selection + “capability matrix” (default vs PCRE2 vs auto/hybrid)

### F1) Default engine (Rust regex): finite automata + linear-time guarantees

The man page states the contract:

* default regex engine uses **finite automata** and guarantees **linear-time searching**
* therefore **backreferences** and **arbitrary look-around** are not supported ([Debian Manpages][1])

The underlying Rust `regex` crate explains *why*:

* it “lacks several features that are not known how to implement efficiently,” including **look-around** and **backreferences**
* in exchange it provides worst-case time bounds (documented as `O(m * n)` for the crate’s search model) ([Docs.rs][2])

**Operational takeaway**

* If you can express it without lookaround/backrefs, default engine is typically the fastest and most predictable. ([Debian Manpages][1])

---

### F2) PCRE2 enablement: `-P/--pcre2` and `--engine=…`

#### Force PCRE2

```bash
rg -P 'PATTERN' [PATH...]
# equivalent to:
rg --engine=pcre2 'PATTERN' [PATH...]
```

Equivalence + purpose (look-around/backrefs) + “PCRE2 may be absent” are explicitly documented. ([Debian Manpages][1])

If PCRE2 was not compiled into your `rg`, then `-P` / `--engine=pcre2` prints an error and exits. ([Debian Manpages][1])

#### Choose engine explicitly

```bash
rg --engine=default 'PATTERN' [PATH...]
rg --engine=pcre2   'PATTERN' [PATH...]
rg --engine=auto    'PATTERN' [PATH...]
```

Key rule: the engine choice applies to **every regex provided** (positional PATTERN, all `-e`, all `-f`). ([Debian Manpages][1])

Also: `--engine=…` overrides prior uses of `-P/--pcre2` and the deprecated `--auto-hybrid-regex`. ([Debian Manpages][1])

---

### F3) Auto/hybrid selection: “use PCRE2 only when needed” (and why it can surprise you)

`--engine=auto` is the supported way to do hybrid selection. ([Debian Manpages][1])
The older `--auto-hybrid-regex` exists but is **deprecated** (“Use --engine instead”). ([Debian Manpages][1])

**How auto/hybrid behaves (as specified):**

* ripgrep tries to compile with the default engine whenever possible
* if compilation fails under default and PCRE2 is available, it switches to PCRE2
* if PCRE2 is unavailable, auto/hybrid has no effect (only one engine exists)
* the decision is based on **static analysis of the patterns**, may evolve over time
* the downside: it may not be obvious which engine was used; semantics/perf can subtly change ([Debian Manpages][1])

```bash
# “prefer fast default, but allow lookaround/backrefs transparently”
rg --engine=auto '(?<=class\s)Foo' src/
```

([Debian Manpages][1])

---

### F4) Engine capability matrix (practical, not philosophical)

**default (Rust regex)**

* ✅ finite automata / linear-time guarantee ([Debian Manpages][3])
* ❌ look-around, ❌ backreferences ([Debian Manpages][3])

**pcre2**

* ✅ look-around, ✅ backreferences (if build includes PCRE2) ([Debian Manpages][1])
* ⚠ optional feature; may be missing from packaged builds ([Debian Manpages][1])

**auto**

* ✅ “default unless pattern can’t compile; then PCRE2” (best-effort) ([Debian Manpages][1])
* ⚠ can be non-obvious which engine ran; heuristics may change ([Debian Manpages][1])

---

## G) Literal search mode + “which regex syntax doc applies?”

### G1) Literal mode: `-F/--fixed-strings` (global)

```bash
rg -F 'LITERAL' [PATH...]
rg --fixed-strings 'LITERAL' [PATH...]
```

Semantics (documented):

* Treat **all patterns as literals** (no regex parsing)
* regex metacharacters like `.(){}*+` generally do not need escaping
* can be disabled with `--no-fixed-strings` ([Debian Manpages][1])

```bash
# literal dot, literal brackets, literal parens
rg -F 'foo.bar' .
rg -F '[tool.ruff]' pyproject.toml
```

([Debian Manpages][1])

---

### G2) “Which syntax doc applies?” (rule you should codify for agents)

The ripgrep man page states this directly:

* Default engine: consult Rust `regex` syntax docs (and the byte-oriented flavor). ([Debian Manpages][1])
* PCRE2 engine (`-P` / `--engine=pcre2`): consult PCRE2 documentation / PCRE2 man pages. ([Debian Manpages][1])

**Default engine references**

* Rust `regex` (overall syntax + semantics): ([Debian Manpages][1])
* “byte-oriented regex” notes (ripgrep uses byte-oriented regexes): ([Debian Manpages][1])

**PCRE2 references**

* PCRE2 full pattern reference (`pcre2pattern`): ([PCRE][4])
* PCRE2 quick syntax summary (`pcre2syntax`): ([PCRE][5])

---

### G3) Fast operational guardrails (agent-friendly)

```bash
# 1) Need lookbehind/backrefs? force pcre2 (and fail fast if missing)
rg --engine=pcre2 '(?<=foo)bar' .

# 2) Want “pcre2 only when required”? use auto
rg --engine=auto '(?<=foo)bar' .

# 3) Don’t want regex at all? use fixed-strings
rg -F '(?<=foo)bar' .

# 4) Pattern begins with "-"? use -e or -- delimiter
rg -e -foo .
rg -- -foo .
```

All behaviors above are explicitly specified in `rg(1)`. ([Debian Manpages][1])

[1]: https://manpages.debian.org/testing/ripgrep/rg.1.en.html "rg(1) — ripgrep — Debian testing — Debian Manpages"
[2]: https://docs.rs/regex/latest/src/regex/lib.rs.html "lib.rs - source"
[3]: https://manpages.debian.org/testing/ripgrep/rg.1.en.html?utm_source=chatgpt.com "rg(1) — ripgrep — Debian testing"
[4]: https://pcre.org/current/doc/html/pcre2pattern.html "pcre2pattern specification"
[5]: https://www.pcre.org/current/doc/html/pcre2syntax.html?utm_source=chatgpt.com "pcre2syntax specification"

## H) Regex syntax + match operators (default Rust engine vs PCRE2, byte/Unicode mode, anchors/“whole line”)

### H0) First decision: **which syntax rules apply right now?**

Ripgrep has three mutually exclusive “parsing regimes” for `PATTERN` (and all `-e/-f` patterns):

1. **Fixed strings**: `-F/--fixed-strings` → *no regex parsing* (every pattern is literal). ([Debian Manpages][1])
2. **Default regex engine**: Rust `regex` syntax (no lookaround/backrefs) and **byte-oriented** matching. ([Debian Manpages][1])
3. **PCRE2 regex engine**: full PCRE2 pattern syntax (lookaround/backrefs, etc.) via `-P` or `--engine=pcre2`. ([Debian Manpages][1])

Critical globality rule:

* `--engine=ENGINE` applies to **every regex provided** (positional `PATTERN` + every `-e` + every line in every `-f`). ([Debian Manpages][1])

```bash
# All three patterns run under the same engine:
rg --engine=auto -e 'p1' -e 'p2' -f patterns.txt src/
```

---

## H1) Default engine = Rust `regex` (byte-oriented) — “feature families you actually use”

### H1.1 One-character atoms + Unicode properties

Rust regex’s “one-char” atoms (these are the building blocks you compose with repetition / grouping):

```regex
.             any character except new line (unless s flag)
[0-9]         any ASCII digit
\d            digit (Unicode Nd)
\D            not digit
\pX           Unicode class by one-letter name
\p{Greek}     Unicode class by name (script/category/etc.)
\PX / \P{...} negated Unicode classes
```

([Docs.rs][2])

Action patterns:

```bash
# Find any Greek letters:
rg '\p{Greek}+' .

# Mixed scripts/categories (e.g., numerals OR Greek OR Cherokee):
rg '[\pN\p{Greek}\p{Cherokee}]+' .
```

([Docs.rs][2])

---

### H1.2 Bracket character classes + set algebra (intersection/diff/symdiff)

Rust regex supports nested classes + set operations (this is where it gets “compiler-y”):

```regex
[xyz]         union
[^xyz]        negation
[a-z]         range
[[:alpha:]]   ASCII class ([A-Za-z])
[x[^xyz]]     nested class
[a-y&&xyz]    intersection
[0-9&&[^4]]   subtraction via intersection + negation
[0-9--4]      direct subtraction
[a-g~~b-h]    symmetric difference
[\[\]]        escaping inside [...]
```

([Docs.rs][2])

High-leverage usage patterns:

```bash
# “identifier-ish” but exclude leading underscore:
rg '\b[[:alpha:]][[:word:]]*\b' .

# Hex digits (ASCII) without ambiguity:
rg '[0-9A-Fa-f]+' .

# Match Greek letters only (Greek ∩ Letter):
rg '[\p{Greek}&&\pL]+' .
```

([Docs.rs][2])

---

### H1.3 Perl classes (`\w \d \s`) + “ASCII-only” variants

In the default engine, `\d \s \w` are Unicode-aware by default and are defined in terms of Unicode properties. ([Docs.rs][2])

But you often want ASCII-only for speed and precision. Rust regex gives multiple equivalent forms (example shown for digits):

```regex
[0-9]
(?-u:\d)
[[:digit:]]
[\d&&\p{ascii}]
```

([Docs.rs][2])

Practical ripgrep patterns:

```bash
# ASCII digits only (safe + fast):
rg '[0-9]{4}-[0-9]{2}-[0-9]{2}' .

# ASCII-only word boundary (often faster than Unicode-aware \b):
rg '(?-u:\b)[A-Za-z_][A-Za-z0-9_]*(?-u:\b)' .
```

([Docs.rs][2])

---

### H1.4 Anchors + boundaries (haystack vs line vs word)

Rust regex’s boundary primitives (these become especially important once you use `-U` or `(?m)`):

```regex
^   beginning of haystack (or start-of-line with (?m))
$   end of haystack (or end-of-line with (?m))
\A  only beginning of haystack (even with (?m))
\z  only end of haystack (even with (?m))

\b  Unicode word boundary
\B  not a Unicode word boundary

\b{start}, \<        start-of-word boundary
\b{end},   \>        end-of-word boundary
\b{start-half}       left half of start-of-word boundary
\b{end-half}         right half of end-of-word boundary
```

([Docs.rs][2])

Anchor patterns you’ll use constantly:

```bash
# Whole line match (manual):
rg '^ERROR: .*' .

# Whole file / whole haystack match:
rg '\A[0-9]{4}-[0-9]{2}-[0-9]{2}\z' file.txt

# Start-of-word / end-of-word without relying on full \b:
rg '\b{start}foo\b{end}' .
```

([Docs.rs][2])

---

### H1.5 Grouping + inline flags (this is how you localize semantics)

Grouping forms:

```regex
(exp)          numbered capture group
(?P<name>exp)  named capture group
(?<name>exp)   named capture group
(?:exp)        non-capturing group
(?flags)       set/clear flags for current group
(?flags:exp)   set/clear flags for exp (non-capturing)
```

([Docs.rs][2])

Inline flags supported by Rust regex:

* `i` case-insensitive
* `m` multi-line (`^`/`$` become line-anchors)
* `s` dot matches `\n`
* `R` CRLF mode (anchors won’t match *between* `\r` and `\n`)
* `U` swap greedy vs lazy meaning
* `u` Unicode support (**enabled by default**)
* `x` verbose mode (ignore whitespace, `#` comments) ([Docs.rs][2])

Action patterns:

```bash
# Mixed case handling inside one pattern:
rg '(?i)a+(?-i)b+' .

# Line-anchored inside a file:
rg '(?m)^def\s+\w+' src/

# CRLF-safe anchors (inline):
rg '(?mR)^foo$' .
```

([Docs.rs][2])

---

### H1.6 Byte-oriented + Unicode mode controls (`--no-unicode` and `(?-u:...)`)

Ripgrep explicitly documents that it uses **byte-oriented** regexes (and points to the Rust `regex::bytes` docs for the extra semantics). ([Debian Manpages][1])

**Unicode mode (on by default)** affects fundamental matching:

* `.` matches valid UTF-8 scalar values (not arbitrary bytes)
* `\w \s \d` are Unicode-aware
* `\b` uses Unicode word definition
* lots of `\p{…}` classes are available (and the set varies by engine) ([Debian Manpages][1])

Disable Unicode mode globally:

```bash
rg --no-unicode 'pattern' .
```

([Debian Manpages][1])

Or disable locally:

```bash
# ASCII-only \w inside a Unicode-enabled search:
rg '(?-u:\w)+' .
```

([Docs.rs][2])

Byte-level implications you should internalize:

* When Unicode is enabled, `.` matches a *Unicode scalar value* (may be multiple bytes).
* When Unicode is disabled (e.g., `(?-u:.)`), `.` matches a **single byte**. ([Docs.rs][2])

If you’re intentionally matching **invalid UTF-8 bytes**, the `regex::bytes` model allows disabling Unicode even when it would otherwise break UTF-8 invariants (example shown with `(?-u)` and byte escapes). ([Docs.rs][3])

---

## H2) PCRE2-only deltas (lookaround + backreferences) — and `--engine` globality

### H2.1 Enabling PCRE2 (and what it means)

* `-P/--pcre2` is equivalent to `--engine=pcre2`. ([Debian Manpages][1])
* PCRE2 is optional; if your build lacks it, `--engine=pcre2` / `-P` errors. ([Debian Manpages][1])
* Engine choice applies to **all** provided patterns (`PATTERN`, `-e`, `-f`). ([Debian Manpages][1])

```bash
# Force PCRE2 for the whole run:
rg --engine=pcre2 '...' .
# shorthand:
rg -P '...' .
```

([Debian Manpages][1])

### H2.2 Lookahead / lookbehind (PCRE2 assertions)

PCRE2 lookaround syntax (these are **assertions**: they don’t consume characters):

* Positive lookahead: `(?=...)`
* Negative lookahead: `(?!...)` ([pcre.org][4])

```bash
# Word followed by semicolon (semicolon not consumed):
rg -P '\w+(?=;)' .

# "foo" not followed by "bar":
rg -P 'foo(?!bar)' .
```

([pcre.org][4])

Lookbehind syntax:

* Positive lookbehind: `(?<=...)`
* Negative lookbehind: `(?<!...)` ([pcre.org][4])

```bash
# "bar" not preceded by "foo":
rg -P '(?<!foo)bar' .
```

([pcre.org][4])

Lookbehind restrictions matter operationally:

* PCRE2 requires a **known maximum length** for lookbehind alternatives; unlimited repetition like `\d*` isn’t allowed inside lookbehind. ([pcre.org][4])

### H2.3 Backreferences (PCRE2)

PCRE2 backreferences match the *exact text* previously captured:

* Numeric backref example: `(sens|respons)e and \1ibility` matches “sense and sensibility” / “response and responsibility”. ([pcre.org][4])
* Named capture + named backref variants are supported (e.g., `\k<name>`, `\k{name}`, `(?P=name)`, `\g{name}`). ([pcre.org][4])

```bash
# Ensure matching quotes via backreference (PCRE2):
rg -P 'title=(["'\'']).*?\1' .

# Named capture + named backreference:
rg -P '(?<q>["'\'']).*?\k<q>' .
```

([pcre.org][4])

---

## H3) Line-boundary anchored *modes* in ripgrep (`-x`, `-w`) — “wrappers” around every pattern

These are `rg` match-operator flags (not regex syntax) that **rewrite your effective pattern**.

### H3.1 `-x/--line-regexp` = whole-line match

`-x` is equivalent to surrounding **every pattern** with `^` and `$` (prints only lines where the entire line participates in a match). ([Debian Manpages][1])

```bash
# Match lines that are exactly "pass"
rg -x 'pass' src/

# With multiple patterns, each is wrapped:
rg -x -e 'pass' -e 'return' src/
```

([Debian Manpages][1])

### H3.2 `-w/--word-regexp` = whole-word match (word boundaries)

`-w` wraps every pattern with `\b{start-half}` and `\b{end-half}` (word-boundary halves). ([Debian Manpages][1])

```bash
# Match foo as a whole word (not foobar)
rg -w 'foo' .

# Equivalent-ish manual form:
rg '\b{start-half}foo\b{end-half}' .
```

([Debian Manpages][1])

Override rules:

* `-x` overrides `-w` and `-w` overrides `-x` (last/stronger semantics win as documented). ([Debian Manpages][1])

### H3.3 Unicode vs ASCII word boundaries (why `-w` can be “too smart”)

By default, word boundaries use the Unicode definition of word characters. ([Debian Manpages][1])
If you want ASCII word boundaries (common for source code token-ish searches), use either:

* `--no-unicode` (global), or
* `(?-u:\b)` (local) ([Debian Manpages][1])

```bash
# ASCII word-boundary search:
rg --no-unicode -w 'foo' .

# Local ASCII boundary inside regex:
rg '(?-u:\b)foo(?-u:\b)' .
```

([Debian Manpages][1])

---

## H4) Fast “what should I use?” recipes

```bash
# Need whole-line matches:
rg -x '^\s*from\s+\w+' src/

# Need whole-word matches, ASCII semantics:
rg --no-unicode -w 'yield' src/

# Need advanced assertions/backrefs:
rg -P '(?<=class\s)\w+' src/
rg -P '(?<q>["'\'']).*?\k<q>' .

# Want default engine but still precise boundaries/flags:
rg '(?m)^\s*def\s+\w+' src/
```

([Debian Manpages][1])

[1]: https://manpages.debian.org/testing/ripgrep/rg.1.en.html "rg(1) — ripgrep — Debian testing — Debian Manpages"
[2]: https://docs.rs/regex/latest/regex/ "regex - Rust"
[3]: https://docs.rs/regex/latest/regex/bytes/index.html "regex::bytes - Rust"
[4]: https://pcre.org/current/doc/html/pcre2pattern.html "pcre2pattern specification"

## I) Recursive traversal and search space definition (PATH roots, recursion, symlinks, hidden, unrestricted)

### I0) Baseline traversal model (what happens with *no flags*)

* If you omit `PATH`, `rg` **recursively searches the current directory**. ([Debian Manpages][1])
* In recursive mode, `rg` applies “smart filtering” by default: it respects ignore rules, skips hidden files/dirs, skips binary files, and does **not** follow symlinks. ([Debian Manpages][1])

**Canonical form**

```bash
rg 'PATTERN'                 # recursive search from CWD
rg 'PATTERN' src tests docs  # recursive search from multiple roots
```

([Debian Manpages][1])

---

### I1) PATH semantics: file vs directory, and “explicit paths override ignore/glob”

#### I1.1 Files vs directories

`PATH` can be a file or directory:

* **Directories are searched recursively**
* **File paths specified explicitly** on the command line override glob and ignore rules ([Debian Manpages][1])

```bash
rg 'needle' pyproject.toml     # search one file
rg 'needle' src/               # recurse under src/
rg 'needle' src/ tests/        # multiple roots
```

([Debian Manpages][1])

#### I1.2 Explicit path override: how to use it deliberately

If you suspect something is being skipped due to ignore/glob rules, pass the file explicitly:

```bash
# “Search this exact file even if it’s ignored”
rg 'needle' path/to/ignored_file.txt
```

This works because explicit file paths override glob/ignore rules. ([Debian Manpages][1])

> Practical nuance: if you pass a **directory root** (`rg needle docs/`), users commonly observe that ignore patterns can behave differently than “search from repo root” (because ignore pattern matching is path-relative). If you see “ignored folder suddenly searched when I pass a subdir,” rewrite ignore patterns using `**/...` or switch to explicit `-g '!…'` globs. ([til.codeinthehole.com][2])

---

### I2) Traversal *shape* controls (depth + filesystem boundaries)

#### I2.1 `-d/--max-depth=NUM`: cap recursion depth

**Syntax**

```bash
rg -d 0 'PATTERN' dir/
rg --max-depth 1 'PATTERN' dir/
```

**Semantics**

* Limits traversal to `NUM` levels beyond the given roots
* `0` means “only the explicit paths themselves” (no descent)
* `1` means “only direct children” ([Debian Manpages][1])

```bash
rg --max-depth 0 'needle' src/   # no-op: does not descend into src/
rg --max-depth 1 'needle' src/   # only src/* (one level)
```

([Debian Manpages][1])

#### I2.2 `--one-file-system`: don’t cross mount boundaries per root

**Syntax**

```bash
rg --one-file-system 'PATTERN' /var/log
rg --one-file-system 'PATTERN' /foo/bar /quux/baz
```

**Semantics**

* For each `PATH` root, `rg` will not cross filesystem boundaries while traversing that root’s directory tree
* Similar to `find -xdev` / `-mount` ([Debian Manpages][1])

---

### I3) Hidden files/dirs: default skip, explicit include, and “hidden whitelisting”

#### I3.1 Default behavior + `-./--hidden`

By default, hidden files and directories are skipped. ([Debian Manpages][1])

**Syntax**

```bash
rg -.'PATTERN'          # (shell-safe form is usually: rg --hidden 'PATTERN')
rg -.\ 'PATTERN'        # avoid; use --hidden
rg --hidden 'PATTERN' .
```

**Semantics**

* `--hidden` searches hidden files and directories
* “Hidden” means basename starts with `.` (and on Windows, hidden attribute also counts) ([Debian Manpages][1])

```bash
rg --hidden 'needle' .
```

([Debian Manpages][1])

#### I3.2 Two important exceptions (why hidden content may show up “unexpectedly”)

Even **without** `--hidden`, a hidden file/dir can be searched if:

* it is **whitelisted** in an ignore file, or
* it is **given explicitly** as an argument ([Debian Manpages][1])

```bash
rg 'needle' .env                 # explicit hidden file
rg 'needle' .config/myapp/       # explicit hidden dir root
```

([Debian Manpages][1])

#### I3.3 `.git` gotcha

`--hidden` will include `.git` even if you’ve disabled VCS ignores; excluding it requires an explicit ignore/glob. ([Debian Manpages][1])

```bash
# include hidden but explicitly exclude .git
rg --hidden -g '!.git/**' 'needle' .
```

`-g/--glob` overrides other ignore logic; later globs win. ([Debian Manpages][1])

---

### I4) “Unrestricted” modes: `-u`, `-uu`, `-uuu` (smart-filtering ladders)

#### I4.1 The ladder

`-u` can be repeated up to 3 times; each repetition reduces smart filtering further. ([Debian Manpages][1])

* `-u`  ≡ `--no-ignore`
* `-uu` ≡ `--no-ignore --hidden`
* `-uuu` ≡ `--no-ignore --hidden --binary` ([Debian Manpages][1])

```bash
rg -u  'needle' .   # ignore rules off, but still skips hidden/binary/symlinks
rg -uu 'needle' .   # + include hidden
rg -uuu 'needle' .  # + search binary (and broadly “grep -r”-like)
```

([Debian Manpages][1])

#### I4.2 What `-uuu` still does *not* do

Even with `-uuu`, `rg` still:

* skips symlinks unless you add `-L/--follow`
* may avoid printing binary matches unless you explicitly treat as text (`-a/--text`) ([Debian Manpages][1])

---

### I5) Symlink strategy: `-L/--follow`, loops, broken links, suppression

#### I5.1 Default: symlinks are not followed

Symlink non-following is one of the default automatic filters. ([Debian Manpages][1])

#### I5.2 `-L/--follow`: follow symlinks during directory traversal

**Syntax**

```bash
rg -L 'PATTERN' .
rg --follow 'PATTERN' .
```

**Semantics**

* Enables symlink following while traversing directories (off by default)
* Detects and reports **symlink loops**
* Reports errors for **broken links**
* Use `--no-messages` to suppress these file-open/read error messages ([Debian Manpages][1])

```bash
# Follow symlinks (may emit loop/broken-link errors)
rg --follow 'needle' .

# Follow symlinks but suppress file-open/read errors
rg --follow --no-messages 'needle' .
```

([Debian Manpages][1])

#### I5.3 Message suppression vs exit status (scripting footgun)

* `--no-messages` suppresses some error messages related to failing to open/read files; pattern syntax errors still show. ([Debian Manpages][1])
* Separately, `rg` uses exit status `2` if *any* error occurred, including “soft errors” like unable to read a file. (Assume a broken symlink still counts as an error even if you suppress the message.) ([Debian Manpages][1])

---

### I6) Debugging “what exactly is being searched?”

These are the two fastest “search space inspectors”:

#### I6.1 `--files`: enumerate the candidate file set without searching

```bash
rg --files
rg --files src/
```

Useful for validating ignore/hidden/symlink behavior before running expensive searches. ([Debian Manpages][1])

#### I6.2 `--debug`: explain why files were skipped

```bash
rg --debug 'needle' .
```

`--debug` is specifically called out as useful for figuring out why a file was skipped (and should mention skipped files and reasons). ([Debian Manpages][1])

---

If you want to keep marching in outline order, the next high-ROI deep dive is the **ignore stack + glob/type filtering** (how `.gitignore`/`.ignore`/`.rgignore` precedence interacts with `-g`, `--ignore-file`, `--no-ignore-*`, and `-t/-T`), since that’s the other half of “search space definition.”

[1]: https://manpages.debian.org/testing/ripgrep/rg.1.en.html "rg(1) — ripgrep — Debian testing — Debian Manpages"
[2]: https://til.codeinthehole.com/posts/how-ripgrep-interprets-gitignore-rules-changes-when-a-filepath-argument-is-used/ "
    TIL How 'ripgrep' interprets '.gitignore' rules changes when a filepath argument is used — David Winterbottom
"

## J) Automatic filtering: ignore stack + “smart filtering tiers” (`-u/-uu/-uuu`), debugging, noise control

### J0) The execution model: *candidate enumeration → automatic filters → matching*

When you run `rg PATTERN [PATH...]` in recursive mode, ripgrep first enumerates the directory tree, then applies **automatic “smart filtering”** to decide *what not to search*, and only then runs the matcher on the remaining files. This is a major differentiator from “classic `grep -r`,” and it’s the source of most “why didn’t it match?” confusion. ([Debian Manpages][1])

---

## J1) Default filtering behaviors (what gets excluded without you asking)

### J1.1 Ignore rules: `.gitignore` + friends

By default, ripgrep respects ignore rules and will ignore files/directories that match globs from multiple ignore sources (detailed in J2). ([Debian Manpages][1])

### J1.2 Hidden files/dirs

By default, hidden files/dirs are skipped. Hidden means basename starts with `.` (and on some OSes “hidden attribute” also counts). ([Debian Manpages][1])

### J1.3 Binary files

By default, binary files are skipped/short-circuited using a **NUL-byte heuristic**. Once a NUL byte is seen, ripgrep stops searching that file in default behavior. ([Debian Manpages][1])

### J1.4 Symlinks

By default, symlinks are not followed. ([Docs.rs][2])

---

## J2) Ignore stack anatomy: *which ignore files exist, where they’re discovered, and precedence*

### J2.1 What “ignore stack” means in ripgrep

In recursive directory traversal, ripgrep automatically considers **three on-disk ignore file categories** plus Git’s repo/global excludes:

1. **VCS ignores**: `.gitignore` (and related Git-specific sources like `.git/info/exclude` and global `core.excludesFile`). ([Docs.rs][2])
2. **App-agnostic ignores**: `.ignore` (works like `.gitignore` rules, but not tied to git). ([Docs.rs][2])
3. **ripgrep-specific ignores**: `.rgignore` (same rule language, highest precedence among the three file types). ([Docs.rs][2])

Key detail: ripgrep will also respect `.ignore` / `.rgignore` files in **parent directories**, and `.gitignore` files in parent directories **within the same git repository**. ([Docs.rs][2])

### J2.2 Precedence: “later overrides earlier”

The `rg(1)` man page gives an explicit precedence list (later items override earlier items):

* `--ignore-file` files (lowest precedence)
* global gitignore rules (e.g., `$HOME/.config/git/ignore`)
* `.git/info/exclude`
* `.gitignore`
* `.ignore`
* `.rgignore` (highest precedence) ([Debian Manpages][1])

Concrete implication (whitelist override example): if `foo` is ignored in `.gitignore` but whitelisted as `!foo` in `.rgignore`, then `foo` is **not ignored** because `.rgignore` overrides `.gitignore`. ([Debian Manpages][1])

### J2.3 Why `.ignore` / `.rgignore` exist (typical workflow)

ripgrep’s guide explicitly frames `.ignore` / `.rgignore` as the mechanism to **override** Git ignore rules without changing git tracking semantics (e.g., search `log/` while keeping it untracked). ([Docs.rs][2])

Example pattern (in repo root):

```gitignore
# .gitignore
log/
```

```gitignore
# .ignore  (or .rgignore)
!log/
```

This causes ripgrep to search `log/` even though Git ignores it. ([Docs.rs][2])

### J2.4 `.gitignore` applicability outside a git repo: `--no-require-git`

By default, ripgrep only respects source-control ignore files like `.gitignore` when it detects it is inside a source control repository (e.g., a `.git` directory exists). `--no-require-git` relaxes that and makes `.gitignore` rules apply even if repo metadata is missing (common for vendored/copy-only trees). ([Debian Manpages][1])

---

## J3) Controlling the ignore stack: `--no-ignore` and the fine-grained `--no-ignore-*` switches

### J3.1 The “big hammer”: `--no-ignore` (alias: `-u` once)

`--no-ignore` disables ignore files like `.gitignore`, `.ignore`, `.rgignore`. It also implies several narrower toggles (`--no-ignore-dot`, `--no-ignore-exclude`, `--no-ignore-global`, `--no-ignore-parent`, `--no-ignore-vcs`). ([Debian Manpages][1])

**Important nuance:** `--no-ignore` does **not** imply `--no-ignore-files` (i.e., it does not automatically disable explicit `--ignore-file …` rules, because those are command-line supplied). ([Debian Manpages][1])

Common usage:

```bash
# ignore-stack off (but still skips hidden + binary unless you add more flags)
rg --no-ignore 'needle' .
# shorthand
rg -u 'needle' .
```

([Debian Manpages][1])

### J3.2 Fine-grained controls (use these when you want *some* ignores but not others)

These flags are the “surgical” tools for debugging or specialized policies:

* `--no-ignore-vcs`: don’t respect VCS ignore files (e.g., `.gitignore`). Also implies `--no-ignore-parent` for VCS ignore files. ([Debian Manpages][1])
* `--no-ignore-dot`: don’t respect `.ignore` or `.rgignore` (but still respects `.gitignore`). Note: this is unrelated to dot-prefixed *hidden* filenames; that’s `--hidden`. ([Debian Manpages][1])
* `--no-ignore-parent`: don’t apply ignore files discovered in parent directories (ripgrep normally ascends parents). ([Debian Manpages][1])
* `--no-ignore-global`: don’t respect global gitignore rules like `core.excludesFile` defaults. ([Debian Manpages][1])
* `--no-ignore-exclude`: don’t respect repo-specific excludes like `.git/info/exclude`. ([Debian Manpages][1])
* `--no-ignore-files`: ignore any `--ignore-file …` arguments (even ones after this flag). ([Debian Manpages][1])

### J3.3 Adding custom ignore rules: `--ignore-file=PATH`

`--ignore-file=PATH` lets you inject one or more additional gitignore-formatted rule files. The man page defines two high-stakes semantics:

* They’re matched **relative to the current working directory**
* They have **lower precedence** than ignore files discovered in the directory tree (and the formal precedence list places `--ignore-file` lowest). ([Debian Manpages][1])

```bash
# Add an ad-hoc ignore rule file (gitignore syntax) for this invocation:
rg --ignore-file ~/.config/rg/extra.ignore 'needle' .
```

([Debian Manpages][1])

---

## J4) “Smart filtering tiers”: `-u`, `-uu`, `-uuu` (and what remains filtered)

### J4.1 The tier ladder (documented equivalences)

Ripgrep defines `-u/--unrestricted` as a convenience flag you can repeat up to three times:

* `-u`   ≡ `--no-ignore`
* `-uu`  ≡ `--no-ignore --hidden`
* `-uuu` ≡ `--no-ignore --hidden --binary` ([Debian Manpages][1])

This is the fastest “am I being filtered?” diagnostic:

```bash
rg 'needle' .
rg -u 'needle' .
rg -uu 'needle' .
rg -uuu 'needle' .
```

([Docs.rs][2])

### J4.2 What `-uuu` *still* does (and how to remove those last barriers)

Even with `-uuu`, ripgrep still:

* does not follow symlinks (use `-L/--follow`) ([Debian Manpages][1])
* and still avoids dumping raw binary matches to stdout unless you choose “treat as text” behavior (`-a/--text`). ([Debian Manpages][1])

Practical “closest to grep -r over everything” baseline:

```bash
rg -uuu -L 'needle' .
```

([Debian Manpages][1])

### J4.3 Binary knobs: `--binary` vs `-a/--text` (choose intentionally)

* `--binary` is part of the automatic filtering system: it tells ripgrep to *search binary files* (detected via NUL bytes), but ripgrep may still suppress printing raw matches and emit warnings about binary suppression/early stop. It only matters for recursive directory search. ([Debian Manpages][1])
* `-a/--text` disables binary detection and searches binary files “as text,” which may print control characters that mess with your terminal. ([Debian Manpages][1])

If you want **deterministic “print matches even in binary”** behavior while also using `-uuu`, explicitly disable binary-mode and force text:

```bash
rg -uuu --no-binary -a 'needle' .
```

(`--no-binary` is explicitly supported.) ([Debian Manpages][1])

---

## J5) “Why was this skipped?” debugging (file-level introspection)

### J5.1 `--debug` (first-line tool)

`--debug` is explicitly described as the tool for figuring out why ripgrep skipped a particular file; it should mention skipped files and reasons. ([Debian Manpages][1])

```bash
rg --debug 'needle' .
```

### J5.2 `--trace` (when debug isn’t enough)

`--trace` implies `--debug` and adds additional trace data; it can be very large and is mainly for deeper troubleshooting. ([Debian Manpages][1])

```bash
rg --trace 'needle' .
```

### J5.3 `--files` (cheap “candidate set” sanity check)

Before heavy tracing, use `--files` to print the file inventory that would be searched. ([leancrew.com][3])

```bash
rg --files .
rg --files src/ | rg -F 'suspect_file'
```

---

## J6) Noise control: `--no-ignore-messages` vs `--no-messages`

These two are often confused; they suppress **different** classes of stderr output:

### J6.1 `--no-ignore-messages`

Suppresses **error messages related to parsing ignore files** (e.g., malformed ignore syntax). Useful when you expect noisy ignore parsing issues and don’t want stderr spam. ([Debian Manpages][1])

```bash
rg --no-ignore-messages 'needle' .
```

### J6.2 `--no-messages`

Suppresses some errors related to **failed opening/reading of files** (permissions, broken symlinks, transient IO), but still shows pattern syntax errors. It’s also the documented way to silence broken-link / symlink-loop noise when using `--follow`. ([Debian Manpages][1])

```bash
rg --no-messages 'needle' .
rg --follow --no-messages 'needle' .
```

([Debian Manpages][1])

---

### Minimal “operator playbook” for smart filtering

1. Suspect filtering? Append `-u`, then `-uu`, then `-uuu`. ([Debian Manpages][1])
2. Still missing? Add `--debug` and inspect skip reasons. ([Debian Manpages][1])
3. Need truly “search everything including symlinks”? add `-L`. ([Debian Manpages][1])
4. Need binary printed as text? add `--no-binary -a` (explicitly). ([Debian Manpages][1])

If you want to continue in outline order, the next deep dive is **manual filtering overrides** that sit “above” this ignore stack (`-g/--glob`, `--iglob`, precedence vs ignores, negation semantics and common pitfalls like needing `foo/**` instead of `foo`).

[1]: https://manpages.debian.org/unstable/ripgrep/rg.1.en.html "rg(1) — ripgrep — Debian unstable — Debian Manpages"
[2]: https://docs.rs/crate/ripgrep/latest/source/GUIDE.md "ripgrep 15.1.0 - Docs.rs"
[3]: https://leancrew.com/all-this/man/man1/rg.html "rg(1) man page"

## K) Manual filtering by glob (path include/exclude) + file type system (`-g/-t`) and precedence

### K0) Mental model: **glob overrides → ignore rules → type filters → hidden/binary heuristics**

Ripgrep’s traversal/filtering pipeline checks **glob overrides first**; if a path matches a `-g/--glob` override, matching *stops* (the override decides include vs exclude). Then ignore files are checked, then file-type filters are applied (for non-directories), then hidden-ness, etc. ([Docs.rs][1])

Operational corollary:

* `-g/--glob` is the highest-leverage “manual override” for file selection and is explicitly documented as **overriding all other ignore logic**. ([Arch Manual Pages][2])
* `-t/--type` is *lower precedence* than both `-g` and ignore rules (so `-t` won’t “resurrect” ignored files). ([Arch Manual Pages][2])

---

# K1) Glob filtering: `-g/--glob` (include/exclude), `!` negation, precedence, gitignore semantics

## K1.1 Syntax and semantics (what `-g` means)

```bash
rg -g 'GLOB' PATTERN [PATH...]
rg --glob='GLOB' PATTERN [PATH...]
```

* Globs can be **include** or **exclude**; a leading `!` makes it an exclude glob. ([Arch Manual Pages][2])
* **Always overrides** other ignore logic (e.g., `.gitignore`, `.ignore`, `--ignore-file`). ([Arch Manual Pages][2])
* Multiple `-g` may be provided; **if multiple globs match**, the **later** glob on the command line wins. ([Arch Manual Pages][2])
* Glob syntax matches **gitignore-style globs**. ([Arch Manual Pages][2])

### Ordering rule (high-impact)

If you’re doing “include a broad set, then carve out exclusions,” put excludes **after** includes:

```bash
# include all *.py, then exclude vendor subtree
rg -g '*.py' -g '!vendor/**' 'needle' .
```

Because “later glob wins,” swapping the order can re-include things you meant to exclude. ([Arch Manual Pages][2])

---

## K1.2 The “directory glob footgun”: `-g foo` is NOT “search foo/”

Ripgrep applies your glob overrides to *every file and directory path* it sees, and the match is tested against the path. Consequently, `-g foo` does **not** match `foo/bar` (so it does not include the files you actually care about). The manpage explicitly calls this out:

> if you only want to search in a directory `foo`, `-g foo` is incorrect; use `-g 'foo/**'`. ([Arch Manual Pages][2])

Canonical directory scoping patterns:

```bash
# restrict search to files under foo/
rg -g 'foo/**' 'needle' .

# restrict to foo/ and bar/ only
rg -g 'foo/**' -g 'bar/**' 'needle' .

# restrict to foo/ but exclude foo/generated/
rg -g 'foo/**' -g '!foo/generated/**' 'needle' .
```

([Arch Manual Pages][2])

---

## K1.3 Anchoring and `/` semantics (gitignore rules apply, not “absolute paths”)

`-g/--glob` uses gitignore match semantics “everywhere,” which means `/` in a glob is not an OS absolute path; it’s an anchored path relative to the root where the glob is applied (often your current search root). ([GitHub][3])

Practical anchored exclude:

```bash
# exclude only the top-level target/ at the search root
rg -g '!/target/**' 'needle' .
```

(Leading `/` anchors relative to the search root; it doesn’t mean `/target` on the filesystem.) ([GitHub][3])

---

## K1.4 Case-insensitive globbing: `--iglob` and `--glob-case-insensitive`

Globs are matched case-sensitively by default; `-i/--ignore-case` only affects *content matching*, not file globs. Use glob-specific switches: ([GitHub][4])

### `--iglob=GLOB` (case-insensitive glob overrides)

```bash
rg --iglob='*.PY' 'needle' .
```

`--iglob` is the same override system as `-g`, but matched case-insensitively. ([Arch Manual Pages][2])

### `--glob-case-insensitive` (make all `-g` case-insensitive)

```bash
rg --glob-case-insensitive -g '*.PY' 'needle' .
```

This “effectively treats `-g/--glob` as `--iglob`.” ([Arch Manual Pages][2])

---

## K1.5 ripgrep’s “brace alternation” glob extension `{a,b}`

Ripgrep extends gitignore-style globs by allowing brace alternatives in `-g` globs:

* `-g 'ab{c,d}*'` is equivalent to `-g 'abc*' -g 'abd*'`
* empty alternatives like `{,c}` are not supported (currently) ([Arch Manual Pages][2])

```bash
# include both *.test.py and *.spec.py
rg -g '*.{test,spec}.py' 'needle' .
```

([Arch Manual Pages][2])

---

## K1.6 “Gitignore-style glob semantics” reused elsewhere (important for consistency)

The same gitignore-style glob language + `!` exclude prefix is reused in multiple places, including:

* `-g/--glob` and `--iglob` ([Arch Manual Pages][2])
* `--pre-glob` (limits which files go through a preprocessor; supports multiple globs + `!`) ([Arch Manual Pages][2])
* ignore files and `--ignore-file` (“gitignore formatted rules files”) ([Arch Manual Pages][2])
* `--type-add` (file types are defined by glob rules) ([Arch Manual Pages][2])

---

# K2) Manual filtering by file type (type system): `--type-list`, `-t/-T`, `--type-add`, `--type-clear`

## K2.1 Listing types: `--type-list` (and what it reflects)

```bash
rg --type-list
```

* Prints all supported file types and their corresponding globs.
* Critically: it **takes any `--type-add` and `--type-clear` flags into account**, so it’s your debugging lens for type edits. ([Arch Manual Pages][2])
* Note: `--files` overrides `--type-list` if both are given. ([Arch Manual Pages][2])

Debug pattern:

```bash
# See how ripgrep defines "py" on your build
rg --type-list | rg '^py:'
```

---

## K2.2 Include by type: `-t TYPE` / `--type=TYPE`

```bash
rg -tpy 'needle' .
rg --type=py 'needle' .
```

Semantics:

* Limits search to files matching the given `TYPE`.
* Repeatable (`-t` multiple times = union of types). ([Arch Manual Pages][2])

### Special value: `--type=all` (“whitelist mode”)

`--type=all` behaves as if `-t` was provided for every known type (including custom types). The result is **whitelist mode**:

> only search files that ripgrep recognizes via its type definitions. ([Arch Manual Pages][2])

This is extremely useful when you want to avoid searching unknown blobs/odd extensions, but it can surprise you by excluding unrecognized files.

**Precedence note:** `--type=all` has *lower precedence* than `-g/--glob` and ignore rules. ([Arch Manual Pages][2])

---

## K2.3 Exclude by type: `-T TYPE` / `--type-not=TYPE`

```bash
rg -Tmd -Tjson 'needle' .
rg --type-not=md --type-not=json 'needle' .
```

Semantics:

* Excludes files matching the given `TYPE`.
* Repeatable. ([Arch Manual Pages][2])

### Special value: `--type-not=all` (“blacklist mode”)

`--type-not=all` behaves as if `-T` was provided for every known type (including custom types). The result is **blacklist mode**:

> only search files that are **unrecognized** by ripgrep’s type definitions. ([Arch Manual Pages][2])

This is a niche but powerful mode for hunting in “weird files” only (e.g., extensionless config dumps).

---

## K2.4 How types interact with directories (important subtlety)

Type filtering applies to **files**, not directories: the file type matcher runs “unless the path is a directory.” ([Docs.rs][1])
So `-tpy` won’t prevent traversal into subdirectories; it just means only `py`-typed files get searched.

---

## K2.5 Extending the type system: `--type-add=…` (single glob per flag, include directive)

### Basic form: add one glob to one type

```bash
rg --type-add 'foo:*.foo' -tfoo 'needle' .
```

Rules (non-negotiable; baked into the CLI):

* Only **one glob can be added at a time** (one per `--type-add` flag).
* Multiple `--type-add` flags are allowed.
* Unless `--type-clear` is used, new globs are **added** onto existing globs for that type (including built-ins).
* Type settings are **not persisted**; you must pass them every time (or use a config-file workaround). ([Arch Manual Pages][2])

### Include directive: compose new types from existing ones

```bash
# “src” is union of existing types
rg --type-add 'src:include:cpp,py,md' -tsrc 'needle' .
```

This “include directive” imports glob rules from named types. ([Arch Manual Pages][2])

You can still add extra globs after:

```bash
rg --type-add 'src:include:cpp,py,md' \
   --type-add 'src:*.foo' \
   -tsrc 'needle' .
```

([Arch Manual Pages][2])

Type-name constraint: names must be Unicode letters/numbers only (no punctuation). ([Arch Manual Pages][2])

### Persistence workaround (CLI-native): config file

Because type settings are not persisted, the official workaround is to put `--type-add ...` lines into your ripgreprc (`RIPGREP_CONFIG_PATH`). ([Arch Manual Pages][2])
(We can deep dive config mechanics later when we hit the config chapter.)

---

## K2.6 Resetting / redefining types: `--type-clear=TYPE`

```bash
rg --type-clear foo --type-add 'foo:*.foo' -tfoo 'needle' .
```

Semantics:

* Clears the previously defined globs for `TYPE`.
* You can add globs again after clearing.
* Not persisted; must be passed every invocation (config workaround applies). ([Arch Manual Pages][2])

This is the “I want to redefine a built-in type” tool:

```bash
# redefine "py" to only *.py (example)
rg --type-clear py --type-add 'py:*.py' -tpy 'needle' .
```

(Use `--type-list` to verify the resulting mapping in that invocation.) ([Arch Manual Pages][2])

---

# K3) Practical compositions (glob + type + ignore precedence)

### K3.1 “Search only python, but exclude vendor and build”

```bash
rg -tpy -g '!vendor/**' -g '!build/**' 'needle' .
```

Type restricts files; glob overrides carve out known-noisy subtrees. ([Arch Manual Pages][2])

### K3.2 “Include an ignored subtree anyway” (override ignore logic)

If `generated/**` is ignored by `.gitignore`, `-g` can still force it into the candidate set because glob overrides are checked first and override ignore logic. ([Arch Manual Pages][2])

```bash
# include generated subtree explicitly, then exclude the rest of generated noise if needed
rg -g 'generated/**' 'needle' .
```

### K3.3 “Restrict to one directory correctly”

```bash
rg -g 'src/**' 'needle' .
```

Not `-g src`. ([Arch Manual Pages][2])

---

If you want the next chapter in the same style, the natural follow-on is: **“ignore-file + glob + type precedence truth table”** (include/exclude ordering patterns, directory-vs-file matching, and a debugging playbook using `--files`, `--debug`, and `--type-list` to explain exactly why a given file was searched or skipped).

[1]: https://docs.rs/ignore/latest/ignore/struct.WalkBuilder.html "WalkBuilder in ignore - Rust"
[2]: https://man.archlinux.org/man/rg.1.raw "man.archlinux.org"
[3]: https://github.com/BurntSushi/ripgrep/discussions/2156?utm_source=chatgpt.com "Ignoring files matching a pattern? #2156 - BurntSushi ripgrep"
[4]: https://github.com/BurntSushi/ripgrep/discussions/2967?utm_source=chatgpt.com "How do I match globs case insensitively? #2967"

## L) Precedence truth table: ignore files vs `--ignore-file` vs `-g/--glob` overrides vs `-t/-T` type filters

### L0) The *actual* decision procedure (what wins, in what order)

When `rg` is walking a directory tree, the filtering logic is effectively “stacked matchers.” The underlying walker (`ignore::WalkBuilder`, which ripgrep uses) documents the exact order: **glob overrides → ignore files → type matcher → hidden → max-filesize**. ([Docs.rs][1])

**Canonical order (per-path)** ([Docs.rs][1])

1. **Glob overrides** (`-g/--glob`, `--iglob`): first match decides; “whitelist unless starts with `!`.”
2. **Ignore rules** (`.gitignore`, `.ignore`, `.rgignore`, git excludes, global excludes, `--ignore-file`): evaluated next (with precedence rules below).
3. If ignore step yields an **ignore match**, skip immediately; if it yields a **whitelist match**, continue (but later matchers may still override). ([Docs.rs][1])
4. **Type matcher** (unless path is a directory): `-t/-T` and the internal type definitions run here. ([Docs.rs][1])
5. **Hidden** check (unless whitelisted): hidden paths are skipped unless `--hidden` / `-uu` etc. ([Docs.rs][1])
6. **Max filesize** check (unless directory). ([Docs.rs][1])
7. Yield path to be searched.

---

### L1) Precedence truth table (who overrides whom)

This table is the “apply in this order” view you can hand to an agent.

| Mechanism                                                                            | Applies to                       | Include vs exclude                       | Precedence vs others                                                   | Tie-breaker                                                                                                |
| ------------------------------------------------------------------------------------ | -------------------------------- | ---------------------------------------- | ---------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| **Explicit file PATH argument**                                                      | the explicit file path only      | implicitly “include”                     | overrides **glob + ignore rules** (not a promise about type filters)   | n/a ([Debian Manpages][2])                                                                                 |
| `-g/--glob` / `--iglob` (override globs)                                             | **files + directories**          | include glob vs `!` exclude glob         | **always overrides any other ignore logic**                            | if multiple match: **later `-g/--glob` wins** ([Debian Manpages][2])                                       |
| Ignore files in tree: `.gitignore`, `.ignore`, `.rgignore`, plus git excludes/global | files + directories              | include via `!` lines, exclude otherwise | lower than override globs; higher than `--ignore-file`                 | later items override earlier; within a level, **more nested overrides less nested** ([Debian Manpages][2]) |
| `--ignore-file=PATH` (extra ignore sources)                                          | files + directories              | same gitignore syntax                    | **lower precedence** than ignore files found automatically in the tree | earlier files lower than later files ([Debian Manpages][2])                                                |
| `-t/--type`, `-T/--type-not`                                                         | **files only** (not directories) | whitelist/blacklist by file type         | **lower precedence than `-g/--glob` and ignore rules**                 | union across multiple `-t` / multiple `-T` ([Debian Manpages][2])                                          |

Key takeaways (memorize these):

* `-g/--glob` is the **top-most manual override**: it is checked first and short-circuits other ignore logic. ([Docs.rs][1])
* `--ignore-file` is **always “low priority”**: it is applied after `.gitignore/.ignore/.rgignore` and loses conflicts to them. ([Debian Manpages][2])
* Type filtering (`-t/-T`) is **late** and cannot “resurrect” a path excluded by ignore rules (or a `!` glob). ([Debian Manpages][2])

---

## L2) Ignore-rule precedence (the exact stack inside “ignore files”)

ripgrep’s own manual gives a concrete precedence list, **later overrides earlier**: ([Debian Manpages][2])

1. `--ignore-file` (lowest)
2. global gitignore (e.g. `$HOME/.config/git/ignore`)
3. `.git/info/exclude`
4. `.gitignore`
5. `.ignore`
6. `.rgignore` (highest)

So: if `.gitignore` ignores `foo` but `.rgignore` contains `!foo`, then `foo` is **included**. ([Debian Manpages][2])

Also: `--ignore-file` patterns are matched **relative to the current working directory**, and multiple `--ignore-file` args are ordered (later wins). ([Debian Manpages][2])

---

## L3) Include/exclude ordering patterns you should standardize on

### L3.1 `-g/--glob`: **later wins**, so write globs in “broad include → narrow exclude” order

`-g/--glob` always overrides ignore logic; multiple globs allowed; if multiple match, the later one wins. ([Debian Manpages][2])

```bash
# include all Python, then carve out noisy subtrees
rg -g '*.py' -g '!**/.venv/**' -g '!**/vendor/**' 'needle' .
```

### L3.2 Directory scoping: use `dir/**`, not `dir`

`-g` is applied to **every file and directory**. Because `foo/bar` does not match glob `foo`, `-g foo` is not “restrict to foo/”. Use `-g 'foo/**'`. ([Debian Manpages][2])

```bash
# correct directory restriction
rg -g 'src/**' 'needle' .
```

### L3.3 `--ignore-file` is “lowest precedence” by design

Use `--ignore-file` for **ad hoc** ignore packs, but don’t expect it to override `.rgignore`/`.ignore`/`.gitignore`. It is explicitly documented as lower precedence than ignore files found in the directory tree. ([Debian Manpages][2])

```bash
# add temporary ignores (won’t override repo .rgignore/.ignore/.gitignore conflicts)
rg --ignore-file /tmp/extra.ignore 'needle' .
```

### L3.4 Type filters are *late*: use them to reduce work, not to fight ignores

`-t/--type` has lower precedence than `-g/--glob` and ignore rules. ([Debian Manpages][2])
So the common pattern is:

```bash
# “only Python” + explicit subtree exclusions
rg -tpy -g '!**/.venv/**' -g '!**/build/**' 'needle' .
```

---

## L4) Directory-vs-file matching: what applies where

### L4.1 Globs apply to **directories and files**

Globs are tested against every file and directory encountered. ([Debian Manpages][2])
Implication: if you exclude a directory, ripgrep won’t descend into it.

```bash
# exclude a whole subtree (directory-level match)
rg -g '!target/**' 'needle' .
```

### L4.2 Type filters apply to **files only**

The walker runs the type matcher “unless the path is a directory.” ([Docs.rs][1])
So `-tpy` does not prevent directory traversal; it prevents *searching* non-`py` files once reached.

---

## L5) Debugging playbook: “why was this skipped?” (fastest path to ground truth)

### L5.1 Start with **`--files`** to see the candidate set *before searching*

`--files` prints each file that would be searched without searching. ([Debian Manpages][2])

```bash
# baseline candidate set
rg --files .

# candidate set under your current filters
rg -tpy -g '!**/.venv/**' --files .

# “is this file even in-scope?”
rg --files . | rg -F 'path/to/suspect.py'
```

Note: `--files` overrides `--type-list`, so don’t combine them expecting both outputs. ([Debian Manpages][2])

### L5.2 Use **`--debug`** to get per-file skip reasons (the authoritative explainer)

`--debug` is explicitly called out as “generally useful for figuring out why ripgrep skipped searching a particular file,” and it should mention skipped files and why. ([Debian Manpages][2])

```bash
rg --debug 'needle' .
```

Escalate to `--trace` if `--debug` isn’t enough (it implies `--debug`). ([Debian Manpages][2])

### L5.3 Validate the *type universe* with **`--type-list`** (especially after `--type-add/--type-clear`)

`--type-list` shows all supported file types + globs and reflects any `--type-add`/`--type-clear` passed. ([Debian Manpages][2])

```bash
rg --type-list | rg '^py:'

# after redefining types in an invocation, verify:
rg --type-clear py --type-add 'py:*.py' --type-list | rg '^py:'
```

(`--type-add` is one-glob-per-flag, not persisted without config; `--type-clear` resets the type.) ([Debian Manpages][2])

### L5.4 Noise control: suppress the right class of stderr

* `--no-ignore-messages`: suppresses **ignore-file parsing** errors (noise from malformed ignore files). ([Debian Manpages][2])
* `--no-messages`: suppresses **file open/read** errors (permissions, broken links), but still shows regex syntax errors. ([Debian Manpages][2])

```bash
rg --debug --no-ignore-messages 'needle' .
rg --follow --no-messages 'needle' .
```

---

If you want to keep the same momentum, the next chapter that compounds with this one is **“filtering playbooks as reusable aliases / ripgreprc profiles”** (e.g., `rgp` for “python-only + ignore build artifacts”, `rga` for “audit mode: -uuu -L”, etc.), because the last-flag-wins rule makes override-friendly profiles easy to maintain. ([Debian Manpages][2])

[1]: https://docs.rs/ignore/latest/ignore/struct.WalkBuilder.html "WalkBuilder in ignore - Rust"
[2]: https://manpages.debian.org/testing/ripgrep/rg.1.en.html "rg(1) — ripgrep — Debian testing — Debian Manpages"

## M) Encodings and transcoding (`-E/--encoding`, BOM sniffing, raw-bytes mode, and JSON “text vs bytes”)

### M0) The actual pipeline: bytes on disk → (maybe) transcode → regex engine

ripgrep starts with “files are bytes,” and it only gets *reliable text semantics* if it can treat the searched stream as **UTF-8** (or at least ASCII-compatible). The docs make two key points:

* With the default (`--encoding auto`), `rg` assumes ASCII-compatible encodings and only does **BOM sniffing** for UTF-8/UTF-16; it does not try to guess other encodings. ([Debian Manpages][1])
* When BOM sniffing detects UTF-16 (or when you force an encoding), `rg` **transcodes the file to UTF-8** and runs the search on the transcoded bytes (performance cost; invalid UTF-16 becomes replacement characters). ([Docs.rs][2])

So, you can think of `--encoding` as choosing between:

1. **Search on transcoded UTF-8** (nice Unicode regex semantics), or
2. **Search on raw bytes** (forensics / binary-ish searching / offsets stability).

---

## M1) `-E/--encoding=auto` (default): BOM sniffing only for UTF-8/UTF-16

### M1.1 Syntax

```bash
rg -E auto 'PATTERN' [PATH...]
rg --encoding=auto 'PATTERN' [PATH...]
```

### M1.2 What “auto” means (strictly)

* “Best effort” per-file detection, but it only triggers when a file begins with a **UTF-8 or UTF-16 BOM**. No other detection occurs. ([Debian Manpages][1])
* For UTF-16 specifically, `rg` reads the first bytes, detects the BOM, and **transcodes UTF-16 → UTF-8**, then searches the transcoded content. ([Docs.rs][2])

### M1.3 Why this matters to match semantics (`\w`, `.`, Unicode classes)

ripgrep’s Unicode-friendly regex operators (`\w`, `.`, Unicode classes/boundaries) fundamentally assume UTF-8. The guide is explicit: those constructions “simply won’t match” when the engine encounters non-UTF-8 bytes. ([Docs.rs][2])

**Implication**: If you’re searching a non-UTF-8 file for non-ASCII text and you *don’t* set `--encoding`, your pattern may “mysteriously” never match (because the bytes do not correspond to UTF-8). ([Docs.rs][2])

---

## M2) Explicit encodings: `-E/--encoding=<label>` (transcode everything unless BOM says otherwise)

### M2.1 Syntax

```bash
rg -E latin1 'PATTERN' [PATH...]
rg --encoding=shift_jis 'PATTERN' [PATH...]
```

### M2.2 What it does

* You provide an encoding **label** from the WHATWG Encoding Standard; ripgrep assumes **all files** are in that encoding (unless a file has a BOM), transcodes to UTF-8, and then searches. ([Docs.rs][2])
* Supported names are the WHATWG labels (ripgrep points you at the label list directly). ([Debian Manpages][1])

### M2.3 Practical command patterns

**“My repo has Windows-1252 / latin1 text and I need to match accented text”**

```bash
rg -E windows-1252 'café' docs/
# (or) rg -E latin1 'café' docs/
```

(Exact label availability comes from the WHATWG label list.) ([Debian Manpages][1])

**“My patterns are UTF-8 in my shell; make files match by transcoding”**
That’s exactly what `-E` buys you: the file is transcoded to UTF-8 so your terminal-provided pattern is comparable. ([Docs.rs][2])

### M2.4 Config override knob: `--no-encoding`

If your ripgreprc hard-codes an encoding, `--no-encoding` reverts back to automatic detection mode. ([Debian Manpages][1])

---

## M3) Raw-bytes mode: `-E none` / `--encoding=none` (disable BOM sniffing + all transcoding)

### M3.1 Syntax

```bash
rg -E none 'PATTERN' [PATH...]
rg --encoding=none 'PATTERN' [PATH...]
```

### M3.2 Exact semantics

* Completely disables encoding logic **including BOM sniffing**.
* Searches the **raw bytes**, including the BOM if present. ([Debian Manpages][1])

This is your “forensics / exact byte layout” mode.

### M3.3 Byte-level patterns: disable Unicode interpretation

If you’re operating on raw bytes, you usually also want byte-oriented regex interpretation:

* Disable Unicode in-pattern via `(?-u:...)` (so `.` can match any byte, including invalid UTF-8). ([Docs.rs][2])

Example from the guide (raw UTF-16 byte search):

```bash
rg -E none -a '(?-u)\(\x045\x04@\x04;\x04>\x04:\x04' some-utf16-file
```

([Docs.rs][2])

**Why `-a` appears here**: it prevents binary filtering from skipping/short-circuiting the file (common when raw bytes contain NUL). (Binary filtering behavior is part of ripgrep’s default “smart filtering.”) ([Debian Manpages][1])

---

## M4) Output implications: bytes vs text, and why `--json` is special

### M4.1 Standard output vs JSON constraints

Your terminal output can *display* mojibake, but JSON has a harder constraint: it must be representable in a Unicode encoding (ripgrep’s JSON printer focuses on UTF-8). Therefore, JSON output needs a lossless strategy for invalid UTF-8 in:

* file paths, and
* matched content. ([Debian Manpages][1])

### M4.2 `--json`: the `text` vs `bytes` union (base64 for non-UTF-8)

In `--json` mode, ripgrep wraps “arbitrary data” as:

* `{"text": "..."}` when underlying bytes are valid UTF-8, else
* `{"bytes": "..."}` where the value is **base64** of the raw bytes. ([Debian Manpages][1])

This applies to both `path` and `lines` (and submatch `match` text). ([Docs.rs][3])

Minimal shape (schematic):

```json
{"type":"match","data":{
  "path":{"text":"src/file.txt"},
  "lines":{"bytes":"...base64..."},
  "absolute_offset":123,
  "submatches":[{"start":5,"end":9,"match":{"bytes":"..."} }]
}}
```

(Exact envelope/message types are documented; `match` includes text + offsets.) ([Debian Manpages][1])

### M4.3 Offsets are byte offsets into the searched data

The JSON format defines:

* `absolute_offset`: absolute **byte offset** to the start of `lines` in the **data being searched**
* `submatches[].start/end`: byte offsets into `lines` (and if `lines` is base64-encoded, offsets are into the decoded bytes). ([Docs.rs][3])

**Encoding consequence (important)**: if `rg` is transcoding (UTF-16→UTF-8 via BOM sniffing, or `-E <encoding>`), the “data being searched” is the **transcoded** UTF-8 stream. So offsets in JSON are stable with respect to what `rg` searched/printed, but they are not necessarily the same as byte offsets in the original on-disk encoding. This follows directly from “search executes on the transcoded version of the file” plus JSON’s “offsets are byte offsets into the data being searched.” ([Docs.rs][2])

**If you need offsets that line up with the original raw bytes** (e.g., editor patching in-place), use `-E none` and operate in raw-bytes mode (and accept that your patterns must be byte-aware). ([Debian Manpages][1])

---

## M5) High-ROI recipes (copy/paste)

### M5.1 “Just search the UTF-16 file like a human” (let BOM sniffing work)

```bash
rg 'Шерлок' some-utf16-file
```

Works because BOM sniffing + UTF-16→UTF-8 transcoding happens automatically under `--encoding auto` when BOM is present. ([Docs.rs][2])

### M5.2 “Non-UTF file with meaningful non-ASCII text: force the encoding”

```bash
rg -E windows-1252 'café' docs/
```

`-E` forces transcoding using a WHATWG label so UTF-8 terminal patterns match file content. ([Docs.rs][2])

### M5.3 “Forensics / exact bytes / include BOM”: disable all encoding logic

```bash
rg -E none -a '(?-u:\x00)' suspect.bin
```

* `-E none`: search raw bytes (no BOM sniffing/transcoding) ([Debian Manpages][1])
* `(?-u:...)`: avoid Unicode constraints so byte escapes behave as bytes ([Docs.rs][2])
* `-a`: avoid binary skipping/early stop interfering with the search ([Debian Manpages][1])

### M5.4 “Consume `--json` correctly”: handle `text` or base64 `bytes`

When parsing JSON, treat fields like `data.path` and `data.lines` as an object containing either:

* `.text` (use as string), or
* `.bytes` (base64-decode to bytes). ([Debian Manpages][1])

---

If you want to keep moving in outline order, the next encoding-adjacent chapter that compounds with this is **binary data handling** (`--binary`, `-a/--text`, `--null-data`, `--max-columns`, terminal safety), because encoding and “binary heuristics” collide constantly when you’re searching UTF-16 or raw bytes.

[1]: https://manpages.debian.org/unstable/ripgrep/rg.1.en.html "rg(1) — ripgrep — Debian unstable — Debian Manpages"
[2]: https://docs.rs/crate/ripgrep/latest/source/GUIDE.md "ripgrep 15.1.0 - Docs.rs"
[3]: https://docs.rs/grep-printer/%2A/grep_printer/struct.JSON.html "JSON in grep_printer - Rust"
