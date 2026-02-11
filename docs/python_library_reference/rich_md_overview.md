[![python - How to programmatically generate markdown output in Jupyter ...](https://tse4.mm.bing.net/th/id/OIP.A1X3cRZIHurX3_Dvsa54HwHaFF?pid=Api)](https://stackoverflow.com/questions/36288670/how-to-programmatically-generate-markdown-output-in-jupyter-notebooks?utm_source=chatgpt.com)

Rich’s “materialized markdown in terminal” story is basically: **parse Markdown → turn it into Rich renderables → print via `Console`**. ([Rich Documentation][1])

## 1) The core API (Python)

* **Renderable:** `rich.markdown.Markdown(markup: str, ...)` — you construct it from a markdown string and `console.print()` it. ([Rich Documentation][1])
* **Code blocks:** fenced code blocks are rendered with **full syntax highlighting** (Rich uses Pygments themes for this). ([Rich Documentation][1])
* **Quick “render a file” CLI:** `python -m rich.markdown README.md` (and `-h` for args). ([Rich Documentation][1])

```python
from rich.console import Console
from rich.markdown import Markdown

console = Console()
md = Markdown(open("README.md", encoding="utf-8").read())
console.print(md)
```

([Rich Documentation][1])

## 2) What Markdown constructs Rich actually renders

Rich’s markdown renderer maps markdown-it tokens into a set of renderable “elements” (so it’s not just plain text with ANSI codes). Supported elements include: ([Rich Documentation][2])

* **Headings** (`#`, `##`, …) ([Rich Documentation][2])
* **Paragraphs** ([Rich Documentation][2])
* **Ordered + unordered lists** ([Rich Documentation][2])
* **Block quotes** ([Rich Documentation][2])
* **Horizontal rules** ([Rich Documentation][2])
* **Links** (either clickable hyperlinks or “text (url)” style) ([Rich Documentation][2])
* **Inline emphasis / strong / code / strikethrough** (Rich explicitly enables strikethrough in its markdown-it parser, and styles inline tags) ([Rich Documentation][3])
* **Tables** (Rich enables markdown-it’s table support and renders tables as Rich `Table`) ([Rich Documentation][2])
* **Images:** rendered as a **placeholder** (terminals don’t generally display inline bitmap images), with optional hyperlink behavior. ([Rich Documentation][2])

## 3) The knobs you use to control “formatting”

`Markdown(...)` exposes a handful of high-leverage formatting controls: ([Rich Documentation][2])

* `code_theme="monokai"`: Pygments theme for **fenced code blocks**
* `justify=...`: paragraph justification
* `style=...`: apply a base Rich style to the whole document
* `hyperlinks=True`: emit terminal hyperlinks (when supported) instead of showing raw URLs
* `inline_code_lexer=...` / `inline_code_theme=...`: **optional** Pygments highlighting for inline code spans (normally inline code is styled; with a lexer it can be highlighted) ([Rich Documentation][2])

## 4) Styling “Markdown theme” via Rich style keys

Internally, Rich looks up style names like `markdown.paragraph`, `markdown.code_block`, `markdown.block_quote`, `markdown.hr`, `markdown.link`, `markdown.link_url`, plus heading styles like `markdown.h1` and an `markdown.h1.border` panel border. ([Rich Documentation][3])
So if you want your markdown to match a light terminal theme, you typically override these styles in a `Theme` you pass to `Console`.

## 5) If you literally want “render markdown files in terminal” as a tool

Two convenient options:

* Built-in: `python -m rich.markdown README.md` ([Rich Documentation][1])
* **rich-cli** (separate package) provides a `rich` command that auto-detects `.md` and supports switches like `--markdown`, `--hyperlinks`, `--pager`, plus width/alignment/style options. ([GitHub][4])

If you tell me your terminal context (dark/light theme, do you want clickable links, do you care about tables), I can suggest a minimal Theme map (the exact `markdown.*` keys) that makes rendered READMEs look great.

[1]: https://rich.readthedocs.io/en/latest/markdown.html "Markdown — Rich 14.1.0 documentation"
[2]: https://rich.readthedocs.io/en/stable/reference/markdown.html "rich.markdown — Rich 14.1.0 documentation"
[3]: https://rich.readthedocs.io/en/stable/_modules/rich/markdown.html "rich.markdown — Rich 14.1.0 documentation"
[4]: https://github.com/Textualize/rich-cli "GitHub - Textualize/rich-cli: Rich-cli is a command line toolbox for fancy output in the terminal"

## 6) Markdown dialect + extensions (what Rich actually parses)

* **Parser engine:** `rich.markdown.Markdown` uses **markdown-it-py** under the hood. ([Rich Documentation][1])
* **Baseline rules:** `MarkdownIt()` defaults to the **CommonMark** configuration in markdown-it-py. ([Markdown-it-py][2])
* **Explicitly enabled extensions:** Rich turns on only:

  * **Strikethrough** (`~~like this~~`)
  * **Tables** (pipe tables)
    by calling `.enable("strikethrough").enable("table")`. ([Rich Documentation][1])
* **Implication:** You get **CommonMark + {tables, strikethrough}**, not a full “GFM kitchen sink” renderer. ([Rich Documentation][1])

  * For example, markdown-it-py’s “gfm-like” preset also enables **linkify** (auto-linking bare URLs), but Rich’s Markdown doesn’t enable that. ([Markdown-it-py][2])

## 7) Headings, line breaks, and layout rules (the “why does it wrap like that?” layer)

* **Headings are “opinionated” renderables:**

  * `#` (h1) is rendered as a **Panel** with a heavy border using style key `markdown.h1.border`. ([Rich Documentation][1])
  * `##` (h2) inserts an extra blank line before the heading output (Rich yields an empty `Text("")` first). ([Rich Documentation][1])
  * h2+ are centered text (Rich sets `text.justify = "center"`). ([Rich Documentation][1])
* **Soft vs hard line breaks:**

  * A markdown “softbreak” becomes a **space**.
  * A “hardbreak” becomes a **newline**. ([Rich Documentation][1])
    This matters when you’re rendering READMEs where “wrapped” lines may or may not preserve breaks.
* **Width-sensitive rendering:** lists and blockquotes explicitly reduce the available width so their prefixes don’t destroy wrapping alignment. ([Rich Documentation][1])

## 8) Code blocks: lexer selection + wrapping + padding (what you actually get)

* **Lexer selection from fenced info string:**

  * For a fenced block like <code>```python</code>, Rich takes the **first word** of the fence “info” (`token.info.partition(" ")[0]`) as the lexer name.
  * If absent, it uses `"text"`. ([Rich Documentation][1])
* **Rendering details:**

  * Code blocks are rendered with `rich.syntax.Syntax(...)` and Rich sets:

    * `word_wrap=True`
    * `padding=1`
      so long lines wrap and the block has breathing room. ([Rich Documentation][1])
* **Theme control:** `Markdown(..., code_theme="...")` picks the Pygments theme used for fenced blocks. ([Rich Documentation][3])

## 9) Inline code highlighting: the “hidden switch”

Rich *can* do Pygments highlighting for inline code spans, but only if you opt in:

* Pass `inline_code_lexer="python"` (or whatever lexer you want). Rich will build a `Syntax` highlighter for that lexer. ([Rich Documentation][1])
* When the parser encounters `code_inline` (and also when handling `fence` text), it routes the content through `Syntax.highlight(...)` and inserts the highlighted `Text`. ([Rich Documentation][1])
* `inline_code_theme` defaults to `code_theme` if you don’t pass it. ([Rich Documentation][1])

Practical consequence: **inline code is styled by markdown styles unless you supply `inline_code_lexer`** (in which case it becomes token-highlighted). ([Rich Documentation][1])

## 10) Links: hyperlink mode vs “(url)” fallback mode

Rich has two distinct behaviors controlled by `hyperlinks`:

* **`hyperlinks=True` (default):** on `link_open`, it applies a style containing `Style(link=href)` (using base style `markdown.link_url`). This creates a terminal hyperlink in terminals that support it. ([Rich Documentation][1])
* **`hyperlinks=False`:** Rich prints links as:
  `link_text (url)`
  with separate style keys: `markdown.link` for the text, and `markdown.link_url` for the URL. ([Rich Documentation][1])

## 11) Images: placeholder rendering + link behavior

Rich does **not** render bitmap images inline; it renders an **image placeholder token**:

* The image element is `ImageItem`, created from the image `src` attribute. ([Rich Documentation][1])
* Output is a small inline text chunk like `" <title> "`:

  * If the markdown provides image text, it uses that; otherwise it falls back to the filename-ish tail of the destination. ([Rich Documentation][1])
* If `hyperlinks=True`, it will apply a `Style(link=...)` so the placeholder is clickable (either the surrounding link or the image destination). ([Rich Documentation][1])

## 12) Lists: exact bullet/number formatting and wrap behavior

* **Bullet lists**:

  * Prefix is literally `" • "` (styled by `markdown.item.bullet`).
  * Wrapped lines get a padding prefix of 3 spaces to align under the text. ([Rich Documentation][1])
* **Ordered lists**:

  * Rich computes a fixed `number_width` based on the last number (so alignment stays clean).
  * Numbers are right-justified and styled via `markdown.item.number`. ([Rich Documentation][1])
* **Why wrapping stays pretty:** each item is rendered in a narrowed width (`options.max_width - prefix_width`) and then prefixed line-by-line. ([Rich Documentation][1])

## 13) Block quotes + horizontal rules: special renderers

* **Block quotes**:

  * Render children at `max_width - 4`.
  * Prefix each rendered line with `"▌ "` (styled by `markdown.block_quote`). ([Rich Documentation][1])
* **Horizontal rules (`---`)**:

  * Rendered via `rich.rule.Rule` with style key `markdown.hr`. ([Rich Documentation][1])

## 14) Tables: how markdown tables become `rich.table.Table`

* Rich builds a `Table(box=box.SIMPLE_HEAVY)`. ([Rich Documentation][1])
* **Header row** becomes `table.add_column(...)`; **body rows** become `table.add_row(...)`. ([Rich Documentation][1])
* **Cell alignment:** `TableDataElement` inspects the token’s `"style"` attribute for `text-align:right|center|left` and maps it to Rich justify modes. ([Rich Documentation][1])
  (So markdown alignment markers in the separator row can carry through to terminal alignment.)

## 15) Style key surface area you can theme (what to override)

Rich’s markdown renderer looks up named styles on the `Console` theme, including:

* `markdown.paragraph` ([Rich Documentation][1])
* `markdown.h1`, `markdown.h1.border`, `markdown.h2`, … ([Rich Documentation][1])
* `markdown.code_block` ([Rich Documentation][1])
* `markdown.block_quote` ([Rich Documentation][1])
* `markdown.hr` ([Rich Documentation][1])
* `markdown.item`, `markdown.item.bullet`, `markdown.item.number` ([Rich Documentation][1])
* Inline styles: `markdown.em`, `markdown.strong`, `markdown.code`, `markdown.s` ([Rich Documentation][1])
* Link styles: `markdown.link`, `markdown.link_url` ([Rich Documentation][1])

To customize, define a `Theme` and pass it to `Console(theme=...)`. ([Rich Documentation][4])

## 16) “Materializing” Markdown output: paging, capture, and export

If your intent is “render markdown → view nicely / ship somewhere”:

* **Pager for long markdown:** use `Console.pager()` context manager; Rich will send printed output to a pager when the context exits. ([Rich Documentation][5])
  (The `python -m rich.markdown` CLI also has `--page` which uses a pager path internally.) ([Rich Documentation][1])
* **Capture as a string (for tests / pipelines):** `with console.capture() as c: ...; c.get()` ([Rich Documentation][5])
* **Export to HTML / SVG / text:** construct `Console(record=True)` then call `export_html()`, `export_svg()`, or `export_text()` (or `save_*`). ([Rich Documentation][5])
  This is handy if you want terminal-faithful markdown rendering embedded in docs, CI artifacts, etc.

[1]: https://rich.readthedocs.io/en/stable/_modules/rich/markdown.html "rich.markdown — Rich 14.1.0 documentation"
[2]: https://markdown-it-py.readthedocs.io/en/latest/using.html?utm_source=chatgpt.com "Using markdown_it - markdown-it-py"
[3]: https://rich.readthedocs.io/en/stable/reference/markdown.html "rich.markdown — Rich 14.1.0 documentation"
[4]: https://rich.readthedocs.io/en/latest/style.html "Styles — Rich 14.1.0 documentation"
[5]: https://rich.readthedocs.io/en/latest/console.html "Console API — Rich 14.1.0 documentation"

## 17) Built-in CLI: `python -m rich.markdown` (flags + exact behavior)

You get a surprisingly capable “materialize markdown → terminal” pipeline out of the box, with knobs that map straight to `Console(...)` + `Markdown(...)`. ([Rich Documentation][1])

**Invocation patterns**

* Render a file:

  ```bash
  python -m rich.markdown README.md
  ```
* Render stdin (PATH is `-`):

  ````bash
  cat README.md | python -m rich.markdown -
  ``` :contentReference[oaicite:1]{index=1}
  ````

**Flags (and what they actually do)**

* `-c / --force-color`: passes `force_terminal=True` into `Console(...)` (useful when piping / in CI). ([Rich Documentation][1])
* `-t / --code-theme THEME`: sets `Markdown(..., code_theme=THEME)`. ([Rich Documentation][1])
* `-i / --inline-code-lexer LEXER`: enables *inline* Pygments highlighting (same mechanism as fenced code highlighting, but for backticks). ([Rich Documentation][1])
* `-y / --hyperlinks`: **CLI default is off** (because it’s `store_true`). Without `-y`, links are rendered as `text (url)`; with `-y`, Rich emits hyperlink styles. ([Rich Documentation][1])
* `-w / --width N`: passes `width=N` to `Console(...)` (affects wrapping/layout). ([Rich Documentation][1])
* `-j / --justify`: sets `Markdown(justify="full")`, otherwise `"left"` (only affects paragraphs). ([Rich Documentation][1])
* `-p / --page`: renders into a `StringIO` via `Console(file=...)` and sends the resulting text to `pydoc.pager(...)`. ([Rich Documentation][1])

---

## 18) Render pipeline internals (what actually gets “materialized”)

Rich Markdown is a deterministic **token-stream → element-stack → renderables** machine:

**Tokenization**

* Rich parses once in `Markdown.__init__` and stores the parsed token stream on the instance (`self.parsed = parser.parse(markup)`), so repeated `console.print(md)` reuses the parse. ([Rich Documentation][1])
* `markdown-it`’s `parse()` returns block tokens; “inline” content is typically nested under a special `inline` token’s `children`. Rich deliberately **flattens** tokens via `_flatten_tokens()` so it can stream-render in a single pass. ([Rich Documentation][1])

**Flattening rule (important for extensibility)**

* Rich flattens `token.children` **except** for fences and images (those need to remain atomic/self-contained). ([Rich Documentation][1])

**Style stack**

* Inline tags (`em`, `strong`, `code`, `s`) are handled as *style contexts*: `context.enter_style("markdown.<tag>")` on open, `leave_style()` on close. This is why nested emphasis combines correctly. ([Rich Documentation][1])

**Element stack**

* Block-ish constructs map from token type → `MarkdownElement` class via `Markdown.elements` (see next section). When a node closes, `on_child_close()` lets the parent “take over” rendering of children (lists, blockquotes, tables all use this). ([Rich Documentation][1])

---

## 19) Extensibility: overriding what Rich renders (and how)

Rich’s Markdown renderer is explicitly designed around **a token→element registry**, which is the cleanest “hook point” you have. ([Rich Documentation][1])

### 19.1 Token→Element registry (`Markdown.elements`)

This is the authoritative list of *rendered* token types (everything else falls back to `UnknownElement`). ([Rich Documentation][1])

Notable entries (beyond the obvious):

* Table substructure tokens: `thead_open`, `tbody_open`, `tr_open`, `td_open`, `th_open` (Rich builds a real `Table`). ([Rich Documentation][1])

### 19.2 Drop-in behavior change: “don’t word-wrap fenced code blocks”

Rich hard-codes `Syntax(..., word_wrap=True, padding=1)` for fenced blocks. If you want different behavior, override the element. ([Rich Documentation][1])

```python
from rich.console import Console
from rich.markdown import Markdown, CodeBlock
from rich.syntax import Syntax

class NoWrapCodeBlock(CodeBlock):
    def __rich_console__(self, console, options):
        code = str(self.text).rstrip()
        yield Syntax(code, self.lexer_name, theme=self.theme, word_wrap=False, padding=1)

class MyMarkdown(Markdown):
    elements = {**Markdown.elements, "fence": NoWrapCodeBlock, "code_block": NoWrapCodeBlock}

console = Console()
console.print(MyMarkdown(open("README.md", encoding="utf-8").read()))
```

### 19.3 Swap the markdown-it preset (autolinks, HTML handling posture)

Rich currently creates `MarkdownIt()` and only enables `strikethrough` + `table`. ([Rich Documentation][1])
markdown-it-py exposes presets like:

* `gfm-like`: enables table/strikethrough/**linkify** (requires `linkify-it-py`) ([Markdown-it-py][2])
* `js-default`: disables HTML parsing and enables table/strikethrough (useful if you want raw HTML to remain as literal text, vs becoming html tokens that won’t render) ([Markdown-it-py][2])

You can subclass `Markdown` to build with a different preset and still reuse Rich’s rendering machinery:

```python
from markdown_it import MarkdownIt
from rich.markdown import Markdown

class GFMishMarkdown(Markdown):
    def __init__(self, markup: str, **kwargs):
        super().__init__(markup, **kwargs)
        parser = MarkdownIt("gfm-like")  # requires linkify-it-py
        self.parsed = parser.parse(markup)
```

---

## 20) Unsupported constructs: what happens (and how to harden your docs)

Anything parsed into token types **not** in `Markdown.elements` and **not** handled as an inline style tag is rendered via `UnknownElement` (i.e., effectively dropped / no-op). ([Rich Documentation][1])

Practical implications:

* **Raw HTML**: markdown-it commonly emits tokens like `html_inline` / `html_block`; those aren’t in Rich’s element map, so they won’t show up in terminal output. ([GitHub][3])
* **Autolink bare URLs**: markdown-it’s “linkify” is an optional feature; Rich doesn’t enable it (Rich only enables table/strikethrough), so bare URLs won’t become clickable links unless you (a) write `[text](url)` or (b) swap parser config to something like `gfm-like`. ([Rich Documentation][1])
* **Footnotes / typographer / extra extensions**: these are plugin-style features in markdown-it’s architecture; Rich won’t render them unless you both enable parsing and add element renderers for the resulting token types. ([Markdown-it-py][4])

Hardening playbook (for “docs that render well in Rich”):

* Prefer **CommonMark + tables + strikethrough** constructs.
* Prefer explicit links: `[label](url)` (don’t rely on linkify).
* Avoid raw HTML; if you must include it, consider using a markdown-it preset that disables HTML parsing so it remains literal. ([Markdown-it-py][2])

---

## 21) Newline + spacing semantics (why some blocks “stick together”)

Spacing is governed by `MarkdownElement.new_line`:

* Default is `True` (insert a newline segment between rendered elements).
* Some elements set it `False` (notably `HorizontalRule`, and `ImageItem` placeholder rendering). ([Rich Documentation][1])

If you’re building a custom element and your output is “double spaced” or “jammed,” this flag is the first thing to tweak.

---

## 22) Output control: width, wrap, crop, overflow (terminal ergonomics)

Even with Markdown doing its own indentation math, **`Console.print()` is still your master control** for how the final render behaves:

Key `Console.print()` controls:

* `width`: force a target width (otherwise auto-detect). ([Rich Documentation][5])
* `no_wrap`: disable word wrapping. ([Rich Documentation][5])
* `soft_wrap`: “soft wrap mode” (disables word-wrapping *and* cropping). ([Rich Documentation][5])
* `overflow`: `"fold" | "crop" | "ellipsis" | "ignore"`; default `"fold"`. ([Rich Documentation][6])
* `crop`: crop output to terminal width (default True). ([Rich Documentation][5])

Example “docs viewer” posture:

```python
console.print(md, width=100, overflow="fold", crop=True)
```

Two key gotchas:

* **Fenced code blocks wrap regardless** unless you override `CodeBlock`, because Rich constructs `Syntax(..., word_wrap=True)` inside the Markdown renderer. ([Rich Documentation][1])
* Indented constructs (lists / blockquotes) compute `max_width - prefix` internally, so changing console width has a visible effect on their reflow. ([Rich Documentation][1])

---

## 23) Hyperlinks: what “enable hyperlinks” really means

Rich hyperlinks are implemented via the `Style(link=...)` attribute (terminal support-dependent). ([Rich Documentation][7])

Where Markdown hooks in:

* On `link_open`, if `hyperlinks=True`, Rich merges `markdown.link_url` style with `Style(link=href)` and pushes it on the style stack. ([Rich Documentation][1])
* If `hyperlinks=False`, Rich renders `text (url)` with separate styles (`markdown.link` and `markdown.link_url`). ([Rich Documentation][1])

Two easy-to-miss nuances:

* **Library default:** `Markdown(..., hyperlinks=True)` by default. ([Rich Documentation][8])
* **`python -m rich.markdown` default:** hyperlinks are **off unless you pass `-y`** (because the CLI flag is `store_true`). ([Rich Documentation][1])

(If you need hyperlinks outside Markdown, Rich’s own markup syntax supports `[link=URL]text[/link]` too.) ([Rich Documentation][9])

---

## 24) `rich-cli` as a “markdown viewer” (auto-detect, pager, export)

If your goal is literally “materialize markdown nicely in terminal,” `rich-cli` is often the path of least resistance.

Markdown behaviors:

* Auto-detect `.md`, or force with `--markdown` / `-m`. ([GitHub][10])
* Enable terminal hyperlinks with `--hyperlinks` / `-y`. ([GitHub][10])
* Built-in pager: `--pager`. ([GitHub][10])
* Read from URL (`https://...`) and render as markdown. ([GitHub][10])
* Export rendered output to HTML: `--export-html` / `-o output.html`. ([GitHub][10])

If you want “render once → ship artifact,” pair that with `Console(record=True)` + `export_html/export_svg/export_text` from the library side. ([Rich Documentation][6])

[1]: https://rich.readthedocs.io/en/stable/_modules/rich/markdown.html "rich.markdown — Rich 14.1.0 documentation"
[2]: https://markdown-it-py.readthedocs.io/en/latest/using.html?utm_source=chatgpt.com "Using markdown_it - markdown-it-py"
[3]: https://github.com/markdown-it/markdown-it/issues/320?utm_source=chatgpt.com "Token stream, type `html_inline` - Should \"tag\" also be ..."
[4]: https://markdown-it-py.readthedocs.io/en/latest/architecture.html?utm_source=chatgpt.com "Design principles - markdown-it-py - Read the Docs"
[5]: https://rich.readthedocs.io/en/stable/reference/console.html?utm_source=chatgpt.com "rich.console — Rich 14.1.0 documentation"
[6]: https://rich.readthedocs.io/en/latest/console.html?utm_source=chatgpt.com "Console API — Rich 14.1.0 documentation"
[7]: https://rich.readthedocs.io/en/latest/style.html?utm_source=chatgpt.com "Styles — Rich 14.1.0 documentation"
[8]: https://rich.readthedocs.io/en/stable/reference/markdown.html "rich.markdown — Rich 14.1.0 documentation"
[9]: https://rich.readthedocs.io/en/latest/markup.html?utm_source=chatgpt.com "Console Markup — Rich 14.1.0 documentation"
[10]: https://github.com/Textualize/rich-cli "GitHub - Textualize/rich-cli: Rich-cli is a command line toolbox for fancy output in the terminal"

## 25) “Hidden / reveal later” isn’t a native concept in `rich.markdown.Markdown`

`rich.markdown.Markdown` renders a markdown string into a **static** terminal renderable (there’s no folding state, no disclosure widgets, no click-to-expand semantics). Internally it’s just `MarkdownIt().enable("strikethrough").enable("table")` → parse → render token stream. ([Rich Documentation][1])

So: **not as a pure Markdown feature** (like HTML `<details>`), and **not as a “toggle” feature** inside Rich’s Markdown renderer.

---

## 26) Why `<details><summary>…</summary>…</details>` won’t give you collapsible sections in Rich Markdown

On the web, collapsible markdown is commonly done with HTML `<details>` blocks. In Rich Markdown this doesn’t translate into “collapsible UI” because:

* Rich’s Markdown renderer only maps a fixed set of markdown-it token types into renderable elements (paragraphs/headings/lists/blockquote/hr/code/image/tables, etc.). ([Rich Documentation][1])
* There’s **no element mapping for HTML block/inline tokens**, so HTML-based disclosure isn’t rendered as a widget (at best it prints literally, at worst it’s ignored depending on how the parser tokenizes it). ([Rich Documentation][1])

Net: `<details>` is **not** a usable “hide/reveal” mechanism in Rich terminal Markdown.

---

## 27) If you want real “expand/collapse” in terminal: use **Textual** (Rich-powered TUI)

Textual is designed for interactive terminal UI, and it has a first-class `Collapsible` container (“show/hide content… by clicking or focusing and pressing Enter”). ([Textual Documentation][2])
It also has a `Markdown` widget to display markdown documents. ([Textual Documentation][3])

**Minimal pattern: Markdown inside Collapsible**

````python
from textual.app import App, ComposeResult
from textual.widgets import Collapsible, Markdown

SUMMARY = """# Doc\n\nShort summary...\n"""
DETAILS = """## Deep details\n\nLong section...\n\n```python\nprint("hi")\n```"""

class DocApp(App):
    def compose(self) -> ComposeResult:
        yield Markdown(SUMMARY)
        with Collapsible(title="Deep details", collapsed=True):
            yield Markdown(DETAILS)

DocApp().run()
````

This gives you exactly what you described: **content exists locally (no re-query), but is hidden until expanded**.

---

## 28) If the “consumer” is an LLM: you can’t truly “hide” tokens from the model without gating what you send

If you hand an LLM a single artifact that *contains* the detailed text, the model “sees” it (and it counts against context), even if it’s visually hidden in a TUI.

So for LLM workflows, “reveal” usually means:

* **Generate once**, store the full markdown locally.
* Send the LLM only the **summary view** initially.
* When the LLM asks to “expand section X”, you **extract and send that slice** from the already-generated markdown (no regeneration; just retrieval).

This is the same “progressive disclosure” idea, but enforced at the transport boundary (what you include in the model context), not at rendering time.

---

## 29) Practical “generate once, reveal later” patterns without rebuilding markdown

### 29.1 Marker-blocks + view filter (single source, multiple renders)

Write one markdown document with explicit markers for “detail blocks”, and create two render modes:

* `--mode summary`: strip detail blocks before printing
* `--mode full`: print everything

Example markers:

```md
# Title

Summary paragraph.

<!--DETAIL:profiling-->
## Profiling deep dive
...lots of text...
<!--/DETAIL:profiling-->
```

Then your tool just does a cheap string/range filter and re-renders with Rich (no new query).

### 29.2 Section extractor (reveal a heading on demand)

Store the markdown to disk once, then reveal specific sections by heading.

* List headings (your ToC):

  * `rg -n '^#{1,6}\s' doc.md`
* Extract a heading range (“from `## X` until next `##`”)

This is especially LLM-friendly: the LLM can request “show me `## Profiling deep dive`”, and your agent returns just that slice.

### 29.3 Textual UI + backed by the same stored markdown

Combine the two:

* Textual provides human-friendly collapse/expand
* Your agent / LLM interface provides “expand by id” that reads the same stored markdown and returns only what’s requested

---

If you tell me whether your *primary consumer* is (a) **you in terminal**, (b) **an LLM agent with tool access**, or (c) **both**, I’ll give you a concrete “one-file doc format + reveal protocol” (marker scheme, section IDs, and extraction rules) that fits cleanly with Rich/Textual.

[1]: https://rich.readthedocs.io/en/stable/_modules/rich/markdown.html "rich.markdown — Rich 14.1.0 documentation"
[2]: https://textual.textualize.io/widgets/collapsible/ "Collapsible - Textual"
[3]: https://textual.textualize.io/widgets/markdown/ "Markdown - Textual"

## 30) One-file “LLM progressive disclosure” Markdown format (LDMD v1)

Goal: **generate the full doc once**, store it locally, and let the agent **reveal slices** by deterministic extraction (no regeneration). This is “hidden” in the only sense that matters for LLMs: **not sent in the model context unless requested**.

Rich/Textual compatibility note: Rich’s Markdown renderer is `markdown-it-py`-based and only maps a defined set of token types to renderable elements; anything else falls back to an unknown element handler. That means **HTML comment markers are safe as invisible structure metadata** (they’ll parse, but won’t render as visible content). ([Rich Documentation][1])

### 30.1 Marker scheme (HTML comments that won’t “materialize”)

Use **start/end section markers** at the beginning of a line (no indentation), one per line:

```md
<!--LDMD:BEGIN id="S03" parent="S00" title="CLI topology" level="2" tags="cli,config"-->
## CLI topology
...content...
<!--LDMD:END id="S03"-->
```

Why HTML comments:

* `markdown-it-py` CommonMark preset enables HTML tokenization (`html_inline`, `html_block`). ([Daobook][2])
* Rich Markdown only renders token types listed in its `elements` mapping (paragraphs, headings, lists, blockquotes, hr, code blocks, images, tables, etc.); everything else goes through a fallback element. ([Rich Documentation][1])

### 30.2 Required attributes (minimal but sufficient)

* `id` (stable key; unique in doc)
* `title` (human label; may drift over time)
* `level` (1–6; informational)
* `parent` (optional; enables a deterministic tree even if headings move)

Optional but useful:

* `tags` (comma-separated)
* `audience` (`agent|human|both`)
* `size_hint` (`xs|s|m|l|xl`) to guide retrieval budgeting

### 30.3 IDs: stable, hierarchical, never renumber

Recommended grammar:

* Top-level: `S00`, `S01`, `S02`…
* Nested: `S03.01`, `S03.02`…
* Deep: `S03.02.a` (only if you truly need it; prefer numeric)

Rules:

* **Never reuse an ID** for different content.
* **Never renumber** (append new sections; don’t reshuffle).
* Titles can change; IDs shouldn’t.

### 30.4 Built-in “reveal tiers” inside each section (TL;DR / body)

Give every section an optional **TL;DR block** with its own markers so the agent can request summary-first:

```md
<!--LDMD:TLDR_BEGIN id="S03"-->
- What this section is about…
- Key commands / syntax…
- Footguns…
<!--LDMD:TLDR_END id="S03"-->

<!--LDMD:BEGIN id="S03" ...-->
## CLI topology
...full detail...
<!--LDMD:END id="S03"-->
```

This allows the protocol to be: *fetch TL;DR → decide whether to fetch body*.

### 30.5 One-file manifest (optional, but great for fast “index”)

At top of file, include a hidden manifest (agent can read it instantly; renderers ignore it):

```md
<!--LDMD:MANIFEST
schema: 1
doc_id: "pyrefly.rich.markdown.v1"
created_at: "2026-02-08"
root_id: "S00"
-->
```

No need to list sections here—your indexer can derive them from `LDMD:BEGIN` lines—but the manifest helps with **versioning** and **schema migrations**.

---

## 31) Reveal protocol (LLM ↔ tool contract)

Assume the agent has a tool that can read files and run lightweight parsing/search. The protocol is intentionally small:

### 31.1 `INDEX(path) → SectionIndex`

Return *only metadata*, not content.

Suggested output shape:

```json
{
  "doc_id": "…",
  "schema": 1,
  "sections": [
    {"id":"S03","title":"CLI topology","parent":"S00","level":2,"tags":["cli","config"],"line_start":120,"line_end":260,"bytes":18422},
    {"id":"S03.01","title":"Core commands","parent":"S03","level":3,"line_start":140,"line_end":190,"bytes":6021}
  ]
}
```

Notes:

* Include either `(line_start,line_end)` **or** `(byte_start,byte_end)`. If you already like byte spans as canonical, store byte ranges (fast slicing, stable across different newline conventions).
* The agent uses this to choose what to reveal without ever loading the full doc into context.

### 31.2 `GET(path, id, mode, depth, cursor, limit) → Slice`

Modes:

* `mode="tldr"` → return TL;DR block only (if absent, return first N lines of the section)
* `mode="body"` → return section body excluding TL;DR (and excluding markers)
* `mode="full"` → TL;DR + body
* `depth=0|1|2…` → include children (subsections) to N depth (default 0)

Limits:

* `limit_bytes` (or `limit_chars`) hard-cap the returned payload.
* If truncated, return a continuation token:

```json
{"id":"S03","cursor":{"offset_bytes":4096}}
```

### 31.3 `SEARCH(path, query, scope_ids, max_hits) → Hits`

Return small snippets + section IDs, not whole blocks:

```json
{"hits":[{"id":"S03.02","line":211,"snippet":"--baseline=… --update-baseline …"}]}
```

### 31.4 `NEIGHBORS(path, id, before, after) → {prev,next}`

For “give me the surrounding context” without guessing which parent/child to fetch.

---

## 32) Extraction rules (deterministic, marker-first)

### 32.1 Section boundaries (regex-level contract)

* Begin marker: `^<!--LDMD:BEGIN\b.*\bid="([^"]+)"[^>]*-->`
* End marker: `^<!--LDMD:END\b.*\bid="([^"]+)"[^>]*-->`
* TL;DR begin/end similarly: `LDMD:TLDR_BEGIN`, `LDMD:TLDR_END`

Hard rules:

* Markers **must be on their own line**.
* `BEGIN/END` IDs must match (tool should validate).
* No nested `BEGIN/END` with the same ID.

### 32.2 Preferred extraction algorithm

1. Read file as bytes (`rb`) so offsets are stable.
2. Locate `BEGIN(id)` byte offset.
3. Locate the matching `END(id)` byte offset after it.
4. Slice `[begin,end]`, then post-process:

   * Remove marker lines
   * Optionally remove TL;DR block depending on mode
5. Return slice + continuation cursor if above limit.

This avoids “line-number drift” issues and plays nicely with your broader “byte-span canonical” worldview.

---

## 33) Authoring conventions that make retrieval *much* better for LLM agents

### 33.1 Keep sections small and composable

Instead of one giant `S03`, create child IDs:

* `S03.01 Core commands`
* `S03.02 Exit codes`
* `S03.03 Config precedence`

LLM agents do best when they can request **the exact micro-slice** they need.

### 33.2 Put “what to ask for next” inside TL;DR

Make TL;DR end with explicit suggested follow-ups:

* “If you need flag semantics, reveal `S03.01`”
* “If you need CI posture, reveal `S03.02`”

### 33.3 Make headings human-readable; keep IDs in markers

Because HTML comment markers won’t render in Rich’s Markdown output (only specific token types are mapped; others fall back), the doc still prints cleanly if you ever view it via Rich/Textual. ([Rich Documentation][1])

---

## 34) “Fits cleanly with Rich/Textual” (without relying on interactivity)

* Rich Markdown: uses `MarkdownIt()` and renders a known set of elements (headings, lists, code blocks, tables, etc.). ([Rich Documentation][1])
* Textual: has a `Markdown` widget (and a `MarkdownViewer` with ToC); collapsibles exist (`Collapsible`) if you ever want a human-facing TUI—but your LLM protocol doesn’t depend on any UI state. ([Textual Documentation][3])

If you want, I can also give you a **ready-to-drop “LDMD indexer/extractor”** in ~100 lines of Python (byte-offset index + `GET/SEARCH` primitives), but the spec above is the core contract your agent should follow.

[1]: https://rich.readthedocs.io/en/stable/_modules/rich/markdown.html "rich.markdown — Rich 14.1.0 documentation"
[2]: https://daobook.github.io/markdown-it-py/_modules/markdown_it/presets/commonmark.html?utm_source=chatgpt.com "markdown_it.presets.commonmark — markdown-it-py"
[3]: https://textual.textualize.io/widgets/markdown/?utm_source=chatgpt.com "Markdown - Textual"

## 35) `MarkdownViewer`: “browser-like” selective navigation over Markdown

* **Optional Table of Contents pane**: `MarkdownViewer` is explicitly described as adding “browser-like functionality” on top of `Markdown`, including an optional TOC. ([Textual Documentation][1])
* **History + navigation**:

  * `back()` / `forward()` move through history
  * `go(location)` navigates to a new document path ([Textual Documentation][1])
* **TOC visibility toggle**: `show_table_of_contents` is a documented attribute controlling whether the TOC is shown. ([Textual Documentation][1])

## 36) Table-of-contents as a first-class data model (jump-to-section hooks)

* **TOC structure (`TableOfContentsType`)**: the TOC is a list of triples encoding `(level, label, optional block_id)` for each heading. That “block_id” is the key to deterministic section addressing. ([Textual Documentation][1])
* **TOC events**:

  * `TableOfContentsUpdated(markdown, table_of_contents)` fires when TOC changes. ([Textual Documentation][1])
  * `TableOfContentsSelected(markdown, block_id)` fires when a TOC item is selected, providing the selected `block_id`. ([Textual Documentation][1])
* **Standalone TOC widget**: `MarkdownTableOfContents` is a separate widget that “displays a table of contents for a markdown document,” with a reactive `table_of_contents`, plus `rebuild_table_of_contents(...)` and `watch_table_of_contents(...)`. ([Textual Documentation][1])

## 37) Anchor navigation (`#...`) and stable “jump” semantics

* **`goto_anchor(anchor)`**: searches headings for a slug matching the anchor; docs note the slugging is “similar to that found on GitHub.” ([Textual Documentation][2])
* **`sanitize_location(location)`**: splits a location into `(path, anchor)` so `path.md#section` can be routed predictably. ([Textual Documentation][1])

## 38) Link routing as your selective-delivery control plane

This is the most direct “selective reveal” mechanism in Textual’s Markdown stack: treat links as *commands*.

* **`open_links` switch**:

  * When `open_links=True`, links open automatically.
  * When `open_links=False`, you can handle `LinkClicked` events yourself. ([Textual Documentation][1])
* **`LinkClicked` payload** includes the clicked `href` (and the `Markdown` widget that contained it). ([Textual Documentation][1])
* **Per-block handler hook**: `MarkdownBlock.action_link(href)` is explicitly documented as “Called on link click.” ([Textual Documentation][1])

Practical pattern: encode “section links” like `[Expand S03.02](ldmd://S03.02)` and route `href` to your file slicer instead of opening anything.

## 39) Parser control + token extensibility (structure beyond vanilla Markdown)

* **`parser_factory`**: both `Markdown` and `MarkdownViewer` accept a factory that returns a configured `MarkdownIt` parser; if `None`, Textual uses a “gfm-like” parser. ([Textual Documentation][1])
  *This is the knob that lets you parse custom directives or extensions (then render them via custom blocks).*
* **Unhandled token hook**: `Markdown.unhandled_token(token)` lets you “Process an unhandled token” and return either a `MarkdownBlock` widget to include in output or `None`. ([Textual Documentation][2])
* **Block-class resolution**: `get_block_class(block_name)` returns the widget class used for a given block token name. ([Textual Documentation][1])
* **Inline style component classes**: `MarkdownBlock.COMPONENT_CLASSES = {'em','strong','s','code_inline'}` defines the inline styling hooks; modifying them can break standard formatting (useful to know if you theme aggressively). ([Textual Documentation][1])

## 40) Streaming + incremental loading (selective delivery without re-render storms)

If you want “progressively reveal” *within the same session* (e.g., agent asks for deeper detail and you stream it in chunks):

* **`append(markdown_fragment)`**: append fragments; returns an awaitable that you can await to ensure the UI updates. ([Textual Documentation][2])
* **`update(markdown)`**: replace the document contents; returns an awaitable to ensure children mount. ([Textual Documentation][2])
* **`Markdown.get_stream(markdown_widget)` → `MarkdownStream`**:

  * Designed specifically to batch updates when appending frequently (docs call out ~20 appends/sec as a point where the UI can lag).
  * `MarkdownStream.write(fragment)` enqueues/accumulates fragments; `stop()` stops/awaits finish. ([Textual Documentation][2])
* **Async execution model**: Textual’s widget guide notes “Every widget runs in its own asyncio task,” which is exactly what you want for background fetch + controlled reveal. ([Textual Documentation][3])

## 41) Multi-view selective presentation (summary vs detail, multiple docs)

These are the “structure the experience” primitives you didn’t use earlier:

* **`ContentSwitcher`**: container that “switch[es] display between multiple child widgets”; you set `ContentSwitcher.current` (or call `set_current`) to select which child is visible. ([Textual Documentation][4])
* **`TabbedContent`**:

  * Composed of a `Tabs` + a `ContentSwitcher`.
  * Has a reactive `active` attribute (“Set this to switch tabs.”). ([Textual Documentation][5])
    This is ideal for: `Summary` | `Full` | `Raw` | `Index` | `Search hits` tabs, all sourced from the same underlying one-file doc.

## 42) File + outline navigation widgets for selective loading

* **`DirectoryTree`**: a filesystem tree control; use it to browse a docs folder and load only the chosen file into `MarkdownViewer`. ([Textual Documentation][6])
* **`Tree`**: general-purpose tree with a `root` node and methods like `add()` / `add_leaf()` to build arbitrary hierarchies. Use this to display *your own* LDMD section tree (IDs, tags, sizes) and load slices on selection. ([Textual Documentation][7])

## 43) “Selective interaction” inside Markdown tables (DataTable-backed)

If your markdown contains large tables, Textual’s stack can give you selective interaction *within* that content:

* `MarkdownViewer`’s own example explicitly states: “Tables are displayed in a DataTable widget.” ([Textual Documentation][1])
* `DataTable` supports cursor navigation, mouse click responses, updates, row/column deletion, and per-cell Rich renderables. ([Textual Documentation][8])

This matters because you can keep the doc static but still let the consumer (human or agent-driven tooling) focus on a row/column subset via events.

---

If you want, I can translate these Textual primitives into an **agent-first protocol** that uses `open_links=False` + `LinkClicked.href` as the single “expand” command channel, and never loads more than the requested LDMD slice into any widget (or into the agent context).

[1]: https://textual.textualize.io/widgets/markdown_viewer/ "MarkdownViewer - Textual"
[2]: https://textual.textualize.io/widgets/markdown/ "Markdown - Textual"
[3]: https://textual.textualize.io/guide/widgets/?utm_source=chatgpt.com "Widgets - Textual"
[4]: https://textual.textualize.io/widgets/content_switcher/ "ContentSwitcher - Textual"
[5]: https://textual.textualize.io/widgets/tabbed_content/ "TabbedContent - Textual"
[6]: https://textual.textualize.io/widgets/directory_tree/ "DirectoryTree - Textual"
[7]: https://textual.textualize.io/widgets/tree/?utm_source=chatgpt.com "Tree - Textual"
[8]: https://textual.textualize.io/widgets/data_table/?utm_source=chatgpt.com "DataTable - Textual"
