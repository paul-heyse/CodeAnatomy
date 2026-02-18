**object_store > delimited**

# Module: delimited

## Contents

**Functions**

- [`newline_delimited_stream`](#newline_delimited_stream) - Given a [`Stream`] of [`Bytes`] returns a [`Stream`] where each

---

## object_store::delimited::newline_delimited_stream

*Function*

Given a [`Stream`] of [`Bytes`] returns a [`Stream`] where each
yielded [`Bytes`] contains a whole number of new line delimited records
accounting for `\` style escapes and `"` quotes

```rust
fn newline_delimited_stream<S>(s: S) -> impl Trait
```



