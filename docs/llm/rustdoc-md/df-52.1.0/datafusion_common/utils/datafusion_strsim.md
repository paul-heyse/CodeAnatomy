**datafusion_common > utils > datafusion_strsim**

# Module: utils::datafusion_strsim

## Contents

**Functions**

- [`levenshtein`](#levenshtein) - Calculates the minimum number of insertions, deletions, and substitutions
- [`levenshtein_with_buffer`](#levenshtein_with_buffer) - Calculates the Levenshtein distance using a reusable cache buffer.
- [`normalized_levenshtein`](#normalized_levenshtein) - Calculates the normalized Levenshtein distance between two strings.

---

## datafusion_common::utils::datafusion_strsim::levenshtein

*Function*

Calculates the minimum number of insertions, deletions, and substitutions
required to change one string into the other.

```
use datafusion_common::utils::datafusion_strsim::levenshtein;

assert_eq!(3, levenshtein("kitten", "sitting"));
```

```rust
fn levenshtein(a: &str, b: &str) -> usize
```



## datafusion_common::utils::datafusion_strsim::levenshtein_with_buffer

*Function*

Calculates the Levenshtein distance using a reusable cache buffer.
This avoids allocating a new Vec for each call, improving performance
when computing many distances.

The `cache` buffer will be resized as needed and reused across calls.

```rust
fn levenshtein_with_buffer(a: &str, b: &str, cache: & mut Vec<usize>) -> usize
```



## datafusion_common::utils::datafusion_strsim::normalized_levenshtein

*Function*

Calculates the normalized Levenshtein distance between two strings.
The normalized distance is a value between 0.0 and 1.0, where 1.0 indicates
that the strings are identical and 0.0 indicates no similarity.

```
use datafusion_common::utils::datafusion_strsim::normalized_levenshtein;

assert!((normalized_levenshtein("kitten", "sitting") - 0.57142).abs() < 0.00001);

assert!(normalized_levenshtein("", "second").abs() < 0.00001);

assert!((normalized_levenshtein("kitten", "sitten") - 0.833).abs() < 0.001);
```

```rust
fn normalized_levenshtein(a: &str, b: &str) -> f64
```



