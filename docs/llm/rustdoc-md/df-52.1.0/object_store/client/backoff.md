**object_store > client > backoff**

# Module: client::backoff

## Contents

**Structs**

- [`BackoffConfig`](#backoffconfig) - Exponential backoff with decorrelated jitter algorithm

---

## object_store::client::backoff::BackoffConfig

*Struct*

Exponential backoff with decorrelated jitter algorithm

The first backoff will always be `init_backoff`.

Subsequent backoffs will pick a random value between `init_backoff` and
`base * previous` where `previous` is the duration of the previous backoff

See <https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/>

**Fields:**
- `init_backoff: std::time::Duration` - The initial backoff duration
- `max_backoff: std::time::Duration` - The maximum backoff duration
- `base: f64` - The multiplier to use for the next backoff duration

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **Clone**
  - `fn clone(self: &Self) -> BackoffConfig`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



