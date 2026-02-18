**datafusion_functions > crypto > basic**

# Module: crypto::basic

## Contents

**Enums**

- [`DigestAlgorithm`](#digestalgorithm)

**Functions**

- [`blake2b`](#blake2b) - computes blake2b hash digest of the given input
- [`blake2s`](#blake2s) - computes blake2s hash digest of the given input
- [`blake3`](#blake3) - computes blake3 hash digest of the given input
- [`digest_process`](#digest_process)
- [`md5`](#md5) - computes md5 hash digest of the given input
- [`sha224`](#sha224) - computes sha224 hash digest of the given input
- [`sha256`](#sha256) - computes sha256 hash digest of the given input
- [`sha384`](#sha384) - computes sha384 hash digest of the given input
- [`sha512`](#sha512) - computes sha512 hash digest of the given input

---

## datafusion_functions::crypto::basic::DigestAlgorithm

*Enum*

**Variants:**
- `Md5`
- `Sha224`
- `Sha256`
- `Sha384`
- `Sha512`
- `Blake2s`
- `Blake2b`
- `Blake3`

**Traits:** Eq, Copy

**Trait Implementations:**

- **FromStr**
  - `fn from_str(name: &str) -> Result<DigestAlgorithm>`
- **Display**
  - `fn fmt(self: &Self, f: & mut fmt::Formatter) -> fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> DigestAlgorithm`
- **PartialEq**
  - `fn eq(self: &Self, other: &DigestAlgorithm) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_functions::crypto::basic::blake2b

*Function*

computes blake2b hash digest of the given input

```rust
fn blake2b(args: &[ColumnarValue]) -> Result<ColumnarValue>
```



## datafusion_functions::crypto::basic::blake2s

*Function*

computes blake2s hash digest of the given input

```rust
fn blake2s(args: &[ColumnarValue]) -> Result<ColumnarValue>
```



## datafusion_functions::crypto::basic::blake3

*Function*

computes blake3 hash digest of the given input

```rust
fn blake3(args: &[ColumnarValue]) -> Result<ColumnarValue>
```



## datafusion_functions::crypto::basic::digest_process

*Function*

```rust
fn digest_process(value: &datafusion_expr::ColumnarValue, digest_algorithm: DigestAlgorithm) -> datafusion_common::Result<datafusion_expr::ColumnarValue>
```



## datafusion_functions::crypto::basic::md5

*Function*

computes md5 hash digest of the given input

```rust
fn md5(args: &[datafusion_expr::ColumnarValue]) -> datafusion_common::Result<datafusion_expr::ColumnarValue>
```



## datafusion_functions::crypto::basic::sha224

*Function*

computes sha224 hash digest of the given input

```rust
fn sha224(args: &[ColumnarValue]) -> Result<ColumnarValue>
```



## datafusion_functions::crypto::basic::sha256

*Function*

computes sha256 hash digest of the given input

```rust
fn sha256(args: &[ColumnarValue]) -> Result<ColumnarValue>
```



## datafusion_functions::crypto::basic::sha384

*Function*

computes sha384 hash digest of the given input

```rust
fn sha384(args: &[ColumnarValue]) -> Result<ColumnarValue>
```



## datafusion_functions::crypto::basic::sha512

*Function*

computes sha512 hash digest of the given input

```rust
fn sha512(args: &[ColumnarValue]) -> Result<ColumnarValue>
```



