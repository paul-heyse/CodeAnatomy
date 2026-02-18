**object_store > aws > checksum**

# Module: aws::checksum

## Contents

**Enums**

- [`Checksum`](#checksum) - Enum representing checksum algorithm supported by S3.

---

## object_store::aws::checksum::Checksum

*Enum*

Enum representing checksum algorithm supported by S3.

**Variants:**
- `SHA256` - SHA-256 algorithm.

**Traits:** Eq, Copy

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Checksum`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Checksum) -> bool`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **FromStr**
  - `fn from_str(s: &str) -> Result<Self, <Self as >::Err>`
- **TryFrom**
  - `fn try_from(value: &String) -> Result<Self, <Self as >::Error>`



