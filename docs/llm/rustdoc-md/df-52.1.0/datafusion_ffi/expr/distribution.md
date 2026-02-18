**datafusion_ffi > expr > distribution**

# Module: expr::distribution

## Contents

**Structs**

- [`FFI_BernoulliDistribution`](#ffi_bernoullidistribution)
- [`FFI_ExponentialDistribution`](#ffi_exponentialdistribution)
- [`FFI_GaussianDistribution`](#ffi_gaussiandistribution)
- [`FFI_GenericDistribution`](#ffi_genericdistribution)
- [`FFI_UniformDistribution`](#ffi_uniformdistribution)

**Enums**

- [`FFI_Distribution`](#ffi_distribution) - A stable struct for sharing [`Distribution`] across FFI boundaries.

---

## datafusion_ffi::expr::distribution::FFI_BernoulliDistribution

*Struct*

**Traits:** GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **TryFrom**
  - `fn try_from(value: &BernoulliDistribution) -> Result<Self, <Self as >::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_ffi::expr::distribution::FFI_Distribution

*Enum*

A stable struct for sharing [`Distribution`] across FFI boundaries.
See ['Distribution'] for the meaning of each variant.

**Variants:**
- `Uniform(FFI_UniformDistribution)`
- `Exponential(FFI_ExponentialDistribution)`
- `Gaussian(FFI_GaussianDistribution)`
- `Bernoulli(FFI_BernoulliDistribution)`
- `Generic(FFI_GenericDistribution)`

**Traits:** GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **TryFrom**
  - `fn try_from(value: &Distribution) -> Result<Self, <Self as >::Error>`



## datafusion_ffi::expr::distribution::FFI_ExponentialDistribution

*Struct*

**Traits:** GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **TryFrom**
  - `fn try_from(value: &ExponentialDistribution) -> Result<Self, <Self as >::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_ffi::expr::distribution::FFI_GaussianDistribution

*Struct*

**Traits:** GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **TryFrom**
  - `fn try_from(value: &GaussianDistribution) -> Result<Self, <Self as >::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_ffi::expr::distribution::FFI_GenericDistribution

*Struct*

**Traits:** GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **TryFrom**
  - `fn try_from(value: &GenericDistribution) -> Result<Self, <Self as >::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_ffi::expr::distribution::FFI_UniformDistribution

*Struct*

**Traits:** GetStaticEquivalent_, StableAbi

**Trait Implementations:**

- **TryFrom**
  - `fn try_from(value: &UniformDistribution) -> Result<Self, <Self as >::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



