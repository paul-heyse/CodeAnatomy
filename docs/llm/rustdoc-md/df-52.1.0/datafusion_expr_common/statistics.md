**datafusion_expr_common > statistics**

# Module: statistics

## Contents

**Structs**

- [`BernoulliDistribution`](#bernoullidistribution) - Bernoulli distribution with success probability `p`. If `p` has a null value,
- [`ExponentialDistribution`](#exponentialdistribution) - Exponential distribution with an optional shift. The probability density
- [`GaussianDistribution`](#gaussiandistribution) - Gaussian (normal) distribution, represented by its mean and variance.
- [`GenericDistribution`](#genericdistribution) - A generic distribution whose functional form is not available, which is
- [`UniformDistribution`](#uniformdistribution) - Uniform distribution, represented by its range. If the given range extends

**Enums**

- [`Distribution`](#distribution) - This object defines probabilistic distributions that encode uncertain

**Functions**

- [`combine_bernoullis`](#combine_bernoullis) - This function takes a logical operator and two Bernoulli distributions,
- [`combine_gaussians`](#combine_gaussians) - Applies the given operation to the given Gaussian distributions. Currently,
- [`compute_mean`](#compute_mean) - Computes the mean value for the result of the given binary operation on
- [`compute_median`](#compute_median) - Computes the median value for the result of the given binary operation on
- [`compute_variance`](#compute_variance) - Computes the variance value for the result of the given binary operation on
- [`create_bernoulli_from_comparison`](#create_bernoulli_from_comparison) - Creates a new `Bernoulli` distribution by computing the resulting probability.
- [`new_generic_from_binary_op`](#new_generic_from_binary_op) - Creates a new [`Generic`] distribution that represents the result of the

---

## datafusion_expr_common::statistics::BernoulliDistribution

*Struct*

Bernoulli distribution with success probability `p`. If `p` has a null value,
the success probability is unknown. For a more in-depth discussion, see:

<https://en.wikipedia.org/wiki/Bernoulli_distribution>

**Methods:**

- `fn data_type(self: &Self) -> DataType`
- `fn p_value(self: &Self) -> &ScalarValue`
- `fn mean(self: &Self) -> &ScalarValue`
- `fn median(self: &Self) -> Result<ScalarValue>` - Computes the median value of this distribution. In case of an unknown
- `fn variance(self: &Self) -> Result<ScalarValue>` - Computes the variance value of this distribution. In case of an unknown
- `fn range(self: &Self) -> Interval`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &BernoulliDistribution) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> BernoulliDistribution`



## datafusion_expr_common::statistics::Distribution

*Enum*

This object defines probabilistic distributions that encode uncertain
information about a single, scalar value. Currently, we support five core
statistical distributions. New variants will be added over time.

This object is the lowest-level object in the statistics hierarchy, and it
is the main unit of calculus when evaluating expressions in a statistical
context. Notions like column and table statistics are built on top of this
object and the operations it supports.

**Variants:**
- `Uniform(UniformDistribution)`
- `Exponential(ExponentialDistribution)`
- `Gaussian(GaussianDistribution)`
- `Bernoulli(BernoulliDistribution)`
- `Generic(GenericDistribution)`

**Methods:**

- `fn new_uniform(interval: Interval) -> Result<Self>` - Constructs a new [`Uniform`] distribution from the given [`Interval`].
- `fn new_exponential(rate: ScalarValue, offset: ScalarValue, positive_tail: bool) -> Result<Self>` - Constructs a new [`Exponential`] distribution from the given rate/offset
- `fn new_gaussian(mean: ScalarValue, variance: ScalarValue) -> Result<Self>` - Constructs a new [`Gaussian`] distribution from the given mean/variance
- `fn new_bernoulli(p: ScalarValue) -> Result<Self>` - Constructs a new [`Bernoulli`] distribution from the given success
- `fn new_generic(mean: ScalarValue, median: ScalarValue, variance: ScalarValue, range: Interval) -> Result<Self>` - Constructs a new [`Generic`] distribution from the given mean, median,
- `fn new_from_interval(range: Interval) -> Result<Self>` - Constructs a new [`Generic`] distribution from the given range. Other
- `fn mean(self: &Self) -> Result<ScalarValue>` - Extracts the mean value of this uncertain quantity, depending on its
- `fn median(self: &Self) -> Result<ScalarValue>` - Extracts the median value of this uncertain quantity, depending on its
- `fn variance(self: &Self) -> Result<ScalarValue>` - Extracts the variance value of this uncertain quantity, depending on
- `fn range(self: &Self) -> Result<Interval>` - Extracts the range of this uncertain quantity, depending on its
- `fn data_type(self: &Self) -> DataType` - Returns the data type of the statistical parameters comprising this
- `fn target_type(args: &[&ScalarValue]) -> Result<DataType>`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Distribution) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> Distribution`



## datafusion_expr_common::statistics::ExponentialDistribution

*Struct*

Exponential distribution with an optional shift. The probability density
function (PDF) is defined as follows:

For a positive tail (when `positive_tail` is `true`):

`f(x; λ, offset) = λ exp(-λ (x - offset))    for x ≥ offset`

For a negative tail (when `positive_tail` is `false`):

`f(x; λ, offset) = λ exp(-λ (offset - x))    for x ≤ offset`


In both cases, the PDF is `0` outside the specified domain.

For more information, see:

<https://en.wikipedia.org/wiki/Exponential_distribution>

**Methods:**

- `fn data_type(self: &Self) -> DataType`
- `fn rate(self: &Self) -> &ScalarValue`
- `fn offset(self: &Self) -> &ScalarValue`
- `fn positive_tail(self: &Self) -> bool`
- `fn mean(self: &Self) -> Result<ScalarValue>`
- `fn median(self: &Self) -> Result<ScalarValue>`
- `fn variance(self: &Self) -> Result<ScalarValue>`
- `fn range(self: &Self) -> Result<Interval>`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &ExponentialDistribution) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> ExponentialDistribution`



## datafusion_expr_common::statistics::GaussianDistribution

*Struct*

Gaussian (normal) distribution, represented by its mean and variance.
For a more in-depth discussion, see:

<https://en.wikipedia.org/wiki/Normal_distribution>

**Methods:**

- `fn data_type(self: &Self) -> DataType`
- `fn mean(self: &Self) -> &ScalarValue`
- `fn variance(self: &Self) -> &ScalarValue`
- `fn median(self: &Self) -> &ScalarValue`
- `fn range(self: &Self) -> Result<Interval>`

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> GaussianDistribution`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &GaussianDistribution) -> bool`



## datafusion_expr_common::statistics::GenericDistribution

*Struct*

A generic distribution whose functional form is not available, which is
approximated via some summary statistics. For a more in-depth discussion, see:

<https://en.wikipedia.org/wiki/Summary_statistics>

**Methods:**

- `fn data_type(self: &Self) -> DataType`
- `fn mean(self: &Self) -> &ScalarValue`
- `fn median(self: &Self) -> &ScalarValue`
- `fn variance(self: &Self) -> &ScalarValue`
- `fn range(self: &Self) -> &Interval`

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &GenericDistribution) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> GenericDistribution`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr_common::statistics::UniformDistribution

*Struct*

Uniform distribution, represented by its range. If the given range extends
towards infinity, the distribution will be improper -- which is OK. For a
more in-depth discussion, see:

<https://en.wikipedia.org/wiki/Continuous_uniform_distribution>
<https://en.wikipedia.org/wiki/Prior_probability#Improper_priors>

**Methods:**

- `fn data_type(self: &Self) -> DataType`
- `fn mean(self: &Self) -> Result<ScalarValue>` - Computes the mean value of this distribution. In case of improper
- `fn median(self: &Self) -> Result<ScalarValue>`
- `fn variance(self: &Self) -> Result<ScalarValue>` - Computes the variance value of this distribution. In case of improper
- `fn range(self: &Self) -> &Interval`

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &UniformDistribution) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> UniformDistribution`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr_common::statistics::combine_bernoullis

*Function*

This function takes a logical operator and two Bernoulli distributions,
and it returns a new Bernoulli distribution that represents the result of
the operation. Currently, only `AND` and `OR` operations are supported.

```rust
fn combine_bernoullis(op: &crate::operator::Operator, left: &BernoulliDistribution, right: &BernoulliDistribution) -> datafusion_common::Result<BernoulliDistribution>
```



## datafusion_expr_common::statistics::combine_gaussians

*Function*

Applies the given operation to the given Gaussian distributions. Currently,
this function handles only addition and subtraction operations. If the
result is not a Gaussian random variable, it returns `None`. For details,
see:

<https://en.wikipedia.org/wiki/Sum_of_normally_distributed_random_variables>

```rust
fn combine_gaussians(op: &crate::operator::Operator, left: &GaussianDistribution, right: &GaussianDistribution) -> datafusion_common::Result<Option<GaussianDistribution>>
```



## datafusion_expr_common::statistics::compute_mean

*Function*

Computes the mean value for the result of the given binary operation on
two unknown quantities represented by their [`Distribution`] objects.

```rust
fn compute_mean(op: &crate::operator::Operator, left: &Distribution, right: &Distribution) -> datafusion_common::Result<datafusion_common::ScalarValue>
```



## datafusion_expr_common::statistics::compute_median

*Function*

Computes the median value for the result of the given binary operation on
two unknown quantities represented by its [`Distribution`] objects. Currently,
the median is calculable only for addition and subtraction operations on:
- [`Uniform`] and [`Uniform`] distributions, and
- [`Gaussian`] and [`Gaussian`] distributions.

```rust
fn compute_median(op: &crate::operator::Operator, left: &Distribution, right: &Distribution) -> datafusion_common::Result<datafusion_common::ScalarValue>
```



## datafusion_expr_common::statistics::compute_variance

*Function*

Computes the variance value for the result of the given binary operation on
two unknown quantities represented by their [`Distribution`] objects.

```rust
fn compute_variance(op: &crate::operator::Operator, left: &Distribution, right: &Distribution) -> datafusion_common::Result<datafusion_common::ScalarValue>
```



## datafusion_expr_common::statistics::create_bernoulli_from_comparison

*Function*

Creates a new `Bernoulli` distribution by computing the resulting probability.
Expects `op` to be a comparison operator, with `left` and `right` having
numeric distributions. The resulting distribution has the `Float64` data
type.

```rust
fn create_bernoulli_from_comparison(op: &crate::operator::Operator, left: &Distribution, right: &Distribution) -> datafusion_common::Result<Distribution>
```



## datafusion_expr_common::statistics::new_generic_from_binary_op

*Function*

Creates a new [`Generic`] distribution that represents the result of the
given binary operation on two unknown quantities represented by their
[`Distribution`] objects. The function computes the mean, median and
variance if possible.

```rust
fn new_generic_from_binary_op(op: &crate::operator::Operator, left: &Distribution, right: &Distribution) -> datafusion_common::Result<Distribution>
```



