**datafusion_expr_common > signature**

# Module: signature

## Contents

**Structs**

- [`ImplicitCoercion`](#implicitcoercion) - Defines rules for implicit type coercion, specifying which source types can be
- [`Signature`](#signature) - Provides  information necessary for calling a function.

**Enums**

- [`Arity`](#arity) - Represents the arity (number of arguments) of a function signature
- [`ArrayFunctionArgument`](#arrayfunctionargument)
- [`ArrayFunctionSignature`](#arrayfunctionsignature)
- [`Coercion`](#coercion) - Represents type coercion rules for function arguments, specifying both the desired type
- [`TypeSignature`](#typesignature) - The types of arguments for which a function has implementations.
- [`TypeSignatureClass`](#typesignatureclass) - Represents the class of types that can be used in a function signature.
- [`Volatility`](#volatility) - How a function's output changes with respect to a fixed input

**Constants**

- [`FIXED_SIZE_LIST_WILDCARD`](#fixed_size_list_wildcard) - Constant that is used as a placeholder for any valid fixed size list.
- [`TIMEZONE_WILDCARD`](#timezone_wildcard) - Constant that is used as a placeholder for any valid timezone.

---

## datafusion_expr_common::signature::Arity

*Enum*

Represents the arity (number of arguments) of a function signature

**Variants:**
- `Fixed(usize)` - Fixed number of arguments
- `Variable` - Variable number of arguments (e.g., Variadic, VariadicAny, UserDefined)

**Traits:** Eq, Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Arity) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> Arity`



## datafusion_expr_common::signature::ArrayFunctionArgument

*Enum*

**Variants:**
- `Element` - A non-list or list argument. The list dimensions should be one less than the Array's list
- `Index` - An Int64 index argument.
- `Array` - An argument of type List/LargeList/FixedSizeList. All Array arguments must be coercible
- `String`

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Clone**
  - `fn clone(self: &Self) -> ArrayFunctionArgument`
- **PartialEq**
  - `fn eq(self: &Self, other: &ArrayFunctionArgument) -> bool`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &ArrayFunctionArgument) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr_common::signature::ArrayFunctionSignature

*Enum*

**Variants:**
- `Array{ arguments: Vec<ArrayFunctionArgument>, array_coercion: Option<datafusion_common::utils::ListCoercion> }` - A function takes at least one List/LargeList/FixedSizeList argument.
- `RecursiveArray` - A function takes a single argument that must be a List/LargeList/FixedSizeList
- `MapArray` - Specialized Signature for MapArray

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Clone**
  - `fn clone(self: &Self) -> ArrayFunctionSignature`
- **PartialEq**
  - `fn eq(self: &Self, other: &ArrayFunctionSignature) -> bool`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &ArrayFunctionSignature) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr_common::signature::Coercion

*Enum*

Represents type coercion rules for function arguments, specifying both the desired type
and optional implicit coercion rules for source types.

# Examples

```
use datafusion_common::types::{logical_binary, logical_string, NativeType};
use datafusion_expr_common::signature::{Coercion, TypeSignatureClass};

// Exact coercion that only accepts timestamp types
let exact = Coercion::new_exact(TypeSignatureClass::Timestamp);

// Implicit coercion that accepts string types but can coerce from binary types
let implicit = Coercion::new_implicit(
    TypeSignatureClass::Native(logical_string()),
    vec![TypeSignatureClass::Native(logical_binary())],
    NativeType::String,
);
```

There are two variants:

* `Exact` - Only accepts arguments that exactly match the desired type
* `Implicit` - Accepts the desired type and can coerce from specified source types

**Variants:**
- `Exact{ desired_type: TypeSignatureClass }` - Coercion that only accepts arguments exactly matching the desired type.
- `Implicit{ desired_type: TypeSignatureClass, implicit_coercion: ImplicitCoercion }` - Coercion that accepts the desired type and can implicitly coerce from other types.

**Methods:**

- `fn new_exact(desired_type: TypeSignatureClass) -> Self`
- `fn new_implicit(desired_type: TypeSignatureClass, allowed_source_types: Vec<TypeSignatureClass>, default_casted_type: NativeType) -> Self` - Create a new coercion with implicit coercion rules.
- `fn allowed_source_types(self: &Self) -> &[TypeSignatureClass]`
- `fn default_casted_type(self: &Self) -> Option<&NativeType>`
- `fn desired_type(self: &Self) -> &TypeSignatureClass`
- `fn implicit_coercion(self: &Self) -> Option<&ImplicitCoercion>`

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Coercion`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Coercion) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Hash**
  - `fn hash<H>(self: &Self, state: & mut H)`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`



## datafusion_expr_common::signature::FIXED_SIZE_LIST_WILDCARD

*Constant*: `i32`

Constant that is used as a placeholder for any valid fixed size list.
This is used where a function can accept a fixed size list type with any
valid length. It exists to avoid the need to enumerate all possible fixed size list lengths.



## datafusion_expr_common::signature::ImplicitCoercion

*Struct*

Defines rules for implicit type coercion, specifying which source types can be
coerced and the default type to use when coercing.

This is used by functions to specify which types they can accept via implicit
coercion in addition to their primary desired type.

# Examples

```
use arrow::datatypes::TimeUnit;

use datafusion_expr_common::signature::{Coercion, ImplicitCoercion, TypeSignatureClass};
use datafusion_common::types::{NativeType, logical_binary};

// Allow coercing from binary types to timestamp, coerce to specific timestamp unit and timezone
let implicit = Coercion::new_implicit(
    TypeSignatureClass::Timestamp,
    vec![TypeSignatureClass::Native(logical_binary())],
    NativeType::Timestamp(TimeUnit::Second, None),
);
```

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<H>(self: &Self, state: & mut H)`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> ImplicitCoercion`
- **PartialEq**
  - `fn eq(self: &Self, other: &Self) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &ImplicitCoercion) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr_common::signature::Signature

*Struct*

Provides  information necessary for calling a function.

- [`TypeSignature`] defines the argument types that a function has implementations
  for.

- [`Volatility`] defines how the output of the function changes with the input.

**Fields:**
- `type_signature: TypeSignature` - The data types that the function accepts. See [TypeSignature] for more information.
- `volatility: Volatility` - The volatility of the function. See [Volatility] for more information.
- `parameter_names: Option<Vec<String>>` - Optional parameter names for the function arguments.

**Methods:**

- `fn new(type_signature: TypeSignature, volatility: Volatility) -> Self` - Creates a new Signature from a given type signature and volatility.
- `fn variadic(common_types: Vec<DataType>, volatility: Volatility) -> Self` - An arbitrary number of arguments with the same type, from those listed in `common_types`.
- `fn user_defined(volatility: Volatility) -> Self` - User-defined coercion rules for the function.
- `fn numeric(arg_count: usize, volatility: Volatility) -> Self` - A specified number of numeric arguments
- `fn string(arg_count: usize, volatility: Volatility) -> Self` - A specified number of string arguments
- `fn variadic_any(volatility: Volatility) -> Self` - An arbitrary number of arguments of any type.
- `fn uniform(arg_count: usize, valid_types: Vec<DataType>, volatility: Volatility) -> Self` - A fixed number of arguments of the same type, from those listed in `valid_types`.
- `fn exact(exact_types: Vec<DataType>, volatility: Volatility) -> Self` - Exactly matches the types in `exact_types`, in order.
- `fn coercible(target_types: Vec<Coercion>, volatility: Volatility) -> Self` - Target coerce types in order
- `fn comparable(arg_count: usize, volatility: Volatility) -> Self` - Used for function that expects comparable data types, it will try to coerced all the types into single final one.
- `fn nullary(volatility: Volatility) -> Self`
- `fn any(arg_count: usize, volatility: Volatility) -> Self` - A specified number of arguments of any type
- `fn one_of(type_signatures: Vec<TypeSignature>, volatility: Volatility) -> Self` - Any one of a list of [TypeSignature]s.
- `fn array_and_element(volatility: Volatility) -> Self` - Specialized [Signature] for ArrayAppend and similar functions.
- `fn element_and_array(volatility: Volatility) -> Self` - Specialized [Signature] for ArrayPrepend and similar functions.
- `fn arrays(n: usize, coercion: Option<ListCoercion>, volatility: Volatility) -> Self` - Specialized [Signature] for functions that take a fixed number of arrays.
- `fn array_and_element_and_optional_index(volatility: Volatility) -> Self` - Specialized [Signature] for Array functions with an optional index.
- `fn array_and_index(volatility: Volatility) -> Self` - Specialized [Signature] for ArrayElement and similar functions.
- `fn array(volatility: Volatility) -> Self` - Specialized [Signature] for ArrayEmpty and similar functions.
- `fn with_parameter_names<impl Into<String>>(self: Self, names: Vec<impl Trait>) -> Result<Self>` - Add parameter names to this signature, enabling named argument notation.

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Signature`
- **PartialEq**
  - `fn eq(self: &Self, other: &Signature) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Signature) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr_common::signature::TIMEZONE_WILDCARD

*Constant*: `&str`

Constant that is used as a placeholder for any valid timezone.
This is used where a function can accept a timestamp type with any
valid timezone, it exists to avoid the need to enumerate all possible
timezones. See [`TypeSignature`] for more details.

Type coercion always ensures that functions will be executed using
timestamp arrays that have a valid time zone. Functions must never
return results with this timezone.



## datafusion_expr_common::signature::TypeSignature

*Enum*

The types of arguments for which a function has implementations.

[`TypeSignature`] **DOES NOT** define the types that a user query could call the
function with. DataFusion will automatically coerce (cast) argument types to
one of the supported function signatures, if possible.

# Overview
Functions typically provide implementations for a small number of different
argument [`DataType`]s, rather than all possible combinations. If a user
calls a function with arguments that do not match any of the declared types,
DataFusion will attempt to automatically coerce (add casts to) function
arguments so they match the [`TypeSignature`]. See the [`type_coercion`] module
for more details

# Example: Numeric Functions
For example, a function like `cos` may only provide an implementation for
[`DataType::Float64`]. When users call `cos` with a different argument type,
such as `cos(int_column)`, and type coercion automatically adds a cast such
as `cos(CAST int_column AS DOUBLE)` during planning.

[`type_coercion`]: crate::type_coercion

## Example: Strings

There are several different string types in Arrow, such as
[`DataType::Utf8`], [`DataType::LargeUtf8`], and [`DataType::Utf8View`].

Some functions may have specialized implementations for these types, while others
may be able to handle only one of them. For example, a function that
only works with [`DataType::Utf8View`] would have the following signature:

```
# use arrow::datatypes::DataType;
# use datafusion_expr_common::signature::{TypeSignature};
// Declares the function must be invoked with a single argument of type `Utf8View`.
// if a user calls the function with `Utf8` or `LargeUtf8`, DataFusion will
// automatically add a cast to `Utf8View` during planning.
let type_signature = TypeSignature::Exact(vec![DataType::Utf8View]);
```

# Example: Timestamps

Types to match are represented using Arrow's [`DataType`].  [`DataType::Timestamp`] has an optional variable
timezone specification. To specify a function can handle a timestamp with *ANY* timezone, use
the [`TIMEZONE_WILDCARD`]. For example:

```
# use arrow::datatypes::{DataType, TimeUnit};
# use datafusion_expr_common::signature::{TIMEZONE_WILDCARD, TypeSignature};
let type_signature = TypeSignature::Exact(vec![
    // A nanosecond precision timestamp with ANY timezone
    // matches  Timestamp(Nanosecond, Some("+0:00"))
    // matches  Timestamp(Nanosecond, Some("+5:00"))
    // does not match  Timestamp(Nanosecond, None)
    DataType::Timestamp(TimeUnit::Nanosecond, Some(TIMEZONE_WILDCARD.into())),
]);
```

**Variants:**
- `Variadic(Vec<arrow::datatypes::DataType>)` - One or more arguments of a common type out of a list of valid types.
- `UserDefined` - The acceptable signature and coercions rules are special for this
- `VariadicAny` - One or more arguments with arbitrary types
- `Uniform(usize, Vec<arrow::datatypes::DataType>)` - One or more arguments of an arbitrary but equal type out of a list of valid types.
- `Exact(Vec<arrow::datatypes::DataType>)` - One or more arguments with exactly the specified types in order.
- `Coercible(Vec<Coercion>)` - One or more arguments belonging to the [`TypeSignatureClass`], in order.
- `Comparable(usize)` - One or more arguments coercible to a single, comparable type.
- `Any(usize)` - One or more arguments of arbitrary types.
- `OneOf(Vec<TypeSignature>)` - Matches exactly one of a list of [`TypeSignature`]s.
- `ArraySignature(ArrayFunctionSignature)` - A function that has an [`ArrayFunctionSignature`]
- `Numeric(usize)` - One or more arguments of numeric types, coerced to a common numeric type.
- `String(usize)` - One or arguments of all the same string types.
- `Nullary` - No arguments

**Methods:**

- `fn to_string_repr(self: &Self) -> Vec<String>`
- `fn to_string_repr_with_names(self: &Self, parameter_names: Option<&[String]>) -> Vec<String>` - Return string representation of the function signature with parameter names.
- `fn join_types<T>(types: &[T], delimiter: &str) -> String` - Helper function to join types with specified delimiter.
- `fn supports_zero_argument(self: &Self) -> bool` - Check whether 0 input argument is valid for given `TypeSignature`
- `fn used_to_support_zero_arguments(self: &Self) -> bool` - Returns true if the signature currently supports or used to supported 0
- `fn get_possible_types(self: &Self) -> Vec<Vec<DataType>>`
- `fn get_example_types(self: &Self) -> Vec<Vec<DataType>>` - Return example acceptable types for this `TypeSignature`'
- `fn is_one_of(self: &Self) -> bool`
- `fn arity(self: &Self) -> Arity` - Returns the arity (expected number of arguments) for this type signature.

**Traits:** Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &TypeSignature) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Clone**
  - `fn clone(self: &Self) -> TypeSignature`
- **PartialEq**
  - `fn eq(self: &Self, other: &TypeSignature) -> bool`



## datafusion_expr_common::signature::TypeSignatureClass

*Enum*

Represents the class of types that can be used in a function signature.

This is used to specify what types are valid for function arguments in a more flexible way than
just listing specific DataTypes. For example, TypeSignatureClass::Timestamp matches any timestamp
type regardless of timezone or precision.

Used primarily with [`TypeSignature::Coercible`] to define function signatures that can accept
arguments that can be coerced to a particular class of types.

**Variants:**
- `Any` - Allows an arbitrary type argument without coercing the argument.
- `Timestamp` - Timestamps, allowing arbitrary (or no) timezones
- `Time` - All time types
- `Interval` - All interval types
- `Duration` - All duration types
- `Native(datafusion_common::types::LogicalTypeRef)` - A specific native type
- `Integer` - Signed and unsigned integers
- `Float` - All float types
- `Decimal` - All decimal types, allowing arbitrary precision & scale
- `Numeric` - Integers, floats and decimals
- `Binary` - Encompasses both the native Binary/LargeBinary types as well as arbitrarily sized FixedSizeBinary types

**Methods:**

- `fn matches_native_type(self: &Self, logical_type: &NativeType) -> bool` - Does the specified `NativeType` match this type signature class?
- `fn default_casted_type(self: &Self, native_type: &NativeType, origin_type: &DataType) -> Result<DataType>` - What type would `origin_type` be casted to when casting to the specified native type?

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> TypeSignatureClass`
- **Display**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &TypeSignatureClass) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialEq**
  - `fn eq(self: &Self, other: &TypeSignatureClass) -> bool`



## datafusion_expr_common::signature::Volatility

*Enum*

How a function's output changes with respect to a fixed input

The volatility of a function determines eligibility for certain
optimizations. You should always define your function to have the strictest
possible volatility to maximize performance and avoid unexpected
results.

**Variants:**
- `Immutable` - Always returns the same output when given the same input.
- `Stable` - May return different values given the same input across different
- `Volatile` - May change the return value from evaluation to evaluation.

**Traits:** Eq, Copy

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Volatility) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Ord**
  - `fn cmp(self: &Self, other: &Volatility) -> $crate::cmp::Ordering`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Volatility) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> Volatility`



