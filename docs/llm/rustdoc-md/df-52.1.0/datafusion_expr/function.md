**datafusion_expr > function**

# Module: function

## Contents

**Enums**

- [`Hint`](#hint)

**Type Aliases**

- [`AggregateFunctionSimplification`](#aggregatefunctionsimplification) - [crate::udaf::AggregateUDFImpl::simplify] simplifier closure
- [`PartitionEvaluatorFactory`](#partitionevaluatorfactory) - Factory that creates a PartitionEvaluator for the given window
- [`ReturnTypeFunction`](#returntypefunction) - Factory that returns the functions's return type given the input argument types
- [`ScalarFunctionImplementation`](#scalarfunctionimplementation) - Scalar function
- [`StateTypeFunction`](#statetypefunction) - Factory that returns the types used by an aggregator to serialize
- [`WindowFunctionSimplification`](#windowfunctionsimplification) - [crate::udwf::WindowUDFImpl::simplify] simplifier closure

---

## datafusion_expr::function::AggregateFunctionSimplification

*Type Alias*: `Box<dyn Fn>`

[crate::udaf::AggregateUDFImpl::simplify] simplifier closure
A closure with two arguments:
* 'aggregate_function': [crate::expr::AggregateFunction] for which simplified has been invoked
* 'info': [crate::simplify::SimplifyInfo]

Closure returns simplified [Expr] or an error.



## datafusion_expr::function::Hint

*Enum*

**Variants:**
- `Pad` - Indicates the argument needs to be padded if it is scalar
- `AcceptsSingular` - Indicates the argument can be converted to an array of length 1

**Traits:** Copy

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Hint`



## datafusion_expr::function::PartitionEvaluatorFactory

*Type Alias*: `std::sync::Arc<dyn Fn>`

Factory that creates a PartitionEvaluator for the given window
function



## datafusion_expr::function::ReturnTypeFunction

*Type Alias*: `std::sync::Arc<dyn Fn>`

Factory that returns the functions's return type given the input argument types



## datafusion_expr::function::ScalarFunctionImplementation

*Type Alias*: `std::sync::Arc<dyn Fn>`

Scalar function

The Fn param is the wrapped function but be aware that the function will
be passed with the slice / vec of columnar values (either scalar or array)
with the exception of zero param function, where a singular element vec
will be passed. In that case the single element is a null array to indicate
the batch's row count (so that the generative zero-argument function can know
the result array size).



## datafusion_expr::function::StateTypeFunction

*Type Alias*: `std::sync::Arc<dyn Fn>`

Factory that returns the types used by an aggregator to serialize
its state, given its return datatype.



## datafusion_expr::function::WindowFunctionSimplification

*Type Alias*: `Box<dyn Fn>`

[crate::udwf::WindowUDFImpl::simplify] simplifier closure
A closure with two arguments:
* 'window_function': [crate::expr::WindowFunction] for which simplified has been invoked
* 'info': [crate::simplify::SimplifyInfo]

Closure returns simplified [Expr] or an error.



