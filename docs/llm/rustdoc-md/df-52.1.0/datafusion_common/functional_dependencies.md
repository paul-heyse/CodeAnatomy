**datafusion_common > functional_dependencies**

# Module: functional_dependencies

## Contents

**Structs**

- [`Constraints`](#constraints) - This object encapsulates a list of functional constraints:
- [`FunctionalDependence`](#functionaldependence) - This object defines a functional dependence in the schema. A functional
- [`FunctionalDependencies`](#functionaldependencies) - This object encapsulates all functional dependencies in a given relation.

**Enums**

- [`Constraint`](#constraint) - This object defines a constraint on a table.
- [`Dependency`](#dependency) - Describes functional dependency mode.

**Functions**

- [`aggregate_functional_dependencies`](#aggregate_functional_dependencies) - Calculates functional dependencies for aggregate output, when there is a GROUP BY expression.
- [`get_required_group_by_exprs_indices`](#get_required_group_by_exprs_indices) - Returns indices for the minimal subset of GROUP BY expressions that are
- [`get_target_functional_dependencies`](#get_target_functional_dependencies) - Returns target indices, for the determinant keys that are inside

---

## datafusion_common::functional_dependencies::Constraint

*Enum*

This object defines a constraint on a table.

**Variants:**
- `PrimaryKey(Vec<usize>)` - Columns with the given indices form a composite primary key (they are
- `Unique(Vec<usize>)` - Columns with the given indices form a composite unique key:

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Constraint`
- **PartialEq**
  - `fn eq(self: &Self, other: &Constraint) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Constraint) -> $crate::option::Option<$crate::cmp::Ordering>`



## datafusion_common::functional_dependencies::Constraints

*Struct*

This object encapsulates a list of functional constraints:

**Methods:**

- `fn new_unverified(constraints: Vec<Constraint>) -> Self` - Create a new [`Constraints`] object from the given `constraints`.
- `fn extend(self: & mut Self, other: Constraints)` - Extends the current constraints with the given `other` constraints.
- `fn project(self: &Self, proj_indices: &[usize]) -> Option<Self>` - Projects constraints using the given projection indices. Returns `None`

**Traits:** Eq

**Trait Implementations:**

- **Default**
  - `fn default() -> Constraints`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Constraints) -> $crate::option::Option<$crate::cmp::Ordering>`
- **IntoIterator**
  - `fn into_iter(self: Self) -> <Self as >::IntoIter`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> std::fmt::Result`
- **Deref**
  - `fn deref(self: &Self) -> &<Self as >::Target`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **PartialEq**
  - `fn eq(self: &Self, other: &Constraints) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> Constraints`



## datafusion_common::functional_dependencies::Dependency

*Enum*

Describes functional dependency mode.

**Variants:**
- `Single`
- `Multi`

**Traits:** Copy, Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Dependency`
- **PartialEq**
  - `fn eq(self: &Self, other: &Dependency) -> bool`



## datafusion_common::functional_dependencies::FunctionalDependence

*Struct*

This object defines a functional dependence in the schema. A functional
dependence defines a relationship between determinant keys and dependent
columns. A determinant key is a column, or a set of columns, whose value
uniquely determines values of some other (dependent) columns. If two rows
have the same determinant key, dependent columns in these rows are
necessarily the same. If the determinant key is unique, the set of
dependent columns is equal to the entire schema and the determinant key can
serve as a primary key. Note that a primary key may "downgrade" into a
determinant key due to an operation such as a join, and this object is
used to track dependence relationships in such cases. For more information
on functional dependencies, see:
<https://www.scaler.com/topics/dbms/functional-dependency-in-dbms/>

**Fields:**
- `source_indices: Vec<usize>`
- `target_indices: Vec<usize>`
- `nullable: bool` - Flag indicating whether one of the `source_indices` can receive NULL values.
- `mode: Dependency`

**Methods:**

- `fn new(source_indices: Vec<usize>, target_indices: Vec<usize>, nullable: bool) -> Self`
- `fn with_mode(self: Self, mode: Dependency) -> Self`

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> FunctionalDependence`
- **PartialEq**
  - `fn eq(self: &Self, other: &FunctionalDependence) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_common::functional_dependencies::FunctionalDependencies

*Struct*

This object encapsulates all functional dependencies in a given relation.

**Methods:**

- `fn empty() -> Self` - Creates an empty `FunctionalDependencies` object.
- `fn new(dependencies: Vec<FunctionalDependence>) -> Self` - Creates a new `FunctionalDependencies` object from a vector of
- `fn new_from_constraints(constraints: Option<&Constraints>, n_field: usize) -> Self` - Creates a new `FunctionalDependencies` object from the given constraints.
- `fn with_dependency(self: Self, mode: Dependency) -> Self`
- `fn extend(self: & mut Self, other: FunctionalDependencies)` - Merges the given functional dependencies with these.
- `fn is_valid(self: &Self, n_field: usize) -> bool` - Sanity checks if functional dependencies are valid. For example, if
- `fn add_offset(self: & mut Self, offset: usize)` - Adds the `offset` value to `source_indices` and `target_indices` for
- `fn project_functional_dependencies(self: &Self, proj_indices: &[usize], n_out: usize) -> FunctionalDependencies` - Updates `source_indices` and `target_indices` of each functional
- `fn join(self: &Self, other: &FunctionalDependencies, join_type: &JoinType, left_cols_len: usize) -> FunctionalDependencies` - This function joins this set of functional dependencies with the `other`
- `fn extend_target_indices(self: & mut Self, n_out: usize)` - This function ensures that functional dependencies involving uniquely

**Traits:** Eq

**Trait Implementations:**

- **Deref**
  - `fn deref(self: &Self) -> &<Self as >::Target`
- **Clone**
  - `fn clone(self: &Self) -> FunctionalDependencies`
- **PartialEq**
  - `fn eq(self: &Self, other: &FunctionalDependencies) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_common::functional_dependencies::aggregate_functional_dependencies

*Function*

Calculates functional dependencies for aggregate output, when there is a GROUP BY expression.

```rust
fn aggregate_functional_dependencies(aggr_input_schema: &crate::DFSchema, group_by_expr_names: &[String], aggr_schema: &crate::DFSchema) -> FunctionalDependencies
```



## datafusion_common::functional_dependencies::get_required_group_by_exprs_indices

*Function*

Returns indices for the minimal subset of GROUP BY expressions that are
functionally equivalent to the original set of GROUP BY expressions.

```rust
fn get_required_group_by_exprs_indices(schema: &crate::DFSchema, group_by_expr_names: &[String]) -> Option<Vec<usize>>
```



## datafusion_common::functional_dependencies::get_target_functional_dependencies

*Function*

Returns target indices, for the determinant keys that are inside
group by expressions.

```rust
fn get_target_functional_dependencies(schema: &crate::DFSchema, group_by_expr_names: &[String]) -> Option<Vec<usize>>
```



