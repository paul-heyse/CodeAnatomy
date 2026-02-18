**datafusion_expr > logical_plan > statement**

# Module: logical_plan::statement

## Contents

**Structs**

- [`Deallocate`](#deallocate) - Deallocate a prepared statement.
- [`Execute`](#execute) - Execute a prepared statement.
- [`Prepare`](#prepare) - Prepare a statement but do not execute it. Prepare statements can have 0 or more
- [`ResetVariable`](#resetvariable) - Reset a configuration variable to its default
- [`SetVariable`](#setvariable) - Set a Variable's value -- value in
- [`TransactionEnd`](#transactionend) - Indicator that any current transaction should be terminated
- [`TransactionStart`](#transactionstart) - Indicator that the following statements should be committed or rolled back atomically

**Enums**

- [`Statement`](#statement) - Various types of Statements.
- [`TransactionAccessMode`](#transactionaccessmode) - Indicates if this transaction is allowed to write
- [`TransactionConclusion`](#transactionconclusion) - Indicates if a transaction was committed or aborted
- [`TransactionIsolationLevel`](#transactionisolationlevel) - Indicates ANSI transaction isolation level

---

## datafusion_expr::logical_plan::statement::Deallocate

*Struct*

Deallocate a prepared statement.

**Fields:**
- `name: String` - The name of the prepared statement to deallocate

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Deallocate`
- **PartialEq**
  - `fn eq(self: &Self, other: &Deallocate) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Deallocate) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::logical_plan::statement::Execute

*Struct*

Execute a prepared statement.

**Fields:**
- `name: String` - The name of the prepared statement to execute
- `parameters: Vec<crate::Expr>` - The execute parameters

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> Execute`
- **PartialEq**
  - `fn eq(self: &Self, other: &Execute) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Execute) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::logical_plan::statement::Prepare

*Struct*

Prepare a statement but do not execute it. Prepare statements can have 0 or more
`Expr::Placeholder` expressions that are filled in during execution

**Fields:**
- `name: String` - The name of the statement
- `fields: Vec<arrow::datatypes::FieldRef>` - Data types of the parameters ([`Expr::Placeholder`])
- `input: std::sync::Arc<crate::LogicalPlan>` - The logical plan of the statements

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Prepare`
- **PartialEq**
  - `fn eq(self: &Self, other: &Prepare) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Prepare) -> $crate::option::Option<$crate::cmp::Ordering>`



## datafusion_expr::logical_plan::statement::ResetVariable

*Struct*

Reset a configuration variable to its default

**Fields:**
- `variable: String` - The variable name

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> ResetVariable`
- **PartialEq**
  - `fn eq(self: &Self, other: &ResetVariable) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &ResetVariable) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::logical_plan::statement::SetVariable

*Struct*

Set a Variable's value -- value in
[`ConfigOptions`](datafusion_common::config::ConfigOptions)

**Fields:**
- `variable: String` - The variable name
- `value: String` - The value to set

**Traits:** Eq

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> SetVariable`
- **PartialEq**
  - `fn eq(self: &Self, other: &SetVariable) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &SetVariable) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`



## datafusion_expr::logical_plan::statement::Statement

*Enum*

Various types of Statements.

# Transactions:

While DataFusion does not offer support transactions, it provides
[`LogicalPlan`] support to assist building database systems
using DataFusion

**Variants:**
- `TransactionStart(TransactionStart)`
- `TransactionEnd(TransactionEnd)`
- `SetVariable(SetVariable)` - Set a Variable
- `ResetVariable(ResetVariable)` - Reset a Variable
- `Prepare(Prepare)` - Prepare a statement and find any bind parameters
- `Execute(Execute)` - Execute a prepared statement. This is used to implement SQL 'EXECUTE'.
- `Deallocate(Deallocate)` - Deallocate a prepared statement.

**Methods:**

- `fn schema(self: &Self) -> &DFSchemaRef` - Get a reference to the logical plan's schema
- `fn name(self: &Self) -> &str` - Return a descriptive string describing the type of this
- `fn display(self: &Self) -> impl Trait` - Return a `format`able structure with the a human readable

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &Statement) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &Statement) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> Statement`



## datafusion_expr::logical_plan::statement::TransactionAccessMode

*Enum*

Indicates if this transaction is allowed to write

**Variants:**
- `ReadOnly`
- `ReadWrite`

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> TransactionAccessMode`
- **PartialEq**
  - `fn eq(self: &Self, other: &TransactionAccessMode) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &TransactionAccessMode) -> $crate::option::Option<$crate::cmp::Ordering>`



## datafusion_expr::logical_plan::statement::TransactionConclusion

*Enum*

Indicates if a transaction was committed or aborted

**Variants:**
- `Commit`
- `Rollback`

**Traits:** Eq

**Trait Implementations:**

- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> TransactionConclusion`
- **PartialEq**
  - `fn eq(self: &Self, other: &TransactionConclusion) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &TransactionConclusion) -> $crate::option::Option<$crate::cmp::Ordering>`



## datafusion_expr::logical_plan::statement::TransactionEnd

*Struct*

Indicator that any current transaction should be terminated

**Fields:**
- `conclusion: TransactionConclusion` - whether the transaction committed or aborted
- `chain: bool` - if specified a new transaction is immediately started with same characteristics

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> TransactionEnd`
- **PartialEq**
  - `fn eq(self: &Self, other: &TransactionEnd) -> bool`
- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &TransactionEnd) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::logical_plan::statement::TransactionIsolationLevel

*Enum*

Indicates ANSI transaction isolation level

**Variants:**
- `ReadUncommitted`
- `ReadCommitted`
- `RepeatableRead`
- `Serializable`
- `Snapshot`

**Traits:** Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &TransactionIsolationLevel) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> TransactionIsolationLevel`
- **PartialEq**
  - `fn eq(self: &Self, other: &TransactionIsolationLevel) -> bool`



## datafusion_expr::logical_plan::statement::TransactionStart

*Struct*

Indicator that the following statements should be committed or rolled back atomically

**Fields:**
- `access_mode: TransactionAccessMode` - indicates if transaction is allowed to write
- `isolation_level: TransactionIsolationLevel`

**Traits:** Eq

**Trait Implementations:**

- **PartialOrd**
  - `fn partial_cmp(self: &Self, other: &TransactionStart) -> $crate::option::Option<$crate::cmp::Ordering>`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Clone**
  - `fn clone(self: &Self) -> TransactionStart`
- **PartialEq**
  - `fn eq(self: &Self, other: &TransactionStart) -> bool`



