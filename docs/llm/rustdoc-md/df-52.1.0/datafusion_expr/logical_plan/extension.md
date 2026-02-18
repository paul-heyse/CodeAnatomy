**datafusion_expr > logical_plan > extension**

# Module: logical_plan::extension

## Contents

**Traits**

- [`UserDefinedLogicalNode`](#userdefinedlogicalnode) - This defines the interface for [`LogicalPlan`] nodes that can be
- [`UserDefinedLogicalNodeCore`](#userdefinedlogicalnodecore) - This trait facilitates implementation of the [`UserDefinedLogicalNode`].

---

## datafusion_expr::logical_plan::extension::UserDefinedLogicalNode

*Trait*

This defines the interface for [`LogicalPlan`] nodes that can be
used to extend DataFusion with custom relational operators.

The [`UserDefinedLogicalNodeCore`] trait is *the recommended way to implement*
this trait and avoids having implementing some required boiler plate code.

**Methods:**

- `as_any`: Return a reference to self as Any, to support dynamic downcasting
- `name`: Return the plan's name.
- `inputs`: Return the logical plan's inputs.
- `schema`: Return the output schema of this logical plan node.
- `check_invariants`: Perform check of invariants for the extension node.
- `expressions`: Returns all expressions in the current logical plan node. This should
- `prevent_predicate_push_down_columns`: A list of output columns (e.g. the names of columns in
- `fmt_for_explain`: Write a single line, human readable string to `f` for use in explain plan.
- `with_exprs_and_inputs`: Create a new `UserDefinedLogicalNode` with the specified children
- `necessary_children_exprs`: Returns the necessary input columns for this node required to compute
- `dyn_hash`: Update the hash `state` with this node requirements from
- `dyn_eq`: Compare `other`, respecting requirements from [Eq].
- `dyn_ord`: Compare `other`, respecting requirements from [PartialOrd].
- `supports_limit_pushdown`: Returns `true` if a limit can be safely pushed down through this



## datafusion_expr::logical_plan::extension::UserDefinedLogicalNodeCore

*Trait*

This trait facilitates implementation of the [`UserDefinedLogicalNode`].

See the example in
[user_defined_plan.rs](https://github.com/apache/datafusion/blob/main/datafusion/core/tests/user_defined/user_defined_plan.rs)
file for an example of how to use this extension API.

**Methods:**

- `name`: Return the plan's name.
- `inputs`: Return the logical plan's inputs.
- `schema`: Return the output schema of this logical plan node.
- `check_invariants`: Perform check of invariants for the extension node.
- `expressions`: Returns all expressions in the current logical plan node. This
- `prevent_predicate_push_down_columns`: A list of output columns (e.g. the names of columns in
- `fmt_for_explain`: Write a single line, human readable string to `f` for use in explain plan.
- `with_exprs_and_inputs`: Create a new `UserDefinedLogicalNode` with the specified children
- `necessary_children_exprs`: Returns the necessary input columns for this node required to compute
- `supports_limit_pushdown`: Returns `true` if a limit can be safely pushed down through this



