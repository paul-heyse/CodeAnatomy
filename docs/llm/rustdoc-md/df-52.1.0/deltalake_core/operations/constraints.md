**deltalake_core > operations > constraints**

# Module: operations::constraints

## Contents

**Structs**

- [`ConstraintBuilder`](#constraintbuilder) - Build a constraint to add to a table

---

## deltalake_core::operations::constraints::ConstraintBuilder

*Struct*

Build a constraint to add to a table

**Methods:**

- `fn with_constraint<S, E>(self: Self, name: S, expression: E) -> Self` - Specify the constraint to be added
- `fn with_constraints<S, E>(self: Self, constraints: HashMap<S, E>) -> Self` - Specify multiple constraints to be added
- `fn with_session_state(self: Self, session: Arc<dyn Session>) -> Self` - The Datafusion session state to use
- `fn with_commit_properties(self: Self, commit_properties: CommitProperties) -> Self` - Additional metadata to be added to commit info
- `fn with_custom_execute_handler(self: Self, handler: Arc<dyn CustomExecuteHandler>) -> Self` - Set a custom execute handler, for pre and post execution

**Trait Implementations:**

- **IntoFuture**
  - `fn into_future(self: Self) -> <Self as >::IntoFuture`



