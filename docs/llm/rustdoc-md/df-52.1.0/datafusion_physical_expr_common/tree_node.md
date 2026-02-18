**datafusion_physical_expr_common > tree_node**

# Module: tree_node

## Contents

**Structs**

- [`ExprContext`](#exprcontext) - A node object encapsulating a [`PhysicalExpr`] node with a payload. Since there are

---

## datafusion_physical_expr_common::tree_node::ExprContext

*Struct*

A node object encapsulating a [`PhysicalExpr`] node with a payload. Since there are
two ways to access child plans—directly from the plan  and through child nodes—it's
recommended to perform mutable operations via [`Self::update_expr_from_children`].

**Generic Parameters:**
- T

**Fields:**
- `expr: std::sync::Arc<dyn PhysicalExpr>` - The physical expression associated with this context.
- `data: T` - Custom data payload of the node.
- `children: Vec<Self>` - Child contexts of this node.

**Methods:**

- `fn new_default(plan: Arc<dyn PhysicalExpr>) -> Self`
- `fn new_unknown(expr: Arc<dyn PhysicalExpr>) -> Self` - Constructs a new `ExprPropertiesNode` with unknown properties for a
- `fn new(expr: Arc<dyn PhysicalExpr>, data: T, children: Vec<Self>) -> Self`
- `fn update_expr_from_children(self: Self) -> Result<Self>`

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, f: & mut Formatter) -> fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **ConcreteTreeNode**
  - `fn children(self: &Self) -> &[Self]`
  - `fn take_children(self: Self) -> (Self, Vec<Self>)`
  - `fn with_new_children(self: Self, children: Vec<Self>) -> Result<Self>`



