**datafusion_common > tree_node**

# Module: tree_node

## Contents

**Structs**

- [`Transformed`](#transformed) - Result of tree walk / transformation APIs

**Enums**

- [`TreeNodeRecursion`](#treenoderecursion) - Controls how [`TreeNode`] recursions should proceed.

**Traits**

- [`ConcreteTreeNode`](#concretetreenode) - Instead of implementing [`TreeNode`], it's recommended to implement a [`ConcreteTreeNode`] for
- [`DynTreeNode`](#dyntreenode) - Helper trait for implementing [`TreeNode`] that have children stored as
- [`TransformedResult`](#transformedresult) - Transformation helper to access [`Transformed`] fields in a [`Result`] easily.
- [`TreeNode`](#treenode) - API for inspecting and rewriting tree data structures.
- [`TreeNodeContainer`](#treenodecontainer) - [`TreeNodeContainer`] contains elements that a function can be applied on or mapped.
- [`TreeNodeIterator`](#treenodeiterator) - Transformation helper to process a sequence of iterable tree nodes that are siblings.
- [`TreeNodeRefContainer`](#treenoderefcontainer) - [`TreeNodeRefContainer`] contains references to elements that a function can be
- [`TreeNodeRewriter`](#treenoderewriter) - A [Visitor](https://en.wikipedia.org/wiki/Visitor_pattern) for recursively
- [`TreeNodeVisitor`](#treenodevisitor) - A [Visitor](https://en.wikipedia.org/wiki/Visitor_pattern) for recursively

---

## datafusion_common::tree_node::ConcreteTreeNode

*Trait*

Instead of implementing [`TreeNode`], it's recommended to implement a [`ConcreteTreeNode`] for
trees that contain nodes with payloads. This approach ensures safe execution of algorithms
involving payloads, by enforcing rules for detaching and reattaching child nodes.

**Methods:**

- `children`: Provides read-only access to child nodes.
- `take_children`: Detaches the node from its children, returning the node itself and its detached children.
- `with_new_children`: Reattaches updated child nodes to the node, returning the updated node.



## datafusion_common::tree_node::DynTreeNode

*Trait*

Helper trait for implementing [`TreeNode`] that have children stored as
`Arc`s. If some trait object, such as `dyn T`, implements this trait,
its related `Arc<dyn T>` will automatically implement [`TreeNode`].

**Methods:**

- `arc_children`: Returns all children of the specified `TreeNode`.
- `with_new_arc_children`: Constructs a new node with the specified children.



## datafusion_common::tree_node::Transformed

*Struct*

Result of tree walk / transformation APIs

`Transformed` is a wrapper around the tree node data (e.g. `Expr` or
`LogicalPlan`). It is used to indicate whether the node was transformed
and how the recursion should proceed.

[`TreeNode`] API users control the transformation by returning:
- The resulting (possibly transformed) node,
- `transformed`: flag indicating whether any change was made to the node
- `tnr`: [`TreeNodeRecursion`] specifying how to proceed with the recursion.

At the end of the transformation, the return value will contain:
- The final (possibly transformed) tree,
- `transformed`: flag indicating whether any change was made to the node
- `tnr`: [`TreeNodeRecursion`] specifying how the recursion ended.

See also
* [`Transformed::update_data`] to modify the node without changing the `transformed` flag
* [`Transformed::map_data`] for fallable operation that return the same type
* [`Transformed::transform_data`] to chain fallable transformations
* [`TransformedResult`] for working with `Result<Transformed<U>>`

# Examples

Use [`Transformed::yes`] and [`Transformed::no`] to signal that a node was
rewritten and the recursion should continue:

```
# use datafusion_common::tree_node::Transformed;
# // note use i64 instead of Expr as Expr is not in datafusion-common
# fn orig_expr() -> i64 { 1 }
# fn make_new_expr(i: i64) -> i64 { 2 }
let expr = orig_expr();

// Create a new `Transformed` object signaling the node was not rewritten
let ret = Transformed::no(expr.clone());
assert!(!ret.transformed);

// Create a new `Transformed` object signaling the node was rewritten
let ret = Transformed::yes(expr);
assert!(ret.transformed)
```

Access the node within the `Transformed` object:
```
# use datafusion_common::tree_node::Transformed;
# // note use i64 instead of Expr as Expr is not in datafusion-common
# fn orig_expr() -> i64 { 1 }
# fn make_new_expr(i: i64) -> i64 { 2 }
let expr = orig_expr();

// `Transformed` object signaling the node was not rewritten
let ret = Transformed::no(expr.clone());
// Access the inner object using .data
assert_eq!(expr, ret.data);
```

Transform the node within the `Transformed` object.

```
# use datafusion_common::tree_node::Transformed;
# // note use i64 instead of Expr as Expr is not in datafusion-common
# fn orig_expr() -> i64 { 1 }
# fn make_new_expr(i: i64) -> i64 { 2 }
let expr = orig_expr();
let ret = Transformed::no(expr.clone())
    .transform_data(|expr| {
        // closure returns a result and potentially transforms the node
        // in this example, it does transform the node
        let new_expr = make_new_expr(expr);
        Ok(Transformed::yes(new_expr))
    })
    .unwrap();
// transformed flag is the union of the original ans closure's  transformed flag
assert!(ret.transformed);
```
# Example APIs that use `TreeNode`
- [`TreeNode`],
- [`TreeNode::rewrite`],
- [`TreeNode::transform_down`],
- [`TreeNode::transform_up`],
- [`TreeNode::transform_down_up`]

**Generic Parameters:**
- T

**Fields:**
- `data: T`
- `transformed: bool`
- `tnr: TreeNodeRecursion`

**Methods:**

- `fn new(data: T, transformed: bool, tnr: TreeNodeRecursion) -> Self` - Create a new `Transformed` object with the given information.
- `fn new_transformed(data: T, transformed: bool) -> Self` - Create a `Transformed` with `transformed` and [`TreeNodeRecursion::Continue`].
- `fn yes(data: T) -> Self` - Wrapper for transformed data with [`TreeNodeRecursion::Continue`] statement.
- `fn complete(data: T) -> Self` - Wrapper for transformed data with [`TreeNodeRecursion::Stop`] statement.
- `fn no(data: T) -> Self` - Wrapper for unchanged data with [`TreeNodeRecursion::Continue`] statement.
- `fn update_data<U, F>(self: Self, f: F) -> Transformed<U>` - Applies an infallible `f` to the data of this [`Transformed`] object,
- `fn map_data<U, F>(self: Self, f: F) -> Result<Transformed<U>>` - Applies a fallible `f` (returns `Result`) to the data of this
- `fn transform_data<U, F>(self: Self, f: F) -> Result<Transformed<U>>` - Applies a fallible transforming `f` to the data of this [`Transformed`]
- `fn transform_children<F>(self: Self, f: F) -> Result<Transformed<T>>` - Maps the [`Transformed`] object to the result of the given `f` depending on the
- `fn transform_sibling<F>(self: Self, f: F) -> Result<Transformed<T>>` - Maps the [`Transformed`] object to the result of the given `f` depending on the
- `fn transform_parent<F>(self: Self, f: F) -> Result<Transformed<T>>` - Maps the [`Transformed`] object to the result of the given `f` depending on the

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &Transformed<T>) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_common::tree_node::TransformedResult

*Trait*

Transformation helper to access [`Transformed`] fields in a [`Result`] easily.

# Example
Access the internal data of a `Result<Transformed<T>>`
as a `Result<T>` using the `data` method:
```
# use datafusion_common::Result;
# use datafusion_common::tree_node::{Transformed, TransformedResult};
# // note use i64 instead of Expr as Expr is not in datafusion-common
# fn update_expr() -> i64 { 1 }
# fn main() -> Result<()> {
let transformed: Result<Transformed<_>> = Ok(Transformed::yes(update_expr()));
// access the internal data of the transformed result, or return the error
let transformed_expr = transformed.data()?;
# Ok(())
# }
```

**Methods:**

- `data`
- `transformed`
- `tnr`



## datafusion_common::tree_node::TreeNode

*Trait*

API for inspecting and rewriting tree data structures.

The `TreeNode` API is used to express algorithms separately from traversing
the structure of `TreeNode`s, avoiding substantial code duplication.

This trait is implemented for plans ([`ExecutionPlan`], [`LogicalPlan`]) and
expression trees ([`PhysicalExpr`], [`Expr`]) as well as Plan+Payload
combinations [`PlanContext`] and [`ExprContext`].

# Overview
There are three categories of TreeNode APIs:

1. "Inspecting" APIs to traverse a tree of `&TreeNodes`:
   [`apply`], [`visit`], [`exists`].

2. "Transforming" APIs that traverse and consume a tree of `TreeNode`s
   producing possibly changed `TreeNode`s: [`transform`], [`transform_up`],
   [`transform_down`], [`transform_down_up`], and [`rewrite`].

3. Internal APIs used to implement the `TreeNode` API: [`apply_children`],
   and [`map_children`].

| Traversal Order | Inspecting | Transforming |
| --- | --- | --- |
| top-down | [`apply`], [`exists`] | [`transform_down`]|
| bottom-up | | [`transform`] , [`transform_up`]|
| combined with separate `f_down` and `f_up` closures | | [`transform_down_up`] |
| combined with `f_down()` and `f_up()` in an object | [`visit`]  | [`rewrite`] |

**Note**:while there is currently no in-place mutation API that uses `&mut
TreeNode`, the transforming APIs are efficient and optimized to avoid
cloning.

[`apply`]: Self::apply
[`visit`]: Self::visit
[`exists`]: Self::exists
[`transform`]: Self::transform
[`transform_up`]: Self::transform_up
[`transform_down`]: Self::transform_down
[`transform_down_up`]: Self::transform_down_up
[`rewrite`]: Self::rewrite
[`apply_children`]: Self::apply_children
[`map_children`]: Self::map_children

# Terminology
The following terms are used in this trait

* `f_down`: Invoked before any children of the current node are visited.
* `f_up`: Invoked after all children of the current node are visited.
* `f`: closure that is applied to the current node.
* `map_*`: applies a transformation to rewrite owned nodes
* `apply_*`:  invokes a function on borrowed nodes
* `transform_`: applies a transformation to rewrite owned nodes

<!-- Since these are in the datafusion-common crate, can't use intra doc links) -->
[`ExecutionPlan`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html
[`PhysicalExpr`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.PhysicalExpr.html
[`LogicalPlan`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/logical_plan/enum.LogicalPlan.html
[`Expr`]: https://docs.rs/datafusion-expr/latest/datafusion_expr/expr/enum.Expr.html
[`PlanContext`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/tree_node/struct.PlanContext.html
[`ExprContext`]: https://docs.rs/datafusion/latest/datafusion/physical_expr/tree_node/struct.ExprContext.html

**Methods:**

- `visit`: Visit the tree node with a [`TreeNodeVisitor`], performing a
- `rewrite`: Rewrite the tree node with a [`TreeNodeRewriter`], performing a
- `apply`: Applies `f` to the node then each of its children, recursively (a
- `transform`: Recursively rewrite the node's children and then the node using `f`
- `transform_down`: Recursively rewrite the tree using `f` in a top-down (pre-order)
- `transform_up`: Recursively rewrite the node using `f` in a bottom-up (post-order)
- `transform_down_up`: Transforms the node using `f_down` while traversing the tree top-down
- `exists`: Returns true if `f` returns true for any node in the tree.
- `apply_children`: Low-level API used to implement other APIs.
- `map_children`: Low-level API used to implement other APIs.



## datafusion_common::tree_node::TreeNodeContainer

*Trait*

[`TreeNodeContainer`] contains elements that a function can be applied on or mapped.
The elements of the container are siblings so the continuation rules are similar to
[`TreeNodeRecursion::visit_sibling`] / [`Transformed::transform_sibling`].

**Methods:**

- `apply_elements`: Applies `f` to all elements of the container.
- `map_elements`: Maps all elements of the container with `f`.



## datafusion_common::tree_node::TreeNodeIterator

*Trait*

Transformation helper to process a sequence of iterable tree nodes that are siblings.

**Methods:**

- `apply_until_stop`: Apples `f` to each item in this iterator
- `map_until_stop_and_collect`: Apples `f` to each item in this iterator



## datafusion_common::tree_node::TreeNodeRecursion

*Enum*

Controls how [`TreeNode`] recursions should proceed.

**Variants:**
- `Continue` - Continue recursion with the next node.
- `Jump` - In top-down traversals, skip recursing into children but continue with
- `Stop` - Stop recursion.

**Methods:**

- `fn visit_children<F>(self: Self, f: F) -> Result<TreeNodeRecursion>` - Continues visiting nodes with `f` depending on the current [`TreeNodeRecursion`]
- `fn visit_sibling<F>(self: Self, f: F) -> Result<TreeNodeRecursion>` - Continues visiting nodes with `f` depending on the current [`TreeNodeRecursion`]
- `fn visit_parent<F>(self: Self, f: F) -> Result<TreeNodeRecursion>` - Continues visiting nodes with `f` depending on the current [`TreeNodeRecursion`]

**Traits:** Copy

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> TreeNodeRecursion`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &TreeNodeRecursion) -> bool`



## datafusion_common::tree_node::TreeNodeRefContainer

*Trait*

[`TreeNodeRefContainer`] contains references to elements that a function can be
applied on. The elements of the container are siblings so the continuation rules are
similar to [`TreeNodeRecursion::visit_sibling`].

This container is similar to [`TreeNodeContainer`], but the lifetime of the reference
elements (`T`) are not derived from the container's lifetime.
A typical usage of this container is in `Expr::apply_children` when we need to
construct a temporary container to be able to call `apply_ref_elements` on a
collection of tree node references. But in that case the container's temporary
lifetime is different to the lifetime of tree nodes that we put into it.
Please find an example use case in `Expr::apply_children` with the `Expr::Case` case.

Most of the cases we don't need to create a temporary container with
`TreeNodeRefContainer`, but we can just call `TreeNodeContainer::apply_elements`.
Please find an example use case in `Expr::apply_children` with the `Expr::GroupingSet`
case.

**Methods:**

- `apply_ref_elements`: Applies `f` to all elements of the container.



## datafusion_common::tree_node::TreeNodeRewriter

*Trait*

A [Visitor](https://en.wikipedia.org/wiki/Visitor_pattern) for recursively
rewriting [`TreeNode`]s via [`TreeNode::rewrite`].

For example you can implement this trait on a struct to rewrite `Expr` or
`LogicalPlan` that needs to track state during the rewrite.

See [`TreeNode`] for more details on available APIs

When passed to [`TreeNode::rewrite`], [`TreeNodeRewriter::f_down`] and
[`TreeNodeRewriter::f_up`] are invoked recursively on the tree.
See [`TreeNodeRecursion`] for more details on controlling the traversal.

# Return Value
The returns value of `f_up` and `f_down` specifies how the tree walk should
proceed. See [`TreeNodeRecursion`] for details. If an [`Err`] is returned,
the recursion stops immediately.

Note: If using the default implementations of [`TreeNodeRewriter::f_up`] or
[`TreeNodeRewriter::f_down`] that do nothing, consider using
[`TreeNode::transform_up`] or [`TreeNode::transform_down`] instead.

# See Also:
* [`TreeNode::visit`] to inspect borrowed `TreeNode`s

**Methods:**

- `Node`: The node type which is rewritable.
- `f_down`: Invoked while traversing down the tree before any children are rewritten.
- `f_up`: Invoked while traversing up the tree after all children have been rewritten.



## datafusion_common::tree_node::TreeNodeVisitor

*Trait*

A [Visitor](https://en.wikipedia.org/wiki/Visitor_pattern) for recursively
inspecting [`TreeNode`]s via [`TreeNode::visit`].

See [`TreeNode`] for more details on available APIs

When passed to [`TreeNode::visit`], [`TreeNodeVisitor::f_down`] and
[`TreeNodeVisitor::f_up`] are invoked recursively on the tree.
See [`TreeNodeRecursion`] for more details on controlling the traversal.

# Return Value
The returns value of `f_up` and `f_down` specifies how the tree walk should
proceed. See [`TreeNodeRecursion`] for details. If an [`Err`] is returned,
the recursion stops immediately.

Note: If using the default implementations of [`TreeNodeVisitor::f_up`] or
[`TreeNodeVisitor::f_down`] that do nothing, consider using
[`TreeNode::apply`] instead.

# See Also:
* [`TreeNode::rewrite`] to rewrite owned `TreeNode`s

**Methods:**

- `Node`: The node type which is visitable.
- `f_down`: Invoked while traversing down the tree, before any children are visited.
- `f_up`: Invoked while traversing up the tree after children are visited. Default



