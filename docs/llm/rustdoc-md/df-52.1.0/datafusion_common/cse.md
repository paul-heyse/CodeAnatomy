**datafusion_common > cse**

# Module: cse

## Contents

**Structs**

- [`CSE`](#cse) - The main entry point of Common Subexpression Elimination.

**Enums**

- [`FoundCommonNodes`](#foundcommonnodes) - The result of potentially rewriting a list of [`TreeNode`]s to eliminate common

**Traits**

- [`CSEController`](#csecontroller) - The [`TreeNode`] specific definition of elimination.
- [`HashNode`](#hashnode) - Hashes the direct content of an [`TreeNode`] without recursing into its children.
- [`NormalizeEq`](#normalizeeq) - The `NormalizeEq` trait extends `Eq` and `Normalizeable` to provide a method for comparing
- [`Normalizeable`](#normalizeable) - The `Normalizeable` trait defines a method to determine whether a node can be normalized.

---

## datafusion_common::cse::CSE

*Struct*

The main entry point of Common Subexpression Elimination.

[`CSE`] requires a [`CSEController`], that defines how common subtrees of a particular
[`TreeNode`] tree can be eliminated. The elimination process can be started with the
[`CSE::extract_common_nodes()`] method.

**Generic Parameters:**
- N
- C

**Methods:**

- `fn new(controller: C) -> Self`
- `fn extract_common_nodes(self: & mut Self, nodes_list: Vec<Vec<N>>) -> Result<FoundCommonNodes<N>>` - Extracts common [`TreeNode`]s and rewrites `nodes_list`.



## datafusion_common::cse::CSEController

*Trait*

The [`TreeNode`] specific definition of elimination.

**Methods:**

- `Node`: The type of the tree nodes.
- `conditional_children`: Splits the children to normal and conditionally evaluated ones or returns `None`
- `is_valid`
- `is_ignored`
- `generate_alias`
- `rewrite`
- `rewrite_f_down`
- `rewrite_f_up`



## datafusion_common::cse::FoundCommonNodes

*Enum*

The result of potentially rewriting a list of [`TreeNode`]s to eliminate common
subtrees.

**Generic Parameters:**
- N

**Variants:**
- `No{ original_nodes_list: Vec<Vec<N>> }` - No common [`TreeNode`]s were found
- `Yes{ common_nodes: Vec<(N, String)>, new_nodes_list: Vec<Vec<N>>, original_nodes_list: Vec<Vec<N>> }` - Common [`TreeNode`]s were found

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_common::cse::HashNode

*Trait*

Hashes the direct content of an [`TreeNode`] without recursing into its children.

This method is useful to incrementally compute hashes, such as in [`CSE`] which builds
a deep hash of a node and its descendants during the bottom-up phase of the first
traversal and so avoid computing the hash of the node and then the hash of its
descendants separately.

If a node doesn't have any children then the value returned by `hash_node()` is
similar to '.hash()`, but not necessarily returns the same value.

**Methods:**

- `hash_node`



## datafusion_common::cse::NormalizeEq

*Trait*

The `NormalizeEq` trait extends `Eq` and `Normalizeable` to provide a method for comparing
normalized nodes in optimizations like Common Subexpression Elimination (CSE).

The `normalize_eq` method ensures that two nodes that are semantically equivalent (after normalization)
are considered equal in CSE optimization, even if their original forms differ.

This trait allows for equality comparisons between nodes with equivalent semantics, regardless of their
internal representations.

**Methods:**

- `normalize_eq`



## datafusion_common::cse::Normalizeable

*Trait*

The `Normalizeable` trait defines a method to determine whether a node can be normalized.

Normalization is the process of converting a node into a canonical form that can be used
to compare nodes for equality. This is useful in optimizations like Common Subexpression Elimination (CSE),
where semantically equivalent nodes (e.g., `a + b` and `b + a`) should be treated as equal.

**Methods:**

- `can_normalize`



