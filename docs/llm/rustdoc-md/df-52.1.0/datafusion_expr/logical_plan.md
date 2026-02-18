**datafusion_expr > logical_plan**

# Module: logical_plan

## Contents

**Modules**

- [`builder`](#builder) - This module provides a builder for creating LogicalPlans
- [`display`](#display) - This module provides logic for displaying LogicalPlans in various styles
- [`dml`](#dml)
- [`tree_node`](#tree_node) -  [`TreeNode`] based visiting and rewriting for [`LogicalPlan`]s

---

## Module: builder

This module provides a builder for creating LogicalPlans



## Module: display

This module provides logic for displaying LogicalPlans in various styles



## Module: dml



## Module: tree_node

 [`TreeNode`] based visiting and rewriting for [`LogicalPlan`]s

Visiting (read only) APIs
* [`LogicalPlan::visit`]: recursively visit the node and all of its inputs
* [`LogicalPlan::visit_with_subqueries`]: recursively visit the node and all of its inputs, including subqueries
* [`LogicalPlan::apply_children`]: recursively visit all inputs of this node
* [`LogicalPlan::apply_expressions`]: (non recursively) visit all expressions of this node
* [`LogicalPlan::apply_subqueries`]: (non recursively) visit all subqueries of this node
* [`LogicalPlan::apply_with_subqueries`]: recursively visit all inputs and embedded subqueries.

Rewriting (update) APIs:
* [`LogicalPlan::exists`]: search for an expression in a plan
* [`LogicalPlan::rewrite`]: recursively rewrite the node and all of its inputs
* [`LogicalPlan::map_children`]: recursively rewrite all inputs of this node
* [`LogicalPlan::map_expressions`]: (non recursively) visit all expressions of this node
* [`LogicalPlan::map_subqueries`]: (non recursively) rewrite all subqueries of this node
* [`LogicalPlan::rewrite_with_subqueries`]: recursively rewrite the node and all of its inputs, including subqueries

(Re)creation APIs (these require substantial cloning and thus are slow):
* [`LogicalPlan::with_new_exprs`]: Create a new plan with different expressions
* [`LogicalPlan::expressions`]: Return a copy of the plan's expressions



