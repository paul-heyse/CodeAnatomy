# CPG Quality Artifacts

This guide describes the quality artifacts emitted by the CPG build stage.

## What is captured

Quality artifacts capture invalid identifiers discovered during CPG construction.
An identifier is considered invalid when it is:

- null or missing, or
- numerically equal to `0`.

These artifacts are emitted as standalone Arrow tables so they can be inspected
without mutating the main CPG outputs.

## Schema

The quality table has the following columns:

- `entity_kind` (string): `node`, `edge`, or `prop`.
- `entity_id` (string): the offending identifier (or null if missing).
- `issue` (string): issue code (for example `invalid_node_id`).
- `source_table` (string): origin of the check (for example `cpg_nodes_raw`).

## How to access

In the Hamilton pipeline, quality artifacts are exposed as:

- `cpg_nodes_quality`
- `cpg_edges_quality`
- `cpg_props_quality`

Use these tables to audit upstream extraction issues without losing the raw
signal from the original inputs.
