**deltalake_core > kernel**

# Module: kernel

## Contents

**Modules**

- [`arrow`](#arrow) - Conversions between Delta and Arrow data types
- [`error`](#error) - Error types for Delta Lake operations.
- [`models`](#models) - Actions are the fundamental unit of work in Delta Lake. Each action performs a single atomic
- [`scalars`](#scalars) - Auxiliary methods for dealing with kernel scalars
- [`schema`](#schema)
- [`transaction`](#transaction) -  Add a commit entry to the Delta Table.

---

## Module: arrow

Conversions between Delta and Arrow data types



## Module: error

Error types for Delta Lake operations.



## Module: models

Actions are the fundamental unit of work in Delta Lake. Each action performs a single atomic
operation on the state of a Delta table. Actions are stored in the `_delta_log` directory of a
Delta table in JSON format. The log is a time series of actions that represent all the changes
made to a table.



## Module: scalars

Auxiliary methods for dealing with kernel scalars



## Module: schema



## Module: transaction

 Add a commit entry to the Delta Table.
 This module provides a unified interface for modifying commit behavior and attributes

 [`CommitProperties`] provides an unified client interface for all Delta operations.
 Internally this is used to initialize a [`CommitBuilder`].

 For advanced use cases [`CommitBuilder`] can be used which allows
 finer control over the commit process. The builder can be converted
 into a future the yield either a [`PreparedCommit`] or a [`FinalizedCommit`].

 A [`PreparedCommit`] represents a temporary commit marker written to storage.
 To convert to a [`FinalizedCommit`] an atomic rename is attempted. If the rename fails
 then conflict resolution is performed and the atomic rename is tried for the latest version.

<pre>
                                          Client Interface
        ┌─────────────────────────────┐
        │      Commit Properties      │
        │                             │
        │ Public commit interface for │
        │     all Delta Operations    │
        │                             │
        └─────────────┬───────────────┘
                      │
 ─────────────────────┼────────────────────────────────────
                      │
                      ▼                  Advanced Interface
        ┌─────────────────────────────┐
        │       Commit Builder        │
        │                             │
        │   Advanced entry point for  │
        │     creating a commit       │
        └─────────────┬───────────────┘
                      │
                      ▼
     ┌───────────────────────────────────┐
     │                                   │
     │ ┌───────────────────────────────┐ │
     │ │        Prepared Commit        │ │
     │ │                               │ │
     │ │     Represents a temporary    │ │
     │ │   commit marker written to    │ │
     │ │           storage             │ │
     │ └──────────────┬────────────────┘ │
     │                │                  │
     │                ▼                  │
     │ ┌───────────────────────────────┐ │
     │ │       Finalize Commit         │ │
     │ │                               │ │
     │ │   Convert the commit marker   │ │
     │ │   to a commit using atomic    │ │
     │ │         operations            │ │
     │ │                               │ │
     │ └───────────────────────────────┘ │
     │                                   │
     └────────────────┬──────────────────┘
                      │
                      ▼
       ┌───────────────────────────────┐
       │          Post Commit          │
       │                               │
       │ Commit that was materialized  │
       │ to storage with post commit   │
       │      hooks to be executed     │
       └──────────────┬────────────────┘
                      │
                      ▼
       ┌───────────────────────────────┐
       │        Finalized Commit       │
       │                               │
       │ Commit that was materialized  │
       │         to storage            │
       │                               │
       └───────────────────────────────┘
</pre>



