**deltalake_core > protocol > checkpoints**

# Module: protocol::checkpoints

## Contents

**Functions**

- [`cleanup_expired_logs_for`](#cleanup_expired_logs_for) - Delete expired Delta log files up to a safe checkpoint boundary.
- [`cleanup_metadata`](#cleanup_metadata) - Delete expires log files before given version from table. The table log retention is based on
- [`create_checkpoint`](#create_checkpoint) - Creates checkpoint at current table version
- [`create_checkpoint_from_table_url_and_cleanup`](#create_checkpoint_from_table_url_and_cleanup) - Loads table from given table [Url] at given `version` and creates checkpoint for it.

---

## deltalake_core::protocol::checkpoints::cleanup_expired_logs_for

*Function*

Delete expired Delta log files up to a safe checkpoint boundary.

This routine removes JSON commit files, in-progress JSON temp files, and
checkpoint files under `_delta_log/` that are both:
- older than the provided `cutoff_timestamp` (milliseconds since epoch), and
- strictly less than the provided `until_version`.

Safety guarantee:
To avoid deleting files that might still be required to reconstruct the
table state at or before the requested cutoff, the function first identifies
the most recent checkpoint whose version is `<= until_version` and whose file
modification time is `<= cutoff_timestamp`. Only files strictly older than
this checkpoint (both by version and timestamp) are considered for deletion.
If no such checkpoint exists (including when there is no `_last_checkpoint`),
the function performs no deletions and returns `Ok(0)`.

See also: https://github.com/delta-io/delta-rs/issues/3692 for background on
why cleanup must align to an existing checkpoint.

```rust
fn cleanup_expired_logs_for(keep_version: i64, log_store: &dyn LogStore, cutoff_timestamp: i64, operation_id: Option<uuid::Uuid>) -> crate::DeltaResult<usize>
```



## deltalake_core::protocol::checkpoints::cleanup_metadata

*Function*

Delete expires log files before given version from table. The table log retention is based on
the `logRetentionDuration` property of the Delta Table, 30 days by default.

```rust
fn cleanup_metadata(table: &crate::DeltaTable, operation_id: Option<uuid::Uuid>) -> crate::DeltaResult<usize>
```



## deltalake_core::protocol::checkpoints::create_checkpoint

*Function*

Creates checkpoint at current table version

```rust
fn create_checkpoint(table: &crate::DeltaTable, operation_id: Option<uuid::Uuid>) -> crate::DeltaResult<()>
```



## deltalake_core::protocol::checkpoints::create_checkpoint_from_table_url_and_cleanup

*Function*

Loads table from given table [Url] at given `version` and creates checkpoint for it.
The `cleanup` param decides whether to run metadata cleanup of obsolete logs.
If it's empty then the table's `enableExpiredLogCleanup` is used.

```rust
fn create_checkpoint_from_table_url_and_cleanup(table_url: url::Url, version: i64, cleanup: Option<bool>, operation_id: Option<uuid::Uuid>) -> crate::DeltaResult<()>
```



