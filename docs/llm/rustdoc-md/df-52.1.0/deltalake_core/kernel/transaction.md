**deltalake_core > kernel > transaction**

# Module: kernel::transaction

## Contents

**Structs**

- [`CommitBuilder`](#commitbuilder) - Prepare data to be committed to the Delta log and control how the commit is performed
- [`CommitData`](#commitdata) - Data that was actually written to the log store.
- [`CommitMetrics`](#commitmetrics)
- [`CommitProperties`](#commitproperties) - End user facing interface to be used by operations on the table.
- [`FinalizedCommit`](#finalizedcommit) - A commit that successfully completed
- [`Metrics`](#metrics)
- [`PostCommit`](#postcommit) - Represents items for the post commit hook
- [`PostCommitHookProperties`](#postcommithookproperties) - Properties for post commit hook.
- [`PostCommitMetrics`](#postcommitmetrics)
- [`PreCommit`](#precommit) - Represents a commit that has not yet started but all details are finalized
- [`PreparedCommit`](#preparedcommit) - Represents a inflight commit

**Enums**

- [`CommitBuilderError`](#commitbuildererror) - Error raised while commititng transaction
- [`TransactionError`](#transactionerror) - Error raised while commititng transaction

**Traits**

- [`TableReference`](#tablereference) - Reference to some structure that contains mandatory attributes for performing a commit.

---

## deltalake_core::kernel::transaction::CommitBuilder

*Struct*

Prepare data to be committed to the Delta log and control how the commit is performed

**Methods:**

- `fn with_actions(self: Self, actions: Vec<Action>) -> Self` - Actions to be included in the commit
- `fn with_app_metadata(self: Self, app_metadata: HashMap<String, Value>) -> Self` - Metadata for the operation performed like metrics, user, and notebook
- `fn with_max_retries(self: Self, max_retries: usize) -> Self` - Maximum number of times to retry the transaction before failing to commit
- `fn with_post_commit_hook(self: Self, post_commit_hook: PostCommitHookProperties) -> Self` - Specify all the post commit hook properties
- `fn with_operation_id(self: Self, operation_id: Uuid) -> Self` - Propagate operation id to log store
- `fn with_post_commit_hook_handler(self: Self, handler: Option<Arc<dyn CustomExecuteHandler>>) -> Self` - Set a custom execute handler, for pre and post execution
- `fn build(self: Self, table_data: Option<&'a dyn TableReference>, log_store: LogStoreRef, operation: DeltaOperation) -> PreCommit<'a>` - Prepare a Commit operation using the configured builder

**Trait Implementations:**

- **Default**
  - `fn default() -> Self`
- **From**
  - `fn from(value: CommitProperties) -> Self`



## deltalake_core::kernel::transaction::CommitBuilderError

*Enum*

Error raised while commititng transaction

**Traits:** Error

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, __formatter: & mut ::core::fmt::Formatter) -> ::core::fmt::Result`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## deltalake_core::kernel::transaction::CommitData

*Struct*

Data that was actually written to the log store.

**Fields:**
- `actions: Vec<crate::kernel::Action>` - The actions
- `operation: crate::protocol::DeltaOperation` - The Operation
- `app_metadata: std::collections::HashMap<String, serde_json::Value>` - The Metadata
- `app_transactions: Vec<crate::kernel::Transaction>` - Application specific transaction

**Methods:**

- `fn new(actions: Vec<Action>, operation: DeltaOperation, app_metadata: HashMap<String, Value>, app_transactions: Vec<Transaction>) -> Self` - Create new data to be committed
- `fn get_bytes(self: &Self) -> Result<bytes::Bytes, TransactionError>` - Obtain the byte representation of the commit.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## deltalake_core::kernel::transaction::CommitMetrics

*Struct*

**Fields:**
- `num_retries: u64` - Number of retries before a successful commit

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &CommitMetrics) -> bool`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Clone**
  - `fn clone(self: &Self) -> CommitMetrics`
- **Default**
  - `fn default() -> CommitMetrics`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`



## deltalake_core::kernel::transaction::CommitProperties

*Struct*

End user facing interface to be used by operations on the table.
Enable controlling commit behaviour and modifying metadata that is written during a commit.

**Methods:**

- `fn with_metadata<impl IntoIterator<Item = (String, serde_json::Value)>>(self: Self, metadata: impl Trait) -> Self` - Specify metadata the be committed
- `fn with_max_retries(self: Self, max_retries: usize) -> Self` - Specify maximum number of times to retry the transaction before failing to commit
- `fn with_create_checkpoint(self: Self, create_checkpoint: bool) -> Self` - Specify if it should create a checkpoint when the commit interval condition is met
- `fn with_application_transaction(self: Self, txn: Transaction) -> Self` - Add an additional application transaction to the commit
- `fn with_application_transactions(self: Self, txn: Vec<Transaction>) -> Self` - Override application transactions for the commit
- `fn with_cleanup_expired_logs(self: Self, cleanup_expired_logs: Option<bool>) -> Self` - Specify if it should clean up the logs when the logRetentionDuration interval is met

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> Self`
- **Clone**
  - `fn clone(self: &Self) -> CommitProperties`



## deltalake_core::kernel::transaction::FinalizedCommit

*Struct*

A commit that successfully completed

**Fields:**
- `snapshot: crate::table::state::DeltaTableState` - The new table state after a commit
- `version: i64` - Version of the finalized commit
- `metrics: Metrics` - Metrics associated with the commit operation

**Methods:**

- `fn snapshot(self: &Self) -> DeltaTableState` - The new table state after a commit
- `fn version(self: &Self) -> i64` - Version of the finalized commit

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`



## deltalake_core::kernel::transaction::Metrics

*Struct*

**Fields:**
- `num_retries: u64` - Number of retries before a successful commit
- `new_checkpoint_created: bool` - Whether a new checkpoint was created as part of this commit
- `num_log_files_cleaned_up: u64` - Number of log files cleaned up

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &Metrics) -> bool`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Clone**
  - `fn clone(self: &Self) -> Metrics`
- **Default**
  - `fn default() -> Metrics`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`



## deltalake_core::kernel::transaction::PostCommit

*Struct*

Represents items for the post commit hook

**Fields:**
- `version: i64` - The winning version number of the commit
- `data: CommitData` - The data that was committed to the log store

**Trait Implementations:**

- **IntoFuture**
  - `fn into_future(self: Self) -> <Self as >::IntoFuture`



## deltalake_core::kernel::transaction::PostCommitHookProperties

*Struct*

Properties for post commit hook.

**Traits:** Copy

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> PostCommitHookProperties`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## deltalake_core::kernel::transaction::PostCommitMetrics

*Struct*

**Fields:**
- `new_checkpoint_created: bool` - Whether a new checkpoint was created as part of this commit
- `num_log_files_cleaned_up: u64` - Number of log files cleaned up

**Trait Implementations:**

- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &PostCommitMetrics) -> bool`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Clone**
  - `fn clone(self: &Self) -> PostCommitMetrics`
- **Default**
  - `fn default() -> PostCommitMetrics`



## deltalake_core::kernel::transaction::PreCommit

*Struct*

Represents a commit that has not yet started but all details are finalized

**Generic Parameters:**
- 'a

**Methods:**

- `fn into_prepared_commit_future(self: Self) -> BoxFuture<'a, DeltaResult<PreparedCommit<'a>>>` - Prepare the commit but do not finalize it

**Trait Implementations:**

- **IntoFuture**
  - `fn into_future(self: Self) -> <Self as >::IntoFuture`



## deltalake_core::kernel::transaction::PreparedCommit

*Struct*

Represents a inflight commit

**Generic Parameters:**
- 'a

**Methods:**

- `fn commit_or_bytes(self: &Self) -> &CommitOrBytes` - The temporary commit file created

**Trait Implementations:**

- **IntoFuture**
  - `fn into_future(self: Self) -> <Self as >::IntoFuture`



## deltalake_core::kernel::transaction::TableReference

*Trait*

Reference to some structure that contains mandatory attributes for performing a commit.

**Methods:**

- `config`: Well known table configuration
- `protocol`: Get the table protocol of the snapshot
- `metadata`: Get the table metadata of the snapshot
- `eager_snapshot`: Try to cast this table reference to a `EagerSnapshot`



## deltalake_core::kernel::transaction::TransactionError

*Enum*

Error raised while commititng transaction

**Variants:**
- `VersionAlreadyExists(i64)` - Version already exists
- `SerializeLogJson{ json_err: serde_json::error::Error }` - Error returned when reading the delta log object failed.
- `ObjectStore{ source: object_store::Error }` - Error returned when reading the delta log object failed.
- `CommitConflict(CommitConflictError)` - Error returned when a commit conflict occurred
- `MaxCommitAttempts(i32)` - Error returned when maximum number of commit trioals is exceeded
- `DeltaTableAppendOnly` - The transaction includes Remove action with data change but Delta table is append-only
- `UnsupportedTableFeatures(Vec<delta_kernel::table_features::TableFeature>)` - Error returned when unsupported table features are required
- `TableFeaturesRequired(delta_kernel::table_features::TableFeature)` - Error returned when table features are required but not specified
- `LogStoreError{ msg: String, source: Box<dyn std::error::Error> }` - The transaction failed to commit due to an error in an implementation-specific layer.

**Trait Implementations:**

- **Display**
  - `fn fmt(self: &Self, __formatter: & mut ::core::fmt::Formatter) -> ::core::fmt::Result`
- **From**
  - `fn from(source: ObjectStoreError) -> Self`
- **Error**
  - `fn source(self: &Self) -> ::core::option::Option<&dyn ::thiserror::__private18::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(source: CommitConflictError) -> Self`



