**object_store > aws > dynamo**

# Module: aws::dynamo

## Contents

**Structs**

- [`DynamoCommit`](#dynamocommit) - A DynamoDB-based commit protocol, used to provide conditional write support for S3

---

## object_store::aws::dynamo::DynamoCommit

*Struct*

A DynamoDB-based commit protocol, used to provide conditional write support for S3

## Limitations

Only conditional operations, e.g. `copy_if_not_exists` will be synchronized, and can
therefore race with non-conditional operations, e.g. `put`, `copy`, `delete`, or
conditional operations performed by writers not configured to synchronize with DynamoDB.

Workloads making use of this mechanism **must** ensure:

* Conditional and non-conditional operations are not performed on the same paths
* Conditional operations are only performed via similarly configured clients

Additionally as the locking mechanism relies on timeouts to detect stale locks,
performance will be poor for systems that frequently delete and then create
objects at the same path, instead being optimised for systems that primarily create
files with paths never used before, or perform conditional updates to existing files

## Commit Protocol

The DynamoDB schema is as follows:

* A string partition key named `"path"`
* A string sort key named `"etag"`
* A numeric [TTL] attribute named `"ttl"`
* A numeric attribute named `"generation"`
* A numeric attribute named `"timeout"`

An appropriate DynamoDB table can be created with the CLI as follows:

```bash
$ aws dynamodb create-table --table-name <TABLE_NAME> --key-schema AttributeName=path,KeyType=HASH AttributeName=etag,KeyType=RANGE --attribute-definitions AttributeName=path,AttributeType=S AttributeName=etag,AttributeType=S
$ aws dynamodb update-time-to-live --table-name <TABLE_NAME> --time-to-live-specification Enabled=true,AttributeName=ttl
```

To perform a conditional operation on an object with a given `path` and `etag` (`*` if creating),
the commit protocol is as follows:

1. Perform HEAD request on `path` and error on precondition mismatch
2. Create record in DynamoDB with given `path` and `etag` with the configured timeout
    1. On Success: Perform operation with the configured timeout
    2. On Conflict:
        1. Periodically re-perform HEAD request on `path` and error on precondition mismatch
        2. If `timeout * max_skew_rate` passed, replace the record incrementing the `"generation"`
            1. On Success: GOTO 2.1
            2. On Conflict: GOTO 2.2

Provided no writer modifies an object with a given `path` and `etag` without first adding a
corresponding record to DynamoDB, we are guaranteed that only one writer will ever commit.

This is inspired by the [DynamoDB Lock Client] but simplified for the more limited
requirements of synchronizing object storage. The major changes are:

* Uses a monotonic generation count instead of a UUID rvn, as this is:
    * Cheaper to generate, serialize and compare
    * Cannot collide
    * More human readable / interpretable
* Relies on [TTL] to eventually clean up old locks

It also draws inspiration from the DeltaLake [S3 Multi-Cluster] commit protocol, but
generalised to not make assumptions about the workload and not rely on first writing
to a temporary path.

[TTL]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/howitworks-ttl.html
[DynamoDB Lock Client]: https://aws.amazon.com/blogs/database/building-distributed-locks-with-the-dynamodb-lock-client/
[S3 Multi-Cluster]: https://docs.google.com/document/d/1Gs4ZsTH19lMxth4BSdwlWjUNR-XhKHicDvBjd2RqNd8/edit#heading=h.mjjuxw9mcz9h

**Methods:**

- `fn new(table_name: String) -> Self` - Create a new [`DynamoCommit`] with a given table name
- `fn with_timeout(self: Self, millis: u64) -> Self` - Overrides the lock timeout.
- `fn with_max_clock_skew_rate(self: Self, rate: u32) -> Self` - The maximum clock skew rate tolerated by the system.
- `fn with_ttl(self: Self, ttl: Duration) -> Self` - The length of time a record should be retained in DynamoDB before being cleaned up

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> DynamoCommit`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &DynamoCommit) -> bool`



