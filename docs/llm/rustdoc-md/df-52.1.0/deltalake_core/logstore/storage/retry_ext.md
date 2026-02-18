**deltalake_core > logstore > storage > retry_ext**

# Module: logstore::storage::retry_ext

## Contents

**Traits**

- [`ObjectStoreRetryExt`](#objectstoreretryext) - Retry extension for [`ObjectStore`]

---

## deltalake_core::logstore::storage::retry_ext::ObjectStoreRetryExt

*Trait*

Retry extension for [`ObjectStore`]

Read-only operations are retried by [`ObjectStore`] internally. However, PUT/DELETE operations
are not retried even thought they are technically idempotent. [`ObjectStore`] does not retry
those operations because having preconditions may produce different results for the same
request. PUT/DELETE operations without preconditions are idempotent and can be retried.
Unfortunately, [`ObjectStore`]'s retry mechanism only works on HTTP request level, thus there
is no way to distinguish whether a request has preconditions or not.

This trait provides additional methods for working with [`ObjectStore`] that automatically retry
unconditional operations when they fail.

See also:
- https://github.com/apache/arrow-rs/pull/5278

**Methods:**

- `put_with_retries`: Save the provided bytes to the specified location
- `delete_with_retries`: Delete the object at the specified location



