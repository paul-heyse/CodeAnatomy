**deltalake_core > kernel > snapshot > stream**

# Module: kernel::snapshot::stream

## Contents

**Traits**

- [`RecordBatchStream`](#recordbatchstream) - Trait for types that stream [RecordBatch]

**Type Aliases**

- [`SendableRBStream`](#sendablerbstream)
- [`SendableRecordBatchStream`](#sendablerecordbatchstream) - Trait for a [`Stream`] of [`RecordBatch`]es that can be passed between threads

---

## deltalake_core::kernel::snapshot::stream::RecordBatchStream

*Trait*

Trait for types that stream [RecordBatch]

See [`SendableRecordBatchStream`] for more details.

**Methods:**

- `schema`: Returns the schema of this `RecordBatchStream`.



## deltalake_core::kernel::snapshot::stream::SendableRBStream

*Type Alias*: `std::pin::Pin<Box<dyn Stream>>`



## deltalake_core::kernel::snapshot::stream::SendableRecordBatchStream

*Type Alias*: `std::pin::Pin<Box<dyn RecordBatchStream>>`

Trait for a [`Stream`] of [`RecordBatch`]es that can be passed between threads

This trait is used to retrieve the results of DataFusion execution plan nodes.

The trait is a specialized Rust Async [`Stream`] that also knows the schema
of the data it will return (even if the stream has no data). Every
`RecordBatch` returned by the stream should have the same schema as returned
by [`schema`](`RecordBatchStream::schema`).

# See Also

* [`RecordBatchStreamAdapter`] to convert an existing [`Stream`]
  to [`SendableRecordBatchStream`]

[`RecordBatchStreamAdapter`]: https://docs.rs/datafusion/latest/datafusion/physical_plan/stream/struct.RecordBatchStreamAdapter.html

# Error Handling

Once a stream returns an error, it should not be polled again (the caller
should stop calling `next`) and handle the error.

However, returning `Ready(None)` (end of stream) is likely the safest
behavior after an error. Like [`Stream`]s, `RecordBatchStream`s should not
be polled after end of stream or returning an error. However, also like
[`Stream`]s there is no mechanism to prevent callers polling  so returning
`Ready(None)` is recommended.



