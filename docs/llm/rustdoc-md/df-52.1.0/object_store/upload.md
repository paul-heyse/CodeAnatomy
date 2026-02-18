**object_store > upload**

# Module: upload

## Contents

**Structs**

- [`WriteMultipart`](#writemultipart) - A synchronous write API for uploading data in parallel in fixed size chunks

**Traits**

- [`MultipartUpload`](#multipartupload) - A trait allowing writing an object in fixed size chunks

**Type Aliases**

- [`UploadPart`](#uploadpart) - An upload part request

---

## object_store::upload::MultipartUpload

*Trait*

A trait allowing writing an object in fixed size chunks

Consecutive chunks of data can be written by calling [`MultipartUpload::put_part`] and polling
the returned futures to completion. Multiple futures returned by [`MultipartUpload::put_part`]
may be polled in parallel, allowing for concurrent uploads.

Once all part uploads have been polled to completion, the upload can be completed by
calling [`MultipartUpload::complete`]. This will make the entire uploaded object visible
as an atomic operation.It is implementation behind behaviour if [`MultipartUpload::complete`]
is called before all [`UploadPart`] have been polled to completion.

**Methods:**

- `put_part`: Upload the next part
- `complete`: Complete the multipart upload
- `abort`: Abort the multipart upload



## object_store::upload::UploadPart

*Type Alias*: `futures::future::BoxFuture<'static, crate::Result<()>>`

An upload part request



## object_store::upload::WriteMultipart

*Struct*

A synchronous write API for uploading data in parallel in fixed size chunks

Uses multiple tokio tasks in a [`JoinSet`] to multiplex upload tasks in parallel

The design also takes inspiration from [`Sink`] with [`WriteMultipart::wait_for_capacity`]
allowing back pressure on producers, prior to buffering the next part. However, unlike
[`Sink`] this back pressure is optional, allowing integration with synchronous producers

[`Sink`]: futures::sink::Sink

**Methods:**

- `fn new(upload: Box<dyn MultipartUpload>) -> Self` - Create a new [`WriteMultipart`] that will upload using 5MB chunks
- `fn new_with_chunk_size(upload: Box<dyn MultipartUpload>, chunk_size: usize) -> Self` - Create a new [`WriteMultipart`] that will upload in fixed `chunk_size` sized chunks
- `fn poll_for_capacity(self: & mut Self, cx: & mut Context, max_concurrency: usize) -> Poll<Result<()>>` - Polls for there to be less than `max_concurrency` [`UploadPart`] in progress
- `fn wait_for_capacity(self: & mut Self, max_concurrency: usize) -> Result<()>` - Wait until there are less than `max_concurrency` [`UploadPart`] in progress
- `fn write(self: & mut Self, buf: &[u8])` - Write data to this [`WriteMultipart`]
- `fn put(self: & mut Self, bytes: Bytes)` - Put a chunk of data into this [`WriteMultipart`] without copying
- `fn abort(self: Self) -> Result<()>` - Abort this upload, attempting to clean up any successfully uploaded parts
- `fn finish(self: Self) -> Result<PutResult>` - Flush final chunk, and await completion of all in-flight requests

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



