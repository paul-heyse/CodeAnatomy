**object_store > aws > resolve**

# Module: aws::resolve

## Contents

**Functions**

- [`resolve_bucket_region`](#resolve_bucket_region) - Get the bucket region using the [HeadBucket API]. This will fail if the bucket does not exist.

---

## object_store::aws::resolve::resolve_bucket_region

*Function*

Get the bucket region using the [HeadBucket API]. This will fail if the bucket does not exist.

[HeadBucket API]: https://docs.aws.amazon.com/AmazonS3/latest/API/API_HeadBucket.html

```rust
fn resolve_bucket_region(bucket: &str, client_options: &crate::ClientOptions) -> crate::Result<String>
```



