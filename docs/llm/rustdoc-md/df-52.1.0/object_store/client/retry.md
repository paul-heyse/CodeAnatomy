**object_store > client > retry**

# Module: client::retry

## Contents

**Structs**

- [`RetryConfig`](#retryconfig) - The configuration for how to respond to request errors
- [`RetryError`](#retryerror) - Retry request error

**Enums**

- [`RequestError`](#requesterror) - The reason a request failed

---

## object_store::client::retry::RequestError

*Enum*

The reason a request failed

**Variants:**
- `BareRedirect`
- `Status{ status: reqwest::StatusCode, body: Option<String> }`
- `Response{ status: reqwest::StatusCode, body: String }`
- `Http(crate::client::HttpError)`



## object_store::client::retry::RetryConfig

*Struct*

The configuration for how to respond to request errors

The following categories of error will be retried:

* 5xx server errors
* Connection errors
* Dropped connections
* Timeouts for [safe] / read-only requests

Requests will be retried up to some limit, using exponential
backoff with jitter. See [`BackoffConfig`] for more information

[safe]: https://datatracker.ietf.org/doc/html/rfc7231#section-4.2.1

**Fields:**
- `backoff: crate::client::backoff::BackoffConfig` - The backoff configuration
- `max_retries: usize` - The maximum number of times to retry a request
- `retry_timeout: std::time::Duration` - The maximum length of time from the initial request

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> Self`
- **Clone**
  - `fn clone(self: &Self) -> RetryConfig`



## object_store::client::retry::RetryError

*Struct*

Retry request error

**Tuple Struct**: `()`



