**object_store > client > http > body**

# Module: client::http::body

## Contents

**Structs**

- [`HttpRequestBody`](#httprequestbody) - The [`Body`] of an [`HttpRequest`]
- [`HttpResponseBody`](#httpresponsebody) - The body of an [`HttpResponse`]

**Type Aliases**

- [`HttpRequest`](#httprequest) - An HTTP Request
- [`HttpResponse`](#httpresponse) - An HTTP response

---

## object_store::client::http::body::HttpRequest

*Type Alias*: `http::Request<HttpRequestBody>`

An HTTP Request



## object_store::client::http::body::HttpRequestBody

*Struct*

The [`Body`] of an [`HttpRequest`]

**Tuple Struct**: `()`

**Methods:**

- `fn empty() -> Self` - An empty [`HttpRequestBody`]
- `fn is_empty(self: &Self) -> bool` - Returns true if this body is empty
- `fn content_length(self: &Self) -> usize` - Returns the total length of the [`Bytes`] in this body
- `fn as_bytes(self: &Self) -> Option<&Bytes>` - If this body consists of a single contiguous [`Bytes`], returns it

**Trait Implementations:**

- **Body**
  - `fn poll_frame(self: Pin<& mut Self>, _cx: & mut Context) -> Poll<Option<Result<Frame<<Self as >::Data>, <Self as >::Error>>>`
  - `fn is_end_stream(self: &Self) -> bool`
  - `fn size_hint(self: &Self) -> SizeHint`
- **From**
  - `fn from(value: PutPayload) -> Self`
- **Clone**
  - `fn clone(self: &Self) -> HttpRequestBody`
- **From**
  - `fn from(value: String) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(value: Vec<u8>) -> Self`
- **From**
  - `fn from(value: Bytes) -> Self`



## object_store::client::http::body::HttpResponse

*Type Alias*: `http::Response<HttpResponseBody>`

An HTTP response



## object_store::client::http::body::HttpResponseBody

*Struct*

The body of an [`HttpResponse`]

**Tuple Struct**: `()`

**Methods:**

- `fn new<B>(body: B) -> Self` - Create an [`HttpResponseBody`] from the provided [`Body`]
- `fn bytes(self: Self) -> Result<Bytes, HttpError>` - Collects this response into a [`Bytes`]
- `fn bytes_stream(self: Self) -> BoxStream<'static, Result<Bytes, HttpError>>` - Returns a stream of this response data

**Trait Implementations:**

- **Body**
  - `fn poll_frame(self: Pin<& mut Self>, cx: & mut Context) -> Poll<Option<Result<Frame<<Self as >::Data>, <Self as >::Error>>>`
  - `fn is_end_stream(self: &Self) -> bool`
  - `fn size_hint(self: &Self) -> SizeHint`
- **From**
  - `fn from(value: String) -> Self`
- **From**
  - `fn from(value: Vec<u8>) -> Self`
- **From**
  - `fn from(value: Bytes) -> Self`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



