**object_store > aws > credential**

# Module: aws::credential

## Contents

**Structs**

- [`AwsAuthorizer`](#awsauthorizer) - Authorize a [`HttpRequest`] with an [`AwsCredential`] using [AWS SigV4]
- [`AwsCredential`](#awscredential) - A set of AWS security credentials

---

## object_store::aws::credential::AwsAuthorizer

*Struct*

Authorize a [`HttpRequest`] with an [`AwsCredential`] using [AWS SigV4]

[AWS SigV4]: https://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html

**Generic Parameters:**
- 'a

**Methods:**

- `fn new(credential: &'a AwsCredential, service: &'a str, region: &'a str) -> Self` - Create a new [`AwsAuthorizer`]
- `fn with_sign_payload(self: Self, signed: bool) -> Self` - Controls whether this [`AwsAuthorizer`] will attempt to sign the request payload,
- `fn with_request_payer(self: Self, request_payer: bool) -> Self` - Set whether to include requester pays headers
- `fn authorize(self: &Self, request: & mut HttpRequest, pre_calculated_digest: Option<&[u8]>)` - Authorize `request` with an optional pre-calculated SHA256 digest by attaching

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## object_store::aws::credential::AwsCredential

*Struct*

A set of AWS security credentials

**Fields:**
- `key_id: String` - AWS_ACCESS_KEY_ID
- `secret_key: String` - AWS_SECRET_ACCESS_KEY
- `token: Option<String>` - AWS_SESSION_TOKEN

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &AwsCredential) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`



