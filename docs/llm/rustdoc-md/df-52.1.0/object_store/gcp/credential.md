**object_store > gcp > credential**

# Module: gcp::credential

## Contents

**Structs**

- [`GcpCredential`](#gcpcredential) - A Google Cloud Storage Credential
- [`GcpSigningCredential`](#gcpsigningcredential) - A Google Cloud Storage Credential for signing
- [`ServiceAccountKey`](#serviceaccountkey) - A private RSA key for a service account

**Enums**

- [`Error`](#error)

---

## object_store::gcp::credential::Error

*Enum*

**Variants:**
- `OpenCredentials{ source: std::io::Error, path: std::path::PathBuf }`
- `DecodeCredentials{ source: serde_json::Error }`
- `MissingKey`
- `InvalidKey{ source: ring::error::KeyRejected }`
- `Sign{ source: ring::error::Unspecified }`
- `Encode{ source: serde_json::Error }`
- `UnsupportedKey{ encoding: String }`
- `TokenRequest{ source: crate::client::retry::RetryError }`
- `TokenResponseBody{ source: crate::client::HttpError }`
- `ReadPem{ source: std::io::Error }`



## object_store::gcp::credential::GcpCredential

*Struct*

A Google Cloud Storage Credential

**Fields:**
- `bearer: String` - An HTTP bearer token

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &GcpCredential) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## object_store::gcp::credential::GcpSigningCredential

*Struct*

A Google Cloud Storage Credential for signing

**Fields:**
- `email: String` - The email of the service account
- `private_key: Option<ServiceAccountKey>` - An optional RSA private key

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## object_store::gcp::credential::ServiceAccountKey

*Struct*

A private RSA key for a service account

**Tuple Struct**: `()`

**Methods:**

- `fn from_pem(encoded: &[u8]) -> std::result::Result<Self, Error>` - Parses a pem-encoded RSA key
- `fn from_pkcs8(key: &[u8]) -> std::result::Result<Self, Error>` - Parses an unencrypted PKCS#8-encoded RSA private key.
- `fn from_der(key: &[u8]) -> std::result::Result<Self, Error>` - Parses an unencrypted PKCS#8-encoded RSA private key.

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



