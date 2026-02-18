**object_store > azure > credential**

# Module: azure::credential

## Contents

**Modules**

- [`authority_hosts`](#authority_hosts) - A list of known Azure authority hosts

**Structs**

- [`AzureAccessKey`](#azureaccesskey) - A shared Azure Storage Account Key
- [`AzureAuthorizer`](#azureauthorizer) - Authorize a [`HttpRequest`] with an [`AzureAuthorizer`]

**Enums**

- [`AzureCredential`](#azurecredential) - An Azure storage credential
- [`Error`](#error)

---

## object_store::azure::credential::AzureAccessKey

*Struct*

A shared Azure Storage Account Key

**Tuple Struct**: `()`

**Methods:**

- `fn try_new(key: &str) -> std::result::Result<Self, Error>` - Create a new [`AzureAccessKey`], checking it for validity

**Traits:** Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> AzureAccessKey`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **PartialEq**
  - `fn eq(self: &Self, other: &AzureAccessKey) -> bool`



## object_store::azure::credential::AzureAuthorizer

*Struct*

Authorize a [`HttpRequest`] with an [`AzureAuthorizer`]

**Generic Parameters:**
- 'a

**Methods:**

- `fn new(credential: &'a AzureCredential, account: &'a str) -> Self` - Create a new [`AzureAuthorizer`]
- `fn authorize(self: &Self, request: & mut HttpRequest)` - Authorize `request`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## object_store::azure::credential::AzureCredential

*Enum*

An Azure storage credential

**Variants:**
- `AccessKey(AzureAccessKey)` - A shared access key
- `SASToken(Vec<(String, String)>)` - A shared access signature
- `BearerToken(String)` - An authorization token

**Methods:**

- `fn sensitive_request(self: &Self) -> bool` - Determines if the credential requires the request be treated as sensitive

**Traits:** Eq

**Trait Implementations:**

- **PartialEq**
  - `fn eq(self: &Self, other: &AzureCredential) -> bool`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## object_store::azure::credential::Error

*Enum*

**Variants:**
- `TokenRequest{ source: crate::client::retry::RetryError }`
- `TokenResponseBody{ source: crate::client::HttpError }`
- `FederatedTokenFile`
- `InvalidAccessKey{ source: base64::DecodeError }`
- `AzureCli{ message: String }`
- `AzureCliResponse{ source: serde_json::Error }`
- `SASforSASNotSupported`



## Module: authority_hosts

A list of known Azure authority hosts



