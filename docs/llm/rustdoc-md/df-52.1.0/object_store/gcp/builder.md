**object_store > gcp > builder**

# Module: gcp::builder

## Contents

**Structs**

- [`GoogleCloudStorageBuilder`](#googlecloudstoragebuilder) - Configure a connection to Google Cloud Storage.

**Enums**

- [`GoogleConfigKey`](#googleconfigkey) - Configuration keys for [`GoogleCloudStorageBuilder`]

---

## object_store::gcp::builder::GoogleCloudStorageBuilder

*Struct*

Configure a connection to Google Cloud Storage.

If no credentials are explicitly provided, they will be sourced
from the environment as documented [here](https://cloud.google.com/docs/authentication/application-default-credentials).

# Example
```
# let BUCKET_NAME = "foo";
# use object_store::gcp::GoogleCloudStorageBuilder;
let gcs = GoogleCloudStorageBuilder::from_env().with_bucket_name(BUCKET_NAME).build();
```

**Methods:**

- `fn new() -> Self` - Create a new [`GoogleCloudStorageBuilder`] with default values.
- `fn from_env() -> Self` - Create an instance of [`GoogleCloudStorageBuilder`] with values pre-populated from environment variables.
- `fn with_url<impl Into<String>>(self: Self, url: impl Trait) -> Self` - Parse available connection info form a well-known storage URL.
- `fn with_config<impl Into<String>>(self: Self, key: GoogleConfigKey, value: impl Trait) -> Self` - Set an option on the builder via a key - value pair.
- `fn get_config_value(self: &Self, key: &GoogleConfigKey) -> Option<String>` - Get config value via a [`GoogleConfigKey`].
- `fn with_bucket_name<impl Into<String>>(self: Self, bucket_name: impl Trait) -> Self` - Set the bucket name (required)
- `fn with_service_account_path<impl Into<String>>(self: Self, service_account_path: impl Trait) -> Self` - Set the path to the service account file.
- `fn with_service_account_key<impl Into<String>>(self: Self, service_account: impl Trait) -> Self` - Set the service account key. The service account must be in the JSON
- `fn with_application_credentials<impl Into<String>>(self: Self, application_credentials_path: impl Trait) -> Self` - Set the path to the application credentials file.
- `fn with_skip_signature(self: Self, skip_signature: bool) -> Self` - If enabled, [`GoogleCloudStorage`] will not fetch credentials and will not sign requests.
- `fn with_credentials(self: Self, credentials: GcpCredentialProvider) -> Self` - Set the credential provider overriding any other options
- `fn with_retry(self: Self, retry_config: RetryConfig) -> Self` - Set the retry configuration
- `fn with_proxy_url<impl Into<String>>(self: Self, proxy_url: impl Trait) -> Self` - Set the proxy_url to be used by the underlying client
- `fn with_proxy_ca_certificate<impl Into<String>>(self: Self, proxy_ca_certificate: impl Trait) -> Self` - Set a trusted proxy CA certificate
- `fn with_proxy_excludes<impl Into<String>>(self: Self, proxy_excludes: impl Trait) -> Self` - Set a list of hosts to exclude from proxy connections
- `fn with_client_options(self: Self, options: ClientOptions) -> Self` - Sets the client options, overriding any already set
- `fn with_http_connector<C>(self: Self, connector: C) -> Self` - The [`HttpConnector`] to use
- `fn build(self: Self) -> Result<GoogleCloudStorage>` - Configure a connection to Google Cloud Storage, returning a

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> Self`
- **Clone**
  - `fn clone(self: &Self) -> GoogleCloudStorageBuilder`



## object_store::gcp::builder::GoogleConfigKey

*Enum*

Configuration keys for [`GoogleCloudStorageBuilder`]

Configuration via keys can be done via [`GoogleCloudStorageBuilder::with_config`]

# Example
```
# use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};
let builder = GoogleCloudStorageBuilder::new()
    .with_config("google_service_account".parse().unwrap(), "my-service-account")
    .with_config(GoogleConfigKey::Bucket, "my-bucket");
```

**Variants:**
- `ServiceAccount` - Path to the service account file
- `ServiceAccountKey` - The serialized service account key.
- `Bucket` - Bucket name
- `ApplicationCredentials` - Application credentials path
- `SkipSignature` - Skip signing request
- `Client(crate::ClientConfigKey)` - Client options

**Traits:** Copy, Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> GoogleConfigKey`
- **PartialEq**
  - `fn eq(self: &Self, other: &GoogleConfigKey) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **AsRef**
  - `fn as_ref(self: &Self) -> &str`
- **FromStr**
  - `fn from_str(s: &str) -> Result<Self, <Self as >::Err>`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`



