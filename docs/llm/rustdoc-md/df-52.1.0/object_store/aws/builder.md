**object_store > aws > builder**

# Module: aws::builder

## Contents

**Structs**

- [`AmazonS3Builder`](#amazons3builder) - Configure a connection to Amazon S3 using the specified credentials in

**Enums**

- [`AmazonS3ConfigKey`](#amazons3configkey) - Configuration keys for [`AmazonS3Builder`]
- [`S3EncryptionConfigKey`](#s3encryptionconfigkey) - Encryption configuration options for S3.

---

## object_store::aws::builder::AmazonS3Builder

*Struct*

Configure a connection to Amazon S3 using the specified credentials in
the specified Amazon region and bucket.

# Example
```
# let REGION = "foo";
# let BUCKET_NAME = "foo";
# let ACCESS_KEY_ID = "foo";
# let SECRET_KEY = "foo";
# use object_store::aws::AmazonS3Builder;
let s3 = AmazonS3Builder::new()
 .with_region(REGION)
 .with_bucket_name(BUCKET_NAME)
 .with_access_key_id(ACCESS_KEY_ID)
 .with_secret_access_key(SECRET_KEY)
 .build();
```

**Methods:**

- `fn new() -> Self` - Create a new [`AmazonS3Builder`] with default values.
- `fn from_env() -> Self` - Fill the [`AmazonS3Builder`] with regular AWS environment variables
- `fn with_url<impl Into<String>>(self: Self, url: impl Trait) -> Self` - Parse available connection info form a well-known storage URL.
- `fn with_config<impl Into<String>>(self: Self, key: AmazonS3ConfigKey, value: impl Trait) -> Self` - Set an option on the builder via a key - value pair.
- `fn get_config_value(self: &Self, key: &AmazonS3ConfigKey) -> Option<String>` - Get config value via a [`AmazonS3ConfigKey`].
- `fn with_access_key_id<impl Into<String>>(self: Self, access_key_id: impl Trait) -> Self` - Set the AWS Access Key
- `fn with_secret_access_key<impl Into<String>>(self: Self, secret_access_key: impl Trait) -> Self` - Set the AWS Secret Access Key
- `fn with_token<impl Into<String>>(self: Self, token: impl Trait) -> Self` - Set the AWS Session Token to use for requests
- `fn with_region<impl Into<String>>(self: Self, region: impl Trait) -> Self` - Set the region, defaults to `us-east-1`
- `fn with_bucket_name<impl Into<String>>(self: Self, bucket_name: impl Trait) -> Self` - Set the bucket_name (required)
- `fn with_endpoint<impl Into<String>>(self: Self, endpoint: impl Trait) -> Self` - Sets the endpoint for communicating with AWS S3, defaults to the [region endpoint]
- `fn with_credentials(self: Self, credentials: AwsCredentialProvider) -> Self` - Set the credential provider overriding any other options
- `fn with_allow_http(self: Self, allow_http: bool) -> Self` - Sets what protocol is allowed. If `allow_http` is :
- `fn with_virtual_hosted_style_request(self: Self, virtual_hosted_style_request: bool) -> Self` - Sets if virtual hosted style request has to be used.
- `fn with_s3_express(self: Self, s3_express: bool) -> Self` - Configure this as an S3 Express One Zone Bucket
- `fn with_retry(self: Self, retry_config: RetryConfig) -> Self` - Set the retry configuration
- `fn with_imdsv1_fallback(self: Self) -> Self` - By default instance credentials will only be fetched over [IMDSv2], as AWS recommends
- `fn with_unsigned_payload(self: Self, unsigned_payload: bool) -> Self` - Sets if unsigned payload option has to be used.
- `fn with_skip_signature(self: Self, skip_signature: bool) -> Self` - If enabled, [`AmazonS3`] will not fetch credentials and will not sign requests
- `fn with_checksum_algorithm(self: Self, checksum_algorithm: Checksum) -> Self` - Sets the [checksum algorithm] which has to be used for object integrity check during upload.
- `fn with_metadata_endpoint<impl Into<String>>(self: Self, endpoint: impl Trait) -> Self` - Set the [instance metadata endpoint](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html),
- `fn with_proxy_url<impl Into<String>>(self: Self, proxy_url: impl Trait) -> Self` - Set the proxy_url to be used by the underlying client
- `fn with_proxy_ca_certificate<impl Into<String>>(self: Self, proxy_ca_certificate: impl Trait) -> Self` - Set a trusted proxy CA certificate
- `fn with_proxy_excludes<impl Into<String>>(self: Self, proxy_excludes: impl Trait) -> Self` - Set a list of hosts to exclude from proxy connections
- `fn with_client_options(self: Self, options: ClientOptions) -> Self` - Sets the client options, overriding any already set
- `fn with_copy_if_not_exists(self: Self, config: S3CopyIfNotExists) -> Self` - Configure how to provide `copy_if_not_exists`
- `fn with_conditional_put(self: Self, config: S3ConditionalPut) -> Self` - Configure how to provide conditional put operations.
- `fn with_disable_tagging(self: Self, ignore: bool) -> Self` - If set to `true` will ignore any tags provided to put_opts
- `fn with_sse_kms_encryption<impl Into<String>>(self: Self, kms_key_id: impl Trait) -> Self` - Use SSE-KMS for server side encryption.
- `fn with_dsse_kms_encryption<impl Into<String>>(self: Self, kms_key_id: impl Trait) -> Self` - Use dual server side encryption for server side encryption.
- `fn with_ssec_encryption<impl Into<String>>(self: Self, customer_key_base64: impl Trait) -> Self` - Use SSE-C for server side encryption.
- `fn with_bucket_key(self: Self, enabled: bool) -> Self` - Set whether to enable bucket key for server side encryption. This overrides
- `fn with_request_payer(self: Self, enabled: bool) -> Self` - Set whether to charge requester for bucket operations.
- `fn with_http_connector<C>(self: Self, connector: C) -> Self` - The [`HttpConnector`] to use
- `fn build(self: Self) -> Result<AmazonS3>` - Create a [`AmazonS3`] instance from the provided values,

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Default**
  - `fn default() -> AmazonS3Builder`
- **Clone**
  - `fn clone(self: &Self) -> AmazonS3Builder`



## object_store::aws::builder::AmazonS3ConfigKey

*Enum*

Configuration keys for [`AmazonS3Builder`]

Configuration via keys can be done via [`AmazonS3Builder::with_config`]

# Example
```
# use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
let builder = AmazonS3Builder::new()
    .with_config("aws_access_key_id".parse().unwrap(), "my-access-key-id")
    .with_config(AmazonS3ConfigKey::DefaultRegion, "my-default-region");
```

**Variants:**
- `AccessKeyId` - AWS Access Key
- `SecretAccessKey` - Secret Access Key
- `Region` - Region
- `DefaultRegion` - Default region
- `Bucket` - Bucket name
- `Endpoint` - Sets custom endpoint for communicating with AWS S3.
- `Token` - Token to use for requests (passed to underlying provider)
- `ImdsV1Fallback` - Fall back to ImdsV1
- `VirtualHostedStyleRequest` - If virtual hosted style request has to be used
- `UnsignedPayload` - Avoid computing payload checksum when calculating signature.
- `Checksum` - Set the checksum algorithm for this client
- `MetadataEndpoint` - Set the instance metadata endpoint
- `ContainerCredentialsRelativeUri` - Set the container credentials relative URI when used in ECS
- `ContainerCredentialsFullUri` - Set the container credentials full URI when used in EKS
- `ContainerAuthorizationTokenFile` - Set the authorization token in plain text when used in EKS to authenticate with ContainerCredentialsFullUri
- `WebIdentityTokenFile` - Web identity token file path for AssumeRoleWithWebIdentity
- `RoleArn` - Role ARN to assume when using web identity token
- `RoleSessionName` - Session name for web identity role assumption
- `StsEndpoint` - Custom STS endpoint for web identity token exchange
- `CopyIfNotExists` - Configure how to provide `copy_if_not_exists`
- `ConditionalPut` - Configure how to provide conditional put operations
- `SkipSignature` - Skip signing request
- `DisableTagging` - Disable tagging objects
- `S3Express` - Enable Support for S3 Express One Zone
- `RequestPayer` - Enable Support for S3 Requester Pays
- `Client(crate::ClientConfigKey)` - Client options
- `Encryption(S3EncryptionConfigKey)` - Encryption options

**Traits:** Copy, Eq

**Trait Implementations:**

- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **FromStr**
  - `fn from_str(s: &str) -> Result<Self, <Self as >::Err>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **AsRef**
  - `fn as_ref(self: &Self) -> &str`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **Clone**
  - `fn clone(self: &Self) -> AmazonS3ConfigKey`
- **PartialEq**
  - `fn eq(self: &Self, other: &AmazonS3ConfigKey) -> bool`



## object_store::aws::builder::S3EncryptionConfigKey

*Enum*

Encryption configuration options for S3.

These options are used to configure server-side encryption for S3 objects.
To configure them, pass them to [`AmazonS3Builder::with_config`].

[SSE-S3]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingServerSideEncryption.html
[SSE-KMS]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html
[DSSE-KMS]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingDSSEncryption.html
[SSE-C]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/ServerSideEncryptionCustomerKeys.html

**Variants:**
- `ServerSideEncryption` - Type of encryption to use. If set, must be one of "AES256" (SSE-S3), "aws:kms" (SSE-KMS), "aws:kms:dsse" (DSSE-KMS) or "sse-c".
- `KmsKeyId` - The KMS key ID to use for server-side encryption. If set, ServerSideEncryption
- `BucketKeyEnabled` - If set to true, will use the bucket's default KMS key for server-side encryption.
- `CustomerEncryptionKey` - The base64 encoded, 256-bit customer encryption key to use for server-side encryption.



