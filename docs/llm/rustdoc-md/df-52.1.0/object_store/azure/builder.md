**object_store > azure > builder**

# Module: azure::builder

## Contents

**Structs**

- [`MicrosoftAzureBuilder`](#microsoftazurebuilder) - Configure a connection to Microsoft Azure Blob Storage container using

**Enums**

- [`AzureConfigKey`](#azureconfigkey) - Configuration keys for [`MicrosoftAzureBuilder`]

---

## object_store::azure::builder::AzureConfigKey

*Enum*

Configuration keys for [`MicrosoftAzureBuilder`]

Configuration via keys can be done via [`MicrosoftAzureBuilder::with_config`]

# Example
```
# use object_store::azure::{MicrosoftAzureBuilder, AzureConfigKey};
let builder = MicrosoftAzureBuilder::new()
    .with_config("azure_client_id".parse().unwrap(), "my-client-id")
    .with_config(AzureConfigKey::AuthorityId, "my-tenant-id");
```

**Variants:**
- `AccountName` - The name of the azure storage account
- `AccessKey` - Master key for accessing storage account
- `ClientId` - Service principal client id for authorizing requests
- `ClientSecret` - Service principal client secret for authorizing requests
- `AuthorityId` - Tenant id used in oauth flows
- `AuthorityHost` - Authority host used in oauth flows
- `SasKey` - Shared access signature.
- `Token` - Bearer token
- `UseEmulator` - Use object store with azurite storage emulator
- `Endpoint` - Override the endpoint used to communicate with blob storage
- `UseFabricEndpoint` - Use object store with url scheme account.dfs.fabric.microsoft.com
- `MsiEndpoint` - Endpoint to request a imds managed identity token
- `ObjectId` - Object id for use with managed identity authentication
- `MsiResourceId` - Msi resource id for use with managed identity authentication
- `FederatedTokenFile` - File containing token for Azure AD workload identity federation
- `UseAzureCli` - Use azure cli for acquiring access token
- `SkipSignature` - Skip signing requests
- `ContainerName` - Container name
- `DisableTagging` - Disables tagging objects
- `FabricTokenServiceUrl` - Fabric token service url
- `FabricWorkloadHost` - Fabric workload host
- `FabricSessionToken` - Fabric session token
- `FabricClusterIdentifier` - Fabric cluster identifier
- `Client(crate::ClientConfigKey)` - Client options

**Traits:** Copy, Eq

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> AzureConfigKey`
- **PartialEq**
  - `fn eq(self: &Self, other: &AzureConfigKey) -> bool`
- **Hash**
  - `fn hash<__H>(self: &Self, state: & mut __H)`
- **AsRef**
  - `fn as_ref(self: &Self) -> &str`
- **FromStr**
  - `fn from_str(s: &str) -> Result<Self, <Self as >::Err>`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`



## object_store::azure::builder::MicrosoftAzureBuilder

*Struct*

Configure a connection to Microsoft Azure Blob Storage container using
the specified credentials.

# Example
```
# let ACCOUNT = "foo";
# let BUCKET_NAME = "foo";
# let ACCESS_KEY = "foo";
# use object_store::azure::MicrosoftAzureBuilder;
let azure = MicrosoftAzureBuilder::new()
 .with_account(ACCOUNT)
 .with_access_key(ACCESS_KEY)
 .with_container_name(BUCKET_NAME)
 .build();
```

**Methods:**

- `fn new() -> Self` - Create a new [`MicrosoftAzureBuilder`] with default values.
- `fn from_env() -> Self` - Create an instance of [`MicrosoftAzureBuilder`] with values pre-populated from environment variables.
- `fn with_url<impl Into<String>>(self: Self, url: impl Trait) -> Self` - Parse available connection info form a well-known storage URL.
- `fn with_config<impl Into<String>>(self: Self, key: AzureConfigKey, value: impl Trait) -> Self` - Set an option on the builder via a key - value pair.
- `fn get_config_value(self: &Self, key: &AzureConfigKey) -> Option<String>` - Get config value via a [`AzureConfigKey`].
- `fn with_account<impl Into<String>>(self: Self, account: impl Trait) -> Self` - Set the Azure Account (required)
- `fn with_container_name<impl Into<String>>(self: Self, container_name: impl Trait) -> Self` - Set the Azure Container Name (required)
- `fn with_access_key<impl Into<String>>(self: Self, access_key: impl Trait) -> Self` - Set the Azure Access Key (required - one of access key, bearer token, or client credentials)
- `fn with_bearer_token_authorization<impl Into<String>>(self: Self, bearer_token: impl Trait) -> Self` - Set a static bearer token to be used for authorizing requests
- `fn with_client_secret_authorization<impl Into<String>, impl Into<String>, impl Into<String>>(self: Self, client_id: impl Trait, client_secret: impl Trait, tenant_id: impl Trait) -> Self` - Set a client secret used for client secret authorization
- `fn with_client_id<impl Into<String>>(self: Self, client_id: impl Trait) -> Self` - Sets the client id for use in client secret or k8s federated credential flow
- `fn with_client_secret<impl Into<String>>(self: Self, client_secret: impl Trait) -> Self` - Sets the client secret for use in client secret flow
- `fn with_tenant_id<impl Into<String>>(self: Self, tenant_id: impl Trait) -> Self` - Sets the tenant id for use in client secret or k8s federated credential flow
- `fn with_sas_authorization<impl Into<Vec<(String, String)>>>(self: Self, query_pairs: impl Trait) -> Self` - Set query pairs appended to the url for shared access signature authorization
- `fn with_credentials(self: Self, credentials: AzureCredentialProvider) -> Self` - Set the credential provider overriding any other options
- `fn with_use_emulator(self: Self, use_emulator: bool) -> Self` - Set if the Azure emulator should be used (defaults to false)
- `fn with_endpoint(self: Self, endpoint: String) -> Self` - Override the endpoint used to communicate with blob storage
- `fn with_use_fabric_endpoint(self: Self, use_fabric_endpoint: bool) -> Self` - Set if Microsoft Fabric url scheme should be used (defaults to false)
- `fn with_allow_http(self: Self, allow_http: bool) -> Self` - Sets what protocol is allowed
- `fn with_authority_host<impl Into<String>>(self: Self, authority_host: impl Trait) -> Self` - Sets an alternative authority host for OAuth based authorization
- `fn with_retry(self: Self, retry_config: RetryConfig) -> Self` - Set the retry configuration
- `fn with_proxy_url<impl Into<String>>(self: Self, proxy_url: impl Trait) -> Self` - Set the proxy_url to be used by the underlying client
- `fn with_proxy_ca_certificate<impl Into<String>>(self: Self, proxy_ca_certificate: impl Trait) -> Self` - Set a trusted proxy CA certificate
- `fn with_proxy_excludes<impl Into<String>>(self: Self, proxy_excludes: impl Trait) -> Self` - Set a list of hosts to exclude from proxy connections
- `fn with_client_options(self: Self, options: ClientOptions) -> Self` - Sets the client options, overriding any already set
- `fn with_msi_endpoint<impl Into<String>>(self: Self, msi_endpoint: impl Trait) -> Self` - Sets the endpoint for acquiring managed identity token
- `fn with_federated_token_file<impl Into<String>>(self: Self, federated_token_file: impl Trait) -> Self` - Sets a file path for acquiring azure federated identity token in k8s
- `fn with_use_azure_cli(self: Self, use_azure_cli: bool) -> Self` - Set if the Azure Cli should be used for acquiring access token
- `fn with_skip_signature(self: Self, skip_signature: bool) -> Self` - If enabled, [`MicrosoftAzure`] will not fetch credentials and will not sign requests
- `fn with_disable_tagging(self: Self, ignore: bool) -> Self` - If set to `true` will ignore any tags provided to put_opts
- `fn with_http_connector<C>(self: Self, connector: C) -> Self` - The [`HttpConnector`] to use
- `fn build(self: Self) -> Result<MicrosoftAzure>` - Configure a connection to container with given name on Microsoft Azure Blob store.

**Trait Implementations:**

- **Clone**
  - `fn clone(self: &Self) -> MicrosoftAzureBuilder`
- **Default**
  - `fn default() -> MicrosoftAzureBuilder`
- **Debug**
  - `fn fmt(self: &Self, f: & mut std::fmt::Formatter) -> std::fmt::Result`



