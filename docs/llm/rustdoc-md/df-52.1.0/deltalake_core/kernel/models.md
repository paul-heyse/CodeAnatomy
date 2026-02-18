**deltalake_core > kernel > models**

# Module: kernel::models

## Contents

**Enums**

- [`Action`](#action)

---

## deltalake_core::kernel::models::Action

*Enum*

**Variants:**
- `Metadata(Metadata)`
- `Protocol(Protocol)`
- `Add(Add)`
- `Remove(Remove)`
- `Cdc(AddCDCFile)`
- `Txn(Transaction)`
- `CommitInfo(CommitInfo)`
- `DomainMetadata(DomainMetadata)`

**Methods:**

- `fn commit_info(info: HashMap<String, serde_json::Value>) -> Self` - Create a commit info from a map

**Traits:** Eq

**Trait Implementations:**

- **From**
  - `fn from(a: CommitInfo) -> Self`
- **From**
  - `fn from(a: Protocol) -> Self`
- **From**
  - `fn from(a: AddCDCFile) -> Self`
- **From**
  - `fn from(a: Remove) -> Self`
- **Serialize**
  - `fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private228::Result<<__S as >::Ok, <__S as >::Error>`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **From**
  - `fn from(a: Transaction) -> Self`
- **PartialEq**
  - `fn eq(self: &Self, other: &Action) -> bool`
- **Clone**
  - `fn clone(self: &Self) -> Action`
- **From**
  - `fn from(a: DomainMetadata) -> Self`
- **From**
  - `fn from(a: Metadata) -> Self`
- **From**
  - `fn from(a: Add) -> Self`
- **Deserialize**
  - `fn deserialize<__D>(__deserializer: __D) -> _serde::__private228::Result<Self, <__D as >::Error>`



