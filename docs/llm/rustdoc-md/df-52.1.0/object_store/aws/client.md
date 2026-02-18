**object_store > aws > client**

# Module: aws::client

## Contents

**Enums**

- [`RequestError`](#requesterror)

---

## object_store::aws::client::RequestError

*Enum*

**Variants:**
- `Generic{ source: crate::Error }`
- `Retry{ source: crate::client::retry::RetryError, path: String }`



