**datafusion_ffi > util**

# Module: util

## Contents

**Functions**

- [`rvec_wrapped_to_vec_datatype`](#rvec_wrapped_to_vec_datatype) - This is a utility function to convert an FFI friendly vector of [`WrappedSchema`]
- [`rvec_wrapped_to_vec_fieldref`](#rvec_wrapped_to_vec_fieldref) - This is a utility function to convert an FFI friendly vector of [`WrappedSchema`]
- [`vec_datatype_to_rvec_wrapped`](#vec_datatype_to_rvec_wrapped) - This is a utility function to convert a slice of [`DataType`] to its equivalent
- [`vec_fieldref_to_rvec_wrapped`](#vec_fieldref_to_rvec_wrapped) - This is a utility function to convert a slice of [`Field`] to its equivalent

**Type Aliases**

- [`FFIResult`](#ffiresult) - Convenience type for results passed through the FFI boundary. Since the

---

## datafusion_ffi::util::FFIResult

*Type Alias*: `abi_stable::std_types::RResult<T, abi_stable::std_types::RString>`

Convenience type for results passed through the FFI boundary. Since the
`DataFusionError` enum is complex and little value is gained from creating
a FFI safe variant of it, we convert errors to strings when passing results
back. These are converted back and forth using the `df_result`, `rresult`,
and `rresult_return` macros.



## datafusion_ffi::util::rvec_wrapped_to_vec_datatype

*Function*

This is a utility function to convert an FFI friendly vector of [`WrappedSchema`]
to their equivalent [`DataType`].

```rust
fn rvec_wrapped_to_vec_datatype(data_types: &abi_stable::std_types::RVec<crate::arrow_wrappers::WrappedSchema>) -> Result<Vec<arrow::datatypes::DataType>, arrow::error::ArrowError>
```



## datafusion_ffi::util::rvec_wrapped_to_vec_fieldref

*Function*

This is a utility function to convert an FFI friendly vector of [`WrappedSchema`]
to their equivalent [`Field`].

```rust
fn rvec_wrapped_to_vec_fieldref(fields: &abi_stable::std_types::RVec<crate::arrow_wrappers::WrappedSchema>) -> Result<Vec<arrow_schema::FieldRef>, arrow::error::ArrowError>
```



## datafusion_ffi::util::vec_datatype_to_rvec_wrapped

*Function*

This is a utility function to convert a slice of [`DataType`] to its equivalent
FFI friendly counterpart, [`WrappedSchema`]

```rust
fn vec_datatype_to_rvec_wrapped(data_types: &[arrow::datatypes::DataType]) -> Result<abi_stable::std_types::RVec<crate::arrow_wrappers::WrappedSchema>, arrow::error::ArrowError>
```



## datafusion_ffi::util::vec_fieldref_to_rvec_wrapped

*Function*

This is a utility function to convert a slice of [`Field`] to its equivalent
FFI friendly counterpart, [`WrappedSchema`]

```rust
fn vec_fieldref_to_rvec_wrapped(fields: &[arrow_schema::FieldRef]) -> Result<abi_stable::std_types::RVec<crate::arrow_wrappers::WrappedSchema>, arrow::error::ArrowError>
```



