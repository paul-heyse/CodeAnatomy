**datafusion_ffi**

# Module: datafusion_ffi

## Contents

**Modules**

- [`arrow_wrappers`](#arrow_wrappers)
- [`catalog_provider`](#catalog_provider)
- [`catalog_provider_list`](#catalog_provider_list)
- [`execution`](#execution)
- [`execution_plan`](#execution_plan)
- [`expr`](#expr)
- [`insert_op`](#insert_op)
- [`physical_expr`](#physical_expr)
- [`plan_properties`](#plan_properties)
- [`proto`](#proto)
- [`record_batch_stream`](#record_batch_stream)
- [`schema_provider`](#schema_provider)
- [`session`](#session)
- [`table_provider`](#table_provider)
- [`table_source`](#table_source)
- [`udaf`](#udaf)
- [`udf`](#udf)
- [`udtf`](#udtf)
- [`udwf`](#udwf)
- [`util`](#util)
- [`volatility`](#volatility)

**Macros**

- [`df_result`](#df_result) - This macro is a helpful conversion utility to convert from an abi_stable::RResult to a
- [`rresult`](#rresult) - This macro is a helpful conversion utility to convert from a DataFusion Result to an abi_stable::RResult
- [`rresult_return`](#rresult_return) - This macro is a helpful conversion utility to convert from a DataFusion Result to an abi_stable::RResult

**Functions**

- [`get_library_marker_id`](#get_library_marker_id) - This utility is used to determine if two FFI structs are within
- [`version`](#version) - Returns the major version of the FFI implementation. If the API evolves,

---

## Module: arrow_wrappers



## Module: catalog_provider



## Module: catalog_provider_list



## datafusion_ffi::df_result

*Declarative Macro*

This macro is a helpful conversion utility to convert from an abi_stable::RResult to a
DataFusion result.

```rust
macro_rules! df_result {
    ( $x:expr ) => { ... };
}
```



## Module: execution



## Module: execution_plan



## Module: expr



## datafusion_ffi::get_library_marker_id

*Function*

This utility is used to determine if two FFI structs are within
the same library. It is possible that the interplay between
foreign and local functions calls create one FFI struct that
references another. It is helpful to determine if a foreign
struct in the same library or called from a different one.
If we are in the same library, then we can access the underlying
types directly.

This function works by checking the address of the library
marker. Each library that implements the FFI code will have
a different address for the marker. By checking the marker
address we can determine if a struct is truly foreign or is
actually within the same originating library.

See the crate's `README.md` for additional information.

```rust
fn get_library_marker_id() -> usize
```



## Module: insert_op



## Module: physical_expr



## Module: plan_properties



## Module: proto



## Module: record_batch_stream



## datafusion_ffi::rresult

*Declarative Macro*

This macro is a helpful conversion utility to convert from a DataFusion Result to an abi_stable::RResult

```rust
macro_rules! rresult {
    ( $x:expr ) => { ... };
}
```



## datafusion_ffi::rresult_return

*Declarative Macro*

This macro is a helpful conversion utility to convert from a DataFusion Result to an abi_stable::RResult
and to also call return when it is an error. Since you cannot use `?` on an RResult, this is designed
to mimic the pattern.

```rust
macro_rules! rresult_return {
    ( $x:expr ) => { ... };
}
```



## Module: schema_provider



## Module: session



## Module: table_provider



## Module: table_source



## Module: udaf



## Module: udf



## Module: udtf



## Module: udwf



## Module: util



## datafusion_ffi::version

*Function*

Returns the major version of the FFI implementation. If the API evolves,
we use the major version to identify compatibility over the unsafe
boundary. This call is intended to be used by implementers to validate
they have compatible libraries.

```rust
fn version() -> u64
```



## Module: volatility



