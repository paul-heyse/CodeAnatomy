**datafusion_datasource > display**

# Module: display

## Contents

**Structs**

- [`FileGroupDisplay`](#filegroupdisplay) - A wrapper to customize partitioned group of files display

---

## datafusion_datasource::display::FileGroupDisplay

*Struct*

A wrapper to customize partitioned group of files display

Prints in the format:
```text
[file1, file2,...]
```

**Generic Parameters:**
- 'a

**Tuple Struct**: `(&'a crate::file_groups::FileGroup)`

**Trait Implementations:**

- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`
- **DisplayAs**
  - `fn fmt_as(self: &Self, t: DisplayFormatType, f: & mut Formatter) -> FmtResult`



