**arrow_ipc**

# Module: arrow_ipc

## Contents

**Modules**

- [`convert`](#convert) - Utilities for converting between IPC types and native Arrow types
- [`gen`](#gen) - Generated code
- [`reader`](#reader) - Arrow IPC File and Stream Readers
- [`writer`](#writer) - Arrow IPC File and Stream Writers

---

## Module: convert

Utilities for converting between IPC types and native Arrow types



## Module: gen

Generated code



## Module: reader

Arrow IPC File and Stream Readers

# Notes

The [`FileReader`] and [`StreamReader`] have similar interfaces,
however the [`FileReader`] expects a reader that supports [`Seek`]ing

[`Seek`]: std::io::Seek



## Module: writer

Arrow IPC File and Stream Writers

# Notes

[`FileWriter`] and [`StreamWriter`] have similar interfaces,
however the [`FileWriter`] expects a reader that supports [`Seek`]ing

[`Seek`]: std::io::Seek



