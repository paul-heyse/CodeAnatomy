# arrow_ipc

Support for the [Arrow IPC Format]

The Arrow IPC format defines how to read and write [`RecordBatch`]es to/from
a file or stream of bytes. This format can be used to serialize and deserialize
data to files and over the network.

There are two variants of the IPC format:
1. [IPC Streaming Format]: Supports streaming data sources, implemented by
   [StreamReader] and [StreamWriter]

2. [IPC File Format]: Supports random access, implemented by [FileReader] and
   [FileWriter].

See the [`reader`] and [`writer`] modules for more information.

[Arrow IPC Format]: https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc
[IPC Streaming Format]: https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
[StreamReader]: reader::StreamReader
[StreamWriter]: writer::StreamWriter
[IPC File Format]: https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format
[FileReader]: reader::FileReader
[FileWriter]: writer::FileWriter

## Modules

### [`arrow_ipc`](arrow_ipc.md)

*4 modules*

### [`compression`](compression.md)

*1 struct*

### [`convert`](convert.md)

*2 structs, 5 functions*

### [`gen`](gen.md)

*5 modules*

### [`gen::File`](gen/File.md)

*1 enum, 4 structs, 8 functions*

### [`gen::Message`](gen/Message.md)

*17 structs, 4 enums, 8 functions, 9 constants*

### [`gen::Schema`](gen/Schema.md)

*102 structs, 30 constants, 30 enums, 8 functions*

### [`gen::SparseTensor`](gen/SparseTensor.md)

*15 structs, 4 enums, 6 constants, 8 functions*

### [`gen::Tensor`](gen/Tensor.md)

*2 enums, 6 structs, 8 functions*

### [`reader`](reader.md)

*3 functions, 5 structs*

### [`reader::stream`](reader/stream.md)

*1 struct*

### [`writer`](writer.md)

*1 function, 2 enums, 6 structs*

