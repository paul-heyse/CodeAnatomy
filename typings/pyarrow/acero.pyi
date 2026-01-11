from collections.abc import Sequence

import pyarrow as pa
import pyarrow.compute as pc

class Declaration:
    def __init__(
        self,
        factory_name: str,
        options: object,
        *,
        inputs: Sequence[Declaration] | None = None,
    ) -> None: ...
    def to_reader(self, *, use_threads: bool = True) -> pa.RecordBatchReader: ...
    def to_table(self, *, use_threads: bool = True) -> pa.Table: ...

class ScanNodeOptions:
    def __init__(
        self,
        dataset: object,
        *,
        columns: object | None = None,
        **kwargs: object,
    ) -> None: ...

class TableSourceNodeOptions:
    def __init__(self, table: pa.Table) -> None: ...

class FilterNodeOptions:
    def __init__(self, predicate: pc.Expression) -> None: ...

class ProjectNodeOptions:
    def __init__(self, expressions: Sequence[pc.Expression], names: Sequence[str]) -> None: ...

class OrderByNodeOptions:
    def __init__(self, *, sort_keys: Sequence[object]) -> None: ...

class HashJoinNodeOptions:
    def __init__(
        self,
        *,
        join_type: str,
        left_keys: Sequence[str],
        right_keys: Sequence[str],
        left_output: Sequence[str],
        right_output: Sequence[str],
        output_suffix_for_left: str = "",
        output_suffix_for_right: str = "",
    ) -> None: ...
