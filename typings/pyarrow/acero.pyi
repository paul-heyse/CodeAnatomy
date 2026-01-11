from collections.abc import Sequence

from pyarrow import RecordBatchReader, Table

class Declaration:
    def __init__(
        self,
        factory_name: str,
        options: object | None = None,
        inputs: Sequence[Declaration] | None = None,
    ) -> None: ...
    def to_table(self, *, use_threads: bool | None = None) -> Table: ...
    def to_reader(self, *, use_threads: bool | None = None) -> RecordBatchReader: ...

class TableSourceNodeOptions:
    def __init__(self, table: Table) -> None: ...

class ScanNodeOptions:
    def __init__(
        self,
        dataset: object,
        *,
        columns: object | None = None,
        filter_expr: object | None = None,
        **kwargs: object,
    ) -> None: ...

class FilterNodeOptions:
    def __init__(self, predicate: object) -> None: ...

class ProjectNodeOptions:
    def __init__(self, expressions: Sequence[object], names: Sequence[str]) -> None: ...

class OrderByNodeOptions:
    def __init__(self, *, sort_keys: Sequence[tuple[str, str]]) -> None: ...

class AggregateNodeOptions:
    def __init__(
        self,
        aggregates: Sequence[tuple[object, str, object | None, str]],
        keys: Sequence[object] | None = None,
    ) -> None: ...

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
