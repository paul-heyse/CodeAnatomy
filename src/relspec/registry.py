from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

from ..arrowdsl.contracts import Contract
from .model import RelationshipRule


PathLike = Union[str, Path]


@dataclass(frozen=True)
class DatasetLocation:
    """
    Where a dataset lives (filesystem-centric default).

    This is intentionally light-weight; storage/format specifics can be extended later.
    """
    path: PathLike
    format: str = "parquet"
    partitioning: Optional[str] = "hive"
    filesystem: object = None  # fsspec/pyarrow.fs filesystem, optional


class DatasetCatalog:
    """
    Maps dataset names to locations (for FilesystemPlanResolver).
    """
    def __init__(self) -> None:
        self._locs: Dict[str, DatasetLocation] = {}

    def register(self, name: str, location: DatasetLocation) -> None:
        if not name:
            raise ValueError("DatasetCatalog.register: name must be non-empty")
        self._locs[name] = location

    def get(self, name: str) -> DatasetLocation:
        if name not in self._locs:
            raise KeyError(f"DatasetCatalog: unknown dataset {name!r}")
        return self._locs[name]

    def has(self, name: str) -> bool:
        return name in self._locs

    def names(self) -> List[str]:
        return sorted(self._locs.keys())


class ContractCatalog:
    """
    Maps contract names to arrowdsl.Contract objects.
    """
    def __init__(self) -> None:
        self._contracts: Dict[str, Contract] = {}

    def register(self, contract: Contract) -> None:
        self._contracts[contract.name] = contract

    def get(self, name: str) -> Contract:
        if name not in self._contracts:
            raise KeyError(f"ContractCatalog: unknown contract {name!r}")
        return self._contracts[name]

    def has(self, name: str) -> bool:
        return name in self._contracts

    def names(self) -> List[str]:
        return sorted(self._contracts.keys())


class RelationshipRegistry:
    """
    Holds RelationshipRule objects and provides grouping utilities.

    Note: it is valid for multiple rules to share the same output_dataset,
    but if you do that you should rely on:
      - output contract dedupe keys + tie-breakers (including rule_priority)
      - OR introduce an explicit UNION_ALL rule.
    """
    def __init__(self) -> None:
        self._rules_by_name: Dict[str, RelationshipRule] = {}

    def add(self, rule: RelationshipRule) -> None:
        rule.validate()
        if rule.name in self._rules_by_name:
            raise ValueError(f"Duplicate rule name: {rule.name!r}")
        self._rules_by_name[rule.name] = rule

    def extend(self, rules: Iterable[RelationshipRule]) -> None:
        for r in rules:
            self.add(r)

    def get(self, name: str) -> RelationshipRule:
        return self._rules_by_name[name]

    def rules(self) -> List[RelationshipRule]:
        return [self._rules_by_name[n] for n in sorted(self._rules_by_name.keys())]

    def by_output(self) -> Dict[str, List[RelationshipRule]]:
        out: Dict[str, List[RelationshipRule]] = {}
        for r in self._rules_by_name.values():
            out.setdefault(r.output_dataset, []).append(r)
        # deterministic ordering
        for k in list(out.keys()):
            out[k] = sorted(out[k], key=lambda rr: (rr.priority, rr.name))
        return out

    def outputs(self) -> List[str]:
        return sorted({r.output_dataset for r in self._rules_by_name.values()})

    def inputs(self) -> List[str]:
        s = set()
        for r in self._rules_by_name.values():
            for dref in r.inputs:
                s.add(dref.name)
        return sorted(s)

    def validate_contract_consistency(self) -> None:
        """
        Enforce that if multiple rules produce the same output_dataset, they declare the same contract_name.
        """
        by_out = self.by_output()
        for out_name, rules in by_out.items():
            contracts = {r.contract_name for r in rules}
            if len(contracts) > 1:
                raise ValueError(
                    f"Output {out_name!r} has inconsistent contract_name across rules: {sorted(map(str, contracts))}"
                )
