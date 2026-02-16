"""Tests for span field templating."""

from __future__ import annotations

import pyarrow as pa

from schema_spec.arrow_types import ArrowPrimitiveSpec
from schema_spec.span_fields import (
    SPAN_PREFIXES,
    STANDARD_SPAN_TYPES,
    make_span_field_specs,
    make_span_pa_fields,
    make_span_pa_tuples,
    span_field_names,
)
from schema_spec.specs import (
    FieldBundle,
    alias_span_bundle,
    call_span_bundle,
    def_span_bundle,
    name_span_bundle,
    prefixed_span_bundle,
    span_bundle,
    stmt_span_bundle,
)

SPAN_FIELD_COUNT = 2


class TestSpanFieldNames:
    """Tests for span_field_names function."""

    @staticmethod
    def test_empty_prefix() -> None:
        """Empty prefix returns standard field names."""
        bstart, bend = span_field_names("")
        assert bstart == "bstart"
        assert bend == "bend"

    @staticmethod
    def test_call_prefix() -> None:
        """Call prefix returns prefixed field names."""
        bstart, bend = span_field_names("call_")
        assert bstart == "call_bstart"
        assert bend == "call_bend"

    @staticmethod
    def test_def_prefix() -> None:
        """Def prefix returns prefixed field names."""
        bstart, bend = span_field_names("def_")
        assert bstart == "def_bstart"
        assert bend == "def_bend"

    @staticmethod
    def test_name_prefix() -> None:
        """Name prefix returns prefixed field names."""
        bstart, bend = span_field_names("name_")
        assert bstart == "name_bstart"
        assert bend == "name_bend"

    @staticmethod
    def test_all_known_prefixes() -> None:
        """All known prefixes produce valid field names."""
        for prefix in SPAN_PREFIXES:
            bstart, bend = span_field_names(prefix)
            expected_prefix = prefix if prefix else ""
            assert bstart == f"{expected_prefix}bstart"
            assert bend == f"{expected_prefix}bend"


class TestMakeSpanFieldSpecs:
    """Tests for make_span_field_specs function."""

    @staticmethod
    def test_empty_prefix_returns_field_specs() -> None:
        """Empty prefix returns FieldSpec with standard names."""
        bstart, bend = make_span_field_specs("")
        assert bstart.name == "bstart"
        assert bend.name == "bend"

    @staticmethod
    def test_field_specs_are_int64() -> None:
        """Field specs have int64 dtype."""
        bstart, bend = make_span_field_specs("call_")
        assert isinstance(bstart.dtype, ArrowPrimitiveSpec)
        assert isinstance(bend.dtype, ArrowPrimitiveSpec)
        assert bstart.dtype.name == "int64"
        assert bend.dtype.name == "int64"

    @staticmethod
    def test_prefixed_field_specs() -> None:
        """Prefixed spans have correct names."""
        bstart, bend = make_span_field_specs("stmt_")
        assert bstart.name == "stmt_bstart"
        assert bend.name == "stmt_bend"


class TestMakeSpanPaFields:
    """Tests for make_span_pa_fields function."""

    @staticmethod
    def test_returns_pa_fields() -> None:
        """Function returns pyarrow.Field instances."""
        bstart, bend = make_span_pa_fields("")
        assert isinstance(bstart, pa.Field)
        assert isinstance(bend, pa.Field)

    @staticmethod
    def test_pa_fields_have_correct_type() -> None:
        """PyArrow fields have int64 type."""
        bstart, bend = make_span_pa_fields("alias_")
        assert bstart.type == pa.int64()
        assert bend.type == pa.int64()

    @staticmethod
    def test_pa_fields_have_correct_names() -> None:
        """PyArrow fields have correct names."""
        bstart, bend = make_span_pa_fields("callee_")
        assert bstart.name == "callee_bstart"
        assert bend.name == "callee_bend"


class TestMakeSpanPaTuples:
    """Tests for make_span_pa_tuples function."""

    @staticmethod
    def test_returns_tuples() -> None:
        """Function returns tuple of tuples."""
        result = make_span_pa_tuples("")
        assert len(result) == SPAN_FIELD_COUNT
        assert all(isinstance(item, tuple) for item in result)

    @staticmethod
    def test_tuple_structure() -> None:
        """Each tuple contains name and dtype."""
        result = make_span_pa_tuples("owner_def_")
        (bstart_name, bstart_dtype), (bend_name, bend_dtype) = result
        assert bstart_name == "owner_def_bstart"
        assert bend_name == "owner_def_bend"
        assert bstart_dtype == pa.int64()
        assert bend_dtype == pa.int64()


class TestStandardSpanTypes:
    """Tests for STANDARD_SPAN_TYPES mapping."""

    @staticmethod
    def test_all_prefixes_have_types() -> None:
        """All span prefixes are in the types mapping."""
        for prefix in SPAN_PREFIXES:
            assert prefix in STANDARD_SPAN_TYPES

    @staticmethod
    def test_empty_prefix_is_span() -> None:
        """Empty prefix maps to 'span'."""
        assert STANDARD_SPAN_TYPES[""] == "span"

    @staticmethod
    def test_call_prefix_is_call_span() -> None:
        """Call prefix maps to 'call_span'."""
        assert STANDARD_SPAN_TYPES["call_"] == "call_span"


class TestSpanBundles:
    """Tests for span bundle functions."""

    @staticmethod
    def test_span_bundle_name() -> None:
        """span_bundle returns bundle with name 'span'."""
        bundle = span_bundle()
        assert isinstance(bundle, FieldBundle)
        assert bundle.name == "span"

    @staticmethod
    def test_span_bundle_fields() -> None:
        """span_bundle has bstart and bend fields."""
        bundle = span_bundle()
        field_names = [f.name for f in bundle.fields]
        assert field_names == ["bstart", "bend"]

    @staticmethod
    def test_call_span_bundle_name() -> None:
        """call_span_bundle returns bundle with name 'call_span'."""
        bundle = call_span_bundle()
        assert bundle.name == "call_span"

    @staticmethod
    def test_call_span_bundle_fields() -> None:
        """call_span_bundle has prefixed fields."""
        bundle = call_span_bundle()
        field_names = [f.name for f in bundle.fields]
        assert field_names == ["call_bstart", "call_bend"]

    @staticmethod
    def test_name_span_bundle() -> None:
        """name_span_bundle works correctly."""
        bundle = name_span_bundle()
        assert bundle.name == "name_span"
        assert [f.name for f in bundle.fields] == ["name_bstart", "name_bend"]

    @staticmethod
    def test_def_span_bundle() -> None:
        """def_span_bundle works correctly."""
        bundle = def_span_bundle()
        assert bundle.name == "def_span"
        assert [f.name for f in bundle.fields] == ["def_bstart", "def_bend"]

    @staticmethod
    def test_stmt_span_bundle() -> None:
        """stmt_span_bundle works correctly."""
        bundle = stmt_span_bundle()
        assert bundle.name == "stmt_span"
        assert [f.name for f in bundle.fields] == ["stmt_bstart", "stmt_bend"]

    @staticmethod
    def test_alias_span_bundle() -> None:
        """alias_span_bundle works correctly."""
        bundle = alias_span_bundle()
        assert bundle.name == "alias_span"
        assert [f.name for f in bundle.fields] == ["alias_bstart", "alias_bend"]


class TestPrefixedSpanBundle:
    """Tests for prefixed_span_bundle function."""

    @staticmethod
    def test_prefixed_call() -> None:
        """prefixed_span_bundle with 'call_' prefix."""
        bundle = prefixed_span_bundle("call_")
        assert bundle.name == "call_span"
        assert [f.name for f in bundle.fields] == ["call_bstart", "call_bend"]

    @staticmethod
    def test_prefixed_callee() -> None:
        """prefixed_span_bundle with 'callee_' prefix."""
        bundle = prefixed_span_bundle("callee_")
        assert bundle.name == "callee_span"
        assert [f.name for f in bundle.fields] == ["callee_bstart", "callee_bend"]

    @staticmethod
    def test_prefixed_container_def() -> None:
        """prefixed_span_bundle with 'container_def_' prefix."""
        bundle = prefixed_span_bundle("container_def_")
        assert bundle.name == "container_def_span"
        assert [f.name for f in bundle.fields] == ["container_def_bstart", "container_def_bend"]

    @staticmethod
    def test_unknown_prefix_fallback() -> None:
        """Unknown prefix uses fallback naming."""
        bundle = prefixed_span_bundle("custom_")
        assert bundle.name == "custom_span"
        assert [f.name for f in bundle.fields] == ["custom_bstart", "custom_bend"]
