# DataFusion Displacement Mapping

## ExprIR calls in current rules

- total ExprIR call names: 11

### DataFusion built-in / Python API functions
- coalesce
- concat

### DataFusion UDF (Python) targets
- stable_hash128

### DataFusion SQL expressions/operators
- bit_wise_and -> AND
- equal -> =
- fill_null -> COALESCE(value, fill_value)
- if_else -> CASE WHEN ... THEN ... ELSE ... END
- invert -> NOT
- is_null -> IS NULL
- not_equal -> !=
- stringify -> CAST(... AS STRING)

### Missing from DataFusion

## Plan operations (ArrowDSL catalog)

- aggregate: acero, datafusion | DataFusion: GROUP BY
- explode_list: kernel | DataFusion: UNNEST (plus index via range)
- filter: acero, datafusion | DataFusion: WHERE
- hash_join: acero | DataFusion: JOIN
- order_by: acero, datafusion, kernel | DataFusion: ORDER BY
- project: acero, datafusion | DataFusion: SELECT
- scan: acero, datafusion | DataFusion: FROM <table> (scan)
- table_source: acero | DataFusion: FROM <registered table>
- union_all: acero | DataFusion: UNION ALL
- winner_select: acero | DataFusion: ROW_NUMBER() OVER (...) + filter

## Ibis UDF specs

- none (DataFusion-native registry only)
