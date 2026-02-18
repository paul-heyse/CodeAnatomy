**datafusion_expr > registry**

# Module: registry

## Contents

**Structs**

- [`MemoryFunctionRegistry`](#memoryfunctionregistry) - A  [`FunctionRegistry`] that uses in memory [`HashMap`]s

**Traits**

- [`FunctionRegistry`](#functionregistry) - A registry knows how to build logical expressions out of user-defined function' names
- [`SerializerRegistry`](#serializerregistry) - Serializer and deserializer registry for extensions like [UserDefinedLogicalNode].

---

## datafusion_expr::registry::FunctionRegistry

*Trait*

A registry knows how to build logical expressions out of user-defined function' names

**Methods:**

- `udfs`: Returns names of all available scalar user defined functions.
- `udafs`: Returns names of all available aggregate user defined functions.
- `udwfs`: Returns names of all available window user defined functions.
- `udf`: Returns a reference to the user defined scalar function (udf) named
- `udaf`: Returns a reference to the user defined aggregate function (udaf) named
- `udwf`: Returns a reference to the user defined window function (udwf) named
- `register_udf`: Registers a new [`ScalarUDF`], returning any previously registered
- `register_udaf`: Registers a new [`AggregateUDF`], returning any previously registered
- `register_udwf`: Registers a new [`WindowUDF`], returning any previously registered
- `deregister_udf`: Deregisters a [`ScalarUDF`], returning the implementation that was
- `deregister_udaf`: Deregisters a [`AggregateUDF`], returning the implementation that was
- `deregister_udwf`: Deregisters a [`WindowUDF`], returning the implementation that was
- `register_function_rewrite`: Registers a new [`FunctionRewrite`] with the registry.
- `expr_planners`: Set of all registered [`ExprPlanner`]s
- `register_expr_planner`: Registers a new [`ExprPlanner`] with the registry.



## datafusion_expr::registry::MemoryFunctionRegistry

*Struct*

A  [`FunctionRegistry`] that uses in memory [`HashMap`]s

**Methods:**

- `fn new() -> Self`

**Trait Implementations:**

- **FunctionRegistry**
  - `fn udfs(self: &Self) -> HashSet<String>`
  - `fn udf(self: &Self, name: &str) -> Result<Arc<ScalarUDF>>`
  - `fn udaf(self: &Self, name: &str) -> Result<Arc<AggregateUDF>>`
  - `fn udwf(self: &Self, name: &str) -> Result<Arc<WindowUDF>>`
  - `fn register_udf(self: & mut Self, udf: Arc<ScalarUDF>) -> Result<Option<Arc<ScalarUDF>>>`
  - `fn register_udaf(self: & mut Self, udaf: Arc<AggregateUDF>) -> Result<Option<Arc<AggregateUDF>>>`
  - `fn register_udwf(self: & mut Self, udaf: Arc<WindowUDF>) -> Result<Option<Arc<WindowUDF>>>`
  - `fn expr_planners(self: &Self) -> Vec<Arc<dyn ExprPlanner>>`
  - `fn udafs(self: &Self) -> HashSet<String>`
  - `fn udwfs(self: &Self) -> HashSet<String>`
- **Default**
  - `fn default() -> MemoryFunctionRegistry`
- **Debug**
  - `fn fmt(self: &Self, f: & mut $crate::fmt::Formatter) -> $crate::fmt::Result`



## datafusion_expr::registry::SerializerRegistry

*Trait*

Serializer and deserializer registry for extensions like [UserDefinedLogicalNode].

**Methods:**

- `serialize_logical_plan`: Serialize this node to a byte array. This serialization should not include
- `deserialize_logical_plan`: Deserialize user defined logical plan node ([UserDefinedLogicalNode]) from



