// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use datafusion::logical_expr::logical_plan::Union;
use pyo3::prelude::*;

use crate::common::df_schema::PyDFSchema;
use crate::expr::logical_node::LogicalNode;
use crate::sql::logical::PyLogicalPlan;

logical_plan_wrapper!(
    wrapper = PyUnion,
    inner = Union,
    field = union_,
    py_name = "Union",
    display = |this, f| {
        write!(
            f,
            "Union
            Inputs: {:?}
            Schema: {:?}",
            &this.union_.inputs, &this.union_.schema,
        )
    },
    inputs = |this| {
        this.union_
            .inputs
            .iter()
            .map(|x| x.as_ref().clone().into())
            .collect()
    }
);

#[pymethods]
impl PyUnion {
    /// Retrieves the input `LogicalPlan` to this `Union` node
    fn input(&self) -> PyResult<Vec<PyLogicalPlan>> {
        Ok(Self::inputs(self))
    }

    /// Resulting Schema for this `Union` node instance
    fn schema(&self) -> PyResult<PyDFSchema> {
        Ok(self.union_.schema.as_ref().clone().into())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Union({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("Union".to_string())
    }
}
