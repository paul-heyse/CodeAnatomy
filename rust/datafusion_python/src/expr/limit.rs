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

use datafusion::logical_expr::logical_plan::Limit;
use pyo3::prelude::*;

use crate::common::df_schema::PyDFSchema;
use crate::expr::logical_node::LogicalNode;
use crate::sql::logical::PyLogicalPlan;

logical_plan_wrapper!(
    wrapper = PyLimit,
    inner = Limit,
    field = limit,
    py_name = "Limit",
    display = |this, f| {
        write!(
            f,
            "Limit
            Skip: {:?}
            Fetch: {:?}
            Input: {:?}",
            &this.limit.skip, &this.limit.fetch, &this.limit.input
        )
    },
    inputs = |this| { vec![PyLogicalPlan::from((*this.limit.input).clone())] }
);

#[pymethods]
impl PyLimit {
    // NOTE: Upstream now has expressions for skip and fetch
    // TODO: Do we still want to expose these?
    // REF: https://github.com/apache/datafusion/pull/12836

    // /// Retrieves the skip value for this `Limit`
    // fn skip(&self) -> usize {
    //     self.limit.skip
    // }

    // /// Retrieves the fetch value for this `Limit`
    // fn fetch(&self) -> Option<usize> {
    //     self.limit.fetch
    // }

    /// Retrieves the input `LogicalPlan` to this `Limit` node
    fn input(&self) -> PyResult<Vec<PyLogicalPlan>> {
        Ok(Self::inputs(self))
    }

    /// Resulting Schema for this `Limit` node instance
    fn schema(&self) -> PyResult<PyDFSchema> {
        Ok(self.limit.input.schema().as_ref().clone().into())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Limit({self})"))
    }
}
