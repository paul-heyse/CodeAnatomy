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

use datafusion::logical_expr::logical_plan::Sort;
use pyo3::prelude::*;

use crate::common::df_schema::PyDFSchema;
use crate::expr::logical_node::LogicalNode;
use crate::expr::sort_expr::PySortExpr;
use crate::sql::logical::PyLogicalPlan;

logical_plan_wrapper!(
    wrapper = PySort,
    inner = Sort,
    field = sort,
    py_name = "Sort",
    display = |this, f| {
        write!(
            f,
            "Sort
            \nExpr(s): {:?}
            \nInput: {:?}
            \nSchema: {:?}",
            &this.sort.expr,
            this.sort.input,
            this.sort.input.schema()
        )
    },
    inputs = |this| { vec![PyLogicalPlan::from((*this.sort.input).clone())] }
);

#[pymethods]
impl PySort {
    /// Retrieves the sort expressions for this `Sort`
    fn sort_exprs(&self) -> PyResult<Vec<PySortExpr>> {
        Ok(self
            .sort
            .expr
            .iter()
            .map(|e| PySortExpr::from(e.clone()))
            .collect())
    }

    fn get_fetch_val(&self) -> PyResult<Option<usize>> {
        Ok(self.sort.fetch)
    }

    /// Retrieves the input `LogicalPlan` to this `Sort` node
    fn input(&self) -> PyResult<Vec<PyLogicalPlan>> {
        Ok(Self::inputs(self))
    }

    /// Resulting Schema for this `Sort` node instance
    fn schema(&self) -> PyDFSchema {
        self.sort.input.schema().as_ref().clone().into()
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("Sort({self})"))
    }
}
