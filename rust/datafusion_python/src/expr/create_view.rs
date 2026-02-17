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

use datafusion::logical_expr::{CreateView, DdlStatement, LogicalPlan};
use pyo3::prelude::*;

use super::logical_node::LogicalNode;
use crate::errors::py_type_err;
use crate::sql::logical::PyLogicalPlan;

logical_plan_wrapper!(
    wrapper = PyCreateView,
    inner = CreateView,
    field = create,
    py_name = "CreateView",
    display = |this, f| {
        write!(
            f,
            "CreateView
            name: {:?}
            input: {:?}
            or_replace: {:?}
            definition: {:?}",
            &this.create.name, &this.create.input, &this.create.or_replace, &this.create.definition,
        )
    },
    inputs = |this| { vec![PyLogicalPlan::from((*this.create.input).clone())] }
);

#[pymethods]
impl PyCreateView {
    fn name(&self) -> PyResult<String> {
        Ok(self.create.name.to_string())
    }

    fn input(&self) -> PyResult<Vec<PyLogicalPlan>> {
        Ok(Self::inputs(self))
    }

    fn or_replace(&self) -> bool {
        self.create.or_replace
    }

    fn definition(&self) -> PyResult<Option<String>> {
        Ok(self.create.definition.clone())
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("CreateView({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("CreateView".to_string())
    }
}
impl TryFrom<LogicalPlan> for PyCreateView {
    type Error = PyErr;

    fn try_from(logical_plan: LogicalPlan) -> Result<Self, Self::Error> {
        match logical_plan {
            LogicalPlan::Ddl(DdlStatement::CreateView(create)) => Ok(PyCreateView { create }),
            _ => Err(py_type_err("unexpected plan")),
        }
    }
}
