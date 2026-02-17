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

use std::sync::Arc;

use datafusion::logical_expr::DropView;
use pyo3::prelude::*;

use crate::common::df_schema::PyDFSchema;

logical_plan_wrapper!(
    wrapper = PyDropView,
    inner = DropView,
    field = drop,
    py_name = "DropView",
    display = |this, f| {
        write!(
            f,
            "DropView: {name:?} if not exist:={if_exists}",
            name = this.drop.name,
            if_exists = this.drop.if_exists
        )
    },
    inputs = |_this| { vec![] }
);

#[pymethods]
impl PyDropView {
    #[new]
    fn new(name: String, schema: PyDFSchema, if_exists: bool) -> PyResult<Self> {
        Ok(PyDropView {
            drop: DropView {
                name: name.into(),
                schema: Arc::new(schema.into()),
                if_exists,
            },
        })
    }

    fn name(&self) -> PyResult<String> {
        Ok(self.drop.name.to_string())
    }

    fn schema(&self) -> PyDFSchema {
        (*self.drop.schema).clone().into()
    }

    fn if_exists(&self) -> PyResult<bool> {
        Ok(self.drop.if_exists)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok(format!("DropView({self})"))
    }

    fn __name__(&self) -> PyResult<String> {
        Ok("DropView".to_string())
    }
}
