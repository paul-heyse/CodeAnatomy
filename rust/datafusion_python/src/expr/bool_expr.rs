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

use pyo3::prelude::*;

unary_bool_expr_wrapper!(PyNot, "Not");
unary_bool_expr_wrapper!(PyIsNotNull, "IsNotNull");
unary_bool_expr_wrapper!(PyIsNull, "IsNull");
unary_bool_expr_wrapper!(PyIsTrue, "IsTrue");
unary_bool_expr_wrapper!(PyIsFalse, "IsFalse");
unary_bool_expr_wrapper!(PyIsUnknown, "IsUnknown");
unary_bool_expr_wrapper!(PyIsNotTrue, "IsNotTrue");
unary_bool_expr_wrapper!(PyIsNotFalse, "IsNotFalse");
unary_bool_expr_wrapper!(PyIsNotUnknown, "IsNotUnknown");
unary_bool_expr_wrapper!(PyNegative, "Negative");
