use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FunctionKind {
    Scalar,
    Aggregate,
    Window,
    Table,
}
