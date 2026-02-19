use datafusion_expr::planner::TypePlanner;

#[derive(Debug, Default)]
pub struct CodeAnatomyTypePlanner;

impl TypePlanner for CodeAnatomyTypePlanner {}
