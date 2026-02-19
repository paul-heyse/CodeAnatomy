use datafusion_common::Result;
use datafusion_expr::planner::{RelationPlanner, RelationPlannerContext, RelationPlanning};
use datafusion_expr::sqlparser::ast::TableFactor;

/// Pilot relation planner that keeps planning on builder-native hooks.
///
/// This planner is intentionally conservative: it currently delegates all
/// relations back to DataFusion's default relation planning path while
/// exercising the relation-planner extension surface.
#[derive(Debug, Default)]
pub struct CodeAnatomyRelationPlanner;

impl RelationPlanner for CodeAnatomyRelationPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        _context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        Ok(RelationPlanning::Original(relation))
    }
}
