use datafusion_common::{DataFusionError, Result};
use datafusion_expr::planner::{RelationPlanner, RelationPlannerContext, RelationPlanning};
use datafusion_expr::sqlparser::ast::TableFactor;

/// Pilot relation planner that keeps planning on builder-native hooks.
///
/// This planner is intentionally conservative: it currently delegates all
/// relations back to DataFusion's default relation planning path while
/// exercising the relation-planner extension surface.
#[derive(Debug, Default)]
pub struct CodeAnatomyRelationPlanner;

pub fn supports_codeanatomy_relation(relation: &TableFactor) -> bool {
    match relation {
        TableFactor::Table { name, .. } => {
            let relation_name = name.to_string().to_ascii_lowercase();
            relation_name.starts_with("codeanatomy.")
                || relation_name.starts_with("__codeanatomy_")
        }
        _ => false,
    }
}

impl RelationPlanner for CodeAnatomyRelationPlanner {
    fn plan_relation(
        &self,
        relation: TableFactor,
        _context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        if supports_codeanatomy_relation(&relation) {
            return Err(DataFusionError::Plan(
                "CodeAnatomy relation planning is enabled but no relation handler is configured."
                    .to_string(),
            ));
        }
        Ok(RelationPlanning::Original(relation))
    }
}
