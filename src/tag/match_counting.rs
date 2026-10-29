use crate::error::Error;
use crate::model::ReconcileTagMatchingCountsTask;
use crate::query::compiler::ast::{
    Node, PostTagNode, QueryBuilderVisitor, SemanticAnalysisVisitor,
};
use crate::query::compiler::dict::Scope;
use crate::query::compiler::{Location, Log};
use crate::query::{QueryParametersFilter, compiler, prepare_query_parameters};
use crate::run_serializable_transaction;
use crate::schema::{reconcile_tag_matching_counts_task, tag};
use diesel::sql_types::{Array, BigInt, Nullable};
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl};
use diesel_async::{AsyncPgConnection, RunQueryDsl};
use itertools::Itertools;

pub fn compile_tag_match_count_sql(tag_name: String, scope: &Scope) -> Result<String, Error> {
    let query_parameters_filter = QueryParametersFilter::default();
    let mut query_parameters = prepare_query_parameters(&query_parameters_filter, &None, scope)?;
    query_parameters.privileged = true;
    query_parameters.select_statements = vec![String::from("COUNT(*)")];

    let mut root_node = compiler::create_root_node_for_expression(Some(Box::new(Node {
        location: Location::default(),
        node_type: PostTagNode {
            identifier: tag_name,
        },
    })));

    let mut log = Log { errors: Vec::new() };
    let mut semantic_analysis_visitor = SemanticAnalysisVisitor::new();
    let mut query_builder_visitor = QueryBuilderVisitor::new(&mut query_parameters);

    root_node.accept(&mut semantic_analysis_visitor, scope, &mut log);
    if !log.errors.is_empty() {
        return Err(Error::QueryCompilationError(
            String::from("semantic analysis"),
            log.errors,
        ));
    }

    root_node.accept(&mut query_builder_visitor, scope, &mut log);
    if !log.errors.is_empty() {
        return Err(Error::QueryCompilationError(
            String::from("query builder"),
            log.errors,
        ));
    }

    let ctes = query_builder_visitor
        .ctes
        .into_values()
        .sorted_by_key(|cte| cte.idx)
        .map(|cte| cte.expression)
        .collect::<Vec<_>>();
    let where_expressions = query_builder_visitor.where_expressions;
    let from_table_name = query_parameters
        .from_table_override
        .unwrap_or(query_parameters.base_table_name);

    let mut sql_query = String::new();

    compiler::apply_ctes(&mut sql_query, &ctes)?;

    sql_query.push_str(" SELECT COUNT(*) AS matching_count FROM ");
    sql_query.push_str(from_table_name);

    if !query_parameters.join_statements.is_empty() {
        sql_query.push(' ');
        sql_query.push_str(&query_parameters.build_join_statements(true));
    }

    compiler::apply_where_conditions(&mut sql_query, &where_expressions, &query_parameters);

    Ok(sql_query)
}

#[derive(QueryableByName)]
struct MatchingCount {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    matching_count: i64,
}

#[derive(QueryableByName)]
struct AliasGroupMember {
    #[diesel(sql_type = diesel::sql_types::BigInt)]
    pk: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileTagMatchingCountsOutcome {
    Completed,
    Requeued,
    Discarded,
}

/// Reconciles the dirty matching counts for the alias group of the task's tag.
///
/// Only counts marked for reconciliation are calculated. The results are then
/// published to all members of the resolved alias group in a serializable
/// transaction, provided the matching topology has not changed.
///
/// Concurrent invalidations are handled according to their type:
/// - Usage changes do not prevent publication, but leave the task pending for
///   another reconciliation.
/// - Alias, hierarchy or tag name changes invalidate the calculated results,
///   which are discarded without updating any counts.
///
/// Returns:
/// - `Completed` if the counts were published and the task was deleted.
/// - `Requeued` if the counts were published, but subsequent usage changes
///   require another reconciliation.
/// - `Discarded` if the task no longer exists or its topology version changed
///   during calculation.
///
/// Retained tasks are unlocked by the caller's task sentinel and can be
/// processed again during a subsequent scheduler invocation.
pub async fn run_reconcile_tag_matching_counts_task(
    task: &ReconcileTagMatchingCountsTask,
    connection: &mut AsyncPgConnection,
) -> Result<ReconcileTagMatchingCountsOutcome, Error> {
    let Some(tag_name) = tag::table
        .filter(tag::pk.eq(task.fk_tag))
        .select(tag::tag_name)
        .first::<String>(connection)
        .await
        .optional()?
    else {
        // A deleted tag cascades to its task row.
        return Ok(ReconcileTagMatchingCountsOutcome::Discarded);
    };

    let tag_name = tag_name.to_lowercase();

    // Alias synchronisation maintains complete equivalence classes.
    // Every member is directly related to task.fk_tag.
    let alias_group_pks = diesel::sql_query(
        "SELECT $1::BIGINT AS pk
         UNION
         SELECT fk_target AS pk FROM tag_alias WHERE fk_source = $1
         UNION
         SELECT fk_source AS pk FROM tag_alias WHERE fk_target = $1
         ORDER BY pk",
    )
    .bind::<BigInt, _>(task.fk_tag)
    .load::<AliasGroupMember>(connection)
    .await?
    .into_iter()
    .map(|member| member.pk)
    .collect::<Vec<_>>();

    // Usage changes increment invalidation_version, while structural changes
    // also increment topology_version. Counts can be published after usage
    // invalidations, but must be discarded after structural changes.
    let post_count = if task.reconcile_post_count {
        Some(
            diesel::sql_query(compile_tag_match_count_sql(tag_name.clone(), &Scope::Post)?)
                .get_result::<MatchingCount>(connection)
                .await?
                .matching_count,
        )
    } else {
        None
    };

    let post_collection_count = if task.reconcile_post_collection_count {
        Some(
            diesel::sql_query(compile_tag_match_count_sql(
                tag_name.clone(),
                &Scope::Collection,
            )?)
            .get_result::<MatchingCount>(connection)
            .await?
            .matching_count,
        )
    } else {
        None
    };

    let user_group_count = if task.reconcile_user_group_count {
        Some(
            diesel::sql_query(compile_tag_match_count_sql(tag_name, &Scope::UserGroup)?)
                .get_result::<MatchingCount>(connection)
                .await?
                .matching_count,
        )
    } else {
        None
    };

    run_serializable_transaction(connection, async |connection| {
        let current_versions = reconcile_tag_matching_counts_task::table
            .filter(reconcile_tag_matching_counts_task::pk.eq(task.pk))
            .filter(reconcile_tag_matching_counts_task::locked_at.is_not_null())
            .select((
                reconcile_tag_matching_counts_task::invalidation_version,
                reconcile_tag_matching_counts_task::topology_version,
            ))
            .for_update()
            .first::<(i64, i64)>(connection)
            .await
            .optional()?;

        let Some((invalidation_version, topology_version)) = current_versions else {
            return Ok(ReconcileTagMatchingCountsOutcome::Discarded);
        };

        // The matching semantics changed. The calculated results may belong to an obsolete alias group or hierarchy, so discard them.
        if topology_version != task.topology_version {
            return Ok(ReconcileTagMatchingCountsOutcome::Discarded);
        }

        // Ordinary usage changes do not prevent publication. The resulting counts may be slightly stale, but are still useful.
        let alias_group_pks = alias_group_pks.clone();

        diesel::sql_query(
            "UPDATE tag
            SET matching_post_count = COALESCE($2, matching_post_count),
                matching_post_collection_count = COALESCE($3, matching_post_collection_count),
                matching_user_group_count = COALESCE($4, matching_user_group_count)
            WHERE pk = ANY($1)",
        )
        .bind::<Array<BigInt>, _>(&alias_group_pks)
        .bind::<Nullable<BigInt>, _>(post_count)
        .bind::<Nullable<BigInt>, _>(post_collection_count)
        .bind::<Nullable<BigInt>, _>(user_group_count)
        .execute(connection)
        .await?;

        if invalidation_version == task.invalidation_version {
            // No changes during calculation. Everything is up to date.
            diesel::delete(reconcile_tag_matching_counts_task::table)
                .filter(reconcile_tag_matching_counts_task::pk.eq(task.pk))
                .execute(connection)
                .await?;

            Ok(ReconcileTagMatchingCountsOutcome::Completed)
        } else {
            // Only usage changes occurred. Publish the calculated counts, but keep the task and its dirty flags for another run.
            diesel::update(reconcile_tag_matching_counts_task::table)
                .filter(reconcile_tag_matching_counts_task::pk.eq(task.pk))
                .set(reconcile_tag_matching_counts_task::fail_count.eq(0))
                .execute(connection)
                .await?;

            Ok(ReconcileTagMatchingCountsOutcome::Requeued)
        }
    })
    .await
}
