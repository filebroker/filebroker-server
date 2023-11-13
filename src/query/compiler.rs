use serde::Serialize;

use lexer::Lexer;
use std::{collections::HashMap, fmt};

use self::{
    ast::{Node, QueryBuilderVisitor, QueryNode, SemanticAnalysisVisitor},
    dict::Scope,
    parser::Parser,
};

use super::QueryParameters;

use crate::{
    model::User,
    perms,
    query::{Direction, Ordering, DEFAULT_LIMIT_STR, MAX_SHUFFLE_LIMIT_STR},
};

pub mod ast;
pub mod dict;
pub mod lexer;
pub mod parser;

pub const INTEGER_LIMIT: u32 = 1 << 31;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct Location {
    pub start: usize,
    pub end: usize,
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.start, self.end)
    }
}

#[derive(Default)]
pub struct Log {
    pub errors: Vec<Error>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Error {
    pub location: Location,
    pub msg: String,
}

pub struct Cte {
    pub idx: usize,
    pub expression: String,
}

pub fn compile_sql(
    query: Option<String>,
    mut query_parameters: QueryParameters,
    scope: &Scope,
    user: &Option<User>,
) -> Result<String, crate::Error> {
    let (source_query, instant) = if log::log_enabled!(log::Level::Debug) {
        (query.clone(), Some(std::time::Instant::now()))
    } else {
        (None, None)
    };

    let (ctes, mut where_expressions) = if let Some(query) = query {
        compile_expressions(query, &mut query_parameters, scope)?
    } else {
        (HashMap::new(), Vec::new())
    };

    let mut sql_query = String::new();

    apply_ctes(&mut sql_query, &ctes)?;

    if ctes.is_empty() {
        sql_query.push_str("WITH ");
    } else {
        sql_query.push_str(", ");
    }

    let from_table_name = query_parameters
        .from_table_override
        .unwrap_or(query_parameters.base_table_name);
    if !query_parameters.shuffle {
        // Only get a full count of the result set if the number of results is below 100000, the count query that
        // checks if there are more than 100000 results does not apply the post permission conditions to speed up
        // the query, that means the effective result size may be smaller.
        sql_query.push_str("countCte AS (SELECT CASE WHEN (SELECT COUNT(*) FROM (SELECT ");
        sql_query.push_str(query_parameters.base_table_name);
        sql_query.push_str(".pk FROM ");
        sql_query.push_str(from_table_name);
        if !query_parameters.join_statements.is_empty() {
            sql_query.push(' ');
            sql_query.push_str(&query_parameters.join_statements.join(" "));
        }
        apply_where_conditions(&mut sql_query, &mut where_expressions, &query_parameters);
        sql_query
            .push_str(" LIMIT 100000) limitedPks) < 100000 THEN (SELECT COUNT(*) FROM (SELECT ");
        sql_query.push_str(query_parameters.base_table_name);
        sql_query.push_str(".pk FROM ");
        sql_query.push_str(from_table_name);
        if !query_parameters.join_statements.is_empty() {
            sql_query.push(' ');
            sql_query.push_str(&query_parameters.join_statements.join(" "));
        }
        perms::append_secure_query_condition(&mut where_expressions, user, &query_parameters);
        apply_where_conditions(&mut sql_query, &mut where_expressions, &query_parameters);
        sql_query.push_str(") pks) END AS full_count)");
    } else {
        sql_query.push_str("reducedRandomSet AS (SELECT ");
        sql_query.push_str(query_parameters.base_table_name);
        sql_query.push_str(".pk FROM ");
        sql_query.push_str(from_table_name);
        if !query_parameters.join_statements.is_empty() {
            sql_query.push(' ');
            sql_query.push_str(&query_parameters.join_statements.join(" "));
        }
        perms::append_secure_query_condition(&mut where_expressions, user, &query_parameters);
        apply_where_conditions(&mut sql_query, &mut where_expressions, &query_parameters);
        apply_ordering(
            &mut sql_query,
            &mut query_parameters.ordering,
            query_parameters.fallback_ordering.clone(),
        )?;
        sql_query.push_str(" LIMIT ");
        sql_query.push_str(MAX_SHUFFLE_LIMIT_STR);
        sql_query.push(')');
    }

    sql_query.push_str(" SELECT ");
    sql_query.push_str(&query_parameters.select_statements.join(", "));
    if !query_parameters.shuffle {
        sql_query.push_str(", (SELECT full_count FROM countCte), ");
    } else {
        sql_query.push_str(", NULL AS full_count, ");
    }
    // in case limit is not a constant expression (but e.g. a binary expression 50 + 10), evaluate the expression by selecting it
    // since the effective limit is needed to calculate the number of pages
    let limit = query_parameters
        .limit
        .as_deref()
        .unwrap_or(DEFAULT_LIMIT_STR);
    sql_query.push_str(limit);
    sql_query.push_str(" AS evaluated_limit FROM ");
    sql_query.push_str(from_table_name);
    if !query_parameters.join_statements.is_empty() {
        sql_query.push(' ');
        sql_query.push_str(&query_parameters.join_statements.join(" "));
    }

    if query_parameters.shuffle {
        sql_query.push_str(" WHERE ");
        sql_query.push_str(query_parameters.base_table_name);
        sql_query.push_str(".pk in(SELECT pk FROM reducedRandomSet) ORDER BY RANDOM()");
    } else {
        apply_where_conditions(&mut sql_query, &mut where_expressions, &query_parameters);
        apply_ordering(
            &mut sql_query,
            &mut query_parameters.ordering,
            query_parameters.fallback_ordering.clone(),
        )?;
    }
    apply_pagination(&mut sql_query, &query_parameters, limit, false)?;

    log::debug!(
        "Compiled query [{}] (in {} microseconds) to sql {}",
        &source_query.as_deref().unwrap_or(""),
        instant
            .map(|instant| instant.elapsed().as_micros())
            .unwrap_or(0),
        &sql_query
    );

    Ok(sql_query)
}

pub fn compile_window_query(
    current_pk: i64,
    query: Option<String>,
    mut query_parameters: QueryParameters,
    scope: &Scope,
    user: &Option<User>,
) -> Result<String, crate::Error> {
    let (source_query, instant) = if log::log_enabled!(log::Level::Debug) {
        (query.clone(), Some(std::time::Instant::now()))
    } else {
        (None, None)
    };

    let (ctes, mut where_expressions) = if let Some(query) = query {
        compile_expressions(query, &mut query_parameters, scope)?
    } else {
        (HashMap::new(), Vec::new())
    };

    let mut sql_query = String::new();
    apply_ctes(&mut sql_query, &ctes)?;

    let from_table_name = query_parameters
        .from_table_override
        .unwrap_or(query_parameters.base_table_name);
    if !query_parameters.shuffle {
        sql_query.push_str("SELECT * FROM (SELECT ROW_NUMBER() OVER(");
        apply_ordering(
            &mut sql_query,
            &mut query_parameters.ordering,
            query_parameters.fallback_ordering.clone(),
        )?;
        sql_query.push_str(" ) AS row_number, lag(");
        sql_query.push_str(from_table_name);
        sql_query.push_str(".pk) OVER(");
        apply_ordering(
            &mut sql_query,
            &mut query_parameters.ordering,
            query_parameters.fallback_ordering.clone(),
        )?;
        sql_query.push_str(" ) AS prev, ");
        sql_query.push_str(from_table_name);
        sql_query.push_str(".pk, lead(");
        sql_query.push_str(from_table_name);
        sql_query.push_str(".pk) OVER(");
        apply_ordering(
            &mut sql_query,
            &mut query_parameters.ordering,
            query_parameters.fallback_ordering.clone(),
        )?;
        sql_query.push_str(" ) AS next, ");

        // in case limit is not a constant expression (but e.g. a binary expression 50 + 10), evaluate the expression by selecting it
        // since the effective limit is needed to calculate the number of pages
        let limit = query_parameters
            .limit
            .as_deref()
            .unwrap_or(DEFAULT_LIMIT_STR);

        sql_query.push_str(limit);
        sql_query.push_str(" AS evaluated_limit FROM ");
        sql_query.push_str(from_table_name);
        if !query_parameters.join_statements.is_empty() {
            sql_query.push(' ');
            sql_query.push_str(&query_parameters.join_statements.join(" "));
        }

        perms::append_secure_query_condition(&mut where_expressions, user, &query_parameters);
        apply_where_conditions(&mut sql_query, &mut where_expressions, &query_parameters);
        apply_ordering(
            &mut sql_query,
            &mut query_parameters.ordering,
            query_parameters.fallback_ordering.clone(),
        )?;

        apply_pagination(&mut sql_query, &query_parameters, limit, true)?;

        sql_query.push_str(") sub WHERE pk = ");
        sql_query.push_str(&current_pk.to_string());
    } else {
        if ctes.is_empty() {
            sql_query.push_str("WITH ");
        } else {
            sql_query.push_str(", ");
        }

        sql_query.push_str("reducedRandomSet AS (SELECT ");
        sql_query.push_str(query_parameters.base_table_name);
        sql_query.push_str(".pk FROM ");
        sql_query.push_str(from_table_name);
        if !query_parameters.join_statements.is_empty() {
            sql_query.push(' ');
            sql_query.push_str(&query_parameters.join_statements.join(" "));
        }
        perms::append_secure_query_condition(&mut where_expressions, user, &query_parameters);
        apply_where_conditions(&mut sql_query, &mut where_expressions, &query_parameters);
        apply_ordering(
            &mut sql_query,
            &mut query_parameters.ordering,
            query_parameters.fallback_ordering,
        )?;
        sql_query.push_str(" LIMIT ");
        sql_query.push_str(MAX_SHUFFLE_LIMIT_STR);
        sql_query.push(')');

        sql_query.push_str(" SELECT 1::BIGINT AS row_number, NULL AS prev, ");
        sql_query.push_str(&current_pk.to_string());
        sql_query.push_str("::BIGINT AS pk, ");
        sql_query.push_str(query_parameters.base_table_name);
        sql_query.push_str(".pk AS next, 50 AS evaluated_limit FROM ");
        sql_query.push_str(from_table_name);
        if !query_parameters.join_statements.is_empty() {
            sql_query.push(' ');
            sql_query.push_str(&query_parameters.join_statements.join(" "));
        }
        sql_query.push_str(" WHERE ");
        sql_query.push_str(query_parameters.base_table_name);
        sql_query.push_str(".pk != ");
        sql_query.push_str(&current_pk.to_string());
        sql_query.push_str(" AND ");
        sql_query.push_str(query_parameters.base_table_name);
        sql_query.push_str(".pk IN(SELECT pk FROM reducedRandomSet) ORDER BY RANDOM() LIMIT 1");
    }

    log::debug!(
        "Compiled window query for [{}] (in {} microseconds) to sql {}",
        &source_query.as_deref().unwrap_or(""),
        instant
            .map(|instant| instant.elapsed().as_micros())
            .unwrap_or(0),
        &sql_query
    );

    Ok(sql_query)
}

/// Compile the provided query string to a [`QueryNode`]. Reports errors observed up until (including) the parser step but does not perform semantic analysis.
///
/// The provided log reference is consumed (swapped for an empty log) when an error is logged and a QueryCompilationError is returned.
///
/// If `fail_on_parse_error` is false, the compiled ast is still returned even if errors were observed, this is useful for autocomplete.
pub fn compile_ast(
    query: String,
    log: &mut Log,
    fail_on_parse_error: bool,
) -> Result<Node<QueryNode>, crate::Error> {
    let token_stream = Lexer::new_for_string(query, log).read_token_stream();
    if !log.errors.is_empty() {
        let log = std::mem::take(log);
        return Err(crate::Error::QueryCompilationError(
            String::from("lexer"),
            log.errors,
        ));
    }

    let ast = Parser::new(token_stream, log).parse_query();

    if fail_on_parse_error && !log.errors.is_empty() {
        let log = std::mem::take(log);
        return Err(crate::Error::QueryCompilationError(
            String::from("parser"),
            log.errors,
        ));
    }

    Ok(ast)
}

fn compile_expressions(
    query: String,
    query_parameters: &mut QueryParameters,
    scope: &Scope,
) -> Result<(HashMap<String, Cte>, Vec<String>), crate::Error> {
    let mut log = Log { errors: Vec::new() };
    let ast = compile_ast(query, &mut log, true)?;
    let mut semantic_analysis_visitor = SemanticAnalysisVisitor {};
    ast.accept(&mut semantic_analysis_visitor, scope, &mut log);
    if !log.errors.is_empty() {
        return Err(crate::Error::QueryCompilationError(
            String::from("semantic analysis"),
            log.errors,
        ));
    }

    let mut query_builder_visitor = QueryBuilderVisitor::new(query_parameters);
    ast.accept(&mut query_builder_visitor, scope, &mut log);
    if !log.errors.is_empty() {
        return Err(crate::Error::QueryCompilationError(
            String::from("query builder"),
            log.errors,
        ));
    }

    Ok((
        query_builder_visitor.ctes,
        query_builder_visitor.where_expressions,
    ))
}

pub fn apply_ctes(sql_query: &mut String, ctes: &HashMap<String, Cte>) -> Result<(), crate::Error> {
    let cte_len = ctes.len();
    if cte_len > 50 {
        return Err(crate::Error::IllegalQueryInputError(format!(
            "Exceeded maximum number of CTEs of 50 (recorded {}), too many tags supplied.",
            cte_len
        )));
    } else if cte_len > 0 {
        sql_query.push_str("WITH ");

        for (i, cte) in ctes.values().enumerate() {
            sql_query.push_str(&cte.expression);
            if i < cte_len - 1 {
                sql_query.push_str(", ");
            }
        }
    }

    Ok(())
}

fn apply_where_conditions(
    sql_query: &mut String,
    where_expressions: &mut Vec<String>,
    query_parameters: &QueryParameters,
) {
    let where_expressions_len = where_expressions.len()
        + query_parameters
            .predefined_where_conditions
            .as_ref()
            .map(|v| v.len())
            .unwrap_or(0);
    if where_expressions_len > 0 {
        sql_query.push_str(" WHERE ");

        for (i, where_expression) in query_parameters
            .predefined_where_conditions
            .as_deref()
            .unwrap_or_default()
            .iter()
            .chain(where_expressions.iter())
            .enumerate()
        {
            sql_query.push_str(where_expression);

            if i < where_expressions_len - 1 {
                sql_query.push_str(" AND ");
            }
        }
    }
}

pub fn apply_ordering(
    sql_query: &mut String,
    ordering: &mut Vec<Ordering>,
    fallback_ordering: Ordering,
) -> Result<(), crate::Error> {
    let ordered_by_fallback = ordering
        .last()
        .map(|ord| ord == &fallback_ordering)
        .unwrap_or(false);
    if (ordered_by_fallback && ordering.len() > 3) || (!ordered_by_fallback && ordering.len() > 2) {
        return Err(crate::Error::IllegalQueryInputError(String::from(
            "Cannot sort by more than two attributes",
        )));
    }
    // always sort by pk desc last for consistent sorting
    if !ordered_by_fallback {
        ordering.push(fallback_ordering);
    }

    let ordering_len = ordering.len();
    if ordering_len > 0 {
        sql_query.push_str(" ORDER BY ");
        for (i, ordering) in ordering.iter().enumerate() {
            sql_query.push_str(&ordering.expression);

            match ordering.direction {
                Direction::Ascending => {
                    if ordering.nullable {
                        sql_query.push_str(" ASC NULLS LAST")
                    } else {
                        sql_query.push_str(" ASC")
                    }
                }
                Direction::Descending => {
                    if ordering.nullable {
                        sql_query.push_str(" DESC NULLS LAST")
                    } else {
                        sql_query.push_str(" DESC")
                    }
                }
            }

            if i < ordering_len - 1 {
                sql_query.push_str(", ");
            }
        }
    }

    Ok(())
}

pub fn apply_pagination(
    sql_query: &mut String,
    query_parameters: &QueryParameters,
    limit: &str,
    include_window: bool,
) -> Result<(), crate::Error> {
    let page = query_parameters.page.unwrap_or(0);

    // If the limit is not a valid u16 (e.g. if it's a binary expression '50 + 10'), the error is returned
    // after the query has been evaluated, the limit is supplied to the LEAST function to protect against large
    // limits and enforce max limit
    if let Ok(parsed_limit) = limit.parse::<u32>() {
        if parsed_limit > query_parameters.max_limit {
            return Err(crate::Error::IllegalQueryInputError(format!(
                "Limit '{}' exceeds maximum limit of {}.",
                parsed_limit, query_parameters.max_limit
            )));
        }
    }

    // for window queries, increase the limit by 2 and subtract 1 from the offset
    // to include the last post from the previous page and the first post from the next page
    sql_query.push_str(" LIMIT LEAST(");
    if include_window {
        sql_query.push('(');
        sql_query.push_str(limit);
        sql_query.push_str(" + 2)");
    } else {
        sql_query.push_str(limit);
    }
    sql_query.push_str(", ");
    let max_limit_str = query_parameters.max_limit.to_string();
    if include_window {
        sql_query.push('(');
        sql_query.push_str(&max_limit_str);
        sql_query.push_str(" + 2)");
    } else {
        sql_query.push_str(&max_limit_str);
    }
    sql_query.push_str(") OFFSET (");
    if include_window {
        sql_query.push_str("GREATEST(((");
    }
    sql_query.push_str(limit);
    sql_query.push_str(") * ");
    sql_query.push_str(&page.to_string());
    if include_window {
        sql_query.push_str(") - 1, 0))");
    }

    Ok(())
}
