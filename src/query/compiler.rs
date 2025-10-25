use serde::Serialize;

use lexer::Lexer;
use std::{collections::HashMap, fmt};

use self::{
    ast::{Node, QueryBuilderVisitor, QueryNode, SemanticAnalysisVisitor},
    dict::Scope,
    parser::Parser,
};

use super::{MAX_PAGE, QueryBuilderPagination, QueryParameters};

use crate::query::compiler::ast::{
    ExpressionStatement, Operator, StatementNode, combine_expressions,
};
use crate::query::compiler::dict::Type;
use crate::{
    model::User,
    perms,
    query::{DEFAULT_LIMIT_STR, Direction, MAX_FULL_LIMIT_STR, Ordering},
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

#[allow(dead_code)] // keep both for completion's sake
pub enum Junction {
    And,
    Or,
}

pub fn compile_conditions(
    conditions: Vec<String>,
    junction: Junction,
    mut query_parameters: QueryParameters,
    scope: &Scope,
    user: &Option<User>,
) -> Result<String, crate::Error> {
    let (source_conditions, instant) = if log::log_enabled!(log::Level::Debug) {
        (Some(conditions.clone()), Some(std::time::Instant::now()))
    } else {
        (None, None)
    };
    let mut log = Log { errors: Vec::new() };

    let mut root_node = compile_conditions_ast(conditions, junction, scope, &mut log)?;

    let mut query_builder_visitor = QueryBuilderVisitor::new(&mut query_parameters);
    root_node.accept(&mut query_builder_visitor, scope, &mut log);
    if !log.errors.is_empty() {
        return Err(crate::Error::QueryCompilationError(
            String::from("query builder"),
            log.errors,
        ));
    }

    let sql_query = build_sql_string(
        query_builder_visitor.ctes,
        query_builder_visitor.where_expressions,
        query_parameters,
        user,
    )?;
    log::debug!(
        "Compiled conditions [{:?}] (in {} microseconds) to sql {}",
        source_conditions.unwrap_or_else(Vec::new),
        instant
            .map(|instant| instant.elapsed().as_micros())
            .unwrap_or(0),
        &sql_query
    );

    Ok(sql_query)
}

pub fn compile_conditions_ast(
    conditions: Vec<String>,
    junction: Junction,
    scope: &Scope,
    log: &mut Log,
) -> Result<Node<QueryNode>, crate::Error> {
    let mut root_expression = None;

    for condition in conditions {
        let mut ast = compile_ast(condition, log, true)?;
        let mut semantic_analysis_visitor = SemanticAnalysisVisitor {};
        ast.accept(&mut semantic_analysis_visitor, scope, log);

        let mut condition_expression = None;
        for statement in ast.node_type.statements {
            let expression_statement = statement.node_type.downcast_ref::<ExpressionStatement>();
            match expression_statement {
                Some(expression_statement) if expression_statement.expression_node.node_type.get_return_type(scope) == Type::Boolean => {
                    let expression_node = expression_statement.expression_node.clone_boxed_node();
                    condition_expression = match condition_expression {
                        None => Some(expression_node),
                        Some(existing_expression) => Some(combine_expressions(existing_expression, expression_node, Operator::And)),
                    };
                }
                _ => {
                    log.errors.push(Error {
                        location: statement.location,
                        msg: format!("All statements used in FQL condition must be boolean expression but got {:?}", &statement.node_type),
                    })
                }
            }
        }

        if !log.errors.is_empty() {
            return Err(crate::Error::QueryCompilationError(
                String::from("semantic analysis"),
                log.errors.clone(),
            ));
        }

        root_expression = match root_expression {
            None => condition_expression,
            Some(existing_expression) => {
                if let Some(condition_expression) = condition_expression {
                    Some(combine_expressions(
                        existing_expression,
                        condition_expression,
                        match junction {
                            Junction::And => Operator::And,
                            Junction::Or => Operator::Or,
                        },
                    ))
                } else {
                    Some(existing_expression)
                }
            }
        }
    }

    let root_node = Node {
        location: Location { start: 0, end: 0 },
        node_type: QueryNode {
            statements: root_expression
                .into_iter()
                .map(|root_expression| {
                    Box::new(Node {
                        location: Location { start: 0, end: 0 },
                        node_type: ExpressionStatement {
                            expression_node: root_expression,
                        },
                    }) as Box<Node<dyn StatementNode>>
                })
                .collect(),
        },
    };

    Ok(root_node)
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

    let (ctes, where_expressions) = if let Some(query) = query {
        compile_expressions(query, &mut query_parameters, scope)?
    } else {
        (HashMap::new(), Vec::new())
    };

    let sql_query = build_sql_string(ctes, where_expressions, query_parameters, user)?;
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

fn build_sql_string(
    ctes: HashMap<String, Cte>,
    mut where_expressions: Vec<String>,
    mut query_parameters: QueryParameters,
    user: &Option<User>,
) -> Result<String, crate::Error> {
    let mut sql_query = String::new();

    let from_table_name = query_parameters
        .from_table_override
        .unwrap_or(query_parameters.base_table_name);

    apply_ctes(&mut sql_query, &ctes)?;
    if query_parameters.include_full_count {
        if ctes.is_empty() {
            sql_query.push_str("WITH ");
        } else {
            sql_query.push_str(", ");
        }

        // Load a full set of PKs limited by MAX_FULL_LIMIT_STR (10000) to create a limited count of results
        // as well as a limited set of PKs to shuffle (shuffling is too slow for larger results)
        sql_query.push_str("limitedPkSet AS (SELECT ");
        sql_query.push_str(from_table_name);
        sql_query.push_str(".pk FROM ");
        sql_query.push_str(from_table_name);
        if !query_parameters.join_statements.is_empty() {
            sql_query.push(' ');
            sql_query.push_str(&query_parameters.join_statements.join(" "));
        }
        perms::append_secure_query_condition(&mut where_expressions, user, &query_parameters);
        apply_where_conditions(&mut sql_query, &mut where_expressions, &query_parameters);
        sql_query.push_str(" LIMIT ");
        sql_query.push_str(MAX_FULL_LIMIT_STR);
        sql_query.push_str("), countCte AS (SELECT NULLIF((SELECT count(*) FROM limitedPkSet), ");
        sql_query.push_str(MAX_FULL_LIMIT_STR);
        sql_query.push_str(") AS full_count)");

        sql_query.push_str(" SELECT ");
        sql_query.push_str(&query_parameters.select_statements.join(", "));
        sql_query.push_str(", (SELECT full_count FROM countCte)");
    } else {
        if ctes.is_empty() {
            sql_query.push_str("SELECT ");
        } else {
            sql_query.push_str(" SELECT ");
        }
        sql_query.push_str(&query_parameters.select_statements.join(", "));
    }

    // in case limit is not a constant expression (but e.g. a binary expression 50 + 10), evaluate the expression by selecting it
    // since the effective limit is needed to calculate the number of pages
    if let Some(ref pagination) = query_parameters.pagination {
        let limit = pagination.limit.as_deref().unwrap_or(DEFAULT_LIMIT_STR);
        sql_query.push_str(", ");
        sql_query.push_str(limit);
        sql_query.push_str(" AS evaluated_limit");
    }

    sql_query.push_str(" FROM ");
    sql_query.push_str(from_table_name);
    if !query_parameters.join_statements.is_empty() {
        sql_query.push(' ');
        sql_query.push_str(&query_parameters.join_statements.join(" "));
    }

    if query_parameters.shuffle {
        sql_query.push_str(" WHERE ");
        sql_query.push_str(from_table_name);
        sql_query.push_str(".pk IN(SELECT pk FROM limitedPkSet) ORDER BY RANDOM()");
    } else {
        apply_where_conditions(&mut sql_query, &mut where_expressions, &query_parameters);
        apply_ordering(
            &mut sql_query,
            &mut query_parameters.ordering,
            &query_parameters.fallback_orderings,
        )?;
    }
    apply_pagination(&mut sql_query, &query_parameters, false)?;

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
        sql_query.push_str(" SELECT * FROM (SELECT ROW_NUMBER() OVER(");
        apply_ordering(
            &mut sql_query,
            &mut query_parameters.ordering,
            &query_parameters.fallback_orderings,
        )?;
        sql_query.push_str(" ) AS row_number, lag(");
        sql_query.push_str(from_table_name);
        sql_query.push_str(".pk) OVER(");
        apply_ordering(
            &mut sql_query,
            &mut query_parameters.ordering,
            &query_parameters.fallback_orderings,
        )?;
        sql_query.push_str(" ) AS prev, ");
        sql_query.push_str(from_table_name);
        sql_query.push_str(".pk, lead(");
        sql_query.push_str(from_table_name);
        sql_query.push_str(".pk) OVER(");
        apply_ordering(
            &mut sql_query,
            &mut query_parameters.ordering,
            &query_parameters.fallback_orderings,
        )?;
        sql_query.push_str(" ) AS next");

        // in case limit is not a constant expression (but e.g. a binary expression 50 + 10), evaluate the expression by selecting it
        // since the effective limit is needed to calculate the number of pages
        if let Some(ref pagination) = query_parameters.pagination {
            let limit = pagination.limit.as_deref().unwrap_or(DEFAULT_LIMIT_STR);
            sql_query.push_str(", ");
            sql_query.push_str(limit);
            sql_query.push_str(" AS evaluated_limit FROM ");
        } else {
            sql_query.push_str(" FROM ");
        }

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
            &query_parameters.fallback_orderings,
        )?;

        apply_pagination(&mut sql_query, &query_parameters, true)?;

        sql_query.push_str(") sub WHERE pk = ");
        sql_query.push_str(&current_pk.to_string());
    } else {
        if ctes.is_empty() {
            sql_query.push_str("WITH ");
        } else {
            sql_query.push_str(", ");
        }

        sql_query.push_str("reducedRandomSet AS (SELECT ");
        sql_query.push_str(from_table_name);
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
            &query_parameters.fallback_orderings,
        )?;
        sql_query.push_str(" LIMIT ");
        sql_query.push_str(MAX_FULL_LIMIT_STR);
        sql_query.push(')');

        sql_query.push_str(" SELECT 1::BIGINT AS row_number, NULL AS prev, ");
        sql_query.push_str(&current_pk.to_string());
        sql_query.push_str("::BIGINT AS pk, ");
        sql_query.push_str(from_table_name);
        sql_query.push_str(".pk AS next, 50 AS evaluated_limit FROM ");
        sql_query.push_str(from_table_name);
        if !query_parameters.join_statements.is_empty() {
            sql_query.push(' ');
            sql_query.push_str(&query_parameters.join_statements.join(" "));
        }
        sql_query.push_str(" WHERE ");
        sql_query.push_str(from_table_name);
        sql_query.push_str(".pk != ");
        sql_query.push_str(&current_pk.to_string());
        sql_query.push_str(" AND ");
        sql_query.push_str(from_table_name);
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
    let mut ast = compile_ast(query, &mut log, true)?;
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
            "Exceeded maximum number of CTEs of 50 (recorded {cte_len}), too many tags supplied."
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
    where_expressions: &mut [String],
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
    fallback_orderings: &[Ordering],
) -> Result<(), crate::Error> {
    let fallback_ordering = if let Some(o) = ordering.first() {
        fallback_orderings
            .iter()
            .find(|f| o.table == f.table)
            .or(fallback_orderings.first())
    } else {
        fallback_orderings.first()
    };
    let ordered_by_fallback = fallback_ordering.is_some()
        && ordering
            .last()
            .map(|ord| ord == fallback_ordering.unwrap())
            .unwrap_or(false);
    match fallback_ordering {
        Some(fallback_ordering) if !ordered_by_fallback => {
            ordering.push(fallback_ordering.clone());
        }
        _ => {}
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
    include_window: bool,
) -> Result<(), crate::Error> {
    if let Some(ref pagination) = query_parameters.pagination {
        let page = pagination.page.unwrap_or(0);
        let limit = pagination.limit.as_deref().unwrap_or(DEFAULT_LIMIT_STR);

        // If the limit is not a valid u16 (e.g. if it's a binary expression '50 + 10'), the error is returned
        // after the query has been evaluated, the limit is supplied to the LEAST function to protect against large
        // limits and enforce max limit
        if let Ok(parsed_limit) = limit.parse::<u32>()
            && parsed_limit > pagination.max_limit
        {
            return Err(crate::Error::IllegalQueryInputError(format!(
                "Limit '{}' exceeds maximum limit of {}.",
                parsed_limit, pagination.max_limit
            )));
        }
        if page > MAX_PAGE {
            return Err(crate::Error::IllegalQueryInputError(format!(
                "Page {} exceeds maximum page of {}.",
                page, MAX_PAGE
            )));
        }

        // wrap the evaluated limit in a LEAST function to protect against large limits and enforce max limit
        fn append_safe_limit(
            sql_query: &mut String,
            limit: &str,
            pagination: &QueryBuilderPagination,
            include_window: bool,
        ) {
            sql_query.push_str("LEAST(");
            if include_window {
                sql_query.push('(');
                sql_query.push_str(limit);
                sql_query.push_str(" + 2)");
            } else {
                sql_query.push_str(limit);
            }
            sql_query.push_str(", ");
            let max_limit_str = pagination.max_limit.to_string();
            if include_window {
                sql_query.push('(');
                sql_query.push_str(&max_limit_str);
                sql_query.push_str(" + 2)");
            } else {
                sql_query.push_str(&max_limit_str);
            }
            sql_query.push(')');
        }

        // for window queries, increase the limit by 2 and subtract 1 from the offset
        // to include the last post from the previous page and the first post from the next page
        sql_query.push_str(" LIMIT ");
        append_safe_limit(sql_query, limit, pagination, include_window);
        sql_query.push_str(" OFFSET (");
        if include_window {
            sql_query.push_str("GREATEST(((");
        }
        append_safe_limit(sql_query, limit, pagination, include_window);
        sql_query.push_str(") * ");
        sql_query.push_str(&page.to_string());
        if include_window {
            sql_query.push_str(") - 1, 0))");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::query::compiler::ast::{
        AttributeNode, BinaryExpressionNode, ExpressionStatement, Operator, PostTagNode,
        VariableNode,
    };
    use crate::query::compiler::dict::Scope;
    use crate::query::compiler::{Junction, Log, compile_conditions_ast};

    #[test]
    fn test_compile_single_condition() {
        let mut log = Log { errors: Vec::new() };
        let ast = compile_conditions_ast(
            vec![String::from("Liara")],
            Junction::And,
            &Scope::Post,
            &mut log,
        )
        .expect("Failed to compile conditions");
        assert_eq!(ast.node_type.statements.len(), 1);

        let expression_statement = ast.node_type.statements[0]
            .node_type
            .downcast_ref::<ExpressionStatement>()
            .unwrap();
        let post_tag_node = expression_statement
            .expression_node
            .node_type
            .downcast_ref::<PostTagNode>()
            .unwrap();
        assert_eq!(post_tag_node.identifier, "liara");
    }

    #[test]
    fn test_compile_single_condition_with_multiple_statements() {
        let mut log = Log { errors: Vec::new() };
        let ast = compile_conditions_ast(
            vec![String::from("Liara tag2")],
            Junction::And,
            &Scope::Post,
            &mut log,
        )
        .expect("Failed to compile conditions");
        // compile_conditions_ast logic still combines each condition into a single statement
        assert_eq!(ast.node_type.statements.len(), 1);

        let expression_statement = ast.node_type.statements[0]
            .node_type
            .downcast_ref::<ExpressionStatement>()
            .unwrap();
        let binary_expression_node = expression_statement
            .expression_node
            .node_type
            .downcast_ref::<BinaryExpressionNode>()
            .unwrap();
        assert_eq!(binary_expression_node.op, Operator::And);
        assert_eq!(
            binary_expression_node
                .left
                .node_type
                .downcast_ref::<PostTagNode>()
                .unwrap()
                .identifier,
            "liara"
        );
        assert_eq!(
            binary_expression_node
                .right
                .node_type
                .downcast_ref::<PostTagNode>()
                .unwrap()
                .identifier,
            "tag2"
        );
    }

    #[test]
    fn test_compile_multiple_conditions_with_single_statement() {
        let mut log = Log { errors: Vec::new() };
        let ast = compile_conditions_ast(
            vec![String::from("Liara"), String::from("tag2")],
            Junction::And,
            &Scope::Post,
            &mut log,
        )
        .expect("Failed to compile conditions");
        // compile_conditions_ast logic still combines each condition into a single statement
        assert_eq!(ast.node_type.statements.len(), 1);

        let expression_statement = ast.node_type.statements[0]
            .node_type
            .downcast_ref::<ExpressionStatement>()
            .unwrap();
        let binary_expression_node = expression_statement
            .expression_node
            .node_type
            .downcast_ref::<BinaryExpressionNode>()
            .unwrap();
        assert_eq!(binary_expression_node.op, Operator::And);
        assert_eq!(
            binary_expression_node
                .left
                .node_type
                .downcast_ref::<PostTagNode>()
                .unwrap()
                .identifier,
            "liara"
        );
        assert_eq!(
            binary_expression_node
                .right
                .node_type
                .downcast_ref::<PostTagNode>()
                .unwrap()
                .identifier,
            "tag2"
        );
    }

    #[test]
    fn test_compile_multiple_conditions_with_multiple_statements() {
        let mut log = Log { errors: Vec::new() };
        let ast = compile_conditions_ast(
            vec![
                String::from("Liara tag2"),
                String::from("tag3 @uploader = :self"),
                String::from(
                    "(`Linkin Park` AND `Nu Metal`) OR (`Bring Me The Horizon` AND Metalcore)",
                ),
            ],
            Junction::Or,
            &Scope::Post,
            &mut log,
        )
        .expect("Failed to compile conditions");
        // compile_conditions_ast logic still combines each condition into a single statement
        assert_eq!(ast.node_type.statements.len(), 1);

        let expression_statement = ast.node_type.statements[0]
            .node_type
            .downcast_ref::<ExpressionStatement>()
            .unwrap();
        let binary_expression_node = expression_statement
            .expression_node
            .node_type
            .downcast_ref::<BinaryExpressionNode>()
            .unwrap();
        assert_eq!(binary_expression_node.op, Operator::Or);

        // BinaryExpressionNode {
        //     left: BinaryExpressionNode {
        //         left: BinaryExpression (first condition)
        //         op: Or,
        //         right: BinaryExpression (second condition)
        //     }
        //     op: Or,
        //     right: BinaryExpressionNode (third condition)
        // }
        let left_binary_expression_node = binary_expression_node
            .left
            .node_type
            .downcast_ref::<BinaryExpressionNode>()
            .unwrap();
        assert_eq!(left_binary_expression_node.op, Operator::Or);
        let first_binary_expression_node = left_binary_expression_node
            .left
            .node_type
            .downcast_ref::<BinaryExpressionNode>()
            .unwrap();
        let second_binary_expression_node = left_binary_expression_node
            .right
            .node_type
            .downcast_ref::<BinaryExpressionNode>()
            .unwrap();
        let third_binary_expression_node = binary_expression_node
            .right
            .node_type
            .downcast_ref::<BinaryExpressionNode>()
            .unwrap();

        assert_eq!(
            first_binary_expression_node
                .left
                .node_type
                .downcast_ref::<PostTagNode>()
                .unwrap()
                .identifier,
            "liara"
        );
        assert_eq!(first_binary_expression_node.op, Operator::And);
        assert_eq!(
            first_binary_expression_node
                .right
                .node_type
                .downcast_ref::<PostTagNode>()
                .unwrap()
                .identifier,
            "tag2"
        );

        assert_eq!(
            second_binary_expression_node
                .left
                .node_type
                .downcast_ref::<PostTagNode>()
                .unwrap()
                .identifier,
            "tag3"
        );
        assert_eq!(second_binary_expression_node.op, Operator::And);
        let attr_comparison_node = second_binary_expression_node
            .right
            .node_type
            .downcast_ref::<BinaryExpressionNode>()
            .unwrap();
        assert_eq!(
            attr_comparison_node
                .left
                .node_type
                .downcast_ref::<AttributeNode>()
                .unwrap()
                .identifier,
            "uploader"
        );
        assert_eq!(attr_comparison_node.op, Operator::Equal);
        assert_eq!(
            attr_comparison_node
                .right
                .node_type
                .downcast_ref::<VariableNode>()
                .unwrap()
                .identifier,
            "self"
        );

        let third_node_left = third_binary_expression_node
            .left
            .node_type
            .downcast_ref::<BinaryExpressionNode>()
            .unwrap();
        let third_node_right = third_binary_expression_node
            .right
            .node_type
            .downcast_ref::<BinaryExpressionNode>()
            .unwrap();
        assert_eq!(
            third_node_left
                .left
                .node_type
                .downcast_ref::<PostTagNode>()
                .unwrap()
                .identifier,
            "linkin park"
        );
        assert_eq!(third_node_left.op, Operator::And);
        assert_eq!(
            third_node_left
                .right
                .node_type
                .downcast_ref::<PostTagNode>()
                .unwrap()
                .identifier,
            "nu metal"
        );
        assert_eq!(third_binary_expression_node.op, Operator::Or);
        assert_eq!(
            third_node_right
                .left
                .node_type
                .downcast_ref::<PostTagNode>()
                .unwrap()
                .identifier,
            "bring me the horizon"
        );
        assert_eq!(third_node_right.op, Operator::And);
        assert_eq!(
            third_node_right
                .right
                .node_type
                .downcast_ref::<PostTagNode>()
                .unwrap()
                .identifier,
            "metalcore"
        );
    }
}
