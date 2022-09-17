use serde::Serialize;

use lexer::Lexer;
use std::{collections::HashMap, fmt};

use self::{
    ast::{QueryBuilderVisitor, SemanticAnalysisVisitor},
    parser::{Parser, ParserError},
};

use super::QueryParameters;

use crate::{
    model::User,
    perms,
    query::{Direction, Ordering, DEFAULT_LIMIT_STR, MAX_LIMIT, MAX_LIMIT_STR},
};

pub mod ast;
pub mod dict;
pub mod lexer;
pub mod parser;

pub const INTEGER_LIMIT: u32 = 1 << 31;

#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize)]
pub struct Location {
    pub start: usize,
    pub end: usize,
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.start, self.end)
    }
}

pub struct Log {
    pub errors: Vec<Error>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
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
    user: &Option<User>,
) -> Result<String, crate::Error> {
    let (source_query, instant) = if log::log_enabled!(log::Level::Debug) {
        (query.clone(), Some(std::time::Instant::now()))
    } else {
        (None, None)
    };

    let (ctes, mut where_expressions) = if let Some(query) = query {
        let len = query.len();
        let mut log = Log { errors: Vec::new() };
        let token_stream = Lexer::new_for_string(query, &mut log).read_token_stream();
        if !log.errors.is_empty() {
            return Err(crate::Error::QueryCompilationError(
                String::from("lexer"),
                log.errors,
            ));
        }

        let ast = match Parser::new(token_stream, &mut log).parse_query() {
            Ok(query_node) => query_node,
            Err(ParserError::PrematureEof) => {
                return Err(crate::Error::QueryCompilationError(
                    String::from("parser"),
                    vec![Error {
                        location: Location {
                            start: len - 1,
                            end: len - 1,
                        },
                        msg: String::from("Expected additional token"),
                    }],
                ))
            }
            Err(ParserError::UnexpectedToken(token)) => {
                return Err(crate::Error::QueryCompilationError(
                    String::from("parser"),
                    vec![Error {
                        location: token.location,
                        msg: format!("Unexpected token: {:?}", token.parsed_token),
                    }],
                ))
            }
        };

        if !log.errors.is_empty() {
            return Err(crate::Error::QueryCompilationError(
                String::from("parser"),
                log.errors,
            ));
        }

        let mut semantic_analysis_visitor = SemanticAnalysisVisitor {};
        ast.accept(&mut semantic_analysis_visitor, &mut log);
        if !log.errors.is_empty() {
            return Err(crate::Error::QueryCompilationError(
                String::from("semantic analysis"),
                log.errors,
            ));
        }

        let mut query_builder_visitor = QueryBuilderVisitor::new(&mut query_parameters);
        ast.accept(&mut query_builder_visitor, &mut log);
        if !log.errors.is_empty() {
            return Err(crate::Error::QueryCompilationError(
                String::from("query builder"),
                log.errors,
            ));
        }

        (
            query_builder_visitor.ctes,
            query_builder_visitor.where_expressions,
        )
    } else {
        (HashMap::new(), Vec::new())
    };

    let mut sql_query = String::new();

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

    sql_query.push_str(" SELECT *, obj.thumbnail_object_key, count(*) OVER() AS full_count, ");
    // in case limit is not a constant expression (but e.g. a binary expression 50 + 10), evaluate the expression by selecting it
    // since the effective limit is needed to calculate the number of pages
    let limit = query_parameters
        .limit
        .as_deref()
        .unwrap_or(DEFAULT_LIMIT_STR);
    sql_query.push_str(limit);
    sql_query.push_str(
        " AS evaluated_limit FROM post LEFT JOIN s3_object obj ON obj.object_key = post.s3_object",
    );

    perms::append_secure_query_condition(&mut where_expressions, user);

    let where_expressions_len = where_expressions.len();
    if where_expressions_len > 0 {
        sql_query.push_str(" WHERE ");

        for (i, where_expression) in where_expressions.iter().enumerate() {
            sql_query.push_str(where_expression);

            if i < where_expressions_len - 1 {
                sql_query.push_str(" AND ");
            }
        }
    }

    if query_parameters.ordering.is_empty() {
        query_parameters.ordering.push(Ordering {
            expression: String::from("pk"),
            direction: Direction::Descending,
        });
    }

    let ordering_len = query_parameters.ordering.len();
    if ordering_len > 0 {
        sql_query.push_str(" ORDER BY ");
        for (i, ordering) in query_parameters.ordering.iter().enumerate() {
            sql_query.push_str(&ordering.expression);

            match ordering.direction {
                Direction::Ascending => sql_query.push_str(" ASC"),
                Direction::Descending => sql_query.push_str(" DESC"),
            }

            if i < ordering_len - 1 {
                sql_query.push_str(", ");
            }
        }
    }

    let page = query_parameters.page.unwrap_or(0);

    // If the limit is not a valid u16 (e.g. if it's a binary expression '50 + 10'), the error is returned
    // after the query has been evaluated, the limit is supplied to the LEAST function to protect agains large
    // limits and enforce max limit
    if let Ok(parsed_limit) = limit.parse::<u16>() {
        if parsed_limit > MAX_LIMIT {
            return Err(crate::Error::IllegalQueryInputError(format!(
                "Limit '{}' exceeds maximum limit of {}.",
                parsed_limit, MAX_LIMIT
            )));
        }
    }

    sql_query.push_str(" LIMIT LEAST(");
    sql_query.push_str(limit);
    sql_query.push_str(", ");
    sql_query.push_str(MAX_LIMIT_STR);
    sql_query.push_str(") OFFSET (");
    sql_query.push_str(limit);
    sql_query.push_str(") * ");
    sql_query.push_str(&page.to_string());

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
