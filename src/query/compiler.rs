use serde::Serialize;

use lexer::Lexer;
use std::fmt;

use self::{
    ast::{QueryBuilderVisitor, SemanticAnalysisVisitor},
    parser::{Parser, ParserError},
};

use super::QueryParameters;

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

pub fn compile_sql(
    query: Option<String>,
    mut query_parameters: QueryParameters,
) -> Result<String, crate::Error> {
    let query_builder_visitor = if let Some(query) = query {
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

        query_builder_visitor
    } else {
        QueryBuilderVisitor::new(&mut query_parameters)
    };

    let mut sql_query = String::new();

    let cte_len = query_builder_visitor.ctes.len();
    if cte_len > 0 {
        sql_query.push_str("WITH ");

        for (i, cte) in query_builder_visitor.ctes.iter().enumerate() {
            sql_query.push_str(cte);
            if i < cte_len - 1 {
                sql_query.push_str(", ");
            }
        }
    }

    sql_query.push_str(" SELECT * FROM post");

    let where_expressions_len = query_builder_visitor.where_expressions.len();
    if where_expressions_len > 0 {
        sql_query.push_str(" WHERE ");

        for (i, where_expression) in query_builder_visitor.where_expressions.iter().enumerate() {
            sql_query.push('(');
            sql_query.push_str(where_expression);
            sql_query.push(')');

            if i < where_expressions_len - 1 {
                sql_query.push_str(" AND ");
            }
        }
    }

    let limit = query_parameters.limit.as_deref().unwrap_or("50");
    let page = query_parameters.page.unwrap_or(0);

    sql_query.push_str(" LIMIT ");
    sql_query.push_str(limit);
    sql_query.push_str(" OFFSET (");
    sql_query.push_str(limit);
    sql_query.push_str(") * ");
    sql_query.push_str(&page.to_string());

    Ok(sql_query)
}
