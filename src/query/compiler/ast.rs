use std::{collections::HashMap, fmt::Debug};

use downcast_rs::{Downcast, impl_downcast};
use lazy_static::lazy_static;

use crate::query::{Direction, Ordering, QueryParameters};

use super::{
    Cte, Error, Location, Log,
    dict::{Scope, Type},
    lexer::Tag,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Operator {
    And,
    Divide,
    Equal,
    FuzzyEqual,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    Minus,
    Modulo,
    Not,
    Or,
    Plus,
    Times,
    Unequal,
}

impl Operator {
    pub fn for_unary_operator_tag(tag: Tag) -> Option<Self> {
        match tag {
            Tag::Not => Some(Operator::Not),
            Tag::Plus => Some(Operator::Plus),
            Tag::Minus => Some(Operator::Minus),
            _ => None,
        }
    }

    pub fn for_math_term_operator_tag(tag: Tag) -> Option<Self> {
        match tag {
            Tag::Times => Some(Operator::Times),
            Tag::Divide => Some(Operator::Divide),
            Tag::Modulo => Some(Operator::Modulo),
            _ => None,
        }
    }

    pub fn for_simple_math_operator_tag(tag: Tag) -> Option<Self> {
        match tag {
            Tag::Plus => Some(Operator::Plus),
            Tag::Minus => Some(Operator::Minus),
            _ => None,
        }
    }

    pub fn for_compare_operator_tag(tag: Tag) -> Option<Self> {
        match tag {
            Tag::Equal => Some(Operator::Equal),
            Tag::FuzzyEqual => Some(Operator::FuzzyEqual),
            Tag::Unequal => Some(Operator::Unequal),
            Tag::Less => Some(Operator::Less),
            Tag::LessEqual => Some(Operator::LessEqual),
            Tag::Greater => Some(Operator::Greater),
            Tag::GreaterEqual => Some(Operator::GreaterEqual),
            _ => None,
        }
    }

    pub fn get_sql_string(&self, binary_types: Option<(Type, Type)>) -> &str {
        match self {
            Self::And => "AND",
            Self::Divide => "/",
            Self::Equal => {
                if let Some((_, Type::Null)) = binary_types {
                    "IS"
                } else {
                    "="
                }
            }
            Self::FuzzyEqual => "LIKE",
            Self::Greater => ">",
            Self::GreaterEqual => ">=",
            Self::Less => "<",
            Self::LessEqual => "<=",
            Self::Minus => "-",
            Self::Modulo => "%",
            Self::Not => "NOT",
            Self::Or => "OR",
            Self::Plus => {
                if let Some((Type::String, Type::String)) = binary_types {
                    "||"
                } else {
                    "+"
                }
            }
            Self::Times => "*",
            Self::Unequal => {
                if let Some((_, Type::Null)) = binary_types {
                    "IS NOT"
                } else {
                    "!="
                }
            }
        }
    }

    pub fn accepts_binary_expression(&self, left: Type, right: Type) -> Option<Type> {
        match self {
            Self::And if both_of_type_or_null(left, right, Type::Boolean) => Some(Type::Boolean),
            Self::Divide if both_of_type_or_null(left, right, Type::Number) => Some(Type::Number),
            Self::Equal if left == right || left == Type::Null || right == Type::Null => {
                Some(Type::Boolean)
            }
            Self::FuzzyEqual if left == Type::String && right == Type::String => {
                Some(Type::Boolean)
            }
            Self::Greater if both_of_type_or_null(left, right, Type::Number) => Some(Type::Boolean),
            Self::GreaterEqual if both_of_type_or_null(left, right, Type::Number) => {
                Some(Type::Boolean)
            }
            Self::Less if both_of_type_or_null(left, right, Type::Number) => Some(Type::Boolean),
            Self::LessEqual if both_of_type_or_null(left, right, Type::Number) => {
                Some(Type::Boolean)
            }
            Self::Greater if both_of_type_or_null(left, right, Type::String) => Some(Type::Boolean),
            Self::GreaterEqual if both_of_type_or_null(left, right, Type::String) => {
                Some(Type::Boolean)
            }
            Self::Less if both_of_type_or_null(left, right, Type::String) => Some(Type::Boolean),
            Self::LessEqual if both_of_type_or_null(left, right, Type::String) => {
                Some(Type::Boolean)
            }
            Self::Minus if both_of_type_or_null(left, right, Type::Number) => Some(Type::Number),
            Self::Modulo if both_of_type_or_null(left, right, Type::Number) => Some(Type::Number),
            // Not is a unary operator only
            Self::Not => None,
            Self::Or if left == Type::Boolean && right == Type::Boolean => Some(Type::Boolean),
            Self::Plus if both_of_type_or_null(left, right, Type::Number) => Some(Type::Number),
            Self::Plus if both_of_type_or_null(left, right, Type::String) => Some(Type::String),
            Self::Times if both_of_type_or_null(left, right, Type::Number) => Some(Type::Number),
            Self::Unequal if left == right || left == Type::Null || right == Type::Null => {
                Some(Type::Boolean)
            }
            // allow date to string comparisons (validated in #visit_binary_expression_node)
            Self::Greater
            | Self::GreaterEqual
            | Self::Less
            | Self::LessEqual
            | Self::Equal
            | Self::Unequal
                if type_is_date_or_string(left) && type_is_date_or_string(right) =>
            {
                Some(Type::Boolean)
            }
            // allow interval to string comparisons (validated in #visit_binary_expression_node)
            Self::Greater
            | Self::GreaterEqual
            | Self::Less
            | Self::LessEqual
            | Self::Equal
            | Self::Unequal
                if (is_type_or_null(left, Type::Interval)
                    || is_type_or_null(left, Type::String))
                    && (is_type_or_null(right, Type::Interval)
                        || is_type_or_null(right, Type::String)) =>
            {
                Some(Type::Boolean)
            }
            // allow date interval additions and subtractions (validated in #visit_binary_expression_node)
            Self::Minus | Self::Plus
                if (left == Type::Date || left == Type::DateTime || left == Type::Interval)
                    && (right == Type::String || right == Type::Interval) =>
            {
                Some(left)
            }
            _ => None,
        }
    }

    pub fn accepts_unary_expression(&self, expression_type: Type) -> Option<Type> {
        match self {
            Self::Not if expression_type == Type::Boolean => Some(Type::Boolean),
            Self::Minus if expression_type == Type::Number => Some(Type::Number),
            _ => None,
        }
    }
}

#[inline]
fn type_is_date_or_string(t: Type) -> bool {
    is_type_or_null(t, Type::String)
        || is_type_or_null(t, Type::Date)
        || is_type_or_null(t, Type::DateTime)
}

#[inline]
fn is_type_or_null(s: Type, t: Type) -> bool {
    s == t || s == Type::Null
}

#[inline]
fn both_of_type_or_null(l: Type, r: Type, t: Type) -> bool {
    is_type_or_null(l, t) && is_type_or_null(r, t)
}

pub trait Visitor {
    fn visit_query_node(
        &mut self,
        query_node: &mut QueryNode,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    );

    fn visit_expression_statement(
        &mut self,
        expression_statement: &mut ExpressionStatement,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    );

    fn visit_post_tag_node(
        &mut self,
        post_tag_node: &mut PostTagNode,
        log: &mut Log,
        location: Location,
    );

    fn visit_binary_expression_node(
        &mut self,
        binary_expression_node: &mut BinaryExpressionNode,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    );

    fn visit_integer_literal_node(
        &mut self,
        integer_literal_node: &mut IntegerLiteralNode,
        log: &mut Log,
        location: Location,
    );

    fn visit_string_literal_node(
        &mut self,
        string_literal_node: &mut StringLiteralNode,
        log: &mut Log,
        location: Location,
    );

    fn visit_boolean_literal_node(
        &mut self,
        boolean_literal_node: &mut BooleanLiteralNode,
        log: &mut Log,
        location: Location,
    );

    fn visit_null_literal_node(
        &mut self,
        null_literal_node: &mut NullLiteralNode,
        log: &mut Log,
        location: Location,
    );

    fn visit_unary_expression_node(
        &mut self,
        unary_expression_node: &mut UnaryExpressionNode,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    );

    fn visit_attribute_node(
        &mut self,
        attribute_node: &mut AttributeNode,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    );

    fn visit_function_call_node(
        &mut self,
        function_call_node: &mut FunctionCallNode,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    );

    fn visit_modifier_node(
        &mut self,
        modifier_node: &mut ModifierNode,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    );

    fn visit_variable_node(
        &mut self,
        variable_node: &mut VariableNode,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    );
}

pub struct SemanticAnalysisVisitor {}

impl Visitor for SemanticAnalysisVisitor {
    fn visit_query_node(
        &mut self,
        query_node: &mut QueryNode,
        scope: &Scope,
        log: &mut Log,
        _location: Location,
    ) {
        for node in query_node.statements.iter_mut() {
            node.accept(self, scope, log);
        }
    }

    fn visit_expression_statement(
        &mut self,
        expression_statement: &mut ExpressionStatement,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    ) {
        expression_statement
            .expression_node
            .accept(self, scope, log);
        let return_type = expression_statement
            .expression_node
            .node_type
            .get_return_type(scope);
        if return_type != Type::Boolean && return_type != Type::Null {
            log.errors.push(Error {
                location,
                msg: format!("Expressions used as statement must evaluate to a boolean value but got type {return_type:?}")
            });
        }
    }

    fn visit_post_tag_node(
        &mut self,
        _post_tag_node: &mut PostTagNode,
        _log: &mut Log,
        _location: Location,
    ) {
    }

    fn visit_binary_expression_node(
        &mut self,
        binary_expression_node: &mut BinaryExpressionNode,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    ) {
        let op = binary_expression_node.op;
        let left = &mut binary_expression_node.left;
        let right = &mut binary_expression_node.right;
        let left_type = left.node_type.get_return_type(scope);
        let right_type = right.node_type.get_return_type(scope);

        left.accept(self, scope, log);
        right.accept(self, scope, log);

        if op
            .accepts_binary_expression(left_type, right_type)
            .is_none()
        {
            log.errors.push(Error {
                location,
                msg: format!(
                    "Operator {:?} cannot be applied to expressions of type {:?} and {:?}",
                    &op, &left_type, &right_type
                ),
            });
        }

        if op == Operator::FuzzyEqual {
            // if the patter does not contain any wildcards (% or _) explicitly, then wrap it in % to make it a fuzzy search
            if let Some(string_literal) = right.node_type.downcast_mut::<StringLiteralNode>()
                && !string_literal.val.contains('%')
                && !string_literal.val.contains('_')
            {
                string_literal.val = format!("%{}%", string_literal.val);
            }
        }

        fn validate_date_string(
            expression_node: &Node<dyn ExpressionNode>,
            date_type: Type,
            log: &mut Log,
        ) {
            let string_location = expression_node.location;
            let string_literal = expression_node
                .node_type
                .downcast_ref::<StringLiteralNode>();
            if let Some(string_literal) = string_literal {
                if date_type == Type::Date && iso8601::date(&string_literal.val).is_err() {
                    log.errors.push(Error {
                        location: string_location,
                        msg: format!("'{}' is not a valid date", &string_literal.val),
                    });
                } else if iso8601::datetime(&string_literal.val).is_err()
                    && iso8601::date(&string_literal.val).is_err()
                {
                    log.errors.push(Error {
                        location: string_location,
                        msg: format!("'{}' is not a valid date nor datetime", &string_literal.val),
                    });
                }
            } else {
                log.errors.push(Error {
                    location: string_location,
                    msg: String::from("Strings compared with dates must be string literals"),
                });
            }
        }

        fn validate_interval_string(expression_node: &mut Node<dyn ExpressionNode>, log: &mut Log) {
            let string_literal = expression_node
                .node_type
                .downcast_mut::<StringLiteralNode>();

            lazy_static! {
                static ref SQL_INTERVAL_REGEX: regex::Regex =
                    regex::Regex::new(r#"^((\d{1,8}):)?([0-5]?\d):([0-5]?\d)$"#)
                        .expect("Failed to compile SQL_INTERVAL_REGEX");
                static ref MM_SS_REGEX: regex::Regex =
                    regex::Regex::new(r#"^(\d+):(\d+)$"#).expect("Failed to compile MM_SS_REGEX");
            }

            if let Some(string_literal) = string_literal {
                let is_sql_interval_regex = SQL_INTERVAL_REGEX.is_match(&string_literal.val);
                // convert interval '3:21' to '00:03:21', overriding the default sql behaviour that would make it '03:21:00' and making it MM:SS instead of HH:MM
                if is_sql_interval_regex {
                    let interval_string = MM_SS_REGEX.replace(&string_literal.val, "00:$1:$2");
                    log::debug!(
                        "Converted MM:SS interval string '{}' to '{}'",
                        &string_literal.val,
                        &interval_string
                    );
                    string_literal.val = interval_string.to_string();
                }

                if !is_sql_interval_regex
                    && pg_interval::Interval::from_iso(&string_literal.val).is_err()
                    && pg_interval::Interval::from_postgres(&string_literal.val).is_err()
                {
                    log.errors.push(Error {
                        location: expression_node.location,
                        msg: format!(
                            "'{}' is not a valid postgres, SQL or ISO 8601 interval",
                            &string_literal.val
                        ),
                    });
                }
            } else {
                log.errors.push(Error {
                    location: expression_node.location,
                    msg: String::from("Strings used as interval must be a string literal"),
                });
            }
        }

        // handle date comparisons and interval additions or subtractions
        if (op == Operator::Plus || op == Operator::Minus)
            && (left_type == Type::Date
                || left_type == Type::DateTime
                || left_type == Type::Interval)
            && right_type == Type::String
        {
            validate_interval_string(right, log);
        } else if left_type == Type::String
            && (right_type == Type::Date || right_type == Type::DateTime)
        {
            validate_date_string(left, right_type, log);
        } else if right_type == Type::String
            && (left_type == Type::Date || left_type == Type::DateTime)
        {
            validate_date_string(right, left_type, log);
        } else if left_type == Type::String && right_type == Type::Interval {
            validate_interval_string(left, log);
        } else if right_type == Type::String && left_type == Type::Interval {
            validate_interval_string(right, log);
        }
    }

    fn visit_integer_literal_node(
        &mut self,
        _integer_literal_node: &mut IntegerLiteralNode,
        _log: &mut Log,
        _location: Location,
    ) {
    }

    fn visit_string_literal_node(
        &mut self,
        _string_literal_node: &mut StringLiteralNode,
        _log: &mut Log,
        _location: Location,
    ) {
    }

    fn visit_boolean_literal_node(
        &mut self,
        _boolean_literal_node: &mut BooleanLiteralNode,
        _log: &mut Log,
        _location: Location,
    ) {
    }

    fn visit_null_literal_node(
        &mut self,
        _null_literal_node: &mut NullLiteralNode,
        _log: &mut Log,
        _location: Location,
    ) {
    }

    fn visit_unary_expression_node(
        &mut self,
        unary_expression_node: &mut UnaryExpressionNode,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    ) {
        let operand = &mut unary_expression_node.operand;
        let expression_type = operand.node_type.get_return_type(scope);
        let op = unary_expression_node.op;

        operand.accept(self, scope, log);
        if op.accepts_unary_expression(expression_type).is_none() {
            log.errors.push(Error {
                location,
                msg: format!(
                    "Operator {op:?} cannot be applied to unary expression of type {expression_type:?}"
                ),
            });
        }
    }

    fn visit_attribute_node(
        &mut self,
        attribute_node: &mut AttributeNode,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    ) {
        let identifier: &str = &attribute_node.identifier;
        if !scope.get_attributes().contains_key(identifier) {
            log.errors.push(Error {
                location,
                msg: format!("No such attribute '{identifier}'"),
            });
        }
    }

    fn visit_function_call_node(
        &mut self,
        function_call_node: &mut FunctionCallNode,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    ) {
        for argument in function_call_node.arguments.iter_mut() {
            argument.accept(self, scope, log);
        }

        let identifier: &str = &function_call_node.identifier;
        if let Some(function) = scope.get_functions().get(identifier) {
            let arguments = &function_call_node.arguments;
            (function.accept_arguments)(&function.params, arguments, scope, location, log);
        } else {
            log.errors.push(Error {
                location,
                msg: format!("No such function '{identifier}'"),
            });
        }
    }

    fn visit_modifier_node(
        &mut self,
        modifier_node: &mut ModifierNode,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    ) {
        for argument in modifier_node.arguments.iter_mut() {
            argument.accept(self, scope, log);
        }

        let identifier: &str = &modifier_node.identifier;
        if let Some(modifier) = scope.get_modifiers().get(identifier) {
            let arguments = &modifier_node.arguments;
            (modifier.accept_arguments)(&modifier.params, arguments, scope, location, log);
        } else {
            log.errors.push(Error {
                location,
                msg: format!("No such modifier '{identifier}'"),
            });
        }
    }

    fn visit_variable_node(
        &mut self,
        variable_node: &mut VariableNode,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    ) {
        let identifier: &str = &variable_node.identifier;
        if !scope.get_variables().contains_key(identifier) {
            log.errors.push(Error {
                location,
                msg: format!("No such variable '{identifier}'"),
            });
        }
    }
}

pub struct QueryBuilderVisitor<'p> {
    pub ctes: HashMap<String, Cte>,
    pub where_expressions: Vec<String>,
    pub query_parameters: &'p mut QueryParameters,
    buffer: Option<String>,
}

impl<'p> QueryBuilderVisitor<'p> {
    pub fn new(query_parameters: &'p mut QueryParameters) -> Self {
        QueryBuilderVisitor {
            ctes: HashMap::new(),
            where_expressions: Vec::new(),
            query_parameters,
            buffer: None,
        }
    }
}

impl QueryBuilderVisitor<'_> {
    pub fn visit_limit_modifier(
        &mut self,
        arguments: &mut [Box<Node<dyn ExpressionNode>>],
        scope: &Scope,
        log: &mut Log,
    ) {
        if !arguments.is_empty() {
            self.buffer = Some(String::new());
            arguments[0].accept(self, scope, log);
            self.query_parameters
                .pagination
                .get_or_insert_default()
                .limit = self.buffer.take();
        }
    }

    pub fn visit_sort_modifier(
        &mut self,
        arguments: &mut [Box<Node<dyn ExpressionNode>>],
        scope: &Scope,
        log: &mut Log,
        location: Location,
    ) {
        if !arguments.is_empty() {
            if self.query_parameters.ordering.len() >= 2 {
                log.errors.push(Error {
                    location,
                    msg: String::from("Cannot sort by more than two attributes"),
                });
            }
            self.buffer = Some(String::new());
            let attr_arg = &arguments[0];
            let attribute_node = attr_arg.node_type.downcast_ref::<AttributeNode>();
            let attribute = attribute_node.and_then(|attribute_node| {
                scope
                    .get_attributes()
                    .get(attribute_node.identifier.as_str())
                    .cloned()
            });
            let is_string = attr_arg.node_type.get_return_type(scope) == Type::String;
            let nullable = attribute
                .as_ref()
                .is_some_and(|attribute| attribute.nullable);
            let table = attribute.map_or("--invalid--", |attribute| attribute.table);
            if !self.query_parameters.ordering.is_empty() {
                let last_ordering = &self.query_parameters.ordering[0];
                if last_ordering.table != table {
                    log.errors.push(Error {
                        location,
                        msg: format!(
                            "Cannot sort by attributes from different tables ({} and {})",
                            last_ordering.table, table
                        ),
                    });
                }
            }

            if is_string {
                self.write_buff("LOWER(");
            }
            arguments[0].accept(self, scope, log);
            if is_string {
                self.write_buff(")");
            }
            let expression = self.buffer.take().unwrap();
            let direction = if arguments.len() > 1 {
                match arguments[1].node_type.downcast_ref::<StringLiteralNode>() {
                    Some(string_literal)
                        if "desc".eq_ignore_ascii_case(&string_literal.val)
                            || "descending".eq_ignore_ascii_case(&string_literal.val) =>
                    {
                        Direction::Descending
                    }
                    _ => Direction::Ascending,
                }
            } else {
                Direction::Ascending
            };

            self.query_parameters.ordering.push(Ordering {
                expression,
                direction,
                nullable,
                table,
            });
        }
    }

    pub(crate) fn write_buff(&mut self, s: &str) {
        self.buffer
            .as_mut()
            .expect("No current querybuilder buffer")
            .push_str(s);
    }
}

impl Visitor for QueryBuilderVisitor<'_> {
    fn visit_query_node(
        &mut self,
        query_node: &mut QueryNode,
        scope: &Scope,
        log: &mut Log,
        _location: Location,
    ) {
        for node in query_node.statements.iter_mut() {
            node.accept(self, scope, log);
        }
    }

    fn visit_expression_statement(
        &mut self,
        expression_statement: &mut ExpressionStatement,
        scope: &Scope,
        log: &mut Log,
        _location: Location,
    ) {
        self.buffer = Some(String::new());
        expression_statement
            .expression_node
            .accept(self, scope, log);
        let expression_string = self.buffer.take().unwrap();
        self.where_expressions.push(expression_string);
    }

    fn visit_post_tag_node(
        &mut self,
        post_tag_node: &mut PostTagNode,
        _log: &mut Log,
        _location: Location,
    ) {
        let idx = self.ctes.len();
        let tag_name = sanitize_string_literal(&post_tag_node.identifier);

        let cte = self
            .ctes
            .entry(tag_name)
            .or_insert_with_key(|tag_name| Cte {
                idx,
                expression: format!(
                    "cte{idx} AS (
                        SELECT unnest(
                            tag.pk
                            || array(SELECT fk_target FROM tag_alias WHERE fk_source = tag.pk)
                            || array(SELECT fk_source FROM tag_alias WHERE fk_target = tag.pk)
                            || array(SELECT fk_child FROM tag_closure_table WHERE fk_parent = tag.pk)
                        ) AS tag_keys
                        FROM tag WHERE lower(tag.tag_name) = '{tag_name}'
                    )"
                ),
            });

        let cte_idx = cte.idx;
        let base_table_name = self.query_parameters.base_table_name;
        self.write_buff(&format!("EXISTS(SELECT * FROM {base_table_name}_tag WHERE fk_{base_table_name} = {base_table_name}.pk AND fk_tag IN(SELECT tag_keys FROM cte{cte_idx}))"));
    }

    fn visit_binary_expression_node(
        &mut self,
        binary_expression_node: &mut BinaryExpressionNode,
        scope: &Scope,
        log: &mut Log,
        _location: Location,
    ) {
        let op = binary_expression_node.op;
        let left = &mut binary_expression_node.left;
        let left_type = left.node_type.get_return_type(scope);
        let right = &mut binary_expression_node.right;
        let right_type = right.node_type.get_return_type(scope);
        let binary_types = (left_type, right_type);

        self.write_buff("(");

        if (op == Operator::Plus || op == Operator::Minus)
            && (left_type == Type::Date
                || left_type == Type::DateTime
                || left_type == Type::Interval)
            && right_type == Type::String
        {
            // add explicit types for date interval addition / subtraction
            left.accept(self, scope, log);
            if left_type == Type::DateTime {
                self.write_buff("::timestamp ");
            } else if left_type == Type::Interval {
                self.write_buff("::interval ");
            } else {
                self.write_buff("::date ");
            }
            self.write_buff(op.get_sql_string(Some(binary_types)));
            self.write_buff(" interval ");
            right.accept(self, scope, log);
        } else if op == Operator::FuzzyEqual
            || ((op == Operator::Equal || op == Operator::Unequal)
                && left_type == Type::String
                && right_type == Type::String)
        {
            // for fuzzy equals, explicitly add an IS NOT NULL condition on the left side
            // this helps the query planner when used with column that are frequently NULL
            if op == Operator::FuzzyEqual {
                left.accept(self, scope, log);
                self.write_buff(" IS NOT NULL AND ");
            }

            // case insensitive matching for fuzzy equals and string equals
            self.write_buff("LOWER(");
            left.accept(self, scope, log);
            self.write_buff(") ");
            self.write_buff(op.get_sql_string(Some(binary_types)));
            self.write_buff(" LOWER(");
            right.accept(self, scope, log);
            self.write_buff(")");

            // when using the equals operator on post.description, add the LENGTH(description) < 2048 condition in order to use index post_description_idx
            // for fuzzy equals, the post_description_gin_idx GIN index may be used

            fn is_description_attribute(node: &Node<dyn ExpressionNode>) -> bool {
                match node.node_type.downcast_ref::<AttributeNode>() {
                    Some(attribute_node) => attribute_node.identifier == "description",
                    None => false,
                }
            }

            if op == Operator::Equal
                && (is_description_attribute(left) || is_description_attribute(right))
            {
                self.write_buff(" AND LENGTH(description) < 2048");
            }
        } else {
            left.accept(self, scope, log);
            self.write_buff(" ");
            self.write_buff(op.get_sql_string(Some(binary_types)));
            self.write_buff(" ");
            right.accept(self, scope, log);
        }

        self.write_buff(")");
    }

    fn visit_integer_literal_node(
        &mut self,
        integer_literal_node: &mut IntegerLiteralNode,
        _log: &mut Log,
        _location: Location,
    ) {
        self.write_buff(&integer_literal_node.val.to_string());
    }

    fn visit_string_literal_node(
        &mut self,
        string_literal_node: &mut StringLiteralNode,
        _log: &mut Log,
        _location: Location,
    ) {
        self.write_buff(&format!(
            "'{}'",
            &sanitize_string_literal(&string_literal_node.val)
        ));
    }

    fn visit_boolean_literal_node(
        &mut self,
        boolean_literal_node: &mut BooleanLiteralNode,
        _log: &mut Log,
        _location: Location,
    ) {
        self.write_buff(&boolean_literal_node.val.to_string());
    }

    fn visit_null_literal_node(
        &mut self,
        _null_literal_node: &mut NullLiteralNode,
        _log: &mut Log,
        _location: Location,
    ) {
        self.write_buff("NULL");
    }

    fn visit_unary_expression_node(
        &mut self,
        unary_expression_node: &mut UnaryExpressionNode,
        scope: &Scope,
        log: &mut Log,
        _location: Location,
    ) {
        self.write_buff(unary_expression_node.op.get_sql_string(None));
        self.write_buff(" (");
        unary_expression_node.operand.accept(self, scope, log);
        self.write_buff(")");
    }

    fn visit_attribute_node(
        &mut self,
        attribute_node: &mut AttributeNode,
        scope: &Scope,
        _log: &mut Log,
        _location: Location,
    ) {
        let identifier: &str = &attribute_node.identifier;
        if let Some(attribute) = scope.get_attributes().get(identifier) {
            self.write_buff(&attribute.selection_expression);
        } else {
            self.write_buff("NULL");
        }
    }

    fn visit_function_call_node(
        &mut self,
        function_call_node: &mut FunctionCallNode,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    ) {
        let identifier: &str = &function_call_node.identifier;
        if let Some(function) = scope.get_functions().get(identifier) {
            let arguments = &mut function_call_node.arguments;
            (function.write_expression_fn)(self, arguments, scope, location, log);
        } else {
            self.write_buff("NULL");
        }
    }

    fn visit_modifier_node(
        &mut self,
        modifier_node: &mut ModifierNode,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    ) {
        let identifier: &str = &modifier_node.identifier;
        if let Some(modifier) = scope.get_modifiers().get(identifier) {
            (modifier.visit_query_builder)(
                self,
                &mut modifier_node.arguments,
                scope,
                log,
                location,
            );
        }
    }

    fn visit_variable_node(
        &mut self,
        variable_node: &mut VariableNode,
        scope: &Scope,
        _log: &mut Log,
        _location: Location,
    ) {
        let identifier: &str = &variable_node.identifier;
        if let Some(variable) = scope.get_variables().get(identifier) {
            let variable_expression =
                (variable.get_expression_fn)(&self.query_parameters.variables);
            self.write_buff(&variable_expression);
        } else {
            self.write_buff("NULL");
        }
    }
}

#[inline]
pub fn sanitize_string_literal(val: &str) -> String {
    val.replace('\'', "''")
}

#[derive(Clone, Debug)]
pub struct Node<T: NodeType + Send + ?Sized> {
    pub location: Location,
    pub node_type: T,
}

impl<T: NodeType + Send + ?Sized> Node<T> {
    pub fn accept(&mut self, visitor: &mut dyn Visitor, scope: &Scope, log: &mut Log) {
        self.node_type.accept(visitor, scope, log, self.location);
    }
}

impl Node<dyn ExpressionNode> {
    pub fn clone_boxed_node(&self) -> Box<Node<dyn ExpressionNode>> {
        self.node_type.clone_boxed_node(self)
    }
}

pub trait NodeType: Downcast + Debug + Send {
    fn accept(
        &mut self,
        visitor: &mut dyn Visitor,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    );
}

impl_downcast!(NodeType);

#[derive(Debug)]
pub struct QueryNode {
    pub statements: Vec<Box<Node<dyn StatementNode>>>,
}

impl NodeType for QueryNode {
    fn accept(
        &mut self,
        visitor: &mut dyn Visitor,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    ) {
        visitor.visit_query_node(self, scope, log, location);
    }
}

pub trait StatementNode: NodeType + Downcast + Send + Sync {
    /// Return an iterator over the nested expressions inside this expression (e.g. left and right expression in binary expression).
    fn unnest(&self) -> UnnestIter<'_> {
        UnnestIter::Empty
    }
}

impl_downcast!(StatementNode);

#[derive(Debug)]
pub struct ExpressionStatement {
    pub expression_node: Box<Node<dyn ExpressionNode>>,
}

impl NodeType for ExpressionStatement {
    fn accept(
        &mut self,
        visitor: &mut dyn Visitor,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    ) {
        visitor.visit_expression_statement(self, scope, log, location);
    }
}

impl StatementNode for ExpressionStatement {
    fn unnest(&self) -> UnnestIter<'_> {
        UnnestIter::ExpressionStatement {
            expression_statement: self,
            yielded: false,
        }
    }
}

#[derive(Debug)]
pub struct ModifierNode {
    pub identifier: String,
    pub arguments: Vec<Box<Node<dyn ExpressionNode>>>,
}

impl NodeType for ModifierNode {
    fn accept(
        &mut self,
        visitor: &mut dyn Visitor,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    ) {
        visitor.visit_modifier_node(self, scope, log, location);
    }
}

impl StatementNode for ModifierNode {
    fn unnest(&self) -> UnnestIter<'_> {
        UnnestIter::ModifierNode {
            modifier_node: self,
            idx: 0,
        }
    }
}

pub trait ExpressionNode: NodeType + Downcast + Send + Sync {
    fn get_return_type(&self, scope: &Scope) -> Type;

    /// Return an iterator over the nested expressions inside this expression (e.g. left and right expression in binary expression).
    fn unnest(&self) -> UnnestIter<'_> {
        UnnestIter::Empty
    }

    fn clone_boxed_node(&self, node: &Node<dyn ExpressionNode>) -> Box<Node<dyn ExpressionNode>>;
}

pub fn combine_expressions(
    left: Box<Node<dyn ExpressionNode>>,
    right: Box<Node<dyn ExpressionNode>>,
    op: Operator,
) -> Box<Node<dyn ExpressionNode>> {
    Box::new(Node {
        location: Location {
            start: left.location.start,
            end: right.location.end,
        },
        node_type: BinaryExpressionNode { left, op, right },
    })
}

impl_downcast!(ExpressionNode);

pub enum UnnestIter<'a> {
    BinaryExpression {
        binary_expression_node: &'a BinaryExpressionNode,
        yielded_left: bool,
        yielded_right: bool,
    },
    Empty,
    ExpressionStatement {
        expression_statement: &'a ExpressionStatement,
        yielded: bool,
    },
    FunctionCallNode {
        function_call_node: &'a FunctionCallNode,
        idx: usize,
    },
    ModifierNode {
        modifier_node: &'a ModifierNode,
        idx: usize,
    },
    UnaryExpression {
        unary_expression_node: &'a UnaryExpressionNode,
        yielded: bool,
    },
}

impl<'a> Iterator for UnnestIter<'a> {
    type Item = &'a Box<Node<dyn ExpressionNode>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::BinaryExpression {
                binary_expression_node,
                yielded_left,
                yielded_right,
            } => {
                if !*yielded_left {
                    *yielded_left = true;
                    Some(&binary_expression_node.left)
                } else if !*yielded_right {
                    *yielded_right = true;
                    Some(&binary_expression_node.right)
                } else {
                    None
                }
            }
            Self::Empty => None,
            Self::ExpressionStatement {
                expression_statement,
                yielded,
            } => {
                if !*yielded {
                    *yielded = true;
                    Some(&expression_statement.expression_node)
                } else {
                    None
                }
            }
            Self::FunctionCallNode {
                function_call_node,
                idx,
            } => {
                if *idx < function_call_node.arguments.len() {
                    let argument_expr = &function_call_node.arguments[*idx];
                    *idx += 1;
                    Some(argument_expr)
                } else {
                    None
                }
            }
            Self::ModifierNode { modifier_node, idx } => {
                if *idx < modifier_node.arguments.len() {
                    let argument_expr = &modifier_node.arguments[*idx];
                    *idx += 1;
                    Some(argument_expr)
                } else {
                    None
                }
            }
            Self::UnaryExpression {
                unary_expression_node,
                yielded,
            } => {
                if !*yielded {
                    *yielded = true;
                    Some(&unary_expression_node.operand)
                } else {
                    None
                }
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct PostTagNode {
    pub identifier: String,
}

impl NodeType for PostTagNode {
    fn accept(
        &mut self,
        visitor: &mut dyn Visitor,
        _scope: &Scope,
        log: &mut Log,
        location: Location,
    ) {
        visitor.visit_post_tag_node(self, log, location);
    }
}

impl ExpressionNode for PostTagNode {
    fn get_return_type(&self, _scope: &Scope) -> Type {
        Type::Boolean
    }

    fn clone_boxed_node(&self, node: &Node<dyn ExpressionNode>) -> Box<Node<dyn ExpressionNode>> {
        Box::new(Node {
            location: node.location,
            node_type: PostTagNode {
                identifier: self.identifier.clone(),
            },
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct AttributeNode {
    pub identifier: String,
}

impl NodeType for AttributeNode {
    fn accept(
        &mut self,
        visitor: &mut dyn Visitor,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    ) {
        visitor.visit_attribute_node(self, scope, log, location);
    }
}

impl ExpressionNode for AttributeNode {
    fn get_return_type(&self, scope: &Scope) -> Type {
        let identifier: &str = &self.identifier;
        if let Some(attribute) = scope.get_attributes().get(identifier) {
            attribute.return_type
        } else {
            Type::Void
        }
    }

    fn clone_boxed_node(&self, node: &Node<dyn ExpressionNode>) -> Box<Node<dyn ExpressionNode>> {
        Box::new(Node {
            location: node.location,
            node_type: AttributeNode {
                identifier: self.identifier.clone(),
            },
        })
    }
}

#[derive(Debug)]
pub struct FunctionCallNode {
    pub identifier: String,
    pub arguments: Vec<Box<Node<dyn ExpressionNode>>>,
}

impl NodeType for FunctionCallNode {
    fn accept(
        &mut self,
        visitor: &mut dyn Visitor,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    ) {
        visitor.visit_function_call_node(self, scope, log, location);
    }
}

impl ExpressionNode for FunctionCallNode {
    fn get_return_type(&self, scope: &Scope) -> Type {
        let identifier: &str = &self.identifier;
        if let Some(function) = scope.get_functions().get(identifier) {
            function.return_type
        } else {
            Type::Void
        }
    }

    fn unnest(&self) -> UnnestIter<'_> {
        UnnestIter::FunctionCallNode {
            function_call_node: self,
            idx: 0,
        }
    }

    fn clone_boxed_node(&self, node: &Node<dyn ExpressionNode>) -> Box<Node<dyn ExpressionNode>> {
        Box::new(Node {
            location: node.location,
            node_type: FunctionCallNode {
                identifier: self.identifier.clone(),
                arguments: self
                    .arguments
                    .iter()
                    .map(|arg| arg.clone_boxed_node())
                    .collect(),
            },
        })
    }
}

#[derive(Debug)]
pub struct BinaryExpressionNode {
    pub left: Box<Node<dyn ExpressionNode>>,
    pub op: Operator,
    pub right: Box<Node<dyn ExpressionNode>>,
}

impl NodeType for BinaryExpressionNode {
    fn accept(
        &mut self,
        visitor: &mut dyn Visitor,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    ) {
        visitor.visit_binary_expression_node(self, scope, log, location);
    }
}

impl ExpressionNode for BinaryExpressionNode {
    fn get_return_type(&self, scope: &Scope) -> Type {
        self.op
            .accepts_binary_expression(
                self.left.node_type.get_return_type(scope),
                self.right.node_type.get_return_type(scope),
            )
            .unwrap_or(Type::Void)
    }

    fn unnest(&self) -> UnnestIter<'_> {
        UnnestIter::BinaryExpression {
            binary_expression_node: self,
            yielded_left: false,
            yielded_right: false,
        }
    }

    fn clone_boxed_node(&self, node: &Node<dyn ExpressionNode>) -> Box<Node<dyn ExpressionNode>> {
        Box::new(Node {
            location: node.location,
            node_type: BinaryExpressionNode {
                left: self.left.clone_boxed_node(),
                op: self.op,
                right: self.right.clone_boxed_node(),
            },
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct IntegerLiteralNode {
    pub val: i32,
}

impl NodeType for IntegerLiteralNode {
    fn accept(
        &mut self,
        visitor: &mut dyn Visitor,
        _scope: &Scope,
        log: &mut Log,
        location: Location,
    ) {
        visitor.visit_integer_literal_node(self, log, location);
    }
}

impl ExpressionNode for IntegerLiteralNode {
    fn get_return_type(&self, _scope: &Scope) -> Type {
        Type::Number
    }

    fn clone_boxed_node(&self, node: &Node<dyn ExpressionNode>) -> Box<Node<dyn ExpressionNode>> {
        Box::new(Node {
            location: node.location,
            node_type: IntegerLiteralNode { val: self.val },
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct StringLiteralNode {
    pub val: String,
}

impl NodeType for StringLiteralNode {
    fn accept(
        &mut self,
        visitor: &mut dyn Visitor,
        _scope: &Scope,
        log: &mut Log,
        location: Location,
    ) {
        visitor.visit_string_literal_node(self, log, location);
    }
}

impl ExpressionNode for StringLiteralNode {
    fn get_return_type(&self, _scope: &Scope) -> Type {
        Type::String
    }

    fn clone_boxed_node(&self, node: &Node<dyn ExpressionNode>) -> Box<Node<dyn ExpressionNode>> {
        Box::new(Node {
            location: node.location,
            node_type: StringLiteralNode {
                val: self.val.clone(),
            },
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct BooleanLiteralNode {
    pub val: bool,
}

impl NodeType for BooleanLiteralNode {
    fn accept(
        &mut self,
        visitor: &mut dyn Visitor,
        _scope: &Scope,
        log: &mut Log,
        location: Location,
    ) {
        visitor.visit_boolean_literal_node(self, log, location);
    }
}

impl ExpressionNode for BooleanLiteralNode {
    fn get_return_type(&self, _scope: &Scope) -> Type {
        Type::Boolean
    }

    fn clone_boxed_node(&self, node: &Node<dyn ExpressionNode>) -> Box<Node<dyn ExpressionNode>> {
        Box::new(Node {
            location: node.location,
            node_type: BooleanLiteralNode { val: self.val },
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct NullLiteralNode {}

impl NodeType for NullLiteralNode {
    fn accept(
        &mut self,
        visitor: &mut dyn Visitor,
        _scope: &Scope,
        log: &mut Log,
        location: Location,
    ) {
        visitor.visit_null_literal_node(self, log, location);
    }
}

impl ExpressionNode for NullLiteralNode {
    fn get_return_type(&self, _scope: &Scope) -> Type {
        Type::Null
    }

    fn clone_boxed_node(&self, node: &Node<dyn ExpressionNode>) -> Box<Node<dyn ExpressionNode>> {
        Box::new(Node {
            location: node.location,
            node_type: NullLiteralNode {},
        })
    }
}

#[derive(Debug)]
pub struct UnaryExpressionNode {
    pub op: Operator,
    pub operand: Box<Node<dyn ExpressionNode>>,
}

impl NodeType for UnaryExpressionNode {
    fn accept(
        &mut self,
        visitor: &mut dyn Visitor,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    ) {
        visitor.visit_unary_expression_node(self, scope, log, location);
    }
}

impl ExpressionNode for UnaryExpressionNode {
    fn get_return_type(&self, scope: &Scope) -> Type {
        self.op
            .accepts_unary_expression(self.operand.node_type.get_return_type(scope))
            .unwrap_or(Type::Void)
    }

    fn unnest(&self) -> UnnestIter<'_> {
        UnnestIter::UnaryExpression {
            unary_expression_node: self,
            yielded: false,
        }
    }

    fn clone_boxed_node(&self, node: &Node<dyn ExpressionNode>) -> Box<Node<dyn ExpressionNode>> {
        Box::new(Node {
            location: node.location,
            node_type: UnaryExpressionNode {
                op: self.op,
                operand: self.operand.clone_boxed_node(),
            },
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct VariableNode {
    pub identifier: String,
}

impl NodeType for VariableNode {
    fn accept(
        &mut self,
        visitor: &mut dyn Visitor,
        scope: &Scope,
        log: &mut Log,
        location: Location,
    ) {
        visitor.visit_variable_node(self, scope, log, location);
    }
}

impl ExpressionNode for VariableNode {
    fn get_return_type(&self, scope: &Scope) -> Type {
        let identifier: &str = &self.identifier;
        if let Some(variable) = scope.get_variables().get(identifier) {
            variable.return_type
        } else {
            Type::Void
        }
    }

    fn clone_boxed_node(&self, node: &Node<dyn ExpressionNode>) -> Box<Node<dyn ExpressionNode>> {
        Box::new(Node {
            location: node.location,
            node_type: VariableNode {
                identifier: self.identifier.clone(),
            },
        })
    }
}
