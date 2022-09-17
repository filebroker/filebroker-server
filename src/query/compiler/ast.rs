use std::{collections::HashMap, fmt::Debug};

use downcast_rs::{impl_downcast, Downcast};

use crate::query::{Direction, Ordering, QueryParameters};

use super::{dict, dict::Type, lexer::Tag, Cte, Error, Location, Log};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Operator {
    And,
    Divide,
    Equal,
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
            Self::Unequal => "!=",
        }
    }

    pub fn accepts_binary_expression(&self, left: Type, right: Type) -> Option<Type> {
        match self {
            Self::And if both_of_type_or_null(left, right, Type::Boolean) => Some(Type::Boolean),
            Self::Divide if both_of_type_or_null(left, right, Type::Number) => Some(Type::Number),
            Self::Equal if left == right || left == Type::Null || right == Type::Null => {
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
            // allow date interval additions and subtractions (validated in #visit_binary_expression_node)
            Self::Minus | Self::Plus
                if (left == Type::Date || left == Type::DateTime) && right == Type::String =>
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
    fn visit_query_node(&mut self, query_node: &QueryNode, log: &mut Log, location: Location);

    fn visit_expression_statement(
        &mut self,
        expression_statement: &ExpressionStatement,
        log: &mut Log,
        location: Location,
    );

    fn visit_post_tag_node(
        &mut self,
        post_tag_node: &PostTagNode,
        log: &mut Log,
        location: Location,
    );

    fn visit_binary_expression_node(
        &mut self,
        binary_expression_node: &BinaryExpressionNode,
        log: &mut Log,
        location: Location,
    );

    fn visit_integer_literal_node(
        &mut self,
        integer_literal_node: &IntegerLiteralNode,
        log: &mut Log,
        location: Location,
    );

    fn visit_string_literal_node(
        &mut self,
        string_literal_node: &StringLiteralNode,
        log: &mut Log,
        location: Location,
    );

    fn visit_boolean_literal_node(
        &mut self,
        boolean_literal_node: &BooleanLiteralNode,
        log: &mut Log,
        location: Location,
    );

    fn visit_null_literal_node(
        &mut self,
        null_literal_node: &NullLiteralNode,
        log: &mut Log,
        location: Location,
    );

    fn visit_unary_expression_node(
        &mut self,
        unary_expression_node: &UnaryExpressionNode,
        log: &mut Log,
        location: Location,
    );

    fn visit_attribute_node(
        &mut self,
        attribute_node: &AttributeNode,
        log: &mut Log,
        location: Location,
    );

    fn visit_function_call_node(
        &mut self,
        function_call_node: &FunctionCallNode,
        log: &mut Log,
        location: Location,
    );

    fn visit_modifier_node(
        &mut self,
        modifier_node: &ModifierNode,
        log: &mut Log,
        location: Location,
    );

    fn visit_variable_node(
        &mut self,
        variable_node: &VariableNode,
        log: &mut Log,
        location: Location,
    );
}

pub struct SemanticAnalysisVisitor {}

impl Visitor for SemanticAnalysisVisitor {
    fn visit_query_node(&mut self, query_node: &QueryNode, log: &mut Log, _location: Location) {
        for node in query_node.statements.iter() {
            node.accept(self, log);
        }
    }

    fn visit_expression_statement(
        &mut self,
        expression_statement: &ExpressionStatement,
        log: &mut Log,
        location: Location,
    ) {
        expression_statement.expression_node.accept(self, log);
        let return_type = expression_statement
            .expression_node
            .node_type
            .get_return_type();
        if return_type != Type::Boolean && return_type != Type::Null {
            log.errors.push(Error {
                location,
                msg: format!("Expressions used as statement must evaluate to a boolean value but got type {:?}", return_type)
            });
        }
    }

    fn visit_post_tag_node(
        &mut self,
        _post_tag_node: &PostTagNode,
        _log: &mut Log,
        _location: Location,
    ) {
    }

    fn visit_binary_expression_node(
        &mut self,
        binary_expression_node: &BinaryExpressionNode,
        log: &mut Log,
        location: Location,
    ) {
        let op = binary_expression_node.op;
        let left = &binary_expression_node.left;
        let right = &binary_expression_node.right;
        let left_type = left.node_type.get_return_type();
        let right_type = right.node_type.get_return_type();

        left.accept(self, log);
        right.accept(self, log);

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

        // handle date comparisons and interval additions or subtractions
        if (op == Operator::Plus || op == Operator::Minus)
            && (left_type == Type::Date || left_type == Type::DateTime)
            && right_type == Type::String
        {
            let string_literal = right.node_type.downcast_ref::<StringLiteralNode>();
            if let Some(string_literal) = string_literal {
                if pg_interval::Interval::from_postgres(&string_literal.val).is_err() {
                    log.errors.push(Error {
                        location: right.location,
                        msg: format!("'{}' is not a valid postgres interval", &string_literal.val),
                    });
                }
            } else {
                log.errors.push(Error {
                    location: right.location,
                    msg: String::from("Strings added to dates must be interval string literals"),
                });
            }
        } else if left_type == Type::String
            && (right_type == Type::Date || right_type == Type::DateTime)
        {
            validate_date_string(left, right_type, log);
        } else if right_type == Type::String
            && (left_type == Type::Date || left_type == Type::DateTime)
        {
            validate_date_string(right, left_type, log);
        }
    }

    fn visit_integer_literal_node(
        &mut self,
        _integer_literal_node: &IntegerLiteralNode,
        _log: &mut Log,
        _location: Location,
    ) {
    }

    fn visit_string_literal_node(
        &mut self,
        _string_literal_node: &StringLiteralNode,
        _log: &mut Log,
        _location: Location,
    ) {
    }

    fn visit_boolean_literal_node(
        &mut self,
        _boolean_literal_node: &BooleanLiteralNode,
        _log: &mut Log,
        _location: Location,
    ) {
    }

    fn visit_null_literal_node(
        &mut self,
        _null_literal_node: &NullLiteralNode,
        _log: &mut Log,
        _location: Location,
    ) {
    }

    fn visit_unary_expression_node(
        &mut self,
        unary_expression_node: &UnaryExpressionNode,
        log: &mut Log,
        location: Location,
    ) {
        let operand = &unary_expression_node.operand;
        let expression_type = operand.node_type.get_return_type();
        let op = unary_expression_node.op;

        operand.accept(self, log);
        if op.accepts_unary_expression(expression_type).is_none() {
            log.errors.push(Error {
                location,
                msg: format!(
                    "Operator {:?} cannot be applied to unary expression of type {:?}",
                    op, expression_type
                ),
            });
        }
    }

    fn visit_attribute_node(
        &mut self,
        attribute_node: &AttributeNode,
        log: &mut Log,
        location: Location,
    ) {
        let identifier: &str = &attribute_node.identifier;
        if !dict::ATTRIBUTES.contains_key(identifier) {
            log.errors.push(Error {
                location,
                msg: format!("No such attribute '{}'", identifier),
            });
        }
    }

    fn visit_function_call_node(
        &mut self,
        function_call_node: &FunctionCallNode,
        log: &mut Log,
        location: Location,
    ) {
        let identifier: &str = &function_call_node.identifier;
        if let Some(function) = dict::FUNCTIONS.get(identifier) {
            let arguments = &function_call_node.arguments;
            (function.accept_arguments)(&function.params, arguments, location, log);
        } else {
            log.errors.push(Error {
                location,
                msg: format!("No such function '{}'", identifier),
            });
        }
    }

    fn visit_modifier_node(
        &mut self,
        modifier_node: &ModifierNode,
        log: &mut Log,
        location: Location,
    ) {
        let identifier: &str = &modifier_node.identifier;
        if let Some(modifier) = dict::MODIFIERS.get(identifier) {
            let arguments = &modifier_node.arguments;
            (modifier.accept_arguments)(&modifier.params, arguments, location, log);
        } else {
            log.errors.push(Error {
                location,
                msg: format!("No such modifier '{}'", identifier),
            });
        }
    }

    fn visit_variable_node(
        &mut self,
        variable_node: &VariableNode,
        log: &mut Log,
        location: Location,
    ) {
        let identifier: &str = &variable_node.identifier;
        if !dict::VARIABLES.contains_key(identifier) {
            log.errors.push(Error {
                location,
                msg: format!("No such variable '{}'", identifier),
            });
        }
    }
}

pub struct QueryBuilderVisitor<'p> {
    pub ctes: HashMap<String, Cte>,
    pub where_expressions: Vec<String>,
    query_parameters: &'p mut QueryParameters,
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
        arguments: &[Box<Node<dyn ExpressionNode>>],
        log: &mut Log,
    ) {
        if !arguments.is_empty() {
            self.buffer = Some(String::new());
            arguments[0].accept(self, log);
            self.query_parameters.limit = self.buffer.take();
        }
    }

    pub fn visit_sort_modifier(
        &mut self,
        arguments: &[Box<Node<dyn ExpressionNode>>],
        log: &mut Log,
    ) {
        if !arguments.is_empty() {
            self.buffer = Some(String::new());
            arguments[0].accept(self, log);
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
    fn visit_query_node(&mut self, query_node: &QueryNode, log: &mut Log, _location: Location) {
        for node in query_node.statements.iter() {
            node.accept(self, log);
        }
    }

    fn visit_expression_statement(
        &mut self,
        expression_statement: &ExpressionStatement,
        log: &mut Log,
        _location: Location,
    ) {
        self.buffer = Some(String::new());
        expression_statement.expression_node.accept(self, log);
        let expression_string = self.buffer.take().unwrap();
        self.where_expressions.push(expression_string);
    }

    fn visit_post_tag_node(
        &mut self,
        post_tag_node: &PostTagNode,
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
        self.write_buff(&format!("EXISTS(SELECT * FROM post_tag WHERE fk_post = post.pk AND fk_tag IN(SELECT tag_keys FROM cte{cte_idx}))"));
    }

    fn visit_binary_expression_node(
        &mut self,
        binary_expression_node: &BinaryExpressionNode,
        log: &mut Log,
        _location: Location,
    ) {
        let op = binary_expression_node.op;
        let left = &binary_expression_node.left;
        let left_type = left.node_type.get_return_type();
        let right = &binary_expression_node.right;
        let right_type = right.node_type.get_return_type();
        let binary_types = (
            left.node_type.get_return_type(),
            right.node_type.get_return_type(),
        );

        self.write_buff("(");

        if (op == Operator::Plus || op == Operator::Minus)
            && (left_type == Type::Date || left_type == Type::DateTime)
            && right_type == Type::String
        {
            // add explicit types for date interval addition / subtraction
            left.accept(self, log);
            self.write_buff("::date ");
            self.write_buff(op.get_sql_string(Some(binary_types)));
            self.write_buff(" interval ");
            right.accept(self, log);
        } else {
            left.accept(self, log);
            self.write_buff(" ");
            self.write_buff(op.get_sql_string(Some(binary_types)));
            self.write_buff(" ");
            right.accept(self, log);
        }

        self.write_buff(")");
    }

    fn visit_integer_literal_node(
        &mut self,
        integer_literal_node: &IntegerLiteralNode,
        _log: &mut Log,
        _location: Location,
    ) {
        self.write_buff(&integer_literal_node.val.to_string());
    }

    fn visit_string_literal_node(
        &mut self,
        string_literal_node: &StringLiteralNode,
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
        boolean_literal_node: &BooleanLiteralNode,
        _log: &mut Log,
        _location: Location,
    ) {
        self.write_buff(&boolean_literal_node.val.to_string());
    }

    fn visit_null_literal_node(
        &mut self,
        _null_literal_node: &NullLiteralNode,
        _log: &mut Log,
        _location: Location,
    ) {
        self.write_buff("NULL");
    }

    fn visit_unary_expression_node(
        &mut self,
        unary_expression_node: &UnaryExpressionNode,
        log: &mut Log,
        _location: Location,
    ) {
        self.write_buff(unary_expression_node.op.get_sql_string(None));
        self.write_buff(" (");
        unary_expression_node.operand.accept(self, log);
        self.write_buff(")");
    }

    fn visit_attribute_node(
        &mut self,
        attribute_node: &AttributeNode,
        _log: &mut Log,
        _location: Location,
    ) {
        let identifier: &str = &attribute_node.identifier;
        if let Some(attribute) = dict::ATTRIBUTES.get(identifier) {
            self.write_buff(&attribute.selection_expression);
        } else {
            self.write_buff("NULL");
        }
    }

    fn visit_function_call_node(
        &mut self,
        function_call_node: &FunctionCallNode,
        log: &mut Log,
        location: Location,
    ) {
        let identifier: &str = &function_call_node.identifier;
        if let Some(function) = dict::FUNCTIONS.get(identifier) {
            let arguments = &function_call_node.arguments;
            (function.write_expression_fn)(self, arguments, location, log);
        } else {
            self.write_buff("NULL");
        }
    }

    fn visit_modifier_node(
        &mut self,
        modifier_node: &ModifierNode,
        log: &mut Log,
        _location: Location,
    ) {
        let identifier: &str = &modifier_node.identifier;
        if let Some(modifier) = dict::MODIFIERS.get(identifier) {
            (modifier.visit_query_builder)(self, &modifier_node.arguments, log);
        }
    }

    fn visit_variable_node(
        &mut self,
        variable_node: &VariableNode,
        _log: &mut Log,
        _location: Location,
    ) {
        let identifier: &str = &variable_node.identifier;
        if let Some(variable) = dict::VARIABLES.get(identifier) {
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

#[derive(Debug)]
pub struct Node<T: NodeType + ?Sized> {
    pub location: Location,
    pub node_type: T,
}

impl<T: NodeType + ?Sized> Node<T> {
    pub fn accept(&self, visitor: &mut dyn Visitor, log: &mut Log) {
        self.node_type.accept(visitor, log, self.location);
    }
}

pub trait NodeType: Downcast + Debug {
    fn accept(&self, visitor: &mut dyn Visitor, log: &mut Log, location: Location);
}

impl_downcast!(NodeType);

#[derive(Debug)]
pub struct QueryNode {
    pub statements: Vec<Box<Node<dyn StatementNode>>>,
}

impl NodeType for QueryNode {
    fn accept(&self, visitor: &mut dyn Visitor, log: &mut Log, location: Location) {
        visitor.visit_query_node(self, log, location);
    }
}

pub trait StatementNode: NodeType + Downcast {}

impl_downcast!(StatementNode);

#[derive(Debug)]
pub struct ExpressionStatement {
    pub expression_node: Box<Node<dyn ExpressionNode>>,
}

impl NodeType for ExpressionStatement {
    fn accept(&self, visitor: &mut dyn Visitor, log: &mut Log, location: Location) {
        visitor.visit_expression_statement(self, log, location);
    }
}

impl StatementNode for ExpressionStatement {}

#[derive(Debug)]
pub struct ModifierNode {
    pub identifier: String,
    pub arguments: Vec<Box<Node<dyn ExpressionNode>>>,
}

impl NodeType for ModifierNode {
    fn accept(&self, visitor: &mut dyn Visitor, log: &mut Log, location: Location) {
        visitor.visit_modifier_node(self, log, location);
    }
}

impl StatementNode for ModifierNode {}

pub trait ExpressionNode: NodeType + Downcast {
    fn get_return_type(&self) -> Type;
}

impl_downcast!(ExpressionNode);

#[derive(Debug, PartialEq)]
pub struct PostTagNode {
    pub identifier: String,
}

impl NodeType for PostTagNode {
    fn accept(&self, visitor: &mut dyn Visitor, log: &mut Log, location: Location) {
        visitor.visit_post_tag_node(self, log, location);
    }
}

impl ExpressionNode for PostTagNode {
    fn get_return_type(&self) -> Type {
        Type::Boolean
    }
}

#[derive(Debug, PartialEq)]
pub struct AttributeNode {
    pub identifier: String,
}

impl NodeType for AttributeNode {
    fn accept(&self, visitor: &mut dyn Visitor, log: &mut Log, location: Location) {
        visitor.visit_attribute_node(self, log, location);
    }
}

impl ExpressionNode for AttributeNode {
    fn get_return_type(&self) -> Type {
        let identifier: &str = &self.identifier;
        if let Some(attribute) = dict::ATTRIBUTES.get(identifier) {
            attribute.return_type
        } else {
            Type::Void
        }
    }
}

#[derive(Debug)]
pub struct FunctionCallNode {
    pub identifier: String,
    pub arguments: Vec<Box<Node<dyn ExpressionNode>>>,
}

impl NodeType for FunctionCallNode {
    fn accept(&self, visitor: &mut dyn Visitor, log: &mut Log, location: Location) {
        visitor.visit_function_call_node(self, log, location);
    }
}

impl ExpressionNode for FunctionCallNode {
    fn get_return_type(&self) -> Type {
        let identifier: &str = &self.identifier;
        if let Some(function) = dict::FUNCTIONS.get(identifier) {
            function.return_type
        } else {
            Type::Void
        }
    }
}

#[derive(Debug)]
pub struct BinaryExpressionNode {
    pub left: Box<Node<dyn ExpressionNode>>,
    pub op: Operator,
    pub right: Box<Node<dyn ExpressionNode>>,
}

impl NodeType for BinaryExpressionNode {
    fn accept(&self, visitor: &mut dyn Visitor, log: &mut Log, location: Location) {
        visitor.visit_binary_expression_node(self, log, location);
    }
}

impl ExpressionNode for BinaryExpressionNode {
    fn get_return_type(&self) -> Type {
        self.op
            .accepts_binary_expression(
                self.left.node_type.get_return_type(),
                self.right.node_type.get_return_type(),
            )
            .unwrap_or(Type::Void)
    }
}

#[derive(Debug, PartialEq)]
pub struct IntegerLiteralNode {
    pub val: i32,
}

impl NodeType for IntegerLiteralNode {
    fn accept(&self, visitor: &mut dyn Visitor, log: &mut Log, location: Location) {
        visitor.visit_integer_literal_node(self, log, location);
    }
}

impl ExpressionNode for IntegerLiteralNode {
    fn get_return_type(&self) -> Type {
        Type::Number
    }
}

#[derive(Debug, PartialEq)]
pub struct StringLiteralNode {
    pub val: String,
}

impl NodeType for StringLiteralNode {
    fn accept(&self, visitor: &mut dyn Visitor, log: &mut Log, location: Location) {
        visitor.visit_string_literal_node(self, log, location);
    }
}

impl ExpressionNode for StringLiteralNode {
    fn get_return_type(&self) -> Type {
        Type::String
    }
}

#[derive(Debug, PartialEq)]
pub struct BooleanLiteralNode {
    pub val: bool,
}

impl NodeType for BooleanLiteralNode {
    fn accept(&self, visitor: &mut dyn Visitor, log: &mut Log, location: Location) {
        visitor.visit_boolean_literal_node(self, log, location);
    }
}

impl ExpressionNode for BooleanLiteralNode {
    fn get_return_type(&self) -> Type {
        Type::Boolean
    }
}

#[derive(Debug, PartialEq)]
pub struct NullLiteralNode {}

impl NodeType for NullLiteralNode {
    fn accept(&self, visitor: &mut dyn Visitor, log: &mut Log, location: Location) {
        visitor.visit_null_literal_node(self, log, location);
    }
}

impl ExpressionNode for NullLiteralNode {
    fn get_return_type(&self) -> Type {
        Type::Null
    }
}

#[derive(Debug)]
pub struct UnaryExpressionNode {
    pub op: Operator,
    pub operand: Box<Node<dyn ExpressionNode>>,
}

impl NodeType for UnaryExpressionNode {
    fn accept(&self, visitor: &mut dyn Visitor, log: &mut Log, location: Location) {
        visitor.visit_unary_expression_node(self, log, location);
    }
}

impl ExpressionNode for UnaryExpressionNode {
    fn get_return_type(&self) -> Type {
        self.op
            .accepts_unary_expression(self.operand.node_type.get_return_type())
            .unwrap_or(Type::Void)
    }
}

#[derive(Debug, PartialEq)]
pub struct VariableNode {
    pub identifier: String,
}

impl NodeType for VariableNode {
    fn accept(&self, visitor: &mut dyn Visitor, log: &mut Log, location: Location) {
        visitor.visit_variable_node(self, log, location);
    }
}

impl ExpressionNode for VariableNode {
    fn get_return_type(&self) -> Type {
        let identifier: &str = &self.identifier;
        if let Some(variable) = dict::VARIABLES.get(identifier) {
            variable.return_type
        } else {
            Type::Void
        }
    }
}
