use std::fmt::Debug;

use downcast_rs::{impl_downcast, Downcast};

use crate::query::{Direction, Ordering, QueryParameters};

use super::{dict, dict::Type, lexer::Tag, Error, Location, Log};

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

    pub fn get_sql_string(&self) -> &str {
        match self {
            Self::And => "AND",
            Self::Divide => "/",
            Self::Equal => "=",
            Self::Greater => ">",
            Self::GreaterEqual => ">=",
            Self::Less => "<",
            Self::LessEqual => "<=",
            Self::Minus => "-",
            Self::Modulo => "%",
            Self::Not => "NOT",
            Self::Or => "OR",
            Self::Plus => "+",
            Self::Times => "*",
            Self::Unequal => "!=",
        }
    }

    pub fn accepts_binary_expression(&self, left: Type, right: Type) -> Option<Type> {
        match self {
            Self::And if left == Type::Boolean && right == Type::Boolean => Some(Type::Boolean),
            Self::Divide if left == Type::Number && right == Type::Number => Some(Type::Number),
            Self::Equal if left == right => Some(Type::Boolean),
            Self::Greater if left == Type::Number && right == Type::Number => Some(Type::Boolean),
            Self::GreaterEqual if left == Type::Number && right == Type::Number => {
                Some(Type::Boolean)
            }
            Self::Less if left == Type::Number && right == Type::Number => Some(Type::Boolean),
            Self::LessEqual if left == Type::Number && right == Type::Number => Some(Type::Boolean),
            Self::Minus if left == Type::Number && right == Type::Number => Some(Type::Number),
            Self::Modulo if left == Type::Number && right == Type::Number => Some(Type::Number),
            // Not is a unary operator only
            Self::Not => None,
            Self::Or if left == Type::Boolean && right == Type::Boolean => Some(Type::Boolean),
            Self::Plus if left == Type::Number && right == Type::Number => Some(Type::Number),
            Self::Plus if left == Type::String && right == Type::String => Some(Type::String),
            Self::Times if left == Type::Number && right == Type::Number => Some(Type::Number),
            Self::Unequal if left == right => Some(Type::Boolean),
            // handle date to string comparisons
            Self::Greater
            | Self::GreaterEqual
            | Self::Less
            | Self::LessEqual
            | Self::Equal
            | Self::Unequal
                if (left == Type::String && (right == Type::Date || right == Type::DateTime))
                    || (right == Type::String
                        && (left == Type::Date || left == Type::DateTime)) =>
            {
                Some(Type::Boolean)
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
        if return_type != Type::Boolean {
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

        // TODO allow intervals
        if left_type == Type::String && (right_type == Type::Date || right_type == Type::DateTime) {
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
}

pub struct QueryBuilderVisitor<'p> {
    pub ctes: Vec<String>,
    pub where_expressions: Vec<String>,
    query_parameters: &'p mut QueryParameters,
    buffer: Option<String>,
}

impl<'p> QueryBuilderVisitor<'p> {
    pub fn new(query_parameters: &'p mut QueryParameters) -> Self {
        QueryBuilderVisitor {
            ctes: Vec::new(),
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

    fn write_buff(&mut self, s: &str) {
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
        let cte_idx = self.ctes.len();
        let tag_name = sanitize_string_literal(&post_tag_node.identifier);
        let cte = format!(
            "cte{cte_idx} AS (
                SELECT unnest(
                    tag.pk
                    || array(SELECT fk_target FROM tag_alias WHERE fk_source = tag.pk)
                    || array(SELECT fk_source FROM tag_alias WHERE fk_target = tag.pk)
                ) AS tag_keys
                FROM tag WHERE lower(tag.tag_name) = '{tag_name}'
            )"
        );

        self.ctes.push(cte);
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
        let right = &binary_expression_node.right;

        self.write_buff("(");
        left.accept(self, log);
        self.write_buff(" ");
        self.write_buff(op.get_sql_string());
        self.write_buff(" ");
        right.accept(self, log);
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

    fn visit_unary_expression_node(
        &mut self,
        unary_expression_node: &UnaryExpressionNode,
        log: &mut Log,
        _location: Location,
    ) {
        self.write_buff(unary_expression_node.op.get_sql_string());
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
        _location: Location,
    ) {
        let identifier: &str = &function_call_node.identifier;
        self.write_buff("(SELECT ");
        self.write_buff(identifier);
        self.write_buff("(");

        let argument_count = function_call_node.arguments.len();
        for (i, argument) in function_call_node.arguments.iter().enumerate() {
            argument.accept(self, log);

            if i < argument_count - 1 {
                self.write_buff(", ");
            }
        }

        self.write_buff(") FROM post)");
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
