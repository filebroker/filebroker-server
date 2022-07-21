use std::{iter::Peekable, mem, vec::IntoIter};

use super::{
    ast::{
        AttributeNode, BinaryExpressionNode, ExpressionNode, ExpressionStatement, FunctionCallNode,
        IntegerLiteralNode, ModifierNode, Node, Operator, PostTagNode, QueryNode, StatementNode,
        StringLiteralNode, UnaryExpressionNode,
    },
    lexer::{ParsedToken, Tag, Token},
    Error, Location, Log,
};

pub static ERROR_IDENTIFIER: &str = "$ERROR$";

pub struct Parser<'l> {
    token_stream: Peekable<IntoIter<Token>>,
    log: &'l mut Log,
    curr_tok: Option<Token>,
    prev_end: usize,
    current_start: usize,
}

impl<'l> Parser<'l> {
    pub fn new(token_stream: Vec<Token>, log: &'l mut Log) -> Self {
        Parser {
            token_stream: token_stream.into_iter().peekable(),
            log,
            curr_tok: None,
            prev_end: 0,
            current_start: 0,
        }
    }
}

impl Parser<'_> {
    pub fn parse_query(mut self) -> Result<Node<QueryNode>, ParserError> {
        let mut statements = Vec::new();
        let start = self.current_start;

        if self.advance() {
            loop {
                statements.push(self.parse_statement()?);
                if self.is_end() {
                    break;
                }
            }
        }

        self.advance();
        Ok(Node {
            location: Location {
                start,
                end: self.prev_end,
            },
            node_type: QueryNode { statements },
        })
    }

    fn parse_statement(&mut self) -> Result<Box<Node<dyn StatementNode>>, ParserError> {
        let start = self.current_start;
        if self.curr_is_tag(Tag::Modulo) {
            self.next()?;
            let identifier = self.read_identifier()?;
            let arguments = self.parse_arguments()?;
            Ok(Box::new(Node {
                location: Location {
                    start,
                    end: self.prev_end,
                },
                node_type: ModifierNode {
                    identifier,
                    arguments,
                },
            }))
        } else {
            let expression = self.parse_expression()?;
            Ok(Box::new(Node {
                location: expression.location,
                node_type: ExpressionStatement {
                    expression_node: expression,
                },
            }))
        }
    }

    fn parse_arguments(&mut self) -> Result<Vec<Box<Node<dyn ExpressionNode>>>, ParserError> {
        let mut arguments = Vec::new();
        self.check_curr_tag(Tag::OpenParenthesis);

        if !self.curr_is_tag(Tag::CloseParenthesis) {
            arguments.push(self.parse_expression()?);
            while self.curr_is_tag(Tag::Comma) {
                self.next()?;
                arguments.push(self.parse_expression()?);
            }
        }

        self.check_curr_tag(Tag::CloseParenthesis);
        Ok(arguments)
    }

    fn parse_expression(&mut self) -> Result<Box<Node<dyn ExpressionNode>>, ParserError> {
        let start = self.current_start;
        let mut left = self.parse_logic_term()?;

        while self.curr_is_tag(Tag::Or) {
            self.next()?;
            let right = self.parse_logic_term()?;
            let location = Location {
                start,
                end: self.prev_end,
            };
            left = Box::new(Node {
                location,
                node_type: BinaryExpressionNode {
                    left,
                    op: Operator::Or,
                    right,
                },
            });
        }

        Ok(left)
    }

    fn parse_simple_expression(&mut self) -> Result<Box<Node<dyn ExpressionNode>>, ParserError> {
        let start = self.current_start;
        let mut left = self.parse_term()?;

        while let Some(op) = self.read_operator(Operator::for_simple_math_operator_tag) {
            self.next()?;
            let right = self.parse_term()?;
            let location = Location {
                start,
                end: self.prev_end,
            };
            left = Box::new(Node {
                location,
                node_type: BinaryExpressionNode { left, op, right },
            });
        }

        Ok(left)
    }

    fn parse_term(&mut self) -> Result<Box<Node<dyn ExpressionNode>>, ParserError> {
        let start = self.current_start;
        let mut left = self.parse_factor()?;

        while let Some(op) = self.read_operator(Operator::for_math_term_operator_tag) {
            // a modulo operator followed by an identifier (e.g. %sort) is a modifier, not part of a binary expression
            if op == Operator::Modulo {
                if let Some(Token {
                    parsed_token: ParsedToken::IdentifierToken(_),
                    ..
                }) = self.token_stream.peek()
                {
                    return Ok(left);
                }
            }
            self.next()?;
            let right = self.parse_factor()?;
            let location = Location {
                start,
                end: self.prev_end,
            };
            left = Box::new(Node {
                location,
                node_type: BinaryExpressionNode { left, op, right },
            });
        }

        Ok(left)
    }

    fn parse_logic_term(&mut self) -> Result<Box<Node<dyn ExpressionNode>>, ParserError> {
        let start = self.current_start;
        let mut left = self.parse_logic_factor()?;

        while self.curr_is_tag(Tag::And) {
            self.next()?;
            let right = self.parse_logic_factor()?;
            let location = Location {
                start,
                end: self.prev_end,
            };
            left = Box::new(Node {
                location,
                node_type: BinaryExpressionNode {
                    left,
                    op: Operator::And,
                    right,
                },
            });
        }

        Ok(left)
    }

    fn parse_factor(&mut self) -> Result<Box<Node<dyn ExpressionNode>>, ParserError> {
        let start = self.current_start;

        if let Some(operator) = self.read_operator(Operator::for_unary_operator_tag) {
            self.next()?;
            self.parse_unary_expression(start, operator)
        } else if self.curr_is_tag(Tag::OpenParenthesis) {
            self.next()?;
            let expression = self.parse_expression()?;
            self.check_curr_tag(Tag::CloseParenthesis);
            Ok(expression)
        } else {
            self.parse_operand()
        }
    }

    fn parse_operand(&mut self) -> Result<Box<Node<dyn ExpressionNode>>, ParserError> {
        let start = self.current_start;
        match self.curr_tok {
            Some(Token {
                parsed_token: ParsedToken::IntegerToken(val),
                ..
            }) => {
                self.validate_int_bounds(val, false);
                self.advance();
                Ok(Box::new(Node {
                    location: Location {
                        start,
                        end: self.prev_end,
                    },
                    node_type: IntegerLiteralNode { val },
                }))
            }
            Some(Token {
                parsed_token: ParsedToken::StringToken(ref mut val),
                ..
            }) => {
                let val = mem::take(val);
                self.advance();
                Ok(Box::new(Node {
                    location: Location {
                        start,
                        end: self.prev_end,
                    },
                    node_type: StringLiteralNode { val },
                }))
            }
            Some(Token {
                parsed_token: ParsedToken::IdentifierToken(ref mut val),
                ..
            }) => {
                let val = mem::take(val);
                self.advance();
                Ok(Box::new(Node {
                    location: Location {
                        start,
                        end: self.prev_end,
                    },
                    node_type: PostTagNode { identifier: val },
                }))
            }
            Some(Token {
                parsed_token: ParsedToken::StaticToken(Tag::At),
                ..
            }) => {
                self.next()?;
                let identifier = self.read_identifier()?;
                Ok(Box::new(Node {
                    location: Location {
                        start,
                        end: self.prev_end,
                    },
                    node_type: AttributeNode { identifier },
                }))
            }
            Some(Token {
                parsed_token: ParsedToken::StaticToken(Tag::Period),
                ..
            }) => {
                self.next()?;
                let identifier = self.read_identifier()?;
                let arguments = self.parse_arguments()?;
                Ok(Box::new(Node {
                    location: Location {
                        start,
                        end: self.prev_end,
                    },
                    node_type: FunctionCallNode {
                        identifier,
                        arguments,
                    },
                }))
            }
            Some(ref token) => Err(ParserError::UnexpectedToken(token.clone())),
            None => Err(ParserError::PrematureEof),
        }
    }

    fn parse_logic_factor(&mut self) -> Result<Box<Node<dyn ExpressionNode>>, ParserError> {
        let start = self.current_start;
        let mut left = self.parse_simple_expression()?;

        while let Some(op) = self.read_operator(Operator::for_compare_operator_tag) {
            self.next()?;
            let right = self.parse_simple_expression()?;
            let location = Location {
                start,
                end: self.prev_end,
            };
            left = Box::new(Node {
                location,
                node_type: BinaryExpressionNode { left, op, right },
            });
        }

        Ok(left)
    }

    fn parse_unary_expression(
        &mut self,
        start: usize,
        operator: Operator,
    ) -> Result<Box<Node<dyn ExpressionNode>>, ParserError> {
        if let Some(Token {
            parsed_token: ParsedToken::IntegerToken(val),
            ..
        }) = self.curr_tok
        {
            if operator == Operator::Minus {
                self.validate_int_bounds(val, true);
                self.advance();
                return Ok(Box::new(Node {
                    location: Location {
                        start,
                        end: self.prev_end,
                    },
                    node_type: IntegerLiteralNode {
                        val: if val == i32::MIN { i32::MIN } else { -val },
                    },
                }));
            }
        }

        let operand = self.parse_factor()?;
        let location = Location {
            start,
            end: self.prev_end,
        };
        Ok(Box::new(Node {
            location,
            node_type: UnaryExpressionNode {
                op: operator,
                operand,
            },
        }))
    }

    /// Check that using the literal 2147483648 is only allowed when negative, in which case allow_min_val is true.
    #[inline]
    fn validate_int_bounds(&mut self, val: i32, allow_min_val: bool) {
        if !allow_min_val && val == i32::MIN {
            self.report_error(format!("Integer literal exceeds bound of {}", i32::MAX));
        }
    }

    fn read_identifier(&mut self) -> Result<String, ParserError> {
        match self.curr_tok {
            Some(Token {
                parsed_token: ParsedToken::IdentifierToken(ref mut ident),
                ..
            }) => {
                let val = mem::take(ident);
                self.next().map(|_| val)
            }
            _ => {
                self.report_error(String::from("Expected identifier"));
                self.next().map(|_| String::from(ERROR_IDENTIFIER))
            }
        }
    }

    fn read_operator<F: FnOnce(Tag) -> Option<Operator>>(&self, mapping_fn: F) -> Option<Operator> {
        match self.curr_tok {
            Some(Token {
                parsed_token: ParsedToken::StaticToken(tag),
                ..
            }) => mapping_fn(tag),
            _ => None,
        }
    }

    fn report_error(&mut self, msg: String) {
        self.log.errors.push(Error {
            location: self
                .curr_tok
                .as_ref()
                .map(|tok| tok.location)
                .unwrap_or_default(),
            msg,
        });
    }

    fn check_curr_tag(&mut self, tag: Tag) {
        if !self.curr_is_tag(tag) {
            self.report_error(format!(
                "Expected static token for tag {:?} but got {:?}",
                tag, self.curr_tok
            ));
        }
        self.advance();
    }

    fn curr_is_tag(&self, tag: Tag) -> bool {
        match self.curr_tok {
            Some(Token {
                parsed_token: ParsedToken::StaticToken(t),
                ..
            }) => t == tag,
            _ => false,
        }
    }

    fn is_end(&mut self) -> bool {
        self.curr_tok.is_none() && self.token_stream.peek().is_none()
    }

    fn advance(&mut self) -> bool {
        if let Some(ref curr_tok) = self.curr_tok {
            self.prev_end = curr_tok.location.end;
        }

        if let Some(next) = self.token_stream.next() {
            self.current_start = next.location.start;
            self.curr_tok = Some(next);
            true
        } else {
            self.current_start = self.prev_end;
            self.curr_tok = None;
            false
        }
    }

    fn next(&mut self) -> Result<(), ParserError> {
        if let Some(ref curr_tok) = self.curr_tok {
            self.prev_end = curr_tok.location.end;
        }

        if let Some(next) = self.token_stream.next() {
            self.current_start = next.location.start;
            self.curr_tok = Some(next);
            Ok(())
        } else {
            Err(ParserError::PrematureEof)
        }
    }
}

#[derive(Debug)]
pub enum ParserError {
    PrematureEof,
    UnexpectedToken(Token),
}

#[cfg(test)]
mod tests {

    use crate::query::compiler::{
        ast::{
            AttributeNode, BinaryExpressionNode, ExpressionNode, ExpressionStatement,
            IntegerLiteralNode, ModifierNode, Node, Operator, PostTagNode, QueryNode,
            StatementNode, StringLiteralNode,
        },
        lexer::Lexer,
        parser::Parser,
        Location, Log,
    };

    #[test]
    fn test_empty_source() {
        let query_node = parse(String::new()).node_type;
        assert!(query_node.statements.is_empty());
    }

    #[test]
    fn test_single_tag() {
        let query_node = parse(String::from("tag")).node_type;
        assert_eq!(query_node.statements.len(), 1);
        let statement = &query_node.statements[0];
        assert_post_tag_statement(statement, 0, 2, "tag");
    }

    #[test]
    fn test_two_tags() {
        let query_node = parse(String::from("tag1 tag2")).node_type;
        assert_eq!(query_node.statements.len(), 2);
        let statement1 = &query_node.statements[0];
        let statement2 = &query_node.statements[1];
        assert_post_tag_statement(statement1, 0, 3, "tag1");
        assert_post_tag_statement(statement2, 5, 8, "tag2");
    }

    #[test]
    fn test_modifier() {
        let query_node = parse(String::from("%limit(10)")).node_type;
        assert_eq!(query_node.statements.len(), 1);
        let statement = &query_node.statements[0];
        let modifier_statement = assert_modifier_statement(statement, 0, 9, "limit");
        let arguments = &modifier_statement.arguments;
        assert_eq!(arguments.len(), 1);
        let argument = &arguments[0];
        assert_eq!(argument.location, Location { start: 7, end: 8 });
        let integer_literal_node = argument.node_type.downcast_ref::<IntegerLiteralNode>();
        assert!(integer_literal_node.is_some());
        assert_eq!(integer_literal_node.unwrap().val, 10);
    }

    #[test]
    fn test_two_modifiers() {
        let query_node = parse(String::from("%limit(5 + 6) %sort(@score, \"DESC\")")).node_type;
        assert_eq!(query_node.statements.len(), 2);

        let statement1 = &query_node.statements[0];
        let modifier_statement1 = assert_modifier_statement(statement1, 0, 12, "limit");
        let arguments1 = &modifier_statement1.arguments;
        assert_eq!(arguments1.len(), 1);
        let argument1_1 = &arguments1[0];
        assert_binary_expression_node(
            argument1_1,
            7,
            11,
            Operator::Plus,
            Node {
                location: Location { start: 7, end: 7 },
                node_type: IntegerLiteralNode { val: 5 },
            },
            Node {
                location: Location { start: 11, end: 11 },
                node_type: IntegerLiteralNode { val: 6 },
            },
        );

        let statement2 = &query_node.statements[1];
        let modifier_statement2 = assert_modifier_statement(statement2, 14, 34, "sort");
        let arguments2 = &modifier_statement2.arguments;
        assert_eq!(arguments2.len(), 2);
        let argument2_1 = &arguments2[0];
        let argument2_2 = &arguments2[1];
        assert_attribute_expression(argument2_1, 20, 25, "score");
        assert_string_expression(argument2_2, 28, 33, "DESC");
    }

    #[test]
    fn test_binary_tag_expressions() {
        let query = parse(String::from("tag1 & (tag2 | tag3) `tag 4`"));
        assert_eq!(query.location, Location { start: 0, end: 27 });
        let query_node = query.node_type;
        assert_eq!(query_node.statements.len(), 2);

        let statement = &query_node.statements[0];
        assert_eq!(statement.location, Location { start: 0, end: 19 });
        let expression_statement = statement.node_type.downcast_ref::<ExpressionStatement>();
        assert!(expression_statement.is_some());
        let expression_node = &expression_statement.unwrap().expression_node;
        assert_eq!(expression_node.location, Location { start: 0, end: 19 });
        let binary_expression = expression_node
            .node_type
            .downcast_ref::<BinaryExpressionNode>();
        assert!(binary_expression.is_some());
        let binary_expression = binary_expression.unwrap();
        assert_eq!(binary_expression.op, Operator::And);
        assert_post_tag_expression(&binary_expression.left, 0, 3, "tag1");
        assert_binary_expression_node(
            &binary_expression.right,
            8,
            18,
            Operator::Or,
            Node {
                location: Location { start: 8, end: 11 },
                node_type: PostTagNode {
                    identifier: String::from("tag2"),
                },
            },
            Node {
                location: Location { start: 15, end: 18 },
                node_type: PostTagNode {
                    identifier: String::from("tag3"),
                },
            },
        );

        let statement2 = &query_node.statements[1];
        assert_eq!(statement2.location, Location { start: 21, end: 27 });
        let expression_statement = statement2.node_type.downcast_ref::<ExpressionStatement>();
        assert!(expression_statement.is_some());
        assert_post_tag_expression(
            &expression_statement.unwrap().expression_node,
            21,
            27,
            "tag 4",
        );
    }

    #[test]
    fn test_binary_attribute_expression() {
        let query = parse(String::from("@score > 10"));
        assert_eq!(query.location, Location { start: 0, end: 10 });
        let query_node = query.node_type;
        assert_eq!(query_node.statements.len(), 1);

        let statement = &query_node.statements[0];
        assert_eq!(statement.location, Location { start: 0, end: 10 });
        let expression_statement = statement.node_type.downcast_ref::<ExpressionStatement>();
        assert!(expression_statement.is_some());
        let expression_node = &expression_statement.unwrap().expression_node;
        assert_binary_expression_node(
            expression_node,
            0,
            10,
            Operator::Greater,
            Node {
                location: Location { start: 0, end: 5 },
                node_type: AttributeNode {
                    identifier: String::from("score"),
                },
            },
            Node {
                location: Location { start: 9, end: 10 },
                node_type: IntegerLiteralNode { val: 10 },
            },
        );
    }

    #[test]
    fn test_negative_integer() {
        let query_node = parse(String::from("@score = -15")).node_type;
        assert_eq!(query_node.statements.len(), 1);

        let statement = &query_node.statements[0];
        assert_eq!(statement.location, Location { start: 0, end: 11 });
        let expression_node = statement.node_type.downcast_ref::<ExpressionStatement>();
        assert!(expression_node.is_some());
        assert_binary_expression_node(
            &expression_node.unwrap().expression_node,
            0,
            11,
            Operator::Equal,
            Node {
                location: Location { start: 0, end: 5 },
                node_type: AttributeNode {
                    identifier: String::from("score"),
                },
            },
            Node {
                location: Location { start: 9, end: 11 },
                node_type: IntegerLiteralNode { val: -15 },
            },
        );
    }

    #[test]
    fn test_long_binary_expression() {
        let query = parse(String::from("@score = 2 + 3 * 4 / 7 % 2 tag"));
        assert_eq!(query.location, Location { start: 0, end: 29 });
        let statements = &query.node_type.statements;
        assert_eq!(statements.len(), 2);

        assert_eq!(statements[0].location, Location { start: 0, end: 25 });
        let expression_node1 = statements[0]
            .node_type
            .downcast_ref::<ExpressionStatement>();
        assert!(expression_node1.is_some());
        let expression_node = &expression_node1.unwrap().expression_node;
        assert_eq!(expression_node.location, Location { start: 0, end: 25 });
        let binary_expression = expression_node
            .node_type
            .downcast_ref::<BinaryExpressionNode>();
        assert!(binary_expression.is_some());
        let binary_expression = binary_expression.unwrap();
        assert_eq!(binary_expression.op, Operator::Equal);
        assert_attribute_expression(&binary_expression.left, 0, 5, "score");

        assert_eq!(
            binary_expression.right.location,
            Location { start: 9, end: 25 }
        );
        let right_binary_expression = binary_expression
            .right
            .node_type
            .downcast_ref::<BinaryExpressionNode>();
        assert!(right_binary_expression.is_some());
        let right_binary_expression = right_binary_expression.unwrap();
        assert_eq!(right_binary_expression.op, Operator::Plus);
        assert_integer_expression(&right_binary_expression.left, 9, 9, 2);

        assert_eq!(
            right_binary_expression.right.location,
            Location { start: 13, end: 25 }
        );
        let modulo_expression = right_binary_expression
            .right
            .node_type
            .downcast_ref::<BinaryExpressionNode>();
        assert!(modulo_expression.is_some());
        let modulo_expression = modulo_expression.unwrap();
        assert_eq!(modulo_expression.op, Operator::Modulo);
        assert_integer_expression(&modulo_expression.right, 25, 25, 2);

        assert_eq!(
            modulo_expression.left.location,
            Location { start: 13, end: 21 }
        );
        let division_expression = modulo_expression
            .left
            .node_type
            .downcast_ref::<BinaryExpressionNode>();
        assert!(division_expression.is_some());
        let division_expression = division_expression.unwrap();
        assert_eq!(division_expression.op, Operator::Divide);
        assert_integer_expression(&division_expression.right, 21, 21, 7);

        assert_binary_expression_node(
            &division_expression.left,
            13,
            17,
            Operator::Times,
            Node {
                location: Location { start: 13, end: 13 },
                node_type: IntegerLiteralNode { val: 3 },
            },
            Node {
                location: Location { start: 17, end: 17 },
                node_type: IntegerLiteralNode { val: 4 },
            },
        );
    }

    #[test]
    fn test_mixed_statements() {
        let query = parse(String::from(
            "(tag1 & @score > 10) | (@score >= 5 & tag2) %sort(@score) tag3",
        ));
        assert_eq!(query.location, Location { start: 0, end: 61 });
        let query_node = query.node_type;
        assert_eq!(query_node.statements.len(), 3);

        let statement1 = &query_node.statements[0];
        assert_eq!(statement1.location, Location { start: 0, end: 42 });
        let expression_statement1 = statement1.node_type.downcast_ref::<ExpressionStatement>();
        assert!(expression_statement1.is_some());
        let expression_node1 = &expression_statement1.unwrap().expression_node;
        assert_eq!(expression_node1.location, Location { start: 0, end: 42 });
        let binary_expression1 = expression_node1
            .node_type
            .downcast_ref::<BinaryExpressionNode>();
        assert!(binary_expression1.is_some());
        let binary_expression1 = binary_expression1.unwrap();
        assert_eq!(binary_expression1.op, Operator::Or);

        let binary_expression1_left = &binary_expression1.left;
        assert_eq!(
            binary_expression1_left.location,
            Location { start: 1, end: 18 }
        );
        let binary_expression1_left_binary = binary_expression1_left
            .node_type
            .downcast_ref::<BinaryExpressionNode>();
        assert!(binary_expression1_left_binary.is_some());
        let binary_expression1_left_binary = binary_expression1_left_binary.unwrap();
        assert_eq!(binary_expression1_left_binary.op, Operator::And);
        assert_post_tag_expression(&binary_expression1_left_binary.left, 1, 4, "tag1");
        assert_binary_expression_node(
            &binary_expression1_left_binary.right,
            8,
            18,
            Operator::Greater,
            Node {
                location: Location { start: 8, end: 13 },
                node_type: AttributeNode {
                    identifier: String::from("score"),
                },
            },
            Node {
                location: Location { start: 17, end: 18 },
                node_type: IntegerLiteralNode { val: 10 },
            },
        );

        let binary_expression1_right = &binary_expression1.right;
        assert_eq!(
            binary_expression1_right.location,
            Location { start: 24, end: 41 }
        );
        let binary_expression1_right_binary = binary_expression1_right
            .node_type
            .downcast_ref::<BinaryExpressionNode>();
        assert!(binary_expression1_right_binary.is_some());
        let binary_expression1_right_binary = binary_expression1_right_binary.unwrap();
        assert_eq!(binary_expression1_right_binary.op, Operator::And);
        assert_post_tag_expression(&binary_expression1_right_binary.right, 38, 41, "tag2");
        assert_binary_expression_node(
            &binary_expression1_right_binary.left,
            24,
            34,
            Operator::GreaterEqual,
            Node {
                location: Location { start: 24, end: 29 },
                node_type: AttributeNode {
                    identifier: String::from("score"),
                },
            },
            Node {
                location: Location { start: 34, end: 34 },
                node_type: IntegerLiteralNode { val: 5 },
            },
        );
    }

    fn parse(source: String) -> Node<QueryNode> {
        let mut log = Log { errors: Vec::new() };
        let lexer = Lexer::new_for_string(source, &mut log);
        let token_stream = lexer.read_token_stream();
        assert!(log.errors.is_empty());
        let parser = Parser::new(token_stream, &mut log);
        let query_node = parser.parse_query();
        assert!(query_node.is_ok());
        assert!(log.errors.is_empty());
        query_node.unwrap()
    }

    fn assert_post_tag_statement(
        statement: &Box<Node<dyn StatementNode>>,
        start: usize,
        end: usize,
        identifier: &str,
    ) {
        assert_eq!(statement.location, Location { start, end });
        let expression_statement = statement.node_type.downcast_ref::<ExpressionStatement>();
        assert!(expression_statement.is_some());
        let expression = &expression_statement.unwrap().expression_node;
        assert_post_tag_expression(expression, start, end, identifier);
    }

    fn assert_post_tag_expression(
        expression: &Box<Node<dyn ExpressionNode>>,
        start: usize,
        end: usize,
        identifier: &str,
    ) {
        assert_eq!(expression.location, Location { start, end });
        let post_tag_node = expression.node_type.downcast_ref::<PostTagNode>();
        assert!(post_tag_node.is_some());
        let post_tag_node = post_tag_node.unwrap();
        assert_eq!(post_tag_node.identifier, identifier);
    }

    fn assert_attribute_expression(
        expression_node: &Box<Node<dyn ExpressionNode>>,
        start: usize,
        end: usize,
        identifier: &str,
    ) {
        assert_eq!(expression_node.location, Location { start, end });
        let attribute_node = expression_node.node_type.downcast_ref::<AttributeNode>();
        assert!(attribute_node.is_some());
        assert_eq!(attribute_node.unwrap().identifier, identifier);
    }

    fn assert_string_expression(
        expression_node: &Box<Node<dyn ExpressionNode>>,
        start: usize,
        end: usize,
        val: &str,
    ) {
        assert_eq!(expression_node.location, Location { start, end });
        let string_literal_node = expression_node
            .node_type
            .downcast_ref::<StringLiteralNode>();
        assert!(string_literal_node.is_some());
        assert_eq!(string_literal_node.unwrap().val, val);
    }

    fn assert_integer_expression(
        expression_node: &Box<Node<dyn ExpressionNode>>,
        start: usize,
        end: usize,
        val: i32,
    ) {
        assert_eq!(expression_node.location, Location { start, end });
        let integer_literal_node = expression_node
            .node_type
            .downcast_ref::<IntegerLiteralNode>();
        assert!(integer_literal_node.is_some());
        assert_eq!(integer_literal_node.unwrap().val, val);
    }

    fn assert_modifier_statement<'m>(
        statement: &'m Box<Node<dyn StatementNode>>,
        start: usize,
        end: usize,
        identifier: &str,
    ) -> &'m ModifierNode {
        assert_eq!(statement.location, Location { start, end });
        let modifier_node = statement.node_type.downcast_ref::<ModifierNode>();
        assert!(modifier_node.is_some());
        let modifier_node = modifier_node.unwrap();
        assert_eq!(modifier_node.identifier, identifier);
        modifier_node
    }

    fn assert_binary_expression_node<L, R>(
        expression_node: &Box<Node<dyn ExpressionNode>>,
        start: usize,
        end: usize,
        op: Operator,
        left: Node<L>,
        right: Node<R>,
    ) where
        L: ExpressionNode + PartialEq,
        R: ExpressionNode + PartialEq,
    {
        assert_eq!(expression_node.location, Location { start, end });
        let binary_expression_node = expression_node
            .node_type
            .downcast_ref::<BinaryExpressionNode>();
        assert!(binary_expression_node.is_some());
        let binary_expression_node = binary_expression_node.unwrap();

        assert_eq!(binary_expression_node.op, op);

        assert_eq!(binary_expression_node.left.location, left.location);
        let left_node = binary_expression_node.left.node_type.downcast_ref::<L>();
        assert!(left_node.is_some());
        assert_eq!(left_node.unwrap(), &left.node_type);

        assert_eq!(binary_expression_node.right.location, right.location);
        let right_node = binary_expression_node.right.node_type.downcast_ref::<R>();
        assert!(right_node.is_some());
        assert_eq!(right_node.unwrap(), &right.node_type);
    }
}
