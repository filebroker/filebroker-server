use std::cmp::Ordering;
use std::convert::TryFrom;
use std::fmt;
use std::mem;

use super::Error;
use super::Location;
use super::Log;
use super::INTEGER_LIMIT;

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Tag {
    // operators
    And,
    At,
    Divide,
    Equal,
    Greater,
    GreaterEqual,
    Minus,
    Modulo,
    Not,
    Or,
    Plus,
    Less,
    LessEqual,
    Times,
    Unequal,
    // punctuation
    CloseParenthesis,
    Colon,
    Comma,
    OpenParenthesis,
    Period,
    QuestionMark,
}

impl TryFrom<char> for Tag {
    type Error = ();

    fn try_from(c: char) -> Result<Self, Self::Error> {
        match c {
            '&' => Ok(Tag::And),
            '@' => Ok(Tag::At),
            '/' => Ok(Tag::Divide),
            '=' => Ok(Tag::Equal),
            '-' => Ok(Tag::Minus),
            '%' => Ok(Tag::Modulo),
            '|' => Ok(Tag::Or),
            '+' => Ok(Tag::Plus),
            '*' => Ok(Tag::Times),
            '(' => Ok(Tag::OpenParenthesis),
            ':' => Ok(Tag::Colon),
            ',' => Ok(Tag::Comma),
            ')' => Ok(Tag::CloseParenthesis),
            '.' => Ok(Tag::Period),
            '?' => Ok(Tag::QuestionMark),
            _ => Err(()),
        }
    }
}

impl TryFrom<&str> for Tag {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.eq_ignore_ascii_case("AND") {
            Ok(Tag::And)
        } else if value.eq_ignore_ascii_case("OR") {
            Ok(Tag::Or)
        } else {
            Err(())
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Token {
    pub location: Location,
    pub parsed_token: ParsedToken,
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.parsed_token, self.location)
    }
}

#[derive(Clone, Debug, PartialEq)]
#[allow(clippy::enum_variant_names)]
pub enum ParsedToken {
    IdentifierToken(String),
    IntegerToken(i32),
    StaticToken(Tag),
    StringToken(String),
}

impl fmt::Display for ParsedToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ParsedToken::IdentifierToken(ref val) => format!("IDENTIFIER '{}'", val),
                ParsedToken::IntegerToken(val) => format!("INTEGER {}", val),
                ParsedToken::StaticToken(ref val) => format!("TOKEN {:?}", val),
                ParsedToken::StringToken(ref val) => format!("STRING '{}'", val),
            }
        )
    }
}

pub struct Lexer<'l> {
    source: Vec<char>,
    log: &'l mut Log,
    curr_pos: Option<usize>,
    is_escaped: bool,
}

impl<'l> Lexer<'l> {
    pub fn new_for_string(source: String, log: &'l mut Log) -> Self {
        Lexer {
            source: source.chars().collect(),
            log,
            curr_pos: None,
            is_escaped: false,
        }
    }
}

impl Lexer<'_> {
    pub fn read_token_stream(mut self) -> Vec<Token> {
        let mut token_stream = Vec::new();
        let mut curr_state = State {
            conception_idx: 0,
            state_type: StateType::ScanningState,
        };

        while let Some((pos, c)) = self.read_next() {
            let is_escaped = self.is_escaped;
            if is_escaped {
                self.is_escaped = false;
                if !(c == '`' || c == '\\' || c == '=' || c == ' ' || Tag::try_from(c).is_ok()) {
                    self.log.errors.push(Error {
                        location: Location {
                            start: pos,
                            end: pos,
                        },
                        msg: format!("Illegal escape character '{}'", c),
                    });
                }
            } else if c == '\\' {
                self.is_escaped = true;
                continue;
            }

            if !is_escaped && !curr_state.is_literal_mode(c) {
                if let Some(special_type) = StateType::special_type_for_char(c) {
                    curr_state.terminate(pos.saturating_sub(1), &mut token_stream, self.log);
                    curr_state = State {
                        conception_idx: pos,
                        state_type: special_type,
                    };
                    continue;
                } else if char::is_whitespace(c) {
                    curr_state.terminate(pos.saturating_sub(1), &mut token_stream, self.log);
                    curr_state = State {
                        conception_idx: pos,
                        state_type: StateType::ScanningState,
                    };
                } else if let Ok(tag) = Tag::try_from(c) {
                    curr_state.terminate(pos.saturating_sub(1), &mut token_stream, self.log);
                    token_stream.push(Token {
                        location: Location {
                            start: pos,
                            end: pos,
                        },
                        parsed_token: ParsedToken::StaticToken(tag),
                    });
                    curr_state = State {
                        conception_idx: pos + 1,
                        state_type: StateType::ScanningState,
                    };
                    continue;
                } else if !char_is_valid_identifier(c) && c != '"' && c != '`' && c != '\\' {
                    self.log.errors.push(Error {
                        location: Location {
                            start: pos,
                            end: pos,
                        },
                        msg: format!("Illegal char '{}'", c),
                    });
                }
            }

            curr_state = curr_state.handle_char(c, pos, &mut token_stream, self.log, is_escaped);
        }

        curr_state.terminate(
            self.curr_pos.map(|p| p.saturating_sub(1)).unwrap_or(0),
            &mut token_stream,
            self.log,
        );

        token_stream
    }

    fn read_next(&mut self) -> Option<(usize, char)> {
        let curr_pos = self.curr_pos.get_or_insert(0);
        let pos = *curr_pos;
        let res = self.source.get(*curr_pos);

        if res.is_some() {
            *curr_pos += 1;
        }

        res.map(|res| (pos, *res))
    }
}

#[inline]
pub fn char_is_valid_identifier(c: char) -> bool {
    c == '_' || c.is_alphanumeric()
}

struct State {
    conception_idx: usize,
    state_type: StateType,
}

#[allow(clippy::enum_variant_names)]
enum StateType {
    ScanningState,
    IdentifierState(String),
    IdentifierLiteralState(String),
    IntegerState(String),
    StringState(String),
    /// State for an assortment of operators that may be used in an equality check, e.g. ! for != or < for <= or =
    EqualityState {
        // tag if paired with equality operator
        equality_tag: Tag,
        // tag if single operator
        single_tag: Tag,
    },
}

impl State {
    /// Handle the given character in the current state and return the state used to handle the next char
    fn handle_char(
        mut self,
        c: char,
        pos: usize,
        token_stream: &mut Vec<Token>,
        log: &mut Log,
        is_escaped: bool,
    ) -> Self {
        match self.state_type {
            StateType::ScanningState => {
                if is_escaped {
                    return State {
                        conception_idx: pos,
                        state_type: StateType::IdentifierState(String::new()),
                    };
                } else if char::is_ascii_digit(&c) {
                    return State {
                        conception_idx: pos,
                        state_type: StateType::IntegerState(String::new()),
                    }
                    .handle_char(c, pos, token_stream, log, false);
                } else if char_is_valid_identifier(c) {
                    return State {
                        conception_idx: pos,
                        state_type: StateType::IdentifierState(String::new()),
                    }
                    .handle_char(c, pos, token_stream, log, false);
                } else if c == '"' {
                    return State {
                        conception_idx: pos,
                        state_type: StateType::StringState(String::new()),
                    };
                } else if c == '`' {
                    return State {
                        conception_idx: pos,
                        state_type: StateType::IdentifierLiteralState(String::new()),
                    };
                } else if !c.is_whitespace() {
                    log.errors.push(Error {
                        location: Location {
                            start: self.conception_idx,
                            end: pos,
                        },
                        msg: format!("Unexpected char '{}'", c),
                    });
                }

                self
            }
            StateType::IdentifierState(ref mut val) => {
                if is_escaped || char_is_valid_identifier(c) {
                    val.push(c);
                } else {
                    self.terminate(pos.saturating_sub(1), token_stream, log);
                    return State {
                        conception_idx: pos,
                        state_type: StateType::ScanningState,
                    }
                    .handle_char(c, pos, token_stream, log, false);
                }

                self
            }
            StateType::IdentifierLiteralState(ref mut val) => {
                if c == '`' && !is_escaped {
                    token_stream.push(Token {
                        location: Location {
                            start: self.conception_idx,
                            end: pos,
                        },
                        parsed_token: ParsedToken::IdentifierToken(val.to_lowercase()),
                    });

                    State {
                        conception_idx: pos + 1,
                        state_type: StateType::ScanningState,
                    }
                } else {
                    val.push(c);
                    self
                }
            }
            StateType::IntegerState(ref mut val) => {
                if char::is_ascii_digit(&c) {
                    val.push(c);
                } else if is_escaped || char_is_valid_identifier(c) {
                    val.push(c);
                    return State {
                        conception_idx: self.conception_idx,
                        state_type: StateType::IdentifierState(mem::take(val)),
                    };
                } else {
                    self.terminate(pos.saturating_sub(1), token_stream, log);
                    return State {
                        conception_idx: pos,
                        state_type: StateType::ScanningState,
                    }
                    .handle_char(c, pos, token_stream, log, false);
                }

                self
            }
            StateType::StringState(ref mut val) => {
                if c == '"' && !is_escaped {
                    token_stream.push(Token {
                        location: Location {
                            start: self.conception_idx,
                            end: pos,
                        },
                        // swap the String within this State for an unallocated empty String and move it to the Token
                        // instead of cloning as we do not need the State anymore to save memory allocation
                        parsed_token: ParsedToken::StringToken(mem::take(val)),
                    });

                    State {
                        conception_idx: pos + 1,
                        state_type: StateType::ScanningState,
                    }
                } else {
                    val.push(c);
                    self
                }
            }
            StateType::EqualityState {
                ref equality_tag, ..
            } => {
                if c == '=' && !is_escaped {
                    token_stream.push(Token {
                        location: Location {
                            start: self.conception_idx,
                            end: pos,
                        },
                        parsed_token: ParsedToken::StaticToken(Tag::clone(equality_tag)),
                    });

                    State {
                        conception_idx: pos + 1,
                        state_type: StateType::ScanningState,
                    }
                } else {
                    self.terminate(pos.saturating_sub(1), token_stream, log);
                    State {
                        conception_idx: pos,
                        state_type: StateType::ScanningState,
                    }
                    .handle_char(c, pos, token_stream, log, is_escaped)
                }
            }
        }
    }

    fn terminate(self, pos: usize, token_stream: &mut Vec<Token>, log: &mut Log) {
        match self.state_type {
            StateType::ScanningState => {}
            StateType::IdentifierState(val) => {
                let location = Location {
                    start: self.conception_idx,
                    end: pos,
                };
                if let Ok(tag) = <Tag as TryFrom<&str>>::try_from(&val) {
                    token_stream.push(Token {
                        location,
                        parsed_token: ParsedToken::StaticToken(tag),
                    });
                } else {
                    token_stream.push(Token {
                        location,
                        parsed_token: ParsedToken::IdentifierToken(val.to_lowercase()),
                    });
                }
            }
            StateType::IdentifierLiteralState(_) => log.errors.push(Error {
                location: Location {
                    start: self.conception_idx,
                    end: pos,
                },
                msg: String::from("Unclosed identifier literal"),
            }),
            StateType::IntegerState(val) => {
                let location = Location {
                    start: self.conception_idx,
                    end: pos,
                };
                if let Ok(parsed_val) = val.parse::<u32>() {
                    match parsed_val.cmp(&INTEGER_LIMIT) {
                        Ordering::Greater => log.errors.push(Error {
                            location,
                            msg: format!(
                                "Integer literal {} exceeds maximum size of {}",
                                parsed_val, INTEGER_LIMIT
                            ),
                        }),
                        Ordering::Equal => {
                            // The integer literal token usually stores the value unsigned, but for the minimal integer value this is not
                            // possible, since the absolute value of the minmal value is one higher than the absolute value of the maximum
                            // value (-2147483648 vs. 2147483647), so the minimal value has to be handled separately.
                            //
                            // Here, if the literal was 2147483648 it will simply be stored as Integer.MIN_VALUE, whether the previous token
                            // was a minus (-) and the literal is valid is checked by the parser.
                            token_stream.push(Token {
                                location,
                                parsed_token: ParsedToken::IntegerToken(i32::MIN),
                            });
                        }
                        Ordering::Less => token_stream.push(Token {
                            location,
                            parsed_token: ParsedToken::IntegerToken(parsed_val as i32),
                        }),
                    }
                } else {
                    log.errors.push(Error {
                        location,
                        msg: format!("Invalid integer literal '{}'", val),
                    });
                }
            }
            StateType::StringState(_) => log.errors.push(Error {
                location: Location {
                    start: self.conception_idx,
                    end: pos,
                },
                msg: String::from("Unclosed String literal"),
            }),
            StateType::EqualityState { single_tag, .. } => token_stream.push(Token {
                location: Location {
                    start: self.conception_idx,
                    end: pos,
                },
                parsed_token: ParsedToken::StaticToken(single_tag),
            }),
        }
    }

    /// Determine whether the state accepts a special char that would normally cause a state switch, used for string literals
    /// or for tokens consisting of two special tokens, e.g. !=
    fn is_literal_mode(&self, c: char) -> bool {
        match self.state_type {
            StateType::ScanningState => false,
            StateType::IdentifierState(_) => false,
            StateType::IdentifierLiteralState(_) => c != '\n' && c != '\r',
            StateType::IntegerState(_) => false,
            StateType::StringState(_) => c != '\n' && c != '\r',
            StateType::EqualityState { .. } => c == '=',
        }
    }
}

impl StateType {
    fn special_type_for_char(c: char) -> Option<Self> {
        match c {
            '!' => Some(StateType::EqualityState {
                equality_tag: Tag::Unequal,
                single_tag: Tag::Not,
            }),
            '<' => Some(StateType::EqualityState {
                equality_tag: Tag::LessEqual,
                single_tag: Tag::Less,
            }),
            '>' => Some(StateType::EqualityState {
                equality_tag: Tag::GreaterEqual,
                single_tag: Tag::Greater,
            }),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::query::compiler::{
        lexer::{Lexer, ParsedToken, ParsedToken::*, Tag, Token},
        Location, Log,
    };

    #[test]
    fn test_empty_source() {
        let mut log = Log { errors: Vec::new() };

        let lexer = Lexer::new_for_string(String::new(), &mut log);
        let token_stream = lexer.read_token_stream();
        assert!(token_stream.is_empty());
        assert!(log.errors.is_empty());
    }

    #[test]
    fn test_single_tag() {
        let mut log = Log { errors: Vec::new() };

        let lexer = Lexer::new_for_string(String::from("tag"), &mut log);
        let mut token_stream = lexer.read_token_stream().into_iter();
        assert!(log.errors.is_empty());

        assert_token(
            token_stream.next(),
            0,
            2,
            IdentifierToken(String::from("tag")),
        );

        assert_eq!(token_stream.next(), None)
    }

    #[test]
    fn test_not_single_tag() {
        let mut log = Log { errors: Vec::new() };

        let lexer = Lexer::new_for_string(String::from("!tag"), &mut log);
        let mut token_stream = lexer.read_token_stream().into_iter();
        assert!(log.errors.is_empty());

        assert_token(token_stream.next(), 0, 0, StaticToken(Tag::Not));

        assert_token(
            token_stream.next(),
            1,
            3,
            IdentifierToken(String::from("tag")),
        );

        assert_eq!(token_stream.next(), None)
    }

    #[test]
    fn test_multiple_tags() {
        let mut log = Log { errors: Vec::new() };

        let lexer = Lexer::new_for_string(String::from("tag1 tag2 tag3"), &mut log);
        let mut token_stream = lexer.read_token_stream().into_iter();
        assert!(log.errors.is_empty());

        assert_token(
            token_stream.next(),
            0,
            3,
            IdentifierToken(String::from("tag1")),
        );

        assert_token(
            token_stream.next(),
            5,
            8,
            IdentifierToken(String::from("tag2")),
        );

        assert_token(
            token_stream.next(),
            10,
            13,
            IdentifierToken(String::from("tag3")),
        );

        assert_eq!(token_stream.next(), None)
    }

    #[test]
    fn test_binary_expressions() {
        let mut log = Log { errors: Vec::new() };

        let lexer = Lexer::new_for_string(String::from("tag1 & tag2 | tag3"), &mut log);
        let mut token_stream = lexer.read_token_stream().into_iter();
        assert!(log.errors.is_empty());

        assert_token(
            token_stream.next(),
            0,
            3,
            IdentifierToken(String::from("tag1")),
        );

        assert_token(token_stream.next(), 5, 5, StaticToken(Tag::And));

        assert_token(
            token_stream.next(),
            7,
            10,
            IdentifierToken(String::from("tag2")),
        );

        assert_token(token_stream.next(), 12, 12, StaticToken(Tag::Or));

        assert_token(
            token_stream.next(),
            14,
            17,
            IdentifierToken(String::from("tag3")),
        );

        assert_eq!(token_stream.next(), None);
    }

    #[test]
    fn test_binary_expressions_keywords() {
        let mut log = Log { errors: Vec::new() };

        let lexer = Lexer::new_for_string(String::from("tag1 AND tag2 OR tag3"), &mut log);
        let mut token_stream = lexer.read_token_stream().into_iter();
        assert!(log.errors.is_empty());

        assert_token(
            token_stream.next(),
            0,
            3,
            IdentifierToken(String::from("tag1")),
        );

        assert_token(token_stream.next(), 5, 7, StaticToken(Tag::And));

        assert_token(
            token_stream.next(),
            9,
            12,
            IdentifierToken(String::from("tag2")),
        );

        assert_token(token_stream.next(), 14, 15, StaticToken(Tag::Or));

        assert_token(
            token_stream.next(),
            17,
            20,
            IdentifierToken(String::from("tag3")),
        );

        assert_eq!(token_stream.next(), None);
    }

    #[test]
    fn test_special_tag_literal() {
        let mut log = Log { errors: Vec::new() };

        let lexer = Lexer::new_for_string(
            String::from("`special_tag-test (pa'ren\"thesis\")`"),
            &mut log,
        );
        let mut token_stream = lexer.read_token_stream().into_iter();
        assert!(log.errors.is_empty());

        assert_token(
            token_stream.next(),
            0,
            34,
            IdentifierToken(String::from("special_tag-test (pa'ren\"thesis\")")),
        );

        assert_eq!(token_stream.next(), None);
    }

    #[test]
    fn test_numerical_tags() {
        let mut log = Log { errors: Vec::new() };

        let lexer = Lexer::new_for_string(String::from("`1234` 343industries"), &mut log);
        let mut token_stream = lexer.read_token_stream().into_iter();
        assert!(log.errors.is_empty());

        assert_token(
            token_stream.next(),
            0,
            5,
            IdentifierToken(String::from("1234")),
        );

        assert_token(
            token_stream.next(),
            7,
            19,
            IdentifierToken(String::from("343industries")),
        );

        assert_eq!(token_stream.next(), None);
    }

    #[test]
    fn test_escaping() {
        let mut log = Log { errors: Vec::new() };

        let lexer = Lexer::new_for_string(
            String::from(
                "`back\\`tick` back\\\\slash `b\\\\\\`s` test\\ space\\(paren\\) 123\\ test",
            ),
            &mut log,
        );
        let mut token_stream = lexer.read_token_stream().into_iter();
        assert!(log.errors.is_empty());

        assert_token(
            token_stream.next(),
            0,
            11,
            IdentifierToken(String::from("back`tick")),
        );

        assert_token(
            token_stream.next(),
            13,
            23,
            IdentifierToken(String::from("back\\slash")),
        );

        assert_token(
            token_stream.next(),
            25,
            32,
            IdentifierToken(String::from("b\\`s")),
        );

        assert_token(
            token_stream.next(),
            34,
            53,
            IdentifierToken(String::from("test space(paren)")),
        );

        assert_token(
            token_stream.next(),
            55,
            63,
            IdentifierToken(String::from("123 test")),
        );

        assert_eq!(token_stream.next(), None);
    }

    #[test]
    fn test_modifiers_functions_and_attributes() {
        let mut log = Log { errors: Vec::new() };

        let lexer = Lexer::new_for_string(String::from("tag @creation_timestamp>=\"2022-06-03\" @score > .max(@score) - 10 %sort(@score, \"DESC\")"), &mut log);
        let mut token_stream = lexer.read_token_stream().into_iter();
        assert!(log.errors.is_empty());

        assert_token(
            token_stream.next(),
            0,
            2,
            IdentifierToken(String::from("tag")),
        );

        assert_token(token_stream.next(), 4, 4, StaticToken(Tag::At));

        assert_token(
            token_stream.next(),
            5,
            22,
            IdentifierToken(String::from("creation_timestamp")),
        );

        assert_token(token_stream.next(), 23, 24, StaticToken(Tag::GreaterEqual));

        assert_token(
            token_stream.next(),
            25,
            36,
            StringToken(String::from("2022-06-03")),
        );

        assert_token(token_stream.next(), 38, 38, StaticToken(Tag::At));

        assert_token(
            token_stream.next(),
            39,
            43,
            IdentifierToken(String::from("score")),
        );

        assert_token(token_stream.next(), 45, 45, StaticToken(Tag::Greater));
        assert_token(token_stream.next(), 47, 47, StaticToken(Tag::Period));

        assert_token(
            token_stream.next(),
            48,
            50,
            IdentifierToken(String::from("max")),
        );

        assert_token(
            token_stream.next(),
            51,
            51,
            StaticToken(Tag::OpenParenthesis),
        );

        assert_token(token_stream.next(), 52, 52, StaticToken(Tag::At));

        assert_token(
            token_stream.next(),
            53,
            57,
            IdentifierToken(String::from("score")),
        );

        assert_token(
            token_stream.next(),
            58,
            58,
            StaticToken(Tag::CloseParenthesis),
        );

        assert_token(token_stream.next(), 60, 60, StaticToken(Tag::Minus));

        assert_token(token_stream.next(), 62, 63, IntegerToken(10));
        assert_token(token_stream.next(), 65, 65, StaticToken(Tag::Modulo));

        assert_token(
            token_stream.next(),
            66,
            69,
            IdentifierToken(String::from("sort")),
        );

        assert_token(
            token_stream.next(),
            70,
            70,
            StaticToken(Tag::OpenParenthesis),
        );

        assert_token(token_stream.next(), 71, 71, StaticToken(Tag::At));

        assert_token(
            token_stream.next(),
            72,
            76,
            IdentifierToken(String::from("score")),
        );

        assert_token(token_stream.next(), 77, 77, StaticToken(Tag::Comma));

        assert_token(
            token_stream.next(),
            79,
            84,
            StringToken(String::from("DESC")),
        );

        assert_token(
            token_stream.next(),
            85,
            85,
            StaticToken(Tag::CloseParenthesis),
        );

        assert_eq!(token_stream.next(), None);
    }

    #[inline]
    fn assert_token(
        actual_token: Option<Token>,
        expected_start: usize,
        expected_end: usize,
        expected_parsed_token: ParsedToken,
    ) {
        assert_eq!(
            actual_token,
            Some(Token {
                location: Location {
                    start: expected_start,
                    end: expected_end
                },
                parsed_token: expected_parsed_token
            })
        );
    }
}
