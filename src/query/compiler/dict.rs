use std::collections::HashMap;

use lazy_static::lazy_static;

use super::{
    ast::{AttributeNode, ExpressionNode, Node, QueryBuilderVisitor, StringLiteralNode},
    Error, Location, Log,
};

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Type {
    Any,
    Boolean,
    #[allow(dead_code)] // keep for completion's sake
    Date,
    DateTime,
    Number,
    String,
    Void,
}

lazy_static! {
    pub static ref ATTRIBUTES: HashMap<&'static str, Attribute> = HashMap::from([
        (
            "creation_timestamp",
            Attribute {
                selection_expression: String::from("post.creation_timestamp"),
                return_type: Type::DateTime
            }
        ),
        (
            "title",
            Attribute {
                selection_expression: String::from("post.title"),
                return_type: Type::String
            }
        ),
        (
            "score",
            Attribute {
                selection_expression: String::from("post.score"),
                return_type: Type::Number
            }
        )
    ]);
}

pub struct Attribute {
    pub selection_expression: String,
    pub return_type: Type,
}

#[derive(Debug, Clone, Copy)]
pub struct Parameter {
    parameter_type: ParameterType,
}

#[derive(Debug, Clone, Copy)]
pub enum ParameterType {
    Attribute(Type),
    Object(Type),
}

fn accept_arguments(
    params: &[Parameter],
    arguments: &[Box<Node<dyn ExpressionNode>>],
    location: Location,
    log: &mut Log,
) {
    if params.len() != arguments.len() {
        log.errors.push(Error {
            location,
            msg: format!(
                "Expected {} arguments but got {}",
                params.len(),
                arguments.len()
            ),
        });
        return;
    }

    for i in 0..params.len() {
        let parameter_type = params[i].parameter_type;
        let argument = &arguments[i];
        match parameter_type {
            ParameterType::Attribute(attr_type) => {
                let location = argument.location;
                let attribute_node = argument.node_type.downcast_ref::<AttributeNode>();
                if let Some(attribute_node) = attribute_node {
                    let arg_type = attribute_node.get_return_type();
                    if arg_type != attr_type {
                        log.errors.push(Error {
                            location,
                            msg: format!(
                                "Expected attribute to be of type {:?} but got {:?}",
                                attr_type, arg_type
                            ),
                        });
                    }
                } else {
                    log.errors.push(Error {
                        location,
                        msg: format!(
                            "Expected argument to be an attribute but got expression {:?}",
                            &argument
                        ),
                    });
                }
            }
            ParameterType::Object(obj_type) => {
                let location = argument.location;
                let arg_type = argument.node_type.get_return_type();
                if arg_type != obj_type {
                    log.errors.push(Error {
                        location,
                        msg: format!(
                            "Expected argument to be of type {:?} but got {:?}",
                            obj_type, arg_type
                        ),
                    });
                }
            }
        }
    }
}

lazy_static! {
    pub static ref FUNCTIONS: HashMap<&'static str, Function> = HashMap::from([
        (
            "avg",
            Function {
                params: vec![Parameter {
                    parameter_type: ParameterType::Attribute(Type::Number)
                }],
                return_type: Type::Number,
                accept_arguments
            }
        ),
        (
            "max",
            Function {
                params: vec![Parameter {
                    parameter_type: ParameterType::Attribute(Type::Number)
                }],
                return_type: Type::Number,
                accept_arguments
            }
        ),
        (
            "min",
            Function {
                params: vec![Parameter {
                    parameter_type: ParameterType::Attribute(Type::Number)
                }],
                return_type: Type::Number,
                accept_arguments
            }
        )
    ]);
}

#[allow(clippy::type_complexity)]
pub struct Function {
    pub params: Vec<Parameter>,
    pub return_type: Type,
    pub accept_arguments: fn(&[Parameter], &[Box<Node<dyn ExpressionNode>>], Location, &mut Log),
}

lazy_static! {
    pub static ref MODIFIERS: HashMap<&'static str, Modifier> = HashMap::from([
        (
            "limit",
            Modifier {
                params: vec![Parameter {
                    parameter_type: ParameterType::Object(Type::Number)
                }],
                accept_arguments,
                visit_query_builder: |visitor, arguments, log| visitor
                    .visit_limit_modifier(arguments, log)
            }
        ),
        (
            "sort",
            Modifier {
                params: vec![
                    Parameter {
                        parameter_type: ParameterType::Attribute(Type::Any)
                    },
                    Parameter {
                        parameter_type: ParameterType::Object(Type::String)
                    }
                ],
                accept_arguments: accept_sort_modifier_arguments,
                visit_query_builder: |visitor, arguments, log| visitor
                    .visit_sort_modifier(arguments, log)
            }
        )
    ]);
}

#[allow(clippy::type_complexity)]
pub struct Modifier {
    pub params: Vec<Parameter>,
    pub accept_arguments: fn(&[Parameter], &[Box<Node<dyn ExpressionNode>>], Location, &mut Log),
    pub visit_query_builder:
        fn(&mut QueryBuilderVisitor, &[Box<Node<dyn ExpressionNode>>], &mut Log),
}

fn accept_sort_modifier_arguments(
    _params: &[Parameter],
    arguments: &[Box<Node<dyn ExpressionNode>>],
    location: Location,
    log: &mut Log,
) {
    if !(arguments.len() == 1 || arguments.len() == 2) {
        log.errors.push(Error {
            location,
            msg: format!("Expected 1 or 2 arguments but got {}", arguments.len()),
        });
        return;
    }

    let attr_arg = &arguments[0];
    let attr_arg_location = attr_arg.location;
    if attr_arg.node_type.downcast_ref::<AttributeNode>().is_none() {
        log.errors.push(Error {
            location: attr_arg_location,
            msg: format!(
                "Expected argument to be an attribute but got expression {:?}",
                attr_arg
            ),
        });
    }

    if arguments.len() == 2 {
        let direction_arg = &arguments[1];
        let direction_arg_location = direction_arg.location;

        if let Some(direction_arg) = direction_arg.node_type.downcast_ref::<StringLiteralNode>() {
            let direction = direction_arg.val.to_lowercase();
            match direction.as_str() {
                "asc" | "ascending" | "desc" | "descending" => {},
                _ => log.errors.push(Error {
                    location: direction_arg_location,
                    msg: format!("Expected sorting direction to be 'asc', 'ascending', 'desc', 'descending' but got '{}'", &direction) 
                })
            }
        }
    }
}