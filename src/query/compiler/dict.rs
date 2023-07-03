use std::collections::HashMap;

use lazy_static::lazy_static;

use super::{
    ast::{AttributeNode, ExpressionNode, Node, QueryBuilderVisitor, StringLiteralNode},
    Error, Location, Log,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Type {
    Any,
    Boolean,
    Date,
    DateTime,
    Null,
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
                return_type: Type::DateTime,
                allow_sorting: true,
                nullable: false
            }
        ),
        (
            "title",
            Attribute {
                selection_expression: String::from("post.title"),
                return_type: Type::String,
                allow_sorting: true,
                nullable: true,
            }
        ),
        (
            "uploader",
            Attribute {
                selection_expression: String::from("post.fk_create_user"),
                return_type: Type::Number,
                allow_sorting: true,
                nullable: false
            }
        ),
        (
            "description",
            Attribute {
                selection_expression: String::from("post.description"),
                return_type: Type::String,
                allow_sorting: false,
                nullable: true
            }
        )
    ]);
}

pub struct Attribute {
    pub selection_expression: String,
    pub return_type: Type,
    pub allow_sorting: bool,
    pub nullable: bool,
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
                accept_arguments,
                write_expression_fn: |visitor, args, _location, log| {
                    write_post_aggregate_function_expr("AVG", visitor, args, log)
                }
            }
        ),
        (
            "max",
            Function {
                params: vec![Parameter {
                    parameter_type: ParameterType::Attribute(Type::Number)
                }],
                return_type: Type::Number,
                accept_arguments,
                write_expression_fn: |visitor, args, _location, log| {
                    write_post_aggregate_function_expr("MAX", visitor, args, log)
                }
            }
        ),
        (
            "min",
            Function {
                params: vec![Parameter {
                    parameter_type: ParameterType::Attribute(Type::Number)
                }],
                return_type: Type::Number,
                accept_arguments,
                write_expression_fn: |visitor, args, _location, log| {
                    write_post_aggregate_function_expr("MIX", visitor, args, log)
                }
            }
        ),
        (
            "find_user",
            Function {
                params: vec![Parameter {
                    parameter_type: ParameterType::Object(Type::String)
                }],
                return_type: Type::Number,
                accept_arguments,
                write_expression_fn: |visitor, args, location, log| write_subquery_function_expr(
                    "registered_user",
                    "user_name",
                    true,
                    visitor,
                    args,
                    location,
                    log
                )
            }
        )
    ]);
}

#[allow(clippy::type_complexity)]
pub struct Function {
    pub params: Vec<Parameter>,
    pub return_type: Type,
    pub accept_arguments: fn(&[Parameter], &[Box<Node<dyn ExpressionNode>>], Location, &mut Log),
    pub write_expression_fn:
        fn(&mut QueryBuilderVisitor, &[Box<Node<dyn ExpressionNode>>], Location, &mut Log),
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
                visit_query_builder: |visitor, arguments, log, _| visitor
                    .visit_limit_modifier(arguments, log)
            }
        ),
        (
            "sort",
            Modifier {
                params: vec![
                    Parameter {
                        parameter_type: ParameterType::Object(Type::Any)
                    },
                    Parameter {
                        parameter_type: ParameterType::Object(Type::String)
                    }
                ],
                accept_arguments: accept_sort_modifier_arguments,
                visit_query_builder: |visitor, arguments, log, location| visitor
                    .visit_sort_modifier(arguments, log, location)
            }
        ),
        (
            "shuffle",
            Modifier {
                params: vec![],
                accept_arguments,
                visit_query_builder: |visitor, _, _, _| visitor.query_parameters.shuffle = true
            }
        )
    ]);
}

fn write_subquery_function_expr(
    table: &str,
    column: &str,
    is_string: bool,
    visitor: &mut QueryBuilderVisitor,
    args: &[Box<Node<dyn ExpressionNode>>],
    location: Location,
    log: &mut Log,
) {
    visitor.write_buff("(SELECT pk FROM ");
    visitor.write_buff(table);
    visitor.write_buff(" WHERE ");
    if is_string {
        visitor.write_buff("LOWER(");
    }
    visitor.write_buff(column);
    if is_string {
        visitor.write_buff(")");
    }
    visitor.write_buff(" = ");

    if args.len() == 1 {
        if is_string {
            visitor.write_buff("LOWER(");
        }
        args[0].accept(visitor, log);
        if is_string {
            visitor.write_buff(")");
        }
    } else {
        log.errors.push(Error {
            location,
            msg: format!("Expected 1 argument but got {}", args.len()),
        });
        visitor.write_buff("NULL");
    }

    visitor.write_buff(")");
}

fn write_post_aggregate_function_expr(
    identifier: &str,
    visitor: &mut QueryBuilderVisitor,
    args: &[Box<Node<dyn ExpressionNode>>],
    log: &mut Log,
) {
    visitor.write_buff("(SELECT ");
    visitor.write_buff(identifier);
    visitor.write_buff("(");

    let argument_count = args.len();
    for (i, argument) in args.iter().enumerate() {
        argument.accept(visitor, log);

        if i < argument_count - 1 {
            visitor.write_buff(", ");
        }
    }

    visitor.write_buff(") FROM post)");
}

#[allow(clippy::type_complexity)]
pub struct Modifier {
    pub params: Vec<Parameter>,
    pub accept_arguments: fn(&[Parameter], &[Box<Node<dyn ExpressionNode>>], Location, &mut Log),
    pub visit_query_builder:
        fn(&mut QueryBuilderVisitor, &[Box<Node<dyn ExpressionNode>>], &mut Log, Location),
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
    match attr_arg.node_type.downcast_ref::<AttributeNode>() {
        Some(attribute_node) => {
            if let Some(attribute) = ATTRIBUTES.get(attribute_node.identifier.as_str()) {
                if !attribute.allow_sorting {
                    log.errors.push(Error {
                        location: attr_arg_location,
                        msg: format!("Attribute {} is not sortable", &attribute_node.identifier),
                    });
                }
            }
        }
        None => {
            log.errors.push(Error {
                location: attr_arg_location,
                msg: format!(
                    "Expected argument to be an attribute but got expression {:?}",
                    &attr_arg.node_type
                ),
            });
        }
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

pub struct Variable {
    pub return_type: Type,
    pub get_expression_fn: fn(&HashMap<String, String>) -> String,
}

lazy_static! {
    pub static ref VARIABLES: HashMap<&'static str, Variable> = HashMap::from([
        (
            "self",
            Variable {
                return_type: Type::Number,
                get_expression_fn: |vars| vars
                    .get("current_user_key")
                    .map(String::clone)
                    .unwrap_or_else(|| String::from("NULL"))
            }
        ),
        (
            "now",
            Variable {
                return_type: Type::DateTime,
                get_expression_fn: |vars| vars
                    .get("current_utc_timestamp")
                    .map(|s| format!("'{}'", s))
                    .unwrap_or_else(|| String::from("NULL"))
            }
        ),
        (
            "now_date",
            Variable {
                return_type: Type::Date,
                get_expression_fn: |vars| vars
                    .get("current_utc_date")
                    .map(|s| format!("'{}'", s))
                    .unwrap_or_else(|| String::from("NULL"))
            }
        ),
        (
            "random",
            Variable {
                return_type: Type::Number,
                get_expression_fn: |_vars| String::from("RANDOM()")
            }
        )
    ]);
}
