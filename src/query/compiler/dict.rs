use std::{collections::HashMap, str::FromStr, sync::Arc};

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

pub struct Attribute {
    pub selection_expression: String,
    pub return_type: Type,
    pub allow_sorting: bool,
    pub nullable: bool,
}

#[allow(clippy::type_complexity)]
pub struct Function {
    pub params: Vec<Parameter>,
    pub return_type: Type,
    pub accept_arguments:
        fn(&[Parameter], &[Box<Node<dyn ExpressionNode>>], &Scope, Location, &mut Log),
    pub write_expression_fn:
        fn(&mut QueryBuilderVisitor, &[Box<Node<dyn ExpressionNode>>], &Scope, Location, &mut Log),
}

#[allow(clippy::type_complexity)]
pub struct Modifier {
    pub params: Vec<Parameter>,
    pub accept_arguments:
        fn(&[Parameter], &[Box<Node<dyn ExpressionNode>>], &Scope, Location, &mut Log),
    pub visit_query_builder:
        fn(&mut QueryBuilderVisitor, &[Box<Node<dyn ExpressionNode>>], &Scope, &mut Log, Location),
}

pub struct Variable {
    pub return_type: Type,
    pub get_expression_fn: fn(&HashMap<String, String>) -> String,
}
#[derive(Debug, PartialEq)]
pub enum Scope {
    Global,
    Post,
    Collection,
    CollectionItem { collection_pk: i64 },
}

impl FromStr for Scope {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "global" => Ok(Self::Global),
            "post" => Ok(Self::Post),
            "collection" => Ok(Self::Collection),
            _ => Err(()),
        }
    }
}

impl Scope {
    pub fn get_attributes(&self) -> HashMap<&'static str, Arc<Attribute>> {
        match self {
            Self::Global => HashMap::new(),
            Self::Post => POST_ATTRIBUTES.clone(),
            Self::Collection => COLLECTION_ATTRIBUTES.clone(),
            Self::CollectionItem { .. } => {
                let mut post_attributes = POST_ATTRIBUTES.clone();
                post_attributes.insert(
                    "ordinal",
                    Arc::new(Attribute {
                        selection_expression: String::from("post_collection_item.ordinal"),
                        return_type: Type::Number,
                        allow_sorting: true,
                        nullable: false,
                    }),
                );
                post_attributes
            }
        }
    }

    pub fn get_functions(&self) -> HashMap<&'static str, Arc<Function>> {
        match self {
            Self::Global => GLOBAL_FUNCTIONS.clone(),
            Self::Post => GLOBAL_FUNCTIONS.clone(),
            Self::Collection => GLOBAL_FUNCTIONS.clone(),
            Self::CollectionItem { .. } => GLOBAL_FUNCTIONS.clone(),
        }
    }

    pub fn get_modifiers(&self) -> HashMap<&'static str, Arc<Modifier>> {
        match self {
            Self::Global => GLOBAL_MODIFIERS.clone(),
            Self::Post => GLOBAL_MODIFIERS.clone(),
            Self::Collection => GLOBAL_MODIFIERS.clone(),
            Self::CollectionItem { .. } => GLOBAL_MODIFIERS.clone(),
        }
    }

    pub fn get_variables(&self) -> HashMap<&'static str, Arc<Variable>> {
        match self {
            Self::Global => GLOBAL_VARIABLES.clone(),
            Self::Post => GLOBAL_VARIABLES.clone(),
            Self::Collection => GLOBAL_VARIABLES.clone(),
            Self::CollectionItem { .. } => GLOBAL_VARIABLES.clone(),
        }
    }
}

lazy_static! {
    pub static ref GLOBAL_FUNCTIONS: HashMap<&'static str, Arc<Function>> = HashMap::from([
        (
            "avg",
            Arc::new(Function {
                params: vec![Parameter {
                    parameter_type: ParameterType::Attribute(Type::Number)
                }],
                return_type: Type::Number,
                accept_arguments,
                write_expression_fn: |visitor, args, scope, _location, log| {
                    write_post_aggregate_function_expr("AVG", visitor, args, scope, log)
                }
            })
        ),
        (
            "max",
            Arc::new(Function {
                params: vec![Parameter {
                    parameter_type: ParameterType::Attribute(Type::Number)
                }],
                return_type: Type::Number,
                accept_arguments,
                write_expression_fn: |visitor, args, scope, _location, log| {
                    write_post_aggregate_function_expr("MAX", visitor, args, scope, log)
                }
            })
        ),
        (
            "min",
            Arc::new(Function {
                params: vec![Parameter {
                    parameter_type: ParameterType::Attribute(Type::Number)
                }],
                return_type: Type::Number,
                accept_arguments,
                write_expression_fn: |visitor, args, scope, _location, log| {
                    write_post_aggregate_function_expr("MIX", visitor, args, scope, log)
                }
            })
        ),
        (
            "find_user",
            Arc::new(Function {
                params: vec![Parameter {
                    parameter_type: ParameterType::Object(Type::String)
                }],
                return_type: Type::Number,
                accept_arguments,
                write_expression_fn: |visitor, args, scope, location, log| {
                    write_subquery_function_expr(
                        "registered_user",
                        "user_name",
                        true,
                        visitor,
                        args,
                        scope,
                        location,
                        log,
                    )
                }
            })
        )
    ]);
}

lazy_static! {
    pub static ref GLOBAL_MODIFIERS: HashMap<&'static str, Arc<Modifier>> = HashMap::from([
        (
            "limit",
            Arc::new(Modifier {
                params: vec![Parameter {
                    parameter_type: ParameterType::Object(Type::Number)
                }],
                accept_arguments,
                visit_query_builder: |visitor, arguments, scope, log, _| visitor
                    .visit_limit_modifier(arguments, scope, log)
            })
        ),
        (
            "sort",
            Arc::new(Modifier {
                params: vec![
                    Parameter {
                        parameter_type: ParameterType::Object(Type::Any)
                    },
                    Parameter {
                        parameter_type: ParameterType::Object(Type::String)
                    }
                ],
                accept_arguments: accept_sort_modifier_arguments,
                visit_query_builder: |visitor, arguments, scope, log, location| visitor
                    .visit_sort_modifier(arguments, scope, log, location)
            })
        ),
        (
            "shuffle",
            Arc::new(Modifier {
                params: vec![],
                accept_arguments,
                visit_query_builder: |visitor, _, _, _, _| visitor.query_parameters.shuffle = true
            })
        )
    ]);
}

lazy_static! {
    pub static ref GLOBAL_VARIABLES: HashMap<&'static str, Arc<Variable>> = HashMap::from([
        (
            "self",
            Arc::new(Variable {
                return_type: Type::Number,
                get_expression_fn: |vars| vars
                    .get("current_user_key")
                    .cloned()
                    .unwrap_or_else(|| String::from("NULL"))
            })
        ),
        (
            "now",
            Arc::new(Variable {
                return_type: Type::DateTime,
                get_expression_fn: |vars| vars
                    .get("current_utc_timestamp")
                    .map(|s| format!("'{}'", s))
                    .unwrap_or_else(|| String::from("NULL"))
            })
        ),
        (
            "now_date",
            Arc::new(Variable {
                return_type: Type::Date,
                get_expression_fn: |vars| vars
                    .get("current_utc_date")
                    .map(|s| format!("'{}'", s))
                    .unwrap_or_else(|| String::from("NULL"))
            })
        ),
        (
            "random",
            Arc::new(Variable {
                return_type: Type::Number,
                get_expression_fn: |_vars| String::from("RANDOM()")
            })
        )
    ]);
}

lazy_static! {
    pub static ref POST_ATTRIBUTES: HashMap<&'static str, Arc<Attribute>> = HashMap::from([
        (
            "creation_timestamp",
            Arc::new(Attribute {
                selection_expression: String::from("post.creation_timestamp"),
                return_type: Type::DateTime,
                allow_sorting: true,
                nullable: false
            })
        ),
        (
            "title",
            Arc::new(Attribute {
                selection_expression: String::from("post.title"),
                return_type: Type::String,
                allow_sorting: true,
                nullable: true,
            })
        ),
        (
            "uploader",
            Arc::new(Attribute {
                selection_expression: String::from("post.fk_create_user"),
                return_type: Type::Number,
                allow_sorting: true,
                nullable: false
            })
        ),
        (
            "description",
            Arc::new(Attribute {
                selection_expression: String::from("post.description"),
                return_type: Type::String,
                allow_sorting: false,
                nullable: true
            })
        )
    ]);
    pub static ref COLLECTION_ATTRIBUTES: HashMap<&'static str, Arc<Attribute>> = HashMap::from([
        (
            "creation_timestamp",
            Arc::new(Attribute {
                selection_expression: String::from("post_collection.creation_timestamp"),
                return_type: Type::DateTime,
                allow_sorting: true,
                nullable: false
            })
        ),
        (
            "title",
            Arc::new(Attribute {
                selection_expression: String::from("post_collection.title"),
                return_type: Type::String,
                allow_sorting: true,
                nullable: true,
            })
        ),
        (
            "owner",
            Arc::new(Attribute {
                selection_expression: String::from("post_collection.fk_create_user"),
                return_type: Type::Number,
                allow_sorting: true,
                nullable: false
            })
        ),
        (
            "description",
            Arc::new(Attribute {
                selection_expression: String::from("post_collection.description"),
                return_type: Type::String,
                allow_sorting: false,
                nullable: true
            })
        )
    ]);
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
    scope: &Scope,
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
                    let arg_type = attribute_node.get_return_type(scope);
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
                let arg_type = argument.node_type.get_return_type(scope);
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

#[allow(clippy::too_many_arguments)]
fn write_subquery_function_expr(
    table: &str,
    column: &str,
    is_string: bool,
    visitor: &mut QueryBuilderVisitor,
    args: &[Box<Node<dyn ExpressionNode>>],
    scope: &Scope,
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
        args[0].accept(visitor, scope, log);
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
    scope: &Scope,
    log: &mut Log,
) {
    visitor.write_buff("(SELECT ");
    visitor.write_buff(identifier);
    visitor.write_buff("(");

    let argument_count = args.len();
    for (i, argument) in args.iter().enumerate() {
        argument.accept(visitor, scope, log);

        if i < argument_count - 1 {
            visitor.write_buff(", ");
        }
    }

    visitor.write_buff(") FROM post)");
}

fn accept_sort_modifier_arguments(
    _params: &[Parameter],
    arguments: &[Box<Node<dyn ExpressionNode>>],
    _scope: &Scope,
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
            if let Some(attribute) = POST_ATTRIBUTES.get(attribute_node.identifier.as_str()) {
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
