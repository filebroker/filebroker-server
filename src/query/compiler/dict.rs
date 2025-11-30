use std::{collections::HashMap, str::FromStr, sync::Arc};

use super::{
    Error, Location, Log,
    ast::{AttributeNode, ExpressionNode, Node, QueryBuilderVisitor, StringLiteralNode},
};
use crate::query::compiler::ast::{VariableNode, sanitize_string_literal};
use lazy_static::lazy_static;
use regex::Regex;

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
    Interval,
}

pub struct Attribute {
    pub table: &'static str,
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
    pub write_expression_fn: fn(
        &mut QueryBuilderVisitor,
        &mut [Box<Node<dyn ExpressionNode>>],
        &Scope,
        Location,
        &mut Log,
    ),
}

#[allow(clippy::type_complexity)]
pub struct Modifier {
    pub params: Vec<Parameter>,
    pub accept_arguments:
        fn(&[Parameter], &[Box<Node<dyn ExpressionNode>>], &Scope, Location, &mut Log),
    pub visit_query_builder: fn(
        &mut QueryBuilderVisitor,
        &mut [Box<Node<dyn ExpressionNode>>],
        &Scope,
        &mut Log,
        Location,
    ),
}

pub struct Variable {
    pub return_type: Type,
    pub get_value_plain_fn: fn(&HashMap<String, String>) -> Option<String>,
    pub get_expression_fn: fn(&HashMap<String, String>) -> String,
}

#[derive(Debug, PartialEq)]
pub enum Scope {
    Global,
    Post,
    Collection,
    CollectionItem { collection_pk: i64 },
    UserGroup,
    // Scopes for tag auto match conditions
    TagAutoMatchPost,
    TagAutoMatchCollection,
}

impl FromStr for Scope {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "global" => Ok(Self::Global),
            "post" => Ok(Self::Post),
            "collection" => Ok(Self::Collection),
            "collection_item" => Ok(Self::CollectionItem { collection_pk: -1 }),
            "user_group" => Ok(Self::UserGroup),
            "tag_auto_match_post" => Ok(Self::TagAutoMatchPost),
            "tag_auto_match_collection" => Ok(Self::TagAutoMatchCollection),
            _ => {
                lazy_static! {
                    static ref COLLECTION_ITEM_REGEX: Regex =
                        Regex::new(r"^collection_item_(\d+)$").unwrap();
                }

                if let Some(captures) = COLLECTION_ITEM_REGEX.captures(s) {
                    let collection_pk = captures
                        .get(1)
                        .and_then(|m| m.as_str().parse().ok())
                        .unwrap_or(-1);
                    Ok(Self::CollectionItem { collection_pk })
                } else {
                    Err(())
                }
            }
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
                        table: "post_collection_item",
                        selection_expression: String::from("post_collection_item.ordinal"),
                        return_type: Type::Number,
                        allow_sorting: true,
                        nullable: false,
                    }),
                );
                post_attributes
            }
            Self::UserGroup => USER_GROUP_ATTRIBUTES.clone(),
            Self::TagAutoMatchPost => POST_ATTRIBUTES.clone(),
            Self::TagAutoMatchCollection => COLLECTION_ATTRIBUTES.clone(),
        }
    }

    pub fn get_functions(&self) -> HashMap<&'static str, Arc<Function>> {
        match self {
            Self::Global => GLOBAL_FUNCTIONS.clone(),
            Self::Post => POST_FUNCTIONS.clone(),
            Self::Collection => COLLECTION_FUNCTIONS.clone(),
            Self::CollectionItem { .. } => POST_FUNCTIONS.clone(),
            Self::UserGroup => GLOBAL_FUNCTIONS.clone(),
            Self::TagAutoMatchPost => GLOBAL_FUNCTIONS.clone(),
            Self::TagAutoMatchCollection => GLOBAL_FUNCTIONS.clone(),
        }
    }

    pub fn get_modifiers(&self) -> HashMap<&'static str, Arc<Modifier>> {
        match self {
            Self::Global => GLOBAL_MODIFIERS.clone(),
            Self::Post => GLOBAL_MODIFIERS.clone(),
            Self::Collection => GLOBAL_MODIFIERS.clone(),
            Self::CollectionItem { .. } => GLOBAL_MODIFIERS.clone(),
            Self::UserGroup => GLOBAL_MODIFIERS.clone(),
            Self::TagAutoMatchPost => HashMap::new(),
            Self::TagAutoMatchCollection => HashMap::new(),
        }
    }

    pub fn get_variables(&self) -> HashMap<&'static str, Arc<Variable>> {
        match self {
            Self::Global => GLOBAL_VARIABLES.clone(),
            Self::Post => GLOBAL_VARIABLES.clone(),
            Self::Collection => GLOBAL_VARIABLES.clone(),
            Self::CollectionItem { .. } => GLOBAL_VARIABLES.clone(),
            Self::UserGroup => GLOBAL_VARIABLES.clone(),
            Self::TagAutoMatchPost | Self::TagAutoMatchCollection => {
                let mut global_variables = GLOBAL_VARIABLES.clone();
                global_variables.remove("self");
                global_variables.insert(
                    "tag_name",
                    Arc::new(Variable {
                        return_type: Type::String,
                        get_value_plain_fn: |vars| vars.get("tag_name").cloned(),
                        get_expression_fn: |vars| {
                            vars.get("tag_name")
                                .map(|s| format!("'{}'", sanitize_string_literal(s.as_str())))
                                .unwrap_or_else(|| String::from("NULL"))
                        },
                    }),
                );
                global_variables
            }
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
                    write_post_aggregate_function_expr("MIN", visitor, args, scope, log)
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
        ),
        (
            "delimited_string_contains",
            Arc::new(Function {
                params: vec![Parameter {
                    parameter_type: ParameterType::Attribute(Type::String)
                }, Parameter {
                    parameter_type: ParameterType::Object(Type::String)
                }],
                return_type: Type::Boolean,
                accept_arguments: |params: &[Parameter], arguments: &[Box<Node<dyn ExpressionNode>>], scope: &Scope, location: Location, log: &mut Log| {
                    accept_arguments(params, arguments, scope, location, log);
                    if arguments.len() >= 2 {
                        let argument = &arguments[1].node_type;
                        if let Some(variable_node) = argument.downcast_ref::<VariableNode>() {
                            let ident = variable_node.identifier.as_str();
                            if !scope.get_variables().contains_key(ident) {
                                log.errors.push(Error {
                                    location,
                                    msg: format!("No such variable '{ident}'"),
                                });
                            }
                        } else {
                            let string_literal = argument.downcast_ref::<StringLiteralNode>();
                            if string_literal.is_none() {
                                log.errors.push(Error {
                                    location,
                                    msg: format!(
                                        "Expected argument to be a string literal but got expression {:?}",
                                        &argument
                                    ),
                                });
                            }
                        }
                    }
                },
                write_expression_fn: |visitor, args, scope, _location, log| {
                    if args.len() >= 2 {
                        let variable_value = if let Some(variable_node) = args[1].node_type.downcast_ref::<VariableNode>() {
                            let identifier: &str = &variable_node.identifier;
                            let value = scope.get_variables().get(identifier).and_then(|var| (var.get_value_plain_fn)(&visitor.query_parameters.variables));
                            if value.is_none() {
                                // variable is undefined and cannot match, fast return FALSE
                                visitor.write_buff("FALSE");
                                return;
                            }
                            value
                        } else {
                            None
                        };
                        let value = if let Some(value) = variable_value {
                            sanitize_string_literal(&value)
                        } else if let Some(string_literal) = args[1].node_type.downcast_ref::<StringLiteralNode>() {
                            sanitize_string_literal(&string_literal.val)
                        } else {
                            visitor.write_buff("FALSE");
                            return;
                        };

                        let attribute_arg = &mut args[0];
                        visitor.write_buff("(");
                        // check that the target attribute is not NULL
                        attribute_arg.accept(visitor, scope, log);
                        visitor.write_buff(" IS NOT NULL AND ");
                        // check that the value is contained in the target attribute at all,
                        // this is much faster because it can use trigram indexes, and if the value is not contained, the pattern does not match anyway
                        visitor.write_buff("LOWER(");
                        attribute_arg.accept(visitor, scope, log);
                        visitor.write_buff(") LIKE LOWER('%");
                        visitor.write_buff(&value);
                        visitor.write_buff("%') AND ");
                        // perform the regex match to see if the value is part of a ,&; separated list of values
                        attribute_arg.accept(visitor, scope, log);
                        visitor.write_buff(" ~* ");
                        visitor.write_buff(r"'(^|[,&;]\s*)");
                        let escaped = regex::escape(&value);
                        visitor.write_buff(&escaped);
                        visitor.write_buff(r"(\s*[,&;]|$)'");
                        visitor.write_buff(")");
                    }
                },
            })
        )
    ]);
    pub static ref POST_FUNCTIONS: HashMap<&'static str, Arc<Function>> = {
        let mut functions = GLOBAL_FUNCTIONS.clone();
        functions.insert(
            "shared_with_group",
            Arc::new(Function {
                params: vec![Parameter {
                    parameter_type: ParameterType::Object(Type::Number),
                }],
                return_type: Type::Boolean,
                accept_arguments,
                write_expression_fn: |visitor, args, scope, _location, log| {
                    visitor.write_buff("EXISTS(SELECT * FROM post_group_access WHERE fk_post = post.pk AND fk_granted_group = ");
                    if !args.is_empty() {
                        args[0].accept(visitor, scope, log);
                    } else {
                        visitor.write_buff("NULL");
                    }
                    visitor.write_buff(")");
                },
            })
        );
        functions
    };
    pub static ref COLLECTION_FUNCTIONS: HashMap<&'static str, Arc<Function>> = {
        let mut functions = GLOBAL_FUNCTIONS.clone();
        functions.insert(
            "shared_with_group",
            Arc::new(Function {
                params: vec![Parameter {
                    parameter_type: ParameterType::Object(Type::Number),
                }],
                return_type: Type::Boolean,
                accept_arguments,
                write_expression_fn: |visitor, args, scope, _location, log| {
                    visitor.write_buff("EXISTS(SELECT * FROM post_collection_group_access WHERE fk_post_collection = post_collection.pk AND fk_granted_group = ");
                    if !args.is_empty() {
                        args[0].accept(visitor, scope, log);
                    } else {
                        visitor.write_buff("NULL");
                    }
                    visitor.write_buff(")");
                },
            })
        );
        functions
    };
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
                get_value_plain_fn: |vars| vars.get("current_user_key").cloned(),
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
                get_value_plain_fn: |vars| vars.get("current_utc_timestamp").cloned(),
                get_expression_fn: |vars| vars
                    .get("current_utc_timestamp")
                    .map(|s| format!("'{s}'"))
                    .unwrap_or_else(|| String::from("NULL"))
            })
        ),
        (
            "now_date",
            Arc::new(Variable {
                return_type: Type::Date,
                get_value_plain_fn: |vars| vars.get("current_utc_date").cloned(),
                get_expression_fn: |vars| vars
                    .get("current_utc_date")
                    .map(|s| format!("'{s}'"))
                    .unwrap_or_else(|| String::from("NULL"))
            })
        ),
        (
            "random",
            Arc::new(Variable {
                return_type: Type::Number,
                get_value_plain_fn: |_vars| None,
                get_expression_fn: |_vars| String::from("RANDOM()")
            })
        )
    ]);
}

lazy_static! {
    pub static ref POST_ATTRIBUTES: HashMap<&'static str, Arc<Attribute>> = HashMap::from([
        (
            "date",
            Arc::new(Attribute {
                table: "post",
                selection_expression: String::from("post.creation_timestamp"),
                return_type: Type::DateTime,
                allow_sorting: true,
                nullable: false
            })
        ),
        (
            "title",
            Arc::new(Attribute {
                table: "post",
                selection_expression: String::from("post.title"),
                return_type: Type::String,
                allow_sorting: true,
                nullable: true,
            })
        ),
        (
            "uploader",
            Arc::new(Attribute {
                table: "post",
                selection_expression: String::from("post.fk_create_user"),
                return_type: Type::Number,
                allow_sorting: true,
                nullable: false
            })
        ),
        (
            "description",
            Arc::new(Attribute {
                table: "post",
                selection_expression: String::from("post.description"),
                return_type: Type::String,
                allow_sorting: false,
                nullable: true
            })
        ),
        (
            "type",
            Arc::new(Attribute {
                table: "s3_object_metadata",
                selection_expression: String::from("s3_object_metadata.mime_type"),
                return_type: Type::String,
                allow_sorting: false,
                nullable: true
            })
        ),
        (
            "artist",
            Arc::new(Attribute {
                table: "s3_object_metadata",
                selection_expression: String::from("s3_object_metadata.artist"),
                return_type: Type::String,
                allow_sorting: true,
                nullable: true
            })
        ),
        (
            "album",
            Arc::new(Attribute {
                table: "s3_object_metadata",
                selection_expression: String::from("s3_object_metadata.album"),
                return_type: Type::String,
                allow_sorting: true,
                nullable: true
            })
        ),
        (
            "composer",
            Arc::new(Attribute {
                table: "s3_object_metadata",
                selection_expression: String::from("s3_object_metadata.composer"),
                return_type: Type::String,
                allow_sorting: false,
                nullable: true
            })
        ),
        (
            "genre",
            Arc::new(Attribute {
                table: "s3_object_metadata",
                selection_expression: String::from("s3_object_metadata.genre"),
                return_type: Type::String,
                allow_sorting: false,
                nullable: true
            })
        ),
        (
            "date_meta",
            Arc::new(Attribute {
                table: "s3_object_metadata",
                selection_expression: String::from("s3_object_metadata.date"),
                return_type: Type::DateTime,
                allow_sorting: true,
                nullable: true
            })
        ),
        (
            "duration",
            Arc::new(Attribute {
                table: "s3_object_metadata",
                selection_expression: String::from("s3_object_metadata.duration"),
                return_type: Type::Interval,
                allow_sorting: true,
                nullable: true
            })
        ),
        (
            "track",
            Arc::new(Attribute {
                table: "s3_object_metadata",
                selection_expression: String::from("s3_object_metadata.track_number"),
                return_type: Type::Number,
                allow_sorting: true,
                nullable: true
            })
        ),
        (
            "disc",
            Arc::new(Attribute {
                table: "s3_object_metadata",
                selection_expression: String::from("s3_object_metadata.disc_number"),
                return_type: Type::Number,
                allow_sorting: true,
                nullable: true
            })
        ),
        (
            "width",
            Arc::new(Attribute {
                table: "s3_object_metadata",
                selection_expression: String::from("s3_object_metadata.width"),
                return_type: Type::Number,
                allow_sorting: true,
                nullable: true
            })
        ),
        (
            "height",
            Arc::new(Attribute {
                table: "s3_object_metadata",
                selection_expression: String::from("s3_object_metadata.height"),
                return_type: Type::Number,
                allow_sorting: true,
                nullable: true
            })
        ),
        (
            "size",
            Arc::new(Attribute {
                table: "s3_object_metadata",
                selection_expression: String::from("s3_object_metadata.size"),
                return_type: Type::Number,
                allow_sorting: true,
                nullable: true
            })
        ),
    ]);
    pub static ref COLLECTION_ATTRIBUTES: HashMap<&'static str, Arc<Attribute>> = HashMap::from([
        (
            "date",
            Arc::new(Attribute {
                table: "post_collection",
                selection_expression: String::from("post_collection.creation_timestamp"),
                return_type: Type::DateTime,
                allow_sorting: true,
                nullable: false
            })
        ),
        (
            "title",
            Arc::new(Attribute {
                table: "post_collection",
                selection_expression: String::from("post_collection.title"),
                return_type: Type::String,
                allow_sorting: true,
                nullable: true,
            })
        ),
        (
            "owner",
            Arc::new(Attribute {
                table: "post_collection",
                selection_expression: String::from("post_collection.fk_create_user"),
                return_type: Type::Number,
                allow_sorting: true,
                nullable: false
            })
        ),
        (
            "description",
            Arc::new(Attribute {
                table: "post_collection",
                selection_expression: String::from("post_collection.description"),
                return_type: Type::String,
                allow_sorting: false,
                nullable: true
            })
        )
    ]);
    pub static ref USER_GROUP_ATTRIBUTES: HashMap<&'static str, Arc<Attribute>> = HashMap::from([
        (
            "date",
            Arc::new(Attribute {
                table: "user_group",
                selection_expression: String::from("user_group.creation_timestamp"),
                return_type: Type::DateTime,
                allow_sorting: true,
                nullable: false
            })
        ),
        (
            "name",
            Arc::new(Attribute {
                table: "user_group",
                selection_expression: String::from("user_group.name"),
                return_type: Type::String,
                allow_sorting: true,
                nullable: false,
            })
        ),
        (
            "owner",
            Arc::new(Attribute {
                table: "user_group",
                selection_expression: String::from("user_group.fk_owner"),
                return_type: Type::Number,
                allow_sorting: false,
                nullable: false
            })
        ),
        (
            "description",
            Arc::new(Attribute {
                table: "user_group",
                selection_expression: String::from("user_group.description"),
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
                                "Expected attribute to be of type {attr_type:?} but got {arg_type:?}"
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
                            "Expected argument to be of type {obj_type:?} but got {arg_type:?}"
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
    args: &mut [Box<Node<dyn ExpressionNode>>],
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
    args: &mut [Box<Node<dyn ExpressionNode>>],
    scope: &Scope,
    log: &mut Log,
) {
    visitor.write_buff("(SELECT ");
    visitor.write_buff(identifier);
    visitor.write_buff("(");

    let argument_count = args.len();
    for (i, argument) in args.iter_mut().enumerate() {
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
            if let Some(attribute) = POST_ATTRIBUTES.get(attribute_node.identifier.as_str())
                && !attribute.allow_sorting
            {
                log.errors.push(Error {
                    location: attr_arg_location,
                    msg: format!("Attribute {} is not sortable", &attribute_node.identifier),
                });
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
        } else {
            log.errors.push(Error {
                location: direction_arg_location,
                msg: format!(
                    "Expected argument to be a string literal but got expression {:?}",
                    &direction_arg.node_type
                ),
            });
        }
    }
}
