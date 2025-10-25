use crate::error::{Error, TransactionRuntimeError};
use crate::model::{NewUserGroup, User, UserGroup, UserGroupTag};
use crate::perms::get_current_user_groups;
use crate::schema::{user_group, user_group_tag};
use crate::tag::create::handle_entered_and_selected_tags;
use crate::tag::validate_tags;
use crate::user_group::load_user_group_detailed;
use crate::util::dedup_vec_optional;
use crate::{acquire_db_connection, run_retryable_transaction};
use chrono::Utc;
use diesel_async::RunQueryDsl;
use diesel_async::scoped_futures::ScopedFutureExt;
use serde::Deserialize;
use validator::Validate;
use warp::{Rejection, Reply};

#[derive(Deserialize, Validate)]
pub struct CreateUserGroupRequest {
    pub name: String,
    #[serde(rename = "is_public")]
    pub public: Option<bool>,
    pub description: Option<String>,
    pub allow_member_invite: Option<bool>,
    #[validate(length(max = 100), custom(function = "validate_tags"))]
    pub entered_tags: Option<Vec<String>>,
    #[validate(length(max = 100))]
    pub selected_tags: Option<Vec<i64>>,
}

pub async fn create_user_group_handler(
    mut request: CreateUserGroupRequest,
    user: User,
) -> Result<impl Reply, Rejection> {
    request.validate().map_err(|e| {
        warp::reject::custom(Error::InvalidRequestInputError(format!(
            "Validation failed for CreateUserGroupRequest: {e}"
        )))
    })?;

    dedup_vec_optional(&mut request.entered_tags);
    dedup_vec_optional(&mut request.selected_tags);

    let mut connection = acquire_db_connection().await?;
    let user_group_detailed = run_retryable_transaction(&mut connection, |connection| {
        async move {
            let current_groups = get_current_user_groups(connection, &user).await?;
            if current_groups.len() >= 250 {
                return Err(TransactionRuntimeError::from(Error::BadRequestError(
                    String::from("Cannot be a member of more than 250 groups"),
                )));
            }

            let user_group = diesel::insert_into(user_group::table)
                .values(&NewUserGroup {
                    name: request.name,
                    public: request.public.unwrap_or(false),
                    fk_owner: user.pk,
                    creation_timestamp: Utc::now(),
                    description: request.description,
                    allow_member_invite: request.allow_member_invite.unwrap_or(false),
                    avatar_object_key: None,
                    fk_create_user: user.pk,
                })
                .get_result::<UserGroup>(connection)
                .await?;

            let set_tags = handle_entered_and_selected_tags(
                &request.selected_tags,
                request.entered_tags,
                &user,
                connection,
            )
            .await?;

            let user_group_tags = set_tags
                .iter()
                .map(|tag| UserGroupTag {
                    fk_user_group: user_group.pk,
                    fk_tag: tag.pk,
                    auto_matched: false,
                })
                .collect::<Vec<_>>();

            if !user_group_tags.is_empty() {
                diesel::insert_into(user_group_tag::table)
                    .values(&user_group_tags)
                    .execute(connection)
                    .await?;
            }

            let user_group_detailed =
                load_user_group_detailed(user_group, Some(&user), connection).await?;

            Ok(user_group_detailed)
        }
        .scope_boxed()
    })
    .await?;

    Ok(warp::reply::json(&user_group_detailed))
}
