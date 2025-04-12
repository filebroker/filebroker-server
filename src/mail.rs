use std::{fs, io, num::NonZeroU32};

use governor::{
    Quota, RateLimiter,
    clock::DefaultClock,
    middleware::NoOpMiddleware,
    state::{InMemoryState, NotKeyed},
};
use lazy_static::lazy_static;
use lettre::{
    Address, Message, SmtpTransport, Transport,
    message::{DkimConfig, DkimSigningAlgorithm, DkimSigningKey, Mailbox, header::ContentType},
    transport::smtp::authentication::Credentials,
};
use rusty_pool::ThreadPool;
use tera::Tera;

use crate::model::User;

lazy_static! {
    pub static ref MAIL_WORKER_POOL: ThreadPool = rusty_pool::Builder::new()
        .name(String::from("mail_worker_pool"))
        .core_size(2)
        .max_size(8)
        .build();
    pub static ref DKIM_KEY_PATH: Option<String> = std::env::var("FILEBROKER_DKIM_KEY_PATH").ok();
    pub static ref DKIM_SELECTOR: Option<String> = std::env::var("FILEBROKER_DKIM_SELECTOR").ok();
    pub static ref DKIM_DOMAIN: Option<String> = std::env::var("FILEBROKER_DKIM_DOMAIN").ok();
    pub static ref MAIL_SENDER_NAME: String =
        std::env::var("FILEBROKER_MAIL_SENDER_NAME").unwrap_or_else(|_| String::from("filebroker"));
    pub static ref MAIL_SENDER_ADDRESS: Option<Address> =
        std::env::var("FILEBROKER_MAIL_SENDER_ADDRESS")
            .map(|address| address
                .parse()
                .expect("FILEBROKER_MAIL_SENDER_ADDRESS is invalid"))
            .ok();
    pub static ref SMTP_HOST: Option<String> = std::env::var("FILEBROKER_SMTP_HOST").ok();
    pub static ref SMTP_USER: Option<String> = std::env::var("FILEBROKER_SMTP_USER").ok();
    pub static ref SMTP_PASSWORD: Option<String> = std::env::var("FILEBROKER_SMTP_PASSWORD").ok();
    pub static ref DKIM_KEY: Option<Result<String, io::Error>> =
        (*DKIM_KEY_PATH).as_ref().map(fs::read_to_string);
    pub static ref TEMPLATES: Tera = {
        match Tera::new("templates/*.html") {
            Ok(tera) => tera,
            Err(e) => panic!("Could not load tera templates: '{}'", e),
        }
    };
    pub static ref MAILS_PER_HOUR_LIMIT: u32 = std::env::var("FILEBROKER_MAILS_PER_HOUR_LIMIT")
        .map(|limit| limit
            .parse()
            .expect("FILEBROKER_MAILS_PER_HOUR_LIMIT invalid"))
        .unwrap_or(120);
    pub static ref RATE_LIMITER: RateLimiter<NotKeyed, InMemoryState, DefaultClock, NoOpMiddleware> =
        RateLimiter::direct(Quota::per_hour(
            NonZeroU32::new(*MAILS_PER_HOUR_LIMIT)
                .expect("FILEBROKER_MAILS_PER_HOUR_LIMIT invalid")
        ));
}

pub fn mail_enabled() -> bool {
    !(MAIL_SENDER_ADDRESS.is_none()
        || SMTP_HOST.is_none()
        || SMTP_USER.is_none()
        || SMTP_PASSWORD.is_none())
}

pub fn send_mail(
    template: &'static str,
    mut context: tera::Context,
    subject: String,
    recipient: User,
) {
    if MAIL_SENDER_ADDRESS.is_none()
        || SMTP_HOST.is_none()
        || SMTP_USER.is_none()
        || SMTP_PASSWORD.is_none()
    {
        log::error!("Cannot send mail due to incomplete configuration");
        return;
    }

    MAIL_WORKER_POOL.execute(move || {
        let email_address = if let Some(ref email) = recipient.email {
            email
        } else {
            log::error!("No email address for recipient: {}", &recipient.user_name);
            return;
        };

        let reciepient_address: Address = match email_address.parse() {
            Ok(reciepient_address) => reciepient_address,
            Err(e) => {
                log::error!(
                    "Invalid mail address {} for recipient {}: {e}",
                    email_address,
                    &recipient.user_name
                );
                return;
            }
        };

        context.insert("recipient", &recipient);
        let body = match TEMPLATES.render(template, &context) {
            Ok(body) => body,
            Err(e) => {
                log::error!(
                    "Failed to render template {template} for recipient {}: {e}",
                    &recipient.user_name
                );
                return;
            }
        };

        let mut message = Message::builder()
            .from(Mailbox {
                name: Some(MAIL_SENDER_NAME.clone()),
                email: MAIL_SENDER_ADDRESS.clone().unwrap(),
            })
            .to(Mailbox {
                name: None,
                email: reciepient_address,
            })
            .subject(subject)
            .header(ContentType::TEXT_HTML)
            .body(body)
            .unwrap();

        match *DKIM_KEY {
            Some(Ok(ref dkim_key)) => {
                let signing_key = DkimSigningKey::new(dkim_key, DkimSigningAlgorithm::Rsa);
                if DKIM_SELECTOR.is_some() && DKIM_DOMAIN.is_some() {
                    match signing_key {
                        Ok(key) => message.sign(&DkimConfig::default_config(
                            DKIM_SELECTOR.clone().unwrap(),
                            DKIM_DOMAIN.clone().unwrap(),
                            key,
                        )),
                        Err(e) => log::error!("Failed to sign DKIM: {e}"),
                    }
                } else {
                    log::error!("DKIM config is incomplete");
                }
            }
            Some(Err(ref e)) => log::error!("Failed to read DKIM file: {e}"),
            None => {}
        }

        let creds = Credentials::new(SMTP_USER.clone().unwrap(), SMTP_PASSWORD.clone().unwrap());

        let mailer = SmtpTransport::relay(SMTP_HOST.as_ref().unwrap())
            .unwrap()
            .credentials(creds)
            .build();

        futures::executor::block_on(RATE_LIMITER.until_ready());
        if let Err(e) = mailer.send(&message) {
            log::error!(
                "Failed sending mail of template {template} to '{} <{}>': {e}",
                &recipient.user_name,
                &email_address
            );
        } else {
            log::info!(
                "Mail of template {template} sent to '{} <{}>'",
                &recipient.user_name,
                &email_address
            );
        }
    });
}
