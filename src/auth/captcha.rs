use std::net::SocketAddr;

use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};

use crate::{error::Error, util};

lazy_static! {
    pub static ref CAPTCHA_SITE_KEY: Option<String> =
        std::env::var("FILEBROKER_CAPTCHA_SITE_KEY").ok();
    pub static ref CAPTCHA_SECRET: Option<String> = std::env::var("FILEBROKER_CAPTCHA_SECRET").ok();
}

#[derive(Serialize)]
pub struct CaptchaRequest {
    secret: String,
    response: String,
    remoteip: Option<String>,
    sitekey: Option<String>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
pub struct CaptchaResponse {
    success: bool,
    credit: Option<bool>,
    #[serde(rename = "error-codes")]
    error_codes: Option<Vec<String>>,
}

pub async fn verify_captcha(
    secret: String,
    token: String,
    remote_addr: Option<SocketAddr>,
) -> Result<(), Error> {
    let request = CaptchaRequest {
        secret,
        response: token,
        remoteip: remote_addr.map(|addr| util::addr_to_ip_string(&addr)),
        sitekey: CAPTCHA_SITE_KEY.clone(),
    };

    let client = reqwest::Client::new();
    let response = client
        .post("https://hcaptcha.com/siteverify")
        .form(&request)
        .send()
        .await?
        .json::<CaptchaResponse>()
        .await?;

    if response.success {
        Ok(())
    } else {
        Err(Error::CaptchaValidationError(format!(
            "Captcha validation was unsuccessful with errors: {:?}",
            response.error_codes
        )))
    }
}
