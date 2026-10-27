//! Making the Iggy user the server acts as, so that the server never has to
//! be given the root user's password.
//!
//! An operator, or the compose stack, runs this once with the root credentials
//! and then starts the server with the credentials of the user made here.

use std::time::Duration;

use iggy::prelude::{
    AutoLogin, Client, Credentials, Identifier, IggyClientBuilder, IggyError, UserClient,
    UserStatus,
};
use mansio_core::BrokerError;
use secrecy::{ExposeSecret, SecretString};

use crate::access::admin_permissions;

/// What [`provision_admin`] needs.
#[derive(Debug, Clone)]
pub struct ProvisionConfig {
    /// `host:port` of Iggy's TCP listener.
    pub address: String,
    /// The root user, or another that manages users.
    pub root_username: String,
    /// Its password.
    pub root_password: SecretString,
    /// The user to make for the server.
    pub admin_username: String,
    /// Its password. Used only when the user is made.
    pub admin_password: SecretString,
    /// How long to wait for Iggy.
    pub connect_timeout: Duration,
}

/// What [`provision_admin`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provisioned {
    /// The user did not exist and was made.
    Created,
    /// The user existed. Its permissions were set again and its password was
    /// left as it was.
    Updated,
}

/// Make the admin user, or set its permissions again if it exists: manage
/// users and streams, and read and write every topic, and nothing about the
/// server itself.
///
/// # Errors
///
/// Returns [`BrokerError::Unavailable`] if Iggy does not answer in time, and an
/// error if Iggy refuses the root credentials or the request.
pub async fn provision_admin(config: ProvisionConfig) -> Result<Provisioned, BrokerError> {
    let refused = |error: IggyError| BrokerError::Rejected(error.to_string());
    let client = IggyClientBuilder::new()
        .with_tcp()
        .with_server_address(config.address.clone())
        .with_auto_sign_in(AutoLogin::Enabled(Credentials::UsernamePassword(
            config.root_username.clone(),
            config.root_password.clone(),
        )))
        .build()
        .map_err(refused)?;
    tokio::time::timeout(config.connect_timeout, client.connect())
        .await
        .map_err(|_| {
            BrokerError::Unavailable(format!(
                "no answer from Iggy at {} within {:?}",
                config.address, config.connect_timeout
            ))
        })?
        .map_err(refused)?;

    let made = client
        .create_user(
            &config.admin_username,
            config.admin_password.expose_secret(),
            UserStatus::Active,
            Some(admin_permissions()),
        )
        .await;
    let done = match made {
        Ok(_) => Provisioned::Created,
        Err(IggyError::UserAlreadyExists) => {
            let name = Identifier::named(&config.admin_username).map_err(refused)?;
            client
                .update_permissions(&name, Some(admin_permissions()))
                .await
                .map_err(refused)?;
            Provisioned::Updated
        }
        Err(error) => return Err(refused(error)),
    };
    let _ = client.shutdown().await;
    Ok(done)
}
