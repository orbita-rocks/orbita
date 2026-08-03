//! Connecting to a cluster.
//!
//! Both the admin and the data commands dial the same way, so the endpoint
//! handling and the credential header live here rather than in two places that
//! could drift.
//!
//! A client connects to any worker. The worker forwards a request it does not
//! own to the node that does, so there is no partition map to fetch and no
//! routing to get wrong on this side. That is a deliberate protocol choice:
//! see the Protocol section of the requirements.

use anyhow::{Context, Result};
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

use crate::config::Config;

/// The header a credential secret travels in.
///
/// It is `authorization` with a `Bearer` prefix because that is what every
/// gRPC proxy, gateway, and log scrubber already knows to treat as sensitive.
const AUTHORIZATION: &str = "authorization";

/// Opens a channel to a cluster.
///
/// Connection is lazy. A CLI that failed at connect time would report the
/// error twice, once here and once from the call, and the call's error is the
/// one with the useful context.
pub fn channel(config: &Config) -> Result<Channel> {
    let endpoint = Endpoint::from_shared(config.client.endpoint.clone())
        .with_context(|| format!("{} is not a usable endpoint", config.client.endpoint))?;
    Ok(endpoint.connect_lazy())
}

/// Wraps a message in a request carrying the credential, if there is one.
///
/// An unauthenticated request is allowed through rather than rejected here so
/// that a cluster with authentication turned off stays easy to poke at, and so
/// that the error for a missing credential comes from the server that knows
/// whether one was needed.
pub fn authed<T>(config: &Config, message: T) -> Result<Request<T>> {
    let mut request = Request::new(message);
    if let Some(secret) = &config.client.credential {
        let value: MetadataValue<_> = format!("Bearer {secret}")
            .parse()
            .context("the credential contains characters that cannot go in a header")?;
        request.metadata_mut().insert(AUTHORIZATION, value);
    }
    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Layer;

    fn config(credential: Option<&str>) -> Config {
        let mut layer = Layer::default();
        layer.client.credential = credential.map(str::to_owned);
        layer.resolve().unwrap()
    }

    #[test]
    fn a_credential_travels_as_a_bearer_token() {
        let request = authed(&config(Some("s3cret")), ()).unwrap();
        assert_eq!(
            request.metadata().get(AUTHORIZATION).unwrap(),
            "Bearer s3cret"
        );
    }

    #[test]
    fn a_request_without_a_credential_carries_no_authorization_header() {
        let request = authed(&config(None), ()).unwrap();
        assert!(request.metadata().get(AUTHORIZATION).is_none());
    }

    #[test]
    fn a_credential_with_a_newline_in_it_is_rejected_rather_than_smuggled() {
        assert!(authed(&config(Some("bad\nvalue")), ()).is_err());
    }
}
