//! The JSON credential document AWS's local metadata endpoints serve.
//!
//! IMDS, the ECS task role endpoint, and the EKS Pod Identity agent all answer
//! with the same object: a key pair, a session token AWS calls `Token`, and an
//! RFC 3339 expiry. Parsing it once means the three sources cannot disagree
//! about what an unreadable expiry means, which matters because guessing there
//! is how a node ends up signing with a credential that died an hour ago.

use super::SessionCredentials;
use crate::aws::timestamp::parse_rfc3339_millis;

use orbita_objectstore::s3::Credentials;
use orbita_objectstore::{ObjectError, ObjectResult};

/// Field names are AWS's, and `Token` is the session token rather than
/// anything to do with the IMDSv2 session token, which is a naming collision
/// AWS chose and this struct has to live with.
#[derive(serde::Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CredentialDocument {
    access_key_id: String,
    secret_access_key: String,
    token: String,
    expiration: String,
}

/// Reads one credential document, or says which endpoint produced something
/// unreadable.
///
/// `what` names the source for the message. The parse error itself is never
/// carried into it: the body it failed on is a credential document, and
/// `serde_json` quotes the input it choked on.
pub(crate) fn session_from_json(body: &[u8], what: &str) -> ObjectResult<SessionCredentials> {
    let parsed: CredentialDocument = serde_json::from_slice(body).map_err(|_| {
        ObjectError::Other(format!(
            "{what} returned a credential document this node could not parse"
        ))
    })?;

    let expires_at_millis = parse_rfc3339_millis(&parsed.expiration).ok_or_else(|| {
        ObjectError::Other(format!(
            "{what} reported an expiry this node cannot read: {:?}; refusing to guess how long \
             the credential lasts",
            parsed.expiration
        ))
    })?;

    Ok(SessionCredentials {
        credentials: Credentials {
            access_key_id: parsed.access_key_id,
            secret_access_key: parsed.secret_access_key,
            session_token: Some(parsed.token),
        },
        expires_at_millis: Some(expires_at_millis),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_well_formed_document_carries_its_expiry() {
        let session = session_from_json(
            br#"{"AccessKeyId":"ASIA","SecretAccessKey":"s","Token":"t",
                 "Expiration":"2024-02-29T12:34:56Z"}"#,
            "the test endpoint",
        )
        .expect("parsed");
        assert_eq!(session.credentials.access_key_id, "ASIA");
        assert_eq!(session.expires_at_millis, Some(1_709_210_096_000));
    }

    #[test]
    fn an_unreadable_expiry_is_refused_rather_than_assumed_eternal() {
        assert!(session_from_json(
            br#"{"AccessKeyId":"A","SecretAccessKey":"s","Token":"t","Expiration":"soon"}"#,
            "the test endpoint",
        )
        .is_err());
    }

    #[test]
    fn a_parse_failure_does_not_quote_the_credential_document() {
        let error = session_from_json(br#"{"SecretAccessKey":"the-secret"}"#, "the test endpoint")
            .expect_err("unparseable");
        assert!(
            !format!("{error}").contains("the-secret"),
            "the body that failed to parse is a credential: {error}"
        );
    }
}
