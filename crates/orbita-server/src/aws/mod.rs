//! Sourcing AWS credentials without a long-lived key.
//!
//! `docs/REQUIREMENTS.md` says the S3 backend cannot be limited to static
//! keys: on AWS the node has to be able to assume a role, and it has to be
//! able to read the instance profile, because a long-lived access key sitting
//! in a Kubernetes Secret is the thing security-conscious AWS shops prohibit
//! outright. A deployment that cannot run without one fails review before it
//! starts.
//!
//! # Why not the AWS SDK
//!
//! This node signs its own requests. `orbita-objectstore` explains why —
//! the SDK's signer arrives welded to its own HTTP client, retry policy, and
//! clock, and a deterministic simulator cannot have any of those. Credential
//! sourcing has exactly the same property, and worse: a credential provider
//! with its own background refresh timer would make credential expiry an event
//! that happens between the operations a simulated run can observe. So the
//! providers here are built on the same two seams the store already uses,
//! `HttpTransport` for the network and an injected clock for time, and the
//! `aws-config` dependency is gone.
//!
//! # The shape
//!
//! - [`imds::InstanceProfile`] reads a session credential out of the EC2
//!   metadata service over IMDSv2. This is the keyless floor: no Secret
//!   anywhere.
//! - [`web_identity::WebIdentity`] trades a projected OIDC token for a session,
//!   which is how a pod on EKS authenticates under IRSA.
//! - [`container::ContainerCredentials`] reads the credential an ECS task role
//!   or the EKS Pod Identity agent serves.
//! - [`sts::AssumeRole`] exchanges any base credential for a session on a
//!   named role. It layers over any of the above.
//! - [`refresh::RefreshingCredentials`] caches whatever a source produces and
//!   replaces it before it expires, without stalling traffic and without a
//!   background task.
//! - Static keys are `orbita_objectstore::s3::StaticCredentials` and need
//!   none of this, which is right: MinIO and R2 have nothing else to offer.
//!
//! # Resolution: named, or resolved once and said out loud
//!
//! [`S3CredentialSource`] names a source, and a named source is used and no
//! other is tried. The reasoning is unchanged from when this crate had only
//! two of them: a provider that falls through at request time to whatever is
//! reachable produces an audit log full of the wrong principal, and the first
//! anyone hears of it is a compliance finding.
//!
//! But *not naming one* cannot mean "the instance profile", which is what an
//! earlier draft of this module did. Before it, a keyless configuration went
//! through the AWS default chain, and on EKS that chain reaches IRSA long
//! before it reaches IMDS. Mapping keyless to IMDS therefore does not merely
//! break those deployments — where node IMDS is reachable it *succeeds*, as
//! the node role instead of the workload role. A silent change of principal is
//! worse than an outage, because an outage is noticed.
//!
//! So [`S3CredentialSource::Default`] resolves the AWS chain's order —
//! environment, then web identity, then container, then instance profile — but
//! it does it **once, at startup**, logs which one it picked, and refuses to
//! start when the ambient configuration points at a source this node does not
//! implement (a shared `~/.aws` profile) rather than quietly moving on to the
//! next one. That keeps the property the named sources were introduced for,
//! which is that the principal a node authenticates as is knowable from its
//! logs, without silently re-pointing an existing deployment.

pub(crate) mod container;
pub(crate) mod document;
pub(crate) mod imds;
pub(crate) mod partition;
pub(crate) mod refresh;
pub(crate) mod sts;
mod timestamp;
pub(crate) mod web_identity;

#[cfg(test)]
pub(crate) mod testing;

pub use container::ContainerCredentialsConfig;
pub use sts::{AssumeRoleConfig, DEFAULT_SESSION_DURATION_SECONDS};
pub use web_identity::WebIdentityConfig;

use crate::config::{S3CredentialSource, S3StorageConfig};

use async_trait::async_trait;
use orbita_objectstore::s3::{
    Credentials, CredentialsProvider, HttpTransport, NowMillis, StaticCredentials,
};
use orbita_objectstore::{ObjectError, ObjectResult};
use orbita_runtime::Clock;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

/// The session name used when nothing better is configured.
///
/// Every real deployment overrides this with something carrying the node
/// identity, because a session name that is the same on every node makes an
/// audit log answer no question anybody asks it.
const FALLBACK_SESSION_NAME: &str = "orbita";

/// A credential and the instant it stops working.
///
/// `expires_at_millis` is `None` for a credential that never expires, which is
/// what makes a static key legal underneath the refresh logic with no special
/// case anywhere.
pub(crate) struct SessionCredentials {
    pub(crate) credentials: Credentials,
    pub(crate) expires_at_millis: Option<u64>,
}

/// Delegates to [`Credentials`], which redacts the secret and the session
/// token. The expiry is not a secret and is the one field worth seeing in a
/// log line about credential refresh.
impl std::fmt::Debug for SessionCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionCredentials")
            .field("credentials", &self.credentials)
            .field("expires_at_millis", &self.expires_at_millis)
            .finish()
    }
}

/// Something that can produce a fresh credential from scratch.
///
/// Separate from `CredentialsProvider` on purpose: a provider answers the
/// question "what should I sign with right now", which is usually a cache
/// read, while a source is the expensive thing behind the cache. Splitting
/// them is what keeps the refresh policy in one place instead of copied into
/// every source.
#[async_trait]
pub(crate) trait SessionSource: Send + Sync + 'static {
    async fn fetch(&self) -> ObjectResult<SessionCredentials>;

    /// A short phrase naming this source for a log message. It must never
    /// contain a credential; a role ARN or an endpoint is fine.
    fn describe(&self) -> String;
}

/// The AWS-relevant part of the process environment, snapshotted.
///
/// A snapshot rather than live `std::env::var` calls for two reasons. Source
/// resolution happens once, at startup, and reading the environment twice
/// during it could produce two different answers. And `std::env::set_var` is
/// process-global, so a test that set variables to exercise resolution would
/// race every other test in the binary; passing a map means the resolution
/// tests are ordinary pure-function tests.
#[derive(Debug, Clone, Default)]
pub struct AwsEnvironment {
    vars: BTreeMap<String, String>,
}

impl AwsEnvironment {
    /// Snapshots the real process environment.
    #[must_use]
    pub fn from_process() -> Self {
        Self {
            vars: std::env::vars().collect(),
        }
    }

    /// An environment built from pairs, so that resolution can be exercised
    /// without `std::env::set_var` racing every other test in the binary.
    #[cfg(test)]
    #[must_use]
    pub fn from_pairs<K: Into<String>, V: Into<String>>(
        pairs: impl IntoIterator<Item = (K, V)>,
    ) -> Self {
        Self {
            vars: pairs
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        }
    }

    /// The value of `name`, treating empty as unset.
    ///
    /// Kubernetes renders an unset value as an empty string rather than
    /// omitting the variable, and an empty `AWS_ROLE_ARN` means "no role", not
    /// "a role named nothing".
    fn get(&self, name: &str) -> Option<&str> {
        self.vars
            .get(name)
            .map(String::as_str)
            .filter(|value| !value.trim().is_empty())
    }
}

/// The source resolution settled on, with everything it needs already in hand.
///
/// Not `PartialEq`, because two of its variants carry a credential and an
/// equality operator on a credential is a timing side channel waiting to be
/// used as one. Tests compare [`ResolvedSource::kind`].
#[derive(Debug, Clone)]
pub(crate) enum ResolvedSource {
    /// A key pair from Orbita's own configuration.
    Static(Credentials),
    /// A key pair from `AWS_ACCESS_KEY_ID` and friends.
    Environment(Credentials),
    /// EKS IRSA.
    WebIdentity(WebIdentityConfig),
    /// An ECS task role or the EKS Pod Identity agent.
    Container(ContainerCredentialsConfig),
    /// The EC2 instance profile over IMDSv2.
    InstanceProfile,
}

/// Which source was picked, with nothing secret attached.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SourceKind {
    Static,
    Environment,
    WebIdentity,
    Container,
    InstanceProfile,
}

impl ResolvedSource {
    #[cfg(test)]
    pub(crate) fn kind(&self) -> SourceKind {
        match self {
            Self::Static(_) => SourceKind::Static,
            Self::Environment(_) => SourceKind::Environment,
            Self::WebIdentity(_) => SourceKind::WebIdentity,
            Self::Container(_) => SourceKind::Container,
            Self::InstanceProfile => SourceKind::InstanceProfile,
        }
    }

    /// A phrase for the startup log line. It names the principal's *source*,
    /// which is the thing an operator has to be able to check against what
    /// they intended, and never a credential.
    fn describe(&self) -> String {
        match self {
            Self::Static(_) => "a static key pair from configuration".to_string(),
            Self::Environment(_) => "a static key pair from the process environment".to_string(),
            Self::WebIdentity(config) => {
                format!("EKS IRSA, assuming {} (web identity)", config.role_arn)
            }
            Self::Container(config) => {
                format!("the container credential provider at {}", config.uri)
            }
            Self::InstanceProfile => "the EC2 instance profile over IMDSv2".to_string(),
        }
    }
}

/// Picks the credential source for `config` in this `env`.
///
/// A named source is honoured exactly; only [`S3CredentialSource::Default`]
/// consults the environment, and it does so in the order the AWS SDKs
/// document, so that a deployment which worked before this crate existed keeps
/// authenticating as the same principal.
pub(crate) fn resolve_source(
    config: &S3StorageConfig,
    env: &AwsEnvironment,
) -> ObjectResult<ResolvedSource> {
    match &config.credentials {
        S3CredentialSource::Static(credentials) => Ok(ResolvedSource::Static(credentials.clone())),
        S3CredentialSource::Environment => environment_credentials(env)
            .map(ResolvedSource::Environment)
            .ok_or_else(|| {
                ObjectError::Other(
                    "the credential source is the process environment, but AWS_ACCESS_KEY_ID and \
                     AWS_SECRET_ACCESS_KEY are not both set"
                        .to_string(),
                )
            }),
        S3CredentialSource::WebIdentity => web_identity_config(config, env).ok_or_else(|| {
            ObjectError::Other(format!(
                "the credential source is web identity, but {} and {} are not both set; on EKS \
                 these come from the pod identity webhook, which only injects them when the \
                 service account carries an eks.amazonaws.com/role-arn annotation",
                web_identity::TOKEN_FILE_VARIABLE,
                web_identity::ROLE_ARN_VARIABLE,
            ))
        }),
        S3CredentialSource::ContainerCredentials => container_config(env)
            .map(ResolvedSource::Container)
            .ok_or_else(|| {
                ObjectError::Other(format!(
                    "the credential source is the container credential provider, but neither {} \
                     nor {} is set",
                    container::RELATIVE_URI_VARIABLE,
                    container::FULL_URI_VARIABLE,
                ))
            }),
        S3CredentialSource::InstanceProfile => Ok(ResolvedSource::InstanceProfile),
        S3CredentialSource::Default => resolve_default(config, env),
    }
}

/// The AWS default chain's order, resolved once.
///
/// Environment, then web identity, then container, then instance profile. The
/// shared-profile step the SDKs have between environment and web identity is
/// not implemented here and is therefore an error rather than a step that gets
/// skipped: skipping it is precisely how a node ends up on IMDS as the wrong
/// principal.
fn resolve_default(config: &S3StorageConfig, env: &AwsEnvironment) -> ObjectResult<ResolvedSource> {
    if let Some(credentials) = environment_credentials(env) {
        return Ok(ResolvedSource::Environment(credentials));
    }
    if let Some(web_identity) = web_identity_config(config, env) {
        return Ok(web_identity);
    }
    if let Some(container) = container_config(env) {
        return Ok(ResolvedSource::Container(container));
    }
    if let Some(profile) = shared_profile_hint(env) {
        return Err(ObjectError::Other(format!(
            "this node's AWS credentials would come from {profile}, and Orbita does not read \
             shared AWS profiles. Refusing to start rather than falling through to the EC2 \
             instance profile, which would authenticate as a different principal. Set \
             object_store.credential_source explicitly (static, environment, web-identity, \
             container, or instance-profile), or unset the profile variables."
        )));
    }
    Ok(ResolvedSource::InstanceProfile)
}

/// A key pair from the environment, if both halves are there.
fn environment_credentials(env: &AwsEnvironment) -> Option<Credentials> {
    Some(Credentials {
        access_key_id: env.get("AWS_ACCESS_KEY_ID")?.to_string(),
        secret_access_key: env.get("AWS_SECRET_ACCESS_KEY")?.to_string(),
        session_token: env.get("AWS_SESSION_TOKEN").map(str::to_string),
    })
}

/// The IRSA configuration this pod was injected with, if it was.
fn web_identity_config(config: &S3StorageConfig, env: &AwsEnvironment) -> Option<ResolvedSource> {
    let token_file = env.get(web_identity::TOKEN_FILE_VARIABLE)?;
    let role_arn = env.get(web_identity::ROLE_ARN_VARIABLE)?;
    Some(ResolvedSource::WebIdentity(WebIdentityConfig {
        token_file: PathBuf::from(token_file),
        role_arn: role_arn.to_string(),
        session_name: env
            .get(web_identity::SESSION_NAME_VARIABLE)
            .map(str::to_string)
            .or_else(|| config.session_name.clone())
            .unwrap_or_else(|| FALLBACK_SESSION_NAME.to_string()),
        endpoint: config.sts_endpoint.clone(),
        duration_seconds: DEFAULT_SESSION_DURATION_SECONDS,
    }))
}

/// The container credential endpoint this task was given, if it was given one.
fn container_config(env: &AwsEnvironment) -> Option<ContainerCredentialsConfig> {
    let authorization_token = env.get(container::TOKEN_VARIABLE).map(str::to_string);
    let authorization_token_file = env.get(container::TOKEN_FILE_VARIABLE).map(PathBuf::from);

    // The full form wins: EKS Pod Identity sets it, and a task that somehow
    // had both would be an ECS task inside a Pod Identity pod, which is not a
    // thing.
    if let Some(uri) = env.get(container::FULL_URI_VARIABLE) {
        return Some(ContainerCredentialsConfig {
            uri: uri.to_string(),
            authorization_token,
            authorization_token_file,
        });
    }
    env.get(container::RELATIVE_URI_VARIABLE)
        .map(|relative| ContainerCredentialsConfig {
            authorization_token,
            authorization_token_file,
            ..ContainerCredentialsConfig::relative(relative)
        })
}

/// Whether the environment says credentials should come from a shared profile.
///
/// Returns what to name in the error. The file check is last and cheapest to
/// be wrong about: a `~/.aws/credentials` that exists but is not the intended
/// source costs a startup error and one line of configuration, while silently
/// ignoring one that *is* the intended source costs a node running as the
/// wrong principal until somebody audits CloudTrail.
fn shared_profile_hint(env: &AwsEnvironment) -> Option<String> {
    for variable in [
        "AWS_PROFILE",
        "AWS_SHARED_CREDENTIALS_FILE",
        "AWS_CONFIG_FILE",
    ] {
        if let Some(value) = env.get(variable) {
            return Some(format!("{variable}={value}"));
        }
    }
    let home = env.get("HOME")?;
    let credentials = PathBuf::from(home).join(".aws").join("credentials");
    credentials
        .is_file()
        .then(|| credentials.display().to_string())
}

/// Builds the provider chain one worker signs its S3 requests with.
///
/// Time comes from the runtime's clock and the network from the shared
/// transport, so the whole chain is drivable by a simulated run: injecting an
/// expiry is moving the clock, and injecting an IMDS failure is answering one
/// request differently.
pub(crate) fn credentials_provider<C: Clock>(
    clock: &C,
    transport: Arc<dyn HttpTransport>,
    now_millis: NowMillis,
    config: &S3StorageConfig,
) -> ObjectResult<Arc<dyn CredentialsProvider>> {
    credentials_provider_in(
        clock,
        transport,
        now_millis,
        config,
        &AwsEnvironment::from_process(),
    )
}

pub(crate) fn credentials_provider_in<C: Clock>(
    clock: &C,
    transport: Arc<dyn HttpTransport>,
    now_millis: NowMillis,
    config: &S3StorageConfig,
    env: &AwsEnvironment,
) -> ObjectResult<Arc<dyn CredentialsProvider>> {
    let resolved = resolve_source(config, env)?;

    // Said out loud, once, at startup. The whole argument for naming a source
    // rather than discovering one is that the principal a node authenticates
    // as should be knowable without reading CloudTrail, and that is only true
    // if the node says which one it picked.
    tracing::info!(
        source = %resolved.describe(),
        assume_role = config.assume_role.as_ref().map(|role| role.role_arn.as_str()),
        "sourcing AWS credentials"
    );

    let base: Arc<dyn CredentialsProvider> = match resolved {
        ResolvedSource::Static(credentials) | ResolvedSource::Environment(credentials) => {
            StaticCredentials::shared(credentials)
        }
        ResolvedSource::WebIdentity(web_identity_config) => {
            Arc::new(refresh::RefreshingCredentials::new(
                clock.clone(),
                web_identity::WebIdentity::new(
                    transport.clone(),
                    &config.region,
                    web_identity_config,
                )?,
            ))
        }
        ResolvedSource::Container(container_config) => {
            Arc::new(refresh::RefreshingCredentials::new(
                clock.clone(),
                container::ContainerCredentials::new(transport.clone(), container_config)?,
            ))
        }
        ResolvedSource::InstanceProfile => Arc::new(refresh::RefreshingCredentials::new(
            clock.clone(),
            imds::InstanceProfile::new(transport.clone(), config.imds_authority()),
        )),
    };

    let Some(assume_role) = &config.assume_role else {
        return Ok(base);
    };
    // One endpoint override for both STS calls: a deployment on PrivateLink or
    // in an isolated partition needs the same host for AssumeRole as for the
    // web identity exchange, and making them two settings is making them two
    // things to get wrong.
    let assume_role = AssumeRoleConfig {
        endpoint: assume_role
            .endpoint
            .clone()
            .or_else(|| config.sts_endpoint.clone()),
        ..assume_role.clone()
    };
    Ok(Arc::new(refresh::RefreshingCredentials::new(
        clock.clone(),
        sts::AssumeRole::new(
            base,
            transport,
            now_millis,
            config.region.clone(),
            assume_role,
        )?,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aws::testing::ScriptedTransport;
    use orbita_objectstore::s3::{HttpResponse, TransportFailure};
    use orbita_runtime::tokio_runtime::TokioClock;

    fn storage_config(credentials: S3CredentialSource) -> S3StorageConfig {
        S3StorageConfig {
            endpoint: "https://s3.us-east-1.amazonaws.com".to_string(),
            bucket: "orbita".to_string(),
            region: "us-east-1".to_string(),
            credentials,
            assume_role: None,
            imds_endpoint: None,
            sts_endpoint: None,
            session_name: Some("orbita-node-1".to_string()),
            force_path_style: false,
        }
    }

    fn ok(body: &str) -> Result<HttpResponse, TransportFailure> {
        Ok(HttpResponse {
            status: 200,
            headers: vec![],
            body: bytes::Bytes::from(body.to_string()),
        })
    }

    fn static_keys() -> S3CredentialSource {
        S3CredentialSource::Static(Credentials {
            access_key_id: "AKIABASE".to_string(),
            secret_access_key: "base-secret".to_string(),
            session_token: None,
        })
    }

    /// An environment with nothing AWS-shaped in it, and no `HOME`, so that
    /// the shared-profile check cannot pick up the developer's own laptop.
    fn bare() -> AwsEnvironment {
        AwsEnvironment::default()
    }

    fn irsa() -> Vec<(&'static str, &'static str)> {
        vec![
            (
                web_identity::TOKEN_FILE_VARIABLE,
                "/var/run/secrets/eks.amazonaws.com/serviceaccount/token",
            ),
            (
                web_identity::ROLE_ARN_VARIABLE,
                "arn:aws:iam::123456789012:role/orbita-workload",
            ),
        ]
    }

    fn ecs() -> Vec<(&'static str, &'static str)> {
        vec![(
            container::RELATIVE_URI_VARIABLE,
            "/v2/credentials/abcd-1234",
        )]
    }

    fn resolved(source: S3CredentialSource, env: &AwsEnvironment) -> ResolvedSource {
        resolve_source(&storage_config(source), env).expect("resolvable")
    }

    // --- The chain order -------------------------------------------------
    //
    // These are the tests the review asked for. The order they assert is the
    // AWS default chain's, minus the shared profile, which is an error rather
    // than a skipped step.

    #[test]
    fn a_keyless_configuration_does_not_silently_land_on_the_instance_profile() {
        // The regression this whole module exists to not have: on EKS the
        // instance profile is the *node* role. Falling through to it does not
        // fail, it succeeds as the wrong principal.
        let env = AwsEnvironment::from_pairs(irsa());
        assert!(
            matches!(
                resolved(S3CredentialSource::Default, &env),
                ResolvedSource::WebIdentity(_)
            ),
            "IRSA must beat IMDS, or an EKS workload silently becomes its node"
        );
    }

    #[test]
    fn the_default_chain_prefers_the_environment_then_web_identity_then_container_then_imds() {
        let everything = AwsEnvironment::from_pairs(
            irsa()
                .into_iter()
                .chain(ecs())
                .chain([
                    ("AWS_ACCESS_KEY_ID", "AKIAENV"),
                    ("AWS_SECRET_ACCESS_KEY", "env-secret"),
                ])
                .collect::<Vec<_>>(),
        );
        assert!(matches!(
            resolved(S3CredentialSource::Default, &everything),
            ResolvedSource::Environment(_)
        ));

        let without_keys =
            AwsEnvironment::from_pairs(irsa().into_iter().chain(ecs()).collect::<Vec<_>>());
        assert!(matches!(
            resolved(S3CredentialSource::Default, &without_keys),
            ResolvedSource::WebIdentity(_)
        ));

        let container_only = AwsEnvironment::from_pairs(ecs());
        assert!(matches!(
            resolved(S3CredentialSource::Default, &container_only),
            ResolvedSource::Container(_)
        ));

        assert_eq!(
            resolved(S3CredentialSource::Default, &bare()).kind(),
            SourceKind::InstanceProfile,
            "with nothing ambient, IMDS is the right answer and the only one left"
        );
    }

    #[test]
    fn eks_pod_identity_is_reached_through_the_container_source() {
        let env = AwsEnvironment::from_pairs([
            (
                container::FULL_URI_VARIABLE,
                "http://169.254.170.23/v1/credentials",
            ),
            (
                container::TOKEN_FILE_VARIABLE,
                "/var/run/secrets/pods.eks.amazonaws.com/serviceaccount/eks-pod-identity-token",
            ),
        ]);
        match resolved(S3CredentialSource::Default, &env) {
            ResolvedSource::Container(config) => {
                assert_eq!(config.uri, "http://169.254.170.23/v1/credentials");
                assert!(config.authorization_token_file.is_some());
            }
            other => panic!("expected the container provider, got {other:?}"),
        }
    }

    #[test]
    fn a_shared_profile_fails_closed_rather_than_falling_through_to_imds() {
        let env = AwsEnvironment::from_pairs([("AWS_PROFILE", "orbita-prod")]);
        let error = resolve_source(&storage_config(S3CredentialSource::Default), &env)
            .expect_err("refusing to guess");
        let message = format!("{error}");
        assert!(message.contains("AWS_PROFILE=orbita-prod"), "{message}");
        assert!(
            message.contains("credential_source"),
            "the error has to carry the migration: {message}"
        );
    }

    #[test]
    fn a_named_source_is_used_even_when_the_environment_suggests_another() {
        // The property the named sources exist for: no discovery, ever, once
        // an operator has said which principal this node is.
        let env = AwsEnvironment::from_pairs(
            irsa()
                .into_iter()
                .chain(ecs())
                .chain([
                    ("AWS_ACCESS_KEY_ID", "AKIAENV"),
                    ("AWS_SECRET_ACCESS_KEY", "env-secret"),
                ])
                .collect::<Vec<_>>(),
        );
        assert_eq!(
            resolved(S3CredentialSource::InstanceProfile, &env).kind(),
            SourceKind::InstanceProfile
        );
        assert!(matches!(
            resolved(S3CredentialSource::ContainerCredentials, &env),
            ResolvedSource::Container(_)
        ));
    }

    #[test]
    fn a_named_source_whose_environment_is_missing_fails_at_startup() {
        for source in [
            S3CredentialSource::Environment,
            S3CredentialSource::WebIdentity,
            S3CredentialSource::ContainerCredentials,
        ] {
            assert!(
                resolve_source(&storage_config(source.clone()), &bare()).is_err(),
                "{source:?} with nothing to read it from must not silently become something else"
            );
        }
    }

    #[test]
    fn an_empty_environment_variable_counts_as_unset() {
        // Kubernetes renders an unset value as "", and an empty AWS_ROLE_ARN
        // means no role rather than a role named nothing.
        let env = AwsEnvironment::from_pairs([
            (web_identity::TOKEN_FILE_VARIABLE, "/token"),
            (web_identity::ROLE_ARN_VARIABLE, ""),
        ]);
        assert_eq!(
            resolved(S3CredentialSource::Default, &env).kind(),
            SourceKind::InstanceProfile
        );
    }

    #[test]
    fn the_web_identity_session_name_identifies_the_node() {
        let env = AwsEnvironment::from_pairs(irsa());
        match resolved(S3CredentialSource::Default, &env) {
            ResolvedSource::WebIdentity(config) => {
                assert_eq!(config.session_name, "orbita-node-1");
            }
            other => panic!("expected web identity, got {other:?}"),
        }

        let annotated = AwsEnvironment::from_pairs(
            irsa()
                .into_iter()
                .chain([(web_identity::SESSION_NAME_VARIABLE, "from-the-webhook")])
                .collect::<Vec<_>>(),
        );
        match resolved(S3CredentialSource::Default, &annotated) {
            ResolvedSource::WebIdentity(config) => {
                assert_eq!(config.session_name, "from-the-webhook");
            }
            other => panic!("expected web identity, got {other:?}"),
        }
    }

    #[test]
    fn the_resolved_source_is_describable_without_naming_a_credential() {
        let env = AwsEnvironment::from_pairs([
            ("AWS_ACCESS_KEY_ID", "AKIAENV"),
            ("AWS_SECRET_ACCESS_KEY", "the-env-secret"),
        ]);
        let described = resolved(S3CredentialSource::Default, &env).describe();
        assert!(
            !described.contains("the-env-secret") && !described.contains("AKIAENV"),
            "the startup log line must not be a credential dump: {described}"
        );
    }

    // --- Wiring ----------------------------------------------------------

    #[tokio::test]
    async fn static_keys_reach_the_signer_without_touching_the_network() {
        let transport = ScriptedTransport::new(vec![]);
        let provider = credentials_provider_in(
            &TokioClock::new(),
            transport.clone(),
            Arc::new(|| 0),
            &storage_config(static_keys()),
            &bare(),
        )
        .expect("valid configuration");

        let credentials = provider.credentials().await.expect("static");
        assert_eq!(credentials.access_key_id, "AKIABASE");
        assert!(transport.requests().is_empty());
    }

    #[tokio::test]
    async fn an_irsa_pod_talks_to_sts_and_never_to_the_metadata_service() {
        // The end-to-end form of the regression test above: with IRSA injected
        // and no source named, not one request may go to 169.254.169.254.
        let token = std::env::temp_dir().join(format!(
            "orbita-irsa-resolution-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::write(&token, "projected").expect("writing a temp file");

        let env = AwsEnvironment::from_pairs([
            (
                web_identity::TOKEN_FILE_VARIABLE,
                token.display().to_string(),
            ),
            (
                web_identity::ROLE_ARN_VARIABLE,
                "arn:aws:iam::123456789012:role/orbita-workload".to_string(),
            ),
        ]);
        let transport =
            ScriptedTransport::new(vec![ok("<Credentials><AccessKeyId>ASIAIRSA</AccessKeyId>\
             <SecretAccessKey>s</SecretAccessKey><SessionToken>t</SessionToken>\
             <Expiration>2099-01-01T00:00:00Z</Expiration></Credentials>")]);

        let provider = credentials_provider_in(
            &TokioClock::new(),
            transport.clone(),
            Arc::new(|| 1_369_353_600_000),
            &storage_config(S3CredentialSource::Default),
            &env,
        )
        .expect("valid configuration");

        assert_eq!(
            provider.credentials().await.expect("sourced").access_key_id,
            "ASIAIRSA"
        );
        let authorities: Vec<String> = transport
            .requests()
            .iter()
            .map(|request| request.authority.clone())
            .collect();
        assert_eq!(authorities, vec!["sts.us-east-1.amazonaws.com"]);
        assert!(
            !authorities.iter().any(|a| a == imds::IMDS_AUTHORITY),
            "a keyless IRSA pod that reaches IMDS is authenticating as its node: {authorities:?}"
        );
        let _ = std::fs::remove_file(&token);
    }

    #[tokio::test]
    async fn assume_role_layers_over_static_keys() {
        let mut config = storage_config(static_keys());
        config.assume_role = Some(AssumeRoleConfig::new(
            "arn:aws:iam::123456789012:role/orbita",
            "orbita-node-1",
        ));
        let transport = ScriptedTransport::new(vec![ok(
            "<Credentials><AccessKeyId>ASIAASSUMED</AccessKeyId>\
             <SecretAccessKey>s</SecretAccessKey><SessionToken>t</SessionToken>\
             <Expiration>2099-01-01T00:00:00Z</Expiration></Credentials>",
        )]);

        let provider = credentials_provider_in(
            &TokioClock::new(),
            transport.clone(),
            Arc::new(|| 1_369_353_600_000),
            &config,
            &bare(),
        )
        .expect("valid configuration");

        let credentials = provider.credentials().await.expect("assumed");
        assert_eq!(credentials.access_key_id, "ASIAASSUMED");
        assert_eq!(transport.requests().len(), 1);
    }

    #[tokio::test]
    async fn assume_role_layers_over_the_instance_profile() {
        let mut config = storage_config(S3CredentialSource::InstanceProfile);
        config.assume_role = Some(AssumeRoleConfig::new(
            "arn:aws:iam::123456789012:role/orbita",
            "orbita-node-1",
        ));
        let transport = ScriptedTransport::new(vec![
            ok("the-imds-token"),
            ok("orbita-node-role"),
            ok(
                r#"{"AccessKeyId":"ASIAPROFILE","SecretAccessKey":"s","Token":"t",
                   "Expiration":"2099-01-01T00:00:00Z"}"#,
            ),
            ok("<Credentials><AccessKeyId>ASIAASSUMED</AccessKeyId>\
                <SecretAccessKey>s</SecretAccessKey><SessionToken>t</SessionToken>\
                <Expiration>2099-01-01T00:00:00Z</Expiration></Credentials>"),
        ]);

        let provider = credentials_provider_in(
            &TokioClock::new(),
            transport.clone(),
            Arc::new(|| 1_369_353_600_000),
            &config,
            &bare(),
        )
        .expect("valid configuration");

        let credentials = provider.credentials().await.expect("assumed");
        assert_eq!(
            credentials.access_key_id, "ASIAASSUMED",
            "the assumed session, not the instance profile, is what signs S3 requests"
        );

        let requests = transport.requests();
        let assume = requests.last().expect("an AssumeRole call was made");
        assert!(
            assume
                .header("authorization")
                .expect("signed")
                .contains("Credential=ASIAPROFILE/"),
            "the instance profile credential must sign the exchange: {assume:?}"
        );
    }

    #[tokio::test]
    async fn assume_role_layers_over_an_irsa_session() {
        // The cross-account IRSA shape: the pod's workload role is only
        // allowed to assume the role that carries the bucket policy.
        let token = std::env::temp_dir().join(format!(
            "orbita-irsa-assume-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::write(&token, "projected").expect("writing a temp file");

        let mut config = storage_config(S3CredentialSource::Default);
        config.assume_role = Some(AssumeRoleConfig::new(
            "arn:aws:iam::999999999999:role/orbita-storage",
            "orbita-node-1",
        ));
        let env = AwsEnvironment::from_pairs([
            (
                web_identity::TOKEN_FILE_VARIABLE,
                token.display().to_string(),
            ),
            (
                web_identity::ROLE_ARN_VARIABLE,
                "arn:aws:iam::123456789012:role/orbita-workload".to_string(),
            ),
        ]);
        let transport = ScriptedTransport::new(vec![
            ok("<Credentials><AccessKeyId>ASIAIRSA</AccessKeyId>\
                <SecretAccessKey>s</SecretAccessKey><SessionToken>t</SessionToken>\
                <Expiration>2099-01-01T00:00:00Z</Expiration></Credentials>"),
            ok("<Credentials><AccessKeyId>ASIAASSUMED</AccessKeyId>\
                <SecretAccessKey>s</SecretAccessKey><SessionToken>t</SessionToken>\
                <Expiration>2099-01-01T00:00:00Z</Expiration></Credentials>"),
        ]);

        let provider = credentials_provider_in(
            &TokioClock::new(),
            transport.clone(),
            Arc::new(|| 1_369_353_600_000),
            &config,
            &env,
        )
        .expect("valid configuration");

        assert_eq!(
            provider.credentials().await.expect("assumed").access_key_id,
            "ASIAASSUMED"
        );
        let requests = transport.requests();
        assert!(
            requests[0].header("authorization").is_none(),
            "the web identity exchange is unsigned"
        );
        assert!(
            requests[1]
                .header("authorization")
                .expect("signed")
                .contains("Credential=ASIAIRSA/"),
            "the IRSA session must sign the exchange: {:?}",
            requests[1]
        );
        let _ = std::fs::remove_file(&token);
    }

    #[tokio::test]
    async fn the_instance_profile_alone_needs_no_secret_and_no_role() {
        let transport = ScriptedTransport::new(vec![
            ok("the-imds-token"),
            ok("orbita-node-role"),
            ok(
                r#"{"AccessKeyId":"ASIAPROFILE","SecretAccessKey":"s","Token":"t",
                   "Expiration":"2099-01-01T00:00:00Z"}"#,
            ),
        ]);
        let provider = credentials_provider_in(
            &TokioClock::new(),
            transport,
            Arc::new(|| 0),
            &storage_config(S3CredentialSource::InstanceProfile),
            &bare(),
        )
        .expect("valid configuration");

        assert_eq!(
            provider.credentials().await.expect("sourced").access_key_id,
            "ASIAPROFILE"
        );
    }

    #[tokio::test]
    async fn the_sts_endpoint_override_reaches_the_assume_role_call() {
        let mut config = storage_config(static_keys());
        config.sts_endpoint = Some("https://sts.internal.example.com".to_string());
        config.assume_role = Some(AssumeRoleConfig::new(
            "arn:aws:iam::123456789012:role/orbita",
            "orbita-node-1",
        ));
        let transport = ScriptedTransport::new(vec![ok(
            "<Credentials><AccessKeyId>ASIAASSUMED</AccessKeyId>\
             <SecretAccessKey>s</SecretAccessKey><SessionToken>t</SessionToken>\
             <Expiration>2099-01-01T00:00:00Z</Expiration></Credentials>",
        )]);

        credentials_provider_in(
            &TokioClock::new(),
            transport.clone(),
            Arc::new(|| 1_369_353_600_000),
            &config,
            &bare(),
        )
        .expect("valid configuration")
        .credentials()
        .await
        .expect("assumed");

        assert_eq!(
            transport.requests()[0].authority,
            "sts.internal.example.com"
        );
    }
}
