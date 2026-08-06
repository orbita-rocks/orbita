//! The data commands, wrapping the `Kv` gRPC service.
//!
//! These exist for scripting and for debugging, not as the way an application
//! talks to Orbita. An application should use generated stubs directly, which
//! is the whole point of keeping the protocol small enough that it can.
//!
//! Two results here are answers rather than failures, and both get their own
//! exit code: a `get` that found nothing, and a conditional write that was not
//! applied. Both are the expected outcome of a lock or an election, and a
//! script should be able to branch on them without parsing anything.

use std::io::Read as _;

use anyhow::{Context, Result};
use orbita_proto::v1::condition::Kind;
use orbita_proto::v1::kv_client::KvClient;
use orbita_proto::v1::{Condition, DeleteRequest, GetRequest, ListRequest, SetRequest};
use tonic::transport::Channel;

use crate::cli::{
    DeleteArgs, GetArgs, ListArgs, Outcome, SetArgs, EXIT_CONDITION_NOT_MET, EXIT_NOT_FOUND,
};
use crate::client::{authed, channel};
use crate::config::Config;
use crate::output::{render, Blob, DeleteView, Format, GetView, ListEntryView, ListView, SetView};

/// Opens a data client against the configured endpoint.
///
/// The decode and encode limits match the ceiling the server advertises and
/// sizes its own transport to, so a maximum list page or a value at the
/// largest keyspace cap is not refused inside this client's gRPC stack with an
/// error about message size and nothing about Orbita.
pub fn connect(config: &Config) -> Result<KvClient<Channel>> {
    let limit = orbita_server::max_transport_message_bytes();
    Ok(KvClient::new(channel(config)?)
        .max_decoding_message_size(limit)
        .max_encoding_message_size(limit))
}

/// Reads one key.
pub async fn get(config: &Config, format: Format, args: GetArgs) -> Result<Outcome> {
    let mut client = connect(config)?;
    let request = authed(
        config,
        GetRequest {
            keyspace: args.keyspace,
            key: args.key.clone().into_bytes(),
        },
    )?;
    let response = client.get(request).await?.into_inner();

    let view = GetView {
        found: response.found,
        key: Blob::new(args.key.as_bytes()),
        value: response.found.then(|| Blob::new(&response.value)),
        version: response.found.then_some(response.version),
        expires_at_millis: response.expires_at_millis,
    };
    let text = render(format, &view)?;
    Ok(if response.found {
        Outcome::ok(text)
    } else {
        Outcome::with_code(text, EXIT_NOT_FOUND)
    })
}

/// Writes one key.
pub async fn set(config: &Config, format: Format, args: SetArgs) -> Result<Outcome> {
    let value = read_value(&args)?;
    let mut client = connect(config)?;
    let request = authed(
        config,
        SetRequest {
            keyspace: args.keyspace,
            key: args.key.into_bytes(),
            value,
            ttl_millis: args.ttl,
            condition: condition(args.if_not_present, args.if_version),
        },
    )?;
    let response = client.set(request).await?.into_inner();

    let view = SetView {
        applied: response.applied,
        version: response.version,
        current_version: response.current_version,
    };
    let text = render(format, &view)?;
    Ok(if response.applied {
        Outcome::ok(text)
    } else {
        Outcome::with_code(text, EXIT_CONDITION_NOT_MET)
    })
}

/// Removes one key.
pub async fn delete(config: &Config, format: Format, args: DeleteArgs) -> Result<Outcome> {
    let mut client = connect(config)?;
    let request = authed(
        config,
        DeleteRequest {
            keyspace: args.keyspace,
            key: args.key.into_bytes(),
            condition: condition(false, args.if_version),
        },
    )?;
    let response = client.delete(request).await?.into_inner();

    let view = DeleteView {
        applied: response.applied,
        existed: response.existed,
        current_version: response.current_version,
    };
    let text = render(format, &view)?;
    Ok(if response.applied {
        Outcome::ok(text)
    } else {
        Outcome::with_code(text, EXIT_CONDITION_NOT_MET)
    })
}

/// Lists one page of keys under a prefix.
pub async fn list(config: &Config, format: Format, args: ListArgs) -> Result<Outcome> {
    let mut client = connect(config)?;
    let request = authed(
        config,
        ListRequest {
            keyspace: args.keyspace,
            prefix: args.prefix.into_bytes(),
            cursor: args.cursor.map(String::into_bytes).unwrap_or_default(),
            limit: args.limit,
            include_values: args.values,
        },
    )?;
    let response = client.list(request).await?.into_inner();

    let view = ListView {
        entries: response
            .entries
            .iter()
            .map(|entry| ListEntryView {
                key: Blob::new(&entry.key),
                value: args.values.then(|| Blob::new(&entry.value)),
                version: entry.version,
                expires_at_millis: entry.expires_at_millis,
            })
            .collect(),
        next_cursor: if response.next_cursor.is_empty() {
            None
        } else {
            Some(Blob::new(&response.next_cursor))
        },
    };
    Ok(Outcome::ok(render(format, &view)?))
}

/// Builds the precondition a write carries, if any.
///
/// The two conditions are mutually exclusive at the argument parser, so this
/// only has to decide which one was asked for.
fn condition(if_not_present: bool, if_version: Option<u64>) -> Option<Condition> {
    if if_not_present {
        return Some(Condition {
            kind: Some(Kind::IfNotPresent(true)),
        });
    }
    if_version.map(|version| Condition {
        kind: Some(Kind::IfVersion(version)),
    })
}

/// Resolves where the value comes from.
///
/// A value can be any bytes, and plenty of them do not survive a shell, so a
/// file and standard input are first class rather than an afterthought.
fn read_value(args: &SetArgs) -> Result<Vec<u8>> {
    if let Some(path) = &args.value_file {
        return std::fs::read(path)
            .with_context(|| format!("cannot read the value from {}", path.display()));
    }
    if let Some(value) = &args.value {
        return Ok(value.clone().into_bytes());
    }
    let mut buffer = Vec::new();
    std::io::stdin()
        .read_to_end(&mut buffer)
        .context("cannot read the value from standard input")?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn set_args() -> SetArgs {
        SetArgs {
            keyspace: "demo".to_owned(),
            key: "k".to_owned(),
            value: None,
            value_file: None,
            ttl: None,
            if_not_present: false,
            if_version: None,
        }
    }

    #[test]
    fn no_condition_flags_means_an_unconditional_write() {
        assert!(condition(false, None).is_none());
    }

    #[test]
    fn if_not_present_becomes_the_absence_condition() {
        assert_eq!(
            condition(true, None).unwrap().kind,
            Some(Kind::IfNotPresent(true))
        );
    }

    #[test]
    fn if_version_becomes_a_compare_and_swap_on_that_version() {
        assert_eq!(
            condition(false, Some(9)).unwrap().kind,
            Some(Kind::IfVersion(9))
        );
    }

    #[test]
    fn a_value_file_wins_over_a_value_argument() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("value");
        std::fs::write(&path, b"from the file").unwrap();
        let args = SetArgs {
            value: Some("from the argument".to_owned()),
            value_file: Some(path),
            ..set_args()
        };
        assert_eq!(read_value(&args).unwrap(), b"from the file");
    }

    #[test]
    fn a_missing_value_file_names_the_path_that_was_not_there() {
        let args = SetArgs {
            value_file: Some(PathBuf::from("/nowhere/value")),
            ..set_args()
        };
        let err = read_value(&args).unwrap_err();
        assert!(format!("{err:#}").contains("/nowhere/value"), "{err:#}");
    }

    #[test]
    fn a_value_given_as_an_argument_is_used_verbatim() {
        let args = SetArgs {
            value: Some("hello".to_owned()),
            ..set_args()
        };
        assert_eq!(read_value(&args).unwrap(), b"hello");
    }
}
