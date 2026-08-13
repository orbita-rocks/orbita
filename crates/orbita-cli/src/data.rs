//! The data commands, wrapping the `Kv` gRPC service.
//!
//! These exist for scripting and for debugging, not as the way an application
//! talks to Orbita. An application should use generated stubs directly, which
//! is the whole point of keeping the protocol small enough that it can.
//!
//! Two results here are answers rather than failures, and both get their own
//! exit code: a `get` that found nothing, and a conditional write that was not
//! applied. Both are the expected outcome of a lock or an election, and a
//! script should be able to branch on them without parsing anything. The REPL
//! has no exit code to carry them in and prints a note instead; the note is
//! derived from the same [`Outcome::code`] a script reads, so the two can never
//! disagree. See [`crate::repl`].

use std::io::Read as _;
use std::path::Path;

use anyhow::{bail, Context, Result};
use orbita_proto::v1::condition::Kind;
use orbita_proto::v1::kv_client::KvClient;
use orbita_proto::v1::{Condition, DeleteRequest, GetRequest, ListRequest, SetRequest};
use tonic::transport::Channel;

use crate::cli::{
    DeleteArgs, GetArgs, ListArgs, Outcome, SetArgs, EXIT_CONDITION_NOT_MET, EXIT_NOT_FOUND,
};
use crate::client::authed;
use crate::output::{render, Blob, DeleteView, Format, GetView, ListEntryView, ListView, SetView};
use crate::session::Session;

/// Opens a data client over the session's channel.
///
/// The channel is the session's, cloned rather than dialed anew, so a REPL
/// holds one connection across every line instead of opening one per command.
/// The decode and encode limits match the ceiling the server advertises and
/// sizes its own transport to, so a maximum list page or a value at the largest
/// keyspace cap is not refused inside this client's gRPC stack with an error
/// about message size and nothing about Orbita.
pub fn connect(channel: Channel) -> KvClient<Channel> {
    let limit = orbita_server::max_transport_message_bytes();
    KvClient::new(channel)
        .max_decoding_message_size(limit)
        .max_encoding_message_size(limit)
}

/// Reads one key.
pub async fn get(session: &Session, format: Format, args: GetArgs) -> Result<Outcome> {
    let (keyspace, key) = resolve_keyed(
        args.keyspace_flag,
        session.default_keyspace(),
        args.keyspace,
        args.key,
        "get",
    )?;
    let mut client = connect(session.channel.clone());
    let request = authed(
        &session.config,
        GetRequest {
            keyspace,
            key: key.clone().into_bytes(),
        },
    )?;
    let response = client.get(request).await?.into_inner();

    let view = GetView {
        found: response.found,
        key: Blob::new(key.as_bytes()),
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
pub async fn set(session: &Session, format: Format, args: SetArgs) -> Result<Outcome> {
    let (keyspace, key, inline_value) = resolve_set(
        args.keyspace_flag,
        session.default_keyspace(),
        args.keyspace,
        args.key,
        args.value,
    )?;
    let value = read_value(
        inline_value,
        args.value_file.as_deref(),
        !session.interactive,
    )?;
    let mut client = connect(session.channel.clone());
    let request = authed(
        &session.config,
        SetRequest {
            keyspace,
            key: key.into_bytes(),
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
pub async fn delete(session: &Session, format: Format, args: DeleteArgs) -> Result<Outcome> {
    let (keyspace, key) = resolve_keyed(
        args.keyspace_flag,
        session.default_keyspace(),
        args.keyspace,
        args.key,
        "delete",
    )?;
    let mut client = connect(session.channel.clone());
    let request = authed(
        &session.config,
        DeleteRequest {
            keyspace,
            key: key.into_bytes(),
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
pub async fn list(session: &Session, format: Format, args: ListArgs) -> Result<Outcome> {
    let (keyspace, prefix) = resolve_list(
        args.keyspace_flag,
        session.default_keyspace(),
        args.keyspace,
        args.prefix,
    )?;
    let mut client = connect(session.channel.clone());
    let request = authed(
        &session.config,
        ListRequest {
            keyspace,
            prefix: prefix.into_bytes(),
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

/// The message a data command prints when it cannot find a keyspace to run
/// against, naming every place one can come from.
fn no_keyspace(verb: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "{verb} needs a keyspace. Give one as the first argument, set ORBITA_KEYSPACE or \
         client.keyspace, or run `:use <keyspace>` in the REPL"
    )
}

/// Resolves the keyspace and key for `get` and `delete`.
///
/// `flag` is `--keyspace`, the unambiguous form: when it is set the positionals
/// are only the key, and it wins over both a leading positional and any default.
/// Otherwise the two-argument form always names the keyspace first, so
/// `get demo key` means the same thing whether or not a default is set — a
/// script never changes meaning because the environment changed — and the
/// default only fills a keyspace that was not typed, which is what makes the
/// one-argument `get key` work.
fn resolve_keyed(
    flag: Option<String>,
    default: Option<&str>,
    keyspace: Option<String>,
    key: Option<String>,
    verb: &str,
) -> Result<(String, String)> {
    if let Some(flag) = flag {
        // With an explicit keyspace, the sole positional is the key, so there
        // is nothing left to misread. A second positional would mean the caller
        // named the keyspace twice.
        return match (keyspace, key) {
            (Some(key), None) => Ok((flag, key)),
            (Some(_), Some(_)) => {
                bail!("with --keyspace, give only the key: `{verb} --keyspace <name> <key>`")
            }
            (None, _) => bail!("{verb} needs a key"),
        };
    }
    match (keyspace, key) {
        (Some(keyspace), Some(key)) => Ok((keyspace, key)),
        (Some(key), None) => match default {
            Some(keyspace) => Ok((keyspace.to_owned(), key)),
            None => Err(no_keyspace(verb)),
        },
        (None, _) => bail!("{verb} needs a key"),
    }
}

/// Resolves the keyspace and prefix for `list`.
///
/// `flag` is `--keyspace`, after which the single positional is unambiguously
/// the prefix. Without it, a single argument is the prefix when a default
/// keyspace exists and the keyspace otherwise, which keeps the old `list demo`
/// (scan a keyspace) working with no default and makes `list users/` (scan a
/// prefix of the current keyspace) work with one.
fn resolve_list(
    flag: Option<String>,
    default: Option<&str>,
    keyspace: Option<String>,
    prefix: Option<String>,
) -> Result<(String, String)> {
    if let Some(flag) = flag {
        return match (keyspace, prefix) {
            (Some(prefix), None) => Ok((flag, prefix)),
            (None, _) => Ok((flag, String::new())),
            (Some(_), Some(_)) => {
                bail!("with --keyspace, give only the prefix: `list --keyspace <name> <prefix>`")
            }
        };
    }
    match (keyspace, prefix) {
        (Some(keyspace), Some(prefix)) => Ok((keyspace, prefix)),
        (Some(one), None) => match default {
            Some(keyspace) => Ok((keyspace.to_owned(), one)),
            None => Ok((one, String::new())),
        },
        (None, _) => match default {
            Some(keyspace) => Ok((keyspace.to_owned(), String::new())),
            None => Err(no_keyspace("list")),
        },
    }
}

/// Resolves the keyspace, key, and inline value for `set`.
///
/// `flag` is `--keyspace`, the unambiguous form the help points at for
/// file- and stdin-backed writes: with it the positionals are only the key and
/// an optional value, so `set --keyspace prod key` writes the piped or filed
/// value to `key` in `prod` and can never be misread as key-and-value.
///
/// Without it, `set` has three positionals and an optional value, so a
/// two-argument form is genuinely ambiguous: `set a b` is keyspace-and-key when
/// no default is set, and key-and-value when one is. That ambiguity is the whole
/// reason `--keyspace` exists, and the reason a stdin- or file-backed write with
/// a default keyspace should use it rather than the positional shorthand.
fn resolve_set(
    flag: Option<String>,
    default: Option<&str>,
    keyspace: Option<String>,
    key: Option<String>,
    value: Option<String>,
) -> Result<(String, String, Option<String>)> {
    if let Some(flag) = flag {
        // With an explicit keyspace the positionals collapse to key and an
        // optional value, and the third slot must be empty.
        return match (keyspace, key, value) {
            (Some(key), value, None) => Ok((flag, key, value)),
            (Some(_), Some(_), Some(_)) => bail!(
                "with --keyspace, give the key and an optional value only: \
                 `set --keyspace <name> <key> [value]`"
            ),
            (None, _, _) => bail!("set needs a key"),
            // Unreachable: clap fills positionals left to right.
            (Some(_), None, Some(_)) => bail!("set needs a key"),
        };
    }
    match (keyspace, key, value) {
        // The full three-argument form names the keyspace first and is never
        // ambiguous, whether or not a default is set.
        (Some(keyspace), Some(key), Some(value)) => Ok((keyspace, key, Some(value))),
        // Two arguments with a default in effect are key and value; the
        // keyspace comes from the default.
        (Some(key), Some(value), None) if default.is_some() => {
            Ok((default.unwrap().to_owned(), key, Some(value)))
        }
        // Two arguments with no default are keyspace and key, and the value
        // falls to a file or standard input, exactly as before this existed.
        (Some(keyspace), Some(key), None) => Ok((keyspace, key, None)),
        // One argument with a default is the key; the value falls to a file or
        // standard input.
        (Some(key), None, None) => match default {
            Some(keyspace) => Ok((keyspace.to_owned(), key, None)),
            None => bail!("set needs a key"),
        },
        (None, _, _) => Err(no_keyspace("set")),
        // Unreachable in practice: clap fills positionals left to right, so a
        // value never arrives without a key. Spelled out rather than a wildcard
        // so a future arg change fails loudly here instead of silently.
        (Some(_), None, Some(_)) => bail!("set needs a key"),
    }
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

/// Resolves where a write's value comes from.
///
/// A value can be any bytes, and plenty of them do not survive a shell, so a
/// file and standard input are first class rather than an afterthought.
/// `stdin_allowed` is false in the REPL: the line editor owns standard input
/// there, so the fallback is refused with a message that says why rather than
/// blocking on a terminal the operator is typing commands into.
fn read_value(
    inline: Option<String>,
    value_file: Option<&Path>,
    stdin_allowed: bool,
) -> Result<Vec<u8>> {
    if let (Some(_), Some(path)) = (&inline, value_file) {
        bail!(
            "give the value as an argument or with --value-file at {}, not both",
            path.display()
        );
    }
    if let Some(path) = value_file {
        return std::fs::read(path)
            .with_context(|| format!("cannot read the value from {}", path.display()));
    }
    if let Some(value) = inline {
        return Ok(value.into_bytes());
    }
    if !stdin_allowed {
        bail!(
            "a value is required. Pass it as an argument or with --value-file; standard input is \
             not read in the REPL, where the line editor owns it"
        );
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
    fn a_two_argument_get_names_the_keyspace_first_regardless_of_any_default() {
        // The property that keeps a script's meaning stable: a default in the
        // environment must not turn `get demo key` into a read of some other
        // keyspace.
        let with = resolve_keyed(
            None,
            Some("current"),
            Some("demo".to_owned()),
            Some("key".to_owned()),
            "get",
        )
        .unwrap();
        let without = resolve_keyed(
            None,
            None,
            Some("demo".to_owned()),
            Some("key".to_owned()),
            "get",
        )
        .unwrap();
        assert_eq!(with, ("demo".to_owned(), "key".to_owned()));
        assert_eq!(with, without);
    }

    #[test]
    fn a_one_argument_get_uses_the_default_keyspace() {
        let (keyspace, key) =
            resolve_keyed(None, Some("current"), Some("key".to_owned()), None, "get").unwrap();
        assert_eq!((keyspace.as_str(), key.as_str()), ("current", "key"));
    }

    #[test]
    fn a_one_argument_get_without_a_default_is_refused_for_want_of_a_keyspace() {
        let err = resolve_keyed(None, None, Some("key".to_owned()), None, "get").unwrap_err();
        assert!(format!("{err:#}").contains("needs a keyspace"), "{err:#}");
    }

    #[test]
    fn an_explicit_keyspace_flag_reads_the_lone_positional_as_the_key() {
        // The unambiguous form: `get --keyspace demo key` puts the keyspace in
        // the flag and the key in the one positional, so a default can never
        // change which key is read.
        let (keyspace, key) = resolve_keyed(
            Some("prod".to_owned()),
            Some("current"),
            Some("key".to_owned()),
            None,
            "get",
        )
        .unwrap();
        assert_eq!((keyspace.as_str(), key.as_str()), ("prod", "key"));
        // Naming the keyspace twice — flag plus a second positional — is a
        // mistake, not a silent reinterpretation.
        assert!(resolve_keyed(
            Some("prod".to_owned()),
            None,
            Some("demo".to_owned()),
            Some("key".to_owned()),
            "get",
        )
        .is_err());
    }

    #[test]
    fn list_reads_a_lone_argument_as_prefix_with_a_default_and_keyspace_without() {
        assert_eq!(
            resolve_list(None, Some("current"), Some("users/".to_owned()), None).unwrap(),
            ("current".to_owned(), "users/".to_owned())
        );
        assert_eq!(
            resolve_list(None, None, Some("demo".to_owned()), None).unwrap(),
            ("demo".to_owned(), String::new())
        );
        // With the flag the positional is unambiguously the prefix.
        assert_eq!(
            resolve_list(
                Some("prod".to_owned()),
                None,
                Some("users/".to_owned()),
                None
            )
            .unwrap(),
            ("prod".to_owned(), "users/".to_owned())
        );
    }

    #[test]
    fn a_full_set_names_the_keyspace_first_whether_or_not_a_default_exists() {
        let args = (
            Some("demo".to_owned()),
            Some("key".to_owned()),
            Some("value".to_owned()),
        );
        assert_eq!(
            resolve_set(
                None,
                Some("current"),
                args.0.clone(),
                args.1.clone(),
                args.2.clone()
            )
            .unwrap(),
            (
                "demo".to_owned(),
                "key".to_owned(),
                Some("value".to_owned())
            )
        );
        assert_eq!(
            resolve_set(None, None, args.0, args.1, args.2).unwrap(),
            (
                "demo".to_owned(),
                "key".to_owned(),
                Some("value".to_owned())
            )
        );
    }

    #[test]
    fn a_two_argument_set_is_key_and_value_only_when_a_default_is_in_effect() {
        assert_eq!(
            resolve_set(
                None,
                Some("current"),
                Some("key".to_owned()),
                Some("value".to_owned()),
                None
            )
            .unwrap(),
            (
                "current".to_owned(),
                "key".to_owned(),
                Some("value".to_owned())
            )
        );
        // With no default the same two arguments are keyspace and key, and the
        // value falls to stdin, exactly as before this feature existed.
        assert_eq!(
            resolve_set(
                None,
                None,
                Some("demo".to_owned()),
                Some("key".to_owned()),
                None
            )
            .unwrap(),
            ("demo".to_owned(), "key".to_owned(), None)
        );
    }

    #[test]
    fn an_explicit_keyspace_keeps_a_stdin_or_file_write_pointed_at_the_right_place() {
        // The P1 the review caught: with a default keyspace, the positional
        // shorthand turned `set other key` (value from stdin) into a write of
        // literal "key". The explicit form is proof against that — the keyspace
        // is the flag, the key is the sole positional, and the value stays
        // absent so it comes from stdin or --value-file.
        let (keyspace, key, value) = resolve_set(
            Some("other".to_owned()),
            Some("current"),
            Some("key".to_owned()),
            None,
            None,
        )
        .unwrap();
        assert_eq!(keyspace, "other");
        assert_eq!(key, "key");
        assert_eq!(value, None, "the value is left to stdin or --value-file");

        // An explicit value is still allowed alongside the flag.
        let (keyspace, key, value) = resolve_set(
            Some("other".to_owned()),
            Some("current"),
            Some("key".to_owned()),
            Some("value".to_owned()),
            None,
        )
        .unwrap();
        assert_eq!((keyspace.as_str(), key.as_str()), ("other", "key"));
        assert_eq!(value.as_deref(), Some("value"));
    }

    #[test]
    fn a_value_file_wins_over_stdin_and_reads_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("value");
        std::fs::write(&path, b"from the file").unwrap();
        assert_eq!(
            read_value(None, Some(&path), true).unwrap(),
            b"from the file"
        );
    }

    #[test]
    fn an_inline_value_and_a_file_together_are_refused_rather_than_guessed() {
        let err = read_value(
            Some("inline".to_owned()),
            Some(&PathBuf::from("/tmp/value")),
            true,
        )
        .unwrap_err();
        assert!(format!("{err:#}").contains("not both"), "{err:#}");
    }

    #[test]
    fn a_missing_value_file_names_the_path_that_was_not_there() {
        let err = read_value(None, Some(&PathBuf::from("/nowhere/value")), true).unwrap_err();
        assert!(format!("{err:#}").contains("/nowhere/value"), "{err:#}");
    }

    #[test]
    fn a_value_given_as_an_argument_is_used_verbatim() {
        assert_eq!(
            read_value(Some("hello".to_owned()), None, true).unwrap(),
            b"hello"
        );
    }

    #[test]
    fn a_repl_write_without_a_value_is_refused_and_says_why() {
        // The trap: standard input belongs to the line editor in a REPL, so the
        // stdin fallback has to be disabled there and the failure has to explain
        // itself rather than block on a terminal.
        let err = read_value(None, None, false).unwrap_err();
        let message = format!("{err:#}");
        assert!(
            message.contains("standard input is not read in the REPL"),
            "{message}"
        );
        assert!(message.contains("--value-file"), "{message}");
    }
}
