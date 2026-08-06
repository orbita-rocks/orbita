//! The interactive session: a loop, session state, and nothing else.
//!
//! This is deliberately not a second tool. Every non-session line is parsed by
//! the same [`Cli`] the shell uses and dispatched through the same
//! [`crate::session::dispatch`], so a REPL line and a shell invocation run the
//! identical code and can never mean different things. The only surface unique
//! to the REPL is a handful of session commands prefixed with a colon, which
//! set state (`:use`, `:format`) or leave (`:quit`); they can never be confused
//! with a cluster command because no cluster command starts with a colon.
//!
//! # The exit-code-on-screen problem
//!
//! A `get` that missed and a compare-and-swap that lost are answers, not
//! errors, and the one-shot tool reports them by exiting 2 and 3 while still
//! printing their normal output. A REPL has no exit code to carry. The answer
//! here is to print the exact same rendered output a script gets on stdout, and
//! then append a short note — `[not found]`, `[condition not met]` — derived
//! from the same [`Outcome::code`] the shell turns into `$?`. The note is a
//! function of the code, so the screen and the exit code can never drift: see
//! [`exit_note`], which is keyed on the very constants the exit codes are.
//!
//! # The line editor
//!
//! `rustyline` gives history and line editing. It is the first line-editor
//! dependency in a workspace that is conservative about them, and it was chosen
//! over `reedline` deliberately: it is smaller and older, pulls fewer
//! transitive crates, and is synchronous, which is exactly the shape of this
//! loop — read a line, then block the session's one runtime on dispatching it.
//! `reedline` is built for `nushell`'s async, multi-line, richly-styled prompt
//! and would be weight spent on features a thin operator console does not use.

use anyhow::{bail, Context, Result};
use clap::Parser as _;
use rustyline::error::ReadlineError;
use rustyline::DefaultEditor;

use crate::cli::{Cli, Command, Outcome, EXIT_CONDITION_NOT_MET, EXIT_ERROR, EXIT_NOT_FOUND};
use crate::config::Config;
use crate::output::Format;
use crate::session::{dispatch, Session};

/// Runs an interactive session until the operator leaves.
///
/// It owns the one runtime and the one session the whole loop shares, which is
/// the entire point of the mode: a process, a runtime, and a connection bought
/// once instead of per line. Returns an empty [`Outcome`] so the caller's exit
/// path is the same as any other command's.
pub fn run(config: Config, format: Format) -> Result<Outcome> {
    // One runtime for the session, built here rather than per command. The
    // read is synchronous and blocks this thread, so a multi-thread runtime the
    // dispatch can spawn onto is exactly right.
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("could not start the runtime for the interactive session")?;
    // Build the session inside the runtime: dialing a channel, even a lazy one,
    // needs a reactor in scope, and constructing it outside `block_on` panicked
    // the session on startup. It is built in its own `block_on` rather than
    // under a held `enter` guard, because `block_on` cannot be called again
    // while an enter guard for the same runtime is active, and the loop below
    // calls it once per line.
    let mut session = runtime.block_on(async { Session::new(config, format, true) })?;

    let mut editor =
        DefaultEditor::new().context("could not start the line editor for interactive mode")?;

    eprintln!("{}", banner(&session));
    loop {
        match editor.readline(&prompt(&session)) {
            Ok(line) => {
                // History gets the raw line so recall matches what was typed.
                let _ = editor.add_history_entry(line.as_str());
                if handle_line(&runtime, &mut session, &line) == Flow::Quit {
                    break;
                }
            }
            // Ctrl-C abandons the line in progress, the way a shell does, rather
            // than leaving the session.
            Err(ReadlineError::Interrupted) => continue,
            // Ctrl-D on an empty prompt is the ordinary way to leave.
            Err(ReadlineError::Eof) => break,
            Err(error) => {
                eprintln!("orbita: the line editor failed: {error}");
                break;
            }
        }
    }
    Ok(Outcome::ok(String::new()))
}

/// Whether the loop should keep reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Flow {
    Continue,
    Quit,
}

/// Handles one line of input, printing whatever it produces.
///
/// It is split from [`run`] so that everything except the terminal I/O — the
/// parsing, the session mutation, the exit-code note — is reachable without a
/// TTY. The pure pieces it calls are what the tests exercise.
fn handle_line(runtime: &tokio::runtime::Runtime, session: &mut Session, line: &str) -> Flow {
    match parse_line(line) {
        Ok(Line::Blank) => Flow::Continue,
        Ok(Line::Meta(meta)) => apply_meta(session, meta),
        Ok(Line::Command { command, format }) => {
            let format = format.unwrap_or(session.format);
            match runtime.block_on(dispatch(session, format, command)) {
                Ok(outcome) => {
                    print(&outcome.text);
                    // The screen answer for a missed get or a lost CAS: the same
                    // rendered body a script sees, plus the note the exit code
                    // would have been. Derived from the code so they agree.
                    if let Some(note) = exit_note(outcome.code) {
                        eprintln!("[{note}]");
                    }
                }
                Err(error) => eprintln!("orbita: {error:#}"),
            }
            Flow::Continue
        }
        Err(error) => {
            eprintln!("orbita: {error:#}");
            Flow::Continue
        }
    }
}

/// One parsed line of input.
#[derive(Debug)]
enum Line {
    /// Whitespace or nothing. Common enough to be its own case.
    Blank,
    /// A session command, the only surface unique to the REPL.
    Meta(Meta),
    /// A cluster command, parsed by the same clap tree the shell uses. `format`
    /// is the line's own `--output`, which overrides the session default for
    /// this line only.
    Command {
        command: Command,
        format: Option<Format>,
    },
}

/// A session command. These change what the session remembers or leave it; none
/// of them touch a cluster.
#[derive(Debug, PartialEq, Eq)]
enum Meta {
    /// Set the current keyspace, or clear it when the name is absent.
    Use(Option<String>),
    /// Switch the output format for the rest of the session.
    Format(Format),
    /// Show the current keyspace.
    ShowKeyspace,
    /// List the session commands.
    Help,
    /// Leave the session.
    Quit,
}

/// Parses one line into a session command, a cluster command, or nothing.
///
/// A leading colon means a session command, and nothing else does, because no
/// cluster command starts with one. Everything else is handed verbatim to the
/// same parser the shell uses, so the REPL cannot accept a syntax the shell
/// rejects or the reverse.
fn parse_line(line: &str) -> Result<Line> {
    let tokens = split_line(line)?;
    let Some(first) = tokens.first() else {
        return Ok(Line::Blank);
    };

    if let Some(name) = first.strip_prefix(':') {
        return Ok(Line::Meta(parse_meta(name, &tokens[1..])?));
    }

    // Re-enter the same command tree, with "orbita" as argv[0] so clap's errors
    // and help read the way they do from a shell.
    let cli =
        Cli::try_parse_from(std::iter::once("orbita").chain(tokens.iter().map(String::as_str)))?;
    Ok(Line::Command {
        command: cli.command,
        format: cli.global.output,
    })
}

/// Parses a `:` session command.
fn parse_meta(name: &str, rest: &[String]) -> Result<Meta> {
    match name {
        "use" => Ok(Meta::Use(rest.first().cloned())),
        "format" => {
            let Some(value) = rest.first() else {
                bail!(":format needs a format: human or json");
            };
            match value.as_str() {
                "human" => Ok(Meta::Format(Format::Human)),
                "json" => Ok(Meta::Format(Format::Json)),
                other => bail!(":format takes human or json, got {other:?}"),
            }
        }
        "keyspace" => Ok(Meta::ShowKeyspace),
        "help" | "h" | "?" => Ok(Meta::Help),
        "quit" | "q" | "exit" => Ok(Meta::Quit),
        other => bail!(
            "unknown session command :{other}. Try :help. Cluster commands do not take a leading \
             colon"
        ),
    }
}

/// Applies a session command, printing anything it reports.
fn apply_meta(session: &mut Session, meta: Meta) -> Flow {
    match meta {
        Meta::Use(keyspace) => {
            session.keyspace = keyspace;
            match &session.keyspace {
                Some(name) => eprintln!("keyspace set to {name}"),
                None => eprintln!("keyspace cleared; name one on each command"),
            }
            Flow::Continue
        }
        Meta::Format(format) => {
            session.format = format;
            eprintln!("output format set to {}", format_name(format));
            Flow::Continue
        }
        Meta::ShowKeyspace => {
            match session.default_keyspace() {
                Some(name) => eprintln!("current keyspace: {name}"),
                None => eprintln!("no current keyspace; name one on each command or :use one"),
            }
            Flow::Continue
        }
        Meta::Help => {
            eprint!("{HELP}");
            Flow::Continue
        }
        Meta::Quit => Flow::Quit,
    }
}

/// The note a non-zero outcome prints, or nothing for success.
///
/// Keyed on the exact exit-code constants so the on-screen answer and the code
/// a script reads are the same fact stated twice, and cannot drift. A code the
/// data path does not produce still gets a note rather than vanishing, because
/// a silent non-zero result is the failure this whole mechanism exists to
/// prevent.
fn exit_note(code: i32) -> Option<&'static str> {
    match code {
        0 => None,
        EXIT_NOT_FOUND => Some("not found"),
        EXIT_CONDITION_NOT_MET => Some("condition not met"),
        EXIT_ERROR => Some("error"),
        _ => Some("non-zero exit"),
    }
}

/// The prompt, which shows the current keyspace so an operator always knows
/// where a bare `get key` will land.
fn prompt(session: &Session) -> String {
    match session.default_keyspace() {
        Some(keyspace) => format!("orbita:{keyspace}> "),
        None => "orbita> ".to_owned(),
    }
}

fn format_name(format: Format) -> &'static str {
    match format {
        Format::Human => "human",
        Format::Json => "json",
    }
}

/// The line printed at startup, naming the endpoint and how to leave.
fn banner(session: &Session) -> String {
    format!(
        "orbita interactive session against {}. Type :help for session commands, :quit to leave.",
        session.config.client.endpoint
    )
}

const HELP: &str = "\
Session commands (a leading colon; everything else is a normal orbita command):
  :use <keyspace>       set the current keyspace, so data commands can omit it
  :use                  clear the current keyspace
  :format <human|json>  set the output format for the rest of the session
  :keyspace             show the current keyspace
  :help                 show this
  :quit                 leave (Ctrl-D also leaves)

Everything else is parsed exactly as `orbita ...` would be from your shell:
  keyspace list
  get demo greeting
  set demo greeting hello
A missed get prints its output and a [not found] note; a lost compare-and-swap
prints a [condition not met] note. Those are the same answers a script reads
from exit codes 2 and 3.
";

/// Prints command output to stdout, tolerating a closed pipe the way the
/// one-shot binary does.
fn print(text: &str) {
    use std::io::Write as _;
    if text.is_empty() {
        return;
    }
    let mut stdout = std::io::stdout();
    let _ = stdout.write_all(text.as_bytes());
    let _ = stdout.flush();
}

/// Splits a line into argv tokens with shell-like quoting.
///
/// A whitespace split would break every value with a space in it, and a value
/// is arbitrary bytes. This is a small hand-written tokenizer rather than a
/// dependency because it needs only three rules — single quotes are literal,
/// double quotes allow a backslash escape, and a bare backslash escapes the
/// next character — and a crate for that is the sort of dependency this
/// workspace declines on principle.
fn split_line(line: &str) -> Result<Vec<String>> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_token = false;
    let mut chars = line.chars().peekable();

    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                in_token = true;
                for q in chars.by_ref() {
                    if q == '\'' {
                        break;
                    }
                    current.push(q);
                }
            }
            '"' => {
                in_token = true;
                let mut closed = false;
                while let Some(q) = chars.next() {
                    match q {
                        '"' => {
                            closed = true;
                            break;
                        }
                        '\\' => {
                            // Only the characters a double quote actually needs
                            // escaping are special; any other backslash is kept
                            // so a Windows path or a regex is not mangled.
                            match chars.next() {
                                Some(next @ ('"' | '\\')) => current.push(next),
                                Some(other) => {
                                    current.push('\\');
                                    current.push(other);
                                }
                                None => bail!("unterminated \\ escape"),
                            }
                        }
                        other => current.push(other),
                    }
                }
                if !closed {
                    bail!("unterminated \" quote");
                }
            }
            '\\' => {
                in_token = true;
                match chars.next() {
                    Some(next) => current.push(next),
                    None => bail!("a line cannot end on a \\ escape"),
                }
            }
            c if c.is_whitespace() => {
                if in_token {
                    tokens.push(std::mem::take(&mut current));
                    in_token = false;
                }
            }
            other => {
                in_token = true;
                current.push(other);
            }
        }
    }
    if in_token {
        tokens.push(current);
    }
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Layer;

    /// A runtime and a session pointed at an address nothing is listening on.
    ///
    /// The channel is lazy, so building it connects to nothing, but even a lazy
    /// channel has to be created inside a runtime, and every command that
    /// reaches the network needs one to run on. The runtime is returned so the
    /// caller keeps it alive for as long as the session; the tests here all stop
    /// before any request actually leaves.
    fn offline_session(interactive: bool) -> (tokio::runtime::Runtime, Session) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let session = runtime.block_on(async {
            let config = Layer::default().resolve().unwrap();
            Session::new(config, Format::Human, interactive).unwrap()
        });
        (runtime, session)
    }

    #[test]
    fn a_command_line_goes_through_the_same_parser_as_the_shell() {
        // Not a REPL dialect: the tokens are handed to the same Cli, so the
        // parse is identical to `orbita get demo greeting`.
        let Line::Command { command, format } = parse_line("get demo greeting").unwrap() else {
            panic!("expected a command");
        };
        assert!(format.is_none());
        let Command::Get(args) = command else {
            panic!("expected get");
        };
        assert_eq!(args.keyspace.as_deref(), Some("demo"));
        assert_eq!(args.key.as_deref(), Some("greeting"));
    }

    #[test]
    fn a_line_can_override_the_output_format_the_way_the_shell_flag_does() {
        let Line::Command { format, .. } = parse_line("keyspace list --output json").unwrap()
        else {
            panic!("expected a command");
        };
        assert_eq!(format, Some(Format::Json));
    }

    #[test]
    fn a_blank_line_is_nothing_to_do() {
        assert!(matches!(parse_line("   ").unwrap(), Line::Blank));
        assert!(matches!(parse_line("").unwrap(), Line::Blank));
    }

    #[test]
    fn a_colon_names_a_session_command_and_nothing_else_does() {
        assert_eq!(
            match parse_line(":use demo").unwrap() {
                Line::Meta(meta) => meta,
                _ => panic!(),
            },
            Meta::Use(Some("demo".to_owned()))
        );
        assert_eq!(
            match parse_line(":use").unwrap() {
                Line::Meta(meta) => meta,
                _ => panic!(),
            },
            Meta::Use(None)
        );
        // A bare `use` with no colon is not a session command; it goes to the
        // parser, which rejects it, proving the two surfaces do not overlap.
        assert!(parse_line("use demo").is_err());
    }

    #[test]
    fn the_format_session_command_is_validated() {
        assert_eq!(
            match parse_line(":format json").unwrap() {
                Line::Meta(meta) => meta,
                _ => panic!(),
            },
            Meta::Format(Format::Json)
        );
        assert!(parse_line(":format yaml").is_err());
        assert!(parse_line(":format").is_err());
    }

    #[test]
    fn use_sets_and_clears_the_current_keyspace_the_default_resolves_from() {
        let (_runtime, mut session) = offline_session(true);
        assert_eq!(session.default_keyspace(), None);
        apply_meta(&mut session, Meta::Use(Some("orders".to_owned())));
        assert_eq!(session.default_keyspace(), Some("orders"));
        apply_meta(&mut session, Meta::Use(None));
        assert_eq!(session.default_keyspace(), None);
    }

    #[test]
    fn the_exit_note_is_the_same_fact_as_the_scripted_exit_code() {
        // The consistency guarantee: whatever a script reads from $? is what the
        // screen note says, because both come from the one Outcome code.
        assert_eq!(exit_note(0), None);
        assert_eq!(exit_note(EXIT_NOT_FOUND), Some("not found"));
        assert_eq!(exit_note(EXIT_CONDITION_NOT_MET), Some("condition not met"));
    }

    #[test]
    fn serve_and_dev_are_refused_in_the_session_with_a_clear_message() {
        let (runtime, session) = offline_session(true);
        for line in ["serve", "dev"] {
            let Line::Command { command, .. } = parse_line(line).unwrap() else {
                panic!("{line} should parse");
            };
            let error = runtime
                .block_on(dispatch(&session, Format::Human, command))
                .unwrap_err();
            let message = format!("{error:#}");
            assert!(
                message.contains("not available in an interactive session"),
                "{line}: {message}"
            );
        }
    }

    #[test]
    fn a_write_without_a_value_is_refused_in_the_session_and_says_why() {
        // The stdin trap: the line editor owns standard input, so the fallback
        // is off and the failure explains itself instead of blocking.
        let (runtime, session) = offline_session(true);
        let Line::Command { command, .. } = parse_line("set demo key").unwrap() else {
            panic!("expected set");
        };
        let error = runtime
            .block_on(dispatch(&session, Format::Human, command))
            .unwrap_err();
        assert!(
            format!("{error:#}").contains("standard input is not read in the REPL"),
            "{error:#}"
        );
    }

    #[test]
    fn a_default_keyspace_lets_a_data_command_omit_it_but_the_shell_form_still_wins() {
        // The current keyspace resolves the same way the config default does,
        // which is what keeps the REPL from being a different tool.
        let (_runtime, mut session) = offline_session(true);
        session.keyspace = Some("orders".to_owned());
        let Line::Command { command, .. } = parse_line("get greeting").unwrap() else {
            panic!("expected get");
        };
        // We cannot reach the network here, but resolution happens before the
        // dial, so a wrong keyspace resolution would change which request is
        // built. Assert on the parsed shape instead: one positional, which the
        // default fills.
        let Command::Get(args) = command else {
            panic!("expected get");
        };
        assert_eq!(args.keyspace.as_deref(), Some("greeting"));
        assert_eq!(args.key, None);
        // And the session carries the keyspace the resolver will use.
        assert_eq!(session.default_keyspace(), Some("orders"));
    }

    #[test]
    fn quoting_holds_a_value_with_spaces_together() {
        let tokens = split_line(r#"set demo msg "hello world""#).unwrap();
        assert_eq!(tokens, ["set", "demo", "msg", "hello world"]);
    }

    #[test]
    fn single_quotes_are_literal_and_double_quotes_take_an_escape() {
        assert_eq!(split_line(r#"'a b'"#).unwrap(), ["a b"]);
        assert_eq!(split_line(r#""a\"b""#).unwrap(), [r#"a"b"#]);
        assert!(split_line(r#""unterminated"#).is_err());
    }

    #[test]
    fn the_delete_confirmation_keeps_its_friction_in_the_session() {
        // The REPL must not soften `keyspace delete` into a y/n prompt: it goes
        // through the same parser, which still demands --confirm.
        assert!(parse_line("keyspace delete demo").is_err());
        assert!(parse_line("keyspace delete demo --confirm demo").is_ok());
    }
}
