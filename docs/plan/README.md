# Work briefs

These briefs exist so that several people or agents can build Orbita at the
same time without negotiating over shared code. Each one describes a crate:
what it owns, what it must not touch, the interface other crates will call, and
the tests that decide whether it is done.

Read `docs/REQUIREMENTS.md` first. The briefs assume it. Then read
`docs/adr/`, which records the decisions that were argued about and the
reasoning behind them. Where an ADR and a brief disagree, the ADR is newer and
wins, and the brief should be fixed.

## The rule that makes this work

**Contract crates have one owner.** `orbita-core`, `orbita-runtime`,
`orbita-objectstore`, and `orbita-proto` define the vocabulary and the seams
everything else compiles against. If you are working from a brief and you find
yourself wanting to change one of them, stop and raise it rather than changing
it. Two agents independently improving a shared trait is the failure mode this
whole structure exists to prevent, and it is not detectable until the merge.

Everything else is fair game inside your own crate. Add dependencies, restructure
modules, pick your own internal abstractions.

## Order

Phase 0 is done: the workspace, the contract crates, the protocol definitions,
and CI all exist and pass. The briefs below can start now.

The first wave is briefs 01, 02, and 05. Storage and WAL are independent of each
other, and the simulator is in the first wave despite nothing depending on it
yet, because every other crate's interesting tests need it and retrofitting
determinism does not work.

The second wave is briefs 03 and 04, which need the simulator to test against.
Brief 06 can start any time and is mostly independent.

| Brief | Crate | Depends on | Risk |
|---|---|---|---|
| [01](01-storage.md) | `orbita-storage` | contracts only | low |
| [02](02-wal.md) | `orbita-wal` | contracts only | medium |
| [03](03-control.md) | `orbita-control` | 05 for tests | high |
| [04](04-server.md) | `orbita-server` | 01, 02, 03 | medium |
| [05](05-sim.md) | `orbita-sim` | contracts only | high |
| [06](06-ops.md) | `orbita-cli`, packaging | 04 for a real binary | low |

Partition merge, in brief 03, is the single riskiest requirement in v1. It is
the last thing that should be built and the first thing that should be cut if
the schedule slips.

## What every brief expects

- `cargo fmt`, `cargo clippy --all-targets` with no warnings, and `cargo test`
  all pass before you call something done.
- Tests state the behaviour they protect in their name. `expiry_is_inclusive_of_the_deadline`
  tells the next reader what breaks; `test_ttl_2` does not.
- Public items carry a doc comment explaining why they exist, not what they do.
  The signature already says what they do.
- No `unsafe`. Every crate sets `#![forbid(unsafe_code)]`; leave it that way.
- Nothing calls `SystemTime::now()`, `tokio::spawn`, or a random number
  generator directly. Go through `orbita_runtime::Runtime`, or the simulation
  cannot reproduce a failure and the product's main claim quietly stops being
  true.
