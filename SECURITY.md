# Security policy

## Reporting a vulnerability

Report vulnerabilities privately through
[GitHub's advisory form](https://github.com/orbita-rocks/orbita/security/advisories/new),
or by email to brad.heller@gmail.com if you would rather not use GitHub.

Please do not open a public issue for a suspected vulnerability. Orbita holds
the state that other systems coordinate on, so a disclosed flaw is useful to an
attacker before there is a fix for it.

Tell us what you can reproduce, what version or commit you saw it on, and what
an attacker gets out of it. A seed that reproduces it under the simulation is
the most useful thing you can send.

You should hear back within three business days. We will confirm what we can
reproduce, agree on a disclosure timeline with you, and credit you in the
advisory unless you would rather we did not.

## What is in scope

Anything that lets a client read or write outside the keyspaces its credentials
cover, anything that loses an acknowledged write or breaks the consistency
guarantees in [docs/REQUIREMENTS.md](docs/REQUIREMENTS.md), and anything that
lets one tenant deny service to another.

Two things are known and documented rather than vulnerabilities. The peer port,
7101, uses a private framing and authenticates nothing on its own; it belongs on
a private network, and [ADR 0004](docs/adr/0004-peer-traffic-uses-private-framing.md)
explains why. And Orbita is pre-1.0 and not ready for production, so a report
about a component that is not implemented yet is a bug report rather than a
security one.

## Supported versions

Orbita has not reached 1.0. Fixes land on `develop`, which is the integration
branch, and there are no maintained release branches yet. This section gets a
table when there is something to put in it.
