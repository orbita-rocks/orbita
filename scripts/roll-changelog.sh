#!/usr/bin/env bash
#
# Turns the Unreleased section of CHANGELOG.md into a released one.
#
# Every commit here is a conventional commit, so git-cliff can write the
# entries from the history and nobody has to remember to append a line while
# they work. What git-cliff cannot do is decide that a change deserves a
# sentence a human would write, so the Unreleased section stays hand-editable
# and this script only renames it and opens a fresh empty one above it.
#
#   scripts/roll-changelog.sh 0.1.0
#
set -euo pipefail

readonly ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly CHANGELOG="$ROOT/CHANGELOG.md"

die() {
  echo "roll-changelog: $*" >&2
  exit 1
}

[[ $# -eq 1 ]] || die "usage: roll-changelog.sh <version>"

readonly VERSION="${1#v}"
readonly TODAY="$(date -u +%Y-%m-%d)"

grep -q '^## \[Unreleased\]$' "$CHANGELOG" \
  || die "no '## [Unreleased]' heading in CHANGELOG.md"

# Running the bump twice for the same version is an easy thing to do while
# fixing up a release branch, and without this it silently produces two
# sections for one version and the release notes get extracted from whichever
# one is first.
grep -q "^## \[$VERSION\]" "$CHANGELOG" \
  && die "CHANGELOG.md already has a section for $VERSION"

# If git-cliff is on the path, fill Unreleased from the commits since the last
# tag first. Anything already written by hand stays: cliff's output is appended
# under the same heading, and a duplicate line is cheaper to delete during
# review than a missing one is to notice.
if command -v git-cliff >/dev/null 2>&1; then
  echo "    (git-cliff found, generating entries since the last tag)"
  generated="$(cd "$ROOT" && git-cliff --unreleased --strip all --config cliff.toml 2>/dev/null || true)"
  if [[ -n "$generated" ]]; then
    python3 - "$CHANGELOG" <<'PY' "$generated"
import sys
path, generated = sys.argv[1], sys.argv[2]
text = open(path).read()
marker = "## [Unreleased]\n"
head, _, tail = text.partition(marker)
open(path, "w").write(head + marker + "\n" + generated.strip() + "\n" + tail)
PY
  fi
fi

perl -0pi -e "s{## \\[Unreleased\\]\n}{## [Unreleased]\n\n## [$VERSION] - $TODAY\n}" \
  "$CHANGELOG"

grep -q "^## \[$VERSION\] - $TODAY$" "$CHANGELOG" \
  || die "the changelog section for $VERSION was not written"
