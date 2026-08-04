#!/usr/bin/env bash
#
# Sets the workspace version and every place that has to agree with it.
#
# The workspace version in Cargo.toml is the one source of truth. Everything
# else, the image tag in the Kubernetes manifests, the chart's appVersion, the
# heading in the changelog, is derived from it. They live in different files
# because the tools that read them are different, which means a hand edit
# reliably updates some of them and forgets the rest. This script is the reason
# that cannot happen.
#
# It edits files and stops. It does not commit, tag, or push, so it is safe to
# run locally and read the diff before deciding anything. The release workflow
# commits on its behalf.
#
#   scripts/bump-version.sh 0.1.0
#   scripts/bump-version.sh 0.2.0-dev
#
set -euo pipefail

readonly ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

die() {
  echo "bump-version: $*" >&2
  exit 1
}

[[ $# -eq 1 ]] || die "usage: bump-version.sh <version>, e.g. 0.1.0 or 0.2.0-dev"

readonly VERSION="${1#v}"

# Semver, with an optional prerelease. Build metadata is deliberately not
# accepted: Cargo tolerates it, but it does not participate in ordering, and a
# version that sorts unpredictably is worse than no version at all.
if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  die "'$VERSION' is not a semver version"
fi

# `perl -pi` rather than `sed -i` because the two seds disagree about what -i
# takes as an argument, and this script runs on both a laptop and a runner.
replace() {
  local pattern="$1" replacement="$2" file="$3"
  perl -pi -e "s{$pattern}{$replacement}" "$ROOT/$file"
}

echo "==> workspace version -> $VERSION"
# Anchored inside [workspace.package], which is the only place in the root
# manifest where a bare `version = ` appears at the start of a line.
perl -0pi -e "s{(\[workspace\.package\]\nversion = \")[^\"]+(\")}{\${1}$VERSION\${2}}" \
  "$ROOT/Cargo.toml"
grep -q "^version = \"$VERSION\"$" "$ROOT/Cargo.toml" \
  || die "the workspace version did not change; has Cargo.toml been reshaped?"

echo "==> chart appVersion -> $VERSION"
# The chart's own `version` tracks the templates and moves on its own schedule,
# so only appVersion is touched here. See the comment in Chart.yaml.
replace '^appVersion: ".*"$' "appVersion: \"$VERSION\"" deploy/helm/orbita/Chart.yaml

echo "==> manifest image tags -> $VERSION"
# The plain manifests are the copy-paste path for someone without Helm, so a
# floating `:latest` in them is a promise we cannot keep across a version
# window. They get pinned to the release like everything else.
replace 'image: ghcr\.io/orbita-rocks/orbita:\S+' \
  "image: ghcr.io/orbita-rocks/orbita:$VERSION" deploy/manifests/orbita.yaml

echo "==> Cargo.lock"
# The lockfile carries a version entry per workspace member, so it is part of
# the bump whether or not any dependency moved. --workspace restricts this to
# our own crates and leaves the rest of the graph pinned.
(cd "$ROOT" && cargo update --workspace --quiet)

# Prereleases do not get a changelog section. They are a moving target by
# definition, and cutting a heading for one would leave the real release with
# nothing left under Unreleased.
if [[ "$VERSION" != *-* ]]; then
  echo "==> changelog section for $VERSION"
  "$ROOT/scripts/roll-changelog.sh" "$VERSION"
fi

echo
echo "Done. Review with: git diff"
