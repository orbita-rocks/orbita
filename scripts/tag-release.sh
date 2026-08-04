#!/usr/bin/env bash
#
# Checks that a commit is releasable and tags it.
#
# Pushing the tag is what starts the release, and a release cannot be taken
# back: the image is pulled, the binary is downloaded, the chart is cached.
# Everything this script does before creating the tag is aimed at the mistakes
# that are cheap to make and expensive to undo, mostly tagging a commit that is
# not the one that was reviewed, or tagging a version the manifests disagree
# with.
#
# The push is a separate flag because that is the irreversible half.
#
#   scripts/tag-release.sh v0.1.0
#   scripts/tag-release.sh v0.1.0 --push
#
set -euo pipefail

readonly ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

die() {
  echo "tag-release: $*" >&2
  exit 1
}

[[ $# -ge 1 ]] || die "usage: tag-release.sh <vX.Y.Z> [--push]"

readonly TAG="$1"
PUSH=0
[[ "${2:-}" == "--push" ]] && PUSH=1

[[ "$TAG" == v* ]] || die "the tag must start with v, e.g. v0.1.0"
readonly VERSION="${TAG#v}"

cd "$ROOT"

# A dirty tree means the tag would point at a commit that does not describe
# what is about to be built.
[[ -z "$(git status --porcelain)" ]] || die "the working tree is dirty"

git rev-parse -q --verify "refs/tags/$TAG" >/dev/null \
  && die "the tag $TAG already exists locally"

manifest_version="$(perl -0ne 'print $1 if /\[workspace\.package\]\nversion = "([^"]+)"/' Cargo.toml)"
[[ "$manifest_version" == "$VERSION" ]] \
  || die "the workspace version is $manifest_version but the tag says $VERSION; run scripts/bump-version.sh"

chart_version="$(perl -ne 'print $1 if /^appVersion: "([^"]+)"/' deploy/helm/orbita/Chart.yaml)"
[[ "$chart_version" == "$VERSION" ]] \
  || die "the chart appVersion is $chart_version but the tag says $VERSION"

# The release has to come from main. A tag anywhere else would publish code
# that never went through a release PR, and the artifacts would be
# unreproducible from the default branch.
git fetch --quiet origin main
git merge-base --is-ancestor HEAD origin/main \
  || die "HEAD is not reachable from origin/main; release tags come from main"

# A prerelease tag skips the changelog check, since a moving prerelease has no
# section of its own by design.
if [[ "$VERSION" != *-* ]]; then
  grep -q "^## \[$VERSION\]" CHANGELOG.md \
    || die "CHANGELOG.md has no section for $VERSION"
fi

notes="$(git log --oneline "$(git describe --tags --abbrev=0 2>/dev/null || git rev-list --max-parents=0 HEAD)"..HEAD | wc -l | tr -d ' ')"
echo "Tagging $(git rev-parse --short HEAD) as $TAG ($notes commits since the last tag)."

# Signed if a signing key is configured, annotated otherwise. An unsigned tag
# is still a real tag; refusing to cut a release over a missing GPG key would
# be the tool getting in the way.
if git config --get user.signingkey >/dev/null 2>&1; then
  git tag --sign --message "Orbita $TAG" "$TAG"
else
  echo "note: no user.signingkey configured, creating an unsigned annotated tag"
  git tag --annotate --message "Orbita $TAG" "$TAG"
fi

if [[ "$PUSH" -eq 1 ]]; then
  git push origin "$TAG"
  echo "Pushed $TAG. The release workflow is building."
else
  echo
  echo "Created locally. To start the release:"
  echo "  git push origin $TAG"
fi
