#!/usr/bin/env bash
# Cut a release locally, then push to trigger Maven Central publishing.
#
# Usage:
#   ./scripts/release.sh <release-version> [<next-dev-version>]
#
# Examples:
#   ./scripts/release.sh 0.1.0
#   ./scripts/release.sh 0.1.0 0.2.0-SNAPSHOT
#
# What this does:
#   1. Validates the working tree is clean
#   2. Runs mvn release:prepare (strips SNAPSHOT, commits, tags locally)
#   3. Shows the commits created and prompts to confirm push
#   4. Pushes commits + tag → GitHub Actions deploys to Maven Central
set -euo pipefail

RELEASE_VERSION="${1:-}"
DEV_VERSION="${2:-}"

if [ -z "$RELEASE_VERSION" ]; then
    echo "Usage: $0 <release-version> [<next-dev-version>]" >&2
    exit 1
fi

# Auto-compute next patch version if not provided
if [ -z "$DEV_VERSION" ]; then
    MAJOR=$(echo "$RELEASE_VERSION" | cut -d. -f1)
    MINOR=$(echo "$RELEASE_VERSION" | cut -d. -f2)
    PATCH=$(echo "$RELEASE_VERSION" | cut -d. -f3)
    DEV_VERSION="${MAJOR}.${MINOR}.$((PATCH + 1))-SNAPSHOT"
fi

# Validate clean working tree
if ! git diff --quiet || ! git diff --cached --quiet; then
    echo "error: working tree has uncommitted changes — commit or stash first" >&2
    exit 1
fi

echo "[release] version:     $RELEASE_VERSION"
echo "[release] next dev:    $DEV_VERSION"
echo "[release] tag:         v${RELEASE_VERSION}"
echo ""

./mvnw release:prepare \
    --batch-mode \
    -DreleaseVersion="$RELEASE_VERSION" \
    -DdevelopmentVersion="$DEV_VERSION"

echo ""
echo "[release] Commits created:"
git log --oneline -3
echo ""
echo "[release] Tag created: $(git tag --points-at HEAD^ 2>/dev/null || git describe --tags --exact-match HEAD^ 2>/dev/null || echo "v${RELEASE_VERSION}")"
echo ""
echo "Review the commits above, then write your release notes on GitHub."
echo ""
read -r -p "Push commits and tag to trigger Maven Central deploy? [y/N] " CONFIRM
if [[ "$CONFIRM" =~ ^[Yy]$ ]]; then
    git push && git push --tags
    echo ""
    echo "[release] Pushed. GitHub Actions will deploy v${RELEASE_VERSION} to Maven Central."
    echo "Track progress: https://github.com/dfa1/rocksdbffm/actions"
else
    echo ""
    echo "[release] Skipped push. When ready:"
    echo "  git push && git push --tags"
fi
