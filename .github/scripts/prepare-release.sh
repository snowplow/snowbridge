#!/bin/bash

set -euo pipefail

VERSION="$1"

if [[ -z "$VERSION" ]]; then
    echo "Error: Version argument is required"
    exit 1
fi

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "Error: Version must be in format x.y.z (e.g., 3.5.0)"
    exit 1
fi

echo "Preparing release for version $VERSION"

RELEASE_DATE=$(date +%Y-%m-%d)

if [[ ! -f "VERSION" ]]; then
    echo "Error: VERSION file not found"
    exit 1
fi

if [[ ! -f "CHANGELOG" ]]; then
    echo "Error: CHANGELOG file not found"
    exit 1
fi

if [[ ! -f "README.md" ]]; then
    echo "Error: README.md file not found"
    exit 1
fi

if [[ ! -f "cmd/constants.go" ]]; then
    echo "Error: cmd/constants.go file not found"
    exit 1
fi

echo "Updating VERSION file"
echo "$VERSION" > VERSION

echo "Updating CHANGELOG"

# Get commits since the last release (stop at first "Prepare for X.Y.Z release" commit)
COMMITS=$(git log --oneline --no-merges --pretty=format:"%s" | awk '/^Prepare for .* release$/ {exit} {print}' || true)

# Create the new changelog entry
TEMP_CHANGELOG=$(mktemp)
{
    echo "Version $VERSION ($RELEASE_DATE)"
    echo "--------------------------"
    if [[ -n "$COMMITS" ]]; then
        echo "$COMMITS"
    else
        echo "No changes since last release"
    fi
    echo ""
} > "$TEMP_CHANGELOG"

# Append existing changelog
cat CHANGELOG >> "$TEMP_CHANGELOG"
mv "$TEMP_CHANGELOG" CHANGELOG

echo "Updating README.md version badge"
sed -i.bak "s/golang-[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\(-[a-zA-Z0-9][a-zA-Z0-9]*\)\{0,1\}-6ad7e5/golang-$VERSION-6ad7e5/g" README.md
rm README.md.bak

echo "Updating cmd/constants.go"
sed -i.bak "s/AppVersion = \"[^\"]*\"/AppVersion = \"$VERSION\"/g" cmd/constants.go
rm cmd/constants.go.bak

echo "All files updated successfully for version $VERSION"