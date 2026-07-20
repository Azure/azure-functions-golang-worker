#!/usr/bin/env bash
# ---------------------------------------------------------------
# eng/ci/scripts/build-all.sh
#
# Builds every Go module in this repo (root + submodules) in one
# script, using a temporary go.work so submodules resolve local
# code instead of the published root module.
#
# Before running this script:
#   * Install Go and make sure it is on PATH.
#   * Run this from the repo root.
# ---------------------------------------------------------------

set -euo pipefail

echo "==> Go version"
go version

echo "==> Initializing go workspace"
# See eng/ci/templates/jobs/build.yml for why we need go.work and
# the genproto replace.
if [[ ! -f go.work ]]; then
  go work init . ./middleware/otelfunc ./otelcollector ./triggers/blob
  # Force Go to use the local checkout as the root module instead of
  # downloading an older published copy. See build.yml for the full
  # explanation. The version is read from a submodule's go.mod so
  # releases don't require CI edits.
  ROOT_VER=$(awk '$1=="require" && $2=="github.com/azure/azure-functions-golang-worker" {print $3}' middleware/otelfunc/go.mod)
  echo "Detected root module version: ${ROOT_VER}"
  go work edit -replace "github.com/azure/azure-functions-golang-worker@${ROOT_VER}=."
  go work edit -replace \
    google.golang.org/genproto@v0.0.0-20230110181048-76db0878b65f=google.golang.org/genproto@v0.0.0-20250528174236-200df99c418a
fi
cat go.work

echo "==> Downloading dependencies (root)"
go mod download

echo "==> Building root module"
# Samples are standalone apps with their own (gitignored) go.mod
# files; they are excluded from CI builds and from CodeQL analysis.
go build $(go list ./... | grep -v /samples/)

for mod in middleware/otelfunc otelcollector triggers/blob; do
  echo "==> Building submodule: ${mod}"
  ( cd "${mod}" && go build ./... )
done
