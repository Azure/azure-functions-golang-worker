#!/usr/bin/env bash
# ---------------------------------------------------------------
# eng/ci/scripts/codeql-build.sh
#
# One script that builds every Go module in this repo.
#
# Why this exists:
# CodeQL only sees Go code that gets compiled while it is watching.
# When each module is built in a separate pipeline step, CodeQL
# can miss some of them. Doing every build here, in one place,
# makes sure CodeQL sees everything.
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
  # explanation.
  go work edit -replace github.com/azure/azure-functions-golang-worker=.
  go work edit -replace \
    google.golang.org/genproto@v0.0.0-20230110181048-76db0878b65f=google.golang.org/genproto@v0.0.0-20250528174236-200df99c418a
fi
cat go.work

echo "==> Downloading dependencies (root)"
go mod download

echo "==> Building root module"
# Samples are standalone apps, built separately below.
go build $(go list ./... | grep -v /samples/)

for mod in middleware/otelfunc otelcollector triggers/blob; do
  echo "==> Building submodule: ${mod}"
  ( cd "${mod}" && go build ./... )
done

echo "==> Building samples"
failed=0
for d in samples/*/; do
  if [[ -f "${d}main.go" ]]; then
    echo "  -> ${d}"
    if ! ( cd "${d}" && go build . ); then
      echo "  FAILED: ${d}"
      failed=1
    fi
  fi
done
exit "${failed}"
