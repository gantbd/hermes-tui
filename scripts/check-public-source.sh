#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

for required in CHANGELOG.md CONTRIBUTING.md LICENSE-APACHE LICENSE-MIT README.md SECURITY.md rust-toolchain.toml release/github-source.json docs/advisory-attestation.md; do
  [[ -s "$required" ]] || { echo "missing Hermes TUI public-source file: $required" >&2; exit 1; }
done

rg -q '^channel = "1\.95\.0"$' rust-toolchain.toml
rg -q '"remoteReachabilityQualified": false' release/github-source.json
rg -q '"revision": "2e4faf10ec0a70805aa1db1555f93c77fcf0a1e4"' release/github-source.json
rg -q '"revision": "b272106cf6d0093839a7d696c829bf8a408b0306"' release/github-source.json
rg -q '"revision": "3f60c1c7b00b1a61230508b11422ddfc84b61856"' release/github-source.json
expected=$(awk -F': ' '$1 == "lockfile-sha256" { print $2 }' docs/advisory-attestation.md)
actual=$(shasum -a 256 Cargo.lock | awk '{print $1}')
[[ -n "$expected" && "$actual" == "$expected" ]] || { echo 'Hermes TUI lockfile changed without a new advisory attestation' >&2; exit 1; }

echo 'Hermes TUI public-source contract pins the qualified ledger dependency chain'
