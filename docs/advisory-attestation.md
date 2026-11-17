# RustSec advisory attestation

Status: clean root-lockfile audit against a pinned current RustSec database;
not an SBOM, source review, provider assessment, or legal opinion.

audited-at-utc: 2026-11-17T07:26:25Z
cargo-audit-version: 0.23.0
advisory-database-commit: 02e2517f457a359fc6d3440ea4560c8ea188fee8
advisory-database-commit-date: 2026-11-15T10:59:21+01:00
advisory-count: 1509
lockfile-sha256: edc7afe55b8a389d637867845d02105f40c432d2918af4b41abc35a383b167d1
dependency-count: 113
vulnerability-count: 0
warning-count: 0

Produced with `cargo audit 0.23.0 --no-fetch --deny warnings` against the exact
database commit above. No advisory is ignored. CI audits against the then-current
database. This evidence covers only the coordinated source lockfile.
