# RustSec advisory attestation

Status: clean root-lockfile audit against a pinned current RustSec database;
not an SBOM, source review, provider assessment, or legal opinion.

audited-at-utc: 2026-11-16T22:50:00Z
cargo-audit-version: 0.23.0
advisory-database-commit: 02e2517f457a359fc6d3440ea4560c8ea188fee8
advisory-database-commit-date: 2026-11-15T10:59:21+01:00
advisory-count: 1509
lockfile-sha256: 2643b1cf095ca8aca475f989d10be6bf643ac799148b4c57709a7f9d300b4f65
dependency-count: 113
vulnerability-count: 0
warning-count: 0

Produced with `cargo audit 0.23.0 --no-fetch --deny warnings` against the exact
database commit above. No advisory is ignored. CI audits against the then-current
database. This evidence covers only the coordinated source lockfile.
