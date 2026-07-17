# YKS-owned work for the JVM Hocuspocus runtime

This file contains only YKS-owned compatibility or performance work.
Hocuspocus must not hide these failures with adapter-side state cloning,
rollback emulation, reflection, or private YKS wire formats.

Audited baseline: clean YKS revision
`f0c33ecb73e2a1327378b5893f0e8044ba4e2559` on 2026-07-17.

## P0: publish a green YKS revision for reproducible CI

The audited revision above is clean and green locally, but `git ls-remote
origin` still exposes `e5e2a3355a6c6d48e137f1e98a0caef6c6e12e5b` as
`main`/`HEAD` and does not expose the audited revision through any checked
branch or tag. Hocuspocus CI is pinned to the new revision but cannot check it
out from `hwisu/yks` until that commit is pushed.

Completion criterion: push the audited green revision so the immutable CI pin
is reachable. Publish the same source as a new Maven package version, update
`jvm/hocuspocus-yks/build.gradle.kts` from `dev.yks:yks:0.1.1`, and regenerate
the Hocuspocus dependency locks and verification metadata. Hocuspocus CI runs
the pinned YKS `check` and `consumerSmokeTest` before its own suite, so a red
engine cannot be silently accepted. YKS 0.1.1 predates the
externally-serialized thread policy and strict standard-update policy used by
this adapter, so a Hocuspocus artifact built with the composite checkout is
not independently consumable until that engine release exists.

## Resolved at the audited baseline

- Coroutine callers can use `YThreadAccessPolicy.EXTERNALLY_SERIALIZED`.
  Sequential dispatcher hand-off is supported while overlapping access still
  fails with `YksConcurrentAccessException`.
- Hocuspocus uses `YStandardUpdatePolicy.REQUIRE_STANDARD`. A local mutation
  that cannot be represented as a standard Yjs V1 update is rolled back
  atomically before observers receive it.
- At this clean baseline, functional verification passes:
  - YKS Gradle `check` and `consumerSmokeTest`
  - YKS unit tests: 687/687
  - YKS JVM interoperability tests: 86/86
  - JavaScript Yjs oracle: 106/106
  - Yrs oracle: 4/4
  - strict Yjs performance scenarios: 28/28
  - Hocuspocus JVM Gradle `check`
  - Hocuspocus Provider v4 interoperability oracle

These items are no longer blockers and should not be worked around in the
Hocuspocus adapter.

## Intentional boundaries, not Hocuspocus workarounds

Private lossless envelopes, Kotlin-only metadata, compact/static XML values,
browser DOM helpers, and ambiguous remote root-type inference remain explicit
YKS extension boundaries. They must not cross the Hocuspocus/Yjs standard-wire
API.
