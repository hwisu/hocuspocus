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

## P1: reduce incremental standard-update cleanup allocation

The real WebSocket A/B benchmark added in the Hocuspocus repository runs the
same official Provider v4 workload against upstream Node Hocuspocus and the
Ktor/YKS server. After Hocuspocus-side frame/debounce optimizations, measured
throughput is within the declared 1.5x band, but JVM server CPU remains roughly
4x to 16x Node per scenario.

A steady-state JFR recording with ten times the normal sequential-update
sample identified `dev.yks.YDoc.mergeNewItemsUnobserved(Map, Map)` as the
largest application allocation site at 9.43% of sampled allocation pressure.
The hot path repeatedly obtains `store.itemsForClient(client)` and constructs
`mergeIds` lists while applying many small standard V1 updates. This is owned
by YKS; Hocuspocus must not bypass cleanup, batch Provider updates
artificially, or relay unvalidated input bytes to hide it.

Completion criterion: profile and optimize incremental `applyUpdate` cleanup
inside YKS, preserve standard-wire and observer semantics, rerun YKS's strict
28-scenario performance gate, then rerun
`pnpm benchmark:jvm:ab -- --repetitions=3` from Hocuspocus. Keep the generated
A/B JSON and a before/after JFR allocation comparison as evidence.

## P1: expose type-neutral emptiness for unopened remote roots

Upstream `Document.isEmpty(fieldName)` can inspect a Y.Doc root's internal list
and map state before the caller chooses a concrete root type. YKS represents a
root discovered from a standard remote update as `YUnopenedRoot`; its public
`toJson()` returns `null`, which is indistinguishable from a missing or empty
root to the Hocuspocus adapter.

The adapter exposes `isEmpty` for missing and concretely opened roots, but
throws for an unopened remote root instead of silently returning an incorrect
value. It must not guess Text, Array, Map, or XML type merely to answer this
query.

Completion criterion: add a public, type-neutral YKS root-emptiness query that
matches Yjs `_start`/`_map` visibility semantics for missing, opened, unopened,
deleted-only, and map-only roots. Wire `YksCrdtDocument.isFieldEmpty` to it and
replace the explicit unsupported-path regression test with parity assertions.

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
