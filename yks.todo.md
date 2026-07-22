# YKS-owned work for the JVM Hocuspocus runtime

This file contains only YKS-owned compatibility, performance, or distribution
work. Hocuspocus must not hide an engine failure with adapter-side state
cloning, rollback emulation, reflection, private wire formats, or artificial
Provider batching.

Audited source baseline:
`1ba20238fc81990f7d672c96aee62c9511c6d786` on YKS `main`, targeted for
`dev.yks:yks:0.2.2`. It adds retained-view thread confinement to the
2026-07-17 adversarial-performance,
root-emptiness, advanced oracle, relative-position, ABI, and Node 26
scalar-read changes plus the 2026-07-18 UndoManager optimization.

## No open engine-owned blockers

The current 37-scenario YKS/Yjs gate, JVM test suite, Yjs and Yrs oracles, ABI
check, and standalone local consumer all pass. The `0.2.2` remote-consumer gate
will run when that tag is published. New engine failures found by Hocuspocus
should be recorded here instead of hidden with adapter-side compatibility
state.

## Resolved: high-level UndoManager CPU parity

The expanded cross-runtime benchmark now measures XML construction/rendering,
relative-position resolution, V2 merge/diff, and 1,000 UndoManager undo/redo
steps. The original YKS-owned gap was reported here rather than hidden in
Hocuspocus:

- 1,000 undo plus 1,000 redo steps: initially 81.6 ms versus Yjs 6.1 ms.
  Indexed visible-neighbor lookup first reduced YKS to 39.3 ms versus Yjs
  5.9 ms. Correct observer classification, a minimal internal mutation summary,
  and single-item normalization/restoration then reduced the final 50/30
  complete-gate result to 4.877 ms versus Yjs 6.056 ms, or 0.81x.

The default release gate and checked advanced subset now enforce the same
strict 1.5x ratio and independent 6 ms micro-latency conditions. XML
build/render measures 0.84x Yjs, relative-position resolution 0.70x, V2
merge/diff 0.12x, and packed clock-range snapshot delta 1.22x in the final
37-scenario run.

## Resolved: independently consumable engine artifact

The audited source is configured as immutable GitHub Packages artifact
`dev.yks:yks:0.2.2`, and `hocuspocus-yks` depends on that version. YKS `v0.2.2`
must be published before the Hocuspocus release. The release
workflow rebuilds the tag, verifies reproducible artifacts, runs the standalone
consumer, publishes, and then verifies a clean remote consumer. Local
development can still use `/Volumes/D/yks` as a Gradle composite.

## Resolved: incremental standard-update cleanup

The original core A/B benchmark found
`dev.yks.YDoc.mergeNewItemsUnobserved(Map, Map)` at 9.43% of sampled
allocation pressure. YKS now keeps the client-store list once, processes a
contiguous range, avoids per-update `mergeIds` and duplicate projections, and
caches versioned state-vector snapshots.

The exact 1,000 sequential standard-update workload is part of the strict
cross-runtime gate. The final YKS gate passes all 37 scenarios against Yjs, and
JMH measured the complete workload at 0.954 ms/op and 7,583,892 B/op. A new
Hocuspocus JFR no longer contains `mergeNewItemsUnobserved` as a dominant site.
The remaining end-to-end CPU gap is therefore not assigned to this resolved
engine path.

## Resolved: Node 26 scalar-read ratio regression

A clean Node 26.5.0 rerun exposed a YKS-only failure that Node 24 had not:
`length_read_200000` measured 0.134–0.135 ms versus Yjs
0.052–0.057 ms, or 2.36x–2.57x. One noisy run also placed open-root
apply just over the 1.5x boundary. Hocuspocus did not add an adapter cache or
relax the gate.

The fix is in the sibling YKS source. The two scalar-only fixtures now use
`YThreadAccessPolicy.UNCHECKED`, matching Yjs's lack of a JVM confinement
check, while production safety policies remain unchanged. Mutation-coherent
engine caches cover maintained text length and immutable first-array scalar
reads. The full 35-scenario, 50-warmup/30-sample gate passes again:

- 200,000 text length reads: YKS 0.064 ms, Yjs 0.056 ms, 1.13x;
- 100,000 array length plus first-index reads: YKS 0.217 ms, Yjs
  0.444 ms, 0.49x;
- 5,000 structs into opened roots: YKS 1.577 ms, Yjs 1.101 ms,
  1.43x.

YKS unit, interoperability, subdocument, and cache-invalidation regressions
cover the changed paths.

## Resolved: type-neutral emptiness for unopened remote roots

YKS now exposes `YDoc.isRootEmpty(name)`, defined from the structural sequence
start and map-key visibility that correspond to Yjs `_start` and `_map`.
Regression tests cover missing, concretely opened, remotely unopened,
deleted-only, and map-only roots. `YksCrdtDocument.isFieldEmpty` delegates
directly to this API; Hocuspocus no longer throws or guesses a root type.

## Resolved engine boundaries used by Hocuspocus

- `YThreadAccessPolicy.EXTERNALLY_SERIALIZED` permits sequential coroutine
  dispatcher handoff while rejecting overlapping access.
- `YStandardUpdatePolicy.REQUIRE_STANDARD` rolls back a local mutation that
  cannot be represented as genuine Yjs V1 before observers receive it.
- The strict Yjs performance gate covers 37 scenarios, including adversarial
  snapshot, formatting, observer, wide-tree, and sequential-update workloads.
- Private lossless envelopes and Kotlin-only metadata do not cross the
  Hocuspocus/Yjs standard-wire boundary.

Private lossless metadata, browser-only APIs, DOM helpers, and JavaScript API
shape remain intentional YKS compatibility boundaries. They are not server
runtime blockers and must not be reimplemented inside Hocuspocus.
