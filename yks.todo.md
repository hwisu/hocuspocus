# YKS-owned work for the JVM Hocuspocus runtime

This file contains only YKS-owned compatibility, performance, or distribution
work. Hocuspocus must not hide an engine failure with adapter-side state
cloning, rollback emulation, reflection, private wire formats, or artificial
Provider batching.

Audited source baseline:
`37703a1269ead28e38632b73093953621262cb6d` on YKS `main`, released as
`dev.yks:yks:0.2.0`. It includes the 2026-07-17 adversarial-performance,
root-emptiness, advanced oracle, relative-position, UndoManager, ABI, and
Node 26 scalar-read changes.

## Remaining: high-level UndoManager CPU parity

The expanded cross-runtime benchmark now measures XML construction/rendering,
relative-position resolution, V2 merge/diff, and 1,000 UndoManager undo/redo
steps. One of those workloads exposes a YKS-owned CPU gap and is reported by
`npm run benchmark:performance:advanced` rather than hidden in Hocuspocus:

- 1,000 undo plus 1,000 redo steps: initially 81.6 ms versus Yjs 6.1 ms.
  Indexed visible-neighbor lookup and single-item restore fast paths reduced
  YKS to 39.3 ms versus Yjs 5.9 ms in the final 50/30 run, but the remaining
  6.66x gap is still open.

JFR attributes the remaining UndoManager cost primarily to transaction cleanup
and virtual-merge representative bookkeeping. The strict default 1.5x gate
continues to cover every scenario without a known engine gap; the advanced
command always measures all four high-level paths. XML build/render now
measures 0.96x Yjs, relative-position resolution improved from about 4.0x to
1.10x, and V2 merge/diff measures 0.12x on the final fixture.

## Resolved: independently consumable engine artifact

The audited source is published as immutable GitHub Packages artifact
`dev.yks:yks:0.2.0`, and `hocuspocus-yks` depends on that version. The release
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
cross-runtime gate. The final YKS gate passes all 35 scenarios against Yjs, and
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
- The strict Yjs performance gate covers 35 scenarios, including adversarial
  snapshot, formatting, observer, wide-tree, and sequential-update workloads.
- Private lossless envelopes and Kotlin-only metadata do not cross the
  Hocuspocus/Yjs standard-wire boundary.

Private lossless metadata, browser-only APIs, DOM helpers, and JavaScript API
shape remain intentional YKS compatibility boundaries. They are not server
runtime blockers and must not be reimplemented inside Hocuspocus.
