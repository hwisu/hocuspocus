# YKS work for Hocuspocus

## Baseline

Hocuspocus builds against YKS `v0.2.13` at
`52e32342665251bb1a96355ef094fb94f06f43ad` and declares
`dev.yks:yks:0.2.13`. CI pins the same source commit.

## Status

There are no known YKS-owned blockers for Hocuspocus. The server uses standard
Yjs update V1, externally serialized document access, and type-neutral root
emptiness. Provider and mixed Node/JVM interoperability remain release gates.

YKS `0.2.13` includes release ABI bridges for consumers compiled against
`0.2.8` and `0.2.9`; the former `YEventChanges` migration warning no longer
applies.

## Policy

New engine failures belong here and should be fixed in YKS. Hocuspocus must not
hide them with state cloning, rollback emulation, reflection, private wire
formats, or artificial Provider batching.
