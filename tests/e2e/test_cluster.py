"""Where the multi-node tests go.

Clustering is being built and does not work yet, so there is nothing to test
here. This file is a marker so the next person does not have to decide where
these belong.

What belongs here once a cluster runs:

- A client connected to a node that does not own a key still reads and writes
  it, because the requirement is that a client can talk to any worker and the
  hop is the server's problem.
- One client, several nodes, and the linearizability the requirements promise:
  a value written through one endpoint is visible through every other endpoint
  on the very next read.
- A killed owner, and reads that keep working while writes come back within the
  ten second target.
- Versions across a split, since a client holding a version through a split
  must not have it silently invalidated.

The harness in `harness.py` starts one node with `orbita dev`. A cluster needs
`orbita serve` with a peer list, so growing the harness a `Cluster` class that
starts several is the first step.
"""
