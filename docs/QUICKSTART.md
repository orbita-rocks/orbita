# Quickstart

From nothing to a cluster with a key written and read back. Pick one of the
three paths. They all end at the same place.

## What works today, and what does not

Orbita is being built. Read this before you spend an hour on it.

- `orbita dev` runs a single node and serves reads, writes, deletes, and scans.
  This works end to end.
- Every node binds both listeners, the client one and the peer one, and
  `orbita cluster ping` answers on the client port.
- A multi-node cluster starts and every node reports healthy, but the nodes do
  not yet find each other. A worker does not register with the leader group,
  so a read or a write against one reports `node N is not known to this
  cluster`. Registration lives in `orbita-server` and is the outstanding piece.
- The admin service is not implemented server-side yet, so `keyspace create`,
  `credential create`, `cluster describe`, and the partition commands return
  `Unimplemented` against a real node. The CLI side of all of them is done and
  tested, and `orbita dev` creates a keyspace at startup so you do not need
  `keyspace create` to write a key.
- There is no object store behind `orbita dev`, on purpose. A laptop does not
  need bulk durability to try the thing out.

So: the single node path below works end to end today. The Compose and
Kubernetes paths get you a correct deployment shape, the right ports in the
right places, and a cluster that will serve when registration lands.

## One node on your laptop

```
cargo run --bin orbita -- dev
```

That is a whole cluster: one node that is its own leader group and its own
worker, listening on 127.0.0.1:7100 with state under `.orbita/dev`. It creates
a keyspace called `default` at startup so the first write does not need a
second command.

In another terminal:

```
orbita keyspace create demo
orbita set demo greeting hello
orbita get demo greeting
orbita cluster describe
```

`get` prints the value on the first line so it pipes. It exits 2 when the key
is absent and 3 when a conditional write did not apply, so a script can branch
without parsing anything.

Delete `.orbita/dev` to start over, or run `orbita dev --clean`.

That cargo line is fine for trying the thing out. If you are going to change
the code, read `docs/BUILD.md` first: the build and the test suites run through
moon, which is also what CI runs, so it is how you find out whether a change
holds up before you push it.

## A cluster on Docker Compose

Three leader group members, three workers, and MinIO standing in for S3.

```
docker compose up --build -d
docker compose --profile smoke run --rm smoke
```

The smoke service creates a keyspace, writes a key, reads it back, and prints
the cluster description. Today it stops at the first step, because the admin
service is not implemented server-side yet. To do it by hand instead:

```
docker compose run --rm cli keyspace create demo
docker compose run --rm cli set demo greeting hello
docker compose run --rm cli get demo greeting
```

Worker 1 publishes its client port to 127.0.0.1:7100, so a binary on your
machine works too:

```
orbita --endpoint http://127.0.0.1:7100 cluster describe
```

Check that everything is up with `docker compose ps`. All seven services should
say healthy within a minute.

The first build compiles RocksDB from source and takes about ten minutes.
Everything after that is cached. It builds four files at a time, because each
parallel C++ job wants roughly a gigabyte and the default Docker Desktop
allocation is smaller than most laptops have cores. On a bigger machine:

```
docker compose build --build-arg BUILD_JOBS=16
```

Tear it down with `docker compose down -v`. The `-v` removes the volumes, which
is what you want unless you meant to keep the data.

## A cluster on Kubernetes

```
helm install orbita deploy/helm/orbita --namespace orbita --create-namespace
kubectl --namespace orbita get pods --watch
```

Then from your laptop:

```
kubectl --namespace orbita port-forward svc/orbita 7100:7100
orbita keyspace create demo
orbita set demo greeting hello
orbita get demo greeting
```

If you would rather not have Helm in the loop, `kubectl apply -f
deploy/manifests/orbita.yaml` produces the same topology with no templating.

Set `objectStore.endpoint` and the credentials before this holds anything you
care about. Without an object store, compacted data stays on local disks and
replacing a worker means rehydrating it from its replicas.

Upgrading is a StatefulSet rolling update and nothing else. `docs/UPGRADES.md`
has the procedure and is honest about which parts of it the server does not
support yet.

## The two ports

Every node binds two listeners, and the difference matters when you deploy.

- 7100 carries client and admin gRPC. It speaks a published, versioned
  protocol. This is the port to expose.
- 7101 carries traffic between nodes: WAL replication, control plane messages,
  and requests proxied to a partition owner. It uses a private framing that is
  compatible only within a cluster version window, and it authenticates nothing
  on its own.

Put 7101 on a private network and keep it there. The Compose file publishes
only 7100, and only from one worker. The Helm chart puts 7101 on the headless
Services and leaves it off the client Service, so setting `service.type` to
`LoadBalancer` exposes 7100 and nothing else. The reasoning is in
`docs/adr/0004-peer-traffic-uses-private-framing.md`.

`cluster.leader_peers` is a list of peer addresses on 7101, identical on every
node. A leader forms its initial Raft configuration from it and ignores it once
it has a Raft log. A worker uses it to find a leader to register with, retrying
until one answers, because start order in an orchestrator is nobody's choice.
The most common way to get this wrong is to list the client port, which looks
almost right and never forms a quorum.

## Configuration

Four sources, each beating the one before it: built-in defaults, a TOML file,
environment variables, then flags. Every option has a default that works.

```
orbita config show      what was resolved, and which source won
orbita config env       every environment variable that is read
```

The file is looked for at `--config`, then `ORBITA_CONFIG`, then
`./orbita.toml`, then `/etc/orbita/orbita.toml`. A path you name explicitly has
to exist.

## During an incident

```
orbita cluster describe
```

It prints a summary first, then the nodes, then the partition map. The summary
answers whether anything is wrong; the tables answer which one. Each partition
shows how far every replica trails the owner's committed lamport, and a
partition whose owner is not healthy says so in its own row.

Add `--output json` for anything scripted. The JSON field names are a supported
interface. The tables are not.

To ask whether one node is up rather than whether the cluster is well:

```
orbita --endpoint http://node-3:7100 cluster ping
```

That is what the Compose health check and the Kubernetes probes run. It exits 0
if the node answered at all, including with an error, because a probe that
restarts a node for returning a permission error turns one bad deploy into an
outage.
