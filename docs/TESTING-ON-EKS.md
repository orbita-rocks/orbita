# Testing on EKS

A disposable Orbita cluster on real EKS with real S3, for testing and
benchmarking against the thing we actually ship to rather than MinIO in Compose.
The chart already models this deployment — IRSA, a service-account role, gp3 —
and until now nothing had ever run it. This is the ground the v0.2.0 measurement
work (benches, a soak test, the numbers in the requirements) stands on.

Bring-up and teardown are each one command, and teardown removes everything,
including the bucket. A test cluster you forget about is a bill, so teardown is
part of the procedure here, not an afterthought.

## What it costs

Roughly **a dollar an hour**, before it holds a byte. The EKS control plane is
about $0.10/hour and three `m5.xlarge` nodes are about $0.58/hour between them;
S3 and EBS are rounding error at test scale. A cluster left up over a weekend is
real money for nothing, which is the whole reason `down.sh` exists and this
document leads with it.

## Prerequisites

- `aws`, `eksctl`, `kubectl`, `helm`, and `envsubst` on your PATH. `envsubst`
  ships with GNU gettext (`brew install gettext` on macOS).
- AWS credentials with permission to create an EKS cluster, a VPC, IAM roles and
  an OIDC provider, and an S3 bucket. eksctl drives CloudFormation, so in
  practice this is a fairly broad role; a personal or sandbox account is the
  right place, not a shared production one.
- The `orbita` container image has to be pullable by the cluster. The chart
  defaults to `ghcr.io/orbita-rocks/orbita` at the chart's appVersion. Set
  `image.tag` in `deploy/helm/orbita/values-eks.yaml` to the build you want to
  test, or leave it at the chart default.
- The `orbita` CLI, for the smoke check. A release binary on your PATH, or point
  `ORBITA_BIN` at a local build (`target/debug/orbita`).

## Bring-up

Two values: a region and a bucket name. The account id is read from your
credentials, so you do not pass it.

```
export ORBITA_EKS_REGION=us-west-2
export ORBITA_EKS_BUCKET=orbita-test-$USER
deploy/eks/up.sh
```

That creates the bucket, then an EKS cluster with an OIDC provider and an IRSA
role scoped to exactly that bucket, applies the gp3 StorageClass, and installs
the chart with the AWS overlay. It takes about fifteen minutes, almost all of it
the EKS control plane coming up. The overlay's placeholders are filled for you;
the rendered files land next to the templates as `*.rendered.yaml` and are
gitignored, because they carry your account id and bucket name.

## Smoke check

```
deploy/eks/smoke.sh
```

Port-forwards the client Service and drives the shipped `orbita` CLI to prove
the cluster works end to end: the leader group forms a quorum, a keyspace is
created, a write lands and reads back, and — the part that matters most — a
segment shows up in the bucket. That last check is what proves the object store,
the IRSA credentials, and the flush path all work against real S3, which is the
only reason to stand this up instead of running Compose. It waits up to two
minutes, because a segment is published on the 30-second flush timer rather than
on the write itself.

It uses the CLI through a port-forward rather than the `tests/e2e/` suite, which
spawns its own local node and cannot be aimed at a remote endpoint.

## Teardown

```
deploy/eks/down.sh
```

Uninstalls the release, deletes the cluster — which unwinds the node group, the
OIDC provider, and the IRSA role with it — and empties and deletes the bucket.
Each step tolerates the thing already being gone, so a teardown after a
half-finished bring-up still ends with nothing left running. Run it when you are
done. The bucket also carries a seven-day expiration lifecycle as a backstop, so
a cluster you forget stops accruing storage even if you never run this — but the
control-plane and node bill only stops when the cluster is deleted.

## What it creates

- An S3 bucket (the name you chose), with a seven-day object expiration as a
  safety net.
- An EKS cluster named `orbita-test`, its VPC, and one `m5.xlarge` managed node
  group of three.
- An IAM OIDC provider for the cluster.
- An IRSA role, `orbita-test-s3`, whose trust policy names exactly the
  `orbita/orbita` service account and whose permissions are `Get`/`Put`/`Delete`
  and a prefix `List` on the one bucket. No `s3:*`, no second bucket.
- The `aws-ebs-csi-driver` addon and a `gp3` StorageClass.

## The one decision worth reading: eksctl, not Terraform

The repo's other infrastructure — the live-object-store CI role — is Terraform,
so eksctl here is a deliberate exception. This cluster is disposable, and eksctl
is built for disposable: one command stands up the VPC, control plane, node
group, OIDC provider, and IRSA role, and one command removes all of them by
unwinding the CloudFormation stacks. The same in Terraform means either
assembling the whole EKS stack by hand or leaning on a large community module,
and then carrying state for a cluster whose entire purpose is to be thrown away.
Terraform earns its keep on durable, reviewable infrastructure; a throwaway test
cluster is the case where it does not. The reasoning is repeated in a comment at
the top of `deploy/eks/cluster.yaml`, where the next person will be standing when
they wonder the same thing.

## What this is not

A test cluster, not a production one. There is no multi-AZ data commitment, no
backup story, and no custom domain. It is single-region, it holds nothing you
should care about losing, and it is meant to be deleted. The benchmarks and soak
tests that will run on it are their own work (#69); this only builds the ground
they run on.
