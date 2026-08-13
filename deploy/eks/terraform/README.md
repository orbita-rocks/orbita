# EKS test-cluster Terraform

The AWS half of the disposable test cluster: a VPC, an EKS cluster with a
managed node group and OIDC, the EBS CSI driver for gp3, an IRSA role scoped to
one bucket, and the bucket itself. The Kubernetes half — the gp3 StorageClass
and the Orbita release — is applied by `deploy/eks/up.sh` after this, because a
StorageClass and a Helm chart are Kubernetes objects, not AWS ones, and keeping
Terraform to the AWS layer avoids coupling an apply to a running cluster's
kubeconfig.

You do not normally run this directly. `deploy/eks/up.sh` and `down.sh` drive it
with the right variables and then do the Kubernetes steps around it. Reach for
the commands below only when you are debugging the infrastructure itself.

## What it creates

- A VPC across three Availability Zones, one NAT gateway.
- An EKS cluster named `orbita-test` and one `m5.xlarge` managed node group of
  three, sized for the default chart.
- The cluster's OIDC provider, and two IRSA roles: the EBS CSI driver's, and
  `orbita-test-s3` for the workload, whose trust is scoped to the `orbita/orbita`
  service account and whose policy is `Get`/`Put`/`Delete` plus a prefix `List`
  on the one bucket.
- The S3 bucket, with public access blocked, a seven-day expiration backstop,
  and `force_destroy` so `terraform destroy` actually removes it.

## Why Terraform, and why the community modules

One infrastructure-as-code tool across the repo. The live-object-store role in
`terraform/` is already Terraform, and a second tool for the second cluster
would be a second thing to install, learn, and keep working.

It leans on `terraform-aws-modules/vpc` and `terraform-aws-modules/eks` rather
than declaring subnets, route tables, NAT gateways, the control plane, and the
OIDC provider by hand. Those modules are the idiomatic way to build EKS in
Terraform and they get the fiddly parts right; for a disposable cluster,
reviewing a handful of module blocks is a better use of everyone's attention
than reviewing the VPC plumbing underneath them.

## Before you can apply

- `terraform` or `tofu` on your PATH. The scripts accept either.
- AWS credentials with permission to create a VPC, an EKS cluster, IAM roles and
  an OIDC provider, and an S3 bucket. This is broad; use a personal or sandbox
  account, not a shared production one.
- State is local, the same call the other Terraform in this repo makes. It
  lives in this directory as `terraform.tfstate` and is gitignored.
  `terraform destroy` is the normal end of it, not a migration to a remote
  backend.

## Running it directly

```
cd deploy/eks/terraform
terraform init
terraform apply -var region=us-west-2 -var bucket_name=orbita-test-you
```

Apply takes about fifteen minutes, almost all of it the EKS control plane. Tear
it down with `terraform destroy` and the same `-var`s, which removes everything
including the bucket and its contents.

## After you apply

`deploy/eks/up.sh` reads these back for you; here is what they are.

| Output | What it is for |
| --- | --- |
| `irsa_role_arn` | The role the Helm overlay's service-account annotation names. |
| `update_kubeconfig_command` | Points kubectl at the new cluster. |
| `bucket` | The object store bucket. |
| `cluster_name`, `region` | Everything else keys off these. |
