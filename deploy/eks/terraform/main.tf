# The disposable EKS test cluster: a VPC, an EKS cluster with a managed node
# group and OIDC, the EBS CSI driver for gp3, and the workload IRSA role. The S3
# bucket and its access policy live in s3.tf alongside the outputs.
#
# This leans on the maintained terraform-aws-modules rather than hand-rolling a
# VPC, a control plane, node groups, an OIDC provider, and their IAM. Those
# modules are the idiomatic way to build EKS in Terraform, and for a cluster
# this disposable, reviewing a few module blocks beats reviewing three hundred
# lines of subnets, route tables, and NAT gateways that the module gets right.

locals {
  # Everything carries these, so a stray cluster is easy to find and bill back,
  # and a teardown can be audited for what it missed. Applied globally through
  # the provider's default_tags in versions.tf.
  tags = {
    "orbita.rocks/purpose"    = "test-and-benchmark"
    "orbita.rocks/disposable" = "true"
  }
}

# Spread across three Availability Zones. A test cluster does not need multi-AZ
# durability, but EKS wants subnets in at least two AZs, and three matches the
# three-node group so the scheduler has somewhere to spread the leader group.
data "aws_availability_zones" "available" {
  state = "available"
}

module "vpc" {
  source  = "terraform-aws-modules/vpc/aws"
  version = "~> 5.0"

  name = var.cluster_name
  cidr = "10.0.0.0/16"

  azs             = slice(data.aws_availability_zones.available.names, 0, 3)
  private_subnets = ["10.0.1.0/24", "10.0.2.0/24", "10.0.3.0/24"]
  public_subnets  = ["10.0.101.0/24", "10.0.102.0/24", "10.0.103.0/24"]

  # One NAT gateway, not one per AZ. Three NATs is a production availability
  # choice; a disposable test cluster takes the cheaper single gateway and
  # accepts that losing one AZ's NAT would matter, which for a throwaway it does
  # not.
  enable_nat_gateway = true
  single_nat_gateway = true

  # The tags EKS and its load balancer controller look for to decide which
  # subnets they may place cluster resources in.
  public_subnet_tags = {
    "kubernetes.io/role/elb" = "1"
  }
  private_subnet_tags = {
    "kubernetes.io/role/internal-elb" = "1"
  }
}

module "eks" {
  source  = "terraform-aws-modules/eks/aws"
  version = "~> 20.0"

  cluster_name    = var.cluster_name
  cluster_version = var.kubernetes_version

  # The API server is reachable from the operator's laptop. A private-only
  # endpoint would need a bastion or VPN to run kubectl and helm against, which
  # is the wrong trade for a test cluster.
  cluster_endpoint_public_access = true

  # The identity that runs `terraform apply` gets cluster-admin, so the same
  # operator can immediately run kubectl and helm without a second access-entry
  # dance. This is the whole point of a one-command bring-up.
  enable_cluster_creator_admin_permissions = true

  vpc_id     = module.vpc.vpc_id
  subnet_ids = module.vpc.private_subnets

  # coredns, kube-proxy, and vpc-cni are the baseline. aws-ebs-csi-driver is the
  # one that matters here: it provisions the gp3 volumes the StatefulSets claim,
  # and it authenticates through its own IRSA role rather than the node role.
  cluster_addons = {
    coredns    = {}
    kube-proxy = {}
    vpc-cni    = {}
    aws-ebs-csi-driver = {
      service_account_role_arn = module.ebs_csi_irsa.iam_role_arn
    }
  }

  eks_managed_node_groups = {
    orbita = {
      instance_types = [var.node_instance_type]
      min_size       = var.node_min_size
      max_size       = var.node_max_size
      desired_size   = var.node_desired_size
    }
  }
}

# The EBS CSI driver's role. Enabling IRSA on the cluster gives us an OIDC
# provider; this trades it for a role the driver's kube-system service account
# can assume, carrying the AWS-managed EBS CSI policy. Without it, the gp3
# StorageClass has no permission to create volumes and every claim stays
# Pending.
module "ebs_csi_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.0"

  role_name             = "${var.cluster_name}-ebs-csi"
  attach_ebs_csi_policy = true

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["kube-system:ebs-csi-controller-sa"]
    }
  }
}

# The workload role: what Orbita's pods assume through IRSA
# (credentialSource: web-identity). Its trust policy names exactly one service
# account, orbita/orbita, and it carries exactly the bucket-scoped S3 policy
# defined in s3.tf. The role name is fixed so the Helm overlay's annotation can
# reference it without a fourth thing to fill in.
module "orbita_irsa" {
  source  = "terraform-aws-modules/iam/aws//modules/iam-role-for-service-accounts-eks"
  version = "~> 5.0"

  role_name = var.irsa_role_name

  role_policy_arns = {
    s3 = aws_iam_policy.orbita_s3.arn
  }

  oidc_providers = {
    main = {
      provider_arn               = module.eks.oidc_provider_arn
      namespace_service_accounts = ["${var.namespace}:${var.service_account}"]
    }
  }
}
