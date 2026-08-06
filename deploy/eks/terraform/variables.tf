# Inputs for the disposable EKS test cluster.
#
# Only three ever change in practice: region, bucket_name, and occasionally the
# region's Kubernetes version. Everything else has a default that matches the
# Helm overlay (deploy/helm/orbita/values-eks.yaml) and the docs, and changing
# one means changing the other, so the defaults are the contract.

variable "region" {
  description = "AWS region for the cluster and the bucket. Matches ORBITA_EKS_REGION."
  type        = string
}

variable "bucket_name" {
  description = "S3 bucket for the object store. Created here; emptied and deleted on destroy."
  type        = string
}

variable "cluster_name" {
  description = "EKS cluster name. Fixed by the scripts and docs; change it in all three."
  type        = string
  default     = "orbita-test"
}

variable "kubernetes_version" {
  description = "EKS control-plane version. Pinned so a bring-up next month is the same cluster."
  type        = string
  # Must be a version EKS still creates. AWS drops older versions out of standard
  # support and then refuses to create new clusters on them, so this is not a
  # free-forever pin: it needs a bump when the floor moves. 1.33 is well inside
  # standard support as of this writing, with room before it ages out.
  default = "1.33"
}

# The workload IRSA role. Its name is baked into the Helm overlay's role-arn
# annotation, so it is fixed here to match; the overlay needs only account id,
# region, and bucket filled, and this is why the role name is not one of them.
variable "irsa_role_name" {
  description = "Name of the IRSA role Orbita's pods assume. Must match values-eks.yaml."
  type        = string
  default     = "orbita-test-s3"
}

variable "namespace" {
  description = "Kubernetes namespace the Orbita release runs in."
  type        = string
  default     = "orbita"
}

variable "service_account" {
  description = "Service account the pods run as, and the only one the role trusts."
  type        = string
  default     = "orbita"
}

# The node group, sized for the default chart: three leaders (0.5 vCPU / 1Gi
# requested, 2Gi limit) and three workers (1 vCPU / 4Gi requested, 8Gi limit).
# m5.xlarge is 4 vCPU / 16Gi, and three of them fit that with room for system
# pods and the worker's 8Gi limit, which a smaller node would schedule on
# requests and then let OOM.
variable "node_instance_type" {
  description = "EC2 instance type for the node group."
  type        = string
  default     = "m5.xlarge"
}

variable "node_desired_size" {
  description = "Desired node count. Three so the leader group's soft spread lands one per node."
  type        = number
  default     = 3
}

variable "node_min_size" {
  description = "Minimum node count."
  type        = number
  default     = 3
}

variable "node_max_size" {
  description = "Maximum node count, a little headroom over desired."
  type        = number
  default     = 4
}

variable "object_expiration_days" {
  description = "Backstop lifecycle expiration on the bucket, so a forgotten cluster stops accruing storage."
  type        = number
  default     = 7
}
