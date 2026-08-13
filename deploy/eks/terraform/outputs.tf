# What the bring-up script reads back after an apply. up.sh uses irsa_role_arn
# to check the role the Helm overlay names actually exists, and the kubeconfig
# command to point kubectl at the new cluster.

output "cluster_name" {
  description = "EKS cluster name, for aws eks update-kubeconfig."
  value       = module.eks.cluster_name
}

output "region" {
  description = "Region the cluster and bucket live in."
  value       = var.region
}

output "irsa_role_arn" {
  description = "ARN of the workload role Orbita's pods assume. Matches the Helm overlay annotation."
  value       = module.orbita_irsa.iam_role_arn
}

output "bucket" {
  description = "The object store bucket."
  value       = aws_s3_bucket.data.bucket
}

output "update_kubeconfig_command" {
  description = "Point kubectl at the cluster."
  value       = "aws eks update-kubeconfig --name ${module.eks.cluster_name} --region ${var.region}"
}
