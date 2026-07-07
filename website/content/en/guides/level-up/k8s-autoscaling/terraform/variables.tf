variable "region" {
  type    = string
  default = "us-east-1"
}

variable "cluster_suffix" {
  type        = string
  description = "Optional suffix appended to cluster resource names (e.g. your username); set this if the default \"vector-perf\" name might collide with another concurrent reproduction in the same AWS account"
  default     = ""
}

variable "node_instance_type" {
  type    = string
  # c5.4xlarge: 16 vCPU, 32 GiB
  # Phase 3 (8-worker) CPU requests: 8×1000m Vector + 5×100m producers + 500m consumer + ~200m system ≈ 9.2 vCPU
  default = "c5.4xlarge"
}

variable "my_cidr" {
  type        = string
  description = "CIDR to allow SSH access to the K3s instance (e.g. 1.2.3.4/32)"
}

variable "ssh_public_key_path" {
  type    = string
  default = "~/.ssh/vector_tests.pub"
}

variable "ssh_private_key_path" {
  type    = string
  default = "~/.ssh/vector_tests"
}
