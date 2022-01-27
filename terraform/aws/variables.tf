# Default region for whole cluster
variable "AWS_DEFAULT_REGION" {
  type    = string
  default = "us-east-1"
}

# Number of member instances to provision.
variable "CLUSTER_MEMBERS_COUNT" {
  type    = number
  default = 2
}

# Instance type for MPI cluster.
variable "CLUSTER_INSTANCE_TYPE" {
  type    = string
  default = "c5n.xlarge"
}

# Instance type for notebook.
variable "NOTEBOOK_INSTANCE_TYPE" {
  type    = string
  default = "c5n.xlarge"
}

# IP to allow SSH from
variable "USER_IP" {
}

# Image to use
variable "AMI_ID" {
}

# VPC to deploy in
variable "VPC_ID" {
}

# Subnet to deploy in
variable "SUBNET_ID" {
}
