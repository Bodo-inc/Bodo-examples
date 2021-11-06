# Default region for whole cluster
variable "LOCATION" {
  type    = string
  default = "eastus"
}

# Number of member instances to provision.
variable "CLUSTER_MEMBERS_COUNT" {
  type    = number
  default = 2
}

# Instance type for MPI cluster.
variable "CLUSTER_INSTANCE_TYPE" {
  type    = string
  default = "Standard_D4as_v4"
}

# Azure Subscription ID
variable "AZ_SUBSCRIPTION_ID" {
  type = string
}

# IP to allow SSH from
variable "USER_IP" {
}

# Image to use
variable "IMAGE_ID" {
}
