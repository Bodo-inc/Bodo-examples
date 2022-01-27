# Verify that the provided VPC_ID and SUBNET_ID are valid

data "aws_vpc" "bodo_vpc" {
  id = var.VPC_ID
}

data "aws_subnet" "bodo_worker_subnet" {
  vpc_id = data.aws_vpc.bodo_vpc.id
  id     = var.SUBNET_ID
}

