# security groups for customer cluster workers

# security group for workers
resource "aws_security_group" "worker" {
  name        = "bodo-worker"
  description = "Security group for worker nodes"
  vpc_id      = data.aws_vpc.bodo_vpc.id

  tags = merge(local.default_tags, {
    Name = "bodo-worker"
    Role = "worker"
  })
}

# PERMIT ALL - EGRESS ALL 
resource "aws_security_group_rule" "worker_egress_all" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.worker.id
}

# ALLOW ALL TO ITSELF
resource "aws_security_group_rule" "worker_egress_self" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  self              = true
  security_group_id = aws_security_group.worker.id
}

# ALLOW ALL FROM ITSELF
resource "aws_security_group_rule" "worker_ingress_self" {
  type              = "ingress"
  protocol          = "-1"
  from_port         = 0
  to_port           = 0
  self              = true
  security_group_id = aws_security_group.worker.id
}

# ALLOW SSH FROM USER IP
resource "aws_security_group_rule" "worker_ingress_ssh_customer" {
  count             = var.USER_IP != "" ? 1 : 0
  type              = "ingress"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["${var.USER_IP}/32"]
  security_group_id = aws_security_group.worker.id
}
