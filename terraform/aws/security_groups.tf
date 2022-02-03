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

# PERMIT ALL EGRESS
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


# security group for notebook instance

resource "aws_security_group" "notebook" {
  name        = "bodo-notebook"
  description = "Security group for notebook"
  vpc_id      = data.aws_vpc.bodo_vpc.id

  tags = merge(local.default_tags, {
    Name = "bodo-notebook"
    Role = "notebook"
  })
}

# PERMIT ALL EGRESS 
resource "aws_security_group_rule" "notebook_egress" {
  type              = "egress"
  from_port         = 0
  to_port           = 0
  protocol          = "-1"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.notebook.id
}

# PERMIT ALL - INGRESS on ports 80, 443 and local.jupyter_port
resource "aws_security_group_rule" "notebook_ingress_http" {
  type              = "ingress"
  from_port         = 80
  to_port           = 80
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.notebook.id
}

resource "aws_security_group_rule" "notebook_ingress_https" {
  type              = "ingress"
  from_port         = 443
  to_port           = 443
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.notebook.id
}

resource "aws_security_group_rule" "notebook_ingress_jupyter" {
  type              = "ingress"
  from_port         = local.jupyter_port
  to_port           = local.jupyter_port
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = aws_security_group.notebook.id
}

# Allow user to SSH into the notebook
resource "aws_security_group_rule" "notebook_ingress_ssh" {
  type              = "ingress"
  from_port         = 22
  to_port           = 22
  protocol          = "tcp"
  cidr_blocks       = ["${var.USER_IP}/32"]
  security_group_id = aws_security_group.notebook.id
}


# Allow all from worker to notebook
resource "aws_security_group_rule" "worker_from_notebook" {
  type                     = "ingress"
  protocol                 = "-1"
  from_port                = 0
  to_port                  = 0
  security_group_id        = aws_security_group.notebook.id
  source_security_group_id = aws_security_group.worker.id
}

# Allow all from notebook to workers
resource "aws_security_group_rule" "worker_to_notebook" {
  type                     = "ingress"
  protocol                 = "-1"
  from_port                = 0
  to_port                  = 0
  security_group_id        = aws_security_group.worker.id
  source_security_group_id = aws_security_group.notebook.id
}
