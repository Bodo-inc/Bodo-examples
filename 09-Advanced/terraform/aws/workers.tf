# placement group for cluster workers
resource "aws_placement_group" "bodo" {
  name     = "bodo-placement-group"
  strategy = "cluster"
  tags = merge(local.default_tags, {
    Name = "bodo-placement-group"
  })
}

# ssh key
resource "tls_private_key" "ssh_key" {
  algorithm = "RSA"
  rsa_bits  = 4096
}

# Worker Launch Configuration
resource "aws_launch_template" "bodo_worker_template" {
  name          = "Bodo_Worker_Config"
  image_id      = var.AMI_ID
  instance_type = var.CLUSTER_INSTANCE_TYPE
  user_data     = base64encode(templatefile("templates/userdata_worker.sh.tpl", {
    SSH_PUBLIC_KEY  = tls_private_key.ssh_key.public_key_openssh,
    SSH_PRIVATE_KEY = tls_private_key.ssh_key.private_key_pem
  }))

  placement {
    group_name = aws_placement_group.bodo.id
  }

  network_interfaces {
    subnet_id       = data.aws_subnet.bodo_worker_subnet.id
    security_groups = [aws_security_group.worker.id]
  }

  block_device_mappings {
    device_name = "/dev/xvda"
    ebs {
      volume_type = "gp2"
      volume_size = 16
    }
  }

  tag_specifications {
    resource_type = "instance"
    tags = merge(local.default_tags,
      {
        Name = "bodo-worker",
        Role = "worker",
      },
    )
  }

  tags = merge(local.default_tags,
    {
      Name = "bodo-worker",
      Role = "worker",
    },
  )
}

# Worker Instances

resource "aws_instance" "worker" {
  count           = var.CLUSTER_MEMBERS_COUNT
  placement_group = aws_placement_group.bodo.name
  launch_template {
    id      = aws_launch_template.bodo_worker_template.id
    version = "$Latest"
  }
}
