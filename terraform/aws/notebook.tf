resource "random_uuid" "jupyter_token" {
}


# bootstrap script for worker instances (ssh keys, ...)
data "template_file" "userdata_notebook" {
  template = file("templates/userdata_notebook.sh.tpl")

  vars = {
    SSH_PUBLIC_KEY  = tls_private_key.ssh_key.public_key_openssh
    SSH_PRIVATE_KEY = tls_private_key.ssh_key.private_key_pem
    MACHINEFILE     = "${join("\n", aws_instance.worker.*.private_dns)}"
    JUPYTER_TOKEN   = random_uuid.jupyter_token.result
    JUPYTER_PORT    = local.jupyter_port
  }
}

# Notebook Launch Configuration
resource "aws_launch_template" "bodo_notebook_template" {
  name     = "Bodo_Notebook_Config"
  image_id = var.AMI_ID
  # TODO Choose a different type?
  instance_type = var.CLUSTER_INSTANCE_TYPE
  user_data     = base64encode(data.template_file.userdata_notebook.rendered)

  placement {
    group_name = aws_placement_group.bodo.id
  }

  network_interfaces {
    subnet_id       = data.aws_subnet.bodo_worker_subnet.id
    security_groups = [aws_security_group.notebook.id]
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
        Name = "bodo-notebook",
        Role = "notebook",
      },
    )
  }

  tags = merge(local.default_tags,
    {
      Name = "bodo-notebook",
      Role = "notebook",
    },
  )
}

# Notebook Instances

resource "aws_instance" "notebook" {
  placement_group = aws_placement_group.bodo.name
  launch_template {
    id      = aws_launch_template.bodo_notebook_template.id
    version = "$Latest"
  }
}
