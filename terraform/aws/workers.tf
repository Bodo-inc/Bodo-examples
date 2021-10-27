

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

# bootstrap script for worker instances (ssh keys, ...)
data "template_file" "userdata_worker" {
  template = file("templates/userdata_worker.sh.tpl")

  vars = {
    SSH_PUBLIC_KEY     = tls_private_key.ssh_key.public_key_openssh
    SSH_PRIVATE_KEY    = tls_private_key.ssh_key.private_key_pem
  }
}

# Worker Launch Configuration
resource "aws_launch_template" "bodo_worker_template" {
  name          = "Bodo_Worker_Config"
  image_id      = var.AMI_ID
  instance_type = var.CLUSTER_INSTANCE_TYPE
  user_data     = base64encode(data.template_file.userdata_worker.rendered)

  # TODO
  # iam_instance_profile {
  #  name = aws_iam_instance_profile.cluster.name
  # }

  placement {
    group_name = aws_placement_group.bodo.id
  }
  
  network_interfaces {
    subnet_id       = data.aws_subnet.bodo_worker_subnet.id
    security_groups = [aws_security_group.worker.id]
    interface_type  = var.EFA_ENABLED ? "efa" : null
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
        Name        = "bodo-worker",
        Role        = "worker",
        EFA         = var.EFA_ENABLED,
      },
    )
  }

  tags = merge(local.default_tags,
    {
      Name        = "bodo-worker",
      Role        = "worker",
      EFA         = var.EFA_ENABLED,
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


# Create machinefile on all the instances
resource "null_resource" "machinefile" {
  count = var.CLUSTER_MEMBERS_COUNT

  triggers = {
    cluster_private_ips = "${join(",", aws_instance.worker.*.private_dns)}"
  }

  connection {
    type        = "ssh"
    user        = "ubuntu"
    private_key = tls_private_key.ssh_key.private_key_pem
    timeout     = "2m"
    host        = element(aws_instance.worker.*.public_ip, count.index)
  }

  provisioner "remote-exec" {
    inline = [<<EOF
    WORKER_IPS="${join(" ", aws_instance.worker.*.private_dns)}"
    for NODE in $WORKER_IPS; do
      echo "$NODE" >> /home/ubuntu/machinefile
    done
    EOF
    ]
  }

}
