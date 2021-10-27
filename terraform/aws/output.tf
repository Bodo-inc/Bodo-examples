output "cluster_ips" {
  value = aws_instance.worker.*.public_ip
}

# Write private key to pem file
resource "local_file" "cloud_pem" { 
  filename = "${path.module}/bodo_cluster_ssh.pem"
  content = tls_private_key.ssh_key.private_key_pem
  file_permission = "0400"
}
