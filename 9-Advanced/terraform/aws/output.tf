output "notebook_ip" {
  value = aws_instance.notebook.public_ip
}

output "jupyter_link" {
  value = "${aws_instance.notebook.public_ip}:${local.jupyter_port}/lab?token=${random_uuid.jupyter_token.result}"
}

# Write private key to pem file
resource "local_file" "cloud_pem" {
  filename        = "${path.module}/bodo_cluster_ssh.pem"
  content         = tls_private_key.ssh_key.private_key_pem
  file_permission = "0400"
}

output "jupyter_id" {
  value = aws_instance.notebook.id
}

output "worker_id" {
  value = aws_instance.worker.*.id
}
