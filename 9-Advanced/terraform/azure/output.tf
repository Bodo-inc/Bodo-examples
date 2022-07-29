output "cluster_ips" {
  value = azurerm_linux_virtual_machine.bodo-cluster-vm.*.public_ip_address
}

# Write private key to pem file
resource "local_file" "cloud_pem" {
  filename        = "${path.module}/bodo_cluster_ssh.pem"
  content         = tls_private_key.ssh-key.private_key_pem
  file_permission = "0400"
}
