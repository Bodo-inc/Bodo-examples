# cluster placement group
resource "azurerm_proximity_placement_group" "bodo-placement-group" {
  name                = "bodoClusterProximityPlacementGroup"
  location            = var.LOCATION
  resource_group_name = azurerm_resource_group.bodo_resource_group.name

  tags = merge(local.default_tags, {
    Name = "bodo-placement-group"
  })
}

# availability set for this cluster
resource "azurerm_availability_set" "bodo-availability-set" {
  name                         = "bodoAvailabilitySet"
  location                     = var.LOCATION
  resource_group_name          = azurerm_resource_group.bodo_resource_group.name
  proximity_placement_group_id = azurerm_proximity_placement_group.bodo-placement-group.id

  tags = merge(local.default_tags, {
    Name = "bodo-availability-set"
  })
}

# ssh key
resource "tls_private_key" "ssh-key" {
  algorithm = "RSA"
  rsa_bits  = 4096
}


# cluster VMs
resource "azurerm_linux_virtual_machine" "bodo-cluster-vm" {
  count = var.CLUSTER_MEMBERS_COUNT

  name                  = "bodoClusterVM_${count.index}"
  location              = var.LOCATION
  resource_group_name   = azurerm_resource_group.bodo_resource_group.name
  network_interface_ids = [azurerm_network_interface.bodo-cluster-nic[count.index].id]
  size                  = var.CLUSTER_INSTANCE_TYPE
  availability_set_id   = azurerm_availability_set.bodo-availability-set.id
  custom_data = base64encode(templatefile("templates/userdata_worker.sh.tpl", {
    WORKER_IPS      = join(" ", azurerm_network_interface.bodo-cluster-nic.*.private_ip_address)
    SSH_PUBLIC_KEY  = tls_private_key.ssh-key.public_key_openssh
    SSH_PRIVATE_KEY = tls_private_key.ssh-key.private_key_pem
  }))
  source_image_id                 = var.IMAGE_ID
  computer_name                   = "bodovm${count.index}"
  admin_username                  = "azureuser"
  disable_password_authentication = true

  os_disk {
    name                 = "bodoOsDisk_${count.index}"
    caching              = "ReadWrite"
    storage_account_type = "Premium_LRS"
  }

  admin_ssh_key {
    username   = "azureuser"
    public_key = tls_private_key.ssh-key.public_key_openssh
  }

  tags = merge(local.default_tags, {
    Name = format("%s-%d", "bodoai-mpi-worker", count.index + 1)
    Role = "worker",
  })
}
