
# cluster virtual network (VPC equivalent)
resource "azurerm_virtual_network" "bodo-network" {
  name                = "bodoVnet"
  address_space       = ["10.30.0.0/16"]
  location            = var.LOCATION
  resource_group_name = azurerm_resource_group.bodo_resource_group.name

  tags = merge(local.default_tags, { "Name" = "BodoAi VNet" })
}


# cluster subnet
resource "azurerm_subnet" "bodo-subnet-public" {
  name                 = "bodoSubnetPublic"
  resource_group_name  = azurerm_resource_group.bodo_resource_group.name
  virtual_network_name = azurerm_virtual_network.bodo-network.name
  address_prefixes     = ["10.30.100.0/24"]
}

# cluster public ip
resource "azurerm_public_ip" "bodo-cluster-public-ip" {
  count = var.CLUSTER_MEMBERS_COUNT

  name                = "bodoClusterPublicIP_${count.index}"
  location            = var.LOCATION
  resource_group_name = azurerm_resource_group.bodo_resource_group.name
  allocation_method   = "Dynamic"
  tags                = local.default_tags
}

# cluster network interface
resource "azurerm_network_interface" "bodo-cluster-nic" {
  count = var.CLUSTER_MEMBERS_COUNT

  name                          = "bodoClusterNIC_${count.index}"
  location                      = var.LOCATION
  resource_group_name           = azurerm_resource_group.bodo_resource_group.name
  enable_accelerated_networking = var.ENABLE_ACCELERATED_NETWORKING

  ip_configuration {
    name                          = "bodoClusterNicConfiguration_${count.index}"
    subnet_id                     = azurerm_subnet.bodo-subnet-public.id
    private_ip_address_allocation = "Dynamic"
    public_ip_address_id          = azurerm_public_ip.bodo-cluster-public-ip[count.index].id
  }

  tags = local.default_tags
}

# Connect the security group to the network interface
resource "azurerm_network_interface_security_group_association" "sg-nic-assoc" {
  count = var.CLUSTER_MEMBERS_COUNT

  network_interface_id      = azurerm_network_interface.bodo-cluster-nic[count.index].id
  network_security_group_id = azurerm_network_security_group.bodo-cluster-sg.id
}
