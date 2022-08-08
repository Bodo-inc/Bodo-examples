# cluster security group (SSH)
resource "azurerm_network_security_group" "bodo-cluster-sg" {

  name                = "bodoClusterNetworkSecurityGroup"
  location            = var.LOCATION
  resource_group_name = azurerm_resource_group.bodo_resource_group.name

  security_rule {
    name                       = "SSH_in"
    priority                   = 1001
    direction                  = "Inbound"
    access                     = "Allow"
    protocol                   = "Tcp"
    source_port_range          = "*"
    destination_port_range     = "22"
    source_address_prefixes    = var.USER_IP == "" ? [] : [var.USER_IP]
    destination_address_prefix = "*"
  }

  security_rule {
    name                       = "SSH_out"
    priority                   = 1002
    direction                  = "Outbound"
    access                     = "Allow"
    protocol                   = "*"
    source_port_range          = "*"
    destination_port_range     = "*"
    source_address_prefix      = "*"
    destination_address_prefix = "*"
  }

  tags = local.default_tags
}
