
provider "azurerm" {
  subscription_id = var.AZ_SUBSCRIPTION_ID
  features {}
}

# resource group
resource "azurerm_resource_group" "bodo_resource_group" {
  name     = "Bodo-Example-RG"
  location = var.LOCATION
}
