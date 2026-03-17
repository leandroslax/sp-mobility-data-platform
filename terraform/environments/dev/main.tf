resource "azurerm_resource_group" "sp_mobility" {
  name     = var.resource_group_name
  location = var.location
}
