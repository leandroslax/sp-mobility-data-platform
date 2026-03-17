module "resource_group" {
  source   = "../../modules/resource_group"
  name     = var.resource_group_name
  location = var.location
}

module "storage" {
  source               = "../../modules/storage"
  storage_account_name = var.storage_account_name
  resource_group_name  = module.resource_group.name
  location             = module.resource_group.location
  containers           = ["landing", "bronze", "silver", "gold", "checkpoint"]

  tags = {
    environment = "dev"
    project     = "sp-mobility-data-platform"
    managed_by  = "terraform"
  }
}

module "keyvault" {
  source              = "../../modules/keyvault"
  key_vault_name      = var.key_vault_name
  resource_group_name = module.resource_group.name
  location            = module.resource_group.location

  tags = {
    environment = "dev"
    project     = "sp-mobility-data-platform"
    managed_by  = "terraform"
  }
}
