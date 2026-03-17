output "resource_group_name" {
  value = module.resource_group.name
}

output "resource_group_location" {
  value = module.resource_group.location
}

output "resource_group_id" {
  value = module.resource_group.id
}

output "storage_account_name" {
  value = module.storage.storage_account_name
}

output "storage_account_id" {
  value = module.storage.storage_account_id
}

output "storage_primary_dfs_endpoint" {
  value = module.storage.primary_dfs_endpoint
}

output "storage_containers" {
  value = module.storage.container_names
}

output "key_vault_id" {
  value = module.keyvault.key_vault_id
}

output "key_vault_name" {
  value = module.keyvault.key_vault_name
}

output "key_vault_uri" {
  value = module.keyvault.vault_uri
}
