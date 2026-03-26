variable "location" {
  description = "Azure region for resources"
  type        = string
  default     = "East US"
}

variable "resource_group_name" {
  description = "Resource group name"
  type        = string
  default     = "rg-sp-mobility-dev"
}

variable "storage_account_name" {
  description = "Globally unique storage account name"
  type        = string
  default     = "stspmobilitydev001dev001"
}

variable "key_vault_name" {
  description = "Globally unique Key Vault name"
  type        = string
  default     = "kv-sp-mobility-dev-001"
}

variable "databricks_workspace_name" {
  description = "Azure Databricks workspace name"
  type        = string
  default     = "dbw-sp-mobility-dev"
}
