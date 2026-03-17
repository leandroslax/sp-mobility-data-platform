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
  default     = "stspmobilitydev001"
}
