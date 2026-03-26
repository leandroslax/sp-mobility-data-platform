variable "storage_account_name" {
  description = "Globally unique Azure Storage Account name"
  type        = string
}

variable "resource_group_name" {
  description = "Resource Group name"
  type        = string
}

variable "location" {
  description = "Azure region"
  type        = string
}

variable "containers" {
  description = "List of ADLS Gen2 containers"
  type        = list(string)
}

variable "tags" {
  description = "Tags applied to resources"
  type        = map(string)
  default     = {}
}
