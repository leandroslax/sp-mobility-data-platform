TERRAFORM_DEV_DIR=terraform/environments/dev
TERRAFORM_PROD_DIR=terraform/environments/prod

terraform-fmt:
	terraform fmt -recursive

terraform-dev-init:
	cd $(TERRAFORM_DEV_DIR) && terraform init

terraform-dev-validate:
	cd $(TERRAFORM_DEV_DIR) && terraform validate

terraform-dev-plan:
	cd $(TERRAFORM_DEV_DIR) && terraform plan

terraform-dev-apply:
	cd $(TERRAFORM_DEV_DIR) && terraform apply

terraform-prod-init:
	cd $(TERRAFORM_PROD_DIR) && terraform init

terraform-prod-validate:
	cd $(TERRAFORM_PROD_DIR) && terraform validate

terraform-prod-plan:
	cd $(TERRAFORM_PROD_DIR) && terraform plan
