terraform-init:
	cd terraform/environments/dev && terraform init

terraform-plan:
	cd terraform/environments/dev && terraform plan

terraform-apply:
	cd terraform/environments/dev && terraform apply

terraform-fmt:
	terraform fmt -recursive
