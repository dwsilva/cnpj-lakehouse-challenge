# Policy tags para colunas sensíveis (BigQuery).
# Uso: terraform apply -var="project_id=SEU_PROJETO"
# IDs gerados vão para as vars do dbt em produção (dbt_project.yml).

terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us"
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_data_catalog_taxonomy" "pii" {
  display_name           = "cnpj_lakehouse_pii"
  activated_policy_types   = ["FINE_GRAINED_ACCESS_CONTROL"]
  region                 = var.region
}

resource "google_data_catalog_policy_tag" "cnpj_cpf" {
  taxonomy     = google_data_catalog_taxonomy.pii.id
  display_name = "cnpj_cpf"
  description  = "CNPJ completo, raiz ou CPF/CNPJ de socio (mascarado na RF)."
}

resource "google_data_catalog_policy_tag" "nome" {
  taxonomy     = google_data_catalog_taxonomy.pii.id
  display_name = "nome_pessoa_empresa"
  description  = "Razao social, nome fantasia, nome de socio."
}

resource "google_data_catalog_policy_tag" "endereco" {
  taxonomy     = google_data_catalog_taxonomy.pii.id
  display_name = "endereco"
  description  = "CEP e demais campos de localizacao quando expostos."
}

output "policy_tag_cnpj_cpf" {
  value = google_data_catalog_policy_tag.cnpj_cpf.name
}

output "policy_tag_nome" {
  value = google_data_catalog_policy_tag.nome.name
}

output "policy_tag_endereco" {
  value = google_data_catalog_policy_tag.endereco.name
}
