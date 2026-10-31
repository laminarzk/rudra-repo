# laminar:node:imp-azure-blob_store-13:begin
# ---------------------------------------------------------------------------
# Customer Azure Blob Destination (customer-owned, excluded from pipeline
# allowlist per SCRUM-485). The storage account and container are NOT created
# here. Terraform references them read-only and fails the plan if the customer
# account does not meet the baseline hardening standard (fail-closed).
# ---------------------------------------------------------------------------

locals {
  customer_azure_blob_destination_blob_store_scope = "${data.azurerm_storage_account.customer_azure_blob_destination_blob_store.id}/blobServices/default/containers/${local.cfg.customer_azure_blob_destination_blob_store.container_name}"
}

data "azurerm_storage_account" "customer_azure_blob_destination_blob_store" {
  name                = local.cfg.customer_azure_blob_destination_blob_store.storage_account_name
  resource_group_name = local.cfg.customer_azure_blob_destination_blob_store.storage_account_resource_group

  lifecycle {
    postcondition {
      condition     = self.https_traffic_only_enabled == true
      error_message = "Customer storage account must have Secure transfer required (HTTPS only) enabled."
    }
    postcondition {
      condition     = self.allow_nested_items_to_be_public == false
      error_message = "Customer storage account must have public anonymous access disabled."
    }
    postcondition {
      condition     = self.shared_access_key_enabled == false
      error_message = "Customer storage account must disable shared account keys so that Entra ID RBAC is the only authorization path."
    }
    postcondition {
      condition     = self.min_tls_version == "TLS1_2"
      error_message = "Customer storage account must enforce a minimum TLS version of TLS1_2."
    }
  }
}

data "azurerm_storage_container" "customer_azure_blob_destination_blob_store" {
  name                 = local.cfg.customer_azure_blob_destination_blob_store.container_name
  storage_account_name = data.azurerm_storage_account.customer_azure_blob_destination_blob_store.name

  lifecycle {
    postcondition {
      condition     = self.container_access_type == "private"
      error_message = "Customer blob container must have anonymous access set to private."
    }
  }
}

# Acme-side workload identity (Entra ID). No client secret or SAS token is created,
# so no credential material is written to Terraform state.
resource "azuread_application" "customer_azure_blob_destination_blob_store_app" {
  display_name     = "${var.project_name}-${var.env}-customer-azure-blob-destination-blob-store"
  sign_in_audience = "AzureADMyOrg"
}

resource "azuread_service_principal" "customer_azure_blob_destination_blob_store" {
  client_id                    = azuread_application.customer_azure_blob_destination_blob_store_app.client_id
  app_role_assignment_required = true
}

# Federated credential: the Transform & Encrypt DAG (MWAA) exchanges its own
# OIDC token for an Entra access token. Only the configured issuer and subject
# can obtain a token for this application.
resource "azuread_application_federated_identity_credential" "customer_azure_blob_destination_blob_store_fic" {
  application_id = azuread_application.customer_azure_blob_destination_blob_store_app.id
  display_name   = "${var.project_name}-${var.env}-transform-encrypt-dag"
  description    = "Federated identity for the Transform & Encrypt DAG pushing packages to the customer Azure Blob destination."
  audiences      = ["api://AzureADTokenExchange"]
  issuer         = local.cfg.customer_azure_blob_destination_blob_store.workload_identity_issuer
  subject        = local.cfg.customer_azure_blob_destination_blob_store.workload_identity_subject
}

# Least-privilege custom role: blob read (for SHA-256 read-back verification)
# and blob write only. No delete, no container-management, no key listing.
resource "azurerm_role_definition" "customer_azure_blob_destination_blob_store_role_def" {
  name              = "${var.project_name}-${var.env}-customer-azure-blob-destination-blob-store-writer"
  scope             = local.customer_azure_blob_destination_blob_store_scope
  description       = "Write and read-back blobs in one customer container. No delete or management rights."
  assignable_scopes = [local.customer_azure_blob_destination_blob_store_scope]

  permissions {
    actions = []
    data_actions = [
      "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/read",
      "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/write",
    ]
  }
}

# Scoped to the single container, not the account.
resource "azurerm_role_assignment" "customer_azure_blob_destination_blob_store_role_assignment" {
  scope                            = local.customer_azure_blob_destination_blob_store_scope
  role_definition_id               = azurerm_role_definition.customer_azure_blob_destination_blob_store_role_def.role_definition_resource_id
  principal_id                     = azuread_service_principal.customer_azure_blob_destination_blob_store.object_id
  principal_type                   = "ServicePrincipal"
  skip_service_principal_aad_check = true
}

output "customer_azure_blob_destination_blob_store_client_id" {
  value = azuread_application.customer_azure_blob_destination_blob_store_app.client_id
}

output "customer_azure_blob_destination_blob_store_container_url" {
  value = "${data.azurerm_storage_account.customer_azure_blob_destination_blob_store.primary_blob_endpoint}${local.cfg.customer_azure_blob_destination_blob_store.container_name}"
}

output "customer_azure_blob_destination_blob_store_principal_object_id" {
  value = azuread_service_principal.customer_azure_blob_destination_blob_store.object_id
}

output "customer_azure_blob_destination_blob_store_role_assignment_scope" {
  value = local.customer_azure_blob_destination_blob_store_scope
}
# laminar:node:imp-azure-blob_store-13:end
