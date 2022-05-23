. $PSScriptRoot/steps/00-Config.ps1

$tenantId = (az account show --query tenantId --out tsv)
$authAppId = "069f8930-011e-4361-b065-ab63b120b351"
$workspaceUrl = az resource show `
  --resource-group $resourceGroupName `
  --name $databricksName `
  --resource-type "Microsoft.Databricks/workspaces" `
  --query properties.workspaceUrl `
  --out tsv

atc_az_databricks_token `
  --tenantId $tenantId `
  --appId $authAppId`
  --workspaceUrl $workspaceUrl
