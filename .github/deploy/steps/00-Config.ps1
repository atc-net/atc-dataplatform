
$base_name         = "atc"

$permanentResourceGroup       = "$base_name-permanent"

$resourceGroupName            = "$base_name-integration"

$resourceName                 = "github$base_name"

$databricksName               = $resourceName
$dataLakeName                 = $resourceName
$databaseServerName           = $resourceName + "test"
$deliveryDatabase             = "Delivery"
$ehNamespace                  = $resourceName
$mountSpnName                 = "AtcMountSpn"
$dbDeploySpnName                    = "AtcDbSpn"
$cicdSpnName                    = "AtcGithubPipe"
$cosmosName                    = $resourceName
$keyVaultName                 = "atcGithubCiCd"

$location = "westeurope"  # Use eastus because of free azure subscription
$resourceTags = @(
  "Owner=Auto Deployed",
  "System=ATC-NET",
  "Service=Data Platform"
  )

$dataLakeContainers = @(
    @{name="silver"}
)

$eventHubConfig = @(
    @{
      name="atceh"
      namespace=$ehNamespace
      captureLocation = "silver"
    }
)

Write-Host "**********************************************************************" -ForegroundColor White
Write-Host "* Base Configuration       *******************************************" -ForegroundColor White
Write-Host "**********************************************************************" -ForegroundColor White
Write-Host "* Resource Group                  : $resourceGroupName" -ForegroundColor White
Write-Host "* Permanent Resource Group                  : $permanentResourceGroup" -ForegroundColor White
Write-Host "* location                        : $location" -ForegroundColor White
Write-Host "* Azure Databricks Workspace      : $databricksName" -ForegroundColor White
Write-Host "* Azure Data Lake                 : $dataLakeName" -ForegroundColor White
Write-Host "* Azure SQL server                : $databaseServerName" -ForegroundColor White
Write-Host "* Azure SQL database              : $deliveryDatabase" -ForegroundColor White
Write-Host "* Azure EventHubs Namespace       : $ehNamespace" -ForegroundColor White
Write-Host "* Azure CosmosDb name             : $cosmosName" -ForegroundColor White
Write-Host "* Mounting SPN Name               : $mountSpnName" -ForegroundColor White
Write-Host "**********************************************************************" -ForegroundColor White



$secrets = [DatabricksSecretsManager]::new()
$values = [DatabricksSecretsManager]::new()
