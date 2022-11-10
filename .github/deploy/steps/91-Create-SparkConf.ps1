
Write-Host "Write cluster configuration for Direct Access..." -ForegroundColor DarkYellow

$confDirectAccess = [ordered]@{}

$confDirectAccess["spark.databricks.cluster.profile"]= "singleNode"
$confDirectAccess["spark.databricks.delta.preview.enabled"] = $true
$confDirectAccess["spark.databricks.io.cache.enabled"] = $true
$confDirectAccess["spark.master"]= "local[*, 4]"


$url = az storage account show --name $dataLakeName --resource-group $resourceGroupName  --query "primaryEndpoints.dfs" --out tsv

$storageUrl = ([System.Uri]$url).Host

Write-Host "  Adds Direct Access for $storageUrl..." -ForegroundColor DarkYellow

$confDirectAccess["fs.azure.account.auth.type.$storageUrl"] = "OAuth"
$confDirectAccess["fs.azure.account.oauth.provider.type.$storageUrl"] = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
$confDirectAccess["fs.azure.account.oauth2.client.id.$storageUrl"] = "{{secrets/secrets/DatabricksClientId}}"
$confDirectAccess["fs.azure.account.oauth2.client.endpoint.$storageUrl"] = "{{secrets/secrets/DatabricksOauthEndpoint}}"
$confDirectAccess["fs.azure.account.oauth2.client.secret.$storageUrl"] = "{{secrets/secrets/DatabricksClientSecret}}"

$values.addSecret("StorageAccount--Url", $storageUrl)

Set-Content $repoRoot\.github\submit\sparkconf.json ($confDirectAccess | ConvertTo-Json)
