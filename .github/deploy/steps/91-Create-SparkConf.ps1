
$repoRoot = git rev-parse --show-toplevel

Write-Host "Write cluster configuration for Direct Access..." -ForegroundColor DarkYellow
$confDirectAccess = [ordered]@{}

$confDirectAccess["spark.databricks.cluster.profile"]= "singleNode"
$confDirectAccess["spark.databricks.delta.preview.enabled"] = $true
$confDirectAccess["spark.databricks.io.cache.enabled"] = $true
$confDirectAccess["spark.master"]= "local[*, 4]"

Write-Host "  Adds Direct Access for $resourceName..." -ForegroundColor DarkYellow
$confDirectAccess["fs.azure.account.auth.type.$resourceName.dfs.core.windows.net"] = "OAuth"
$confDirectAccess["fs.azure.account.oauth.provider.type.$resourceName.dfs.core.windows.net"] = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
$confDirectAccess["fs.azure.account.oauth2.client.id.$resourceName.dfs.core.windows.net"] = "{{secrets/secrets/DatabricksClientId}}"
$confDirectAccess["fs.azure.account.oauth2.client.endpoint.$resourceName.dfs.core.windows.net"] = "{{secrets/secrets/DatabricksOauthEndpoint}}"
$confDirectAccess["fs.azure.account.oauth2.client.secret.$resourceName.dfs.core.windows.net"] = "{{secrets/secrets/DatabricksClientSecret}}"

Set-Content $repoRoot\.github\submit\sparkconf.json ($confDirectAccess | ConvertTo-Json)
