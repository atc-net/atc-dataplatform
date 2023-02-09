# Creating direct access
Write-Host "Write cluster configuration for Direct Access..." -ForegroundColor DarkYellow
$confDirectAccess = [ordered]@{}
$confDirectAccess["spark.databricks.delta.preview.enabled"] = $true
$confDirectAccess["spark.databricks.io.cache.enabled"] = $true
$confDirectAccess["spark.master"]= "local[*, 4]"
$confDirectAccess["spark.databricks.cluster.profile"]= "singleNode"

$confDirectAccess["fs.azure.account.oauth2.client.id.$dataLakeName.dfs.core.windows.net"] = "{{secrets/secrets/Databricks--ClientId}}"
$confDirectAccess["fs.azure.account.oauth2.client.endpoint.$dataLakeName.dfs.core.windows.net"] = "{{secrets/secrets/Databricks--OauthEndpoint}}"
$confDirectAccess["fs.azure.account.oauth.provider.type.$dataLakeName.dfs.core.windows.net"] = "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
$confDirectAccess["fs.azure.account.oauth2.client.secret.$dataLakeName.dfs.core.windows.net"] = "{{secrets/secrets/Databricks--ClientSecret}}"
$confDirectAccess["fs.azure.account.auth.type.$dataLakeName.dfs.core.windows.net"] = "OAuth"

Set-Content $repoRoot\.github\submit\sparkconf.json ($confDirectAccess | ConvertTo-Json)