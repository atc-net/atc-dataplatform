$dbworkspaceid = az resource show `
  --resource-group $resourceGroupName `
  --name $databricksName `
  --resource-type "Microsoft.Databricks/workspaces" `
  --query properties.workspaceId


$unityCatalog = databricks unity-catalog metastores create `
    --name my-metastore `
    --region $location `
    --storage-root "abfss://silver@"+$dataLakeName+".dfs.core.windows.net/meta" `
    | jq '.metastore_id'

#Bad way of accessing the catalog id
# The jq utility package could be helpful
# See: https://rajanieshkaushikk.com/2023/01/17/demystifying-azure-databricks-unity-catalog/
$unityCatalogId = (-split $unityCatalog[7])[1] -replace '"', "" -replace ",", ""

databricks unity-catalog metastores assign `
    --workspace-id $dbworkspaceid `
    --metastore-id $unityCatalogId `
    --default-catalog-name main