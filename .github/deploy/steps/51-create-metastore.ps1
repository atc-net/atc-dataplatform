$dbworkspaceid = az resource show `
  --resource-group $resourceGroupName `
  --name $databricksName `
  --resource-type "Microsoft.Databricks/workspaces" `
  --query properties.workspaceId


$unityCatalog = databricks unity-catalog metastores create `
    --name my-metastore `
    --region $location `
    --storage-root "abfss://silver@"+$dataLakeName+".dfs.core.windows.net/meta"

databricks unity-catalog metastores assign `
    --workspace-id $dbworkspaceid `
    --metastore-id $unityCatalog.metastore_id `
    --default-catalog-name main