# When creating a new workspace
# There was no attached metastore

# This script creates the metastore
# Now this could also in the future be used for other purposes.

# It is unknown why it is neccesary to use this now.
# It has not been a requirement to explicitly create a metastore.

$dbworkspaceid = az resource show `
  --resource-group $resourceGroupName `
  --name $databricksName `
  --resource-type "Microsoft.Databricks/workspaces" `
  --query properties.workspaceId


$unityCatalog = databricks unity-catalog metastores create `
    --name my-metastore `
    --region $location `
    --storage-root "abfss://silver@$dataLakeName.dfs.core.windows.net/meta"

# Bad way of accessing the catalog id!!
# Please provide a more correct solution.
# The jq utility package could be helpful
# See: https://rajanieshkaushikk.com/2023/01/17/demystifying-azure-databricks-unity-catalog/
$unityCatalogId = (-split $unityCatalog[7])[1] -replace '"', "" -replace ",", ""

databricks unity-catalog metastores assign `
    --workspace-id $dbworkspaceid.Replace('"',"") `
    --metastore-id $unityCatalogId `
    --default-catalog-name main