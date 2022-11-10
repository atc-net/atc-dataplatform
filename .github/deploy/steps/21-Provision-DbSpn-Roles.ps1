Write-Host "  Assigning Databricks Service Principal as Contributor for Atc"

$output = az role assignment create `
  --role "Contributor" `
  --assignee-principal-type ServicePrincipal `
  --assignee-object-id $dbDeploySpn.id `
  --resource-group $resourceGroupName

Throw-WhenError -output $output



Write-Host "  Assigning Databricks Access Connector as Blob Contributor"

$DbAccessConnectorSpn = Graph-GetSpn -queryDisplayName $accesConName

$output = az role assignment create `
  --role "Storage Blob Data Contributor" `
  --assignee-principal-type ServicePrincipal `
  --assignee-object-id $DbAccessConnectorSpn.id `
  --resource-group $resourceGroupName

Throw-WhenError -output $output
