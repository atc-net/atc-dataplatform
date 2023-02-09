# Add a secret for each layer in the storageaccount
foreach ($layer in $dataLakeContainers.Values) {
    
    # Example: atcsilver
    $secretName = $dataLakeName + $layer

    # Example: silver@atc
    $domainPart = $($layer.ToLower()) + "@" + $dataLakeName

    # Example: abfss://silver@atc.dfs.core.windows.net/
    $secretValue = "abfss://$domainPart.dfs.core.windows.net/"
    Write-Host "  Adds databricks secret $secretName with value $secretValue..." -ForegroundColor DarkYellow

    $values.addSecret($secretName, $secretValue)
}