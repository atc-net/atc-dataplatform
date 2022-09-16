param location string
param keyVaultName string
param devobjectid string
param spnobjectid string
param tags object
param dbname string
param loganalyticsname string
param appinsightname string


//#############################################################################################
//# Provision Keyvault
//#############################################################################################

resource kw 'Microsoft.KeyVault/vaults@2022-07-01' = {
  name: keyVaultName
  location: location
  tags: tags
  properties: {
    sku: {
      family: 'A'
      name: 'standard'
    }
    enabledForTemplateDeployment: true
    tenantId: tenant().tenantId
    accessPolicies: [
      {
        // Developers
        objectId: devobjectid
        permissions: {
          secrets: [
            'list'
            'get'
            'set'
          ] }
        tenantId: tenant().tenantId
      } 
      {
        objectId: spnobjectid
        permissions: {
          secrets: [
            'list'
            'get'
            'set'
          ] }
        tenantId: tenant().tenantId
      }
    ]
  }
}

//#############################################################################################
//# Provision Cosmos database
//#############################################################################################

resource csdb 'Microsoft.DocumentDB/databaseAccounts@2022-05-15' = {
  name: dbname
  tags: tags
  location: location
  properties: {
    databaseAccountOfferType: 'Standard'
    enableFreeTier: true
    locations: [{
      failoverPriority: 0
      isZoneRedundant: false
      locationName: location
    }]
  }
}

//#############################################################################################
//# Provision Application Insights
//#############################################################################################


resource loganalytics 'Microsoft.OperationalInsights/workspaces@2021-12-01-preview' = {
  name: loganalyticsname
  location: location

}

resource appinsight 'Microsoft.Insights/components@2020-02-02' = {
  name: appinsightname
  location: location
  kind: 'web'
  properties: {
    Application_Type: 'web'
    WorkspaceResourceId: loganalytics.id
  }
}
