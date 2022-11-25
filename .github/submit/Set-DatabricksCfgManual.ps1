param (
  # This helper function can set the databricks cfg manually
  [Parameter(Mandatory=$True)]
  [ValidateNotNullOrEmpty()]
  [string]
  $workspaceUrl,

  [Parameter(Mandatory=$True)]
  [ValidateNotNullOrEmpty()]
  [string]
  $token
)


Set-Content ~/.databrickscfg "[DEFAULT]"
Add-Content ~/.databrickscfg "host = https://$workspaceUrl"
Add-Content ~/.databrickscfg "token = $token"
Add-Content ~/.databrickscfg ""