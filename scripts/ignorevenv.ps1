# See https://help.dropbox.com/sync/ignored-files
param (
        [string]$Ignore

 )
$RootPath = Split-Path $PSScriptRoot
Write-Output "Dropbox will ignore the $RootPath\$Ignore directory"
Set-Content -Path "$RootPath\$Ignore" -Stream com.dropbox.ignored -Value 1