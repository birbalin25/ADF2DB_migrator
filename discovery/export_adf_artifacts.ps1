<#
.SYNOPSIS
    Exports all Azure Data Factory artifacts to JSON for migration analysis.

.DESCRIPTION
    Extracts pipelines, linked services, datasets, triggers, data flows,
    integration runtimes, and global parameters from an ADF instance.

.PARAMETER ResourceGroupName
    The Azure resource group containing the ADF instance.

.PARAMETER DataFactoryName
    The name of the Azure Data Factory instance.

.PARAMETER OutputDir
    Directory to write exported JSON files. Defaults to ./adf-export.
#>

param(
    [Parameter(Mandatory=$true)]
    [string]$ResourceGroupName,

    [Parameter(Mandatory=$true)]
    [string]$DataFactoryName,

    [string]$OutputDir = "./adf-export"
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path $OutputDir)) {
    New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
}

Write-Host "Exporting ADF artifacts from '$DataFactoryName' in resource group '$ResourceGroupName'..."

$artifacts = @(
    @{ Name = "pipelines";           Cmd = { Get-AzDataFactoryV2Pipeline -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName } },
    @{ Name = "linkedservices";      Cmd = { Get-AzDataFactoryV2LinkedService -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName } },
    @{ Name = "datasets";            Cmd = { Get-AzDataFactoryV2Dataset -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName } },
    @{ Name = "triggers";            Cmd = { Get-AzDataFactoryV2Trigger -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName } },
    @{ Name = "dataflows";           Cmd = { Get-AzDataFactoryV2DataFlow -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName } },
    @{ Name = "integrationruntimes"; Cmd = { Get-AzDataFactoryV2IntegrationRuntime -ResourceGroupName $ResourceGroupName -DataFactoryName $DataFactoryName } }
)

$summary = @{}

foreach ($artifact in $artifacts) {
    $name = $artifact.Name
    $outFile = Join-Path $OutputDir "$name.json"
    Write-Host "  Exporting $name..."
    try {
        $data = & $artifact.Cmd
        $count = ($data | Measure-Object).Count
        $data | ConvertTo-Json -Depth 100 | Out-File -FilePath $outFile -Encoding utf8
        $summary[$name] = $count
        Write-Host "    -> $count $name exported to $outFile"
    } catch {
        Write-Warning "    Failed to export $name: $_"
        $summary[$name] = "ERROR"
    }
}

# Export global parameters via REST API (not available in PowerShell cmdlet)
Write-Host "  Exporting global parameters..."
try {
    $token = (Get-AzAccessToken -ResourceUrl "https://management.azure.com").Token
    $subscriptionId = (Get-AzContext).Subscription.Id
    $uri = "https://management.azure.com/subscriptions/$subscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.DataFactory/factories/$DataFactoryName`?api-version=2018-06-01"
    $response = Invoke-RestMethod -Uri $uri -Headers @{ Authorization = "Bearer $token" } -Method Get
    $globalParams = $response.properties.globalParameters
    $globalParams | ConvertTo-Json -Depth 100 | Out-File -FilePath (Join-Path $OutputDir "globalparameters.json") -Encoding utf8
    $gpCount = ($globalParams.PSObject.Properties | Measure-Object).Count
    $summary["globalparameters"] = $gpCount
    Write-Host "    -> $gpCount global parameters exported"
} catch {
    Write-Warning "    Failed to export global parameters: $_"
    $summary["globalparameters"] = "ERROR"
}

Write-Host "`nExport Summary:"
Write-Host "==============="
foreach ($key in $summary.Keys | Sort-Object) {
    Write-Host "  $key : $($summary[$key])"
}
Write-Host "`nAll artifacts exported to: $OutputDir"
