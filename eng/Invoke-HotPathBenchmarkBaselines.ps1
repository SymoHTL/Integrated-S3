[CmdletBinding()]
param(
    [string]$JsonOutputPath = "docs\benchmarks\hot-path-baseline.json",
    [string]$MarkdownOutputPath = "docs\benchmarks\hot-path-baseline.md",
    [int]$WarmupIterations = 2,
    [int]$MeasuredIterations = 12,
    [string[]]$Scenario = @()
)

$ErrorActionPreference = 'Stop'

if ($WarmupIterations -le 0) {
    throw "WarmupIterations must be greater than zero."
}

if ($MeasuredIterations -le 0) {
    throw "MeasuredIterations must be greater than zero."
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$projectPath = Join-Path $repoRoot "src\IntegratedS3\IntegratedS3.Benchmarks\IntegratedS3.Benchmarks.csproj"
$jsonFullPath = [System.IO.Path]::GetFullPath((Join-Path $repoRoot $JsonOutputPath))
$markdownFullPath = [System.IO.Path]::GetFullPath((Join-Path $repoRoot $MarkdownOutputPath))

$jsonDirectory = Split-Path -Path $jsonFullPath -Parent
$markdownDirectory = Split-Path -Path $markdownFullPath -Parent
if (-not [string]::IsNullOrWhiteSpace($jsonDirectory)) {
    New-Item -ItemType Directory -Force -Path $jsonDirectory | Out-Null
}

if (-not [string]::IsNullOrWhiteSpace($markdownDirectory)) {
    New-Item -ItemType Directory -Force -Path $markdownDirectory | Out-Null
}

$arguments = @(
    "run",
    "--configuration", "Release",
    "--project", $projectPath,
    "--",
    "--warmup", $WarmupIterations.ToString([System.Globalization.CultureInfo]::InvariantCulture),
    "--measured", $MeasuredIterations.ToString([System.Globalization.CultureInfo]::InvariantCulture),
    "--json-output", $jsonFullPath,
    "--markdown-output", $markdownFullPath
)

if ($Scenario.Count -gt 0) {
    $arguments += @("--scenario", ($Scenario -join ","))
}

Write-Host "Running IntegratedS3 hot-path benchmarks..."
Write-Host "  Project: $projectPath"
Write-Host "  JSON:    $jsonFullPath"
Write-Host "  Markdown:$markdownFullPath"
Write-Host "  Warmup:  $WarmupIterations"
Write-Host "  Measured:$MeasuredIterations"
if ($Scenario.Count -gt 0) {
    Write-Host "  Scenario:$($Scenario -join ', ')"
}

& dotnet @arguments
if ($LASTEXITCODE -ne 0) {
    throw "The benchmark runner exited with code $LASTEXITCODE."
}
