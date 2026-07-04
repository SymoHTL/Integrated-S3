<#
.SYNOPSIS
    Run the IntegratedS3 BenchmarkDotNet hot-path suite (local only; never in shared CI).
.PARAMETER Filter
    BenchmarkDotNet filter glob. Default '*' runs the whole suite.
.EXAMPLE
    ./scripts/bench.ps1
    ./scripts/bench.ps1 '*SigV4*'
#>
param(
    [string]$Filter = '*'
)

$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot

$Project = 'src/IntegratedS3/IntegratedS3.Benchmarks/IntegratedS3.Benchmarks.csproj'
$Artifacts = Join-Path $RepoRoot 'benchmarks/artifacts'

Write-Host '==> Building benchmarks (Release)'
dotnet build $Project -c Release --nologo -v minimal
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "==> Running benchmarks (filter: $Filter)"
if (Test-Path $Artifacts) { Remove-Item -Recurse -Force $Artifacts }
dotnet run -c Release --no-build --project $Project -- --filter $Filter --artifacts $Artifacts
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "==> Results written to $Artifacts/results"
Write-Host '    Compare against baseline: ./scripts/bench-compare.ps1'
