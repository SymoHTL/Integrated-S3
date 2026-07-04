<#
.SYNOPSIS
    Optional local soak: run the full E2E suite in a loop to shake out flakiness / resource leaks.
.PARAMETER Iterations
    Number of iterations (default 10).
#>
param([int]$Iterations = 10)
$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot
$Project = 'src/IntegratedS3/IntegratedS3.E2E.Tests/IntegratedS3.E2E.Tests.csproj'

Write-Host '==> Building (Release)'
dotnet build $Project -c Release --nologo -v minimal
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

for ($i = 1; $i -le $Iterations; $i++) {
    Write-Host "==> Soak iteration $i / $Iterations"
    dotnet test $Project -c Release --no-build
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
Write-Host "==> Soak complete: $Iterations iteration(s) passed."
