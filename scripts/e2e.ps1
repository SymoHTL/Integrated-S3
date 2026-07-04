<#
.SYNOPSIS
    Full offline E2E suite (Smoke + Full + protocol property tests) against the real loopback host.
#>
param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Args)
$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot
dotnet test src/IntegratedS3/IntegratedS3.E2E.Tests/IntegratedS3.E2E.Tests.csproj -c Release @Args
exit $LASTEXITCODE
