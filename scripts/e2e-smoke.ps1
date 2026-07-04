<#
.SYNOPSIS
    Fast, offline E2E smoke subset (< ~30s) — the pre-push / free-CI gate.
#>
param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Args)
$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $RepoRoot
dotnet test src/IntegratedS3/IntegratedS3.E2E.Tests/IntegratedS3.E2E.Tests.csproj -c Release --filter "Suite=Smoke" @Args
exit $LASTEXITCODE
