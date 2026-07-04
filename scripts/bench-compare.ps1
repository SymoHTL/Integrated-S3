<#
.SYNOPSIS
    Compare the latest benchmark run against the committed baseline (the regression gate).
.EXAMPLE
    ./scripts/bench-compare.ps1
    ./scripts/bench-compare.ps1 --update-baseline
    ./scripts/bench-compare.ps1 --mean-threshold 0.15 --alloc-threshold 0.0
#>
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Args
)

$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path -Parent $PSScriptRoot
$Python = if ($env:PYTHON) { $env:PYTHON } else { 'python' }

& $Python (Join-Path $PSScriptRoot 'bench-compare.py') `
    --baseline (Join-Path $RepoRoot 'benchmarks/baseline') `
    --current (Join-Path $RepoRoot 'benchmarks/artifacts') `
    @Args

exit $LASTEXITCODE
