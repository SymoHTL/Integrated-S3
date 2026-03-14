[CmdletBinding()]
param(
    [Parameter()]
    [string]$ProjectPath = 'src/IntegratedS3/WebUi/WebUi.csproj',

    [Parameter()]
    [string]$Configuration = 'Release'
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$resolvedProjectPath = if ([System.IO.Path]::IsPathRooted($ProjectPath)) {
    Resolve-Path -Path $ProjectPath -ErrorAction Stop
}
else {
    Resolve-Path -Path (Join-Path $repoRoot $ProjectPath) -ErrorAction Stop
}

$warningRules = @(
    [pscustomobject]@{
        Name = 'Program ConfigureServices trim warning'
        Pattern = 'Program\.cs.*IL2026.*ConfigureServices\(WebApplicationBuilder\)'
        MaxCount = 2
    },
    [pscustomobject]@{
        Name = 'Program ConfigureServices AOT warning'
        Pattern = 'Program\.cs.*IL3050.*ConfigureServices\(WebApplicationBuilder\)'
        MaxCount = 2
    },
    [pscustomobject]@{
        Name = 'Program ConfigurePipeline trim warning'
        Pattern = 'Program\.cs.*IL2026.*ConfigurePipeline\(WebApplication,\s*(?:Action<IntegratedS3EndpointOptions>|Action`1<IntegratedS3EndpointOptions>)\)'
        MaxCount = 2
    },
    [pscustomobject]@{
        Name = 'Program ConfigurePipeline AOT warning'
        Pattern = 'Program\.cs.*IL3050.*ConfigurePipeline\(WebApplication,\s*(?:Action<IntegratedS3EndpointOptions>|Action`1<IntegratedS3EndpointOptions>)\)'
        MaxCount = 2
    }
)

$logPath = Join-Path ([System.IO.Path]::GetTempPath()) 'IntegratedS3-aot-publish.log'
$publishArguments = @(
    'publish',
    $resolvedProjectPath.Path,
    '-c',
    $Configuration,
    '--self-contained',
    '-tl:off',
    '-v',
    'minimal'
)

Write-Host "Running: dotnet $($publishArguments -join ' ')"
$publishOutput = & dotnet @publishArguments 2>&1 | Tee-Object -FilePath $logPath
$publishExitCode = $LASTEXITCODE

$warningLines = @(
    $publishOutput |
    ForEach-Object { $_.ToString() } |
    Where-Object { $_ -match '\bIL(?:2026|3050)\b' } |
    ForEach-Object { [regex]::Replace($_, '\s+', ' ').Trim() }
)

$warningCounts = @{}
$unexpectedWarnings = New-Object System.Collections.Generic.List[string]

foreach ($warningLine in $warningLines) {
    $matchingRule = $warningRules | Where-Object { $warningLine -match $_.Pattern } | Select-Object -First 1

    if ($null -eq $matchingRule) {
        $unexpectedWarnings.Add($warningLine)
        continue
    }

    if (-not $warningCounts.ContainsKey($matchingRule.Name)) {
        $warningCounts[$matchingRule.Name] = 0
    }

    $warningCounts[$matchingRule.Name]++
}

$countViolations = New-Object System.Collections.Generic.List[string]

foreach ($warningRule in $warningRules) {
    $observedCount = if ($warningCounts.ContainsKey($warningRule.Name)) {
        $warningCounts[$warningRule.Name]
    }
    else {
        0
    }

    if ($observedCount -gt $warningRule.MaxCount) {
        $countViolations.Add("$($warningRule.Name): observed $observedCount warnings, expected at most $($warningRule.MaxCount).")
    }
}

Write-Host "AOT/trim warnings observed: $($warningLines.Count)"
foreach ($warningRule in $warningRules) {
    $observedCount = if ($warningCounts.ContainsKey($warningRule.Name)) {
        $warningCounts[$warningRule.Name]
    }
    else {
        0
    }

    Write-Host (" - {0}: {1}" -f $warningRule.Name, $observedCount)
}
Write-Host "Publish log: $logPath"

if ($env:GITHUB_STEP_SUMMARY) {
    $summaryLines = @(
        '## AOT publish validation',
        '',
        "- Project: ``$($resolvedProjectPath.Path)``",
        "- Publish exit code: ``$publishExitCode``",
        "- Observed IL2026/IL3050 warnings: ``$($warningLines.Count)``",
        ''
    )

    foreach ($warningRule in $warningRules) {
        $observedCount = if ($warningCounts.ContainsKey($warningRule.Name)) {
            $warningCounts[$warningRule.Name]
        }
        else {
            0
        }

        $summaryLines += "- $($warningRule.Name): $observedCount / $($warningRule.MaxCount) allowed"
    }

    if ($unexpectedWarnings.Count -gt 0) {
        $summaryLines += ''
        $summaryLines += '### Unexpected warnings'
        $summaryLines += ''
        foreach ($warningLine in $unexpectedWarnings) {
            $summaryLines += "- ``$warningLine``"
        }
    }

    if ($countViolations.Count -gt 0) {
        $summaryLines += ''
        $summaryLines += '### Count violations'
        $summaryLines += ''
        foreach ($countViolation in $countViolations) {
            $summaryLines += "- $countViolation"
        }
    }

    Add-Content -Path $env:GITHUB_STEP_SUMMARY -Value ($summaryLines -join [Environment]::NewLine)
}

if ($publishExitCode -ne 0) {
    throw "dotnet publish failed with exit code $publishExitCode. See log at $logPath."
}

if ($unexpectedWarnings.Count -gt 0 -or $countViolations.Count -gt 0) {
    $failureReasons = New-Object System.Collections.Generic.List[string]

    if ($unexpectedWarnings.Count -gt 0) {
        $failureReasons.Add("Unexpected IL2026/IL3050 warnings detected:`n$($unexpectedWarnings -join [Environment]::NewLine)")
    }

    if ($countViolations.Count -gt 0) {
        $failureReasons.Add("AOT warning count exceeded the allowed posture:`n$($countViolations -join [Environment]::NewLine)")
    }

    throw ($failureReasons -join [Environment]::NewLine + [Environment]::NewLine)
}

if ($warningLines.Count -eq 0) {
    Write-Host 'No IL2026/IL3050 warnings were observed during publish.'
}
else {
    Write-Host 'AOT warning posture matched the allowed baseline.'
}
