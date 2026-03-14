<#
.SYNOPSIS
Launches GitHub Copilot CLI for a queued issue prompt.

.DESCRIPTION
Reads a prompt from disk and invokes the Copilot CLI with a stable argument
shape so multi-line prompts are not split by Start-Process command-line
construction.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$CopilotCliPath,

    [Parameter(Mandatory)]
    [string]$PromptPath,

    [Parameter(Mandatory)]
    [string]$TranscriptPath,

    [Parameter(Mandatory)]
    [string]$Model,

    [Parameter(Mandatory)]
    [ValidateSet('low', 'medium', 'high', 'xhigh')]
    [string]$ReasoningEffort,

    [Parameter()]
    [string]$Agent,

    [Parameter()]
    [switch]$Interactive
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if (-not (Test-Path -LiteralPath $CopilotCliPath)) {
    throw "Copilot CLI path '$CopilotCliPath' does not exist."
}

if (-not (Test-Path -LiteralPath $PromptPath)) {
    throw "Prompt file '$PromptPath' does not exist."
}

$transcriptDirectory = Split-Path -Parent $TranscriptPath
if (-not [string]::IsNullOrWhiteSpace($transcriptDirectory) -and -not (Test-Path -LiteralPath $transcriptDirectory)) {
    New-Item -ItemType Directory -Path $transcriptDirectory -Force | Out-Null
}

$prompt = Get-Content -Raw -LiteralPath $PromptPath
$copilotArguments = New-Object System.Collections.Generic.List[string]
$copilotArguments.Add('-p')
$copilotArguments.Add($prompt)
$copilotArguments.Add('--share')
$copilotArguments.Add($TranscriptPath)
$copilotArguments.Add('--model')
$copilotArguments.Add($Model)
$copilotArguments.Add('--reasoning-effort')
$copilotArguments.Add($ReasoningEffort)

if (-not $Interactive.IsPresent) {
    $copilotArguments.Add('--allow-all')
    $copilotArguments.Add('--no-ask-user')
}

if (-not [string]::IsNullOrWhiteSpace($Agent)) {
    $copilotArguments.Add('--agent')
    $copilotArguments.Add($Agent)
}

& $CopilotCliPath @copilotArguments
exit $LASTEXITCODE