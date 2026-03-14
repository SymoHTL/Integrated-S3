<#
.SYNOPSIS
Claims queued issues, creates isolated worktrees, and launches Copilot runs.

.DESCRIPTION
Reads a queue file created by Get-IssueAgentQueue.ps1, claims each selected
issue by creating a dedicated remote branch, marks the issue in progress on
GitHub through the GitHub CLI, creates or reuses a local worktree, and starts a
Copilot CLI process in that worktree.

Use -WhatIf first to preview which issues and branches would be claimed.

.EXAMPLE
.\eng\Start-IssueAgentQueue.ps1 -WhatIf -MaxAgents 2

.EXAMPLE
.\eng\Start-IssueAgentQueue.ps1 -QueuePath '.git\copilot\trackh-queue.json' -IssueNumber 15 -Interactive

.EXAMPLE
.\eng\Start-IssueAgentQueue.ps1 -IssueNumber 2 -ShowWindow

.EXAMPLE
.\eng\Start-IssueAgentQueue.ps1

Launches Copilot with the default `gpt-5.4` model, `xhigh` reasoning effort,
and full automatic approvals unless `-Interactive` is specified.
#>
[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [Parameter()]
    [string]$QueuePath,

    [Parameter()]
    [int[]]$IssueNumber = @(),

    [Parameter()]
    [ValidateRange(1, 100)]
    [int]$MaxAgents = 1,

    [Parameter()]
    [string]$WorktreeRoot,

    [Parameter()]
    [string]$RunArtifactsRoot,

    [Parameter()]
    [string]$Agent,

    [Parameter()]
    [string]$Model = 'gpt-5.4',

    [Parameter()]
    [ValidateSet('low', 'medium', 'high', 'xhigh')]
    [string]$ReasoningEffort = 'xhigh',

    [Parameter()]
    [string]$InProgressLabel = 'copilot:in-progress',

    [Parameter()]
    [string]$SkipLabel = 'copilot:skip',

    [Parameter()]
    [switch]$Interactive,

    [Parameter()]
    [switch]$ShowWindow,

    [Parameter()]
    [switch]$PassThru
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Invoke-Git {
    param(
        [Parameter(Mandatory)]
        [string]$RepositoryRoot,

        [Parameter(Mandatory)]
        [string[]]$Arguments,

        [Parameter()]
        [switch]$IgnoreExitCode
    )

    $output = & git -C $RepositoryRoot @Arguments 2>&1
    $exitCode = $LASTEXITCODE
    if (-not $IgnoreExitCode.IsPresent -and $exitCode -ne 0) {
        $commandText = "git -C `"$RepositoryRoot`" $($Arguments -join ' ')"
        throw "$commandText failed with exit code $exitCode.`n$($output -join [Environment]::NewLine)"
    }

    return [pscustomobject]@{
        ExitCode = $exitCode
        Output = @($output | ForEach-Object { $_.ToString().TrimEnd() })
    }
}

function Get-RepositoryRoot {
    $output = & git rev-parse --show-toplevel 2>&1
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        throw "git rev-parse --show-toplevel failed with exit code $exitCode.`n$($output -join [Environment]::NewLine)"
    }

    return @($output)[0].ToString().Trim()
}

function Get-GitCommonDirectory {
    param(
        [Parameter(Mandatory)]
        [string]$RepositoryRoot
    )

    $gitCommonDirectory = (Invoke-Git -RepositoryRoot $RepositoryRoot -Arguments @('rev-parse', '--git-common-dir')).Output[0]
    if ([System.IO.Path]::IsPathRooted($gitCommonDirectory)) {
        return $gitCommonDirectory
    }

    return [System.IO.Path]::GetFullPath((Join-Path $RepositoryRoot $gitCommonDirectory))
}

function Get-RemoteContext {
    param(
        [Parameter(Mandatory)]
        [string]$RepositoryRoot
    )

    $remoteUrl = (Invoke-Git -RepositoryRoot $RepositoryRoot -Arguments @('remote', 'get-url', 'origin')).Output[0].Trim()
    $patterns = @(
        '^(?:https://|http://)github\.com/(?<owner>[^/]+)/(?<repo>[^/]+?)(?:\.git)?/?$',
        '^(?:git@|ssh://git@)github\.com[:/](?<owner>[^/]+)/(?<repo>[^/]+?)(?:\.git)?/?$'
    )

    foreach ($pattern in $patterns) {
        $match = [regex]::Match($remoteUrl, $pattern, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
        if ($match.Success) {
            return [pscustomobject]@{
                Owner = $match.Groups['owner'].Value
                Repository = $match.Groups['repo'].Value
                RemoteUrl = $remoteUrl
            }
        }
    }

    throw "Could not infer the GitHub owner and repository from origin remote '$remoteUrl'."
}

function Resolve-GhCliPath {
    $pathCandidates = New-Object System.Collections.Generic.List[string]

    foreach ($commandName in 'gh.exe', 'gh') {
        $command = Get-Command -Name $commandName -ErrorAction SilentlyContinue
        if ($null -ne $command -and -not [string]::IsNullOrWhiteSpace($command.Source)) {
            $pathCandidates.Add($command.Source)
        }
    }

    foreach ($candidate in @(
        "$env:ProgramFiles\GitHub CLI\gh.exe",
        "$env:LOCALAPPDATA\Programs\GitHub CLI\gh.exe",
        "$env:ProgramFiles(x86)\GitHub CLI\gh.exe",
        "$env:ChocolateyInstall\bin\gh.exe"
    )) {
        if (-not [string]::IsNullOrWhiteSpace($candidate)) {
            $pathCandidates.Add($candidate)
        }
    }

    foreach ($candidate in $pathCandidates | Select-Object -Unique) {
        if (Test-Path -LiteralPath $candidate) {
            return [System.IO.Path]::GetFullPath($candidate)
        }
    }

    throw 'GitHub CLI was not found. Install gh or add it to PATH before running this script.'
}

function Resolve-CopilotCliPath {
    $pathCandidates = New-Object System.Collections.Generic.List[string]

    foreach ($command in @(Get-Command -Name 'copilot' -CommandType Application -All -ErrorAction SilentlyContinue)) {
        foreach ($candidate in @($command.Path, $command.Source)) {
            if (-not [string]::IsNullOrWhiteSpace($candidate)) {
                $pathCandidates.Add($candidate)
            }
        }
    }

    foreach ($candidate in @(
        "$env:LOCALAPPDATA\Microsoft\WinGet\Links\copilot.exe",
        "$env:LOCALAPPDATA\Programs\GitHub Copilot\copilot.exe",
        "$env:ProgramFiles\GitHub Copilot\copilot.exe",
        "$env:ProgramFiles(x86)\GitHub Copilot\copilot.exe"
    )) {
        if (-not [string]::IsNullOrWhiteSpace($candidate)) {
            $pathCandidates.Add($candidate)
        }
    }

    $resolvedCandidate = $pathCandidates |
        Select-Object -Unique |
        Where-Object { Test-Path -LiteralPath $_ } |
        Sort-Object @(
            @{ Expression = {
                    switch ([System.IO.Path]::GetExtension($_).ToLowerInvariant()) {
                        '.exe' { 0 }
                        '.cmd' { 1 }
                        '.bat' { 2 }
                        default { 3 }
                    }
                }
            },
            @{ Expression = { $_ } }
        ) |
        Select-Object -First 1

    if (-not [string]::IsNullOrWhiteSpace($resolvedCandidate)) {
        return [System.IO.Path]::GetFullPath($resolvedCandidate)
    }

    throw 'GitHub Copilot CLI was not found. Install copilot or add it to PATH before running this script.'
}

function Resolve-PowerShellPath {
    $pathCandidates = New-Object System.Collections.Generic.List[string]

    if (-not [string]::IsNullOrWhiteSpace($PSHOME)) {
        foreach ($candidate in @(
            (Join-Path $PSHOME 'pwsh.exe'),
            (Join-Path $PSHOME 'powershell.exe')
        )) {
            $pathCandidates.Add($candidate)
        }
    }

    foreach ($commandName in 'pwsh.exe', 'pwsh', 'powershell.exe', 'powershell') {
        foreach ($command in @(Get-Command -Name $commandName -CommandType Application -All -ErrorAction SilentlyContinue)) {
            foreach ($candidate in @($command.Path, $command.Source)) {
                if (-not [string]::IsNullOrWhiteSpace($candidate)) {
                    $pathCandidates.Add($candidate)
                }
            }
        }
    }

    foreach ($candidate in $pathCandidates | Select-Object -Unique) {
        if (Test-Path -LiteralPath $candidate) {
            return [System.IO.Path]::GetFullPath($candidate)
        }
    }

    throw 'PowerShell executable was not found. Install PowerShell or add it to PATH before running this script.'
}

function ConvertTo-QuotedArgument {
    param(
        [Parameter(Mandatory)]
        [AllowEmptyString()]
        [string]$Value
    )

    $escapedValue = $Value -replace '(\\*)"', '$1$1\\"'
    $escapedValue = $escapedValue -replace '(\\+)$', '$1$1'
    return '"' + $escapedValue + '"'
}

function Invoke-Gh {
    param(
        [Parameter(Mandatory)]
        [string]$GhPath,

        [Parameter(Mandatory)]
        [string[]]$Arguments,

        [Parameter()]
        [string]$WorkingDirectory,

        [Parameter()]
        [switch]$IgnoreExitCode
    )

    $output = if ([string]::IsNullOrWhiteSpace($WorkingDirectory)) {
        & $GhPath @Arguments 2>&1
    }
    else {
        Push-Location -LiteralPath $WorkingDirectory
        try {
            & $GhPath @Arguments 2>&1
        }
        finally {
            Pop-Location
        }
    }

    $exitCode = $LASTEXITCODE
    if (-not $IgnoreExitCode.IsPresent -and $exitCode -ne 0) {
        throw "gh $($Arguments -join ' ') failed with exit code $exitCode.`n$($output -join [Environment]::NewLine)"
    }

    return [pscustomobject]@{
        ExitCode = $exitCode
        Output = @($output | ForEach-Object { $_.ToString().TrimEnd() })
    }
}

function Invoke-GhJson {
    param(
        [Parameter(Mandatory)]
        [string]$GhPath,

        [Parameter(Mandatory)]
        [string[]]$Arguments,

        [Parameter()]
        [string]$WorkingDirectory
    )

    $result = Invoke-Gh -GhPath $GhPath -Arguments $Arguments -WorkingDirectory $WorkingDirectory
    $jsonText = ($result.Output -join [Environment]::NewLine).Trim()
    if ([string]::IsNullOrWhiteSpace($jsonText)) {
        return $null
    }

    return $jsonText | ConvertFrom-Json -Depth 20
}

function Assert-GhAuthenticated {
    param(
        [Parameter(Mandatory)]
        [string]$GhPath,

        [Parameter()]
        [string]$WorkingDirectory
    )

    $result = Invoke-Gh -GhPath $GhPath -Arguments @('auth', 'status', '--hostname', 'github.com') -WorkingDirectory $WorkingDirectory -IgnoreExitCode
    if ($result.ExitCode -ne 0) {
        throw "gh auth status failed. Run `gh auth login` first.`n$($result.Output -join [Environment]::NewLine)"
    }
}

function Get-QueuePath {
    param(
        [Parameter(Mandatory)]
        [string]$RepositoryRoot,

        [Parameter(Mandatory)]
        [string]$GitCommonDirectory,

        [Parameter()]
        [string]$RequestedPath
    )

    if ([string]::IsNullOrWhiteSpace($RequestedPath)) {
        return Join-Path $GitCommonDirectory 'copilot\issue-agent-queue.json'
    }

    if ([System.IO.Path]::IsPathRooted($RequestedPath)) {
        return [System.IO.Path]::GetFullPath($RequestedPath)
    }

    return [System.IO.Path]::GetFullPath((Join-Path $RepositoryRoot $RequestedPath))
}

function Get-DefaultWorktreeRoot {
    param(
        [Parameter(Mandatory)]
        [string]$RepositoryRoot
    )

    $repositoryName = Split-Path -Leaf $RepositoryRoot
    $parentDirectory = Split-Path -Parent $RepositoryRoot
    return Join-Path $parentDirectory ($repositoryName + '-worktrees')
}

function Load-Queue {
    param(
        [Parameter(Mandatory)]
        [string]$Path
    )

    if (-not (Test-Path -LiteralPath $Path)) {
        throw "Queue file '$Path' does not exist. Run eng\Get-IssueAgentQueue.ps1 first."
    }

    return Get-Content -Raw -Path $Path | ConvertFrom-Json -Depth 10
}

function Save-Queue {
    param(
        [Parameter(Mandatory)]
        [object]$Queue,

        [Parameter(Mandatory)]
        [string]$Path
    )

    $directory = Split-Path -Parent $Path
    if (-not [string]::IsNullOrWhiteSpace($directory) -and -not (Test-Path -LiteralPath $directory)) {
        New-Item -ItemType Directory -Path $directory -Force | Out-Null
    }

    $Queue | ConvertTo-Json -Depth 10 | Set-Content -Path $Path -Encoding UTF8
}

function Get-RepositoryMetadata {
    param(
        [Parameter(Mandatory)]
        [string]$GhPath,

        [Parameter(Mandatory)]
        [string]$RepositoryRoot,

        [Parameter(Mandatory)]
        [string]$Owner,

        [Parameter(Mandatory)]
        [string]$Repository
    )

    $repoIdentifier = "$Owner/$Repository"
    $metadata = Invoke-GhJson -GhPath $GhPath -WorkingDirectory $RepositoryRoot -Arguments @(
        'repo', 'view', $repoIdentifier, '--json', 'nameWithOwner,defaultBranchRef,url'
    )

    return [pscustomobject]@{
        nameWithOwner = [string]$metadata.nameWithOwner
        default_branch = [string]$metadata.defaultBranchRef.name
        url = [string]$metadata.url
    }
}

function Get-IssueDetails {
    param(
        [Parameter(Mandatory)]
        [string]$GhPath,

        [Parameter(Mandatory)]
        [string]$RepositoryRoot,

        [Parameter(Mandatory)]
        [string]$Owner,

        [Parameter(Mandatory)]
        [string]$Repository,

        [Parameter(Mandatory)]
        [int]$Number
    )

    $repoIdentifier = "$Owner/$Repository"
    return Invoke-GhJson -GhPath $GhPath -WorkingDirectory $RepositoryRoot -Arguments @(
        'issue', 'view', $Number.ToString(), '--repo', $repoIdentifier, '--json',
        'assignees,body,createdAt,labels,number,state,title,updatedAt,url'
    )
}

function Ensure-RepositoryLabel {
    param(
        [Parameter(Mandatory)]
        [string]$GhPath,

        [Parameter(Mandatory)]
        [string]$RepositoryRoot,

        [Parameter(Mandatory)]
        [string]$Owner,

        [Parameter(Mandatory)]
        [string]$Repository,

        [Parameter(Mandatory)]
        [string]$LabelName
    )

    $repoIdentifier = "$Owner/$Repository"
    [void](Invoke-Gh -GhPath $GhPath -WorkingDirectory $RepositoryRoot -Arguments @(
        'label', 'create', $LabelName, '--repo', $repoIdentifier,
        '--color', '0e8a16',
        '--description', 'Claimed by local Copilot issue automation.',
        '--force'
    ))
}

function Add-IssueLabel {
    param(
        [Parameter(Mandatory)]
        [string]$GhPath,

        [Parameter(Mandatory)]
        [string]$RepositoryRoot,

        [Parameter(Mandatory)]
        [string]$Owner,

        [Parameter(Mandatory)]
        [string]$Repository,

        [Parameter(Mandatory)]
        [int]$Number,

        [Parameter(Mandatory)]
        [string]$LabelName
    )

    $repoIdentifier = "$Owner/$Repository"
    [void](Invoke-Gh -GhPath $GhPath -WorkingDirectory $RepositoryRoot -Arguments @(
        'issue', 'edit', $Number.ToString(), '--repo', $repoIdentifier, '--add-label', $LabelName
    ))
}

function Add-IssueComment {
    param(
        [Parameter(Mandatory)]
        [string]$GhPath,

        [Parameter(Mandatory)]
        [string]$RepositoryRoot,

        [Parameter(Mandatory)]
        [string]$Owner,

        [Parameter(Mandatory)]
        [string]$Repository,

        [Parameter(Mandatory)]
        [int]$Number,

        [Parameter(Mandatory)]
        [string]$Body
    )

    $repoIdentifier = "$Owner/$Repository"
    [void](Invoke-Gh -GhPath $GhPath -WorkingDirectory $RepositoryRoot -Arguments @(
        'issue', 'comment', $Number.ToString(), '--repo', $repoIdentifier, '--body', $Body
    ))
}

function Get-GitWorktrees {
    param(
        [Parameter(Mandatory)]
        [string]$RepositoryRoot
    )

    $result = Invoke-Git -RepositoryRoot $RepositoryRoot -Arguments @('worktree', 'list', '--porcelain')
    $entries = New-Object System.Collections.Generic.List[object]
    $current = @{}

    foreach ($line in $result.Output) {
        if ([string]::IsNullOrWhiteSpace($line)) {
            if ($current.Count -gt 0) {
                $entries.Add([pscustomobject]$current)
                $current = @{}
            }

            continue
        }

        $separatorIndex = $line.IndexOf(' ')
        if ($separatorIndex -lt 0) {
            continue
        }

        $key = $line.Substring(0, $separatorIndex)
        $value = $line.Substring($separatorIndex + 1)
        $current[$key] = $value
    }

    if ($current.Count -gt 0) {
        $entries.Add([pscustomobject]$current)
    }

    return $entries
}

function Ensure-Property {
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$Object,

        [Parameter(Mandatory)]
        [string]$Name,

        [Parameter()]
        [object]$Value
    )

    if ($Object.PSObject.Properties.Match($Name).Count -eq 0) {
        $Object | Add-Member -NotePropertyName $Name -NotePropertyValue $Value
    }
}

function Test-RemoteBranchExists {
    param(
        [Parameter(Mandatory)]
        [string]$RepositoryRoot,

        [Parameter(Mandatory)]
        [string]$BranchName
    )

    $result = Invoke-Git -RepositoryRoot $RepositoryRoot -Arguments @('ls-remote', '--exit-code', '--heads', 'origin', $BranchName) -IgnoreExitCode
    return $result.ExitCode -eq 0
}

function Try-ClaimBranch {
    param(
        [Parameter(Mandatory)]
        [string]$RepositoryRoot,

        [Parameter(Mandatory)]
        [string]$BranchName,

        [Parameter(Mandatory)]
        [string]$BaseBranch
    )

    [void](Invoke-Git -RepositoryRoot $RepositoryRoot -Arguments @('fetch', '--quiet', 'origin', $BaseBranch))
    $baseSha = (Invoke-Git -RepositoryRoot $RepositoryRoot -Arguments @('rev-parse', "origin/$BaseBranch")).Output[0]

    $pushResult = Invoke-Git -RepositoryRoot $RepositoryRoot -Arguments @('push', 'origin', "$($baseSha):refs/heads/$BranchName") -IgnoreExitCode
    if ($pushResult.ExitCode -eq 0) {
        return [pscustomobject]@{
            Claimed = $true
            BaseSha = $baseSha
            Output = ($pushResult.Output -join [Environment]::NewLine)
        }
    }

    if (Test-RemoteBranchExists -RepositoryRoot $RepositoryRoot -BranchName $BranchName) {
        return [pscustomobject]@{
            Claimed = $false
            BaseSha = $baseSha
            Output = ($pushResult.Output -join [Environment]::NewLine)
        }
    }

    throw "Failed to create remote claim branch '$BranchName'.`n$($pushResult.Output -join [Environment]::NewLine)"
}

function Get-ClaimBranchForLaunch {
    param(
        [Parameter(Mandatory)]
        [string]$RepositoryRoot,

        [Parameter(Mandatory)]
        [pscustomobject]$QueueItem,

        [Parameter(Mandatory)]
        [string]$BranchName,

        [Parameter(Mandatory)]
        [string]$BaseBranch
    )

    $status = if ($QueueItem.status) { [string]$QueueItem.status } else { 'queued' }
    $hasExistingClaim = $QueueItem.PSObject.Properties.Match('claim').Count -gt 0 -and
        $null -ne $QueueItem.claim -and
        $QueueItem.claim.PSObject.Properties.Match('branchName').Count -gt 0 -and
        [string]$QueueItem.claim.branchName -eq $BranchName

    if ($status -eq 'launch-failed' -and $hasExistingClaim -and (Test-RemoteBranchExists -RepositoryRoot $RepositoryRoot -BranchName $BranchName)) {
        $existingBaseSha = if ($QueueItem.claim.PSObject.Properties.Match('baseSha').Count -gt 0 -and -not [string]::IsNullOrWhiteSpace([string]$QueueItem.claim.baseSha)) {
            [string]$QueueItem.claim.baseSha
        }
        else {
            (Invoke-Git -RepositoryRoot $RepositoryRoot -Arguments @('rev-parse', "origin/$BranchName")).Output[0]
        }

        return [pscustomobject]@{
            Claimed = $true
            BaseSha = $existingBaseSha
            Output = 'reusing-existing-claim'
            ReusedExistingClaim = $true
        }
    }

    $claimResult = Try-ClaimBranch -RepositoryRoot $RepositoryRoot -BranchName $BranchName -BaseBranch $BaseBranch
    if ($claimResult.PSObject.Properties.Match('ReusedExistingClaim').Count -eq 0) {
        $claimResult | Add-Member -NotePropertyName ReusedExistingClaim -NotePropertyValue $false
    }

    return $claimResult
}

function Ensure-Worktree {
    param(
        [Parameter(Mandatory)]
        [string]$RepositoryRoot,

        [Parameter(Mandatory)]
        [string]$WorktreeRoot,

        [Parameter(Mandatory)]
        [string]$WorktreeName,

        [Parameter(Mandatory)]
        [string]$BranchName,

        [Parameter(Mandatory)]
        [string]$BaseBranch
    )

    $worktreeEntries = Get-GitWorktrees -RepositoryRoot $RepositoryRoot
    $existingBranchEntry = $worktreeEntries | Where-Object { $_.branch -eq "refs/heads/$BranchName" } | Select-Object -First 1
    if ($null -ne $existingBranchEntry) {
        return [System.IO.Path]::GetFullPath($existingBranchEntry.worktree)
    }

    $resolvedWorktreeRoot = [System.IO.Path]::GetFullPath($WorktreeRoot)
    if (-not (Test-Path -LiteralPath $resolvedWorktreeRoot)) {
        New-Item -ItemType Directory -Path $resolvedWorktreeRoot -Force | Out-Null
    }

    $worktreePath = Join-Path $resolvedWorktreeRoot $WorktreeName
    if (Test-Path -LiteralPath $worktreePath) {
        $insideWorktree = Invoke-Git -RepositoryRoot $worktreePath -Arguments @('rev-parse', '--is-inside-work-tree') -IgnoreExitCode
        if ($insideWorktree.ExitCode -eq 0) {
            return [System.IO.Path]::GetFullPath($worktreePath)
        }

        throw "Cannot create worktree at '$worktreePath' because the path already exists and is not a Git worktree."
    }

    $localBranchExists = (Invoke-Git -RepositoryRoot $RepositoryRoot -Arguments @('show-ref', '--verify', '--quiet', "refs/heads/$BranchName") -IgnoreExitCode).ExitCode -eq 0
    if ($localBranchExists) {
        [void](Invoke-Git -RepositoryRoot $RepositoryRoot -Arguments @('worktree', 'add', $worktreePath, $BranchName))
    }
    else {
        [void](Invoke-Git -RepositoryRoot $RepositoryRoot -Arguments @('worktree', 'add', '-b', $BranchName, $worktreePath, "origin/$BaseBranch"))
    }

    [void](Invoke-Git -RepositoryRoot $worktreePath -Arguments @('branch', '--set-upstream-to', "origin/$BranchName", $BranchName) -IgnoreExitCode)
    return [System.IO.Path]::GetFullPath($worktreePath)
}

function New-AgentPrompt {
    param(
        [Parameter(Mandatory)]
        [pscustomobject]$Issue,

        [Parameter(Mandatory)]
        [string]$BranchName
    )

    $body = if ([string]::IsNullOrWhiteSpace($Issue.body)) {
        '(No issue body was provided on GitHub.)'
    }
    else {
        $Issue.body.Trim()
    }

    return @"
Work on GitHub issue #$($Issue.number): $($Issue.title)

Issue URL: $($Issue.url)
Target branch: $BranchName

Issue body:
$body

Execution rules:
- Work only in this worktree on the current branch.
- Keep the change scoped to this issue.
- Add or update automated tests for the behavior you change.
- Validate with:
  - dotnet build src\IntegratedS3\IntegratedS3.slnx
  - dotnet test src\IntegratedS3\IntegratedS3.slnx
- If the issue is underspecified or blocked, stop and explain the blocker instead of guessing.
- Do not merge, rebase, or delete branches.
- When you finish, summarize the files changed, tests run, and any remaining risks.
"@
}

$repositoryRoot = Get-RepositoryRoot
$gitCommonDirectory = Get-GitCommonDirectory -RepositoryRoot $repositoryRoot
$remoteContext = Get-RemoteContext -RepositoryRoot $repositoryRoot
$ghCliPath = Resolve-GhCliPath
$ghAuthRequired = -not $WhatIfPreference
if ($ghAuthRequired) {
    Assert-GhAuthenticated -GhPath $ghCliPath -WorkingDirectory $repositoryRoot
}
$resolvedQueuePath = Get-QueuePath -RepositoryRoot $repositoryRoot -GitCommonDirectory $gitCommonDirectory -RequestedPath $QueuePath
$queue = Load-Queue -Path $resolvedQueuePath

$owner = if ($queue.repository.owner) { [string]$queue.repository.owner } else { $remoteContext.Owner }
$repository = if ($queue.repository.name) { [string]$queue.repository.name } else { $remoteContext.Repository }
$defaultBranch = if ($queue.repository.defaultBranch) {
    [string]$queue.repository.defaultBranch
}
elseif ($ghAuthRequired) {
    [string](Get-RepositoryMetadata -GhPath $ghCliPath -RepositoryRoot $repositoryRoot -Owner $owner -Repository $repository).default_branch
}
else {
    'main'
}

if ([string]::IsNullOrWhiteSpace($WorktreeRoot)) {
    $WorktreeRoot = Get-DefaultWorktreeRoot -RepositoryRoot $repositoryRoot
}

if ([string]::IsNullOrWhiteSpace($RunArtifactsRoot)) {
    $RunArtifactsRoot = Join-Path $gitCommonDirectory 'copilot\issue-runs'
}

$resolvedRunArtifactsRoot = [System.IO.Path]::GetFullPath($RunArtifactsRoot)
if (-not (Test-Path -LiteralPath $resolvedRunArtifactsRoot)) {
    New-Item -ItemType Directory -Path $resolvedRunArtifactsRoot -Force | Out-Null
}

$copilotCliPath = Resolve-CopilotCliPath
$powerShellPath = Resolve-PowerShellPath
$copilotLauncherPath = [System.IO.Path]::GetFullPath((Join-Path $repositoryRoot 'eng\Invoke-CopilotPrompt.ps1'))

$queueItems = @($queue.items)
$eligibleStatuses = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
foreach ($status in 'queued', 'launch-failed') {
    [void]$eligibleStatuses.Add($status)
}

$selectedItems = @(
    $queueItems |
    Where-Object {
        $status = if ($_.status) { [string]$_.status } else { 'queued' }
        if (-not $eligibleStatuses.Contains($status)) {
            return $false
        }

        if ($IssueNumber.Count -gt 0 -and -not ($IssueNumber -contains [int]$_.number)) {
            return $false
        }

        $true
    } |
    Sort-Object -Property number |
    Select-Object -First $MaxAgents
)

if ($selectedItems.Count -eq 0) {
    Write-Host "No queue entries were eligible in $resolvedQueuePath"
    return
}

$launched = New-Object System.Collections.Generic.List[object]

foreach ($item in $selectedItems) {
    Ensure-Property -Object $item -Name 'claim' -Value $null
    Ensure-Property -Object $item -Name 'agent' -Value $null
    Ensure-Property -Object $item -Name 'lastError' -Value $null

    $issueNumberValue = [int]$item.number
    $branchName = [string]$item.branchName
    $worktreeName = [string]$item.worktreeName
        $shouldExecute = $PSCmdlet.ShouldProcess("issue #$issueNumberValue", 'Claim remote branch, mark issue in progress, and launch Copilot')

        if ($WhatIfPreference) {
            $approvalMode = if ($Interactive.IsPresent) { 'interactive approvals' } else { 'full auto approval' }
            $visibilityMode = if ($ShowWindow.IsPresent) { 'visible PowerShell window (kept open)' } else { 'background PowerShell process' }
            Write-Host "What if: would claim issue #$issueNumberValue on branch $branchName, create worktree $worktreeName, and launch Copilot with model $Model, reasoning $ReasoningEffort, $approvalMode, and $visibilityMode"
            continue
        }

    try {
        $issue = Get-IssueDetails -GhPath $ghCliPath -RepositoryRoot $repositoryRoot -Owner $owner -Repository $repository -Number $issueNumberValue
        $issueLabels = @($issue.labels | ForEach-Object { [string]$_.name })
        if ($issueLabels -contains $SkipLabel) {
            $item.status = 'skipped'
            $item.lastError = "Issue has skip label '$SkipLabel'."
            Save-Queue -Queue $queue -Path $resolvedQueuePath
            Write-Host "Skipping issue #$issueNumberValue because it has label '$SkipLabel'."
            continue
        }

        if (-not $shouldExecute) {
            continue
        }

        $claimResult = Get-ClaimBranchForLaunch -RepositoryRoot $repositoryRoot -QueueItem $item -BranchName $branchName -BaseBranch $defaultBranch
        if (-not $claimResult.Claimed) {
            $item.status = 'claimed-elsewhere'
            $item.claim = [pscustomobject]@{
                branchName = $branchName
                observedAtUtc = [DateTimeOffset]::UtcNow.ToString('O')
                reason = 'remote-branch-exists'
            }
            $item.lastError = $null
            Save-Queue -Queue $queue -Path $resolvedQueuePath
            Write-Host "Skipping issue #$issueNumberValue because branch '$branchName' already exists on origin."
            continue
        }

        $worktreePath = Ensure-Worktree -RepositoryRoot $repositoryRoot -WorktreeRoot $WorktreeRoot -WorktreeName $worktreeName -BranchName $branchName -BaseBranch $defaultBranch

        Ensure-RepositoryLabel -GhPath $ghCliPath -RepositoryRoot $repositoryRoot -Owner $owner -Repository $repository -LabelName $InProgressLabel
        Add-IssueLabel -GhPath $ghCliPath -RepositoryRoot $repositoryRoot -Owner $owner -Repository $repository -Number $issueNumberValue -LabelName $InProgressLabel

        $runDirectory = Join-Path $resolvedRunArtifactsRoot ("issue-{0}" -f $issueNumberValue)
        if (-not (Test-Path -LiteralPath $runDirectory)) {
            New-Item -ItemType Directory -Path $runDirectory -Force | Out-Null
        }

        $prompt = New-AgentPrompt -Issue $issue -BranchName $branchName
        $promptPath = Join-Path $runDirectory 'prompt.txt'
        $transcriptPath = Join-Path $runDirectory 'transcript.md'
        $prompt | Set-Content -Path $promptPath -Encoding UTF8

        $stdoutPath = Join-Path $runDirectory 'stdout.log'
        $stderrPath = Join-Path $runDirectory 'stderr.log'
        foreach ($logPath in @($stdoutPath, $stderrPath)) {
            if (Test-Path -LiteralPath $logPath) {
                Remove-Item -LiteralPath $logPath -Force
            }
        }

        $launcherArguments = New-Object System.Collections.Generic.List[string]
        $launcherArguments.Add('-NoLogo')
        $launcherArguments.Add('-NoProfile')
        if ($ShowWindow.IsPresent) {
            $launcherArguments.Add('-NoExit')
        }
        $launcherArguments.Add('-File')
        $launcherArguments.Add($copilotLauncherPath)
        $launcherArguments.Add('-CopilotCliPath')
        $launcherArguments.Add($copilotCliPath)
        $launcherArguments.Add('-PromptPath')
        $launcherArguments.Add($promptPath)
        $launcherArguments.Add('-TranscriptPath')
        $launcherArguments.Add($transcriptPath)
        $launcherArguments.Add('-Model')
        $launcherArguments.Add($Model)
        $launcherArguments.Add('-ReasoningEffort')
        $launcherArguments.Add($ReasoningEffort)

        if (-not [string]::IsNullOrWhiteSpace($Agent)) {
            $launcherArguments.Add('-Agent')
            $launcherArguments.Add($Agent)
        }

        if ($Interactive.IsPresent) {
            $launcherArguments.Add('-Interactive')
        }

        $launcherArgumentText = [string]::Join(' ', @($launcherArguments | ForEach-Object { ConvertTo-QuotedArgument -Value $_ }))

        $commentBody = @"
Copilot issue automation claimed this issue.

- Branch: ``$branchName``
- Worktree: ``$worktreeName``
- Started: $([DateTimeOffset]::UtcNow.ToString('O'))

Please keep follow-up changes on this branch until review is complete.
"@
        Add-IssueComment -GhPath $ghCliPath -RepositoryRoot $repositoryRoot -Owner $owner -Repository $repository -Number $issueNumberValue -Body $commentBody

        if ($ShowWindow.IsPresent) {
            $process = Start-Process -FilePath $powerShellPath -ArgumentList $launcherArgumentText -WorkingDirectory $worktreePath -WindowStyle Normal -PassThru
        }
        else {
            $process = Start-Process -FilePath $powerShellPath -ArgumentList $launcherArgumentText -WorkingDirectory $worktreePath -RedirectStandardOutput $stdoutPath -RedirectStandardError $stderrPath -PassThru
        }

        $item.status = 'launched'
        $item.claim = [pscustomobject]@{
            branchName = $branchName
            worktreePath = $worktreePath
            claimedAtUtc = [DateTimeOffset]::UtcNow.ToString('O')
            baseBranch = $defaultBranch
            baseSha = $claimResult.BaseSha
        }
        $item.agent = [pscustomobject]@{
            processId = [int]$process.Id
            mode = if ($Interactive.IsPresent) { 'interactive' } else { 'autonomous' }
            launchMode = if ($ShowWindow.IsPresent) { 'window' } else { 'background' }
            promptPath = $promptPath
            transcriptPath = $transcriptPath
            stdoutPath = if ($ShowWindow.IsPresent) { $null } else { $stdoutPath }
            stderrPath = if ($ShowWindow.IsPresent) { $null } else { $stderrPath }
            startedAtUtc = [DateTimeOffset]::UtcNow.ToString('O')
        }
        $item.lastError = $null
        Save-Queue -Queue $queue -Path $resolvedQueuePath

        $launched.Add([pscustomobject]@{
            number = $issueNumberValue
            branch = $branchName
            worktree = $worktreePath
            processId = [int]$process.Id
        })
    }
    catch {
        $item.status = 'launch-failed'
        $item.lastError = $_.Exception.Message
        Save-Queue -Queue $queue -Path $resolvedQueuePath
        throw
    }
}

if ($PassThru.IsPresent) {
    $launched
    return
}

foreach ($entry in $launched) {
    Write-Host ("Launched issue #{0} on branch {1} in {2} (PID {3})" -f $entry.number, $entry.branch, $entry.worktree, $entry.processId)
}
