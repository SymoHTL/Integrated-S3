<#
.SYNOPSIS
Exports GitHub issues for the current repository into a local Copilot queue file.

.DESCRIPTION
Fetches issues through the GitHub CLI for the repository behind the current
origin remote, filters them, and writes a JSON queue file that can be consumed
by Start-IssueAgentQueue.ps1.

.EXAMPLE
.\eng\Get-IssueAgentQueue.ps1 -Limit 10

.EXAMPLE
.\eng\Get-IssueAgentQueue.ps1 -IncludeLabel TrackH -OutputPath '.git\copilot\trackh-queue.json'
#>
[CmdletBinding()]
param(
    [Parameter()]
    [string]$Owner,

    [Parameter()]
    [string]$Repository,

    [Parameter()]
    [ValidateSet('open', 'closed', 'all')]
    [string]$State = 'open',

    [Parameter()]
    [string[]]$IncludeLabel = @(),

    [Parameter()]
    [string[]]$ExcludeLabel = @('copilot:skip', 'copilot:done'),

    [Parameter()]
    [int]$Limit = 0,

    [Parameter()]
    [string]$OutputPath,

    [Parameter()]
    [switch]$IncludeBody
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Invoke-Git {
    param(
        [Parameter(Mandatory)]
        [string]$RepositoryRoot,

        [Parameter(Mandatory)]
        [string[]]$Arguments
    )

    $output = & git -C $RepositoryRoot @Arguments 2>&1
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        $commandText = "git -C `"$RepositoryRoot`" $($Arguments -join ' ')"
        throw "$commandText failed with exit code $exitCode.`n$($output -join [Environment]::NewLine)"
    }

    return ,@($output | ForEach-Object { $_.ToString().TrimEnd() })
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

    $output = Invoke-Git -RepositoryRoot $RepositoryRoot -Arguments @('rev-parse', '--git-common-dir')
    $gitCommonDirectory = $output[0]
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

    $remoteUrl = (Invoke-Git -RepositoryRoot $RepositoryRoot -Arguments @('remote', 'get-url', 'origin'))[0]
    $normalizedRemoteUrl = $remoteUrl.Trim()

    $patterns = @(
        '^(?:https://|http://)github\.com/(?<owner>[^/]+)/(?<repo>[^/]+?)(?:\.git)?/?$',
        '^(?:git@|ssh://git@)github\.com[:/](?<owner>[^/]+)/(?<repo>[^/]+?)(?:\.git)?/?$'
    )

    foreach ($pattern in $patterns) {
        $match = [regex]::Match($normalizedRemoteUrl, $pattern, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
        if ($match.Success) {
            return [pscustomobject]@{
                Owner = $match.Groups['owner'].Value
                Repository = $match.Groups['repo'].Value
                RemoteUrl = $normalizedRemoteUrl
            }
        }
    }

    throw "Could not infer the GitHub owner and repository from origin remote '$normalizedRemoteUrl'."
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

function Get-Issues {
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
        [string]$State,

        [Parameter()]
        [string[]]$IncludeLabel
    )

    $issues = New-Object System.Collections.Generic.List[object]

    $labelFilter = @($IncludeLabel | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $encodedLabelFilter = if ($labelFilter.Count -gt 0) {
        [System.Uri]::EscapeDataString([string]::Join(',', $labelFilter))
    }
    else {
        $null
    }

    $queryParts = @(
        "state=$State",
        'per_page=100',
        'sort=created',
        'direction=asc'
    )

    if (-not [string]::IsNullOrWhiteSpace($encodedLabelFilter)) {
        $queryParts += "labels=$encodedLabelFilter"
    }

    $endpoint = "/repos/$Owner/$Repository/issues?{0}" -f ($queryParts -join '&')
    $pages = @(Invoke-GhJson -GhPath $GhPath -WorkingDirectory $RepositoryRoot -Arguments @(
        'api', '--paginate', '--slurp', $endpoint
    ))

    foreach ($page in $pages) {
        foreach ($issue in @($page)) {
            if ($issue.PSObject.Properties.Match('pull_request').Count -eq 0) {
                $issues.Add($issue)
            }
        }
    }

    return $issues
}

function ConvertTo-BranchSlug {
    param(
        [Parameter(Mandatory)]
        [string]$Title,

        [Parameter()]
        [int]$MaxLength = 48
    )

    $slug = $Title.ToLowerInvariant()
    $slug = [regex]::Replace($slug, '[^a-z0-9]+', '-')
    $slug = $slug.Trim('-')

    if ([string]::IsNullOrWhiteSpace($slug)) {
        return 'issue'
    }

    if ($slug.Length -gt $MaxLength) {
        return $slug.Substring(0, $MaxLength).Trim('-')
    }

    return $slug
}

function Get-BodyPreview {
    param(
        [Parameter()]
        [AllowNull()]
        [string]$Body
    )

    if ([string]::IsNullOrWhiteSpace($Body)) {
        return $null
    }

    $normalized = $Body.Replace("`r", '').Replace("`n", ' ')
    if ($normalized.Length -le 240) {
        return $normalized
    }

    return $normalized.Substring(0, 240).TrimEnd() + '...'
}

function Resolve-OutputPath {
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

$repositoryRoot = Get-RepositoryRoot
$gitCommonDirectory = Get-GitCommonDirectory -RepositoryRoot $repositoryRoot
$remoteContext = Get-RemoteContext -RepositoryRoot $repositoryRoot
$ghCliPath = Resolve-GhCliPath
Assert-GhAuthenticated -GhPath $ghCliPath -WorkingDirectory $repositoryRoot

if ([string]::IsNullOrWhiteSpace($Owner)) {
    $Owner = $remoteContext.Owner
}

if ([string]::IsNullOrWhiteSpace($Repository)) {
    $Repository = $remoteContext.Repository
}

$repositoryMetadata = Get-RepositoryMetadata -GhPath $ghCliPath -RepositoryRoot $repositoryRoot -Owner $Owner -Repository $Repository
$outputPath = Resolve-OutputPath -RepositoryRoot $repositoryRoot -GitCommonDirectory $gitCommonDirectory -RequestedPath $OutputPath

$excludeLabelsLookup = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
foreach ($label in @($ExcludeLabel | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })) {
    [void]$excludeLabelsLookup.Add($label.Trim())
}

$includeLabelsLookup = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
foreach ($label in @($IncludeLabel | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })) {
    [void]$includeLabelsLookup.Add($label.Trim())
}

$rawIssues = Get-Issues -GhPath $ghCliPath -RepositoryRoot $repositoryRoot -Owner $Owner -Repository $Repository -State $State -IncludeLabel $IncludeLabel
$filteredIssues = foreach ($issue in $rawIssues) {
    $issueLabels = @($issue.labels | ForEach-Object { $_.name })

    $hasExcludedLabel = $false
    foreach ($label in $issueLabels) {
        if ($excludeLabelsLookup.Contains($label)) {
            $hasExcludedLabel = $true
            break
        }
    }

    if ($hasExcludedLabel) {
        continue
    }

    if ($includeLabelsLookup.Count -gt 0) {
        $allIncluded = $true
        foreach ($label in $includeLabelsLookup) {
            if (-not ($issueLabels -contains $label)) {
                $allIncluded = $false
                break
            }
        }

        if (-not $allIncluded) {
            continue
        }
    }

    $issue
}

$orderedIssues = @($filteredIssues | Sort-Object -Property number)
if ($Limit -gt 0) {
    $orderedIssues = @($orderedIssues | Select-Object -First $Limit)
}

$items = foreach ($issue in $orderedIssues) {
    $slug = ConvertTo-BranchSlug -Title $issue.title
    [pscustomobject]@{
        number = [int]$issue.number
        title = [string]$issue.title
        url = [string]$issue.html_url
        createdAtUtc = [string]$issue.created_at
        updatedAtUtc = [string]$issue.updated_at
        labels = @($issue.labels | ForEach-Object { [string]$_.name })
        assignees = @($issue.assignees | ForEach-Object { [string]$_.login })
        branchName = "copilot/issue-$($issue.number)-$slug"
        worktreeName = "issue-$($issue.number)-$slug"
        status = 'queued'
        claim = $null
        agent = $null
        lastError = $null
        bodyPreview = Get-BodyPreview -Body ([string]$issue.body)
        body = if ($IncludeBody.IsPresent) { [string]$issue.body } else { $null }
    }
}

$queue = [ordered]@{
    schemaVersion = 1
    generatedAtUtc = [DateTimeOffset]::UtcNow.ToString('O')
    repository = [ordered]@{
        owner = $Owner
        name = $Repository
        remoteUrl = $remoteContext.RemoteUrl
        defaultBranch = [string]$repositoryMetadata.default_branch
        root = $repositoryRoot
    }
    filters = [ordered]@{
        state = $State
        includeLabels = @($IncludeLabel)
        excludeLabels = @($ExcludeLabel)
        limit = $Limit
    }
    items = @($items)
}

$outputDirectory = Split-Path -Parent $outputPath
if (-not [string]::IsNullOrWhiteSpace($outputDirectory) -and -not (Test-Path -LiteralPath $outputDirectory)) {
    New-Item -ItemType Directory -Path $outputDirectory -Force | Out-Null
}

$queue | ConvertTo-Json -Depth 10 | Set-Content -Path $outputPath -Encoding UTF8

Write-Host "Queued $($items.Count) issue(s) from $Owner/$Repository into $outputPath"
Write-Host "Default branch: $($repositoryMetadata.default_branch)"
