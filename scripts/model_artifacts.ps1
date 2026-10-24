#requires -Version 5.1
<# Read-only frozen-model preflight; no Python, Docker, download or training. #>
[CmdletBinding()]
param([string]$ProjectRoot, [string]$RuntimeRoot)

function Stop-FareArtifactCheck {
    param([string]$Reason)
    $message = $Reason + ' Prepare the pinned P4 artifact bundle at the paths in README (ML P5), then run scripts/model_artifacts.ps1 again. No download, training or fallback was attempted.'
    $failure = New-Object System.Exception($message)
    $failure.Data['StartupSafeMessage'] = $message
    throw $failure
}

function Get-FareModelIdentity {
    return [pscustomobject]@{
        TrainingRunId = 'd2772f1904d7472490a73b0ac1c0c5d4'
        EvaluationRunId = '58d5008f0ed048f58599a859877ae336'
        ModelSHA256 = '759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee'
        CandidateLockSHA256 = '12442b5684943bb81169bb084cffc8c1b54043882d75711e295a0f1a0ff7338d'
    }
}

function Resolve-FareArtifactPath {
    param([string]$Root, [string]$RelativePath, [switch]$Directory)
    # Reject junctions/symlinks in every component, not just the leaf. No path
    # from the lock is trusted until the lock's pinned byte hash was checked.
    $absoluteRoot = [IO.Path]::GetFullPath($Root).TrimEnd('\', '/')
    $path = [IO.Path]::GetFullPath((Join-Path $absoluteRoot $RelativePath))
    if (-not $path.StartsWith($absoluteRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
        Stop-FareArtifactCheck 'Artifact path escapes the project root.'
    }
    $current = $absoluteRoot
    foreach ($component in @('') + @($RelativePath.Split(@([char]'/', [char]'\'), [StringSplitOptions]::RemoveEmptyEntries))) {
        if ($component) { $current = Join-Path $current $component }
        if (-not (Test-Path -LiteralPath $current)) { Stop-FareArtifactCheck ('Missing artifact/source: ' + $RelativePath + '.') }
        $item = Get-Item -LiteralPath $current -Force
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
            Stop-FareArtifactCheck ('Symlink/junction is not allowed: ' + $RelativePath + '.')
        }
    }
    if ((Get-Item -LiteralPath $path -Force).PSIsContainer -ne [bool]$Directory) {
        Stop-FareArtifactCheck ('Wrong artifact path type: ' + $RelativePath + '.')
    }
    return $path
}

function Get-FareModelTree {
    param([Parameter(Mandatory=$true)][string]$ModelDirectory)
    $root = [IO.Path]::GetFullPath($ModelDirectory).TrimEnd('\', '/')
    $pending = New-Object 'Collections.Generic.Stack[string]'
    $pending.Push($root)
    # Case-sensitive paths must remain distinct even on a case-sensitive NTFS
    # directory/WSL artifact tree; PowerShell's ordinary hashtable is not ordinal.
    $items = [Collections.Generic.Dictionary[string, IO.FileInfo]]::new([StringComparer]::Ordinal)
    while ($pending.Count -gt 0) {
        $directory = $pending.Pop()
        $parent = Get-Item -LiteralPath $directory -Force
        if (($parent.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { Stop-FareArtifactCheck 'Model tree contains a symlink/junction.' }
        foreach ($item in Get-ChildItem -LiteralPath $directory -Force) {
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { Stop-FareArtifactCheck 'Model tree contains a symlink/junction.' }
            if ($item -is [IO.DirectoryInfo]) { $pending.Push($item.FullName); continue }
            $relative = $item.FullName.Substring($root.Length + 1).Replace('\', '/')
            if ($relative -cmatch '[^\x20-\x7e]') { Stop-FareArtifactCheck 'Model inventory contains an unsupported non-ASCII filename.' }
            $items[$relative] = $item
        }
    }
    [string[]]$names = @($items.Keys)
    [Array]::Sort($names, [StringComparer]::Ordinal)
    $entries = @(foreach ($name in $names) {
        [pscustomobject][ordered]@{
            bytes = [long]$items[$name].Length
            path = $name
            sha256 = (Get-FileHash -LiteralPath $items[$name].FullName -Algorithm SHA256).Hash.ToLowerInvariant()
        }
    })
    # Same algorithm as holdout.tree_identity: ordinal relative POSIX names,
    # compact UTF-8 JSON, sorted keys bytes/path/sha256, including dot/CRC files.
    $canonical = ConvertTo-Json -InputObject $entries -Depth 5 -Compress
    $algorithm = [Security.Cryptography.SHA256]::Create()
    try { $hash = [BitConverter]::ToString($algorithm.ComputeHash([Text.Encoding]::UTF8.GetBytes($canonical))).Replace('-', '').ToLowerInvariant() }
    finally { $algorithm.Dispose() }
    return [pscustomobject]@{sha256=$hash; files=$entries}
}

function Assert-FareArtifacts {
    param([Parameter(Mandatory=$true)][string]$ProjectRoot, [string]$RuntimeRoot)
    # Runtime artifacts may be installed in a dedicated directory. Frozen source
    # hashes always resolve under the actual code root, never the runtime tree.
    if (-not $RuntimeRoot) { $RuntimeRoot = $ProjectRoot }
    $pin = Get-FareModelIdentity
    $lockPath = Resolve-FareArtifactPath $RuntimeRoot 'models/ml-p3/20261023-june01/candidate_lock.json'
    if ((Get-FileHash -LiteralPath $lockPath -Algorithm SHA256).Hash.ToLowerInvariant() -cne $pin.CandidateLockSHA256) {
        Stop-FareArtifactCheck 'Pinned candidate_lock.json SHA256 mismatch.'
    }
    try { $lock = Get-Content -LiteralPath $lockPath -Raw -Encoding UTF8 | ConvertFrom-Json }
    catch { Stop-FareArtifactCheck 'Pinned candidate lock is not valid JSON.' }
    $modelPath = Resolve-FareArtifactPath $RuntimeRoot 'models/ml-p2/20261023-pilot02/fitted_model' -Directory
    $featurePath = Resolve-FareArtifactPath $RuntimeRoot 'models/ml-p2/20261023-pilot02/feature_contract.json'
    $zonesPath = Resolve-FareArtifactPath $RuntimeRoot 'data/raw/taxi_zones/taxi_zone_lookup.csv'
    $observed = Get-FareModelTree $modelPath
    if ($lock.training_run_id -cne $pin.TrainingRunId -or $lock.model_tree.sha256 -cne $pin.ModelSHA256 -or
        $observed.sha256 -cne $pin.ModelSHA256 -or @($observed.files).Count -ne 40) {
        Stop-FareArtifactCheck 'Frozen model identity/tree SHA256 mismatch (expected exactly 40 files).'
    }
    foreach ($entry in @(@{Path=$featurePath; Expected=$lock.feature_contract_sha256; Label='feature_contract.json'},
                           @{Path=$zonesPath; Expected=$lock.zone_lookup_sha256; Label='zone lookup CSV'})) {
        if ((Get-FileHash -LiteralPath $entry.Path -Algorithm SHA256).Hash.ToLowerInvariant() -cne $entry.Expected) {
            Stop-FareArtifactCheck ($entry.Label + ' SHA256 mismatch.')
        }
    }
    foreach ($entry in $lock.core_file_sha256.PSObject.Properties) {
        $sourcePath = Resolve-FareArtifactPath $ProjectRoot $entry.Name
        if ((Get-FileHash -LiteralPath $sourcePath -Algorithm SHA256).Hash.ToLowerInvariant() -cne $entry.Value) {
            Stop-FareArtifactCheck ('Frozen preprocessing/dependency source mismatch: ' + $entry.Name + '. Restore the exact frozen bytes; do not update the lock/hash to bypass this check.')
        }
    }
    return [pscustomobject]@{Verified=$true;ModelSHA256=$pin.ModelSHA256;TrainingRunId=$pin.TrainingRunId;
        EvaluationRunId=$pin.EvaluationRunId;ModelDirectory=$modelPath;CandidateLockPath=$lockPath;
        FeatureContractPath=$featurePath;ZonesPath=$zonesPath}
}

if ($MyInvocation.InvocationName -ne '.') {
    $ErrorActionPreference = 'Stop'
    try {
        if (-not $ProjectRoot) { $ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path) }
        $verified = Assert-FareArtifacts -ProjectRoot $ProjectRoot -RuntimeRoot $RuntimeRoot
        Write-Host ('Frozen artifacts verified (40 files). Model SHA256: ' + $verified.ModelSHA256)
        Write-Host ('Training: ' + $verified.TrainingRunId + '; evaluation: ' + $verified.EvaluationRunId)
        exit 0
    } catch {
        $safe = $_.Exception.Data['StartupSafeMessage']
        if (-not $safe) { $safe = 'Artifact check failed. Check required paths/read permissions and the README ML P5 artifact preparation guide; no startup was attempted.' }
        Write-Host ('ERROR: ' + $safe)
        exit 1
    }
}
