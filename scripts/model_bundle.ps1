#requires -Version 5.1
<# Local, explicit frozen-model packaging. No network, training or publication.
   ManifestSHA256 must come from an independently trusted version lock/caller;
   accepting the manifest's own checksum would not authenticate the archive. #>
[CmdletBinding()]
param(
    [ValidateSet('Check','Create','Install')][string]$Action = 'Check',
    [string]$ProjectRoot,
    [string]$RuntimeRoot,
    [string]$OutputDirectory,
    [string]$BundlePath,
    [string]$ManifestPath,
    [string]$ManifestSHA256,
    [switch]$Execute
)

$script:PackageDefaultProjectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
. (Join-Path $PSScriptRoot 'model_artifacts.ps1') -ProjectRoot $ProjectRoot -RuntimeRoot $RuntimeRoot

function Stop-PackageBundle {
    param([string]$Reason)
    $failure = New-Object Exception($Reason)
    $failure.Data['PackageSafeMessage'] = $Reason
    throw $failure
}

function Get-PackageBundleSpec {
    return [pscustomobject]@{
        Version='ml-p4-v1'; Schema='nyc-taxi-frozen-model-bundle-v1'
        Archive='nyc-taxi-model-ml-p4-v1.zip'; Manifest='nyc-taxi-model-ml-p4-v1.manifest.json'
        Checksums='nyc-taxi-model-ml-p4-v1.sha256'
        ModelRoot='models/ml-p2/20261023-pilot02/fitted_model'
        Feature='models/ml-p2/20261023-pilot02/feature_contract.json'
        Lock='models/ml-p3/20261023-june01/candidate_lock.json'
        Zones='data/raw/taxi_zones/taxi_zone_lookup.csv'
        MaxArchiveBytes=1048576; MaxExpandedBytes=1048576; MaxFileBytes=262144; MaxManifestBytes=131072
    }
}

function Get-PackageBytesHash {
    param([byte[]]$Bytes)
    $algorithm = [Security.Cryptography.SHA256]::Create()
    try { return [BitConverter]::ToString($algorithm.ComputeHash($Bytes)).Replace('-','').ToLowerInvariant() }
    finally { $algorithm.Dispose() }
}

function Assert-PackageLocalPath {
    param([string]$Path, [switch]$AllowMissing, [switch]$Directory)
    if (-not $Path -or $Path -match '^[a-zA-Z]+://' -or $Path.StartsWith('\\')) {
        Stop-PackageBundle 'Only explicit local filesystem paths are accepted.'
    }
    try { $full = [IO.Path]::GetFullPath($Path) }
    catch { Stop-PackageBundle 'Invalid local package path.' }
    if ($full.StartsWith('\\')) { Stop-PackageBundle 'Only explicit local filesystem paths are accepted.' }
    $current = $full
    while ($current) {
        if (Test-Path -LiteralPath $current) {
            $item = Get-Item -LiteralPath $current -Force
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                Stop-PackageBundle 'Symlink/junction package paths are not allowed.'
            }
        }
        $parent = Split-Path -Parent $current
        if (-not $parent -or $parent -eq $current) { break }
        $current = $parent
    }
    if (Test-Path -LiteralPath $full) {
        $item = Get-Item -LiteralPath $full -Force
        if ($item.PSIsContainer -ne [bool]$Directory) { Stop-PackageBundle 'Package path has the wrong file/directory type.' }
    } elseif (-not $AllowMissing) { Stop-PackageBundle 'Required local package path is missing.' }
    return $full
}

function Assert-PackageRelativePath {
    param([string]$Path)
    if (-not $Path -or $Path -cmatch '[^\x21-\x7e]' -or $Path.Contains('\') -or $Path.Contains(':') -or $Path.StartsWith('/') -or $Path.EndsWith('/')) {
        Stop-PackageBundle 'Unsafe archive entry path.'
    }
    foreach ($part in $Path.Split('/')) {
        if (-not $part -or $part -in @('.','..') -or $part.EndsWith('.') -or $part.EndsWith(' ') -or
            $part -match '^(?i:CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\.|$)' -or $part -match '[<>"|?*]') {
            Stop-PackageBundle 'Unsafe archive entry path.'
        }
    }
}

function Read-PackageLocalBytes {
    param([string]$Path, [long]$Limit)
    $file = Assert-PackageLocalPath -Path $Path
    if ((Get-Item -LiteralPath $file -Force).Length -gt $Limit) { Stop-PackageBundle 'Package input exceeds the bounded size limit.' }
    [byte[]]$bytes = [IO.File]::ReadAllBytes($file)
    if ($bytes.Length -gt $Limit) { Stop-PackageBundle 'Package input exceeds the bounded size limit.' }
    return ,$bytes
}

function ConvertFrom-PackageBytes {
    param([byte[]]$Bytes)
    try {
        $encoding = New-Object Text.UTF8Encoding($false, $true)
        $value = $encoding.GetString($Bytes) | ConvertFrom-Json
    } catch { Stop-PackageBundle 'Package JSON is not valid bounded UTF-8 JSON.' }
    if ($value -isnot [pscustomobject]) { Stop-PackageBundle 'Package JSON must be an object.' }
    return $value
}

function Read-PackageZipEntry {
    param($Entry, [long]$Limit)
    # Update-mode entries can change their exposed Length while Open() is active;
    # validate against the immutable advertised length captured before opening.
    [long]$expectedLength = $Entry.Length
    if ($expectedLength -gt $Limit) { Stop-PackageBundle 'Archive entry exceeds the bounded size limit.' }
    $stream = $Entry.Open()
    $buffer = New-Object byte[] 8192
    $memory = New-Object IO.MemoryStream
    try {
        while (($read = $stream.Read($buffer, 0, $buffer.Length)) -gt 0) {
            if ($memory.Length + $read -gt $Limit) { Stop-PackageBundle 'Expanded archive data exceeds the bounded size limit.' }
            $memory.Write($buffer, 0, $read)
        }
        [byte[]]$bytes = $memory.ToArray()
        if ($bytes.Length -ne $expectedLength) { Stop-PackageBundle 'Archive entry length mismatch.' }
        return ,$bytes
    } finally { $stream.Dispose(); $memory.Dispose() }
}

function Assert-PackageInventoryRecord {
    param($Record)
    if ($null -eq $Record -or $Record.path -isnot [string]) { Stop-PackageBundle 'Invalid package file inventory.' }
    Assert-PackageRelativePath $Record.path
    if (($Record.bytes -isnot [int] -and $Record.bytes -isnot [long]) -or $Record.bytes -lt 0 -or
        $Record.bytes -gt (Get-PackageBundleSpec).MaxFileBytes -or
        $Record.sha256 -isnot [string] -or $Record.sha256 -cnotmatch '^[0-9a-f]{64}$') {
        Stop-PackageBundle 'Invalid package file inventory size/hash.'
    }
}

function Assert-PackageSourceHashes {
    param([string]$ProjectRoot, $Lock)
    $names = @('src/nyc_taxi/ml/pilot.py','src/nyc_taxi/ml/pilot_data.py',
        'src/nyc_taxi/ml/pilot_model.py','src/nyc_taxi/ml/pilot_tracking.py',
        'src/nyc_taxi/etl/silver.py','requirements.txt','requirements-worker-constraints.txt','pyproject.toml')
    $actual = @($Lock.core_file_sha256.PSObject.Properties)
    if ($actual.Count -ne $names.Count) { Stop-PackageBundle 'Frozen source hash inventory is incomplete.' }
    foreach ($name in $names) {
        $expected = $Lock.core_file_sha256.PSObject.Properties[$name]
        if ($null -eq $expected -or $expected.Value -cnotmatch '^[0-9a-f]{64}$') { Stop-PackageBundle 'Frozen source hash inventory is incomplete.' }
        $source = Resolve-FareArtifactPath $ProjectRoot $name
        if ((Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash.ToLowerInvariant() -cne $expected.Value) {
            Stop-PackageBundle 'Frozen source/dependency hash mismatch; restore exact reviewed bytes.'
        }
    }
}

function Assert-PackageBundle {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)][string]$ProjectRoot,
        [Parameter(Mandatory=$true)][string]$BundlePath,
        [Parameter(Mandatory=$true)][string]$ManifestPath,
        [Parameter(Mandatory=$true)][string]$ManifestSHA256
    )
    if ($ManifestSHA256 -cnotmatch '^[0-9a-f]{64}$') { Stop-PackageBundle 'An independently trusted full manifest SHA256 is required.' }
    $spec = Get-PackageBundleSpec
    $pin = Get-FareModelIdentity
    $root = Assert-PackageLocalPath $ProjectRoot -Directory
    $archivePath = Assert-PackageLocalPath $BundlePath
    $manifestFile = Assert-PackageLocalPath $ManifestPath
    [byte[]]$manifestBytes = Read-PackageLocalBytes $manifestFile $spec.MaxManifestBytes
    if ((Get-PackageBytesHash $manifestBytes) -cne $ManifestSHA256) { Stop-PackageBundle 'Pinned bundle manifest SHA256 mismatch.' }
    $manifest = ConvertFrom-PackageBytes $manifestBytes
    # Distribution notices may advance without changing any of the 43 payload
    # bytes. Both schemas remain independently pinned by the caller's manifest SHA.
    $localNotice=$manifest.distribution.status -ceq 'local-only-unpublished' -and $manifest.distribution.redistribution_rights -ceq 'unverified'
    $bundledNotice=$manifest.distribution.status -ceq 'repository-bundled' -and $manifest.distribution.redistribution_rights -ceq 'project-generated-model-and-nyc-open-data-notices'
    if ($manifest.schema -cne $spec.Schema -or $manifest.version -cne $spec.Version -or
        $manifest.training_run_id -cne $pin.TrainingRunId -or $manifest.evaluation_run_id -cne $pin.EvaluationRunId -or
        $manifest.model_tree_sha256 -cne $pin.ModelSHA256 -or $manifest.candidate_lock_sha256 -cne $pin.CandidateLockSHA256 -or
        $manifest.archive.file -cne $spec.Archive -or (-not $localNotice -and -not $bundledNotice)) { Stop-PackageBundle 'Bundle schema/version/frozen identity mismatch.' }
    if (($manifest.archive.bytes -isnot [int] -and $manifest.archive.bytes -isnot [long]) -or
        $manifest.archive.bytes -le 0 -or $manifest.archive.bytes -gt $spec.MaxArchiveBytes -or
        $manifest.archive.sha256 -isnot [string] -or $manifest.archive.sha256 -cnotmatch '^[0-9a-f]{64}$') {
        Stop-PackageBundle 'Invalid archive size/hash metadata.'
    }
    [byte[]]$archiveBytes = Read-PackageLocalBytes $archivePath $spec.MaxArchiveBytes
    $archiveHash = Get-PackageBytesHash $archiveBytes
    if ($archiveBytes.Length -ne $manifest.archive.bytes -or $archiveHash -cne $manifest.archive.sha256) { Stop-PackageBundle 'Bundle archive SHA256/size mismatch.' }
    $inventory = [Collections.Generic.Dictionary[string,object]]::new([StringComparer]::Ordinal)
    $caseNames = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    if (@($manifest.files).Count -ne 43) { Stop-PackageBundle 'Bundle must contain exactly 43 allowlisted files.' }
    foreach ($record in $manifest.files) {
        Assert-PackageInventoryRecord $record
        if (-not $caseNames.Add($record.path)) { Stop-PackageBundle 'Duplicate/case-colliding manifest paths are not allowed.' }
        $inventory.Add($record.path, $record)
    }
    Add-Type -AssemblyName System.IO.Compression
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $memory = [IO.MemoryStream]::new($archiveBytes,$false)
    $zip = [IO.Compression.ZipArchive]::new($memory,[IO.Compression.ZipArchiveMode]::Read,$false)
    try {
        if ($zip.Entries.Count -ne 43) { Stop-PackageBundle 'Bundle must contain exactly 43 allowlisted files.' }
        $contents = [Collections.Generic.Dictionary[string,byte[]]]::new([StringComparer]::Ordinal)
        $entryNames = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
        [long]$expanded = 0
        foreach ($entry in $zip.Entries) {
            Assert-PackageRelativePath $entry.FullName
            $unixType = (($entry.ExternalAttributes -shr 16) -band 61440)
            if ($unixType -notin @(0,32768) -or ($entry.ExternalAttributes -band 1024) -ne 0) { Stop-PackageBundle 'Archive symlinks/special entries are not allowed.' }
            if (-not $entryNames.Add($entry.FullName)) { Stop-PackageBundle 'Duplicate/case-colliding archive entries are not allowed.' }
            if (-not $inventory.ContainsKey($entry.FullName)) { Stop-PackageBundle 'Unexpected archive entry is not allowlisted.' }
            $record = $inventory[$entry.FullName]
            if ($entry.Length -ne $record.bytes) { Stop-PackageBundle 'Archive entry size differs from manifest.' }
            $expanded += $entry.Length
            if ($expanded -gt $spec.MaxExpandedBytes) { Stop-PackageBundle 'Expanded archive exceeds the bounded size limit.' }
            [byte[]]$bytes = Read-PackageZipEntry $entry $spec.MaxFileBytes
            if ((Get-PackageBytesHash $bytes) -cne $record.sha256) { Stop-PackageBundle 'Archive entry SHA256 mismatch.' }
            $contents.Add($entry.FullName, $bytes)
        }
        if (-not $contents.ContainsKey($spec.Lock) -or (Get-PackageBytesHash $contents[$spec.Lock]) -cne $pin.CandidateLockSHA256) {
            Stop-PackageBundle 'Pinned candidate-lock bytes are missing or mismatched.'
        }
        $lock = ConvertFrom-PackageBytes $contents[$spec.Lock]
        if ($lock.training_run_id -cne $pin.TrainingRunId -or $lock.model_tree.sha256 -cne $pin.ModelSHA256 -or @($lock.model_tree.files).Count -ne 40) {
            Stop-PackageBundle 'Frozen model lock identity/inventory mismatch.'
        }
        $expected = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
        foreach ($file in $lock.model_tree.files) {
            Assert-PackageInventoryRecord $file
            $path = $spec.ModelRoot + '/' + $file.path
            if (-not $expected.Add($path) -or -not $inventory.ContainsKey($path) -or
                $inventory[$path].bytes -ne $file.bytes -or $inventory[$path].sha256 -cne $file.sha256) {
                Stop-PackageBundle 'Model entries differ from the independently pinned lock inventory.'
            }
        }
        foreach ($extra in @(@{Path=$spec.Lock; Hash=$pin.CandidateLockSHA256},
            @{Path=$spec.Feature; Hash=$lock.feature_contract_sha256}, @{Path=$spec.Zones; Hash=$lock.zone_lookup_sha256})) {
            if (-not $expected.Add($extra.Path) -or -not $inventory.ContainsKey($extra.Path) -or $inventory[$extra.Path].sha256 -cne $extra.Hash) {
                Stop-PackageBundle 'Contract/zone/lock entries differ from the pinned artifact hashes.'
            }
        }
        if ($expected.Count -ne 43 -or $inventory.Count -ne $expected.Count) { Stop-PackageBundle 'Archive file allowlist differs from the frozen artifact bundle.' }
        Assert-PackageSourceHashes -ProjectRoot $root -Lock $lock
        return [pscustomobject]@{Verified=$true; Version=$spec.Version; BundlePath=$archivePath; ManifestPath=$manifestFile;
            BundleSHA256=$archiveHash; BundleBytes=[long]$archiveBytes.Length; ManifestSHA256=$ManifestSHA256;
            ManifestBytes=[long]$manifestBytes.Length; ModelSHA256=$pin.ModelSHA256;
            TrainingRunId=$pin.TrainingRunId; EvaluationRunId=$pin.EvaluationRunId; Files=@($manifest.files)}
    } finally { $zip.Dispose(); $memory.Dispose() }
}

function Get-PackageModelBundle {
    [CmdletBinding()]
    param([Parameter(Mandatory=$true)][string]$ProjectRoot,
        [Parameter(Mandatory=$true)][string]$BundlePath,
        [Parameter(Mandatory=$true)][string]$ManifestPath,
        [Parameter(Mandatory=$true)][string]$ManifestSHA256)
    return Assert-PackageBundle @PSBoundParameters
}

function New-PackageModelBundle {
    [CmdletBinding()]
    param([Parameter(Mandatory=$true)][string]$ProjectRoot,
        [Parameter(Mandatory=$true)][string]$OutputDirectory, [string]$RuntimeRoot)
    $spec = Get-PackageBundleSpec
    $root = Assert-PackageLocalPath $ProjectRoot -Directory
    if (-not $RuntimeRoot) { $RuntimeRoot = $root }
    $verified = Assert-FareArtifacts -ProjectRoot $root -RuntimeRoot $RuntimeRoot
    $output = Assert-PackageLocalPath $OutputDirectory -AllowMissing -Directory
    $allowedParent = [IO.Path]::GetFullPath((Join-Path $root '.sandbox/package-bundles'))
    if ($output -cne [IO.Path]::Combine($allowedParent, $spec.Version)) { Stop-PackageBundle 'Bundle output must be the fixed .sandbox/package-bundles/ml-p4-v1 directory.' }
    if (Test-Path -LiteralPath $output) { Stop-PackageBundle 'Versioned bundle output already exists; no overwrite is allowed.' }
    $null = New-Item -ItemType Directory -Path $allowedParent -Force
    $null = Assert-PackageLocalPath $allowedParent -Directory
    $stage = Join-Path $allowedParent ('.creating-' + [guid]::NewGuid().ToString('N'))
    $null = New-Item -ItemType Directory -Path $stage
    # Failure deliberately retains this private stage for inspection; no broad delete.
    $tree = Get-FareModelTree $verified.ModelDirectory
    $records = [Collections.Generic.Dictionary[string,object]]::new([StringComparer]::Ordinal)
    foreach ($file in $tree.files) {
        $path = $spec.ModelRoot + '/' + $file.path
        $records.Add($path,[pscustomobject][ordered]@{bytes=[long]$file.bytes;path=$path;sha256=$file.sha256})
    }
    foreach ($relative in @($spec.Feature,$spec.Lock,$spec.Zones)) {
        $source = Resolve-FareArtifactPath $RuntimeRoot $relative
        $records.Add($relative,[pscustomobject][ordered]@{bytes=[long](Get-Item -LiteralPath $source).Length;
            path=$relative;sha256=(Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash.ToLowerInvariant()})
    }
    [string[]]$names = @($records.Keys)
    [Array]::Sort($names,[StringComparer]::Ordinal)
    Add-Type -AssemblyName System.IO.Compression
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $zipPath = Join-Path $stage $spec.Archive
    $zip = [IO.Compression.ZipFile]::Open($zipPath,[IO.Compression.ZipArchiveMode]::Create)
    try {
        foreach ($name in $names) {
            Assert-PackageInventoryRecord $records[$name]
            $source = Resolve-FareArtifactPath $RuntimeRoot $name
            [byte[]]$bytes = Read-PackageLocalBytes $source $spec.MaxFileBytes
            if ($bytes.Length -ne $records[$name].bytes -or (Get-PackageBytesHash $bytes) -cne $records[$name].sha256) {
                Stop-PackageBundle 'Source artifact changed during packaging.'
            }
            $entry = $zip.CreateEntry($name,[IO.Compression.CompressionLevel]::Optimal)
            $entry.LastWriteTime = [datetimeoffset]'1980-01-01T00:00:00+00:00'
            $stream = $entry.Open()
            try { $stream.Write($bytes,0,$bytes.Length) } finally { $stream.Dispose() }
        }
    } finally { $zip.Dispose() }
    $pin = Get-FareModelIdentity
    $archiveHash = (Get-FileHash -LiteralPath $zipPath -Algorithm SHA256).Hash.ToLowerInvariant()
    $manifest = [ordered]@{schema=$spec.Schema;version=$spec.Version;training_run_id=$pin.TrainingRunId;
        evaluation_run_id=$pin.EvaluationRunId;model_tree_sha256=$pin.ModelSHA256;candidate_lock_sha256=$pin.CandidateLockSHA256;
        archive=[ordered]@{file=$spec.Archive;bytes=[long](Get-Item -LiteralPath $zipPath).Length;sha256=$archiveHash};
        files=@(foreach($name in $names){$records[$name]});
        distribution=[ordered]@{status='local-only-unpublished';redistribution_rights='unverified'}}
    $encoding = New-Object Text.UTF8Encoding($false)
    $manifestFile = Join-Path $stage $spec.Manifest
    [IO.File]::WriteAllText($manifestFile,($manifest | ConvertTo-Json -Depth 10 -Compress) + "`n",$encoding)
    $manifestHash = (Get-FileHash -LiteralPath $manifestFile -Algorithm SHA256).Hash.ToLowerInvariant()
    [IO.File]::WriteAllText((Join-Path $stage $spec.Checksums),
        $archiveHash + '  ' + $spec.Archive + "`n" + $manifestHash + '  ' + $spec.Manifest + "`n",$encoding)
    $null = Assert-PackageBundle -ProjectRoot $root -BundlePath $zipPath -ManifestPath $manifestFile -ManifestSHA256 $manifestHash
    [IO.Directory]::Move($stage,$output)
    return Assert-PackageBundle -ProjectRoot $root -BundlePath (Join-Path $output $spec.Archive) -ManifestPath (Join-Path $output $spec.Manifest) -ManifestSHA256 $manifestHash
}

function Install-PackageModelBundle {
    [CmdletBinding()]
    param([Parameter(Mandatory=$true)][string]$ProjectRoot,
        [Parameter(Mandatory=$true)][string]$RuntimeRoot,
        [Parameter(Mandatory=$true)][string]$BundlePath,
        [Parameter(Mandatory=$true)][string]$ManifestPath,
        [Parameter(Mandatory=$true)][string]$ManifestSHA256)
    $package = Assert-PackageBundle -ProjectRoot $ProjectRoot -BundlePath $BundlePath -ManifestPath $ManifestPath -ManifestSHA256 $ManifestSHA256
    $destination = Assert-PackageLocalPath $RuntimeRoot -Directory -AllowMissing
    if (Test-Path -LiteralPath $destination) {
        $verified = Assert-FareArtifacts -ProjectRoot $ProjectRoot -RuntimeRoot $destination
        return [pscustomobject]@{Verified=$true;Skipped=$true;RuntimeRoot=$destination;ModelSHA256=$verified.ModelSHA256;BundleSHA256=$package.BundleSHA256}
    }
    $parent = Split-Path -Parent $destination
    if (-not $parent -or $destination -eq [IO.Path]::GetPathRoot($destination)) { Stop-PackageBundle 'A dedicated model runtime directory is required.' }
    $null = Assert-PackageLocalPath $parent -Directory -AllowMissing
    $null = New-Item -ItemType Directory -Path $parent -Force
    $null = Assert-PackageLocalPath $parent -Directory
    $stage = Join-Path $parent ('.model-install-' + [guid]::NewGuid().ToString('N'))
    $null = New-Item -ItemType Directory -Path $stage
    # Only the fresh private stage is written. A failed stage is retained, never
    # merged into a pre-existing tree or removed using a broad recursive delete.
    [byte[]]$archiveBytes = Read-PackageLocalBytes $package.BundlePath (Get-PackageBundleSpec).MaxArchiveBytes
    if ((Get-PackageBytesHash $archiveBytes) -cne $package.BundleSHA256) { Stop-PackageBundle 'Bundle changed before extraction; installation was not published.' }
    $memory = [IO.MemoryStream]::new($archiveBytes,$false)
    $zip = [IO.Compression.ZipArchive]::new($memory,[IO.Compression.ZipArchiveMode]::Read,$false)
    try {
        $records = @{}
        foreach ($record in $package.Files) { $records[$record.path] = $record }
        foreach ($entry in $zip.Entries) {
            Assert-PackageRelativePath $entry.FullName
            [byte[]]$bytes = Read-PackageZipEntry $entry (Get-PackageBundleSpec).MaxFileBytes
            if (-not $records.ContainsKey($entry.FullName) -or (Get-PackageBytesHash $bytes) -cne $records[$entry.FullName].sha256) {
                Stop-PackageBundle 'Bundle changed before extraction; installation was not published.'
            }
            $file = Join-Path $stage $entry.FullName
            $null = New-Item -ItemType Directory -Path (Split-Path -Parent $file) -Force
            $output = [IO.File]::Open($file,[IO.FileMode]::CreateNew,[IO.FileAccess]::Write,[IO.FileShare]::None)
            try { $output.Write($bytes,0,$bytes.Length) } finally { $output.Dispose() }
        }
    } finally { $zip.Dispose(); $memory.Dispose() }
    $verified = Assert-FareArtifacts -ProjectRoot $ProjectRoot -RuntimeRoot $stage
    [IO.Directory]::Move($stage,$destination)
    return [pscustomobject]@{Verified=$true;Skipped=$false;RuntimeRoot=$destination;ModelSHA256=$verified.ModelSHA256;BundleSHA256=$package.BundleSHA256}
}

if ($MyInvocation.InvocationName -ne '.') {
    $ErrorActionPreference = 'Stop'
    try {
        if (-not $ProjectRoot) { $ProjectRoot = $script:PackageDefaultProjectRoot }
        if ($Action -in @('Create','Install') -and -not $Execute) {
            Write-Host ('PLAN: ' + $Action + ' local frozen model bundle only. No files written; add -Execute after reviewing paths and independent manifest pin.')
            exit 0
        }
        if ($Action -eq 'Create') {
            if (-not $OutputDirectory) { $OutputDirectory = Join-Path $ProjectRoot '.sandbox/package-bundles/ml-p4-v1' }
            $result = New-PackageModelBundle -ProjectRoot $ProjectRoot -OutputDirectory $OutputDirectory -RuntimeRoot $RuntimeRoot
        } else {
            $spec = Get-PackageBundleSpec
            $repositoryDefault = -not $BundlePath -and -not $ManifestPath
            $defaultBundleDirectory = Join-Path $ProjectRoot 'distribution/models/ml-p4-v1'
            if (-not $BundlePath) { $BundlePath = Join-Path $defaultBundleDirectory $spec.Archive }
            if (-not $ManifestPath) { $ManifestPath = Join-Path $defaultBundleDirectory $spec.Manifest }
            if (-not $ManifestSHA256 -and $repositoryDefault) {
                $pinPath=Assert-PackageLocalPath (Join-Path $ProjectRoot 'config/model_bundle.lock.json')
                $publicPin=ConvertFrom-PackageBytes (Read-PackageLocalBytes $pinPath 16384)
                if($publicPin.version -cne $spec.Version -or $publicPin.distribution.status -cne 'repository-bundled' -or
                    $publicPin.distribution.location -cne 'distribution/models/ml-p4-v1') {Stop-PackageBundle 'Pinned repository distribution location is not configured.'}
                $ManifestSHA256=$publicPin.manifest_sha256
            }
            if (-not $ManifestSHA256) { Stop-PackageBundle 'An independently trusted ManifestSHA256 is required for explicit archive paths; no latest/network lookup is performed.' }
            if ($Action -eq 'Install') {
                if (-not $RuntimeRoot) { Stop-PackageBundle 'Install requires an explicit dedicated RuntimeRoot.' }
                $result = Install-PackageModelBundle -ProjectRoot $ProjectRoot -RuntimeRoot $RuntimeRoot -BundlePath $BundlePath -ManifestPath $ManifestPath -ManifestSHA256 $ManifestSHA256
            } else { $result = Assert-PackageBundle -ProjectRoot $ProjectRoot -BundlePath $BundlePath -ManifestPath $ManifestPath -ManifestSHA256 $ManifestSHA256 }
        }
        $result | Select-Object Verified,Skipped,Version,RuntimeRoot,BundleSHA256,BundleBytes,ManifestSHA256,ManifestBytes,ModelSHA256 | Format-List
        Write-Host 'Local filesystem bundle operation only. No publication, download or model training was performed; see distribution PROVENANCE.md for the content review.'
        exit 0
    } catch {
        $safe = $_.Exception.Data['PackageSafeMessage']
        if (-not $safe) { $safe = $_.Exception.Data['StartupSafeMessage'] }
        if (-not $safe) { $safe = 'Local model bundle operation failed. No overwrite/publication/download/training was attempted; inspect any private staging directory separately.' }
        Write-Host ('ERROR: ' + $safe)
        exit 1
    }
}
