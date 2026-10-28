#requires -Version 5.1
<# Repository-only V2.2 installation. PLAN is read-only; no Python, Java,
   Docker, candidate directory, network, model loading or service lifecycle. #>
[CmdletBinding()]
param([string]$ProjectRoot, [switch]$Execute)

$script:PretripV22InstallerRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
. (Join-Path $PSScriptRoot 'model_bundle.ps1') -ProjectRoot $ProjectRoot -Execute:$Execute
. (Join-Path $PSScriptRoot 'pretrip_model_selection.ps1')

function Get-PretripV22DistributionSpec {
    [pscustomobject]@{
        Schema='nyc-taxi-pretrip-v22-distribution-v1'; Version='pretrip-v2.2-native'
        Directory='distribution/models/pretrip-v2.2-native'
        Archive='nyc-taxi-model-pretrip-v2.2-native.zip'
        Manifest='nyc-taxi-model-pretrip-v2.2-native.manifest.json'
        Pin='config/pretrip_v22_distribution.lock.json'
        Runtime='.sandbox/pretrip-v2/serving-bundle-v22-01'
        MaxArchiveBytes=2097152; MaxExpandedBytes=2097152
        MaxFileBytes=524288; MaxManifestBytes=131072; MaxFiles=64
    }
}

function Assert-PretripV22DistributionIdentities {
    param($Identities)
    $pin=Get-PretripV22Identity
    $expected=@{
        model_tree_sha256=$pin.ModelSHA256; artifact_manifest_sha256=$pin.ArtifactManifestSHA256
        dictionary_sha256=$pin.DictionarySHA256; sample_identity_sha256=$pin.SampleSHA256
        contract_sha256=$pin.ContractSHA256; calendar_sha256=$pin.CalendarSHA256
        tzif_sha256=$pin.TzifSHA256; zone_lookup_sha256=$pin.ZoneLookupSHA256
        contract_version=$pin.ContractVersion; calendar_version=$pin.CalendarVersion
    }
    foreach($entry in $expected.GetEnumerator()) {
        if($null -eq $Identities -or $null -eq $Identities.PSObject.Properties[$entry.Key] -or
            $Identities.($entry.Key) -isnot [string] -or $Identities.($entry.Key) -cne $entry.Value) {
            Stop-PackageBundle 'V2.2 distribution identity differs from the frozen code pins.'
        }
    }
}

function Assert-PretripV22DistributionRecord {
    param($Record)
    $spec=Get-PretripV22DistributionSpec
    if($null -eq $Record -or $Record.path -isnot [string]) {Stop-PackageBundle 'Invalid V2.2 inventory record.'}
    Assert-PackageRelativePath $Record.path
    if(($Record.bytes -isnot [int] -and $Record.bytes -isnot [long]) -or $Record.bytes -lt 0 -or
        $Record.bytes -gt $spec.MaxFileBytes -or $Record.sha256 -isnot [string] -or
        $Record.sha256 -cnotmatch '^[0-9a-f]{64}$') {Stop-PackageBundle 'Invalid V2.2 inventory size/hash.'}
}

function Assert-PretripV22InstalledRoot {
    param([string]$ProjectRoot)
    # Adapt only safe exception handling; the existing frozen guard is unchanged.
    function Stop-Startup {param([string]$Message) Stop-PackageBundle $Message}
    return Assert-PretripV22Artifacts -ProjectRoot $ProjectRoot
}

function Assert-PretripV22OutputInventory {
    param([string]$Directory, $Files, $OptionalExistingFiles=@())
    $root=Assert-PackageLocalPath $Directory -Directory
    $expected=[Collections.Generic.Dictionary[string,object]]::new([StringComparer]::Ordinal)
    $directories=[Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    foreach($record in $Files) {
        $expected.Add($record.path,$record)
        $parts=$record.path.Split('/')
        for($i=1;$i -lt $parts.Length;$i++) {$null=$directories.Add(($parts[0..($i-1)] -join '/'))}
    }
    $optional=[Collections.Generic.Dictionary[string,object]]::new([StringComparer]::Ordinal)
    foreach($record in $OptionalExistingFiles) {$optional.Add($record.path,$record)}
    $pending=New-Object 'Collections.Generic.Stack[string]'
    $pending.Push($root)
    $seen=[Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
    while($pending.Count) {
        $current=$pending.Pop()
        $null=Assert-PackageLocalPath $current -Directory
        foreach($item in Get-ChildItem -LiteralPath $current -Force) {
            if(($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {Stop-PackageBundle 'Existing V2.2 output contains a symlink/junction.'}
            $relative=$item.FullName.Substring($root.Length+1).Replace('\','/')
            Assert-PackageRelativePath $relative
            if($item -is [IO.DirectoryInfo]) {
                if(-not $directories.Contains($relative)) {Stop-PackageBundle 'Existing V2.2 output has unowned directories; no overwrite or deletion.'}
                $pending.Push($item.FullName)
            } else {
                if($item -isnot [IO.FileInfo] -or (-not $expected.ContainsKey($relative) -and -not $optional.ContainsKey($relative)) -or -not $seen.Add($relative)) {
                    Stop-PackageBundle 'Existing V2.2 output has unowned files; no overwrite or deletion.'
                }
                $record=if($expected.ContainsKey($relative)){$expected[$relative]}else{$optional[$relative]}
                if($item.Length -ne $record.bytes -or (Get-FileHash -LiteralPath $item.FullName -Algorithm SHA256).Hash.ToLowerInvariant() -cne $record.sha256) {
                    Stop-PackageBundle 'Existing V2.2 output differs from the pinned payload; no overwrite or deletion.'
                }
            }
        }
    }
    foreach($name in $expected.Keys) {if(-not $seen.Contains($name)) {Stop-PackageBundle 'Existing V2.2 output is incomplete; no overwrite or deletion.'}}
}

function Assert-PretripV22Distribution {
    [CmdletBinding()]
    param([Parameter(Mandatory=$true)][string]$ProjectRoot)
    $spec=Get-PretripV22DistributionSpec
    $root=Assert-PackageLocalPath $ProjectRoot -Directory
    $pin=ConvertFrom-PackageBytes (Read-PackageLocalBytes (Join-Path $root $spec.Pin) 16384)
    if($pin.version -cne $spec.Version) {Stop-PackageBundle 'V2.2 distribution pin version mismatch.'}
    Assert-PretripV22DistributionIdentities $pin.identities
    $optionalExisting=@()
    if($null -ne $pin.PSObject.Properties['optional_existing_files']) {$optionalExisting=@($pin.optional_existing_files)}
    if($optionalExisting.Count -gt 1) {Stop-PackageBundle 'V2.2 optional existing-file inventory is invalid.'}
    foreach($record in $optionalExisting) {
        Assert-PretripV22DistributionRecord $record
        if($record.path -cne 'serving_manifest.json') {Stop-PackageBundle 'Only the independently pinned legacy serving manifest may remain as an optional existing file.'}
    }
    if($pin.manifest_sha256 -isnot [string] -or $pin.manifest_sha256 -cnotmatch '^[0-9a-f]{64}$' -or
        $pin.archive_sha256 -isnot [string] -or $pin.archive_sha256 -cnotmatch '^[0-9a-f]{64}$') {Stop-PackageBundle 'V2.2 distribution requires independent full checksums.'}
    foreach($item in @(@{Name='manifest_bytes';Limit=$spec.MaxManifestBytes},@{Name='archive_bytes';Limit=$spec.MaxArchiveBytes},
        @{Name='expanded_bytes';Limit=$spec.MaxExpandedBytes},@{Name='file_count';Limit=$spec.MaxFiles})) {
        $value=$pin.($item.Name)
        if(($value -isnot [int] -and $value -isnot [long]) -or $value -le 0 -or $value -gt $item.Limit) {
            Stop-PackageBundle 'V2.2 distribution pin exceeds the bounded size/count limits.'
        }
    }
    $source=Join-Path $root $spec.Directory
    [byte[]]$manifestBytes=Read-PackageLocalBytes (Join-Path $source $spec.Manifest) $spec.MaxManifestBytes
    if($manifestBytes.Length -ne $pin.manifest_bytes -or (Get-PackageBytesHash $manifestBytes) -cne $pin.manifest_sha256) {Stop-PackageBundle 'Pinned V2.2 manifest SHA256/size mismatch.'}
    $manifest=ConvertFrom-PackageBytes $manifestBytes
    if($manifest.schema -cne $spec.Schema -or $manifest.version -cne $spec.Version -or $manifest.archive.file -cne $spec.Archive -or
        $manifest.archive.bytes -ne $pin.archive_bytes -or $manifest.archive.sha256 -cne $pin.archive_sha256 -or
        $manifest.file_count -ne $pin.file_count -or $manifest.expanded_bytes -ne $pin.expanded_bytes) {Stop-PackageBundle 'V2.2 manifest and repository pin differ.'}
    Assert-PretripV22DistributionIdentities $manifest.identities
    $inventory=[Collections.Generic.Dictionary[string,object]]::new([StringComparer]::Ordinal)
    $caseNames=[Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    $pathNodes=[Collections.Generic.Dictionary[string,string]]::new([StringComparer]::OrdinalIgnoreCase)
    [long]$expanded=0
    if(@($manifest.files).Count -ne $pin.file_count) {Stop-PackageBundle 'V2.2 inventory count mismatch.'}
    foreach($record in $manifest.files) {
        Assert-PretripV22DistributionRecord $record
        if(-not $caseNames.Add($record.path)) {Stop-PackageBundle 'Duplicate/case-colliding V2.2 paths.'}
        $parts=$record.path.Split('/')
        for($i=1;$i -le $parts.Length;$i++) {
            $node=$parts[0..($i-1)] -join '/'
            if($pathNodes.ContainsKey($node) -and $pathNodes[$node] -cne $node) {Stop-PackageBundle 'Case-colliding V2.2 path components.'}
            $pathNodes[$node]=$node
            if($i -lt $parts.Length -and $caseNames.Contains($node)) {Stop-PackageBundle 'Archive file/directory collision.'}
        }
        $inventory.Add($record.path,$record)
        $expanded += $record.bytes
        if($expanded -gt $spec.MaxExpandedBytes) {Stop-PackageBundle 'Expanded V2.2 inventory exceeds the bounded size limit.'}
    }
    foreach($record in $manifest.files) {
        if(@($manifest.files | Where-Object {$_.path.StartsWith($record.path+'/',[StringComparison]::OrdinalIgnoreCase)}).Count) {Stop-PackageBundle 'Archive file/directory collision.'}
    }
    if($expanded -ne $pin.expanded_bytes) {Stop-PackageBundle 'Expanded V2.2 inventory size mismatch.'}
    [byte[]]$archiveBytes=Read-PackageLocalBytes (Join-Path $source $spec.Archive) $spec.MaxArchiveBytes
    if($archiveBytes.Length -ne $pin.archive_bytes -or (Get-PackageBytesHash $archiveBytes) -cne $pin.archive_sha256) {Stop-PackageBundle 'Pinned V2.2 archive SHA256/size mismatch.'}
    Add-Type -AssemblyName System.IO.Compression
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $memory=[IO.MemoryStream]::new($archiveBytes,$false)
    $zip=[IO.Compression.ZipArchive]::new($memory,[IO.Compression.ZipArchiveMode]::Read,$false)
    try {
        if($zip.Entries.Count -ne $inventory.Count) {Stop-PackageBundle 'V2.2 ZIP member count mismatch.'}
        $contents=[Collections.Generic.Dictionary[string,byte[]]]::new([StringComparer]::Ordinal)
        $seen=[Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
        [long]$actualExpanded=0
        foreach($entry in $zip.Entries) {
            Assert-PackageRelativePath $entry.FullName
            $type=(($entry.ExternalAttributes -shr 16) -band 61440)
            if($type -notin @(0,32768) -or ($entry.ExternalAttributes -band 1024) -ne 0) {Stop-PackageBundle 'Archive symlinks/special entries are not allowed.'}
            if(-not $seen.Add($entry.FullName) -or -not $inventory.ContainsKey($entry.FullName)) {Stop-PackageBundle 'Duplicate or unlisted V2.2 ZIP member.'}
            $record=$inventory[$entry.FullName]
            if($entry.Length -ne $record.bytes) {Stop-PackageBundle 'V2.2 ZIP entry length differs from the pinned inventory.'}
            [byte[]]$bytes=Read-PackageZipEntry $entry $spec.MaxFileBytes
            $actualExpanded += $bytes.Length
            if($actualExpanded -gt $spec.MaxExpandedBytes -or (Get-PackageBytesHash $bytes) -cne $record.sha256) {Stop-PackageBundle 'Expanded V2.2 payload size/hash mismatch.'}
            $contents.Add($entry.FullName,$bytes)
        }
        if($actualExpanded -ne $expanded) {Stop-PackageBundle 'Expanded V2.2 payload total mismatch.'}
        $frozen=Get-PretripV22Identity
        if(-not $contents.ContainsKey('artifacts/artifact_manifest.json') -or
            (Get-PackageBytesHash $contents['artifacts/artifact_manifest.json']) -cne $frozen.ArtifactManifestSHA256) {Stop-PackageBundle 'Frozen artifact manifest is missing or changed.'}
        $artifact=ConvertFrom-PackageBytes $contents['artifacts/artifact_manifest.json']
        if($artifact.version -cne 'pretrip-v2-training-artifacts-1' -or $artifact.model_tree_sha256 -cne $frozen.ModelSHA256 -or
            $artifact.holdout_opened -isnot [bool] -or $artifact.holdout_opened -ne $false -or
            $artifact.promoted -isnot [bool] -or $artifact.promoted -ne $false -or
            $artifact.deployed -isnot [bool] -or $artifact.deployed -ne $false) {Stop-PackageBundle 'Frozen artifact identity/historical flags differ.'}
        $allowlist=[Collections.Generic.HashSet[string]]::new([StringComparer]::Ordinal)
        $null=$allowlist.Add('artifacts/artifact_manifest.json')
        foreach($record in $artifact.files) {
            Assert-PretripV22DistributionRecord $record
            $name='artifacts/'+$record.path
            if(-not $allowlist.Add($name) -or -not $inventory.ContainsKey($name) -or
                $inventory[$name].bytes -ne $record.bytes -or $inventory[$name].sha256 -cne $record.sha256) {Stop-PackageBundle 'Distribution differs from frozen artifact inventory.'}
        }
        $null=$allowlist.Add('zones.csv')
        if(-not $contents.ContainsKey('zones.csv') -or (Get-PackageBytesHash $contents['zones.csv']) -cne $frozen.ZoneLookupSHA256 -or
            $allowlist.Count -ne $inventory.Count) {Stop-PackageBundle 'V2.2 serving payload allowlist differs from frozen artifacts/zones.'}
        foreach($name in $inventory.Keys) {if(-not $allowlist.Contains($name)) {Stop-PackageBundle 'Unnecessary or unowned file in V2.2 archive.'}}
        $config=[Collections.Generic.Dictionary[string,byte[]]]::new([StringComparer]::Ordinal)
        foreach($name in @('model_bundle_v22.lock.json','pretrip_v2_contract.json','pretrip_v2_calendar.json')) {
            [byte[]]$bytes=Read-PackageLocalBytes (Join-Path $root ('config/'+$name)) 16384
            if(($name -eq 'pretrip_v2_contract.json' -and (Get-PackageBytesHash $bytes) -cne $frozen.ContractSHA256) -or
                ($name -eq 'pretrip_v2_calendar.json' -and (Get-PackageBytesHash $bytes) -cne $frozen.CalendarSHA256)) {Stop-PackageBundle 'Repository contract/calendar differs from frozen identity.'}
            $config.Add($name,$bytes)
        }
        $sourceLock=ConvertFrom-PackageBytes $config['model_bundle_v22.lock.json']
        Assert-PretripV22DistributionIdentities $sourceLock
        if($sourceLock.model_version -cne 'v2.2' -or $sourceLock.version -cne $spec.Version -or
            $sourceLock.runtime_directory -cne (Get-PretripV22Identity).RuntimeDirectory -or
            $sourceLock.encoding -cne 'native' -or $sourceLock.loss_type -cne 'squared' -or
            $sourceLock.max_bins -ne 256 -or $sourceLock.vector_dimensions -ne 8) {Stop-PackageBundle 'Repository V2.2 model lock differs from the frozen serving contract.'}
        return [pscustomobject]@{Verified=$true;Version=$spec.Version;ProjectRoot=$root;RuntimeRoot=(Join-Path $root $spec.Runtime);
            Files=@($manifest.files);OptionalExistingFiles=$optionalExisting;Contents=$contents;SourceConfig=$config;BundleSHA256=$pin.archive_sha256;
            ManifestSHA256=$pin.manifest_sha256;BundleBytes=[long]$archiveBytes.Length;ExpandedBytes=$expanded;Identities=$manifest.identities}
    } finally {$zip.Dispose();$memory.Dispose()}
}

function Install-PretripV22Distribution {
    [CmdletBinding()]
    param([Parameter(Mandatory=$true)][string]$ProjectRoot,[switch]$Execute)
    $package=Assert-PretripV22Distribution -ProjectRoot $ProjectRoot
    $destination=Assert-PackageLocalPath $package.RuntimeRoot -Directory -AllowMissing
    $exists=Test-Path -LiteralPath $destination
    if($exists) {
        Assert-PretripV22OutputInventory -Directory $destination -Files $package.Files -OptionalExistingFiles $package.OptionalExistingFiles
        $null=Assert-PretripV22InstalledRoot -ProjectRoot $package.ProjectRoot
    }
    if(-not $Execute -or $exists) {
        $status=if(-not $Execute){'PLAN'}else{'VERIFIED_REUSED'}
        return [pscustomobject]@{Status=$status;Verified=$true;Skipped=[bool]$exists;Writes=$false;
            RuntimeRoot=$destination;ModelSHA256=$package.Identities.model_tree_sha256;
            BundleSHA256=$package.BundleSHA256;FileCount=$package.Files.Count}
    }
    $parent=Split-Path -Parent $destination
    $null=Assert-PackageLocalPath $parent -Directory -AllowMissing
    $null=New-Item -ItemType Directory -Path $parent -Force
    $null=Assert-PackageLocalPath $parent -Directory
    # A separate project layout lets the UNCHANGED V2 guard verify a new stage.
    # It is never merged into an existing tree; failed private stages are retained.
    # A short fresh name avoids needless MAX_PATH pressure in Windows PS5.1.
    # New-Item (without Force) rejects even a rare/concurrent name collision.
    $stageRoot=Join-Path $package.ProjectRoot ('.sandbox/.v22-'+[guid]::NewGuid().ToString('N').Substring(0,8))
    $null=Assert-PackageLocalPath $stageRoot -Directory -AllowMissing
    if(Test-Path -LiteralPath $stageRoot) {Stop-PackageBundle 'Private install staging path already exists.'}
    $null=New-Item -ItemType Directory -Path $stageRoot
    $stageBundle=Join-Path $stageRoot (Get-PretripV22DistributionSpec).Runtime
    $null=New-Item -ItemType Directory -Path $stageBundle -Force
    foreach($record in $package.Files) {
        Assert-PackageRelativePath $record.path
        $file=Join-Path $stageBundle $record.path
        $null=New-Item -ItemType Directory -Path (Split-Path -Parent $file) -Force
        $null=Assert-PackageLocalPath (Split-Path -Parent $file) -Directory
        [byte[]]$bytes=$package.Contents[$record.path]
        if($bytes.Length -ne $record.bytes -or (Get-PackageBytesHash $bytes) -cne $record.sha256) {Stop-PackageBundle 'Validated V2.2 bytes changed before extraction.'}
        $stream=[IO.File]::Open($file,[IO.FileMode]::CreateNew,[IO.FileAccess]::Write,[IO.FileShare]::None)
        try {$stream.Write($bytes,0,$bytes.Length)} finally {$stream.Dispose()}
    }
    $configRoot=Join-Path $stageRoot 'config'
    $null=New-Item -ItemType Directory -Path $configRoot
    foreach($name in $package.SourceConfig.Keys) {
        $stream=[IO.File]::Open((Join-Path $configRoot $name),[IO.FileMode]::CreateNew,[IO.FileAccess]::Write,[IO.FileShare]::None)
        try {$bytes=$package.SourceConfig[$name];$stream.Write($bytes,0,$bytes.Length)} finally {$stream.Dispose()}
    }
    Assert-PretripV22OutputInventory -Directory $stageBundle -Files $package.Files
    $null=Assert-PretripV22InstalledRoot -ProjectRoot $stageRoot
    $null=Assert-PackageLocalPath $parent -Directory
    if(Test-Path -LiteralPath $destination) {Stop-PackageBundle 'V2.2 destination appeared during installation; private stage retained, no overwrite.'}
    try {[IO.Directory]::Move($stageBundle,$destination)}
    catch {Stop-PackageBundle 'Atomic V2.2 publication failed; private stage retained, no overwrite.'}
    $null=Assert-PretripV22InstalledRoot -ProjectRoot $package.ProjectRoot
    # Remove only our three known config files and now-empty scaffolding, never
    # recursively delete a caller's path or touch a pre-existing runtime output.
    $scaffoldingRetained=$false
    try {
        foreach($name in $package.SourceConfig.Keys) {
            $file=Assert-PackageLocalPath (Join-Path $configRoot $name)
            if((Get-FileHash -LiteralPath $file -Algorithm SHA256).Hash.ToLowerInvariant() -cne (Get-PackageBytesHash $package.SourceConfig[$name])) {throw 'Stage config changed'}
            [IO.File]::Delete($file)
        }
        foreach($relative in @('config','.sandbox/pretrip-v2','.sandbox','')) {
            $directory=if($relative){Join-Path $stageRoot $relative}else{$stageRoot}
            $null=Assert-PackageLocalPath $directory -Directory
            [IO.Directory]::Delete($directory,$false)
        }
    } catch {$scaffoldingRetained=$true}
    return [pscustomobject]@{Status='INSTALLED';Verified=$true;Skipped=$false;Writes=$true;
        RuntimeRoot=$destination;ModelSHA256=$package.Identities.model_tree_sha256;
        BundleSHA256=$package.BundleSHA256;FileCount=$package.Files.Count;StageScaffoldingRetained=$scaffoldingRetained}
}

if($MyInvocation.InvocationName -ne '.') {
    $ErrorActionPreference='Stop'
    try {
        if(-not $ProjectRoot) {$ProjectRoot=$script:PretripV22InstallerRoot}
        $result=Install-PretripV22Distribution -ProjectRoot $ProjectRoot -Execute:$Execute
        $result | ConvertTo-Json -Compress
        Write-Host 'Repository model bundle only; no images, bootstrap, service startup or model load was performed.'
        exit 0
    } catch {
        $safe=$_.Exception.Data['PackageSafeMessage']
        if(-not $safe) {$safe='V2.2 repository bundle validation/install failed; no overwrite, download, training or fallback. Any fresh private stage is retained.'}
        Write-Host ('ERROR: '+$safe)
        exit 1
    }
}
