# Explicit local V2.2 artifact/selection guards. Dot-sourcing has no side effects.
# v1 remains the default; no training, download, install, fallback or lifecycle.

function Get-PretripV22Identity {
    [pscustomobject]@{
        ModelVersion='v2.2'
        ModelSHA256='27de872754f96193f204eb3b7c5d18849ea5116ed8d36ad54a1780e1d6261db3'
        ArtifactManifestSHA256='8acbec5b1182372905a16556c75656b2c1b76c40a7f38e0a89267b3ba4be0a75'
        DictionarySHA256='aad357b1b2b730b1bd6d296accfa0783a5c93420c30064dc39b80c7433263c2c'
        SampleSHA256='7f9a0ad2ecf55b61acc91f087cdeb1827cbc420b829b1ce58073c28b17a68e86'
        ContractVersion='pretrip-v2-zone-time-1'
        ContractSHA256='a1ddbe55f0128c20d8189d9f37b3b6172e90f20be71ae7a99300e21fb306add5'
        CalendarVersion='us-opm-nationwide-observed-2025-2027-v1'
        CalendarSHA256='00dd5c8f442955879ade701c726106f12ef0e03669f818c5c06c6a5b067decac'
        TzifSHA256='e9ed07d7bee0c76a9d442d091ef1f01668fee7c4f26014c0a868b19fe6c18a95'
        ZoneLookupSHA256='1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed'
        RuntimeDirectory='.sandbox/pretrip-v2/serving-bundle-v22-01/artifacts'
    }
}

function Resolve-PretripV22ArtifactPath {
    param([string]$Root,[string]$RelativePath,[switch]$Directory)
    try {return Resolve-FareArtifactPath -Root $Root -RelativePath $RelativePath -Directory:$Directory}
    catch {Stop-Startup 'V2.2 bundle is missing, unsafe or incomplete. Run prepare_pretrip_serving_bundle.py --execute with the reviewed local candidate, then retry explicit -ModelVersion v2.2. No v1 fallback or startup attempted.'}
}

function Get-PretripV22PipelineTree {
    param([Parameter(Mandatory=$true)][string]$PipelineDirectory)
    $tree=Get-FareModelTree $PipelineDirectory
    # V2's canonical inventory includes the pipeline/ prefix. The v1 tree helper
    # intentionally remains unchanged because v1 hashes model-relative names.
    $entries=@(foreach($entry in $tree.files){[pscustomobject][ordered]@{
        bytes=[long]$entry.bytes;path=('pipeline/'+$entry.path);sha256=$entry.sha256}})
    $canonical=ConvertTo-Json -InputObject $entries -Depth 5 -Compress
    $algorithm=[Security.Cryptography.SHA256]::Create()
    try {$hash=[BitConverter]::ToString($algorithm.ComputeHash([Text.Encoding]::UTF8.GetBytes($canonical))).Replace('-','').ToLowerInvariant()}
    finally {$algorithm.Dispose()}
    return [pscustomobject]@{sha256=$hash;files=$entries}
}

function Assert-PretripV22Artifacts {
    param([Parameter(Mandatory=$true)][string]$ProjectRoot)
    $pin=Get-PretripV22Identity
    $lockPath=Resolve-PretripV22ArtifactPath $ProjectRoot 'config/model_bundle_v22.lock.json'
    if((Get-Item -LiteralPath $lockPath).Length -gt 16384){Stop-Startup 'V2.2 version lock exceeds the bounded metadata size.'}
    try {$lock=Get-Content -LiteralPath $lockPath -Raw -Encoding UTF8 | ConvertFrom-Json}
    catch {Stop-Startup 'V2.2 version lock is not valid JSON.'}
    if($lock.model_version -cne 'v2.2' -or $lock.version -cne 'pretrip-v2.2-native' -or
        $lock.model_tree_sha256 -cne $pin.ModelSHA256 -or $lock.artifact_manifest_sha256 -cne $pin.ArtifactManifestSHA256 -or
        $lock.dictionary_sha256 -cne $pin.DictionarySHA256 -or $lock.sample_identity_sha256 -cne $pin.SampleSHA256 -or
        $lock.contract_version -cne $pin.ContractVersion -or $lock.contract_sha256 -cne $pin.ContractSHA256 -or
        $lock.calendar_version -cne $pin.CalendarVersion -or $lock.calendar_sha256 -cne $pin.CalendarSHA256 -or
        $lock.tzif_sha256 -cne $pin.TzifSHA256 -or
        $lock.zone_lookup_sha256 -cne $pin.ZoneLookupSHA256 -or $lock.runtime_directory -cne $pin.RuntimeDirectory -or
        $lock.encoding -cne 'native' -or $lock.loss_type -cne 'squared' -or $lock.max_bins -ne 256 -or $lock.vector_dimensions -ne 8){
        Stop-Startup 'V2.2 public version lock differs from the frozen code identity. No artifact or v1 fallback accepted.'
    }
    $root=Resolve-PretripV22ArtifactPath $ProjectRoot $pin.RuntimeDirectory -Directory
    $manifestPath=Resolve-PretripV22ArtifactPath $root 'artifact_manifest.json'
    if ((Get-FileHash -LiteralPath $manifestPath -Algorithm SHA256).Hash.ToLowerInvariant() -cne $pin.ArtifactManifestSHA256) {
        Stop-Startup 'V2.2 artifact manifest mismatch. Prepare the pinned local bundle explicitly; no fallback/startup attempted.'
    }
    try {$manifest=Get-Content -LiteralPath $manifestPath -Raw -Encoding UTF8 | ConvertFrom-Json}
    catch {Stop-Startup 'V2.2 artifact manifest is not valid JSON.'}
    if($manifest.version -cne 'pretrip-v2-training-artifacts-1' -or $manifest.model_tree_sha256 -cne $pin.ModelSHA256 -or
        $manifest.promoted -ne $false -or $manifest.deployed -ne $false){Stop-Startup 'V2.2 candidate version/tree/manifest drift.'}
    $seen=[Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    foreach($entry in @($manifest.files)){
        $relative=[string]$entry.path
        if(-not $relative -or $relative.Contains('\') -or $relative.Contains(':') -or $relative.StartsWith('/') -or
            @($relative.Split('/') | Where-Object {$_ -in @('','..','.')}).Count -gt 0 -or -not $seen.Add($relative) -or
            $entry.sha256 -cnotmatch '^[0-9a-f]{64}$') {Stop-Startup 'V2.2 artifact inventory path/hash is unsafe.'}
        $file=Resolve-PretripV22ArtifactPath $root $relative
        if((Get-Item -LiteralPath $file).Length -ne $entry.bytes -or
            (Get-FileHash -LiteralPath $file -Algorithm SHA256).Hash.ToLowerInvariant() -cne $entry.sha256){Stop-Startup 'V2.2 artifact file identity mismatch.'}
    }
    if($seen.Count -lt 1){Stop-Startup 'V2.2 artifact inventory is empty.'}
    $tree=Get-PretripV22PipelineTree (Resolve-PretripV22ArtifactPath $root 'pipeline' -Directory)
    if($tree.sha256 -cne $pin.ModelSHA256){Stop-Startup 'V2.2 pipeline tree contains missing, changed or unlisted files.'}
    foreach($name in @('pretrip_v2_contract.json','pretrip_v2_calendar.json')){
        $artifact=Resolve-PretripV22ArtifactPath $root $name
        $source=Resolve-PretripV22ArtifactPath $ProjectRoot ('config/'+$name)
        if((Get-FileHash -LiteralPath $source -Algorithm SHA256).Hash -cne (Get-FileHash -LiteralPath $artifact -Algorithm SHA256).Hash){Stop-Startup 'V2.2 source contract/calendar differs from the frozen artifact.'}
    }
    $zones=Resolve-PretripV22ArtifactPath (Split-Path -Parent $root) 'zones.csv'
    if((Get-FileHash -LiteralPath $zones -Algorithm SHA256).Hash.ToLowerInvariant() -cne $pin.ZoneLookupSHA256){Stop-Startup 'V2.2 public zone lookup identity mismatch.'}
    [pscustomobject]@{Verified=$true;ModelVersion='v2.2';ModelSHA256=$pin.ModelSHA256;
        ArtifactManifestSHA256=$pin.ArtifactManifestSHA256;DictionarySHA256=$pin.DictionarySHA256;
        ContractVersion=$pin.ContractVersion;ContractSHA256=$pin.ContractSHA256;
        CalendarVersion=$pin.CalendarVersion;CalendarSHA256=$pin.CalendarSHA256;TzifSHA256=$pin.TzifSHA256;
        ArtifactDirectory=$root;ZonesPath=$zones}
}

function Assert-PretripV22Readiness {
    param($Expected,$Ready)
    $pin=Get-PretripV22Identity
    $loadCount=Get-OptionalProperty $Ready 'model_load_count'
    $fitCount=Get-OptionalProperty $Ready 'fit_calls'
    if((Get-OptionalProperty $Expected 'Verified' $false) -ne $true -or
        (Get-OptionalProperty $Expected 'ModelVersion') -cne 'v2.2' -or
        (Get-OptionalProperty $Ready 'ready') -isnot [bool] -or -not $Ready.ready -or
        (Get-OptionalProperty $Ready 'status') -cne 'ready' -or
        ($loadCount -isnot [int] -and $loadCount -isnot [long]) -or $loadCount -ne 1 -or
        ($fitCount -isnot [int] -and $fitCount -isnot [long]) -or $fitCount -ne 0 -or
        (Get-OptionalProperty $Ready 'model_version') -cne 'v2.2' -or
        (Get-OptionalProperty $Ready 'model_sha256') -cne $Expected.ModelSHA256 -or
        (Get-OptionalProperty $Ready 'artifact_manifest_sha256') -cne $Expected.ArtifactManifestSHA256 -or
        (Get-OptionalProperty $Ready 'dictionary_sha256') -cne $Expected.DictionarySHA256 -or
        (Get-OptionalProperty $Ready 'contract_version') -cne $pin.ContractVersion -or
        (Get-OptionalProperty $Ready 'calendar_version') -cne $pin.CalendarVersion -or
        (Get-OptionalProperty $Ready 'calendar_sha256') -cne $pin.CalendarSHA256 -or
        (Get-OptionalProperty $Ready 'tzdb_sha256') -cne $pin.TzifSHA256){
        Stop-Startup 'V2.2 readiness/identity is not the explicitly verified model. No fallback, prediction or restart attempted.'
    }
    return $Ready
}
