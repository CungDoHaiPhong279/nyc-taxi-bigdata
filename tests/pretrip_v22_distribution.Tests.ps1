# Synthetic PowerShell 5.1 distribution tests; no model load, Docker or network.
. (Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts/install_pretrip_v22_bundle.ps1')
$script:V22PristineSpecFactory=(Get-Command Get-PretripV22DistributionSpec -CommandType Function).ScriptBlock

function Write-V22FixtureFile {
    param([string]$Path,[string]$Text)
    $null=New-Item -ItemType Directory -Path (Split-Path -Parent $Path) -Force
    [IO.File]::WriteAllText($Path,$Text,[Text.UTF8Encoding]::new($false))
}

function Get-V22FixtureHash {
    param([string]$Path)
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Update-V22FixturePins {
    param($Package,$Manifest)
    $Manifest.archive.bytes=[long](Get-Item -LiteralPath $Package.BundlePath).Length
    $Manifest.archive.sha256=Get-V22FixtureHash $Package.BundlePath
    Write-V22FixtureFile $Package.ManifestPath (($Manifest|ConvertTo-Json -Depth 15 -Compress)+"`n")
    $pin=[ordered]@{
        version='pretrip-v2.2-native'
        manifest_sha256=(Get-V22FixtureHash $Package.ManifestPath)
        manifest_bytes=[long](Get-Item -LiteralPath $Package.ManifestPath).Length
        archive_sha256=$Manifest.archive.sha256
        archive_bytes=[long]$Manifest.archive.bytes
        expanded_bytes=[long]$Manifest.expanded_bytes
        file_count=[int]$Manifest.file_count
        identities=$Manifest.identities
    }
    Write-V22FixtureFile $Package.PinPath (($pin|ConvertTo-Json -Depth 15 -Compress)+"`n")
}

function New-V22DistributionFixture {
    param([string]$Root)
    $source=Join-Path $Root 'synthetic-source'
    Write-V22FixtureFile (Join-Path $source 'pipeline/model.txt') 'Synthetic model parameters, not a real serialized model'
    Write-V22FixtureFile (Join-Path $source 'pipeline/_SUCCESS') ''
    Write-V22FixtureFile (Join-Path $source 'pretrip_v2_contract.json') '{"synthetic_contract":true}'
    Write-V22FixtureFile (Join-Path $source 'pretrip_v2_calendar.json') '{"synthetic_calendar":true}'
    $tree=Get-PretripV22PipelineTree (Join-Path $source 'pipeline')
    $members=@($tree.files)
    foreach($name in @('pretrip_v2_contract.json','pretrip_v2_calendar.json')){
        $file=Join-Path $source $name
        $members+=[pscustomobject][ordered]@{bytes=[long](Get-Item -LiteralPath $file).Length;path=$name;sha256=(Get-V22FixtureHash $file)}
    }
    $artifact=[ordered]@{
        version='pretrip-v2-training-artifacts-1';model_tree_sha256=$tree.sha256
        files=$members;holdout_opened=$false;promoted=$false;deployed=$false
    }
    Write-V22FixtureFile (Join-Path $source 'artifact_manifest.json') (($artifact|ConvertTo-Json -Depth 10 -Compress)+"`n")
    Write-V22FixtureFile (Join-Path $source 'zones.csv') "LocationID,Borough,Zone,service_zone`n1,Synthetic,Invented,Yellow`n"
    $identity=[pscustomobject]@{
        ModelVersion='v2.2';ModelSHA256=$tree.sha256
        ArtifactManifestSHA256=(Get-V22FixtureHash (Join-Path $source 'artifact_manifest.json'))
        DictionarySHA256=('d'*64);SampleSHA256=('a'*64)
        ContractVersion='synthetic-contract-v1'
        ContractSHA256=(Get-V22FixtureHash (Join-Path $source 'pretrip_v2_contract.json'))
        CalendarVersion='synthetic-calendar-v1'
        CalendarSHA256=(Get-V22FixtureHash (Join-Path $source 'pretrip_v2_calendar.json'))
        TzifSHA256=('b'*64);ZoneLookupSHA256=(Get-V22FixtureHash (Join-Path $source 'zones.csv'))
        RuntimeDirectory='.sandbox/pretrip-v2/serving-bundle-v22-01/artifacts'
    }
    $identities=[ordered]@{
        model_tree_sha256=$identity.ModelSHA256
        artifact_manifest_sha256=$identity.ArtifactManifestSHA256
        dictionary_sha256=$identity.DictionarySHA256
        sample_identity_sha256=$identity.SampleSHA256
        contract_sha256=$identity.ContractSHA256
        calendar_sha256=$identity.CalendarSHA256
        tzif_sha256=$identity.TzifSHA256
        zone_lookup_sha256=$identity.ZoneLookupSHA256
        contract_version=$identity.ContractVersion
        calendar_version=$identity.CalendarVersion
    }
    $modelLock=[ordered]@{
        model_version='v2.2';version='pretrip-v2.2-native'
        model_tree_sha256=$identity.ModelSHA256;artifact_manifest_sha256=$identity.ArtifactManifestSHA256
        dictionary_sha256=$identity.DictionarySHA256;sample_identity_sha256=$identity.SampleSHA256
        contract_version=$identity.ContractVersion;contract_sha256=$identity.ContractSHA256
        calendar_version=$identity.CalendarVersion;calendar_sha256=$identity.CalendarSHA256
        tzif_sha256=$identity.TzifSHA256;zone_lookup_sha256=$identity.ZoneLookupSHA256
        runtime_directory=$identity.RuntimeDirectory;encoding='native';loss_type='squared';max_bins=256;vector_dimensions=8
    }
    Write-V22FixtureFile (Join-Path $Root 'config/model_bundle_v22.lock.json') ($modelLock|ConvertTo-Json -Depth 10 -Compress)
    foreach($name in @('pretrip_v2_contract.json','pretrip_v2_calendar.json')){
        Write-V22FixtureFile (Join-Path $Root ('config/'+$name)) ([IO.File]::ReadAllText((Join-Path $source $name)))
    }
    $distribution=Join-Path $Root 'distribution/models/pretrip-v2.2-native'
    $null=New-Item -ItemType Directory -Path $distribution -Force
    $package=[pscustomobject]@{
        Root=$Root;Identity=$identity;Distribution=$distribution
        BundlePath=(Join-Path $distribution 'nyc-taxi-model-pretrip-v2.2-native.zip')
        ManifestPath=(Join-Path $distribution 'nyc-taxi-model-pretrip-v2.2-native.manifest.json')
        PinPath=(Join-Path $Root 'config/pretrip_v22_distribution.lock.json')
        RuntimeRoot=(Join-Path $Root '.sandbox/pretrip-v2/serving-bundle-v22-01')
    }
    $contents=[Collections.Generic.Dictionary[string,byte[]]]::new([StringComparer]::Ordinal)
    foreach($member in $members){
        $contents.Add(('artifacts/'+$member.path),[IO.File]::ReadAllBytes((Join-Path $source $member.path)))
    }
    $contents.Add('artifacts/artifact_manifest.json',[IO.File]::ReadAllBytes((Join-Path $source 'artifact_manifest.json')))
    $contents.Add('zones.csv',[IO.File]::ReadAllBytes((Join-Path $source 'zones.csv')))
    [string[]]$names=@($contents.Keys)
    [Array]::Sort($names,[StringComparer]::Ordinal)
    Add-Type -AssemblyName System.IO.Compression
    Add-Type -AssemblyName System.IO.Compression.FileSystem
    $zip=[IO.Compression.ZipFile]::Open($package.BundlePath,[IO.Compression.ZipArchiveMode]::Create)
    try{
        foreach($name in $names){
            $entry=$zip.CreateEntry($name,[IO.Compression.CompressionLevel]::Optimal)
            $entry.LastWriteTime=[datetimeoffset]'1980-01-01T00:00:00+00:00'
            $stream=$entry.Open()
            try{$stream.Write($contents[$name],0,$contents[$name].Length)}finally{$stream.Dispose()}
        }
    }finally{$zip.Dispose()}
    $records=@(foreach($name in $names){
        [pscustomobject][ordered]@{path=$name;bytes=[long]$contents[$name].Length;sha256=(Get-PackageBytesHash $contents[$name])}
    })
    $manifest=[pscustomobject]@{
        schema='nyc-taxi-pretrip-v22-distribution-v1';version='pretrip-v2.2-native'
        archive=[pscustomobject]@{file='nyc-taxi-model-pretrip-v2.2-native.zip';bytes=0;sha256=('0'*64)}
        files=$records;file_count=$records.Count
        expanded_bytes=[long](($records|Measure-Object bytes -Sum).Sum)
        identities=[pscustomobject]$identities
    }
    Update-V22FixturePins $package $manifest
    return $package
}

function Get-V22FixtureSnapshot {
    param([string]$Root)
    $entries=@(Get-ChildItem -LiteralPath $Root -Recurse -Force|Sort-Object FullName)
    $rows=@(foreach($entry in $entries){
        $relative=$entry.FullName.Substring($Root.Length+1)
        if($entry.PSIsContainer){'D|'+$relative+'|'+$entry.LastWriteTimeUtc.Ticks}
        else{'F|'+$relative+'|'+$entry.Length+'|'+(Get-V22FixtureHash $entry.FullName)+'|'+$entry.LastWriteTimeUtc.Ticks}
    })
    return $rows -join "`n"
}

function Set-V22FixtureZipMutation {
    param($Package,[string]$ReplacementPath,[switch]$Symlink,[switch]$DuplicateCase,[switch]$TamperBytes)
    $zip=[IO.Compression.ZipFile]::Open($Package.BundlePath,[IO.Compression.ZipArchiveMode]::Update)
    try{
        $first=@($zip.Entries)[0]
        if($Symlink){$first.ExternalAttributes=-1577058304;return}
        if($DuplicateCase){
            $name=$first.FullName.ToUpperInvariant()
            [byte[]]$bytes=Read-PackageZipEntry $first 524288
            @($zip.Entries)[1].Delete()
        }else{
            $name=if($ReplacementPath){$ReplacementPath}else{$first.FullName}
            [byte[]]$bytes=Read-PackageZipEntry $first 524288
            $first.Delete()
        }
        if($TamperBytes){$bytes=[Text.Encoding]::UTF8.GetBytes('different synthetic member bytes')}
        $entry=$zip.CreateEntry($name)
        $stream=$entry.Open()
        try{$stream.Write($bytes,0,$bytes.Length)}finally{$stream.Dispose()}
    }finally{$zip.Dispose()}
}

Describe 'Portable frozen V2.2 repository distribution' {
    BeforeEach {
        # Pester 4 mocks live until the enclosing Describe ends. Give every It
        # fresh limits, so a tiny-cap test cannot poison later positive cases.
        $script:V22ActiveSpec=& $script:V22PristineSpecFactory
        Mock Get-PretripV22DistributionSpec {$script:V22ActiveSpec}
        $script:V22Fixture=New-V22DistributionFixture (Join-Path $TestDrive ([guid]::NewGuid().ToString('N')))
        $script:V22TestIdentity=$script:V22Fixture.Identity
        # Replace only the immutable test pin. ZIP/path/hash/extraction and the
        # unchanged PowerShell artifact guard operate on real synthetic bytes.
        Mock Get-PretripV22Identity {$script:V22TestIdentity}
        # Flush/read fixture metadata before taking any installer baseline.
        $null=Get-V22FixtureSnapshot $script:V22Fixture.Root
    }

    AfterEach {
        # Keep Pester's proxy intact, but restore its default behavior rather
        # than leaving a shared tiny-cap object visible to another test.
        $script:V22ActiveSpec=& $script:V22PristineSpecFactory
    }

    It 'validates a self-contained repository ZIP without a candidate or runtime' {
        $value=Assert-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root
        $value.Verified | Should -Be $true
        $value.Identities.model_tree_sha256 | Should -BeExactly $script:V22TestIdentity.ModelSHA256
        @($value.Files).Count | Should -Be 6
        Test-Path $script:V22Fixture.RuntimeRoot | Should -Be $false
        Test-Path (Join-Path $script:V22Fixture.Root '.sandbox/pretrip-v2/training-20261027-native-01') | Should -Be $false
    }

    It 'PLAN verifies inputs without creating runtime parents or changing source hashes and mtimes' {
        # The PS5 fixture's directory timestamps were observed changing between
        # two snapshots BEFORE any installer call. Settle that fixture metadata,
        # then require identical baselines; keep all file/directory mtime checks.
        $before=Get-V22FixtureSnapshot $script:V22Fixture.Root
        (Get-V22FixtureSnapshot $script:V22Fixture.Root) | Should -BeExactly $before
        $value=Install-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root
        $value.Status | Should -BeExactly 'PLAN'
        $value.Writes | Should -Be $false
        (Get-V22FixtureSnapshot $script:V22Fixture.Root) | Should -BeExactly $before
        Test-Path (Join-Path $script:V22Fixture.Root '.sandbox') | Should -Be $false
    }

    It 'installs real synthetic files and preserves every file and directory mtime on reuse' {
        $first=Install-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root -Execute
        $first.Status | Should -BeExactly 'INSTALLED'
        $first.Writes | Should -Be $true
        $guard=Assert-PretripV22Artifacts -ProjectRoot $script:V22Fixture.Root
        $guard.ModelSHA256 | Should -BeExactly $script:V22TestIdentity.ModelSHA256
        $before=Get-V22FixtureSnapshot $script:V22Fixture.RuntimeRoot
        $rerun=Install-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root -Execute
        $rerun.Status | Should -BeExactly 'VERIFIED_REUSED'
        $rerun.Writes | Should -Be $false
        $rerun.Skipped | Should -Be $true
        (Get-V22FixtureSnapshot $script:V22Fixture.RuntimeRoot) | Should -BeExactly $before
    }

    It 'works with an explicit project root when the current directory is elsewhere' {
        Push-Location $TestDrive
        try{$value=Install-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root -Execute}
        finally{Pop-Location}
        $value.Status | Should -BeExactly 'INSTALLED'
        Test-Path (Join-Path $TestDrive '.sandbox') | Should -Be $false
        Test-Path (Join-Path $script:V22Fixture.RuntimeRoot 'zones.csv') | Should -Be $true
    }

    It 'fails for a missing ZIP without creating a runtime' {
        Remove-Item -LiteralPath $script:V22Fixture.BundlePath
        {Install-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root -Execute}|Should -Throw
        Test-Path $script:V22Fixture.RuntimeRoot | Should -Be $false
    }

    It 'rejects corrupt ZIP bytes before writing anything' {
        [IO.File]::AppendAllText($script:V22Fixture.BundlePath,'synthetic corruption')
        {Install-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root -Execute}|Should -Throw
        Test-Path (Join-Path $script:V22Fixture.Root '.sandbox') | Should -Be $false
    }

    It 'rejects a changed manifest even if it claims its own checksum' {
        $manifest=Get-Content -LiteralPath $script:V22Fixture.ManifestPath -Raw|ConvertFrom-Json
        $manifest.identities.model_tree_sha256='0'*64
        Write-V22FixtureFile $script:V22Fixture.ManifestPath ($manifest|ConvertTo-Json -Depth 15 -Compress)
        {Assert-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root}|Should -Throw
    }

    It 'rejects identity drift even when transport checksums are freshly pinned' {
        $manifest=Get-Content -LiteralPath $script:V22Fixture.ManifestPath -Raw|ConvertFrom-Json
        $manifest.identities.model_tree_sha256='0'*64
        Update-V22FixturePins $script:V22Fixture $manifest
        {Assert-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root}|Should -Throw
    }

    It 'rejects corrupt members even when ZIP and manifest transport checksums are freshly pinned' {
        Set-V22FixtureZipMutation $script:V22Fixture -TamperBytes
        $manifest=Get-Content -LiteralPath $script:V22Fixture.ManifestPath -Raw|ConvertFrom-Json
        Update-V22FixturePins $script:V22Fixture $manifest
        {Assert-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root}|Should -Throw
    }

    It 'rejects unsafe archive names <Name> before extraction' -TestCases @(
        @{Name='../escape.txt'},@{Name='/absolute.txt'},@{Name='C:/drive.txt'},
        @{Name='safe/../escape.txt'},@{Name='safe\backslash.txt'},
        @{Name='safe/file:stream'},@{Name='safe/CON.txt'}
    ) {
        param($Name)
        Set-V22FixtureZipMutation $script:V22Fixture -ReplacementPath $Name
        $manifest=Get-Content -LiteralPath $script:V22Fixture.ManifestPath -Raw|ConvertFrom-Json
        Update-V22FixturePins $script:V22Fixture $manifest
        {Install-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root -Execute}|Should -Throw
        Test-Path $script:V22Fixture.RuntimeRoot | Should -Be $false
        Test-Path (Join-Path $TestDrive 'escape.txt') | Should -Be $false
    }

    It 'rejects symlink metadata in ZIP entries' {
        Set-V22FixtureZipMutation $script:V22Fixture -Symlink
        $manifest=Get-Content -LiteralPath $script:V22Fixture.ManifestPath -Raw|ConvertFrom-Json
        Update-V22FixturePins $script:V22Fixture $manifest
        {Assert-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root}|Should -Throw
    }

    It 'rejects case-colliding ZIP entries while entry count remains unchanged' {
        Set-V22FixtureZipMutation $script:V22Fixture -DuplicateCase
        $manifest=Get-Content -LiteralPath $script:V22Fixture.ManifestPath -Raw|ConvertFrom-Json
        Update-V22FixturePins $script:V22Fixture $manifest
        {Assert-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root}|Should -Throw
    }

    It 'rejects case-colliding manifest inventory paths' {
        $manifest=Get-Content -LiteralPath $script:V22Fixture.ManifestPath -Raw|ConvertFrom-Json
        $manifest.files[1].path=$manifest.files[0].path.ToUpperInvariant()
        Update-V22FixturePins $script:V22Fixture $manifest
        {Assert-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root}|Should -Throw
    }

    It 'enforces the expanded byte cap using a tiny test limit instead of a large file' {
        $manifest=Get-Content -LiteralPath $script:V22Fixture.ManifestPath -Raw|ConvertFrom-Json
        $script:V22ActiveSpec.MaxExpandedBytes=[long]$manifest.expanded_bytes-1
        {Install-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root -Execute}|Should -Throw
        Test-Path $script:V22Fixture.RuntimeRoot | Should -Be $false
    }

    It 'enforces each member byte cap with tiny synthetic files' {
        $script:V22ActiveSpec.MaxFileBytes=8
        {Assert-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root}|Should -Throw
    }

    It 'bounded ZIP stream reading rejects actual bytes beyond a claimed small length' {
        $entry=[pscustomobject]@{Length=4}
        $entry|Add-Member -MemberType ScriptMethod -Name Open -Value {
            return [IO.MemoryStream]::new([byte[]](1,2,3,4,5),$false)
        }
        {Read-PackageZipEntry $entry 4}|Should -Throw
    }

    It 'refuses a foreign pre-existing output without changing any file' {
        Write-V22FixtureFile (Join-Path $script:V22Fixture.RuntimeRoot 'foreign.txt') 'Synthetic foreign content'
        $before=Get-V22FixtureSnapshot $script:V22Fixture.RuntimeRoot
        {Install-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root -Execute}|Should -Throw
        (Get-V22FixtureSnapshot $script:V22Fixture.RuntimeRoot)|Should -BeExactly $before
    }

    It 'refuses a corrupt existing payload instead of overwriting it' {
        $null=Install-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root -Execute
        Write-V22FixtureFile (Join-Path $script:V22Fixture.RuntimeRoot 'artifacts/pipeline/model.txt') 'Different synthetic model'
        $before=Get-V22FixtureSnapshot $script:V22Fixture.RuntimeRoot
        {Install-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root -Execute}|Should -Throw
        (Get-V22FixtureSnapshot $script:V22Fixture.RuntimeRoot)|Should -BeExactly $before
    }

    It 'refuses unowned extra files even beside a valid model' {
        $null=Install-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root -Execute
        Write-V22FixtureFile (Join-Path $script:V22Fixture.RuntimeRoot 'foreign.txt') 'Extra synthetic content'
        $before=Get-V22FixtureSnapshot $script:V22Fixture.RuntimeRoot
        {Install-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root -Execute}|Should -Throw
        (Get-V22FixtureSnapshot $script:V22Fixture.RuntimeRoot)|Should -BeExactly $before
    }

    It 'reuses an exactly pinned optional legacy serving manifest without changing its mtime' {
        $null=Install-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root -Execute
        $legacy=Join-Path $script:V22Fixture.RuntimeRoot 'serving_manifest.json'
        Write-V22FixtureFile $legacy '{"synthetic_legacy_metadata":true,"status":"PREPARED_NOT_DEPLOYED"}'
        $pin=Get-Content -LiteralPath $script:V22Fixture.PinPath -Raw|ConvertFrom-Json
        $pin|Add-Member -MemberType NoteProperty -Name optional_existing_files -Value @(
            [pscustomobject]@{path='serving_manifest.json';bytes=[long](Get-Item -LiteralPath $legacy).Length;sha256=(Get-V22FixtureHash $legacy)}
        )
        Write-V22FixtureFile $script:V22Fixture.PinPath ($pin|ConvertTo-Json -Depth 15 -Compress)
        $before=Get-V22FixtureSnapshot $script:V22Fixture.RuntimeRoot
        $value=Install-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root -Execute
        $value.Status|Should -BeExactly 'VERIFIED_REUSED'
        $value.Writes|Should -Be $false
        (Get-V22FixtureSnapshot $script:V22Fixture.RuntimeRoot)|Should -BeExactly $before
    }

    It 'refuses a changed optional legacy serving manifest without overwriting it' {
        $null=Install-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root -Execute
        $legacy=Join-Path $script:V22Fixture.RuntimeRoot 'serving_manifest.json'
        Write-V22FixtureFile $legacy '{"synthetic_legacy_metadata":true}'
        $pin=Get-Content -LiteralPath $script:V22Fixture.PinPath -Raw|ConvertFrom-Json
        $pin|Add-Member -MemberType NoteProperty -Name optional_existing_files -Value @(
            [pscustomobject]@{path='serving_manifest.json';bytes=[long](Get-Item -LiteralPath $legacy).Length;sha256=(Get-V22FixtureHash $legacy)}
        )
        Write-V22FixtureFile $script:V22Fixture.PinPath ($pin|ConvertTo-Json -Depth 15 -Compress)
        Write-V22FixtureFile $legacy '{"synthetic_foreign_metadata":true}'
        $before=Get-V22FixtureSnapshot $script:V22Fixture.RuntimeRoot
        {Install-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root -Execute}|Should -Throw
        (Get-V22FixtureSnapshot $script:V22Fixture.RuntimeRoot)|Should -BeExactly $before
    }

    It 'rejects an optional existing-file exception outside the single serving-manifest name' {
        $pin=Get-Content -LiteralPath $script:V22Fixture.PinPath -Raw|ConvertFrom-Json
        $pin|Add-Member -MemberType NoteProperty -Name optional_existing_files -Value @(
            [pscustomobject]@{path='../foreign.json';bytes=2;sha256=('a'*64)}
        )
        Write-V22FixtureFile $script:V22Fixture.PinPath ($pin|ConvertTo-Json -Depth 15 -Compress)
        {Install-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root -Execute}|Should -Throw
        Test-Path $script:V22Fixture.RuntimeRoot|Should -Be $false
    }

    It 'does not publish output when final staged artifact validation fails' {
        Mock Assert-PretripV22InstalledRoot {throw 'Synthetic staged verification failure'} -ParameterFilter {$ProjectRoot -ne $script:V22Fixture.Root}
        {Install-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root -Execute}|Should -Throw
        Test-Path $script:V22Fixture.RuntimeRoot|Should -Be $false
    }

    It 'does not merge or overwrite a destination that appears before atomic publication' {
        Mock Assert-PretripV22InstalledRoot {
            Write-V22FixtureFile (Join-Path $script:V22Fixture.RuntimeRoot 'foreign.txt') 'Synthetic concurrent owner'
            return [pscustomobject]@{Verified=$true}
        } -ParameterFilter {$ProjectRoot -ne $script:V22Fixture.Root}
        {Install-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root -Execute}|Should -Throw
        [IO.File]::ReadAllText((Join-Path $script:V22Fixture.RuntimeRoot 'foreign.txt'))|Should -BeExactly 'Synthetic concurrent owner'
        @(Get-ChildItem -LiteralPath $script:V22Fixture.RuntimeRoot -Force).Count|Should -Be 1
    }

    It 'rejects reparse or junction project paths without following them' {
        Mock Get-Item {[pscustomobject]@{Attributes=[IO.FileAttributes]::ReparsePoint;PSIsContainer=$true}} -ParameterFilter {$LiteralPath -eq $script:V22Fixture.Root}
        {Assert-PretripV22Distribution -ProjectRoot $script:V22Fixture.Root}|Should -Throw
    }
}
