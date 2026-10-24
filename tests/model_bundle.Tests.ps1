# Offline synthetic bundle tests. No actual model, Docker, network or service.
. (Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts/model_bundle.ps1')

function Write-BundleFixtureFile {
    param([string]$Path,[string]$Text)
    $null = New-Item -ItemType Directory -Path (Split-Path -Parent $Path) -Force
    [IO.File]::WriteAllText($Path,$Text,[Text.UTF8Encoding]::new($false))
}

function Update-BundleFixtureManifest {
    param($Package,$Manifest)
    $Manifest.archive.bytes = [long](Get-Item -LiteralPath $Package.BundlePath).Length
    $Manifest.archive.sha256 = (Get-FileHash -LiteralPath $Package.BundlePath -Algorithm SHA256).Hash.ToLowerInvariant()
    Write-BundleFixtureFile $Package.ManifestPath (($Manifest | ConvertTo-Json -Depth 10 -Compress) + "`n")
    return (Get-FileHash -LiteralPath $Package.ManifestPath -Algorithm SHA256).Hash.ToLowerInvariant()
}

function Set-BundleFixtureArchive {
    param($Package,[string]$ReplacementPath,[switch]$Symlink,[switch]$DuplicateCase,[switch]$Oversized)
    $zip = [IO.Compression.ZipFile]::Open($Package.BundlePath,[IO.Compression.ZipArchiveMode]::Update)
    try {
        $original = @($zip.Entries)[0]
        [byte[]]$bytes = Read-PackageZipEntry $original 262144
        $originalName = $original.FullName
        if ($Symlink) { $original.ExternalAttributes = -1577058304; return }
        if ($DuplicateCase) { $replacement = $originalName.ToUpperInvariant() }
        elseif ($ReplacementPath) { $replacement = $ReplacementPath; $original.Delete() }
        else { $replacement = $originalName; $original.Delete() }
        $entry = $zip.CreateEntry($replacement)
        $stream = $entry.Open()
        try {
            if ($Oversized) { $bytes = New-Object byte[] 262145 }
            $stream.Write($bytes,0,$bytes.Length)
        } finally { $stream.Dispose() }
    } finally { $zip.Dispose() }
}

Describe 'Local frozen model bundle' {
    BeforeEach {
        $script:BundleProject = Join-Path $TestDrive ([guid]::NewGuid().ToString('N'))
        $script:BundleSpec = Get-PackageBundleSpec
        $script:BundleOutput = Join-Path $script:BundleProject '.sandbox/package-bundles/ml-p4-v1'
        foreach ($name in @('src/nyc_taxi/ml/pilot.py','src/nyc_taxi/ml/pilot_data.py',
            'src/nyc_taxi/ml/pilot_model.py','src/nyc_taxi/ml/pilot_tracking.py','src/nyc_taxi/etl/silver.py',
            'requirements.txt','requirements-worker-constraints.txt','pyproject.toml')) {
            Write-BundleFixtureFile (Join-Path $script:BundleProject $name) ('synthetic reviewed source: ' + $name)
        }
        for($index=0;$index -lt 40;$index++) {
            $name = 'stage/part-' + $index.ToString('00') + '.txt'
            if($index -eq 0){$name='_SUCCESS'}
            if($index -eq 1){$name='stage/.part.crc'}
            $text = if($index -eq 0){''}else{'synthetic model ' + $index}
            Write-BundleFixtureFile (Join-Path (Join-Path $script:BundleProject $script:BundleSpec.ModelRoot) $name) $text
        }
        Write-BundleFixtureFile (Join-Path $script:BundleProject $script:BundleSpec.Feature) '{"synthetic_contract":true}'
        Write-BundleFixtureFile (Join-Path $script:BundleProject $script:BundleSpec.Zones) "LocationID,Zone,Borough`n1,Invented,Synthetic`n"
        $tree = Get-FareModelTree (Join-Path $script:BundleProject $script:BundleSpec.ModelRoot)
        $sources = [ordered]@{}
        foreach($name in @('src/nyc_taxi/ml/pilot.py','src/nyc_taxi/ml/pilot_data.py',
            'src/nyc_taxi/ml/pilot_model.py','src/nyc_taxi/ml/pilot_tracking.py','src/nyc_taxi/etl/silver.py',
            'requirements.txt','requirements-worker-constraints.txt','pyproject.toml')) {
            $sources[$name]=(Get-FileHash -LiteralPath (Join-Path $script:BundleProject $name) -Algorithm SHA256).Hash.ToLowerInvariant()
        }
        $lock = [ordered]@{training_run_id='d2772f1904d7472490a73b0ac1c0c5d4';model_tree=$tree;
            feature_contract_sha256=(Get-FileHash -LiteralPath (Join-Path $script:BundleProject $script:BundleSpec.Feature) -Algorithm SHA256).Hash.ToLowerInvariant();
            zone_lookup_sha256=(Get-FileHash -LiteralPath (Join-Path $script:BundleProject $script:BundleSpec.Zones) -Algorithm SHA256).Hash.ToLowerInvariant();core_file_sha256=$sources}
        Write-BundleFixtureFile (Join-Path $script:BundleProject $script:BundleSpec.Lock) ($lock | ConvertTo-Json -Depth 10 -Compress)
        $script:SyntheticBundlePin = [pscustomobject]@{TrainingRunId='d2772f1904d7472490a73b0ac1c0c5d4';
            EvaluationRunId='58d5008f0ed048f58599a859877ae336';ModelSHA256=$tree.sha256;
            CandidateLockSHA256=(Get-FileHash -LiteralPath (Join-Path $script:BundleProject $script:BundleSpec.Lock) -Algorithm SHA256).Hash.ToLowerInvariant()}
        # Only the immutable test pin is replaced. All tree/ZIP/byte/source checks
        # run for real against 40 tiny invented files, never real model artifacts.
        Mock Get-FareModelIdentity { $script:SyntheticBundlePin }
    }

    It 'creates a versioned43file bundle and verifies its independent manifest pin' {
        $package=New-PackageModelBundle -ProjectRoot $script:BundleProject -OutputDirectory $script:BundleOutput
        $package.Verified | Should -Be $true
        @($package.Files).Count | Should -Be 43
        $package.ModelSHA256 | Should -BeExactly $script:SyntheticBundlePin.ModelSHA256
        $package.BundleSHA256.Length | Should -Be 64
        $package.ManifestSHA256.Length | Should -Be 64
        Test-Path (Join-Path $script:BundleOutput $script:BundleSpec.Checksums) | Should -Be $true
        $package.Files.path | Should -Contain ($script:BundleSpec.ModelRoot + '/_SUCCESS')
        $package.Files.path | Should -Contain ($script:BundleSpec.ModelRoot + '/stage/.part.crc')
        $manifest = Get-Content -LiteralPath $package.ManifestPath -Raw -Encoding UTF8 | ConvertFrom-Json
        $manifest.distribution.status | Should -BeExactly 'local-only-unpublished'
        $manifest.distribution.redistribution_rights | Should -BeExactly 'unverified'
        $checked=Get-PackageModelBundle -ProjectRoot $script:BundleProject -BundlePath $package.BundlePath -ManifestPath $package.ManifestPath -ManifestSHA256 $package.ManifestSHA256
        $checked.BundleSHA256 | Should -BeExactly $package.BundleSHA256
    }

    It 'does not overwrite an existing versioned bundle' {
        $package=New-PackageModelBundle -ProjectRoot $script:BundleProject -OutputDirectory $script:BundleOutput
        { New-PackageModelBundle -ProjectRoot $script:BundleProject -OutputDirectory $script:BundleOutput } | Should -Throw 'already exists'
        (Get-FileHash -LiteralPath $package.BundlePath -Algorithm SHA256).Hash.ToLowerInvariant() | Should -BeExactly $package.BundleSHA256
    }

    It 'rejects output outside the dedicated fixed version path before creating it' {
        $outside=Join-Path $TestDrive 'wrong-output'
        { New-PackageModelBundle -ProjectRoot $script:BundleProject -OutputDirectory $outside } | Should -Throw 'fixed .sandbox'
        Test-Path $outside | Should -Be $false
    }

    It 'fails source verification without touching original artifact bytes' {
        $model=Join-Path $script:BundleProject ($script:BundleSpec.ModelRoot + '/_SUCCESS')
        $before=(Get-FileHash -LiteralPath $model -Algorithm SHA256).Hash
        Write-BundleFixtureFile (Join-Path $script:BundleProject 'src/nyc_taxi/ml/pilot.py') 'modified synthetic source'
        { New-PackageModelBundle -ProjectRoot $script:BundleProject -OutputDirectory $script:BundleOutput } | Should -Throw 'source mismatch'
        (Get-FileHash -LiteralPath $model -Algorithm SHA256).Hash | Should -BeExactly $before
        Test-Path $script:BundleOutput | Should -Be $false
    }

    It 'requires an independently supplied correct manifest SHA rather than its own claims' {
        $package=New-PackageModelBundle -ProjectRoot $script:BundleProject -OutputDirectory $script:BundleOutput
        { Assert-PackageBundle -ProjectRoot $script:BundleProject -BundlePath $package.BundlePath -ManifestPath $package.ManifestPath -ManifestSHA256 ('0' * 64) } | Should -Throw 'manifest SHA256 mismatch'
    }

    It 'rejects changed archive bytes before extraction' {
        $package=New-PackageModelBundle -ProjectRoot $script:BundleProject -OutputDirectory $script:BundleOutput
        [IO.File]::AppendAllText($package.BundlePath,'corruption')
        { Assert-PackageBundle -ProjectRoot $script:BundleProject -BundlePath $package.BundlePath -ManifestPath $package.ManifestPath -ManifestSHA256 $package.ManifestSHA256 } | Should -Throw 'archive SHA256/size mismatch'
    }

    It 'installs atomically into a dedicated root with source hashes still checked at code root' {
        $package=New-PackageModelBundle -ProjectRoot $script:BundleProject -OutputDirectory $script:BundleOutput
        $target=Join-Path $script:BundleProject '.sandbox/package-test/model-bundle'
        $installed=Install-PackageModelBundle -ProjectRoot $script:BundleProject -RuntimeRoot $target -BundlePath $package.BundlePath -ManifestPath $package.ManifestPath -ManifestSHA256 $package.ManifestSHA256
        $installed.Verified | Should -Be $true
        $installed.Skipped | Should -Be $false
        $checked=Assert-FareArtifacts -ProjectRoot $script:BundleProject -RuntimeRoot $target
        $checked.ModelSHA256 | Should -BeExactly $package.ModelSHA256
        Test-Path (Join-Path $target 'src') | Should -Be $false
        Test-Path (Join-Path $target 'requirements.txt') | Should -Be $false
        @(Get-ChildItem (Split-Path $target -Parent) -Filter '.model-install-*' -Directory).Count | Should -Be 0
    }

    It 'skips an existing fully valid install without changing bytes or timestamps' {
        $package=New-PackageModelBundle -ProjectRoot $script:BundleProject -OutputDirectory $script:BundleOutput
        $target=Join-Path $script:BundleProject '.sandbox/package-model'
        $null=Install-PackageModelBundle -ProjectRoot $script:BundleProject -RuntimeRoot $target -BundlePath $package.BundlePath -ManifestPath $package.ManifestPath -ManifestSHA256 $package.ManifestSHA256
        $probe=Get-Item -LiteralPath (Join-Path $target $script:BundleSpec.Lock)
        $before=$probe.LastWriteTimeUtc.Ticks
        $rerun=Install-PackageModelBundle -ProjectRoot $script:BundleProject -RuntimeRoot $target -BundlePath $package.BundlePath -ManifestPath $package.ManifestPath -ManifestSHA256 $package.ManifestSHA256
        $rerun.Skipped | Should -Be $true
        (Get-Item -LiteralPath $probe.FullName).LastWriteTimeUtc.Ticks | Should -Be $before
    }

    It 'fails for any pre-existing incomplete or wrong destination without overwriting it' {
        $package=New-PackageModelBundle -ProjectRoot $script:BundleProject -OutputDirectory $script:BundleOutput
        $target=Join-Path $script:BundleProject '.sandbox/foreign-model'
        $marker=Join-Path $target 'foreign.txt'
        Write-BundleFixtureFile $marker 'foreign user content'
        { Install-PackageModelBundle -ProjectRoot $script:BundleProject -RuntimeRoot $target -BundlePath $package.BundlePath -ManifestPath $package.ManifestPath -ManifestSHA256 $package.ManifestSHA256 } | Should -Throw
        [IO.File]::ReadAllText($marker) | Should -BeExactly 'foreign user content'
        @(Get-ChildItem -LiteralPath $target -Force).Count | Should -Be 1
    }

    It 'rejects unsafe ZIP paths even with recomputed transport checksums' -TestCases @(
        @{Name='../escape.txt'},@{Name='/absolute.txt'},@{Name='C:/drive.txt'},
        @{Name='safe/../escape.txt'},@{Name='safe\backslash.txt'},@{Name='safe/file:stream'},@{Name='safe/CON.txt'}
    ) {
        param($Name)
        $package=New-PackageModelBundle -ProjectRoot $script:BundleProject -OutputDirectory $script:BundleOutput
        Set-BundleFixtureArchive -Package $package -ReplacementPath $Name
        $manifest=Get-Content -LiteralPath $package.ManifestPath -Raw -Encoding UTF8 | ConvertFrom-Json
        $newPin=Update-BundleFixtureManifest $package $manifest
        { Assert-PackageBundle -ProjectRoot $script:BundleProject -BundlePath $package.BundlePath -ManifestPath $package.ManifestPath -ManifestSHA256 $newPin } | Should -Throw
        Test-Path (Join-Path $TestDrive 'escape.txt') | Should -Be $false
    }

    It 'rejects symlink metadata in ZIP entries' {
        $package=New-PackageModelBundle -ProjectRoot $script:BundleProject -OutputDirectory $script:BundleOutput
        Set-BundleFixtureArchive -Package $package -Symlink
        $manifest=Get-Content -LiteralPath $package.ManifestPath -Raw -Encoding UTF8 | ConvertFrom-Json
        $newPin=Update-BundleFixtureManifest $package $manifest
        { Assert-PackageBundle -ProjectRoot $script:BundleProject -BundlePath $package.BundlePath -ManifestPath $package.ManifestPath -ManifestSHA256 $newPin } | Should -Throw 'symlinks/special'
    }

    It 'rejects extra case-colliding ZIP entries' {
        $package=New-PackageModelBundle -ProjectRoot $script:BundleProject -OutputDirectory $script:BundleOutput
        Set-BundleFixtureArchive -Package $package -DuplicateCase
        $manifest=Get-Content -LiteralPath $package.ManifestPath -Raw -Encoding UTF8 | ConvertFrom-Json
        $newPin=Update-BundleFixtureManifest $package $manifest
        { Assert-PackageBundle -ProjectRoot $script:BundleProject -BundlePath $package.BundlePath -ManifestPath $package.ManifestPath -ManifestSHA256 $newPin } | Should -Throw 'exactly 43'
    }

    It 'rejects oversized expanded entries instead of accepting a compressed archive bomb' {
        $package=New-PackageModelBundle -ProjectRoot $script:BundleProject -OutputDirectory $script:BundleOutput
        Set-BundleFixtureArchive -Package $package -Oversized
        $manifest=Get-Content -LiteralPath $package.ManifestPath -Raw -Encoding UTF8 | ConvertFrom-Json
        $newPin=Update-BundleFixtureManifest $package $manifest
        { Assert-PackageBundle -ProjectRoot $script:BundleProject -BundlePath $package.BundlePath -ManifestPath $package.ManifestPath -ManifestSHA256 $newPin } | Should -Throw
    }

    It 'rejects duplicate case-colliding manifest inventory paths' {
        $package=New-PackageModelBundle -ProjectRoot $script:BundleProject -OutputDirectory $script:BundleOutput
        $manifest=Get-Content -LiteralPath $package.ManifestPath -Raw -Encoding UTF8 | ConvertFrom-Json
        $manifest.files[1].path=$manifest.files[0].path.ToUpperInvariant()
        $newPin=Update-BundleFixtureManifest $package $manifest
        { Assert-PackageBundle -ProjectRoot $script:BundleProject -BundlePath $package.BundlePath -ManifestPath $package.ManifestPath -ManifestSHA256 $newPin } | Should -Throw 'case-colliding manifest'
    }

    It 'rejects a mismatched frozen identity even when a caller pins that manifest' {
        $package=New-PackageModelBundle -ProjectRoot $script:BundleProject -OutputDirectory $script:BundleOutput
        $manifest=Get-Content -LiteralPath $package.ManifestPath -Raw -Encoding UTF8 | ConvertFrom-Json
        $manifest.training_run_id='0' * 32
        $newPin=Update-BundleFixtureManifest $package $manifest
        { Assert-PackageBundle -ProjectRoot $script:BundleProject -BundlePath $package.BundlePath -ManifestPath $package.ManifestPath -ManifestSHA256 $newPin } | Should -Throw 'identity mismatch'
    }

    It 'verifies changed code-root sources even when a valid runtime install exists' {
        $package=New-PackageModelBundle -ProjectRoot $script:BundleProject -OutputDirectory $script:BundleOutput
        $target=Join-Path $script:BundleProject '.sandbox/package-model'
        $null=Install-PackageModelBundle -ProjectRoot $script:BundleProject -RuntimeRoot $target -BundlePath $package.BundlePath -ManifestPath $package.ManifestPath -ManifestSHA256 $package.ManifestSHA256
        Write-BundleFixtureFile (Join-Path $script:BundleProject 'requirements.txt') 'changed dependency fixture'
        { Install-PackageModelBundle -ProjectRoot $script:BundleProject -RuntimeRoot $target -BundlePath $package.BundlePath -ManifestPath $package.ManifestPath -ManifestSHA256 $package.ManifestSHA256 } | Should -Throw 'source/dependency hash mismatch'
    }

    It 'does not publish a destination if final staged artifact verification fails' {
        $package=New-PackageModelBundle -ProjectRoot $script:BundleProject -OutputDirectory $script:BundleOutput
        $target=Join-Path $script:BundleProject '.sandbox/failed-install'
        $original=(Get-FileHash -LiteralPath (Join-Path $script:BundleProject $script:BundleSpec.Lock) -Algorithm SHA256).Hash
        Mock Assert-FareArtifacts { throw 'Synthetic staged verification failure' } -ParameterFilter { $RuntimeRoot -like '*.model-install-*' }
        { Install-PackageModelBundle -ProjectRoot $script:BundleProject -RuntimeRoot $target -BundlePath $package.BundlePath -ManifestPath $package.ManifestPath -ManifestSHA256 $package.ManifestSHA256 } | Should -Throw 'staged verification failure'
        Test-Path $target | Should -Be $false
        (Get-FileHash -LiteralPath (Join-Path $script:BundleProject $script:BundleSpec.Lock) -Algorithm SHA256).Hash | Should -BeExactly $original
    }

    It 'rejects a mocked filesystem junction without following it' {
        Mock Get-Item { [pscustomobject]@{Attributes=[IO.FileAttributes]::ReparsePoint;PSIsContainer=$true} } -ParameterFilter { $LiteralPath -eq $script:BundleProject }
        { Assert-PackageLocalPath -Path $script:BundleProject -Directory } | Should -Throw 'Symlink/junction'
    }

    It 'CLI Create defaults to PLAN without creating the version output' {
        $scriptPath=Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts/model_bundle.ps1'
        $output = & powershell.exe -NoProfile -NonInteractive -ExecutionPolicy Bypass -File $scriptPath -Action Create -ProjectRoot $script:BundleProject 2>&1
        $LASTEXITCODE | Should -Be 0
        ($output -join "`n") | Should -Match 'PLAN: Create'
        Test-Path $script:BundleOutput | Should -Be $false
    }
}
