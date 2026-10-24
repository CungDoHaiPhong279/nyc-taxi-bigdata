# Aggregate contracts and offline public-bundle copies; no database or trip rows.
. (Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts/setup_system.ps1')
Describe 'Package completion requires fresh full counts, Gold lineage, indexes and model readiness' {
    BeforeEach {
        $script:ValidationRoot=Join-Path $TestDrive ('validation-'+[guid]::NewGuid().ToString('N'))
        $null=New-Item -ItemType Directory -Path (Join-Path $script:ValidationRoot 'reports') -Force
        $null=New-Item -ItemType Directory -Path (Join-Path $script:ValidationRoot 'data/gold') -Force
        $script:ValidationContext=[pscustomobject]@{Root=$script:ValidationRoot;RuntimeRoot=$script:ValidationRoot;
            ModelRuntimeRoot=(Join-Path $script:ValidationRoot 'model-bundle');InferenceArtifacts=$null;
            ValidationNotBeforeUTC=[DateTime]::UtcNow.AddMinutes(-1);Model=[pscustomobject]@{services=[pscustomobject]@{'fare-inference'=[pscustomobject]@{}}}}
        $counts=[ordered]@{};$lineage=[ordered]@{};$datasets=@()
        foreach($month in 1..6){$name='2025-{0:00}' -f $month;$key='taxi_'+$name;$counts[$name]=2000000;$lineage[$key]='batch-'+$month;
            $datasets+=@{dataset_name=$key;partition=@{year=2025;month=('{0:00}' -f $month)};gold_rows=2000000;gold_batch_id=('batch-'+$month);validation_status='valid'}}
        $script:ValidationReport=[ordered]@{generated_at_utc=[DateTime]::UtcNow.ToString('o');validation_status='valid';
            dimension_counts=@{location=265;calendar=365;weather=4344};view_trip_enriched_present=$true;
            fact_summary=@{total_trips=12000000};per_month_counts=$counts;gold_batch_lineage=$lineage;
            indexes=@('fact_trip_pickup_datetime_idx','fact_trip_pickup_location_idx','fact_trip_dropoff_location_idx','fact_trip_calendar_date_idx','fact_trip_weather_idx','fact_trip_pickup_hour_idx','dim_weather_local_hour_idx')}
        $script:ValidationGold=@{datasets=$datasets}
        Mock Assert-FareArtifacts {[pscustomobject]@{Verified=$true}}
        Mock Get-ServiceContainer {[pscustomobject]@{ID='fixture-service'}}
        Mock Test-InferenceReadiness {[pscustomobject]@{ready=$true}}
    }
    function Save-PackageValidationFixture {
        $script:ValidationReport|ConvertTo-Json -Depth 8|Set-Content -LiteralPath (Join-Path $script:ValidationRoot 'reports/postgres_load_report.json') -Encoding UTF8
        $script:ValidationGold|ConvertTo-Json -Depth 8|Set-Content -LiteralPath (Join-Path $script:ValidationRoot 'data/gold/gold_manifest.json') -Encoding UTF8
    }
    It 'accepts fresh independent aggregate counts matched to six Gold entries' {
        Save-PackageValidationFixture
        $r=Test-PackageFullValidation $script:ValidationContext
        $r.Verified|Should Be $true
        $r.TotalTrips|Should Be 12000000
        Assert-MockCalled Test-InferenceReadiness -Times 1 -Exactly -Scope It
    }
    It 'rejects stale validation instead of trusting an earlier checkpoint' {
        $script:ValidationReport.generated_at_utc=[DateTime]::UtcNow.AddHours(-1).ToString('o');Save-PackageValidationFixture
        {Test-PackageFullValidation $script:ValidationContext}|Should Throw 'stale/invalid'
    }
    It 'rejects loader valid status without the analytical view' {
        $script:ValidationReport.view_trip_enriched_present=$false;Save-PackageValidationFixture
        {Test-PackageFullValidation $script:ValidationContext}|Should Throw 'view are incomplete'
    }
    It 'rejects loader valid status without every required index' {
        $script:ValidationReport.indexes=@('fact_trip_pickup_datetime_idx');Save-PackageValidationFixture
        {Test-PackageFullValidation $script:ValidationContext}|Should Throw 'indexes are missing'
    }
    It 'rejects small fixtures even when marked valid' {
        $script:ValidationReport.per_month_counts['2025-01']=3;Save-PackageValidationFixture
        {Test-PackageFullValidation $script:ValidationContext}|Should Throw 'not the small fixture'
    }
    It 'rejects monthly counts whose total disagrees with actual fact aggregate' {
        $script:ValidationReport.fact_summary.total_trips=12000001;Save-PackageValidationFixture
        {Test-PackageFullValidation $script:ValidationContext}|Should Throw 'counts disagree'
    }
    It 'rejects changed Gold batch lineage after SQL validation' {
        $script:ValidationGold.datasets[0].gold_batch_id='changed';Save-PackageValidationFixture
        {Test-PackageFullValidation $script:ValidationContext}|Should Throw 'current Gold'
    }
    It 'rejects six duplicate manifest entries instead of proving six distinct months' {
        $script:ValidationGold.datasets=@(1..6 | ForEach-Object {$script:ValidationGold.datasets[0]});Save-PackageValidationFixture
        {Test-PackageFullValidation $script:ValidationContext}|Should Throw 'distinct reviewed months'
    }
    It 'rejects invalid dimensions despite a valid status string' {
        $script:ValidationReport.dimension_counts.location=264;Save-PackageValidationFixture
        {Test-PackageFullValidation $script:ValidationContext}|Should Throw 'dimensions/view'
    }
    It 'does not report ready when the frozen model readiness check fails' {
        Save-PackageValidationFixture;Mock Test-InferenceReadiness {throw 'fixture readiness unavailable'}
        {Test-PackageFullValidation $script:ValidationContext}|Should Throw 'readiness unavailable'
    }
    It 'rejects missing full validation reports' {
        {Test-PackageFullValidation $script:ValidationContext}|Should Throw 'report is absent'
    }
}

Describe 'Package bundle selection rejects remote paths before filesystem lookup' {
    BeforeEach {
        $script:SelectionRoot=Join-Path $TestDrive ('selection-'+[guid]::NewGuid().ToString('N'))
        $null=New-Item -ItemType Directory -Path (Join-Path $script:SelectionRoot 'config') -Force
        $identity=Get-FareModelIdentity
        $pin=@{schema_version=1;version='ml-p4-v1';training_run_id=$identity.TrainingRunId;evaluation_run_id=$identity.EvaluationRunId;
            model_sha256=$identity.ModelSHA256;manifest_sha256=('a'*64);archive_sha256=('b'*64)}
        [IO.File]::WriteAllText((Join-Path $script:SelectionRoot 'config/model_bundle.lock.json'),($pin|ConvertTo-Json))
        Mock Test-Path {throw 'Remote filesystem lookup must never be attempted.'} -ParameterFilter {$LiteralPath -match '^(\\\\|//|https://)'}
        Mock Assert-PackageBundle {throw 'A remote bundle must not reach archive validation.'}
    }
    foreach($remotePath in @('\\unreachable.invalid\share\bundle','//unreachable.invalid/share/bundle','https://unreachable.invalid/bundle')) {
        It ('rejects remote syntax without lookup: '+$remotePath) -TestCases @(@{Remote=$remotePath}) {
            param($Remote)
            {Get-PackageBundleSelection -ProjectRoot $script:SelectionRoot -BundleDirectory $Remote}|Should Throw 'Only explicit local filesystem paths'
            Assert-MockCalled Test-Path -Times 0 -Exactly -Scope It -ParameterFilter {$LiteralPath -match '^(\\\\|//|https://)'}
            Assert-MockCalled Assert-PackageBundle -Times 0 -Exactly -Scope It
        }
    }
}

Describe 'Repository bundle works from a source-only copy without local artifacts or cache' {
    BeforeEach {
        $script:PublicCopyRoot=Join-Path $TestDrive ('public-copy-'+[guid]::NewGuid().ToString('N'))
        $script:PublicSourceRoot=Split-Path -Parent $PSScriptRoot
        $script:PublicCopySpec=Get-PackageBundleSpec
        $script:PublicCopySources=@('src/nyc_taxi/ml/pilot.py','src/nyc_taxi/ml/pilot_data.py',
            'src/nyc_taxi/ml/pilot_model.py','src/nyc_taxi/ml/pilot_tracking.py','src/nyc_taxi/etl/silver.py',
            'requirements.txt','requirements-worker-constraints.txt','pyproject.toml')
        $script:PublicCopyFiles=$script:PublicCopySources+@('config/model_bundle.lock.json')+
            @($script:PublicCopySpec.Archive,$script:PublicCopySpec.Manifest,$script:PublicCopySpec.Checksums |
                ForEach-Object {'distribution/models/ml-p4-v1/'+$_})
        foreach($relative in $script:PublicCopyFiles) {
            $source=Join-Path $script:PublicSourceRoot $relative
            if(-not(Test-Path -LiteralPath $source -PathType Leaf)){throw ('Required repository public file is absent: '+$relative)}
            $destination=Join-Path $script:PublicCopyRoot $relative
            $null=New-Item -ItemType Directory -Path (Split-Path -Parent $destination) -Force
            [IO.File]::Copy($source,$destination,$false)
        }
        $script:PublicCopyPin=Get-Content -LiteralPath (Join-Path $script:PublicCopyRoot 'config/model_bundle.lock.json') -Raw -Encoding UTF8 | ConvertFrom-Json
        $script:PublicCopyBundleRoot=Join-Path $script:PublicCopyRoot 'distribution/models/ml-p4-v1'
        $script:PublicCopyTarget=Join-Path $script:PublicCopyRoot '.sandbox/package-test/model-bundle'
        # Only native Docker is mocked. Real archive, independent pins, source
        # hashes, staged artifact verification and installation all run offline.
        Mock Invoke-DockerCommand {throw 'An offline repository-bundle check must not invoke Docker.'}
    }

    AfterEach {
        Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
    }

    It 'copies exactly eight frozen inputs and four public package files with no runtime or secrets' {
        $actual=@(Get-ChildItem -LiteralPath $script:PublicCopyRoot -Recurse -File | ForEach-Object {
            $_.FullName.Substring($script:PublicCopyRoot.Length+1).Replace('\','/')
        })
        $actual.Count | Should Be 12
        @(Compare-Object $script:PublicCopyFiles $actual).Count | Should Be 0
        foreach($excluded in @('.git','.env','.venv','.sandbox','data','models','logs','reports')) {
            Test-Path -LiteralPath (Join-Path $script:PublicCopyRoot $excluded) | Should Be $false
        }
        foreach($relative in $script:PublicCopySources) {
            $copy=Join-Path $script:PublicCopyRoot $relative
            (Get-FileHash -LiteralPath $copy -Algorithm SHA256).Hash | Should BeExactly (Get-FileHash -LiteralPath (Join-Path $script:PublicSourceRoot $relative) -Algorithm SHA256).Hash
            ([Text.Encoding]::UTF8.GetString([IO.File]::ReadAllBytes($copy))).Contains("`r") | Should Be $false
        }
    }

    It 'selects the public default and verifies full model archive and independent manifest pins for real' {
        $checked=Get-PackageBundleSelection -ProjectRoot $script:PublicCopyRoot
        $checked.Verified | Should Be $true
        $checked.BundlePath | Should BeExactly (Join-Path $script:PublicCopyBundleRoot $script:PublicCopySpec.Archive)
        $checked.ManifestPath | Should BeExactly (Join-Path $script:PublicCopyBundleRoot $script:PublicCopySpec.Manifest)
        $checked.BundleSHA256 | Should BeExactly $script:PublicCopyPin.archive_sha256
        $checked.ManifestSHA256 | Should BeExactly $script:PublicCopyPin.manifest_sha256
        $checked.ModelSHA256 | Should BeExactly (Get-FareModelIdentity).ModelSHA256
        @($checked.Files).Count | Should Be 43
        Test-Path -LiteralPath $script:PublicCopyTarget | Should Be $false
    }

    It 'rejects a missing public archive rather than using original models or a local cache' {
        $archive=Join-Path $script:PublicCopyBundleRoot $script:PublicCopySpec.Archive
        [IO.File]::Move($archive,($archive+'.withheld'))
        {Get-PackageBundleSelection -ProjectRoot $script:PublicCopyRoot} | Should Throw 'Required local package path is missing'
        Test-Path -LiteralPath $script:PublicCopyTarget | Should Be $false
    }

    It 'rejects changed archive bytes before installing any model' {
        [IO.File]::AppendAllText((Join-Path $script:PublicCopyBundleRoot $script:PublicCopySpec.Archive),'fixture corruption')
        {Get-PackageBundleSelection -ProjectRoot $script:PublicCopyRoot} | Should Throw 'archive SHA256/size mismatch'
        Test-Path -LiteralPath $script:PublicCopyTarget | Should Be $false
    }

    It 'rejects changed manifest bytes instead of trusting its own checksum sidecar' {
        [IO.File]::AppendAllText((Join-Path $script:PublicCopyBundleRoot $script:PublicCopySpec.Manifest),"`n")
        {Get-PackageBundleSelection -ProjectRoot $script:PublicCopyRoot} | Should Throw 'manifest SHA256 mismatch'
        Test-Path -LiteralPath $script:PublicCopyTarget | Should Be $false
    }

    It 'rejects a different independently pinned checksum even for an intact repository bundle' {
        $script:PublicCopyPin.manifest_sha256='0'*64
        [IO.File]::WriteAllText((Join-Path $script:PublicCopyRoot 'config/model_bundle.lock.json'),($script:PublicCopyPin|ConvertTo-Json -Depth 8),[Text.UTF8Encoding]::new($false))
        {Get-PackageBundleSelection -ProjectRoot $script:PublicCopyRoot} | Should Throw 'manifest SHA256 mismatch'
    }

    It 'rejects a modified frozen input without rewriting its hash or installing artifacts' {
        [IO.File]::AppendAllText((Join-Path $script:PublicCopyRoot $script:PublicCopySources[0]),"`n# fixture-only changed input`n")
        {Get-PackageBundleSelection -ProjectRoot $script:PublicCopyRoot} | Should Throw 'source/dependency hash mismatch'
        Test-Path -LiteralPath $script:PublicCopyTarget | Should Be $false
    }

    It 'installs all 43 real bundle files into the copy and reruns without changing bytes or mtimes' {
        $checked=Get-PackageBundleSelection -ProjectRoot $script:PublicCopyRoot
        $parameters=@{ProjectRoot=$script:PublicCopyRoot;RuntimeRoot=$script:PublicCopyTarget;
            BundlePath=$checked.BundlePath;ManifestPath=$checked.ManifestPath;ManifestSHA256=$checked.ManifestSHA256}
        $first=Install-PackageModelBundle @parameters
        $first.Verified | Should Be $true
        $first.Skipped | Should Be $false
        $files=@(Get-ChildItem -LiteralPath $script:PublicCopyTarget -Recurse -File -Force | Sort-Object FullName)
        $files.Count | Should Be 43
        $before=@($files | ForEach-Object { $_.FullName+'|'+$_.LastWriteTimeUtc.Ticks+'|'+(Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash })
        $second=Install-PackageModelBundle @parameters
        $second.Verified | Should Be $true
        $second.Skipped | Should Be $true
        $after=@(Get-ChildItem -LiteralPath $script:PublicCopyTarget -Recurse -File -Force | Sort-Object FullName | ForEach-Object {
            $_.FullName+'|'+$_.LastWriteTimeUtc.Ticks+'|'+(Get-FileHash -LiteralPath $_.FullName -Algorithm SHA256).Hash
        })
        ($after -join "`n") | Should BeExactly ($before -join "`n")
        $verified=Assert-FareArtifacts -ProjectRoot $script:PublicCopyRoot -RuntimeRoot $script:PublicCopyTarget
        $verified.ModelSHA256 | Should BeExactly $script:PublicCopyPin.model_sha256
        $lock=Get-Content -LiteralPath $verified.CandidateLockPath -Raw -Encoding UTF8 | ConvertFrom-Json
        @($lock.core_file_sha256.PSObject.Properties).Count | Should Be 8
        foreach($relative in $script:PublicCopySources) {
            (Get-FileHash -LiteralPath (Join-Path $script:PublicCopyRoot $relative) -Algorithm SHA256).Hash.ToLowerInvariant() | Should BeExactly $lock.core_file_sha256.$relative
        }
        Test-Path -LiteralPath (Join-Path $script:PublicCopyRoot 'models') | Should Be $false
        Test-Path -LiteralPath (Join-Path $script:PublicCopyRoot 'data') | Should Be $false
    }

    It 'does not merge into or overwrite a pre-existing destination with unrelated contents' {
        $checked=Get-PackageBundleSelection -ProjectRoot $script:PublicCopyRoot
        $null=New-Item -ItemType Directory -Path $script:PublicCopyTarget -Force
        $sentinel=Join-Path $script:PublicCopyTarget 'unrelated.txt'
        [IO.File]::WriteAllText($sentinel,'invented private destination sentinel',[Text.UTF8Encoding]::new($false))
        $before=(Get-FileHash -LiteralPath $sentinel -Algorithm SHA256).Hash
        {Install-PackageModelBundle -ProjectRoot $script:PublicCopyRoot -RuntimeRoot $script:PublicCopyTarget -BundlePath $checked.BundlePath -ManifestPath $checked.ManifestPath -ManifestSHA256 $checked.ManifestSHA256} | Should Throw 'Missing artifact/source'
        (Get-FileHash -LiteralPath $sentinel -Algorithm SHA256).Hash | Should BeExactly $before
        @(Get-ChildItem -LiteralPath $script:PublicCopyTarget -Recurse -File -Force).Count | Should Be 1
    }
}
