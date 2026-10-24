# Package operations reuse the existing worker CLIs; never train a model.
function Get-PackageBundleSelection {
    param([string]$ProjectRoot,[string]$BundleDirectory)
    $pinPath=Join-Path $ProjectRoot 'config/model_bundle.lock.json'
    if (-not (Test-Path -LiteralPath $pinPath -PathType Leaf)) { Stop-Startup 'Versioned model bundle source/lock is not established. No new-machine completion is possible.' }
    $pin=Get-Content -LiteralPath $pinPath -Raw -Encoding UTF8 | ConvertFrom-Json
    $identity=Get-FareModelIdentity
    if ($pin.schema_version -ne 1 -or $pin.version -ne 'ml-p4-v1' -or $pin.model_sha256 -cne $identity.ModelSHA256 -or
        $pin.training_run_id -cne $identity.TrainingRunID -or $pin.evaluation_run_id -cne $identity.EvaluationRunID -or
        $pin.archive_sha256 -cnotmatch '^[0-9a-f]{64}$' -or $pin.manifest_sha256 -cnotmatch '^[0-9a-f]{64}$') { Stop-Startup 'Model bundle version lock differs from the reviewed frozen identity.' }
    if (-not $BundleDirectory) {
        if($pin.distribution.status -cne 'repository-bundled' -or $pin.distribution.location -cne 'distribution/models/ml-p4-v1') {Stop-Startup 'Pinned repository model distribution location is not configured; no local-cache or network fallback.'}
        $BundleDirectory=Join-Path $ProjectRoot $pin.distribution.location
    }
    $BundleDirectory=Assert-PackageLocalPath -Path $BundleDirectory -Directory -AllowMissing
    if (-not (Test-Path -LiteralPath $BundleDirectory -PathType Container)) { Stop-Startup 'Pinned model bundle unavailable. Restore distribution/models/ml-p4-v1 from this repository version; no local-cache/download/latest/training fallback.' }
    $spec=Get-PackageBundleSpec
    $bundlePath=Join-Path $BundleDirectory $spec.Archive; $manifestPath=Join-Path $BundleDirectory $spec.Manifest
    $result=Assert-PackageBundle -ProjectRoot $ProjectRoot -BundlePath $bundlePath -ManifestPath $manifestPath -ManifestSHA256 $pin.manifest_sha256
    if ($result.BundleSHA256 -cne $pin.archive_sha256 -or (Get-Item -LiteralPath $bundlePath).Length -ne $pin.archive_bytes -or
        (Get-Item -LiteralPath $manifestPath).Length -ne $pin.manifest_bytes) { Stop-Startup 'Model bundle archive/manifest differs from its public version lock.' }
    return $result
}

function Read-PackageCheckpoint {
    param($Context)
    $path=Join-Path $Context.StateRoot 'checkpoint.json'
    Assert-NoEnvironmentReparsePoint $path
    if (-not (Test-Path -LiteralPath $path -PathType Leaf)) { return $null }
    if ((Get-Item -LiteralPath $path).Length -gt 65536) { Stop-Startup 'Package checkpoint is oversized.' }
    $state=Get-Content -LiteralPath $path -Raw -Encoding UTF8 | ConvertFrom-Json
    if ($state.Owner -ne 'setup_system_v1' -or $state.SchemaVersion -ne 1 -or $state.Project -ne $Context.Project -or
        (Get-NormalizedEnvironmentPath $state.RuntimeRoot) -ne (Get-NormalizedEnvironmentPath $Context.RuntimeRoot) -or
        $state.ModelSHA256 -cne (Get-FareModelIdentity).ModelSHA256) { Stop-Startup 'Checkpoint ownership/identity differs; do not adopt another runtime.' }
    return $state
}

function Write-PackageCheckpoint {
    param($Context,$State)
    Assert-NoEnvironmentReparsePoint $Context.StateRoot
    $null=New-Item -ItemType Directory -Path $Context.StateRoot -Force
    $State.UpdatedAtUTC=[DateTime]::UtcNow.ToString('o')
    $path=Join-Path $Context.StateRoot 'checkpoint.json'
    Assert-NoEnvironmentReparsePoint $path
    $temporary=Join-Path $Context.StateRoot ('checkpoint.'+[guid]::NewGuid().ToString('N')+'.tmp')
    [IO.File]::WriteAllText($temporary,($State|ConvertTo-Json -Depth 8),[Text.UTF8Encoding]::new($false))
    if (Test-Path -LiteralPath $path) { [IO.File]::Replace($temporary,$path,$null) }
    else { [IO.File]::Move($temporary,$path) }
}

function Enter-PackageExecutionLock {
    param($Context)
    # Keep the handle open across every stage. A checkpoint alone is not a lock.
    Assert-NoEnvironmentReparsePoint $Context.StateRoot
    $null=New-Item -ItemType Directory -Path $Context.StateRoot -Force
    $path=Join-Path $Context.StateRoot 'execution.lock'
    Assert-NoEnvironmentReparsePoint $path
    try { return [IO.File]::Open($path,[IO.FileMode]::OpenOrCreate,[IO.FileAccess]::ReadWrite,[IO.FileShare]::None) }
    catch { Stop-Startup 'Another package setup holds the execution lock, or its state directory is inaccessible. Do not run overlapping setup/bootstrap jobs.' }
}

function Get-PackageUpdateCommand {
    param($Context,[ValidateSet('postgres','mlflow','worker','fare-inference','dashboard')][string]$Service,[switch]$Build)
    $arguments=$Context.ComposeArguments+@('--profile','jobs')
    if($Build){$arguments+=@('build',$Service)}else{$arguments+=@('up','-d','--no-deps','--no-build','--pull','never',$Service)}
    return 'docker '+(($arguments | ForEach-Object {"'"+([string]$_).Replace("'","''")+"'"}) -join ' ')
}

function Test-PackageImageCode {
    param($Context,[string]$Service,[string]$Image)
    $inspect=Invoke-DockerCommand -Arguments @('image','inspect','--format','{{.Id}}',$Image) -WorkingDirectory $Context.Root -TimeoutSeconds $Context.CommandTimeoutSeconds
    if ($inspect.ExitCode -ne 0) { return $false }
    if ($Service -eq 'worker') { return (Test-BootstrapWorkerFreshness $Context -StepTimeoutSeconds $Context.CommandTimeoutSeconds) }
    $files=@('config/config.yaml')
    if ($Service -eq 'dashboard') {
        $files+=@('requirements-dashboard.txt','dashboard/app.py','src/nyc_taxi/__init__.py','src/nyc_taxi/common/__init__.py','src/nyc_taxi/common/config.py','src/nyc_taxi/database/__init__.py','src/nyc_taxi/database/postgres.py')
        $files+=Get-ChildItem (Join-Path $Context.Root 'src/nyc_taxi/dashboard') -File -Filter '*.py' | ForEach-Object {$_.FullName.Substring($Context.Root.Length+1).Replace('\','/')}
    } else {
        $files+=@('requirements.txt','requirements-worker-constraints.txt','pyproject.toml','Dockerfile.inference','docker-compose.inference.yml')
        $files+=Get-ChildItem (Join-Path $Context.Root 'src') -Recurse -File -Filter '*.py' | ForEach-Object {$_.FullName.Substring($Context.Root.Length+1).Replace('\','/')}
    }
    $hashes=@{}; foreach ($file in $files) {$hashes[$file]=(Get-FileHash (Join-Path $Context.Root $file) -Algorithm SHA256).Hash.ToLowerInvariant()}
    $probe='import hashlib,json,pathlib,sys; r=pathlib.Path("/app"); e=json.load(sys.stdin); ok=all((r/p).is_file() and hashlib.sha256((r/p).read_bytes()).hexdigest()==s for p,s in e.items()); print(json.dumps({"verified":ok})); sys.exit(not ok)'
    $result=Invoke-DockerCommand -Arguments @('run','--rm','-i','--pull','never','--network','none','--read-only',$Image,'python','-I','-B','-c',$probe) -InputText ($hashes|ConvertTo-Json -Compress) -WorkingDirectory $Context.Root -TimeoutSeconds $Context.CommandTimeoutSeconds
    return $result.ExitCode -eq 0
}

function Ensure-PackageImages {
    param($Context)
    # Never rebuild/reconcile an existing service that has drift. Print only a
    # public, targeted review instruction, not resolved environment or secrets.
    foreach ($service in @('postgres','mlflow','fare-inference','dashboard')) {
        $settings=$Context.Model.services.$service
        $container=Get-ServiceContainer $Context $service (Get-OptionalProperty $settings 'container_name' '')
        if ($null -ne $container) {
            $hash=Invoke-StartupCompose $Context @('config','--hash',$service) 'Cannot inspect service config hash'
            $expected=@($hash -split '\r?\n' | Where-Object {$_ -match ('^'+[regex]::Escape($service)+'\s+')})
            $image=Invoke-DockerCommand -Arguments @('image','inspect','--format','{{.Id}}',$settings.image) -WorkingDirectory $Context.Root -TimeoutSeconds $Context.CommandTimeoutSeconds
            if ($expected.Count -ne 1 -or $container.ConfigHash -ne ($expected[0] -split '\s+')[1] -or $image.ExitCode -ne 0 -or $container.ImageID -ne $image.StdOut.Trim() -or $container.ImageReference -ne $settings.image) {
                Write-Host ('Separate reviewed update command (NOT executed): '+(Get-PackageUpdateCommand $Context $service))
                Stop-Startup ('Existing '+$service+' has package config/image drift. Review and build/update ONLY that service with the package Compose layers and --no-deps; setup will not recreate it or bootstrap data.')
            }
        }
    }
    foreach ($service in @('postgres','mlflow')) {
        $image=$Context.Model.services.$service.image
        if ($image -notmatch '@sha256:[0-9a-f]{64}$') { Stop-Startup 'Package infrastructure image must be pinned by digest.' }
        $check=Invoke-DockerCommand -Arguments @('image','inspect','--format','{{.Id}}',$image) -WorkingDirectory $Context.Root -TimeoutSeconds $Context.CommandTimeoutSeconds
        if ($check.ExitCode -ne 0) { $null=Invoke-StartupDocker $Context @('pull',$image) 'Exact infrastructure image pull failed; no upgrade fallback' -TimeoutSeconds 1800 }
    }
    foreach ($service in @('worker','dashboard','fare-inference')) {
        $image=$Context.Model.services.$service.image
        if (-not (Test-PackageImageCode $Context $service $image)) {
            $container=Get-ServiceContainer $Context $service (Get-OptionalProperty $Context.Model.services.$service 'container_name' '')
            if ($null -ne $container) {
                Write-Host ('Separate reviewed build command (NOT executed): '+(Get-PackageUpdateCommand $Context $service -Build))
                if($service -ne 'worker'){Write-Host ('Then reviewed update (NOT executed): '+(Get-PackageUpdateCommand $Context $service))}
                Stop-Startup ('Packaged '+$service+' code is stale. Build/update separately; existing service preserved.')
            }
            Write-Host ('Building selected package image: '+$service+' (no training).')
            $null=Invoke-StartupCompose $Context @('--profile','jobs','build',$service) 'Package image build failed; see bounded build checkpoint, no old-image fallback' -TimeoutSeconds 7200
            if (-not (Test-PackageImageCode $Context $service $image)) { Stop-Startup 'Built package image code hashes do not match this checkout.' }
        } else { Write-Host ('Reusing hash-verified packaged image: '+$service) }
    }
    return [pscustomobject]@{Verified=$true}
}

function Start-PackagePhase {
    param($Context,[ValidateSet('infrastructure','application')][string]$Phase)
    $ctx=$Context.PSObject.Copy()
    $ctx.Services=if ($Phase -eq 'infrastructure') {@('postgres','mlflow')} else {@('postgres','mlflow','fare-inference','dashboard')}
    $code=Invoke-ProjectStart -ProjectRoot $ctx.Root -Environment $ctx.Environment -Context $ctx -WithInference:($Phase -eq 'application') -HealthTimeoutSeconds $ctx.HealthTimeoutSeconds -CommandTimeoutSeconds $ctx.CommandTimeoutSeconds
    if ($code -ne 0) { Stop-Startup ('Package '+$Phase+' startup failed/drifted (exit '+$code+'). Review targeted services; no automatic recreate.') }
    return 0
}

function Test-PackageFullValidation {
    param($Context)
    # The last existing bootstrap CLI executes collect_validation_report using
    # actual COUNT/FK/month/sanity SQL. Do not rescan fact or substitute load logs.
    $path=Join-Path $Context.RuntimeRoot 'reports/postgres_load_report.json'
    Assert-NoEnvironmentReparsePoint $path
    if (-not (Test-Path -LiteralPath $path -PathType Leaf) -or (Get-Item $path).Length -gt 1048576) { Stop-Startup 'Fresh full database validation report is absent/oversized.' }
    $report=Get-Content -LiteralPath $path -Raw -Encoding UTF8 | ConvertFrom-Json
    $generated=[DateTimeOffset]::Parse($report.generated_at_utc)
    if ($generated.UtcDateTime -lt $Context.ValidationNotBeforeUTC -or $report.validation_status -ne 'valid' -or
        $report.dimension_counts.location -ne 265 -or $report.dimension_counts.calendar -ne 365 -or $report.dimension_counts.weather -ne 4344 -or
        -not $report.view_trip_enriched_present) { Stop-Startup 'Full SQL validation is stale/invalid or dimensions/view are incomplete.' }
    $sum=[long]0
    foreach ($month in 1..6) {
        $name='2025-{0:00}' -f $month
        $count=Get-OptionalProperty $report.per_month_counts $name 0
        if ($count -lt 1000000) { Stop-Startup 'Full package requires six real-size monthly partitions, not the small fixture.' }
        $sum+=[long]$count
    }
    if (@($report.per_month_counts.PSObject.Properties).Count -ne 6 -or $sum -ne $report.fact_summary.total_trips) { Stop-Startup 'Actual full fact/month counts disagree.' }
    $goldPath=Join-Path $Context.RuntimeRoot 'data/gold/gold_manifest.json'
    Assert-NoEnvironmentReparsePoint $goldPath
    $gold=Get-Content -LiteralPath $goldPath -Raw -Encoding UTF8 | ConvertFrom-Json
    if(@($gold.datasets).Count -ne 6){Stop-Startup 'Gold manifest must contain all six partitions.'}
    $goldMonths=@($gold.datasets | ForEach-Object {'{0:0000}-{1:00}' -f [int]$_.partition.year,[int]$_.partition.month} | Sort-Object -Unique)
    $requiredMonths=@(1..6 | ForEach-Object {'2025-{0:00}' -f $_})
    if($goldMonths.Count -ne 6 -or @(Compare-Object $requiredMonths $goldMonths).Count -ne 0 -or
        @($gold.datasets | Group-Object dataset_name | Where-Object Count -ne 1).Count){Stop-Startup 'Gold manifest must have six distinct reviewed months and dataset identities.'}
    foreach($entry in $gold.datasets) {
        $month='2025-{0:00}' -f [int]$entry.partition.month
        if($entry.validation_status -ne 'valid' -or [int]$entry.partition.year -ne 2025 -or
            [long]$entry.gold_rows -ne [long](Get-OptionalProperty $report.per_month_counts $month -1) -or
            $entry.gold_batch_id -ne (Get-OptionalProperty $report.gold_batch_lineage $entry.dataset_name '')) {Stop-Startup 'Fresh SQL validation does not match current Gold partition counts/lineage.'}
    }
    foreach ($index in @('fact_trip_pickup_datetime_idx','fact_trip_pickup_location_idx','fact_trip_dropoff_location_idx','fact_trip_calendar_date_idx','fact_trip_weather_idx','fact_trip_pickup_hour_idx','dim_weather_local_hour_idx')) {
        if ($report.indexes -notcontains $index) { Stop-Startup 'Required analytical indexes are missing despite report status.' }
    }
    $artifacts=Assert-FareArtifacts -ProjectRoot $Context.Root -RuntimeRoot $Context.ModelRuntimeRoot
    $Context.InferenceArtifacts=$artifacts
    $settings=$Context.Model.services.'fare-inference'
    $container=Get-ServiceContainer $Context 'fare-inference' (Get-OptionalProperty $settings 'container_name' '')
    $null=Test-InferenceReadiness $Context $container
    return [pscustomobject]@{Verified=$true;TotalTrips=$sum;ReportSHA256=(Get-FileHash $path -Algorithm SHA256).Hash.ToLowerInvariant()}
}
