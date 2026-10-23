#requires -Version 5.1
[CmdletBinding()]
param([switch]$Build, [switch]$TestOnly, [switch]$Execute)

# Preserve options before dot-sourcing a helper script that has its own params.
$holdoutOptions = @{ Build=[bool]$Build; TestOnly=[bool]$TestOnly; Execute=[bool]$Execute }
. (Join-Path $PSScriptRoot 'start.ps1')
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$root = Split-Path -Parent $PSScriptRoot
$runtimeRelative = 'models/ml-p3/20261023-june01'
$runtime = Join-Path $root $runtimeRelative
$training = Join-Path $root 'models/ml-p2/20261023-pilot02'
# Matches paths.raw=data/raw and the existing acquisition CLI's taxi_zones path.
$zones = Join-Path $root 'data/raw/taxi_zones/taxi_zone_lookup.csv'
$workerTag = 'nyc-taxi-worker:ml-p3-20261023'
$jobPrefix = 'nyc-taxi-ml-p3-20261023-'
$artifactPath = '/mlflow/artifacts/ml-p2-20261022'
$sparkLocalDirs = '/tmp/ml-p3-spark'
$savedEnvironment = @{}
foreach ($name in @('MLP2_TRACKING_URI','MLP2_WORKER_IMAGE','MLP2_RUNTIME_DIR','MLP2_SPARK_LOCAL_DIRS')) {
    $savedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}
$script:DockerCliPath = Resolve-DockerCli
if (-not $script:DockerCliPath) { throw 'Docker CLI is unavailable.' }

function Invoke-HoldoutDocker {
    param([string[]]$Arguments, [int]$Timeout=60, [string]$InputText)
    $result = Invoke-DockerCommand -Arguments $Arguments -WorkingDirectory $root -InputText $InputText -TimeoutSeconds $Timeout
    if ($result.ExitCode -ne 0) { throw ('ML P3 Docker check failed (exit '+$result.ExitCode+'); raw output withheld.') }
    return $result.StdOut
}

function Assert-HoldoutPath {
    param([string]$Path)
    $absolute = [IO.Path]::GetFullPath($Path)
    $prefix = [IO.Path]::GetFullPath($root).TrimEnd('\','/') + [IO.Path]::DirectorySeparatorChar
    if (-not $absolute.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) { throw 'ML P3 path escaped the project.' }
    $current = $absolute
    while ($current -and $current.Length -ge $root.Length) {
        if ((Test-Path -LiteralPath $current) -and ((Get-Item -Force -LiteralPath $current).Attributes -band [IO.FileAttributes]::ReparsePoint)) {
            throw 'ML P3 inputs/runtime must not traverse a reparse point.'
        }
        if ($current -eq $root) { break }
        $current = Split-Path -Parent $current
    }
}

function Assert-NoHoldoutWorker {
    $mlJobs = Invoke-HoldoutDocker @('container','ls','-a','--filter','name=^/nyc-taxi-ml-','--format','{{.ID}}')
    $workers = Invoke-HoldoutDocker @('container','ls','-a','--filter','label=com.docker.compose.project=nyc-taxi-bigdata','--filter','label=com.docker.compose.service=worker','--format','{{.ID}}')
    if ($mlJobs.Trim() -or $workers.Trim()) { throw 'An ML/current-project worker exists; inspect it before retrying. Parallel jobs are refused.' }
}

try {
    $services = @{}
    foreach ($entry in @(@{Name='nyc-taxi-postgres';Service='postgres'},@{Name='nyc-taxi-mlflow';Service='mlflow'},@{Name='nyc-taxi-bigdata-dashboard-1';Service='dashboard'})) {
        $template = '{"ID":{{json .Id}},"StartedAt":{{json .State.StartedAt}},"Health":{{json .State.Health.Status}},"Project":{{json (index .Config.Labels "com.docker.compose.project")}},"Service":{{json (index .Config.Labels "com.docker.compose.service")}},"Networks":{{json .NetworkSettings.Networks}}}'
        $value = ConvertFrom-StartupJson (Invoke-HoldoutDocker @('container','inspect','--format',$template,$entry.Name)) 'ML P3 service metadata'
        if ($value.Health -ne 'healthy' -or $value.Project -ne 'nyc-taxi-bigdata' -or $value.Service -ne $entry.Service) {
            throw 'Existing project services must already be healthy with reviewed ownership; none will be started/recreated.'
        }
        $services[$entry.Service] = $value
    }
    $ip = $services.mlflow.Networks.'nyc-taxi-bigdata_default'.IPAddress
    if ($ip -notmatch '^172\.(1[6-9]|2\d|3[01])\.\d+\.\d+$') { throw 'Reviewed current-project MLflow private IP is unavailable.' }
    $trackingUri = 'http://' + $ip + ':5000'
    $serverMounts = ConvertFrom-StartupJson (Invoke-HoldoutDocker @('container','inspect','--format','{{json .Mounts}}',$services.mlflow.ID)) 'MLflow mount metadata'
    if (@($serverMounts | Where-Object { $_.Destination -eq '/mlflow' -and $_.Type -eq 'volume' -and $_.Name -eq 'nyc-taxi-bigdata_mlflow-data' }).Count -ne 1) {
        throw 'MLflow must retain the reviewed existing artifact volume.'
    }
    $env:MLP2_TRACKING_URI = $trackingUri
    $env:MLP2_WORKER_IMAGE = $workerTag
    $env:MLP2_RUNTIME_DIR = './' + $runtimeRelative
    $env:MLP2_SPARK_LOCAL_DIRS = $sparkLocalDirs
    $compose = @('compose','--project-name','nyc-taxi-bigdata','--project-directory',$root,'-f',(Join-Path $root 'docker-compose.yml'),'-f',(Join-Path $root 'docker-compose.ml-pilot.yml'),'-f',(Join-Path $root 'docker-compose.ml-holdout.yml'),'--profile','jobs')
    $null = Invoke-HoldoutDocker ($compose + @('config','--quiet'))
    # Resolve privately: do not print environments, credentials, or other config.
    $resolved = ConvertFrom-StartupJson (Invoke-HoldoutDocker ($compose + @('config','--format','json'))) 'ML P3 Compose model'
    $worker = $resolved.services.worker
    if ($resolved.name -ne 'nyc-taxi-bigdata' -or $resolved.networks.default.name -ne 'nyc-taxi-bigdata_default' -or
        $worker.image -ne $workerTag -or $worker.cpus -ne 2 -or $worker.mem_limit -ne 3GB -or @($worker.volumes).Count -ne 6) {
        throw 'Unexpected holdout project/image/network/resource/mount configuration.'
    }
    if ($worker.environment.SPARK_LOCAL_DIRS -ne $sparkLocalDirs -or $worker.environment.JAVA_TOOL_OPTIONS -ne '-XX:ActiveProcessorCount=2' -or
        $worker.environment.PYSPARK_SUBMIT_ARGS -ne '--driver-memory 1536m --conf spark.ui.enabled=false pyspark-shell' -or
        $worker.environment.MLFLOW_TRACKING_URI -ne $trackingUri) { throw 'Unexpected holdout Spark/MLflow environment.' }
    $expectedBinds = @{
        '/gold'=(Join-Path $root 'data/gold'); '/ml-output'=$runtime
        '/ml-contract/p1-profile.json'=(Join-Path $root 'reports/ml_p1_profile.json')
        '/training-input'=$training; '/ml-contract/zones.csv'=$zones
    }
    foreach ($target in $expectedBinds.Keys) {
        $expected = [IO.Path]::GetFullPath($expectedBinds[$target])
        Assert-HoldoutPath $expected
        if ($target -ne '/ml-output') {
            $pathType = if ($target -in @('/gold','/training-input')) { 'Container' } else { 'Leaf' }
            if (-not (Test-Path -LiteralPath $expected -PathType $pathType)) { throw 'A required holdout read-only input is missing or has the wrong type.' }
        }
        $bind = @($worker.volumes | Where-Object { $_.target -eq $target -and $_.type -eq 'bind' })
        if ($bind.Count -ne 1 -or [IO.Path]::GetFullPath($bind[0].source) -ne $expected -or
            [bool](Get-OptionalProperty $bind[0] 'read_only' $false) -ne ($target -ne '/ml-output')) {
            throw 'Holdout bind source/access mode differs from its dedicated scope.'
        }
    }
    $shared = @($worker.volumes | Where-Object { $_.target -eq $artifactPath -and $_.type -eq 'volume' })
    if ($shared.Count -ne 1 -or $shared[0].source -ne 'ml-p2-artifacts' -or $shared[0].volume.subpath -ne 'artifacts/ml-p2-20261022' -or
        $resolved.volumes.'ml-p2-artifacts'.name -ne 'nyc-taxi-bigdata_mlflow-data') { throw 'Holdout artifact mount differs from the existing owned subpath.' }
    $resources = ConvertFrom-StartupJson (Invoke-HoldoutDocker @('info','--format','{"CPUs":{{.NCPU}},"MemoryBytes":{{.MemTotal}}}')) 'Docker resource budget'
    $drive = New-Object IO.DriveInfo([IO.Path]::GetPathRoot($root))
    if ($resources.CPUs -lt 2 -or $resources.MemoryBytes -lt 7GB -or $drive.AvailableFreeSpace -lt 20GB) { throw 'Reviewed holdout resource/disk budget is unavailable.' }
    Write-Host 'ML P3 PLAN: evaluate the locked June holdout only; frozen training inputs read-only; no fitting, tuning, or service lifecycle actions.'
    Write-Host ('Image='+$workerTag+'; output='+$runtimeRelative+'; inner timeout=1200s; CPU=2; container=3 GiB; heap=1536 MiB.')
    if (-not ($holdoutOptions.Build -or $holdoutOptions.TestOnly -or $holdoutOptions.Execute)) { exit 0 }

    Assert-NoHoldoutWorker
    $baseId = (Invoke-HoldoutDocker @('image','inspect','--format','{{.Id}}','nyc-taxi-worker:p6-clean')).Trim()
    $trainingImageId = (Invoke-HoldoutDocker @('image','inspect','--format','{{.Id}}','nyc-taxi-worker:ml-p21')).Trim()
    if ($holdoutOptions.Build) {
        $null = Invoke-HoldoutDocker ($compose + @('build','worker')) -Timeout 600
        foreach ($old in @(@{Tag='nyc-taxi-worker:p6-clean';ID=$baseId},@{Tag='nyc-taxi-worker:ml-p21';ID=$trainingImageId})) {
            if ((Invoke-HoldoutDocker @('image','inspect','--format','{{.Id}}',$old.Tag)).Trim() -ne $old.ID) { throw 'A pre-existing worker image tag changed.' }
        }
        Write-Host 'Holdout-only code-refresh image built; no dependency upgrade or clean-build claim.'
    }
    $image = (Invoke-HoldoutDocker @('image','inspect','--format','{{.Id}}',$workerTag)).Trim()
    $files = @('requirements.txt','requirements-worker-constraints.txt','pyproject.toml','config/config.yaml','Dockerfile.ml',
        'docker-compose.ml-pilot.yml','docker-compose.ml-holdout.yml','scripts/evaluate_holdout.py','scripts/evaluate_holdout.ps1')
    foreach ($scope in @(@{Directory='src';Pattern='*.py'},@{Directory='sql';Pattern='*.sql'},@{Directory='tests';Pattern='test_ml_holdout*.py'},@{Directory='tests';Pattern='test_ml_pilot*.py'})) {
        $files += Get-ChildItem -LiteralPath (Join-Path $root $scope.Directory) -File -Recurse -Filter $scope.Pattern |
            ForEach-Object { $_.FullName.Substring($root.Length+1).Replace('\','/') }
    }
    $hashes = [ordered]@{}
    foreach ($file in @($files | Sort-Object -Unique)) { $hashes[$file] = (Get-FileHash -LiteralPath (Join-Path $root $file) -Algorithm SHA256).Hash.ToLowerInvariant() }
    $payload = $hashes | ConvertTo-Json -Compress
    $probe = 'import hashlib,json,pathlib,sys; root=pathlib.Path("/app"); expected=json.load(sys.stdin); bad=[name for name,d in expected.items() if not(root/name).is_file() or hashlib.sha256((root/name).read_bytes()).hexdigest()!=d]; print(json.dumps({"verified":not bad,"files":len(expected),"mismatches":bad})); sys.exit(bool(bad))'
    $comparison = ConvertFrom-StartupJson (Invoke-HoldoutDocker @('run','--rm','-i','--pull','never','--network','none','--read-only','--memory','256m',$image,'python','-I','-B','-c',$probe) -InputText $payload) 'Holdout packaged hash probe'
    if (-not $comparison.verified -or $comparison.files -ne $hashes.Count) { throw 'Holdout image is stale; build separately instead of mounting source.' }
    if (-not ($holdoutOptions.TestOnly -or $holdoutOptions.Execute)) { Write-Host ('Verified holdout image: '+$comparison.files+' public files.'); exit 0 }

    $memoryProbe = 'import json; m={k:int(v.split()[0])*1024 for k,v in (line.split(":",1) for line in open("/proc/meminfo")) if k in ("MemTotal","MemAvailable")}; print(json.dumps(m)); assert m["MemAvailable"]>=int(3.5*1024**3), "Insufficient VM available RAM"'
    $available = ConvertFrom-StartupJson (Invoke-HoldoutDocker @('run','--rm','--pull','never','--network','none','--read-only','--memory','256m',$image,'python','-I','-B','-c',$memoryProbe)) 'VM available memory'
    Assert-NoHoldoutWorker
    # Never alter a prior real evaluation's provenance or rerun automatically.
    foreach ($filename in @('holdout_started.json','evaluation_started.json','evaluation_report.json')) {
        if (Test-Path -LiteralPath (Join-Path $runtime $filename)) { throw 'An evaluation attempt already exists; inspect it, do not overwrite/retry.' }
    }
    if ((Test-Path -LiteralPath $runtime) -and -not (Test-Path -LiteralPath (Join-Path $runtime 'ml-p3-owner.json'))) { throw 'Holdout runtime lacks its ownership marker.' }
    $null = New-Item -ItemType Directory -Path $runtime -Force
    $marker = Join-Path $runtime 'ml-p3-owner.json'
    Assert-HoldoutPath $marker
    if (-not (Test-Path -LiteralPath $marker)) { [IO.File]::WriteAllText($marker,'{"scope":"ML P3","owner":"evaluate_holdout_v1"}') }
    $owner = Get-Content -Raw -Encoding UTF8 -LiteralPath $marker | ConvertFrom-Json
    if ($owner.scope -ne 'ML P3' -or $owner.owner -ne 'evaluate_holdout_v1') { throw 'Holdout runtime ownership mismatch.' }
    $sha = [Security.Cryptography.SHA256]::Create()
    try { $inventoryHash = ([BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($payload)))).Replace('-','').ToLowerInvariant() } finally { $sha.Dispose() }
    $identity = @{verified=$true;inventory_sha256=$inventoryHash;file_hashes=$hashes;image_id=$image;image_tag=$workerTag;
        base_image_id=$baseId;training_image_id=$trainingImageId;git_commit=(git -C $root rev-parse HEAD);uncommitted_code=$true;
        runtime_budget=@{cpus=2;container_memory_mib=3072;driver_heap_mib=1536;timeout_seconds=1200;spark_local_dirs=$sparkLocalDirs};vm_memory_available_bytes=$available.MemAvailable}
    Assert-HoldoutPath (Join-Path $runtime 'code_identity.json')
    [IO.File]::WriteAllText((Join-Path $runtime 'code_identity.json'),($identity|ConvertTo-Json -Depth 5),[Text.UTF8Encoding]::new($false))
    # Only verify the already-owned existing artifact directory; never provision it.
    $artifactCheck = 'import pathlib,json; p=pathlib.Path("/mlflow/artifacts/ml-p2-20261022"); assert p.is_dir() and p.resolve()==p and not p.is_symlink(); m=p/"ml-p2-owner.json"; assert m.is_file() and json.loads(m.read_text())=={"scope":"ML P2","owner":"run_ml_pilot_v1"}; print("Existing dedicated artifact ownership verified")'
    $null = Invoke-HoldoutDocker @('exec',$services.mlflow.ID,'python','-c',$artifactCheck)

    function Invoke-HoldoutJob {
        param([string]$Name,[string[]]$Command)
        Assert-NoHoldoutWorker
        if ((Invoke-HoldoutDocker @('image','inspect','--format','{{.Id}}',$workerTag)).Trim() -ne $image) { throw 'Holdout image identity drifted.' }
        foreach ($service in @('postgres','mlflow','dashboard')) {
            $old = $services[$service]
            $current = (Invoke-HoldoutDocker @('container','inspect','--format','{{.Id}} {{.State.StartedAt}}',$old.ID)).Trim()
            if ($current -ne ($old.ID+' '+$old.StartedAt)) { throw 'An existing service changed identity/lifecycle.' }
        }
        $jobName = $jobPrefix + $Name
        $startedAt = [DateTime]::UtcNow
        $watch = [Diagnostics.Stopwatch]::StartNew()
        $bounded = @('timeout','--signal=TERM','--kill-after=30s','1200s') + $Command
        $arguments = $compose + @('run','--rm','--no-deps','--pull','never','-T','--name',$jobName,'worker') + $bounded
        $result = Invoke-DockerCommand -Arguments $arguments -WorkingDirectory $root -TimeoutSeconds 1300
        $watch.Stop()
        Assert-HoldoutPath (Join-Path $runtime ($Name+'-console.log'))
        Assert-HoldoutPath (Join-Path $runtime ($Name+'-result.json'))
        [IO.File]::WriteAllText((Join-Path $runtime ($Name+'-console.log')),($result.StdOut+$result.StdErr),[Text.UTF8Encoding]::new($false))
        $record = [ordered]@{job=$Name;container_name=$jobName;image_id=$image;image_tag=$workerTag;command=@($Command);
            started_at_utc=$startedAt.ToString('o');finished_at_utc=([DateTime]::UtcNow).ToString('o');
            wall_seconds=[Math]::Round($watch.Elapsed.TotalSeconds,3);exit_code=$result.ExitCode;inner_timeout_seconds=1200;host_timeout_seconds=1300}
        [IO.File]::WriteAllText((Join-Path $runtime ($Name+'-result.json')),($record|ConvertTo-Json -Depth 5),[Text.UTF8Encoding]::new($false))
        if ($result.ExitCode -ne 0) {
            $format = '{"ID":{{json .Id}},"Image":{{json .Image}},"Running":{{json .State.Running}},"Project":{{json (index .Config.Labels "com.docker.compose.project")}},"Service":{{json (index .Config.Labels "com.docker.compose.service")}},"OneOff":{{json (index .Config.Labels "com.docker.compose.oneoff")}}}'
            $check = Invoke-DockerCommand -Arguments @('container','inspect','--format',$format,$jobName) -WorkingDirectory $root -TimeoutSeconds 30
            if ($check.ExitCode -eq 0) {
                $job = ConvertFrom-StartupJson $check.StdOut 'Failed holdout job ownership'
                if ($job.Image -ne $image -or $job.Project -ne 'nyc-taxi-bigdata' -or $job.Service -ne 'worker' -or $job.OneOff -ne 'True') { throw 'Failed job ownership differs; manual inspection is required.' }
                if ($job.Running) { $null = Invoke-HoldoutDocker @('container','stop','--time','30',$job.ID) -Timeout 45 }
            }
        }
        Write-Host ($Name+' exit='+$result.ExitCode+' seconds='+[Math]::Round($watch.Elapsed.TotalSeconds,2))
        if ($result.ExitCode -ne 0) { throw 'Holdout job failed; inspect its dedicated log/result. No automatic retry or tuning.' }
    }
    if ($holdoutOptions.TestOnly) {
        $tests = @(Get-ChildItem -LiteralPath (Join-Path $root 'tests') -Filter 'test_ml_holdout*.py' -File | Sort-Object Name | ForEach-Object { 'tests/'+$_.Name })
        if (-not $tests.Count) { throw 'Holdout test inventory is empty; do not report a test pass.' }
        $null = Invoke-HoldoutJob 'holdout-tests' (@('python','-B','-m','pytest','-q','-p','no:cacheprovider','--basetemp=/tmp/ml-p3-holdout-tests') + $tests)
        $null = Invoke-HoldoutJob 'pilot-tests' @('python','-B','-m','pytest','-q','-p','no:cacheprovider','--basetemp=/tmp/ml-p3-pilot-tests','tests/test_ml_pilot_data.py','tests/test_ml_pilot_model.py','tests/test_ml_pilot_tracking.py')
        Write-Host 'Only sequential fixture/mock suites ran; no real holdout evaluation or training.'
        exit 0
    }
    if ($holdoutOptions.Execute) {
        $startFile = Join-Path $runtime 'holdout_started.json'
        $stream = [IO.File]::Open($startFile,[IO.FileMode]::CreateNew,[IO.FileAccess]::Write,[IO.FileShare]::None)
        try {
            $bytes = [Text.Encoding]::UTF8.GetBytes((@{scope='ML P3';image_id=$image;started_at_utc=([DateTime]::UtcNow).ToString('o')}|ConvertTo-Json -Compress))
            $stream.Write($bytes,0,$bytes.Length)
        } finally { $stream.Dispose() }
        $command = @('python','-B','scripts/evaluate_holdout.py','--training-dir','/training-input','--gold-root','/gold',
            '--profile','/ml-contract/p1-profile.json','--zones','/ml-contract/zones.csv','--output','/ml-output',
            '--tracking-uri',$trackingUri,'--image-id',$image)
        $null = Invoke-HoldoutJob 'evaluate' $command
        Write-Host 'ML P3 evaluation finished. No fitting, tuning, model promotion, or dashboard/service changes.'
    }
    exit 0
} catch {
    Write-Error ('ML P3 stopped: '+$_.Exception.Message) -ErrorAction Continue
    exit 1
} finally {
    foreach ($name in $savedEnvironment.Keys) { [Environment]::SetEnvironmentVariable($name,$savedEnvironment[$name],'Process') }
}
