#requires -Version 5.1
[CmdletBinding()]
param(
    [switch]$Build,
    [switch]$TestOnly,
    [switch]$Execute,
    [ValidateSet('p2', 'p21')][string]$Attempt = 'p2'
)

# Dot-sourcing startup only imports its native quoting/capture helpers.
$pilotOptions = @{ Build = [bool]$Build; TestOnly = [bool]$TestOnly; Execute = [bool]$Execute; Attempt = $Attempt.ToLowerInvariant() }
. (Join-Path $PSScriptRoot 'start.ps1')
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$root = Split-Path -Parent $PSScriptRoot
$isP21 = $pilotOptions.Attempt -eq 'p21'
$scopeLabel = if ($isP21) { 'ML P2.1' } else { 'ML P2' }
$runtimeRelative = if ($isP21) { 'models/ml-p2/20261023-pilot02' } else { 'models/ml-p2/20261022-pilot01' }
$runtime = Join-Path $root $runtimeRelative
$workerTag = if ($isP21) { 'nyc-taxi-worker:ml-p21' } else { 'nyc-taxi-worker:ml-p2' }
$jobPrefix = if ($isP21) { 'nyc-taxi-ml-p21-' } else { 'nyc-taxi-ml-p2-' }
$sparkLocalDirs = if ($isP21) { '/tmp/ml-p21-spark' } else { '/ml-output/spark-tmp' }
$artifactPath = '/mlflow/artifacts/ml-p2-20261022'
$pilotEnvironment = @{}
foreach ($name in @('MLP2_TRACKING_URI', 'MLP2_WORKER_IMAGE', 'MLP2_RUNTIME_DIR', 'MLP2_SPARK_LOCAL_DIRS')) {
    $pilotEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}
$script:DockerCliPath = Resolve-DockerCli
if (-not $script:DockerCliPath) { throw 'Docker CLI is unavailable.' }
$context = New-ProjectEnvironmentContext -ProjectRoot $root

function Invoke-PilotDocker {
    param([string[]]$Arguments, [int]$Timeout = 60, [string]$InputText)
    $r = Invoke-DockerCommand -Arguments $Arguments -WorkingDirectory $root -InputText $InputText -TimeoutSeconds $Timeout
    if ($r.ExitCode -ne 0) { throw ('ML P2 Docker check failed (exit ' + $r.ExitCode + '); raw output withheld.') }
    return $r.StdOut
}

try {
    $services = @{}
    foreach ($entry in @(@{Name='nyc-taxi-postgres';Service='postgres'}, @{Name='nyc-taxi-mlflow';Service='mlflow'}, @{Name='nyc-taxi-bigdata-dashboard-1';Service='dashboard'})) {
        $template = '{"ID":{{json .Id}},"StartedAt":{{json .State.StartedAt}},"Health":{{json .State.Health.Status}},"Project":{{json (index .Config.Labels "com.docker.compose.project")}},"Service":{{json (index .Config.Labels "com.docker.compose.service")}},"Networks":{{json .NetworkSettings.Networks}}}'
        $value = ConvertFrom-StartupJson (Invoke-PilotDocker @('container','inspect','--format',$template,$entry.Name)) 'ML P2 service identity'
        if ($value.Health -ne 'healthy' -or $value.Project -ne 'nyc-taxi-bigdata' -or $value.Service -ne $entry.Service) { throw 'Existing project services must be healthy with matching ownership; do not recreate them.' }
        $services[$entry.Service] = $value
    }
    $ip = $services.mlflow.Networks.'nyc-taxi-bigdata_default'.IPAddress
    if ($ip -notmatch '^172\.(1[6-9]|2\d|3[01])\.\d+\.\d+$') { throw 'Expected current-project private MLflow IP is unavailable.' }
    $serverMounts = ConvertFrom-StartupJson (Invoke-PilotDocker @('container','inspect','--format','{{json .Mounts}}',$services.mlflow.ID)) 'MLflow mounts'
    $artifactMount = @($serverMounts | Where-Object { $_.Destination -eq '/mlflow' -and $_.Type -eq 'volume' -and $_.Name -eq 'nyc-taxi-bigdata_mlflow-data' })
    if ($artifactMount.Count -ne 1) { throw 'MLflow is not using the reviewed existing artifact volume.' }
    # Scoped to this script process/job; never edits .env or the server config.
    $env:MLP2_TRACKING_URI = 'http://' + $ip + ':5000'
    $env:MLP2_WORKER_IMAGE = $workerTag
    $env:MLP2_RUNTIME_DIR = './' + $runtimeRelative
    $env:MLP2_SPARK_LOCAL_DIRS = $sparkLocalDirs
    $compose = @('compose','--project-name','nyc-taxi-bigdata','--project-directory',$root,'-f',(Join-Path $root 'docker-compose.yml'),'-f',(Join-Path $root 'docker-compose.ml-pilot.yml'),'--profile','jobs')
    $null = Invoke-PilotDocker ($compose + @('config','--quiet'))
    # Parse privately; never print resolved environment (including other services).
    $resolved = ConvertFrom-StartupJson (Invoke-PilotDocker ($compose + @('config','--format','json'))) 'ML Compose model'
    $worker = $resolved.services.worker
    if ($resolved.name -ne 'nyc-taxi-bigdata' -or $resolved.networks.default.name -ne 'nyc-taxi-bigdata_default' -or $worker.image -ne $workerTag -or $worker.cpus -ne 2 -or $worker.mem_limit -ne 3GB -or @($worker.volumes).Count -ne 4) { throw 'Unexpected ML Compose project/image/network/resource/mount configuration.' }
    if ($worker.environment.SPARK_LOCAL_DIRS -ne $sparkLocalDirs -or
        $worker.environment.JAVA_TOOL_OPTIONS -ne '-XX:ActiveProcessorCount=2' -or
        $worker.environment.PYSPARK_SUBMIT_ARGS -ne '--driver-memory 1536m --conf spark.ui.enabled=false pyspark-shell') {
        throw 'ML attempt Spark temporary path or JVM resource settings differ from the reviewed scope.'
    }
    $expectedBinds = @{'/gold'=(Join-Path $root 'data/gold'); '/ml-output'=$runtime; '/ml-contract/p1-profile.json'=(Join-Path $root 'reports/ml_p1_profile.json')}
    foreach ($target in $expectedBinds.Keys) {
        $mount = @($worker.volumes | Where-Object { $_.target -eq $target -and $_.type -eq 'bind' })
        if ($mount.Count -ne 1 -or [IO.Path]::GetFullPath($mount[0].source) -ne [IO.Path]::GetFullPath($expectedBinds[$target])) { throw 'ML bind mount path differs from the dedicated scope.' }
        $isReadOnly = [bool](Get-OptionalProperty $mount[0] 'read_only' $false)
        if ($isReadOnly -ne ($target -ne '/ml-output')) { throw 'ML bind mount access mode differs from the dedicated scope.' }
    }
    $shared = @($worker.volumes | Where-Object { $_.target -eq $artifactPath -and $_.type -eq 'volume' })
    if ($shared.Count -ne 1 -or $shared[0].source -ne 'ml-p2-artifacts' -or $shared[0].volume.subpath -ne 'artifacts/ml-p2-20261022' -or $resolved.volumes.'ml-p2-artifacts'.name -ne 'nyc-taxi-bigdata_mlflow-data') { throw 'Artifact volume/subpath differs from the reviewed existing volume.' }
    $info = ConvertFrom-StartupJson (Invoke-PilotDocker @('info','--format','{"CPUs":{{.NCPU}},"MemoryBytes":{{.MemTotal}}}')) 'Docker resources'
    if ($info.CPUs -lt 2 -or $info.MemoryBytes -lt 7GB) { throw 'Pilot needs 2 CPUs and a reviewed Docker budget of about 7 GiB including live services.' }
    $drive = New-Object IO.DriveInfo([IO.Path]::GetPathRoot($root))
    if ($drive.AvailableFreeSpace -lt 20GB) { throw 'Need at least 20 GiB host headroom for ML-only shuffle/temp/artifacts.' }
    Write-Host ($scopeLabel + ' PLAN: source Gold read-only; fixed 1% seed 42; train Jan-Apr, validate May; June UNEVALUATED. Docker CPU=' + $info.CPUs + ', GiB=' + [Math]::Round($info.MemoryBytes/1GB,2))
    Write-Host ('Selected attempt=' + $pilotOptions.Attempt + '; image=' + $workerTag + '; runtime=' + $runtimeRelative + '; Spark local dirs=' + $sparkLocalDirs)
    if (-not ($pilotOptions.Build -or $pilotOptions.TestOnly -or $pilotOptions.Execute)) { exit 0 }
    # All attempts share one resource budget: refuse any ML-named container or
    # current-project worker, not only the selected attempt's job names.
    $occupied = Invoke-PilotDocker @('container','ls','-a','--filter','name=^/nyc-taxi-ml-','--format','{{.ID}}')
    $otherWorker = Invoke-PilotDocker @('container','ls','-a','--filter','label=com.docker.compose.project=nyc-taxi-bigdata','--filter','label=com.docker.compose.service=worker','--format','{{.ID}}')
    if ($occupied.Trim() -or $otherWorker.Trim()) { throw 'A previous ML or current-project worker container exists; inspect it before retrying. Parallel pilot workers are not allowed.' }
    $baseId = (Invoke-PilotDocker @('image','inspect','--format','{{.Id}}','nyc-taxi-worker:p6-clean')).Trim()
    if ($pilotOptions.Build) {
        $null = Invoke-PilotDocker ($compose + @('build','worker')) -Timeout 600
        if ((Invoke-PilotDocker @('image','inspect','--format','{{.Id}}','nyc-taxi-worker:p6-clean')).Trim() -ne $baseId) { throw 'Clean base tag changed unexpectedly.' }
        Write-Host 'ML code-refresh image built; dependencies unchanged (not a new clean dependency build).'
    }
    $image = (Invoke-PilotDocker @('image','inspect','--format','{{.Id}}',$workerTag)).Trim()
    $files = @('requirements.txt','requirements-worker-constraints.txt','pyproject.toml','config/config.yaml','Dockerfile.ml','docker-compose.ml-pilot.yml','scripts/train_pilot.py','scripts/run_ml_pilot.ps1','scripts/profile_ml_data.py')
    foreach ($scope in @(@{Directory='src';Pattern='*.py'},@{Directory='sql';Pattern='*.sql'},@{Directory='tests';Pattern='test_ml_pilot*.py'})) {
        $files += Get-ChildItem -LiteralPath (Join-Path $root $scope.Directory) -File -Recurse -Filter $scope.Pattern | ForEach-Object { $_.FullName.Substring($root.Length+1).Replace('\','/') }
    }
    $hashes = [ordered]@{}
    foreach ($file in @($files | Sort-Object -Unique)) { $hashes[$file] = (Get-FileHash -LiteralPath (Join-Path $root $file) -Algorithm SHA256).Hash.ToLowerInvariant() }
    $payload = $hashes | ConvertTo-Json -Compress
    $probe = 'import hashlib,json,pathlib,sys; root=pathlib.Path("/app"); expected=json.load(sys.stdin); bad=[name for name,d in expected.items() if not(root/name).is_file() or hashlib.sha256((root/name).read_bytes()).hexdigest()!=d]; print(json.dumps({"verified":not bad,"files":len(expected),"mismatches":bad})); sys.exit(bool(bad))'
    $match = ConvertFrom-StartupJson (Invoke-PilotDocker @('run','--rm','-i','--pull','never','--network','none','--read-only','--memory','256m',$image,'python','-I','-B','-c',$probe) -InputText $payload) 'ML image hashes'
    if (-not $match.verified) { throw 'ML image is stale; build separately, do not mount source.' }
    if (-not ($pilotOptions.TestOnly -or $pilotOptions.Execute)) { Write-Host ('Verified ML image: '+$match.files+' files.'); exit 0 }

    $resourceCode = 'import json; m={k:int(v.split()[0])*1024 for k,v in (line.split(":",1) for line in open("/proc/meminfo")) if k in ("MemTotal","MemAvailable")}; print(json.dumps(m)); assert m["MemAvailable"]>=int(3.5*1024**3), "Insufficient VM available RAM for pilot plus headroom"'
    $available = ConvertFrom-StartupJson (Invoke-PilotDocker @('run','--rm','--pull','never','--network','none','--read-only','--memory','256m',$image,'python','-I','-B','-c',$resourceCode)) 'VM memory headroom'
    Write-Host ('VM MemAvailable GiB=' + [Math]::Round($available.MemAvailable/1GB,2))
    $occupied = Invoke-PilotDocker @('container','ls','-a','--filter','name=^/nyc-taxi-ml-','--format','{{.ID}}')
    $otherWorker = Invoke-PilotDocker @('container','ls','-a','--filter','label=com.docker.compose.project=nyc-taxi-bigdata','--filter','label=com.docker.compose.service=worker','--format','{{.ID}}')
    if ($occupied.Trim() -or $otherWorker.Trim()) { throw 'A previous ML or current-project worker container exists; inspect it before retrying. Parallel pilot workers are not allowed.' }
    # Refuse a previous attempt BEFORE modifying its provenance/artifacts.
    if (Test-Path -LiteralPath (Join-Path $runtime 'training_started.json')) { throw 'A training attempt already exists; inspect it, do not overwrite provenance or retrain automatically.' }
    $checkedPath = $runtime
    while ($checkedPath -and $checkedPath.Length -ge $root.Length) {
        if ((Test-Path -LiteralPath $checkedPath) -and ((Get-Item -Force -LiteralPath $checkedPath).Attributes -band [IO.FileAttributes]::ReparsePoint)) { throw 'Pilot runtime must not traverse a reparse point.' }
        if ($checkedPath -eq $root) { break }
        $checkedPath = Split-Path -Parent $checkedPath
    }
    if ((Test-Path -LiteralPath $runtime) -and -not (Test-Path -LiteralPath (Join-Path $runtime 'ml-p2-owner.json'))) { throw 'Runtime directory has no pilot ownership marker.' }
    $null = New-Item -ItemType Directory -Path $runtime -Force
    $marker = Join-Path $runtime 'ml-p2-owner.json'
    if (-not (Test-Path -LiteralPath $marker)) { [IO.File]::WriteAllText($marker,'{"scope":"ML P2","owner":"run_ml_pilot_v1"}') }
    $owner = Get-Content -Raw -Encoding UTF8 -LiteralPath $marker | ConvertFrom-Json
    if ($owner.scope -ne 'ML P2' -or $owner.owner -ne 'run_ml_pilot_v1') { throw 'Runtime ownership marker does not match this pilot.' }
    $runtimeDirectories = @('tmp')
    if (-not $isP21) { $runtimeDirectories += 'spark-tmp' }
    foreach ($dir in $runtimeDirectories) { $null = New-Item -ItemType Directory -Path (Join-Path $runtime $dir) -Force }
    $sha = [Security.Cryptography.SHA256]::Create()
    try { $inventoryHash = ([BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($payload)))).Replace('-','').ToLowerInvariant() } finally { $sha.Dispose() }
    $identity = @{ verified=$true; inventory_sha256=$inventoryHash; file_hashes=$hashes; image_id=$image; image_tag=$workerTag; attempt=$pilotOptions.Attempt; base_image_id=$baseId; git_commit=(git -C $root rev-parse HEAD); uncommitted_code=$true; runtime_budget=@{cpus=2;container_memory_mib=3072;driver_heap_mib=1536;timeout_seconds=1800;spark_local_dirs=$sparkLocalDirs}; vm_memory_available_bytes=$available.MemAvailable }
    if ($isP21) { $identity.preparation_mode = 'sample-first' }
    [IO.File]::WriteAllText((Join-Path $runtime 'code_identity.json'),($identity|ConvertTo-Json -Depth 5),[Text.UTF8Encoding]::new($false))
    $create = 'import pathlib,json; p=pathlib.Path("/mlflow/artifacts/ml-p2-20261022"); parent=p.parent; assert parent.is_dir() and parent.resolve()==parent; marker=p/"ml-p2-owner.json"; expected={"scope":"ML P2","owner":"run_ml_pilot_v1"}; assert not p.is_symlink(); assert not p.exists() or (marker.is_file() and json.loads(marker.read_text())==expected), "Unowned artifact directory"; p.mkdir(exist_ok=True); marker.write_text(json.dumps(expected)) if not marker.exists() else None; print("Dedicated ML P2 artifact directory ready; existing volume unchanged.")'
    $null = Invoke-PilotDocker @('exec','nyc-taxi-mlflow','python','-c',$create)

    function Invoke-PilotJob {
        param([string]$Name,[string[]]$Command,[int]$Timeout=300)
        if ((Invoke-PilotDocker @('image','inspect','--format','{{.Id}}',$workerTag)).Trim() -ne $image) { throw 'ML image identity drifted.' }
        foreach ($service in @('postgres','mlflow','dashboard')) {
            $old = $services[$service]
            $now = (Invoke-PilotDocker @('container','inspect','--format','{{.Id}} {{.State.StartedAt}}',$old.ID)).Trim()
            if ($now -ne ($old.ID+' '+$old.StartedAt)) { throw 'An existing service changed identity/lifecycle; stop.' }
        }
        $jobName = $jobPrefix + $Name
        $startedAt = [DateTime]::UtcNow
        $watch = [Diagnostics.Stopwatch]::StartNew()
        $bounded = @('timeout','--signal=TERM','--kill-after=30s',([string]($Timeout-60)+'s')) + $Command
        $arguments = $compose + @('run','--rm','--no-deps','--pull','never','-T','--name',$jobName,'worker') + $bounded
        $r = Invoke-DockerCommand -Arguments $arguments -WorkingDirectory $root -TimeoutSeconds $Timeout
        $watch.Stop()
        # Persist the actual command outcome BEFORE cleanup can fail, and only
        # beneath this selected attempt's dedicated runtime directory.
        [IO.File]::WriteAllText((Join-Path $runtime ($Name+'-console.log')),($r.StdOut+$r.StdErr),[Text.UTF8Encoding]::new($false))
        $jobResult = [ordered]@{
            attempt=$pilotOptions.Attempt; job=$Name; container_name=$jobName
            image_tag=$workerTag; image_id=$image; command=@($Command)
            started_at_utc=$startedAt.ToString('o'); finished_at_utc=([DateTime]::UtcNow).ToString('o')
            wall_seconds=[Math]::Round($watch.Elapsed.TotalSeconds,3); exit_code=$r.ExitCode
            host_timeout_seconds=$Timeout
        }
        [IO.File]::WriteAllText((Join-Path $runtime ($Name+'-result.json')),($jobResult|ConvertTo-Json -Depth 5),[Text.UTF8Encoding]::new($false))
        if ($r.ExitCode -ne 0) {
            # A timed-out CLI may leave its job alive. Stop ONLY this newly named,
            # Compose-owned one-off worker; never stop a pre-existing service.
            $check = Invoke-DockerCommand -Arguments @('container','inspect','--format','{"ID":{{json .Id}},"Image":{{json .Image}},"Running":{{json .State.Running}},"Project":{{json (index .Config.Labels "com.docker.compose.project")}},"Service":{{json (index .Config.Labels "com.docker.compose.service")}},"OneOff":{{json (index .Config.Labels "com.docker.compose.oneoff")}}}',$jobName) -WorkingDirectory $root -TimeoutSeconds 30
            if ($check.ExitCode -eq 0) {
                $job = ConvertFrom-StartupJson $check.StdOut 'Timed-out pilot ownership'
                if ($job.Image -ne $image -or $job.Project -ne 'nyc-taxi-bigdata' -or $job.Service -ne 'worker' -or $job.OneOff -ne 'True') { throw 'Failed job container ownership mismatch; manual inspection required.' }
                if ($job.Running) { $null = Invoke-PilotDocker @('container','stop','--time','30',$job.ID) -Timeout 45 }
            }
        }
        Write-Host ($Name+' exit='+$r.ExitCode+' seconds='+[Math]::Round($watch.Elapsed.TotalSeconds,2))
        if ($r.ExitCode -ne 0) { throw ('ML P2 '+$Name+' failed; inspect dedicated runtime log/report. No automatic retry/tuning.') }
        return $r.StdOut
    }
    if ($pilotOptions.TestOnly) {
        $null = Invoke-PilotJob 'tests' @('python','-B','-m','pytest','-q','-p','no:cacheprovider','--basetemp=/tmp/ml-p2-tests','tests/test_ml_pilot_data.py','tests/test_ml_pilot_model.py','tests/test_ml_pilot_tracking.py','tests/test_ml_pilot_diagnostics.py') -Timeout 600
        Write-Host 'Only ML fixture/mock tests ran; no real-data training.'
        exit 0
    }
    if ($pilotOptions.Execute) {
        if (Test-Path -LiteralPath (Join-Path $runtime 'training_started.json')) { throw 'A training attempt already exists; inspect it, do not retry/tune automatically.' }
        $null = Invoke-PilotJob 'probe' @('python','-B','scripts/train_pilot.py','probe')
        $probeReport = Get-Content -Raw -Encoding UTF8 -LiteralPath (Join-Path $runtime 'artifact_probe.json') | ConvertFrom-Json
        $verify = 'import pathlib,hashlib,json,sys; root=pathlib.Path("/mlflow/artifacts/ml-p2-20261022"); p=root/sys.argv[1]; assert p.resolve().is_relative_to(root) and p.is_file(); assert hashlib.sha256(p.read_bytes()).hexdigest()==sys.argv[2]; print("Server persistent-volume artifact SHA256 verified")'
        $null = Invoke-PilotDocker @('exec','nyc-taxi-mlflow','python','-c',$verify,$probeReport.file_relative_path,$probeReport.sha256)
        $trainCommand = @('timeout','--signal=TERM','--kill-after=30s','1800s','python','-B','scripts/train_pilot.py','train')
        if ($isP21) { $trainCommand += @('--preparation-mode','sample-first') }
        $null = Invoke-PilotJob 'pilot' $trainCommand -Timeout 1900
        $null = Invoke-PilotJob 'reload' @('python','-B','scripts/train_pilot.py','verify-reload') -Timeout 300
        Write-Host ($scopeLabel + ' complete. June was not evaluated. No model promoted or dashboard changed.')
    }
    exit 0
} catch {
    Write-Error ($scopeLabel + ' stopped: '+$_.Exception.Message) -ErrorAction Continue
    exit 1
} finally {
    foreach ($name in $pilotEnvironment.Keys) {
        [Environment]::SetEnvironmentVariable($name, $pilotEnvironment[$name], 'Process')
    }
}
