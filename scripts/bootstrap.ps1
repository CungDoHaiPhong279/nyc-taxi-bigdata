#requires -Version 5.1
<#
Explicit Jan-Jun 2025 bootstrap. Default is a read-only PLAN, not execution.
No host Python/Java/Spark. Never builds, pulls, starts or recreates services.
#>
[CmdletBinding()]
param(
    [ValidateSet('current', 'bootstrap-test', 'package-test')][string]$Environment = 'current',
    [ValidateSet('refresh', 'clean')][string]$WorkerVariant = 'refresh',
    [switch]$Execute,
    [ValidateRange(1, 300)][int]$CommandTimeoutSeconds = 30,
    [ValidateRange(60, 86400)][int]$StepTimeoutSeconds = 21600
)

# Dot-sourcing start.ps1 loads its param defaults too; preserve ours first.
$script:BootstrapInvocation = @{
    Environment = $Environment; WorkerVariant = $WorkerVariant
    Execute = [bool]$Execute; CommandTimeoutSeconds = $CommandTimeoutSeconds
    StepTimeoutSeconds = $StepTimeoutSeconds
}
. (Join-Path $PSScriptRoot 'start.ps1')
$script:BootstrapProjectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))

function Get-BootstrapPlan {
    $steps = @(
        @{ Name = 'calendar'; Arguments = @('python', '-B', 'scripts/bootstrap_calendar.py') }
        @{ Name = 'raw'; Arguments = @('python', '-B', 'scripts/download_raw_data.py', '--taxi-start-month', '2025-01', '--taxi-end-month', '2025-06') }
        @{ Name = 'bronze'; Arguments = @('python', '-B', 'scripts/build_bronze.py', '--all') }
        @{ Name = 'silver'; Arguments = @('python', '-B', 'scripts/build_silver.py', '--all') }
        @{ Name = 'gold'; Arguments = @('python', '-B', 'scripts/build_gold.py', '--all') }
        @{ Name = 'dimensions'; Arguments = @('python', '-B', 'scripts/load_postgres.py', '--dimensions') }
    )
    foreach ($month in 1..6) {
        $label = '2025-{0:00}' -f $month
        $steps += @{ Name = ('fact-' + $label); Arguments = @('python', '-B', 'scripts/load_postgres.py', '--month', $label) }
    }
    $steps += @{ Name = 'indexes-view'; Arguments = @('python', '-B', 'scripts/load_postgres.py', '--create-indexes') }
    $steps += @{ Name = 'validation'; Arguments = @('python', '-B', 'scripts/load_postgres.py', '--validate') }
    foreach ($step in $steps) { [pscustomobject]$step }
}

function Test-BootstrapWorkerModel {
    param($Context, $Model)
    $worker = Get-OptionalProperty (Get-OptionalProperty $Model 'services') 'worker'
    if ($null -eq $worker) { Stop-Startup 'Compose worker is missing.' }
    if (@(Get-OptionalProperty $worker 'profiles' @()) -notcontains 'jobs') {
        Stop-Startup 'Worker must remain in the jobs profile.'
    }
    $environment = Get-OptionalProperty $worker 'environment'
    if ((Get-OptionalProperty $environment 'POSTGRES_HOST') -ne 'postgres' -or
        (Get-OptionalProperty $environment 'POSTGRES_PORT') -ne '5432' -or
        (Get-OptionalProperty $environment 'MLFLOW_TRACKING_URI') -ne 'http://mlflow:5000') {
        Stop-Startup 'Worker internal service endpoints differ from the supported Compose configuration; review separately.'
    }
    $entrypoint = Get-OptionalProperty $worker 'entrypoint'
    $envFiles = Get-OptionalProperty $worker 'env_file'
    if (($null -ne $entrypoint -and @($entrypoint).Count -gt 0) -or
        (Get-OptionalProperty $worker 'working_dir' '/app') -ne '/app' -or
        (Get-OptionalProperty $worker 'network_mode') -or
        ($null -ne $envFiles -and @($envFiles).Count -gt 0)) {
        Stop-Startup 'Unexpected worker entrypoint/workdir/network/env-file override; review separately.'
    }
    $mounts = @(Get-OptionalProperty $worker 'volumes' @())
    if ($mounts.Count -ne 4) { Stop-Startup 'Worker must have exactly the four runtime binds; source-code mounts are not allowed.' }
    foreach ($directory in @('data', 'logs', 'models', 'reports')) {
        $matching = @($mounts | Where-Object { (Get-OptionalProperty $_ 'target') -eq ('/app/' + $directory) })
        if ($matching.Count -ne 1) { Stop-Startup 'Worker runtime bind targets do not match the supported layout.' }
        $mount = $matching[0]
        $source = Get-OptionalProperty $mount 'source' ''
        $runtimeRoot = Get-OptionalProperty $Context 'RuntimeRoot' $Context.Root
        $expected = [IO.Path]::GetFullPath((Join-Path $runtimeRoot $directory)).TrimEnd('\', '/')
        if ((Get-OptionalProperty $mount 'type') -ne 'bind' -or -not $source -or
            [IO.Path]::GetFullPath($source).TrimEnd('\', '/') -ne $expected -or
            (Get-OptionalProperty $mount 'read_only' $false)) {
            Stop-Startup 'Worker runtime bind sources must be the selected environment data/logs/models/reports directories.'
        }
    }
    $image = Get-OptionalProperty $worker 'image' ''
    # Do not interpolate arbitrary image expressions or print possibly sensitive values.
    if (-not $image -or $image -match '[\s$@]' -or $image -match '://') {
        Stop-Startup 'Worker needs an explicit local image reference; review its Compose image separately.'
    }
    return $image
}

function Assert-BootstrapServiceHealth {
    param($Context, $Model)
    foreach ($service in @('postgres', 'mlflow')) {
        $serviceModel = Get-OptionalProperty (Get-OptionalProperty $Model 'services') $service
        if ($null -eq $serviceModel) { Stop-Startup ('Compose service is missing: ' + $service + '.') }
        $container = Get-ServiceContainer $Context $service (Get-OptionalProperty $serviceModel 'container_name' '')
        if ($null -eq $container -or $container.Status -ne 'running' -or $container.Health -ne 'healthy') {
            Stop-Startup ($service + ' must already be running and healthy. Run scripts/start.ps1 for the selected environment separately; bootstrap never starts/recreates services.')
        }
    }
}

function Get-BootstrapWorkerImage {
    param($Context, [string]$Image)
    # Some Docker versions omit an unset Entrypoint key instead of emitting null.
    $template = '{"ID":{{json .Id}},"WorkingDir":{{json .Config.WorkingDir}},"Entrypoint":{{json (index .Config "Entrypoint")}}}'
    $json = Invoke-StartupDocker $Context @('image', 'inspect', '--format', $template, $Image) 'Worker image is unavailable. Build the selected environment worker separately; bootstrap never builds or pulls'
    $metadata = ConvertFrom-StartupJson $json 'worker image metadata'
    $entrypoint = Get-OptionalProperty $metadata 'Entrypoint'
    if (-not (Get-OptionalProperty $metadata 'ID') -or $metadata.WorkingDir -ne '/app' -or
        ($null -ne $entrypoint -and @($entrypoint).Count -gt 0)) {
        Stop-Startup 'Worker image entrypoint/workdir is incompatible; build/review it separately.'
    }
    return $metadata
}

function Get-BootstrapFileHashes {
    param([string]$ProjectRoot)
    # Public execution inputs only. Never hash .env, data, logs, models or reports.
    $files = @('requirements.txt', 'pyproject.toml', 'config/config.yaml')
    $files += @('bootstrap_calendar.py', 'create_calendar.py', 'download_raw_data.py',
        'build_bronze.py', 'build_silver.py', 'build_gold.py', 'load_postgres.py',
        'check_worker_services.py') | ForEach-Object { 'scripts/' + $_ }
    foreach ($scope in @(@{ Directory = 'src'; Pattern = '*.py' }, @{ Directory = 'sql'; Pattern = '*.sql' })) {
        $files += Get-ChildItem -LiteralPath (Join-Path $ProjectRoot $scope.Directory) -Recurse -File -Filter $scope.Pattern |
            ForEach-Object { $_.FullName.Substring($ProjectRoot.TrimEnd('\', '/').Length + 1).Replace('\', '/') }
    }
    $hashes = @{}
    foreach ($relative in @($files | Sort-Object -Unique)) {
        $hashes[$relative] = (Get-FileHash -LiteralPath (Join-Path $ProjectRoot $relative) -Algorithm SHA256).Hash.ToLowerInvariant()
    }
    return $hashes
}

function Test-BootstrapWorkerFreshness {
    param($Context, [int]$StepTimeoutSeconds)
    $expected = Get-BootstrapFileHashes -ProjectRoot $Context.Root
    # Standard-build input for clean/package workers; preserve the unchanged
    # P6.1 refresh image's dependency-input contract.
    if ((Get-OptionalProperty $Context 'WorkerVariant' 'refresh') -eq 'clean' -or
        (Get-OptionalProperty $Context 'IsPackage' $false)) {
        $constraintFile = 'requirements-worker-constraints.txt'
        $expected[$constraintFile] = (Get-FileHash -LiteralPath (Join-Path $Context.Root $constraintFile) -Algorithm SHA256).Hash.ToLowerInvariant()
    }
    # Isolated Python stdlib only. Reads packaged public files, not runtime data.
    $probe = @'
import hashlib, json, pathlib, sys
root = pathlib.Path('/app')
expected = json.load(sys.stdin)
mismatches = []
for name, digest in sorted(expected.items()):
    relative = pathlib.PurePosixPath(name)
    if relative.is_absolute() or '..' in relative.parts:
        raise ValueError('Invalid public inventory path')
    path = root / name
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != digest:
        mismatches.append(name)
for scope, pattern in [('src', '*.py'), ('sql', '*.sql')]:
    for path in (root / scope).rglob(pattern):
        name = path.relative_to(root).as_posix()
        if name not in expected:
            mismatches.append(name)
print(json.dumps({'ok': not mismatches, 'checked': len(expected), 'mismatches': mismatches}))
'@
    $arguments = $Context.ComposeArguments + @('--profile', 'jobs', 'run', '--rm', '--no-deps', '-T', '--pull', 'never', 'worker', 'python', '-I', '-B', '-c', $probe)
    $result = Invoke-DockerCommand -Arguments $arguments -WorkingDirectory $Context.Root -InputText ($expected | ConvertTo-Json -Compress) -TimeoutSeconds $StepTimeoutSeconds
    if ($result.ExitCode -ne 0) { Stop-Startup 'Worker freshness probe failed; no pipeline stage started. Review/build worker separately. Raw output withheld.' }
    $comparison = ConvertFrom-StartupJson $result.StdOut 'worker freshness result'
    if ((Get-OptionalProperty $comparison 'ok') -ne $true -or
        (Get-OptionalProperty $comparison 'checked' 0) -ne $expected.Count -or
        @(Get-OptionalProperty $comparison 'mismatches' @()).Count -ne 0) {
        Write-Warning 'Worker public code/config/SQL/dependency inputs do not match this checkout. No pipeline stage started. Build the selected environment worker separately.'
        return $false
    }
    Write-Host ('Worker freshness verified: ' + $expected.Count + ' packaged public files match; no source mount.')
    return $true
}

function ConvertTo-BootstrapDiagnosticText {
    param([AllowEmptyString()][string]$Text, [string[]]$Secrets = @(),
        [ValidateRange(512, 1048576)][int]$MaxBytes = 1048576)
    # Redact before truncation, including secrets crossing the retained-tail boundary.
    foreach ($secret in @($Secrets | Where-Object { $_ } | Sort-Object Length -Descending)) {
        $Text = $Text.Replace($secret, '[REDACTED]')
        if ($secret.Length -lt 32000) {
            $encoded = [Uri]::EscapeDataString($secret)
            $Text = $Text.Replace($encoded, '[REDACTED]')
        }
    }
    $Text = [regex]::Replace($Text, '(?i)\b([a-z][a-z0-9+.-]{0,31}://)[^\s/?#@]+@', '$1[REDACTED]@')
    $Text = [regex]::Replace($Text, '(?i)\bauthorization\s*:\s*(?:bearer|basic)\s+[^\s,;]+', 'authorization: [REDACTED]')
    $assignment = '(?i)\b(password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|credential|signature)\b["'']?\s*[:=]\s*(?:"(?:\\.|[^"\\])*"|''(?:\\.|[^''\\])*''|[^\s,;&]+)'
    $Text = [regex]::Replace($Text, $assignment, '$1=[REDACTED]')
    $encoding = [Text.UTF8Encoding]::new($false)
    $bytes = $encoding.GetBytes($Text)
    $truncated = $bytes.Length -gt $MaxBytes
    if ($truncated) {
        $prefix = "[earlier output truncated; retained sanitized tail]`n"
        $start = $bytes.Length - ($MaxBytes - $encoding.GetByteCount($prefix))
        while ($start -lt $bytes.Length -and ($bytes[$start] -band 0xC0) -eq 0x80) { $start++ }
        $Text = $prefix + $encoding.GetString($bytes, $start, $bytes.Length - $start)
    }
    return [pscustomobject]@{ Text = $Text; Truncated = $truncated }
}

function Save-BootstrapStepDiagnostics {
    param($Context, $Model, [string]$StepName, $Result, [double]$ElapsedSeconds)
    if (-not (Get-OptionalProperty $Context 'IsPackage' $false)) { return $null }
    if ($StepName -cnotmatch '^[a-z0-9][a-z0-9-]{0,63}$') { Stop-Startup 'Unsafe bootstrap diagnostic stage name.' }
    $secrets = @()
    $services = if ($Model -is [Collections.IDictionary]) { $Model['services'] } else { Get-OptionalProperty $Model 'services' }
    $settings = if ($services -is [Collections.IDictionary]) { @($services.Values) } else { @($services.PSObject.Properties | ForEach-Object { $_.Value }) }
    foreach ($service in $settings) {
        $environment = if ($service -is [Collections.IDictionary]) { $service['environment'] } else { Get-OptionalProperty $service 'environment' }
        if ($null -eq $environment) { continue }
        $variables = if ($environment -is [Collections.IDictionary]) {
            @($environment.GetEnumerator() | ForEach-Object { [pscustomobject]@{ Name = $_.Key; Value = $_.Value } })
        } else { @($environment.PSObject.Properties) }
        foreach ($variable in $variables) {
            if ($variable.Name -match '(?i)password|passwd|pwd|secret|token|api[_-]?key|access[_-]?key|private[_-]?key|credential|authorization|connection[_-]?string|database[_-]?url' -and $variable.Value) {
                $secrets += [string]$variable.Value
            }
        }
    }
    $stdout = ConvertTo-BootstrapDiagnosticText -Text ([string]$Result.StdOut) -Secrets $secrets
    $stderr = ConvertTo-BootstrapDiagnosticText -Text ([string]$Result.StdErr) -Secrets $secrets
    $directory = Join-Path $Context.RuntimeRoot 'logs/bootstrap'
    Assert-NoEnvironmentReparsePoint $directory
    $null = New-Item -ItemType Directory -Path $directory -Force
    Assert-NoEnvironmentReparsePoint $directory
    $path = Join-Path $directory (([DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfffffffZ')) + '-' + $StepName + '-' + [guid]::NewGuid().ToString('N') + '.json')
    Assert-NoEnvironmentReparsePoint $path
    $entry = [ordered]@{ schema_version = 1; stage = $StepName; exit_code = [int]$Result.ExitCode
        generated_at_utc = [DateTime]::UtcNow.ToString('o'); elapsed_seconds = $ElapsedSeconds
        stdout = $stdout.Text; stderr = $stderr.Text
        stdout_truncated = $stdout.Truncated; stderr_truncated = $stderr.Truncated }
    $stream = $null
    try {
        # CreateNew never overwrites a previous attempt, even on a name collision.
        $stream = [IO.File]::Open($path, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
        $bytes = [Text.UTF8Encoding]::new($false).GetBytes(($entry | ConvertTo-Json -Depth 4))
        $stream.Write($bytes, 0, $bytes.Length)
    } catch { Stop-Startup 'Cannot persist sanitized bootstrap diagnostics; outputs/checkpoint preserved. Review runtime log permissions before resuming.' }
    finally { if ($null -ne $stream) { $stream.Dispose() } }
    return $path
}

function Invoke-DataBootstrap {
    param(
        [string]$ProjectRoot = $script:BootstrapProjectRoot,
        [ValidateSet('current', 'bootstrap-test', 'package-test')][string]$Environment = 'current',
        [ValidateSet('refresh', 'clean')][string]$WorkerVariant = 'refresh',
        [switch]$Execute,
        [ValidateRange(1, 300)][int]$CommandTimeoutSeconds = 30,
        [ValidateRange(60, 86400)][int]$StepTimeoutSeconds = 21600,
        $Context = $null
    )
    try {
        $root = [IO.Path]::GetFullPath($ProjectRoot)
        if ($null -eq $Context) {
            $context = New-ProjectEnvironmentContext -ProjectRoot $root -Environment $Environment -WorkerVariant $WorkerVariant -CommandTimeoutSeconds $CommandTimeoutSeconds
        } else {
            # Internal orchestration seam: callers may select the reviewed package
            # layer, but may not silently switch project roots or environments.
            if ((Get-NormalizedEnvironmentPath (Get-OptionalProperty $Context 'Root' '')) -ne
                    (Get-NormalizedEnvironmentPath $root) -or
                (Get-OptionalProperty $Context 'Environment' '') -ne $Environment) {
                Stop-Startup 'Supplied bootstrap context differs from the requested root/environment.'
            }
            $context = $Context
        }
        $required = @('docker-compose.yml', 'requirements.txt', 'pyproject.toml', 'config/config.yaml',
            'scripts/create_calendar.py', 'scripts/start.ps1')
        $plan = @(Get-BootstrapPlan)
        $required += $plan | ForEach-Object { $_.Arguments[2] } | Select-Object -Unique
        foreach ($relative in $required) {
            if (-not (Test-Path -LiteralPath (Join-Path $root $relative) -PathType Leaf)) {
                Stop-Startup ('Required bootstrap input is missing: ' + $relative + '. Prepare configuration/build separately; nothing was created.')
            }
        }
        if (-not (Test-Path -LiteralPath $context.EnvPath -PathType Leaf)) {
            Stop-Startup 'The selected environment configuration file is missing. Prepare it separately; bootstrap never creates or overwrites configuration.'
        }
        $script:DockerCliPath = Resolve-DockerCli
        if (-not $script:DockerCliPath) { Stop-Startup 'Docker CLI is missing. Install/start Docker Desktop separately.' }
        $null = Invoke-StartupDocker $context @('info', '--format', '{{.ServerVersion}}') 'Docker daemon is unavailable. Start Docker Desktop and retry'
        $null = Invoke-StartupDocker $context @('compose', 'version', '--short') 'Docker Compose plugin is unavailable'
        $null = Invoke-StartupCompose $context @('config', '--quiet') 'Compose validation failed; resolved configuration/secrets withheld'
        $modelArguments = @('config', '--no-interpolate', '--format', 'json')
        # The test override uses interpolated, isolated runtime paths. Capture its
        # resolved model privately; never print its environment/secret values.
        if ($Environment -ne 'current' -or (Get-OptionalProperty $context 'IsPackage' $false)) {
            $modelArguments = @('--profile', '*', 'config', '--format', 'json')
        }
        $json = Invoke-StartupCompose $context $modelArguments 'Cannot read selected Compose metadata'
        $model = ConvertFrom-StartupJson $json 'public Compose metadata'
        $context.Project = Get-OptionalProperty $model 'name' ''
        if ($context.Project -notmatch '^[a-z0-9][a-z0-9_-]*$') { Stop-Startup 'Cannot determine Compose project ownership safely.' }
        Assert-ProjectEnvironmentIsolation -Context $context -Model $model
        $image = Test-BootstrapWorkerModel $context $model
        Assert-BootstrapServiceHealth $context $model
        $imageBefore = Get-BootstrapWorkerImage $context $image

        $mode = 'PLAN ONLY'
        if ($Execute) { $mode = 'EXECUTE REQUESTED' }
        Write-Host ('DOCKER P6 - ' + $mode + ' - environment ' + $Environment + ' - worker variant ' + $WorkerVariant + ' - project ' + $context.Project + ' - source months 2025-01 through 2025-06 only.')
        Write-Host 'Calendar: full year 2025; Raw: six Taxi months + zones + Jan-Jun weather.'
        Write-Host 'Gold requires all 9 valid Silver entries; every loader command requires all 6 valid Gold entries/partitions.'
        Write-Host 'Reruns use existing validation/manifest/lineage skips, not force. Dimensions UPSERT; mismatched logged fact partitions may be repaired by the existing loader.'
        $number = 0
        foreach ($step in $plan) {
            $number++
            Write-Host ('[{0:00}/{1}] {2}: {3}' -f $number, $plan.Count, $step.Name, ($step.Arguments -join ' '))
        }
        if (-not $Execute) {
            Write-Host 'PLAN complete. No worker/Python/data operation ran; image contents/freshness and end-to-end bootstrap are NOT verified by this plan.'
            Write-Host 'Execute performs a public-file hash probe first. If stale/missing code: build worker separately, then explicitly use -Execute.'
            return 0
        }
        Write-Host 'Checking packaged worker freshness before any pipeline stage (no automatic build/pull).'
        if (-not (Test-BootstrapWorkerFreshness $context -StepTimeoutSeconds $CommandTimeoutSeconds)) { return 2 }
        $number = 0
        foreach ($step in $plan) {
            # Do not silently run a different image if its tag changes mid-bootstrap.
            Assert-BootstrapServiceHealth $context $model
            $currentImage = Get-BootstrapWorkerImage $context $image
            if ($currentImage.ID -ne $imageBefore.ID) {
                Write-Warning 'Worker image tag changed during bootstrap. Stopping before the next stage; review/build separately.'
                return 2
            }
            $number++
            $timer = [Diagnostics.Stopwatch]::StartNew()
            Write-Host ('START [{0:00}/{1}] {2} at {3:u}' -f $number, $plan.Count, $step.Name, [DateTime]::UtcNow)
            $arguments = $context.ComposeArguments + @('--profile', 'jobs', 'run', '--rm', '--no-deps', '-T', '--pull', 'never', 'worker') + $step.Arguments
            $result = Invoke-DockerCommand -Arguments $arguments -WorkingDirectory $root -TimeoutSeconds $StepTimeoutSeconds
            $timer.Stop()
            if (Get-OptionalProperty $context 'IsPackage' $false) {
                $diagnosticPath = Save-BootstrapStepDiagnostics -Context $context -Model $model -StepName $step.Name -Result $result -ElapsedSeconds $timer.Elapsed.TotalSeconds
                Write-Host ('Sanitized diagnostic log: ' + $diagnosticPath)
            }
            # CLI exceptions can contain connection details. Never replay raw streams.
            Write-Host ('RESULT {0}: exit={1}, elapsed={2:N1}s (raw output withheld; existing CLI logs/reports remain in runtime mounts).' -f $step.Name, $result.ExitCode, $timer.Elapsed.TotalSeconds)
            if ($result.ExitCode -ne 0) {
                if ($result.ExitCode -eq 124) {
                    Write-Warning 'CLI timeout: the one-off worker/transaction may still be running. Inspect it separately and confirm completion/rollback before retrying; no later stage will run.'
                }
                Stop-Startup ('Bootstrap stopped at ' + $step.Name + '; later stages were not run. Resolve the failure before rerunning; no force/recreate was requested.')
            }
        }
        Write-Host 'All bootstrap CLI stages completed, including actual PostgreSQL validation/report. No ML/training was requested.'
        return 0
    } catch {
        $safe = $_.Exception.Data['StartupSafeMessage']
        if (-not $safe) { $safe = 'Bootstrap failed; raw exception/configuration output withheld to protect secrets. No further stages will run.' }
        Write-Warning $safe
        return 1
    }
}

if ($MyInvocation.InvocationName -ne '.') {
    exit (Invoke-DataBootstrap @script:BootstrapInvocation)
}
