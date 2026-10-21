#requires -Version 5.1
<#
P6.3 tiny synthetic integration test, NOT a full data bootstrap.
Default PLAN is read-only. -Run executes the existing packaged pipeline CLIs
against the already-running bootstrap-test services. No build/start/pull.
#>
[CmdletBinding()]
param(
    [switch]$Run,
    [ValidateRange(1, 300)][int]$CommandTimeoutSeconds = 30,
    [ValidateRange(60, 600)][int]$StepTimeoutSeconds = 600
)

$script:FixtureInvocation = @{
    Run = [bool]$Run; CommandTimeoutSeconds = $CommandTimeoutSeconds
    StepTimeoutSeconds = $StepTimeoutSeconds
}
. (Join-Path $PSScriptRoot 'bootstrap.ps1')
$script:FixtureProjectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))

function New-FixtureContext {
    param([string]$ProjectRoot, [int]$CommandTimeoutSeconds = 30)
    $context = New-ProjectEnvironmentContext -ProjectRoot $ProjectRoot -Environment bootstrap-test -WorkerVariant clean -CommandTimeoutSeconds $CommandTimeoutSeconds
    $context.ComposeArguments += @('--file', (Join-Path $context.Root 'docker-compose.fixture.yml'))
    $context | Add-Member -MemberType NoteProperty -Name FixtureRoot -Value (Join-Path $context.RuntimeRoot 'p63-fixture')
    return $context
}

function Get-FixturePipelineSteps {
    foreach ($item in @(
        @{ Name = 'guard-initial'; Action = 'guard' },
        @{ Name = 'prepare'; Action = 'prepare' },
        @{ Name = 'bronze'; Arguments = @('python', '-B', 'scripts/build_bronze.py', '--all') },
        @{ Name = 'silver'; Arguments = @('python', '-B', 'scripts/build_silver.py', '--all') },
        @{ Name = 'gold'; Arguments = @('python', '-B', 'scripts/build_gold.py', '--all') },
        @{ Name = 'files-first'; Action = 'files-first' },
        @{ Name = 'guard-before-load'; Action = 'guard' },
        @{ Name = 'dimensions'; Arguments = @('python', '-B', 'scripts/load_postgres.py', '--dimensions') },
        @{ Name = 'facts'; Arguments = @('python', '-B', 'scripts/load_postgres.py', '--start-month', '2025-01', '--end-month', '2025-06') },
        @{ Name = 'indexes-view'; Arguments = @('python', '-B', 'scripts/load_postgres.py', '--create-indexes') },
        @{ Name = 'validation'; Arguments = @('python', '-B', 'scripts/load_postgres.py', '--validate') },
        @{ Name = 'database-first'; Action = 'database-first' },
        @{ Name = 'guard-before-rerun'; Action = 'guard' },
        @{ Name = 'bronze-rerun'; Arguments = @('python', '-B', 'scripts/build_bronze.py', '--all') },
        @{ Name = 'silver-rerun'; Arguments = @('python', '-B', 'scripts/build_silver.py', '--all') },
        @{ Name = 'gold-rerun'; Arguments = @('python', '-B', 'scripts/build_gold.py', '--all') },
        @{ Name = 'files-rerun'; Action = 'files-rerun' },
        @{ Name = 'guard-before-load-rerun'; Action = 'guard' },
        @{ Name = 'dimensions-rerun'; Arguments = @('python', '-B', 'scripts/load_postgres.py', '--dimensions') },
        @{ Name = 'facts-rerun'; Arguments = @('python', '-B', 'scripts/load_postgres.py', '--start-month', '2025-01', '--end-month', '2025-06') },
        @{ Name = 'indexes-view-rerun'; Arguments = @('python', '-B', 'scripts/load_postgres.py', '--create-indexes') },
        @{ Name = 'validation-rerun'; Arguments = @('python', '-B', 'scripts/load_postgres.py', '--validate') },
        @{ Name = 'database-rerun'; Action = 'database-rerun' }
    )) { [pscustomobject]$item }
}

function Assert-FixtureRuntime {
    param($Context)
    $expected = Join-Path $Context.Root '.sandbox/bootstrap-test/p63-fixture'
    if ((Get-NormalizedEnvironmentPath $Context.FixtureRoot) -ne (Get-NormalizedEnvironmentPath $expected)) {
        Stop-Startup 'Fixture runtime must be the fixed p63-fixture subdirectory, not real/full-bootstrap data.'
    }
    Assert-NoEnvironmentReparsePoint $Context.FixtureRoot
    if ((Test-Path -LiteralPath $Context.FixtureRoot) -and -not (Test-Path -LiteralPath $Context.FixtureRoot -PathType Container)) {
        Stop-Startup 'Fixture runtime is not a directory.'
    }
    if (Test-Path -LiteralPath $Context.FixtureRoot -PathType Container) {
        foreach ($item in Get-ChildItem -LiteralPath $Context.FixtureRoot -Force) {
            if ($item.Name -notin @('data', 'logs', 'models', 'reports', '.run.lock')) {
                Stop-Startup 'Unknown fixture-root entry; nothing will be overwritten or removed.'
            }
            Assert-NoEnvironmentReparsePoint $item.FullName
            if ($item.Name -ne '.run.lock' -and -not $item.PSIsContainer) { Stop-Startup 'Fixture runtime directories must not be files.' }
        }
    }
    foreach ($directory in @('data', 'logs', 'models', 'reports')) {
        Assert-NoEnvironmentReparsePoint (Join-Path $Context.FixtureRoot $directory)
    }
}

function Get-FixtureHostFreeBytes {
    param([string]$ProjectRoot)
    $driveRoot = [IO.Path]::GetPathRoot([IO.Path]::GetFullPath($ProjectRoot))
    return (New-Object IO.DriveInfo($driveRoot)).AvailableFreeSpace
}

function Assert-FixtureResources {
    param($Context)
    $json = Invoke-StartupDocker $Context @('info', '--format', '{"CPUs":{{json .NCPU}},"MemoryBytes":{{json .MemTotal}}}') 'Cannot inspect Docker CPU/RAM capacity'
    $resources = ConvertFrom-StartupJson $json 'Docker resources'
    $free = Get-FixtureHostFreeBytes $Context.Root
    if ((Get-OptionalProperty $resources 'CPUs' 0) -lt 2 -or
        (Get-OptionalProperty $resources 'MemoryBytes' 0) -lt 2GB -or $free -lt 10GB) {
        Stop-Startup 'Fixture requires at least 2 Docker CPUs, 2 GiB Docker RAM and 10 GiB free host disk. No pipeline stage started.'
    }
    Write-Host ('Resources: Docker CPUs={0}, RAM={1:N2} GiB, host free={2:N2} GiB (capacity, not currently free Docker RAM).' -f $resources.CPUs, ($resources.MemoryBytes / 1GB), ($free / 1GB))
}

function Get-FixturePostgresAddress {
    param($Context)
    $inventory = Get-EnvironmentDockerInventory $Context
    $oneoffs = @($inventory.Containers | Where-Object { $_.Project -eq $Context.Project -and $_.Service -eq 'worker' })
    if ($oneoffs.Count -gt 0) { Stop-Startup 'A bootstrap-test worker container already exists; inspect it separately before the fixture run. No container was stopped or removed.' }
    $postgres = @($inventory.Containers | Where-Object { $_.Project -eq $Context.Project -and $_.Service -eq 'postgres' })
    if ($postgres.Count -ne 1) { Stop-Startup 'Cannot identify exactly one isolated PostgreSQL container.' }
    $network = Get-OptionalProperty $postgres[0].Networks ($Context.Project + '_default')
    $address = Get-OptionalProperty $network 'IPAddress' ''
    $parsed = $null
    if (-not [Net.IPAddress]::TryParse($address, [ref]$parsed)) { Stop-Startup 'Cannot identify the isolated PostgreSQL network address.' }
    return $address
}

function ConvertTo-FixtureSafeLog {
    param([AllowEmptyString()][string]$Text)
    # Never persist connection URLs or common secret assignments from CLI errors.
    $safe = [regex]::Replace($Text, '(?i)\b(?:https?|postgres(?:ql)?|jdbc:postgresql)://[^\s"''<>]+', '[URL REDACTED]')
    $safe = [regex]::Replace($safe, '(?im)(\b(?:password|passwd|pwd|token|secret|authorization|api[_-]?key)\s*[=:]\s*)(?:"[^"\r\n]*"|''[^''\r\n]*''|[^\s,;]+)', '$1[REDACTED]')
    return $safe
}

function Write-FixtureNewText {
    param([string]$Path, [AllowEmptyString()][string]$Text)
    $stream = New-Object IO.FileStream($Path, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::Read)
    try {
        $bytes = [Text.Encoding]::UTF8.GetBytes($Text)
        $stream.Write($bytes, 0, $bytes.Length)
    } finally { $stream.Dispose() }
}

function Invoke-FixtureStep {
    param($Context, $Model, $Step, [string]$ImageID, [string]$PostgresIP,
        [string]$Harness, [string]$LogPrefix, [int]$StepTimeoutSeconds = 600)
    Assert-BootstrapServiceHealth $Context $Model
    $image = Get-BootstrapWorkerImage $Context 'nyc-taxi-worker:p6-clean'
    if ($image.ID -ne $ImageID) { Stop-Startup 'Worker clean image tag changed; stopped before the next stage.' }
    $arguments = $Context.ComposeArguments + @('--profile', 'jobs', 'run', '--rm', '--no-deps', '-T', '--pull', 'never',
        '--env', ('P63_EXPECTED_POSTGRES_IP=' + $PostgresIP), '--env', ('P63_EXPECTED_IMAGE_ID=' + $ImageID), 'worker')
    $inputText = $null
    $action = Get-OptionalProperty $Step 'Action' ''
    if ($action) {
        $arguments += @('python', '-B', '-', $action)
        $inputText = $Harness
    } else { $arguments += @($Step.Arguments) }
    Write-Host ('START {0}' -f $Step.Name)
    $timer = [Diagnostics.Stopwatch]::StartNew()
    $result = Invoke-DockerCommand -Arguments $arguments -WorkingDirectory $Context.Root -InputText $inputText -TimeoutSeconds $StepTimeoutSeconds
    $timer.Stop()
    foreach ($streamName in @('StdOut', 'StdErr')) {
        $path = $LogPrefix + '-' + $Step.Name + '.' + $streamName.ToLowerInvariant() + '.log'
        Write-FixtureNewText -Path $path -Text (ConvertTo-FixtureSafeLog (Get-OptionalProperty $result $streamName ''))
    }
    Write-Host ('RESULT {0}: exit={1}; elapsed={2:N2}s; sanitized logs kept only in fixture/logs.' -f $Step.Name, $result.ExitCode, $timer.Elapsed.TotalSeconds)
    if ($result.ExitCode -ne 0) {
        if ($result.ExitCode -eq 124) { Write-Warning 'CLI timeout: the one-off worker/transaction may still be running. Inspect it before retrying; no automatic retry or service stop.' }
        Stop-Startup ('Fixture stopped at ' + $Step.Name + '; later stages were not run. Review its sanitized fixture logs; nothing was deleted.')
    }
    return [pscustomobject]@{ Name = $Step.Name; ExitCode = $result.ExitCode; ElapsedSeconds = [math]::Round($timer.Elapsed.TotalSeconds, 3) }
}

function Invoke-PipelineFixture {
    param([string]$ProjectRoot = $script:FixtureProjectRoot, [switch]$Run,
        [ValidateRange(1, 300)][int]$CommandTimeoutSeconds = 30,
        [ValidateRange(60, 600)][int]$StepTimeoutSeconds = 600)
    $lock = $null
    try {
        $context = New-FixtureContext -ProjectRoot $ProjectRoot -CommandTimeoutSeconds $CommandTimeoutSeconds
        foreach ($relative in @('docker-compose.yml', 'docker-compose.bootstrap-test.yml', 'docker-compose.bootstrap-clean.yml',
            'docker-compose.fixture.yml', 'scripts/pipeline_fixture.py')) {
            if (-not (Test-Path -LiteralPath (Join-Path $context.Root $relative) -PathType Leaf)) { Stop-Startup ('Missing fixture public input: ' + $relative + '.') }
        }
        if (-not (Test-Path -LiteralPath $context.EnvPath -PathType Leaf)) { Stop-Startup 'Existing bootstrap-test configuration is required; fixture never creates credentials.' }
        Assert-FixtureRuntime $context
        $script:DockerCliPath = Resolve-DockerCli
        if (-not $script:DockerCliPath) { Stop-Startup 'Docker CLI is unavailable; start Docker Desktop separately.' }
        $null = Invoke-StartupDocker $context @('info', '--format', '{{.ServerVersion}}') 'Docker daemon is unavailable'
        $null = Invoke-StartupDocker $context @('compose', 'version', '--short') 'Docker Compose is unavailable'
        $null = Invoke-StartupCompose $context @('config', '--quiet') 'Fixture Compose validation failed; resolved secrets withheld'
        $json = Invoke-StartupCompose $context @('--profile', '*', 'config', '--format', 'json') 'Cannot resolve fixture Compose metadata'
        $model = ConvertFrom-StartupJson $json 'fixture Compose metadata'
        $context.Project = Get-OptionalProperty $model 'name' ''
        Assert-ProjectEnvironmentIsolation -Context $context -Model $model
        # The broad isolation guard retains the P6.1 root; only the worker-layout
        # check uses the dedicated fixture subdirectory.
        $workerContext = $context.PSObject.Copy()
        $workerContext.RuntimeRoot = $context.FixtureRoot
        $imageName = Test-BootstrapWorkerModel $workerContext $model
        if ($imageName -ne 'nyc-taxi-worker:p6-clean') { Stop-Startup 'Only the verified clean worker is supported for P6.3.' }
        $worker = $model.services.worker
        if ([double](Get-OptionalProperty $worker 'cpus' 0) -ne 1 -or
            [long](Get-OptionalProperty $worker 'mem_limit' 0) -ne 1536MB -or
            (Get-OptionalProperty $worker.environment 'JAVA_TOOL_OPTIONS') -ne '-XX:ActiveProcessorCount=1' -or
            (Get-OptionalProperty $worker.environment 'PYSPARK_SUBMIT_ARGS') -ne '--driver-memory 512m --conf spark.ui.enabled=false pyspark-shell') {
            Stop-Startup 'Fixture worker resource limits differ from the supported bounded configuration.'
        }
        Assert-BootstrapServiceHealth $context $model
        Assert-FixtureResources $context
        $postgresIP = Get-FixturePostgresAddress $context
        $image = Get-BootstrapWorkerImage $context $imageName
        Write-Host ('DOCKER P6.3 - fixture only - project {0} - clean image {1}' -f $context.Project, $image.ID)
        Write-Host 'Synthetic Jan-Jun/2025 Taxi + complete required dimensions. No acquisition/network downloads; no full bootstrap/training.'
        $steps = @(Get-FixturePipelineSteps)
        foreach ($step in $steps) { Write-Host ('PLAN ' + $step.Name) }
        if (-not $Run) {
            Write-Host 'PLAN ONLY: no directories created, worker run, ETL or database operation performed. Use -Run explicitly for the fixture test.'
            return 0
        }
        Assert-FixtureRuntime $context
        $null = [IO.Directory]::CreateDirectory($context.FixtureRoot)
        try { $lock = New-Object IO.FileStream((Join-Path $context.FixtureRoot '.run.lock'), [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None) }
        catch { Stop-Startup 'Fixture run lock is unavailable; another run may be active. No pipeline stage started.' }
        foreach ($directory in @('data', 'logs', 'models', 'reports')) {
            $null = [IO.Directory]::CreateDirectory((Join-Path $context.FixtureRoot $directory))
        }
        if (-not (Test-BootstrapWorkerFreshness $context -StepTimeoutSeconds $CommandTimeoutSeconds)) { return 2 }
        $harness = [IO.File]::ReadAllText((Join-Path $context.Root 'scripts/pipeline_fixture.py'), [Text.Encoding]::UTF8)
        $runID = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssZ') + '-' + [Guid]::NewGuid().ToString('N').Substring(0, 8)
        $prefix = Join-Path $context.FixtureRoot ('logs/p63-' + $runID)
        $results = @()
        $timer = [Diagnostics.Stopwatch]::StartNew()
        foreach ($step in $steps) {
            $results += Invoke-FixtureStep -Context $context -Model $model -Step $step -ImageID $image.ID -PostgresIP $postgresIP -Harness $harness -LogPrefix $prefix -StepTimeoutSeconds $StepTimeoutSeconds
        }
        $timer.Stop()
        $report = [pscustomobject]@{ Kind = 'P6.3 fixture integration'; ImageID = $image.ID; Project = $context.Project
            AcquisitionVerified = $false; FullBootstrapVerified = $false; ElapsedSeconds = [math]::Round($timer.Elapsed.TotalSeconds, 3); Steps = $results }
        Write-FixtureNewText -Path ($prefix + '-orchestration.json') -Text ($report | ConvertTo-Json -Depth 8)
        Write-Host ('Fixture CLI stages and independent checks completed, including rerun. Total={0:N2}s. Acquisition/full bootstrap NOT verified.' -f $timer.Elapsed.TotalSeconds)
        return 0
    } catch {
        $safe = $_.Exception.Data['StartupSafeMessage']
        if (-not $safe) { $safe = 'Fixture orchestration failed; raw exception withheld to protect secrets. No later stage was run.' }
        Write-Warning $safe
        return 1
    } finally { if ($null -ne $lock) { $lock.Dispose() } }
}

if ($MyInvocation.InvocationName -ne '.') { exit (Invoke-PipelineFixture @script:FixtureInvocation) }
