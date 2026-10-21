#requires -Version 5.1
<#
Prepare/smoke only the isolated bootstrap-test environment. Never invokes Execute.
Build is explicit; no pulls, no data acquisition, and no automatic cleanup.
#>
[CmdletBinding()]
param(
    [switch]$BuildWorker,
    [switch]$CheckOnly,
    [ValidateRange(30, 1800)][int]$HealthTimeoutSeconds = 180,
    [ValidateRange(5, 300)][int]$CommandTimeoutSeconds = 30,
    [ValidateRange(60, 7200)][int]$BuildTimeoutSeconds = 1800
)
$script:PrepareTestInvocation = @{
    BuildWorker = [bool]$BuildWorker; HealthTimeoutSeconds = $HealthTimeoutSeconds
    CheckOnly = [bool]$CheckOnly
    CommandTimeoutSeconds = $CommandTimeoutSeconds; BuildTimeoutSeconds = $BuildTimeoutSeconds
}
. (Join-Path $PSScriptRoot 'bootstrap.ps1')
$script:PrepareTestRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))

function Get-BootstrapTestResources {
    param($Context)
    $template = '{"CPUs":{{.NCPU}},"MemoryBytes":{{.MemTotal}},"OSType":{{json .OSType}},"ServerVersion":{{json .ServerVersion}}}'
    $json = Invoke-StartupDocker $Context @('info', '--format', $template) 'Cannot measure Docker resources'
    $docker = ConvertFrom-StartupJson $json 'Docker resources'
    $drive = New-Object IO.DriveInfo([IO.Path]::GetPathRoot($Context.RuntimeRoot))
    $freeGiB = $drive.AvailableFreeSpace / 1GB
    Write-Host ('Measured host free={0:N2} GiB; Docker CPUs={1}, RAM={2:N2} GiB, engine={3}.' -f $freeGiB, $docker.CPUs, ($docker.MemoryBytes / 1GB), $docker.ServerVersion)
    if ($docker.OSType -ne 'linux' -or $docker.CPUs -lt 2 -or $docker.MemoryBytes -lt 2GB -or $freeGiB -lt 10) {
        Stop-Startup 'Insufficient resources for safe image build/service smoke (Linux, >=2 CPUs, >=2 GiB Docker RAM, >=10 GiB host free required). No services were started.'
    }
    Write-Host 'Full-bootstrap ESTIMATE, not a measured requirement: reserve 40-60 GiB extra and 3-8 hours; allow more for network/WAL/temp. Docker resources are shared with the real stack; no resource settings are changed.'
    Write-Host 'Host free space is not Docker VM free space; the worker will report its filesystem capacity separately.'
}

function Invoke-PrepareBootstrapTest {
    param(
        [string]$ProjectRoot = $script:PrepareTestRoot,
        [switch]$BuildWorker,
        [switch]$CheckOnly,
        [int]$HealthTimeoutSeconds = 180,
        [int]$CommandTimeoutSeconds = 30,
        [int]$BuildTimeoutSeconds = 1800
    )
    try {
        $script:DockerCliPath = Resolve-DockerCli
        if (-not $script:DockerCliPath) { Stop-Startup 'Docker CLI is missing; start/install Docker Desktop separately.' }
        $context = New-ProjectEnvironmentContext -ProjectRoot $ProjectRoot -Environment bootstrap-test -HealthTimeoutSeconds $HealthTimeoutSeconds -CommandTimeoutSeconds $CommandTimeoutSeconds
        $null = Invoke-StartupDocker $context @('info', '--format', '{{.ServerVersion}}') 'Docker daemon is unavailable'
        $version = Invoke-StartupDocker $context @('compose', 'version', '--short') 'Docker Compose is unavailable'
        if ($version.Trim() -notmatch '^v?(\d+\.\d+\.\d+)' -or [version]$Matches[1] -lt [version]'2.24.4') { Stop-Startup 'Compose >=2.24.4 is required for safe !override/!reset merges. No services were started.' }
        Initialize-BootstrapTestRuntime $context
        $null = Invoke-StartupCompose $context @('--profile', '*', 'config', '--quiet') 'Test Compose validation failed; resolved values withheld'
        $json = Invoke-StartupCompose $context @('--profile', '*', 'config', '--format', 'json') 'Cannot resolve isolated Compose metadata'
        $model = ConvertFrom-StartupJson $json 'isolated Compose metadata'
        Assert-ProjectEnvironmentIsolation $context $model
        $context.Project = $model.name
        $null = Test-BootstrapWorkerModel $context $model
        Write-Host ('Verified test project=' + $context.Project + '; worker image=' + $model.services.worker.image + '; published ports=NONE.')
        Write-Host ('Runtime root=' + $context.RuntimeRoot + '; volumes=' + $model.volumes.'postgres-data'.name + ', ' + $model.volumes.'mlflow-data'.name)
        Get-BootstrapTestResources $context
        if ($CheckOnly) {
            Write-Host 'Preflight passed. Only isolated runtime directories/credentials were prepared; no image build, service startup, worker or bootstrap ran.'
            return 0
        }
        $baseBefore = $null
        if ($BuildWorker) { $baseBefore = Get-BootstrapWorkerImage $context 'nyc-taxi-worker:p2' }
        if (-not $BuildWorker) { $null = Get-BootstrapWorkerImage $context $model.services.worker.image }
        # Reuse all ownership/config/image/health and READ ONLY data-presence logic.
        $startupExit = Invoke-ProjectStart -ProjectRoot $context.Root -Environment bootstrap-test -Build:$BuildWorker -HealthTimeoutSeconds $HealthTimeoutSeconds -CommandTimeoutSeconds $CommandTimeoutSeconds -BuildTimeoutSeconds $BuildTimeoutSeconds
        if ($startupExit -ne 0) { Stop-Startup ('Isolated service preparation stopped (exit ' + $startupExit + '); existing services were not recreated.') }
        if ($BuildWorker) {
            $baseAfter = Get-BootstrapWorkerImage $context 'nyc-taxi-worker:p2'
            if ($baseAfter.ID -ne $baseBefore.ID) { Stop-Startup 'P2 base image ID changed during preparation; stopping before worker smoke. Review separately.' }
            Write-Host 'Existing P2 base image ID is unchanged; only the test tag was built.'
        }
        Assert-BootstrapServiceHealth $context $model
        if (-not (Test-BootstrapWorkerFreshness $context -StepTimeoutSeconds $CommandTimeoutSeconds)) { return 2 }
        $text = Invoke-StartupCompose $context @('--profile', 'jobs', 'run', '--rm', '--no-deps', '-T', '--pull', 'never', 'worker', 'python', '-B', 'scripts/check_worker_services.py') 'Test worker connectivity smoke failed'
        Write-Host $text.Trim()

        # Prove service identity against test-network IPs, not just SELECT 1.
        $inventory = Get-EnvironmentDockerInventory $context
        $pg = @($inventory.Containers | Where-Object { $_.Name -eq 'nyc-taxi-bootstrap-test-postgres' })[0]
        $ml = @($inventory.Containers | Where-Object { $_.Name -eq 'nyc-taxi-bootstrap-test-mlflow' })[0]
        $networkName = 'nyc-taxi-bootstrap-test_default'
        $expected = @{
            postgres_ip = $pg.Networks.$networkName.IPAddress
            mlflow_ip = $ml.Networks.$networkName.IPAddress
        }
        $probe = @'
import json, os, platform, socket, sys
from nyc_taxi.common.config import load_config
from nyc_taxi.database.postgres import get_engine
expected = json.load(sys.stdin)
engine = get_engine(load_config(), connect_timeout_seconds=5)
try:
    with engine.connect() as c:
        c.exec_driver_sql('SET TRANSACTION READ ONLY')
        c.exec_driver_sql("SET LOCAL statement_timeout = '5s'")
        c.exec_driver_sql("SET LOCAL lock_timeout = '2s'")
        db, ip, port = c.exec_driver_sql('SELECT current_database(), host(inet_server_addr()), inet_server_port()').one()
        fact, dimensions, logs = c.exec_driver_sql('SELECT EXISTS(SELECT 1 FROM analytics.fact_trip), EXISTS(SELECT 1 FROM analytics.dim_location) OR EXISTS(SELECT 1 FROM analytics.dim_calendar) OR EXISTS(SELECT 1 FROM analytics.dim_weather), (SELECT COUNT(*) FROM analytics.etl_load_log)').one()
finally:
    engine.dispose()
if db != 'nyc_taxi_bootstrap_test' or ip != expected['postgres_ip'] or port != 5432 or socket.gethostbyname('postgres') != ip or socket.gethostbyname('mlflow') != expected['mlflow_ip']:
    raise RuntimeError('Isolated endpoint identity mismatch')
space = os.statvfs('/app')
print(json.dumps({'database': db, 'test_network_identity': True, 'fact_has_rows': fact, 'any_dimension_has_rows': dimensions, 'load_log_entries': logs, 'docker_fs_available_gib': round(space.f_bavail * space.f_frsize / 1024**3, 2), 'docker_fs_total_gib': round(space.f_blocks * space.f_frsize / 1024**3, 2), 'python': platform.python_version()}))
'@
        $arguments = $context.ComposeArguments + @('--profile', 'jobs', 'run', '--rm', '--no-deps', '-T', '--pull', 'never', 'worker', 'python', '-B', '-c', $probe)
        $result = Invoke-DockerCommand -Arguments $arguments -WorkingDirectory $context.Root -InputText ($expected | ConvertTo-Json -Compress) -TimeoutSeconds $CommandTimeoutSeconds
        if ($result.ExitCode -ne 0) { Stop-Startup ('Read-only isolated identity/data/capacity probe failed (exit ' + $result.ExitCode + '); raw details withheld.') }
        $identity = ConvertFrom-StartupJson $result.StdOut 'test identity probe'
        # All fields are defined by the trusted packaged probe, never env/URI values.
        Write-Host ('Test smoke: ' + ($identity | ConvertTo-Json -Compress))
        Write-Host 'Running existing bootstrap in PLAN mode only.'
        $planExit = Invoke-DataBootstrap -ProjectRoot $context.Root -Environment bootstrap-test -CommandTimeoutSeconds $CommandTimeoutSeconds
        if ($planExit -ne 0) { return $planExit }
        Write-Host 'P6.1 preparation/smoke complete. Bootstrap Execute/E2E NOT run. Test services/volumes are retained; no cleanup was requested.'
        return 0
    } catch {
        $safe = $_.Exception.Data['StartupSafeMessage']
        if (-not $safe) { $safe = 'Isolated environment preparation failed; raw output withheld to protect configuration. No automatic cleanup or data bootstrap was attempted.' }
        Write-Warning $safe
        return 1
    }
}

if ($MyInvocation.InvocationName -ne '.') {
    exit (Invoke-PrepareBootstrapTest @script:PrepareTestInvocation)
}
