# Pester 3.4 / Windows PowerShell 5.1. Every external Docker call is mocked.
$bootstrapScript = Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts/bootstrap.ps1'
. $bootstrapScript

function New-BootstrapDockerResult {
    param([string]$StdOut = '', [int]$ExitCode = 0, [string]$StdErr = '')
    [pscustomobject]@{ ExitCode = $ExitCode; StdOut = $StdOut; StdErr = $StdErr }
}

function Get-BootstrapFixtureModel {
    $runtimeVolumes = @()
    foreach ($directory in @('data', 'logs', 'models', 'reports')) {
        $runtimeVolumes += @{
            type = 'bind'; source = (Join-Path $script:BootstrapFixtureRoot $directory)
            target = "/app/$directory"; read_only = $false
        }
    }
    $worker = @{
        image = 'nyc-taxi-worker:p2'; profiles = @('jobs')
        build = @{ context = $script:BootstrapFixtureRoot; dockerfile = 'Dockerfile.spark' }
        command = @('python', 'scripts/check_worker_services.py')
        environment = @{
            POSTGRES_HOST = 'postgres'; POSTGRES_PORT = '5432'
            POSTGRES_DB = '${POSTGRES_DB:-nyc_taxi}'
            POSTGRES_USER = '${POSTGRES_USER:-nyc_taxi}'
            POSTGRES_PASSWORD = '${POSTGRES_PASSWORD:-change_me}'
            MLFLOW_TRACKING_URI = 'http://mlflow:5000'
        }
        volumes = $runtimeVolumes
        networks = @{ default = $null }
        depends_on = @{
            postgres = @{ condition = 'service_healthy' }
            mlflow = @{ condition = 'service_healthy' }
        }
    }
    switch ($script:BootstrapScenario.ModelProblem) {
        'profile' { $worker.profiles = @('other') }
        'host' { $worker.environment.POSTGRES_HOST = 'localhost' }
        'port' { $worker.environment.POSTGRES_PORT = '15432' }
        'mlflow' { $worker.environment.MLFLOW_TRACKING_URI = 'http://localhost:5000' }
        'source-mount' {
            $worker.volumes += @{ type = 'bind'; source = $script:BootstrapFixtureRoot; target = '/app' }
        }
        'wrong-data-mount' { $worker.volumes[0].source = (Join-Path $TestDrive 'other-data') }
        'entrypoint' { $worker.entrypoint = @('sh', '-c') }
        'workdir' { $worker.working_dir = '/other' }
        'network' { $worker.network_mode = 'host' }
    }
    @{
        name = 'nyc-taxi-bigdata'
        services = @{
            postgres = @{
                image = 'postgres:16'; container_name = 'nyc-taxi-postgres'
                networks = @{ default = $null }
            }
            mlflow = @{
                image = 'ghcr.io/mlflow/mlflow:v3.5.1'; container_name = 'nyc-taxi-mlflow'
                networks = @{ default = $null }
            }
            worker = $worker
        }
        networks = @{ default = @{ name = 'nyc-taxi-bigdata_default' } }
        volumes = @{
            'postgres-data' = @{ name = 'nyc-taxi-bigdata_postgres-data' }
            'mlflow-data' = @{ name = 'nyc-taxi-bigdata_mlflow-data' }
        }
    }
}

function Get-BootstrapFixtureStep {
    param([string[]]$Arguments)
    if ($Arguments -contains 'scripts/bootstrap_calendar.py') { return 'calendar' }
    if ($Arguments -contains 'scripts/download_raw_data.py') { return 'raw' }
    if ($Arguments -contains 'scripts/build_bronze.py') { return 'bronze' }
    if ($Arguments -contains 'scripts/build_silver.py') { return 'silver' }
    if ($Arguments -contains 'scripts/build_gold.py') { return 'gold' }
    if ($Arguments -contains 'scripts/load_postgres.py') {
        if ($Arguments -contains '--dimensions') { return 'dimensions' }
        if ($Arguments -contains '--create-indexes') { return 'indexes-view' }
        if ($Arguments -contains '--validate') { return 'validation' }
        $monthPosition = [Array]::IndexOf($Arguments, '--month')
        if ($monthPosition -ge 0) { return ('fact-' + $Arguments[$monthPosition + 1]) }
    }
    return $null
}

function Invoke-BootstrapFakeDocker {
    param([string[]]$Arguments, [string]$InputText, [int]$TimeoutSeconds)
    $script:BootstrapDockerCalls.Add([pscustomobject]@{
        Arguments = @($Arguments); InputText = $InputText; TimeoutSeconds = $TimeoutSeconds
    })
    $joined = $Arguments -join ' '
    if ($Arguments[0] -eq 'info') {
        if ($script:BootstrapScenario.DaemonMissing) {
            return New-BootstrapDockerResult -ExitCode 1 -StdErr 'daemon fixture-secret'
        }
        return New-BootstrapDockerResult -StdOut '29.3.0'
    }
    if ($joined -match '^compose version') {
        if ($script:BootstrapScenario.ComposeMissing) { return New-BootstrapDockerResult -ExitCode 1 }
        return New-BootstrapDockerResult -StdOut '5.6.0'
    }
    if ($Arguments[0] -eq 'compose' -and $Arguments -contains 'config') {
        if ($Arguments -contains '--quiet') {
            if ($script:BootstrapScenario.InvalidConfig) {
                return New-BootstrapDockerResult -ExitCode 1 -StdErr 'config fixture-secret'
            }
            return New-BootstrapDockerResult
        }
        if ($Arguments -contains '--format') {
            return New-BootstrapDockerResult -StdOut ((Get-BootstrapFixtureModel) | ConvertTo-Json -Depth 15 -Compress)
        }
        if ($Arguments -contains '--images') { return New-BootstrapDockerResult -StdOut 'nyc-taxi-worker:p2' }
        if ($Arguments -contains '--hash') {
            return New-BootstrapDockerResult -StdOut ($Arguments[-1] + ' config-' + $Arguments[-1])
        }
    }
    if ($joined -match '^container ls') {
        $service = $null
        foreach ($candidate in @('postgres', 'mlflow')) {
            if ($joined -match ('service=' + $candidate + '(?:\s|$)')) { $service = $candidate }
        }
        if (-not $service) { return New-BootstrapDockerResult }
        if ($script:BootstrapScenario.MissingService -eq $service) { return New-BootstrapDockerResult }
        if ($script:BootstrapScenario.MultipleService -eq $service) {
            return New-BootstrapDockerResult -StdOut "$service-id`n$service-extra-id"
        }
        return New-BootstrapDockerResult -StdOut "$service-id"
    }
    if ($joined -match '^container inspect') {
        $service = $Arguments[-1] -replace '-id$', ''
        $state = 'running'
        $health = 'healthy'
        if ($script:BootstrapScenario.StoppedService -eq $service) { $state = 'exited' }
        if ($script:BootstrapScenario.UnhealthyService -eq $service) { $health = 'unhealthy' }
        $project = 'nyc-taxi-bigdata'
        if ($script:BootstrapScenario.WrongProjectService -eq $service) { $project = 'foreign-project' }
        $snapshot = @{
            ID = "$service-id"; ImageID = "sha256:image-$service"
            ImageReference = 'fixture-image'; Status = $state; Health = $health
            StartedAt = '2026-10-19T13:21:05.000000000Z'
            Project = $project; Service = $service; ConfigHash = "config-$service"
        }
        return New-BootstrapDockerResult -StdOut ($snapshot | ConvertTo-Json -Compress)
    }
    if ($joined -match '^image inspect') {
        if ($script:BootstrapScenario.MissingImage) { return New-BootstrapDockerResult -ExitCode 1 }
        $imageId = 'sha256:worker-image'
        if ($script:BootstrapScenario.ImageChanged -and $script:BootstrapScenario.HashChecked) {
            $imageId = 'sha256:different-worker-image'
        }
        $formatIndex = [Array]::IndexOf($Arguments, '--format')
        if ($formatIndex -ge 0 -and $Arguments[$formatIndex + 1] -notmatch 'WorkingDir|Entrypoint|json') {
            return New-BootstrapDockerResult -StdOut $imageId
        }
        $entrypoint = $null
        $workingDir = '/app'
        if ($script:BootstrapScenario.ImageEntrypoint) { $entrypoint = @('sh', '-c') }
        if ($script:BootstrapScenario.ImageWorkdir) { $workingDir = '/wrong' }
        $metadata = @{
            ID = $imageId; ImageID = $imageId; WorkingDir = $workingDir; Entrypoint = $entrypoint
            Config = @{ WorkingDir = $workingDir; Entrypoint = $entrypoint }
        }
        return New-BootstrapDockerResult -StdOut ($metadata | ConvertTo-Json -Depth 5 -Compress)
    }
    if ($Arguments[0] -eq 'compose' -and $Arguments -contains 'run') {
        if ($Arguments -contains '-I') {
            $script:BootstrapScenario.HashChecked = $true
            if ($script:BootstrapScenario.BadHashJson) {
                return New-BootstrapDockerResult -StdOut 'invalid fixture-secret'
            }
            if ($script:BootstrapScenario.HashProbeFailed) {
                return New-BootstrapDockerResult -ExitCode 1 -StdErr 'probe fixture-secret'
            }
            $mismatches = @()
            if ($script:BootstrapScenario.StaleImage) { $mismatches = @('scripts/bootstrap_calendar.py') }
            $checked = 2
            if ($script:BootstrapScenario.ShortHashCheck) { $checked = 1 }
            return New-BootstrapDockerResult -StdOut (@{
                ok = (-not $script:BootstrapScenario.StaleImage)
                checked = $checked; mismatches = $mismatches
            } | ConvertTo-Json -Compress)
        }
        $step = Get-BootstrapFixtureStep -Arguments $Arguments
        if ($step) {
            $script:BootstrapExecutedSteps.Add($step)
            if ($script:BootstrapScenario.FailingStep -eq $step) {
                return New-BootstrapDockerResult -ExitCode $script:BootstrapScenario.FailingExitCode -StdOut 'job fixture-secret' -StdErr 'password=fixture-secret'
            }
        }
        return New-BootstrapDockerResult -StdOut 'runtime fixture-secret'
    }
    throw "Unexpected mocked command (real Docker is never called): $joined"
}

function Invoke-BootstrapFixture {
    param([switch]$Execute)
    $parameters = @{
        ProjectRoot = $script:BootstrapFixtureRoot; CommandTimeoutSeconds = 3
        StepTimeoutSeconds = 60
    }
    if ($Execute) { $parameters.Execute = $true }
    $script:BootstrapOutput = @(& { Invoke-DataBootstrap @parameters } *>&1)
    $integers = @($script:BootstrapOutput | Where-Object { $_ -is [int] })
    $script:BootstrapExit = $integers[-1]
    $script:BootstrapText = $script:BootstrapOutput | Out-String
}

function Get-BootstrapRunCalls {
    @($script:BootstrapDockerCalls | Where-Object { $_.Arguments -contains 'run' })
}

function Get-BootstrapServiceMutationCalls {
    @($script:BootstrapDockerCalls | Where-Object {
        @($_.Arguments | Where-Object { $_ -in @('up', 'down', 'start', 'stop', 'restart', 'build', 'pull', 'create', 'rm') }).Count -gt 0
    })
}

Describe 'P6 explicit data bootstrap orchestration (mock Docker only)' {
    BeforeEach {
        $script:BootstrapFixtureRoot = Join-Path $TestDrive 'bootstrap project with spaces'
        foreach ($directory in @('', 'scripts', 'src', 'sql', 'config', 'data', 'logs', 'models', 'reports')) {
            $path = $script:BootstrapFixtureRoot
            if ($directory) { $path = Join-Path $path $directory }
            [IO.Directory]::CreateDirectory($path) | Out-Null
        }
        $fixtureFiles = @(
            '.env', 'docker-compose.yml', 'Dockerfile.spark', 'requirements.txt', 'pyproject.toml'
            'config/config.yaml', 'scripts/start.ps1', 'scripts/bootstrap_calendar.py'
            'scripts/create_calendar.py', 'scripts/check_worker_services.py'
            'scripts/download_raw_data.py', 'scripts/build_bronze.py', 'scripts/build_silver.py'
            'scripts/build_gold.py', 'scripts/load_postgres.py'
        )
        foreach ($relative in $fixtureFiles) {
            [IO.File]::WriteAllText((Join-Path $script:BootstrapFixtureRoot $relative), '# mock fixture')
        }
        $script:BootstrapScenario = @{
            DaemonMissing = $false; ComposeMissing = $false; InvalidConfig = $false
            ModelProblem = ''; MissingService = ''; MultipleService = ''; StoppedService = ''
            UnhealthyService = ''; WrongProjectService = ''; MissingImage = $false
            ImageChanged = $false; ImageEntrypoint = $false; ImageWorkdir = $false
            HashChecked = $false; BadHashJson = $false; HashProbeFailed = $false; ShortHashCheck = $false
            StaleImage = $false; FailingStep = ''; FailingExitCode = 7
        }
        $script:BootstrapDockerCalls = New-Object 'System.Collections.Generic.List[object]'
        $script:BootstrapExecutedSteps = New-Object 'System.Collections.Generic.List[string]'
        $script:BootstrapExpectedSteps = @(
            'calendar', 'raw', 'bronze', 'silver', 'gold', 'dimensions'
            'fact-2025-01', 'fact-2025-02', 'fact-2025-03', 'fact-2025-04'
            'fact-2025-05', 'fact-2025-06', 'indexes-view', 'validation'
        )
        Mock Resolve-DockerCli { 'mock-docker.exe' }
        Mock Get-BootstrapFileHashes {
            param($ProjectRoot)
            if ($ProjectRoot -ne $script:BootstrapFixtureRoot) { throw 'Wrong hash inventory root' }
            @{ 'scripts/bootstrap_calendar.py' = ('a' * 64); 'requirements.txt' = ('b' * 64) }
        }
        Mock Invoke-DockerCommand {
            param($Arguments, $WorkingDirectory, $InputText, $TimeoutSeconds)
            if ($WorkingDirectory -ne $script:BootstrapFixtureRoot) { throw 'Wrong project working directory' }
            Invoke-BootstrapFakeDocker -Arguments $Arguments -InputText $InputText -TimeoutSeconds $TimeoutSeconds
        }
    }

    It 'defaults to a plan and never launches Python, jobs, or service mutations' {
        Invoke-BootstrapFixture
        $script:BootstrapExit | Should Be 0
        @(Get-BootstrapRunCalls).Count | Should Be 0
        @(Get-BootstrapServiceMutationCalls).Count | Should Be 0
        $script:BootstrapExecutedSteps.Count | Should Be 0
        $script:BootstrapText | Should Match '2025-01'
        $script:BootstrapText | Should Match '2025-06'
        $script:BootstrapText | Should Not Match 'fixture-secret'
    }

    It 'plans all fourteen dependency-ordered stages' {
        $plan = @(Get-BootstrapPlan)
        $plan.Count | Should Be 14
        $serialized = $plan | ConvertTo-Json -Depth 10 -Compress
        foreach ($file in @('bootstrap_calendar.py', 'download_raw_data.py', 'build_bronze.py', 'build_silver.py', 'build_gold.py', 'load_postgres.py')) {
            $serialized | Should Match ([regex]::Escape($file))
        }
        $serialized | Should Not Match '\-\-force|\-\-recreate-schema'
    }

    It 'executes all stages sequentially only with Execute' {
        Invoke-BootstrapFixture -Execute
        $script:BootstrapExit | Should Be 0
        ($script:BootstrapExecutedSteps -join ',') | Should Be ($script:BootstrapExpectedSteps -join ',')
        @(Get-BootstrapServiceMutationCalls).Count | Should Be 0
        $script:BootstrapText | Should Not Match 'fixture-secret'
    }

    It 'checks image contents before the first data stage' {
        Invoke-BootstrapFixture -Execute
        $script:BootstrapExit | Should Be 0
        $runCalls = @(Get-BootstrapRunCalls)
        ($runCalls[0].Arguments -contains '-I') | Should Be $true
        $runCalls[0].InputText | Should Match 'scripts/bootstrap_calendar.py'
        $runCalls[0].InputText | Should Not Match '\.env|fixture-secret'
    }

    It 'uses jobs profile and no-deps, rm, noninteractive and never-pull flags for every worker run' {
        Invoke-BootstrapFixture -Execute
        $script:BootstrapExit | Should Be 0
        foreach ($call in @(Get-BootstrapRunCalls)) {
            foreach ($flag in @('--profile', 'jobs', '--rm', '--no-deps', '-T', '--pull', 'never', 'worker')) {
                ($call.Arguments -contains $flag) | Should Be $true
            }
            ($call.Arguments -contains '--force') | Should Be $false
            ($call.Arguments -contains '--recreate-schema') | Should Be $false
        }
    }

    It 'preserves exact Jan-Jun CLI selectors and performs indexes before validation' {
        Invoke-BootstrapFixture -Execute
        $script:BootstrapExit | Should Be 0
        $calls = @(Get-BootstrapRunCalls)
        $raw = @($calls | Where-Object { $_.Arguments -contains 'scripts/download_raw_data.py' })[0]
        ($raw.Arguments -join ' ') | Should Match '--taxi-start-month 2025-01 --taxi-end-month 2025-06'
        foreach ($scriptName in @('build_bronze.py', 'build_silver.py', 'build_gold.py')) {
            $call = @($calls | Where-Object { $_.Arguments -contains "scripts/$scriptName" })[0]
            ($call.Arguments -contains '--all') | Should Be $true
        }
        $script:BootstrapExecutedSteps[-2] | Should Be 'indexes-view'
        $script:BootstrapExecutedSteps[-1] | Should Be 'validation'
    }

    It 'reruns the same non-force CLIs so existing manifest and lineage logic controls skips' {
        Invoke-BootstrapFixture -Execute
        $script:BootstrapExit | Should Be 0
        Invoke-BootstrapFixture -Execute
        $script:BootstrapExit | Should Be 0
        ($script:BootstrapExecutedSteps -join ',') | Should Be (($script:BootstrapExpectedSteps + $script:BootstrapExpectedSteps) -join ',')
        foreach ($call in @(Get-BootstrapRunCalls)) {
            ($call.Arguments -contains '--force') | Should Be $false
            ($call.Arguments -contains '--recreate-schema') | Should Be $false
        }
    }

    It 'stops immediately after a failed Silver job and withholds raw diagnostics' {
        $script:BootstrapScenario.FailingStep = 'silver'
        Invoke-BootstrapFixture -Execute
        $script:BootstrapExit | Should Be 1
        ($script:BootstrapExecutedSteps -join ',') | Should Be 'calendar,raw,bronze,silver'
        $script:BootstrapText | Should Not Match 'fixture-secret'
        @(Get-BootstrapServiceMutationCalls).Count | Should Be 0
    }

    It 'stops immediately after a failed fact partition without advancing to June or indexes' {
        $script:BootstrapScenario.FailingStep = 'fact-2025-05'
        Invoke-BootstrapFixture -Execute
        $script:BootstrapExit | Should Be 1
        $script:BootstrapExecutedSteps[-1] | Should Be 'fact-2025-05'
        ($script:BootstrapExecutedSteps -contains 'fact-2025-06') | Should Be $false
        ($script:BootstrapExecutedSteps -contains 'indexes-view') | Should Be $false
    }

    It 'treats a bounded job timeout as a failure and does not continue' {
        $script:BootstrapScenario.FailingStep = 'bronze'
        $script:BootstrapScenario.FailingExitCode = 124
        Invoke-BootstrapFixture -Execute
        $script:BootstrapExit | Should Be 1
        ($script:BootstrapExecutedSteps -join ',') | Should Be 'calendar,raw,bronze'
        $jobCalls = @((Get-BootstrapRunCalls) | Where-Object { $_.Arguments -contains 'scripts/build_bronze.py' })
        $jobCalls[0].TimeoutSeconds | Should Be 60
    }

    It 'requires a separately rebuilt image when packaged runtime files differ' {
        $script:BootstrapScenario.StaleImage = $true
        Invoke-BootstrapFixture -Execute
        $script:BootstrapExit | Should Be 2
        $script:BootstrapExecutedSteps.Count | Should Be 0
        $script:BootstrapText | Should Match 'build'
        @(Get-BootstrapServiceMutationCalls).Count | Should Be 0
    }

    It 'fails safely on a malformed image fingerprint response' {
        $script:BootstrapScenario.BadHashJson = $true
        Invoke-BootstrapFixture -Execute
        $script:BootstrapExit | Should Be 1
        $script:BootstrapExecutedSteps.Count | Should Be 0
        $script:BootstrapText | Should Not Match 'fixture-secret'
    }

    It 'does not run data jobs when the fingerprint probe fails' {
        $script:BootstrapScenario.HashProbeFailed = $true
        Invoke-BootstrapFixture -Execute
        $script:BootstrapExit | Should Be 1
        $script:BootstrapExecutedSteps.Count | Should Be 0
        $script:BootstrapText | Should Not Match 'fixture-secret'
    }

    It 'rejects a fingerprint response that did not check every expected file' {
        $script:BootstrapScenario.ShortHashCheck = $true
        Invoke-BootstrapFixture -Execute
        ($script:BootstrapExit -ne 0) | Should Be $true
        $script:BootstrapExecutedSteps.Count | Should Be 0
    }

    It 'rejects a worker image tag that changes after content verification' {
        $script:BootstrapScenario.ImageChanged = $true
        Invoke-BootstrapFixture -Execute
        ($script:BootstrapExit -ne 0) | Should Be $true
        $script:BootstrapExecutedSteps.Count | Should Be 0
    }

    It 'anchors all Compose calls to script project root independently of cwd' {
        Push-Location $TestDrive
        try { Invoke-BootstrapFixture } finally { Pop-Location }
        $script:BootstrapExit | Should Be 0
        foreach ($call in @($script:BootstrapDockerCalls | Where-Object { $_.Arguments[0] -eq 'compose' -and $_.Arguments[1] -ne 'version' })) {
            ($call.Arguments -contains '--project-directory') | Should Be $true
            ($call.Arguments -contains $script:BootstrapFixtureRoot) | Should Be $true
            ($call.Arguments -contains (Join-Path $script:BootstrapFixtureRoot 'docker-compose.yml')) | Should Be $true
        }
    }

    It 'preserves an existing env file without exposing its values' {
        $envPath = Join-Path $script:BootstrapFixtureRoot '.env'
        [IO.File]::WriteAllText($envPath, 'POSTGRES_PASSWORD=fixture-secret')
        $before = [IO.File]::ReadAllBytes($envPath)
        Invoke-BootstrapFixture
        $script:BootstrapExit | Should Be 0
        [Convert]::ToBase64String([IO.File]::ReadAllBytes($envPath)) | Should Be ([Convert]::ToBase64String($before))
        $script:BootstrapText | Should Not Match 'fixture-secret'
    }

    It 'does not create env configuration when it is missing' {
        $envPath = Join-Path $script:BootstrapFixtureRoot '.env'
        Remove-Item -LiteralPath $envPath
        Invoke-BootstrapFixture
        $script:BootstrapExit | Should Be 1
        (Test-Path -LiteralPath $envPath) | Should Be $false
        @(Get-BootstrapRunCalls).Count | Should Be 0
    }

    It 'fails when Docker CLI is absent without attempting commands' {
        Mock Resolve-DockerCli { $null }
        Invoke-BootstrapFixture
        $script:BootstrapExit | Should Be 1
        $script:BootstrapDockerCalls.Count | Should Be 0
    }

    It 'fails when Docker daemon is unavailable and suppresses its raw output' {
        $script:BootstrapScenario.DaemonMissing = $true
        Invoke-BootstrapFixture
        $script:BootstrapExit | Should Be 1
        @(Get-BootstrapRunCalls).Count | Should Be 0
        $script:BootstrapText | Should Not Match 'fixture-secret'
    }

    It 'fails when Compose is unavailable' {
        $script:BootstrapScenario.ComposeMissing = $true
        Invoke-BootstrapFixture
        $script:BootstrapExit | Should Be 1
        @(Get-BootstrapRunCalls).Count | Should Be 0
    }

    It 'validates Compose quietly and does not run jobs on invalid configuration' {
        $script:BootstrapScenario.InvalidConfig = $true
        Invoke-BootstrapFixture -Execute
        $script:BootstrapExit | Should Be 1
        @(Get-BootstrapRunCalls).Count | Should Be 0
        $script:BootstrapText | Should Not Match 'fixture-secret'
    }

    It 'does not start or recreate a stopped PostgreSQL service' {
        $script:BootstrapScenario.StoppedService = 'postgres'
        Invoke-BootstrapFixture -Execute
        $script:BootstrapExit | Should Be 1
        @(Get-BootstrapRunCalls).Count | Should Be 0
        @(Get-BootstrapServiceMutationCalls).Count | Should Be 0
    }

    It 'does not start or recreate a missing MLflow service' {
        $script:BootstrapScenario.MissingService = 'mlflow'
        Invoke-BootstrapFixture -Execute
        $script:BootstrapExit | Should Be 1
        @(Get-BootstrapRunCalls).Count | Should Be 0
        @(Get-BootstrapServiceMutationCalls).Count | Should Be 0
    }

    It 'requires healthy services before any worker run' {
        $script:BootstrapScenario.UnhealthyService = 'postgres'
        Invoke-BootstrapFixture -Execute
        $script:BootstrapExit | Should Be 1
        @(Get-BootstrapRunCalls).Count | Should Be 0
    }

    It 'rejects a service container owned by another project' {
        $script:BootstrapScenario.WrongProjectService = 'postgres'
        Invoke-BootstrapFixture -Execute
        $script:BootstrapExit | Should Be 1
        @(Get-BootstrapRunCalls).Count | Should Be 0
    }

    It 'rejects ambiguous service container ownership' {
        $script:BootstrapScenario.MultipleService = 'postgres'
        Invoke-BootstrapFixture -Execute
        $script:BootstrapExit | Should Be 1
        @(Get-BootstrapRunCalls).Count | Should Be 0
    }

    It 'does not build or pull a missing worker image' {
        $script:BootstrapScenario.MissingImage = $true
        Invoke-BootstrapFixture -Execute
        ($script:BootstrapExit -ne 0) | Should Be $true
        @(Get-BootstrapRunCalls).Count | Should Be 0
        @(Get-BootstrapServiceMutationCalls).Count | Should Be 0
    }

    It 'rejects a packaged worker image with an unexpected entrypoint' {
        $script:BootstrapScenario.ImageEntrypoint = $true
        Invoke-BootstrapFixture -Execute
        $script:BootstrapExit | Should Be 1
        @(Get-BootstrapRunCalls).Count | Should Be 0
    }

    It 'rejects a packaged worker image outside the expected app directory' {
        $script:BootstrapScenario.ImageWorkdir = $true
        Invoke-BootstrapFixture -Execute
        $script:BootstrapExit | Should Be 1
        @(Get-BootstrapRunCalls).Count | Should Be 0
    }

    It 'rejects public endpoint and profile changes instead of reconfiguring them' {
        foreach ($problem in @('host', 'port', 'mlflow', 'profile')) {
            $script:BootstrapScenario.ModelProblem = $problem
            Invoke-BootstrapFixture -Execute
            $script:BootstrapExit | Should Be 1
        }
        @(Get-BootstrapRunCalls).Count | Should Be 0
    }

    It 'rejects source bind mounts and runtime binds from an unexpected project' {
        foreach ($problem in @('source-mount', 'wrong-data-mount')) {
            $script:BootstrapScenario.ModelProblem = $problem
            Invoke-BootstrapFixture -Execute
            $script:BootstrapExit | Should Be 1
        }
        @(Get-BootstrapRunCalls).Count | Should Be 0
    }

    It 'rejects overridden worker entrypoint, working directory, or host network' {
        foreach ($problem in @('entrypoint', 'workdir', 'network')) {
            $script:BootstrapScenario.ModelProblem = $problem
            Invoke-BootstrapFixture -Execute
            $script:BootstrapExit | Should Be 1
        }
        @(Get-BootstrapRunCalls).Count | Should Be 0
    }
}
