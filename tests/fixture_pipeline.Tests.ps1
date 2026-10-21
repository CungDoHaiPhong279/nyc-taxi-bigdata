# Pester 3.4 / Windows PowerShell 5.1. No real Docker or database calls.
$p63ProjectRoot = Split-Path -Parent $PSScriptRoot
. (Join-Path $p63ProjectRoot 'scripts/test_pipeline_fixture.ps1')

function New-P63Model {
    param([string]$Root)
    $volumes = @()
    foreach ($directory in @('data', 'logs', 'models', 'reports')) {
        $volumes += @{ type = 'bind'; source = (Join-Path $Root ('.sandbox/bootstrap-test/p63-fixture/' + $directory)); target = ('/app/' + $directory); read_only = $false }
    }
    return (@{ name = 'nyc-taxi-bootstrap-test'; services = @{ worker = @{
        image = 'nyc-taxi-worker:p6-clean'; profiles = @('jobs'); working_dir = '/app'
        cpus = 1; mem_limit = 1610612736; volumes = $volumes
        environment = @{ POSTGRES_HOST = 'postgres'; POSTGRES_PORT = '5432'; MLFLOW_TRACKING_URI = 'http://mlflow:5000'
            JAVA_TOOL_OPTIONS = '-XX:ActiveProcessorCount=1'; PYSPARK_SUBMIT_ARGS = '--driver-memory 512m --conf spark.ui.enabled=false pyspark-shell' }
    } } } | ConvertTo-Json -Depth 10 | ConvertFrom-Json)
}

Describe 'P6.3 fixture orchestration, all external operations mocked' {
    BeforeEach {
        $script:P63Root = Join-Path $TestDrive ('fixture project with spaces ' + [Guid]::NewGuid().ToString('N'))
        foreach ($relative in @('docker-compose.yml', 'docker-compose.bootstrap-test.yml', 'docker-compose.bootstrap-clean.yml',
            'docker-compose.fixture.yml', 'scripts/pipeline_fixture.py', '.sandbox/bootstrap-test/compose.env')) {
            $path = Join-Path $script:P63Root $relative
            [IO.Directory]::CreateDirectory((Split-Path -Parent $path)) | Out-Null
            [IO.File]::WriteAllText($path, '# synthetic test input')
        }
        $script:P63Model = New-P63Model $script:P63Root
        $script:P63Calls = New-Object 'System.Collections.Generic.List[object]'
        $script:P63ImageCalls = 0
        $script:P63FailAt = ''
        Mock Resolve-DockerCli { 'mock-docker.exe' }
        Mock Invoke-StartupDocker { 'mock-version' }
        Mock Invoke-StartupCompose {
            param($Context, $Arguments, $FailureMessage, $TimeoutSeconds)
            if ($Arguments -contains '--format') { return ($script:P63Model | ConvertTo-Json -Depth 12 -Compress) }
            return ''
        }
        Mock Assert-ProjectEnvironmentIsolation { }
        Mock Assert-BootstrapServiceHealth { }
        Mock Assert-FixtureResources { }
        Mock Get-FixturePostgresAddress { '172.30.0.2' }
        Mock Get-BootstrapWorkerImage { [pscustomobject]@{ ID = 'sha256:clean'; WorkingDir = '/app' } }
        Mock Test-BootstrapWorkerFreshness { $true }
        Mock Invoke-DockerCommand {
            param($Arguments, $WorkingDirectory, $InputText, $TimeoutSeconds)
            $script:P63Calls.Add([pscustomobject]@{ Arguments = @($Arguments); InputText = $InputText; TimeoutSeconds = $TimeoutSeconds })
            $code = 0
            if ($script:P63FailAt -and $Arguments -contains $script:P63FailAt) { $code = 9 }
            [pscustomobject]@{ ExitCode = $code; StdOut = 'synthetic result'; StdErr = '' }
        }
    }

    It 'fixes test project, clean variant and fixture override while retaining broad sandbox root' {
        $context = New-FixtureContext $script:P63Root
        $context.RuntimeRoot | Should Be (Join-Path $script:P63Root '.sandbox/bootstrap-test')
        $context.FixtureRoot | Should Be (Join-Path $script:P63Root '.sandbox/bootstrap-test/p63-fixture')
        $context.ExpectedProject | Should Be 'nyc-taxi-bootstrap-test'
        $context.ExpectedWorkerImage | Should Be 'nyc-taxi-worker:p6-clean'
        $context.ComposeArguments[-1] | Should Be (Join-Path $script:P63Root 'docker-compose.fixture.yml')
    }

    It 'has the full ordered real CLI sequence and guards around first load and reruns' {
        $names = @(Get-FixturePipelineSteps | ForEach-Object { $_.Name })
        ($names -join ',') | Should Be 'guard-initial,prepare,bronze,silver,gold,files-first,guard-before-load,dimensions,facts,indexes-view,validation,database-first,guard-before-rerun,bronze-rerun,silver-rerun,gold-rerun,files-rerun,guard-before-load-rerun,dimensions-rerun,facts-rerun,indexes-view-rerun,validation-rerun,database-rerun'
    }

    It 'contains no force, recreate, acquisition, lifecycle or host Python command' {
        $steps = @(Get-FixturePipelineSteps)
        $tokens = @($steps | ForEach-Object { @(Get-OptionalProperty $_ 'Arguments' @()) })
        foreach ($bad in @('--force', '--recreate-schema', 'scripts/download_raw_data.py', 'scripts/bootstrap_calendar.py', 'up', 'down', 'build', 'pull')) {
            ($tokens -contains $bad) | Should Be $false
        }
        @($steps | Where-Object { (Get-OptionalProperty $_ 'Name') -in @('facts', 'facts-rerun') } | ForEach-Object { $_.Arguments -join ' ' }) -join '|' |
            Should Match '--start-month 2025-01 --end-month 2025-06'
    }

    It 'defaults to PLAN without directories, worker, freshness probe or data operations' {
        $output = @(& { Invoke-PipelineFixture -ProjectRoot $script:P63Root } *>&1)
        @($output | Where-Object { $_ -is [int] })[-1] | Should Be 0
        Test-Path -LiteralPath (Join-Path $script:P63Root '.sandbox/bootstrap-test/p63-fixture') | Should Be $false
        Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
        Assert-MockCalled Test-BootstrapWorkerFreshness -Times 0 -Exactly -Scope It
        Assert-MockCalled Assert-ProjectEnvironmentIsolation -Times 1 -Exactly -Scope It -ParameterFilter {
            $Context.RuntimeRoot -eq (Join-Path $script:P63Root '.sandbox/bootstrap-test')
        }
    }

    It 'resolves Compose quietly with all profiles rather than hiding worker' {
        $null = Invoke-PipelineFixture -ProjectRoot $script:P63Root
        Assert-MockCalled Invoke-StartupCompose -Times 1 -Exactly -Scope It -ParameterFilter { $Arguments -contains '--quiet' }
        Assert-MockCalled Invoke-StartupCompose -Times 1 -Exactly -Scope It -ParameterFilter { $Arguments -contains '*' -and $Arguments -contains '--format' }
    }

    It 'runs all steps through bounded no-deps worker without source binds or auto-pull' {
        $output = @(& { Invoke-PipelineFixture -ProjectRoot $script:P63Root -Run } *>&1)
        @($output | Where-Object { $_ -is [int] })[-1] | Should Be 0
        $script:P63Calls.Count | Should Be 23
        foreach ($call in $script:P63Calls) {
            foreach ($token in @('run', '--rm', '--no-deps', '-T', '--pull', 'never', 'worker', 'P63_EXPECTED_POSTGRES_IP=172.30.0.2', 'P63_EXPECTED_IMAGE_ID=sha256:clean')) {
                $call.Arguments -contains $token | Should Be $true
            }
            $call.Arguments -contains '-v' | Should Be $false
            $call.TimeoutSeconds | Should Be 600
        }
        $script:P63Calls[0].InputText | Should Be '# synthetic test input'
        $script:P63Calls[0].Arguments[-1] | Should Be 'guard'
        $script:P63Calls[2].Arguments -contains 'scripts/build_bronze.py' | Should Be $true
        Assert-MockCalled Test-BootstrapWorkerFreshness -Times 1 -Exactly -Scope It
        Assert-MockCalled Get-BootstrapWorkerImage -Times 24 -Exactly -Scope It
    }

    It 'fails fast in a middle stage and never starts following steps' {
        $script:P63FailAt = 'scripts/build_silver.py'
        $output = @(& { Invoke-PipelineFixture -ProjectRoot $script:P63Root -Run } *>&1)
        @($output | Where-Object { $_ -is [int] })[-1] | Should Be 1
        $script:P63Calls.Count | Should Be 4
        @($script:P63Calls | Where-Object { $_.Arguments -contains 'scripts/build_gold.py' }).Count | Should Be 0
    }

    It 'stops at the first file oracle failure without loading or resetting the database' {
        $script:P63FailAt = 'files-first'
        $output = @(& { Invoke-PipelineFixture -ProjectRoot $script:P63Root -Run } *>&1)
        @($output | Where-Object { $_ -is [int] })[-1] | Should Be 1
        $script:P63Calls.Count | Should Be 6
        $script:P63Calls[-1].Arguments[-1] | Should Be 'files-first'
        foreach ($call in $script:P63Calls) {
            foreach ($forbidden in @('scripts/load_postgres.py', '--recreate-schema', '--force', 'psql', 'down', 'rm')) {
                $call.Arguments -contains $forbidden | Should Be $false
            }
        }
    }

    It 'fails closed on foreign data detected by the first harness guard' {
        $script:P63FailAt = 'guard'
        $output = @(& { Invoke-PipelineFixture -ProjectRoot $script:P63Root -Run } *>&1)
        @($output | Where-Object { $_ -is [int] })[-1] | Should Be 1
        $script:P63Calls.Count | Should Be 1
        $script:P63Calls[0].Arguments -contains 'prepare' | Should Be $false
    }

    It 'does not begin fixture stages when packaged code is stale' {
        Mock Test-BootstrapWorkerFreshness { $false }
        $output = @(& { Invoke-PipelineFixture -ProjectRoot $script:P63Root -Run } *>&1)
        @($output | Where-Object { $_ -is [int] })[-1] | Should Be 2
        Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
    }

    It 'checks immutable image identity before every stage and stops on a tag change' {
        Mock Get-BootstrapWorkerImage {
            $script:P63ImageCalls++
            $id = 'sha256:clean'
            if ($script:P63ImageCalls -gt 1) { $id = 'sha256:changed' }
            [pscustomobject]@{ ID = $id; WorkingDir = '/app' }
        }
        $output = @(& { Invoke-PipelineFixture -ProjectRoot $script:P63Root -Run } *>&1)
        @($output | Where-Object { $_ -is [int] })[-1] | Should Be 1
        Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
    }

    It 'permits explicit repeated runs without overwriting logs or retaining the lock handle' {
        foreach ($attempt in 1..2) {
            $output = @(& { Invoke-PipelineFixture -ProjectRoot $script:P63Root -Run } *>&1)
            @($output | Where-Object { $_ -is [int] })[-1] | Should Be 0
        }
        $script:P63Calls.Count | Should Be 46
        @(Get-ChildItem -LiteralPath (Join-Path $script:P63Root '.sandbox/bootstrap-test/p63-fixture/logs') -Filter '*-orchestration.json').Count | Should Be 2
    }

    It 'refuses concurrent runs through an exclusive fixture-root lock' {
        $folder = Join-Path $script:P63Root '.sandbox/bootstrap-test/p63-fixture'
        [IO.Directory]::CreateDirectory($folder) | Out-Null
        $hold = New-Object IO.FileStream((Join-Path $folder '.run.lock'), [IO.FileMode]::OpenOrCreate, [IO.FileAccess]::ReadWrite, [IO.FileShare]::None)
        try {
            $output = @(& { Invoke-PipelineFixture -ProjectRoot $script:P63Root -Run } *>&1)
            @($output | Where-Object { $_ -is [int] })[-1] | Should Be 1
            Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
        } finally { $hold.Dispose() }
    }

    It 'rejects resource-limit changes instead of applying them to services' {
        $script:P63Model.services.worker.cpus = 4
        $output = @(& { Invoke-PipelineFixture -ProjectRoot $script:P63Root -Run } *>&1)
        @($output | Where-Object { $_ -is [int] })[-1] | Should Be 1
        Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
    }

    It 'rejects source-code or fifth runtime mounts' {
        $script:P63Model.services.worker.volumes += [pscustomobject]@{ type = 'bind'; source = $script:P63Root; target = '/app' }
        $output = @(& { Invoke-PipelineFixture -ProjectRoot $script:P63Root -Run } *>&1)
        @($output | Where-Object { $_ -is [int] })[-1] | Should Be 1
        Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
    }
}

Describe 'P6.3 fixture runtime guards and private diagnostics' {
    BeforeEach { $script:P63Root = Join-Path $TestDrive ('runtime-guards-' + [Guid]::NewGuid().ToString('N')); [IO.Directory]::CreateDirectory($script:P63Root) | Out-Null }

    It 'rejects a fixture root redirected to full-bootstrap data' {
        $context = New-FixtureContext $script:P63Root
        $context.FixtureRoot = Join-Path $context.RuntimeRoot 'data'
        { Assert-FixtureRuntime $context } | Should Throw
    }

    It 'rejects unknown root entries instead of deleting or overwriting them' {
        $context = New-FixtureContext $script:P63Root
        [IO.Directory]::CreateDirectory($context.FixtureRoot) | Out-Null
        [IO.File]::WriteAllText((Join-Path $context.FixtureRoot 'foreign.txt'), 'keep me')
        { Assert-FixtureRuntime $context } | Should Throw
        [IO.File]::ReadAllText((Join-Path $context.FixtureRoot 'foreign.txt')) | Should Be 'keep me'
    }

    It 'rejects files where a runtime directory is required' {
        $context = New-FixtureContext $script:P63Root
        [IO.Directory]::CreateDirectory($context.FixtureRoot) | Out-Null
        [IO.File]::WriteAllText((Join-Path $context.FixtureRoot 'data'), 'not a directory')
        { Assert-FixtureRuntime $context } | Should Throw
    }

    It 'validates every runtime ancestor with the reparse-point guard' {
        $context = New-FixtureContext $script:P63Root
        Mock Assert-NoEnvironmentReparsePoint { throw 'mock junction refused' }
        { Assert-FixtureRuntime $context } | Should Throw
        Assert-MockCalled Assert-NoEnvironmentReparsePoint -Times 1 -Exactly -Scope It
    }

    It 'redacts URLs and credential assignments from captured streams' {
        $safe = ConvertTo-FixtureSafeLog 'failed postgresql://user:fake@example.test/db password=fake token="not real" https://example.test/path'
        $safe | Should Not Match 'user:fake|password=fake|not real|https://'
        $safe | Should Match '\[REDACTED\]'
        $safe | Should Match '\[URL REDACTED\]'
    }

    It 'never overwrites a pre-existing diagnostics file' {
        $path = Join-Path $script:P63Root 'existing.log'
        Write-FixtureNewText $path 'first'
        { Write-FixtureNewText $path 'second' } | Should Throw
        [IO.File]::ReadAllText($path) | Should Be 'first'
    }

    It 'rejects an already-existing test worker without stopping it' {
        $context = New-FixtureContext $script:P63Root
        $context.Project = 'nyc-taxi-bootstrap-test'
        Mock Get-EnvironmentDockerInventory { [pscustomobject]@{ Containers = @([pscustomobject]@{ Project = 'nyc-taxi-bootstrap-test'; Service = 'worker' }) } }
        { Get-FixturePostgresAddress $context } | Should Throw
    }

    It 'uses the test network address rather than a main PostgreSQL container' {
        $context = New-FixtureContext $script:P63Root
        $context.Project = 'nyc-taxi-bootstrap-test'
        Mock Get-EnvironmentDockerInventory { [pscustomobject]@{ Containers = @(
            [pscustomobject]@{ Project = 'nyc-taxi-bigdata'; Service = 'postgres'; Networks = [pscustomobject]@{} },
            [pscustomobject]@{ Project = 'nyc-taxi-bootstrap-test'; Service = 'postgres'; Networks = [pscustomobject]@{ 'nyc-taxi-bootstrap-test_default' = [pscustomobject]@{ IPAddress = '172.30.0.2' } } }
        ) } }
        Get-FixturePostgresAddress $context | Should Be '172.30.0.2'
    }
}

Describe 'P6.3 bounded resource preflight (mocked capacity, not a benchmark)' {
    BeforeEach {
        $script:P63Root = Join-Path $TestDrive ('resources-' + [Guid]::NewGuid().ToString('N')); [IO.Directory]::CreateDirectory($script:P63Root) | Out-Null
        Mock Get-FixtureHostFreeBytes { 20GB }
        Mock Invoke-StartupDocker { '{"CPUs":2,"MemoryBytes":2147483648}' }
    }
    It 'accepts the stated CPU RAM and disk capacity thresholds' {
        { Assert-FixtureResources (New-FixtureContext $script:P63Root) } | Should Not Throw
    }
    It 'blocks insufficient host disk capacity without starting any worker' {
        Mock Get-FixtureHostFreeBytes { 9GB }
        { Assert-FixtureResources (New-FixtureContext $script:P63Root) } | Should Throw
    }
    It 'blocks insufficient Docker CPU capacity' {
        Mock Invoke-StartupDocker { '{"CPUs":1,"MemoryBytes":8589934592}' }
        { Assert-FixtureResources (New-FixtureContext $script:P63Root) } | Should Throw
    }
    It 'blocks insufficient Docker RAM capacity' {
        Mock Invoke-StartupDocker { '{"CPUs":4,"MemoryBytes":1073741824}' }
        { Assert-FixtureResources (New-FixtureContext $script:P63Root) } | Should Throw
    }
}
