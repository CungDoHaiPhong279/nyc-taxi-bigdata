# Pester 3.4 / Windows PowerShell 5.1. No real Docker or services are used.
$environmentBootstrapScript = Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts/bootstrap.ps1'
. $environmentBootstrapScript
$environmentPreparationScript = Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts/prepare_bootstrap_test.ps1'
. $environmentPreparationScript

function ConvertTo-EnvironmentFixture {
    param($Value)
    $Value | ConvertTo-Json -Depth 30 -Compress | ConvertFrom-Json
}

function Invoke-EnvironmentPreparationTests {
Describe 'P6.1 preparation smoke coordination (all operations mocked)' {
    BeforeEach {
        $script:EnvironmentFixtureRoot = Join-Path $TestDrive 'prepared project with spaces'
        [IO.Directory]::CreateDirectory($script:EnvironmentFixtureRoot) | Out-Null
        $script:EnvironmentModel = New-EnvironmentFixtureModel
        $script:PreparationOrder = New-Object 'System.Collections.Generic.List[string]'
        $script:PreparationModelArguments = @()
        $script:PreparationScenario = @{ ComposeVersion = '5.6.0'; StartupExit = 0; Fresh = $true; Collision = $false }
        Mock Resolve-DockerCli { 'mock-docker.exe' }
        Mock Invoke-StartupDocker {
            param($Context, $Arguments, $FailureMessage, $InputText, $TimeoutSeconds)
            if ($Arguments -contains 'version') { return $script:PreparationScenario.ComposeVersion }
            return '29.3.0'
        }
        Mock Initialize-BootstrapTestRuntime { $script:PreparationOrder.Add('runtime') }
        Mock Invoke-StartupCompose {
            param($Context, $Arguments, $FailureMessage, $TimeoutSeconds)
            if ($Arguments -contains '--format') {
                $script:PreparationModelArguments = @($Arguments)
                return ($script:EnvironmentModel | ConvertTo-Json -Depth 30 -Compress)
            }
            if ($Arguments -contains 'run') {
                if ($Arguments -notcontains 'scripts/check_worker_services.py' -or $Arguments -notcontains '--no-deps' -or $Arguments -notcontains '--rm') {
                    throw 'Unexpected test worker command'
                }
                $script:PreparationOrder.Add('connectivity')
                return 'Service smoke fixture passed'
            }
            return ''
        }
        Mock Assert-ProjectEnvironmentIsolation {
            $script:PreparationOrder.Add('isolation')
            if ($script:PreparationScenario.Collision) { Stop-Startup 'Mock collision blocked.' }
        }
        Mock Get-BootstrapTestResources { $script:PreparationOrder.Add('resources') }
        Mock Invoke-ProjectStart {
            param($ProjectRoot, $Environment, $Build, $HealthTimeoutSeconds, $CommandTimeoutSeconds, $BuildTimeoutSeconds)
            if ($Environment -ne 'bootstrap-test') { throw 'Preparation selected a production startup' }
            $script:PreparationOrder.Add('startup')
            return $script:PreparationScenario.StartupExit
        }
        Mock Assert-BootstrapServiceHealth { $script:PreparationOrder.Add('health') }
        Mock Test-BootstrapWorkerFreshness {
            $script:PreparationOrder.Add('hash')
            return $script:PreparationScenario.Fresh
        }
        Mock Get-BootstrapWorkerImage { [pscustomobject]@{ ID = 'sha256:test-worker' } }
        Mock Get-EnvironmentDockerInventory {
            [pscustomobject]@{
                Containers = @(
                    [pscustomobject]@{ Name = 'nyc-taxi-bootstrap-test-postgres'; Networks = [pscustomobject]@{ 'nyc-taxi-bootstrap-test_default' = [pscustomobject]@{ IPAddress = '172.30.1.2' } } }
                    [pscustomobject]@{ Name = 'nyc-taxi-bootstrap-test-mlflow'; Networks = [pscustomobject]@{ 'nyc-taxi-bootstrap-test_default' = [pscustomobject]@{ IPAddress = '172.30.1.3' } } }
                ); Volumes = @(); Networks = @()
            }
        }
        Mock Invoke-DockerCommand {
            param($Arguments, $WorkingDirectory, $InputText, $TimeoutSeconds)
            if ($Arguments -notcontains 'run' -or $Arguments -notcontains '--rm' -or $Arguments -notcontains '--no-deps' -or $Arguments -notcontains '-c') {
                throw 'Unexpected native call; real Docker is prohibited in unit tests'
            }
            $script:PreparationOrder.Add('identity')
            [pscustomobject]@{
                ExitCode = 0; StdErr = ''
                StdOut = '{"database":"nyc_taxi_bootstrap_test","test_network_identity":true,"fact_has_rows":false,"any_dimension_has_rows":false,"load_log_entries":0}'
            }
        }
        Mock Invoke-DataBootstrap {
            param($ProjectRoot, $Environment, $Execute, $CommandTimeoutSeconds)
            if ($Environment -ne 'bootstrap-test' -or $Execute) { throw 'Preparation must invoke isolated PLAN only' }
            $script:PreparationOrder.Add('plan')
            return 0
        }
    }

    It 'performs isolation/resources before startup, hashes before smoke and calls PLAN without Execute' {
        $output = @(& { Invoke-PrepareBootstrapTest -ProjectRoot $script:EnvironmentFixtureRoot -BuildWorker } *>&1)
        @($output | Where-Object { $_ -is [int] })[-1] | Should Be 0
        ($script:PreparationOrder -join ',') | Should Be 'runtime,isolation,resources,startup,health,hash,connectivity,identity,plan'
        $script:PreparationModelArguments -contains '--profile' | Should Be $true
        $script:PreparationModelArguments -contains '*' | Should Be $true
        Assert-MockCalled Invoke-ProjectStart -Times 1 -Exactly -Scope It -ParameterFilter { $Environment -eq 'bootstrap-test' -and $Build }
        Assert-MockCalled Invoke-DataBootstrap -Times 1 -Exactly -Scope It -ParameterFilter { $Environment -eq 'bootstrap-test' -and -not $Execute }
        $output | Out-String | Should Match 'fact_has_rows'
        $output | Out-String | Should Match 'false'
    }

    It 'CheckOnly performs preflight but no build, startup, worker or bootstrap' {
        $output = @(& { Invoke-PrepareBootstrapTest -ProjectRoot $script:EnvironmentFixtureRoot -CheckOnly -BuildWorker } *>&1)
        @($output | Where-Object { $_ -is [int] })[-1] | Should Be 0
        ($script:PreparationOrder -join ',') | Should Be 'runtime,isolation,resources'
        $script:PreparationModelArguments -contains '*' | Should Be $true
        Assert-MockCalled Invoke-ProjectStart -Times 0 -Exactly -Scope It
        Assert-MockCalled Test-BootstrapWorkerFreshness -Times 0 -Exactly -Scope It
        Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
        Assert-MockCalled Invoke-DataBootstrap -Times 0 -Exactly -Scope It
    }

    It 'blocks unsupported Compose merge semantics before preparing directories or services' {
        $script:PreparationScenario.ComposeVersion = '2.23.0'
        $output = @(& { Invoke-PrepareBootstrapTest -ProjectRoot $script:EnvironmentFixtureRoot -BuildWorker } *>&1)
        @($output | Where-Object { $_ -is [int] })[-1] | Should Be 1
        $script:PreparationOrder.Count | Should Be 0
    }

    It 'blocks collisions before startup, image build or worker smoke' {
        $script:PreparationScenario.Collision = $true
        $output = @(& { Invoke-PrepareBootstrapTest -ProjectRoot $script:EnvironmentFixtureRoot -BuildWorker } *>&1)
        @($output | Where-Object { $_ -is [int] })[-1] | Should Be 1
        ($script:PreparationOrder -join ',') | Should Be 'runtime,isolation'
        Assert-MockCalled Invoke-ProjectStart -Times 0 -Exactly -Scope It
    }

    It 'returns freshness failure without connectivity smoke or bootstrap PLAN' {
        $script:PreparationScenario.Fresh = $false
        $output = @(& { Invoke-PrepareBootstrapTest -ProjectRoot $script:EnvironmentFixtureRoot -BuildWorker } *>&1)
        @($output | Where-Object { $_ -is [int] })[-1] | Should Be 2
        ($script:PreparationOrder -join ',') | Should Be 'runtime,isolation,resources,startup,health,hash'
        Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
        Assert-MockCalled Invoke-DataBootstrap -Times 0 -Exactly -Scope It
    }

    It 'does not proceed when the selected service startup fails' {
        $script:PreparationScenario.StartupExit = 1
        $output = @(& { Invoke-PrepareBootstrapTest -ProjectRoot $script:EnvironmentFixtureRoot -BuildWorker } *>&1)
        @($output | Where-Object { $_ -is [int] })[-1] | Should Be 1
        ($script:PreparationOrder -join ',') | Should Be 'runtime,isolation,resources,startup'
        Assert-MockCalled Test-BootstrapWorkerFreshness -Times 0 -Exactly -Scope It
    }

    It 'stops before health or worker smoke if the protected P2 base image changes during build' {
        $script:PreparationBaseReads = 0
        Mock Get-BootstrapWorkerImage {
            param($Context, $Image)
            if ($Image -ne 'nyc-taxi-worker:p2') { throw 'Unexpected protected base image reference' }
            $script:PreparationBaseReads++
            [pscustomobject]@{ ID = ('sha256:protected-base-' + $script:PreparationBaseReads) }
        }
        $output = @(& { Invoke-PrepareBootstrapTest -ProjectRoot $script:EnvironmentFixtureRoot -BuildWorker } *>&1)
        @($output | Where-Object { $_ -is [int] })[-1] | Should Be 1
        ($script:PreparationOrder -join ',') | Should Be 'runtime,isolation,resources,startup'
        $script:PreparationBaseReads | Should Be 2
        Assert-MockCalled Assert-BootstrapServiceHealth -Times 0 -Exactly -Scope It
        Assert-MockCalled Test-BootstrapWorkerFreshness -Times 0 -Exactly -Scope It
        Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
        Assert-MockCalled Invoke-DataBootstrap -Times 0 -Exactly -Scope It
    }
}

}

function New-EnvironmentFixtureModel {
    param([switch]$Protected)
    $project = 'nyc-taxi-bootstrap-test'
    $runtimeRoot = Join-Path $script:EnvironmentFixtureRoot '.sandbox/bootstrap-test'
    $containerPrefix = $project
    $workerImage = 'nyc-taxi-worker:p6-test'
    if ($Protected) {
        $project = 'nyc-taxi-bigdata'
        $runtimeRoot = $script:EnvironmentFixtureRoot
        $containerPrefix = 'nyc-taxi'
        $workerImage = 'nyc-taxi-worker:p2'
    }
    $runtimeMounts = @()
    foreach ($directory in @('data', 'logs', 'models', 'reports')) {
        $runtimeMounts += @{
            type = 'bind'; source = (Join-Path $runtimeRoot $directory)
            target = "/app/$directory"; read_only = $false
        }
    }
    $services = @{
        postgres = @{
            image = 'postgres:16'; container_name = "$containerPrefix-postgres"
            environment = @{ POSTGRES_PASSWORD = 'fixture-secret' }
            volumes = @(
                @{ type = 'volume'; source = 'postgres-data'; target = '/var/lib/postgresql/data' }
                @{ type = 'bind'; source = (Join-Path $script:EnvironmentFixtureRoot 'sql'); target = '/docker-entrypoint-initdb.d'; read_only = $true }
            )
            networks = @{ default = $null }
        }
        mlflow = @{
            image = 'ghcr.io/mlflow/mlflow:v3.5.1'; container_name = "$containerPrefix-mlflow"
            volumes = @(@{ type = 'volume'; source = 'mlflow-data'; target = '/mlflow' })
            networks = @{ default = $null }
        }
        dashboard = @{
            image = 'nyc-taxi-dashboard:p4'; container_name = "$containerPrefix-dashboard"
            networks = @{ default = $null }
        }
        worker = @{
            image = $workerImage; profiles = @('jobs'); volumes = $runtimeMounts
            build = @{ context = $script:EnvironmentFixtureRoot; dockerfile = 'Dockerfile.spark' }
            environment = @{
                POSTGRES_HOST = 'postgres'; POSTGRES_PORT = '5432'
                POSTGRES_DB = 'nyc_taxi'; POSTGRES_USER = 'nyc_taxi'
                POSTGRES_PASSWORD = 'fixture-secret'; MLFLOW_TRACKING_URI = 'http://mlflow:5000'
            }
            networks = @{ default = $null }
        }
    }
    if ($Protected) {
        $services.postgres.ports = @(@{ target = 5432; published = '5432'; protocol = 'tcp' })
        $services.mlflow.ports = @(@{ target = 5000; published = '5000'; protocol = 'tcp' })
        $services.dashboard.ports = @(@{ target = 8501; published = '8501'; host_ip = '127.0.0.1'; protocol = 'tcp' })
    }
    ConvertTo-EnvironmentFixture @{
        name = $project; services = $services
        networks = @{ default = @{ name = "${project}_default" } }
        volumes = @{
            'postgres-data' = @{ name = "${project}_postgres-data" }
            'mlflow-data' = @{ name = "${project}_mlflow-data" }
        }
    }
}

function Add-EnvironmentFixtureProperty {
    param($Object, [string]$Name, $Value)
    $Object | Add-Member -MemberType NoteProperty -Name $Name -Value $Value -Force
}

Describe 'P6.1 environment selection and isolation (mock metadata only)' {
    BeforeEach {
        $script:EnvironmentFixtureRoot = Join-Path $TestDrive 'project with spaces'
        $script:EnvironmentRuntimeRoot = Join-Path $script:EnvironmentFixtureRoot '.sandbox/bootstrap-test'
        foreach ($directory in @('', 'sql', 'data', 'logs', 'models', 'reports', '.sandbox/bootstrap-test')) {
            [IO.Directory]::CreateDirectory((Join-Path $script:EnvironmentFixtureRoot $directory)) | Out-Null
        }
        foreach ($directory in @('data', 'logs', 'models', 'reports')) {
            [IO.Directory]::CreateDirectory((Join-Path $script:EnvironmentRuntimeRoot $directory)) | Out-Null
        }
        foreach ($file in @('docker-compose.yml', 'docker-compose.bootstrap-test.yml', '.env')) {
            [IO.File]::WriteAllText((Join-Path $script:EnvironmentFixtureRoot $file), '# fixture only')
        }
        [IO.File]::WriteAllText((Join-Path $script:EnvironmentRuntimeRoot 'compose.env'), 'POSTGRES_PASSWORD=fixture-secret')
        $script:EnvironmentModel = New-EnvironmentFixtureModel
        $script:ProtectedEnvironmentModel = New-EnvironmentFixtureModel -Protected
        $script:EnvironmentInventory = [pscustomobject]@{
            Containers = @(); Volumes = @(); Networks = @()
        }
        Mock Get-ProtectedEnvironmentModel { $script:ProtectedEnvironmentModel }
        Mock Get-EnvironmentDockerInventory { $script:EnvironmentInventory }
        Mock Invoke-DockerCommand { throw 'Unexpected external Docker call in isolation unit test' }
        $script:TestEnvironmentContext = New-ProjectEnvironmentContext -ProjectRoot $script:EnvironmentFixtureRoot -Environment bootstrap-test
    }

    It 'keeps the default environment rooted at the project and existing env file' {
        $context = New-ProjectEnvironmentContext -ProjectRoot $script:EnvironmentFixtureRoot
        $context.Environment | Should Be 'current'
        $context.Root | Should Be $script:EnvironmentFixtureRoot
        $context.RuntimeRoot | Should Be $script:EnvironmentFixtureRoot
        $context.EnvPath | Should Be (Join-Path $script:EnvironmentFixtureRoot '.env')
        @($context.ComposeArguments) -contains '-p' | Should Be $false
        @($context.ComposeArguments) -contains '--env-file' | Should Be $false
        @($context.ComposeArguments) -contains (Join-Path $script:EnvironmentFixtureRoot 'docker-compose.bootstrap-test.yml') | Should Be $false
    }

    It 'selects an explicit project, env file and two Compose files for bootstrap-test' {
        $context = $script:TestEnvironmentContext
        $context.Environment | Should Be 'bootstrap-test'
        $context.RuntimeRoot | Should Be $script:EnvironmentRuntimeRoot
        $context.EnvPath | Should Be (Join-Path $script:EnvironmentRuntimeRoot 'compose.env')
        $arguments = @($context.ComposeArguments)
        $arguments -contains 'nyc-taxi-bootstrap-test' | Should Be $true
        $arguments -contains '--env-file' | Should Be $true
        $arguments -contains $context.EnvPath | Should Be $true
        $arguments -contains (Join-Path $script:EnvironmentFixtureRoot 'docker-compose.yml') | Should Be $true
        $arguments -contains (Join-Path $script:EnvironmentFixtureRoot 'docker-compose.bootstrap-test.yml') | Should Be $true
        @($context.Services).Count | Should Be 2
        @($context.Services) -contains 'postgres' | Should Be $true
        @($context.Services) -contains 'mlflow' | Should Be $true
        @($context.Services) -contains 'dashboard' | Should Be $false
        @($context.BuildServices).Count | Should Be 1
        @($context.BuildServices)[0] | Should Be 'worker'
    }

    It 'preserves custom timeout values in the shared context' {
        $context = New-ProjectEnvironmentContext -ProjectRoot $script:EnvironmentFixtureRoot -Environment bootstrap-test -CommandTimeoutSeconds 23 -HealthTimeoutSeconds 117 -PollIntervalSeconds 3
        $context.CommandTimeoutSeconds | Should Be 23
        $context.HealthTimeoutSeconds | Should Be 117
        $context.PollIntervalSeconds | Should Be 3
    }

    It 'does not create runtime files or change the production env in the context constructor' {
        $root = Join-Path $TestDrive 'unprepared project'
        [IO.Directory]::CreateDirectory($root) | Out-Null
        $null = New-ProjectEnvironmentContext -ProjectRoot $root -Environment bootstrap-test
        Test-Path -LiteralPath (Join-Path $root '.sandbox') | Should Be $false
    }

    It 'allows the isolated model and the read-only public SQL bind' {
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Not Throw
    }

    It 'leaves current-environment behavior unchanged without fetching protected metadata' {
        $context = New-ProjectEnvironmentContext -ProjectRoot $script:EnvironmentFixtureRoot -Environment current
        Assert-ProjectEnvironmentIsolation -Context $context -Model $script:ProtectedEnvironmentModel
        Assert-MockCalled Get-ProtectedEnvironmentModel -Times 0 -Exactly -Scope It
        Assert-MockCalled Get-EnvironmentDockerInventory -Times 0 -Exactly -Scope It
    }

    It 'rejects the production project name' {
        $script:EnvironmentModel.name = $script:ProtectedEnvironmentModel.name
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects a production container name' {
        $script:EnvironmentModel.services.postgres.container_name = 'nyc-taxi-postgres'
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects a non-production published host port too: this test environment publishes none' {
        Add-EnvironmentFixtureProperty $script:EnvironmentModel.services.postgres 'ports' @([pscustomobject]@{ target = 5432; published = '15432'; protocol = 'tcp' })
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects the production published PostgreSQL port' {
        Add-EnvironmentFixtureProperty $script:EnvironmentModel.services.postgres 'ports' @([pscustomobject]@{ target = 5432; published = '5432'; protocol = 'tcp' })
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects a production named volume' {
        $script:EnvironmentModel.volumes.'postgres-data'.name = 'nyc-taxi-bigdata_postgres-data'
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects external volumes' {
        Add-EnvironmentFixtureProperty $script:EnvironmentModel.volumes.'postgres-data' 'external' $true
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects volume driver options that could bind a production directory' {
        Add-EnvironmentFixtureProperty $script:EnvironmentModel.volumes.'postgres-data' 'driver_opts' ([pscustomobject]@{ type = 'none'; o = 'bind'; device = (Join-Path $script:EnvironmentFixtureRoot 'data') })
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects a writable production data bind' {
        $script:EnvironmentModel.services.worker.volumes[0].source = Join-Path $script:EnvironmentFixtureRoot 'data'
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects a writable production reports bind and protects the PBIX' {
        $script:EnvironmentModel.services.worker.volumes[3].source = Join-Path $script:EnvironmentFixtureRoot 'reports'
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects a writable path outside the dedicated runtime root' {
        $script:EnvironmentModel.services.worker.volumes[0].source = Join-Path $TestDrive 'elsewhere'
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects a runtime-root prefix sibling rather than using an unsafe string prefix test' {
        $script:EnvironmentModel.services.worker.volumes[0].source = $script:EnvironmentRuntimeRoot + '-outside/data'
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects writable paths that escape through dot-dot segments' {
        $script:EnvironmentModel.services.worker.volumes[0].source = Join-Path $script:EnvironmentRuntimeRoot '../../data'
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects a runtime ancestor that is a junction or symlink' {
        Mock Get-Item { [pscustomobject]@{ Attributes = [IO.FileAttributes]::ReparsePoint } } -ParameterFilter { $LiteralPath -eq $script:EnvironmentRuntimeRoot }
        try {
            { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
            Assert-MockCalled Get-Item -Times 1 -Exactly -Scope It -ParameterFilter { $LiteralPath -eq $script:EnvironmentRuntimeRoot }
        } finally {
            # Pester 3 keeps a filtered mock for the enclosing Describe scope.
            Mock Get-Item { [pscustomobject]@{ Attributes = [IO.File]::GetAttributes($LiteralPath) } } -ParameterFilter { $LiteralPath -eq $script:EnvironmentRuntimeRoot }
        }
    }

    It 'checks writable binds on every service, not only worker' {
        Add-EnvironmentFixtureProperty $script:EnvironmentModel.services.dashboard 'volumes' @([pscustomobject]@{ type = 'bind'; source = (Join-Path $script:EnvironmentFixtureRoot 'reports'); target = '/unsafe'; read_only = $false })
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects a Docker socket bind even when marked read-only' {
        $script:EnvironmentModel.services.worker.volumes += [pscustomobject]@{ type = 'bind'; source = '/var/run/docker.sock'; target = '/var/run/docker.sock'; read_only = $true }
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects the current worker image tag' {
        $script:EnvironmentModel.services.worker.image = 'nyc-taxi-worker:p2'
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects additional worker build tags that could overwrite the production image' {
        Add-EnvironmentFixtureProperty $script:EnvironmentModel.services.worker.build 'tags' @('nyc-taxi-worker:p2')
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'allows the exact dependency-preserving bootstrap-test refresh Dockerfile' {
        $script:EnvironmentModel.services.worker.build.dockerfile = 'Dockerfile.bootstrap-test'
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Not Throw
    }

    It 'rejects arbitrary worker Dockerfiles outside the reviewed allowlist' {
        $script:EnvironmentModel.services.worker.build.dockerfile = 'Dockerfile.unreviewed'
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects a shared production network name' {
        $script:EnvironmentModel.networks.default.name = 'nyc-taxi-bigdata_default'
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects external networks' {
        Add-EnvironmentFixtureProperty $script:EnvironmentModel.networks.default 'external' $true
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects privileged services' {
        Add-EnvironmentFixtureProperty $script:EnvironmentModel.services.worker 'privileged' $true
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects host networking' {
        Add-EnvironmentFixtureProperty $script:EnvironmentModel.services.worker 'network_mode' 'host'
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects volumes_from container inheritance' {
        Add-EnvironmentFixtureProperty $script:EnvironmentModel.services.worker 'volumes_from' @('container:nyc-taxi-postgres:rw')
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects a test container name already owned by a foreign live project' {
        $script:EnvironmentInventory.Containers = @([pscustomobject]@{
            ID = 'foreign-container'; Name = 'nyc-taxi-bootstrap-test-postgres'
            Project = 'unrelated-project'; Service = 'postgres'; Mounts = @(); Networks = @{}; Ports = @()
        })
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects an existing test volume owned by a foreign project' {
        $script:EnvironmentInventory.Volumes = @([pscustomobject]@{
            Name = 'nyc-taxi-bootstrap-test_postgres-data'; Project = 'unrelated-project'
        })
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects an existing test-labeled volume whose driver options redirect storage' {
        $script:EnvironmentInventory.Volumes = @([pscustomobject]@{
            Name = 'nyc-taxi-bootstrap-test_postgres-data'; Project = 'nyc-taxi-bootstrap-test'
            Driver = 'local'; HasOptions = $true
        })
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects an existing test network owned by a foreign project' {
        $script:EnvironmentInventory.Networks = @([pscustomobject]@{
            Name = 'nyc-taxi-bootstrap-test_default'; Project = 'unrelated-project'
        })
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'allows an owned bridge network with only verified Docker IP-family flags' {
        $script:EnvironmentInventory.Networks = @([pscustomobject]@{
            Name = 'nyc-taxi-bootstrap-test_default'; Project = 'nyc-taxi-bootstrap-test'
            Driver = 'bridge'; HasOptions = $true; OnlyIPFamilyOptions = $true
        })
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Not Throw
    }

    It 'rejects network options when the strict IP-family allowlist proof is false or missing' {
        foreach ($proof in @($false, $null)) {
            $network = [pscustomobject]@{
                Name = 'nyc-taxi-bootstrap-test_default'; Project = 'nyc-taxi-bootstrap-test'
                Driver = 'bridge'; HasOptions = $true
            }
            if ($null -ne $proof) { Add-EnvironmentFixtureProperty $network 'OnlyIPFamilyOptions' $proof }
            $script:EnvironmentInventory.Networks = @($network)
            { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
        }
    }

    It 'rejects a runtime bind already mounted writable by a protected container' {
        $script:EnvironmentInventory.Containers = @([pscustomobject]@{
            ID = 'production-postgres'; Name = 'nyc-taxi-postgres'; Project = 'nyc-taxi-bigdata'; Service = 'postgres'
            Mounts = @([pscustomobject]@{
                Type = 'bind'; Source = (Join-Path $script:EnvironmentRuntimeRoot 'data')
                Destination = '/production-data'; RW = $true; Name = ''
            }); Networks = @{}; Ports = @()
        })
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'allows existing resources owned by the isolated test project' {
        $script:EnvironmentInventory.Containers = @([pscustomobject]@{
            ID = 'test-postgres'; Name = 'nyc-taxi-bootstrap-test-postgres'; Project = 'nyc-taxi-bootstrap-test'; Service = 'postgres'
            Mounts = @([pscustomobject]@{ Type = 'volume'; Source = '/var/lib/docker/volumes/test'; Destination = '/var/lib/postgresql/data'; RW = $true; Name = 'nyc-taxi-bootstrap-test_postgres-data' })
            Networks = [pscustomobject]@{ 'nyc-taxi-bootstrap-test_default' = [pscustomobject]@{} }; Ports = $null
        })
        $script:EnvironmentInventory.Volumes = @([pscustomobject]@{ Name = 'nyc-taxi-bootstrap-test_postgres-data'; Project = 'nyc-taxi-bootstrap-test' })
        $script:EnvironmentInventory.Networks = @([pscustomobject]@{ Name = 'nyc-taxi-bootstrap-test_default'; Project = 'nyc-taxi-bootstrap-test' })
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Not Throw
    }

    It 'rejects existing test-labeled containers whose writable binds point to production' {
        $script:EnvironmentInventory.Containers = @([pscustomobject]@{
            ID = 'test-worker'; Name = 'test-worker'; Project = 'nyc-taxi-bootstrap-test'; Service = 'worker'
            Mounts = @([pscustomobject]@{ Type = 'bind'; Source = (Join-Path $script:EnvironmentFixtureRoot 'reports'); Destination = '/app/reports'; RW = $true; Name = '' })
            Networks = [pscustomobject]@{ 'nyc-taxi-bootstrap-test_default' = [pscustomobject]@{} }; Ports = $null
        })
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'rejects an existing test-labeled container with published host ports' {
        $script:EnvironmentInventory.Containers = @([pscustomobject]@{
            ID = 'test-postgres'; Name = 'nyc-taxi-bootstrap-test-postgres'; Project = 'nyc-taxi-bootstrap-test'; Service = 'postgres'
            Mounts = @(); Networks = [pscustomobject]@{ 'nyc-taxi-bootstrap-test_default' = [pscustomobject]@{} }
            Ports = [pscustomobject]@{ '5432/tcp' = @([pscustomobject]@{ HostIp = '0.0.0.0'; HostPort = '15432' }) }
        })
        { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel } | Should Throw
    }

    It 'does not print protected secrets on rejection' {
        $script:EnvironmentModel.name = 'nyc-taxi-bigdata'
        $output = @(& {
            try { Assert-ProjectEnvironmentIsolation -Context $script:TestEnvironmentContext -Model $script:EnvironmentModel }
            catch { $_.Exception.Message }
        } *>&1) | Out-String
        $output | Should Not Match 'fixture-secret'
    }

    Context 'bootstrap.ps1 PLAN integration with the selected environment' {
        BeforeEach {
            foreach ($directory in @('scripts', 'config')) {
                [IO.Directory]::CreateDirectory((Join-Path $script:EnvironmentFixtureRoot $directory)) | Out-Null
            }
            $required = @('requirements.txt', 'pyproject.toml', 'config/config.yaml', 'scripts/create_calendar.py', 'scripts/start.ps1')
            $required += @(Get-BootstrapPlan) | ForEach-Object { $_.Arguments[2] } | Select-Object -Unique
            foreach ($relative in $required) {
                [IO.File]::WriteAllText((Join-Path $script:EnvironmentFixtureRoot $relative), '# fixture only')
            }
            $script:EnvironmentBootstrapCalls = New-Object 'System.Collections.Generic.List[object]'
            Mock Resolve-DockerCli { 'mock-docker.exe' }
            Mock Invoke-StartupDocker { 'fixture-version' }
            Mock Invoke-StartupCompose {
                param($Context, $Arguments, $FailureMessage, $TimeoutSeconds)
                $script:EnvironmentBootstrapCalls.Add([pscustomobject]@{ Arguments = @($Arguments); Context = $Context })
                if ($Arguments -contains '--format') {
                    return ($script:EnvironmentModel | ConvertTo-Json -Depth 30 -Compress)
                }
                return ''
            }
            Mock Assert-BootstrapServiceHealth { }
            Mock Get-BootstrapWorkerImage { [pscustomobject]@{ ID = 'sha256:fixture-image' } }
            Mock Test-BootstrapWorkerFreshness { throw 'A PLAN must never launch the worker freshness probe' }
        }

        It 'plans with isolated paths, selected project and dedicated worker image without launching a worker' {
            $output = @(& { Invoke-DataBootstrap -ProjectRoot $script:EnvironmentFixtureRoot -Environment bootstrap-test } *>&1)
            @($output | Where-Object { $_ -is [int] })[-1] | Should Be 0
            $output | Out-String | Should Match 'environment bootstrap-test'
            $output | Out-String | Should Match 'nyc-taxi-bootstrap-test'
            $output | Out-String | Should Not Match 'fixture-secret'
            $modelCall = @($script:EnvironmentBootstrapCalls | Where-Object { $_.Arguments -contains '--format' })[0]
            $modelCall.Arguments -contains '--no-interpolate' | Should Be $false
            $modelCall.Arguments -contains '--profile' | Should Be $true
            $modelCall.Arguments -contains '*' | Should Be $true
            $modelCall.Context.RuntimeRoot | Should Be $script:EnvironmentRuntimeRoot
            $modelCall.Context.EnvPath | Should Be (Join-Path $script:EnvironmentRuntimeRoot 'compose.env')
            Assert-MockCalled Get-BootstrapWorkerImage -Times 1 -Exactly -Scope It -ParameterFilter { $Image -eq 'nyc-taxi-worker:p6-test' }
            Assert-MockCalled Test-BootstrapWorkerFreshness -Times 0 -Exactly -Scope It
            Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
        }

        It 'keeps the default PLAN on the current runtime paths and non-interpolated metadata' {
            $script:EnvironmentModel = $script:ProtectedEnvironmentModel
            $output = @(& { Invoke-DataBootstrap -ProjectRoot $script:EnvironmentFixtureRoot } *>&1)
            @($output | Where-Object { $_ -is [int] })[-1] | Should Be 0
            $modelCall = @($script:EnvironmentBootstrapCalls | Where-Object { $_.Arguments -contains '--format' })[0]
            $modelCall.Arguments -contains '--no-interpolate' | Should Be $true
            $modelCall.Context.RuntimeRoot | Should Be $script:EnvironmentFixtureRoot
            $modelCall.Context.EnvPath | Should Be (Join-Path $script:EnvironmentFixtureRoot '.env')
            Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
        }

        It 'fails without creating test configuration or falling back to the production env' {
            [IO.File]::Delete((Join-Path $script:EnvironmentRuntimeRoot 'compose.env'))
            $output = @(& { Invoke-DataBootstrap -ProjectRoot $script:EnvironmentFixtureRoot -Environment bootstrap-test } *>&1)
            @($output | Where-Object { $_ -is [int] })[-1] | Should Be 1
            Test-Path -LiteralPath (Join-Path $script:EnvironmentRuntimeRoot 'compose.env') | Should Be $false
            $script:EnvironmentBootstrapCalls.Count | Should Be 0
            Assert-MockCalled Get-BootstrapWorkerImage -Times 0 -Exactly -Scope It
        }

        It 'rejects a production reports collision before worker image or service health checks' {
            $script:EnvironmentModel.services.worker.volumes[3].source = Join-Path $script:EnvironmentFixtureRoot 'reports'
            $output = @(& { Invoke-DataBootstrap -ProjectRoot $script:EnvironmentFixtureRoot -Environment bootstrap-test } *>&1)
            @($output | Where-Object { $_ -is [int] })[-1] | Should Be 1
            Assert-MockCalled Get-BootstrapWorkerImage -Times 0 -Exactly -Scope It
            Assert-MockCalled Assert-BootstrapServiceHealth -Times 0 -Exactly -Scope It
            Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
        }
    }

    Context 'start.ps1 integration with the selected test environment' {
        BeforeEach {
            $script:EnvironmentStartupCalls = New-Object 'System.Collections.Generic.List[object]'
            Mock Resolve-DockerCli { 'mock-docker.exe' }
            Mock Assert-ProjectEnvironmentIsolation { }
            Mock Invoke-StartupDocker { 'fixture-version' }
            Mock Invoke-StartupCompose {
                param($Context, $Arguments, $FailureMessage, $TimeoutSeconds)
                $script:EnvironmentStartupCalls.Add([pscustomobject]@{ Arguments = @($Arguments); Context = $Context })
                if ($Arguments -contains 'config' -and $Arguments -contains '--format') {
                    return ($script:EnvironmentModel | ConvertTo-Json -Depth 30 -Compress)
                }
                if ($Arguments -contains '--images') { return $script:EnvironmentModel.services.($Arguments[-1]).image }
                if ($Arguments -contains '--hash') { return ($Arguments[-1] + ' fixture-config-hash') }
                return ''
            }
            Mock Invoke-DockerCommand {
                param($Arguments, $WorkingDirectory, $InputText, $TimeoutSeconds)
                if ($Arguments[0] -eq 'image' -and $Arguments[1] -eq 'inspect') {
                    return [pscustomobject]@{ ExitCode = 0; StdOut = 'sha256:fixture-image'; StdErr = '' }
                }
                throw 'Unexpected native Docker call in start environment integration test'
            }
            Mock Get-ServiceContainer {
                param($Context, $Service, $ContainerName)
                [pscustomobject]@{
                    ID = "$Service-fixture-id"; ImageID = 'sha256:fixture-image'
                    ImageReference = $script:EnvironmentModel.services.$Service.image
                    Status = 'running'; StartedAt = '2026-10-20T00:00:00Z'; Health = 'healthy'
                    Project = 'nyc-taxi-bootstrap-test'; Service = $Service; ConfigHash = 'fixture-config-hash'
                }
            }
            Mock Wait-ServiceHealth {
                param($Context, $Service, $Id)
                [pscustomobject]@{
                    ID = "$Service-fixture-id"; Service = $Service
                    StartedAt = '2026-10-20T00:00:00Z'; Health = 'healthy'
                }
            }
            Mock Test-DatabasePresence { }
        }

        It 'starts only test PostgreSQL and MLflow while building only the separate worker' {
            $output = @(& { Invoke-ProjectStart -ProjectRoot $script:EnvironmentFixtureRoot -Environment bootstrap-test -Build } *>&1)
            @($output | Where-Object { $_ -is [int] })[-1] | Should Be 0
            $buildCalls = @($script:EnvironmentStartupCalls | Where-Object { $_.Arguments -contains 'build' })
            $buildCalls.Count | Should Be 1
            $buildCalls[0].Arguments[-1] | Should Be 'worker'
            @($script:EnvironmentStartupCalls | Where-Object { $_.Arguments -contains 'dashboard' }).Count | Should Be 0
            @($script:EnvironmentStartupCalls | Where-Object { $_.Arguments -contains 'up' }).Count | Should Be 0
            Assert-MockCalled Get-ServiceContainer -Times 1 -Exactly -Scope It -ParameterFilter { $Service -eq 'postgres' }
            Assert-MockCalled Get-ServiceContainer -Times 1 -Exactly -Scope It -ParameterFilter { $Service -eq 'mlflow' }
            Assert-MockCalled Get-ServiceContainer -Times 0 -Exactly -Scope It -ParameterFilter { $Service -eq 'dashboard' }
            $output | Out-String | Should Not Match 'fixture-secret'
        }

        It 'uses a resolved test model while retaining secret output withholding' {
            $null = Invoke-ProjectStart -ProjectRoot $script:EnvironmentFixtureRoot -Environment bootstrap-test
            $modelCalls = @($script:EnvironmentStartupCalls | Where-Object { $_.Arguments -contains '--format' })
            $modelCalls.Count | Should Be 1
            $modelCalls[0].Arguments -contains '--no-interpolate' | Should Be $false
            $modelCalls[0].Arguments -contains '--profile' | Should Be $true
            $modelCalls[0].Arguments -contains '*' | Should Be $true
            Assert-MockCalled Assert-ProjectEnvironmentIsolation -Times 1 -Exactly -Scope It
        }

        It 'does not copy the real env if dedicated test configuration is missing' {
            [IO.File]::Delete((Join-Path $script:EnvironmentRuntimeRoot 'compose.env'))
            $productionEnv = [IO.File]::ReadAllText((Join-Path $script:EnvironmentFixtureRoot '.env'))
            $output = @(& { Invoke-ProjectStart -ProjectRoot $script:EnvironmentFixtureRoot -Environment bootstrap-test -Build } *>&1)
            @($output | Where-Object { $_ -is [int] })[-1] | Should Be 1
            Test-Path -LiteralPath (Join-Path $script:EnvironmentRuntimeRoot 'compose.env') | Should Be $false
            [IO.File]::ReadAllText((Join-Path $script:EnvironmentFixtureRoot '.env')) | Should Be $productionEnv
            @($script:EnvironmentStartupCalls | Where-Object { $_.Arguments -contains 'build' -or $_.Arguments -contains 'up' }).Count | Should Be 0
        }

        It 'stops on an isolation error before any build or lifecycle operation' {
            Mock Assert-ProjectEnvironmentIsolation { Stop-Startup 'Fixture collision blocked.' }
            $output = @(& { Invoke-ProjectStart -ProjectRoot $script:EnvironmentFixtureRoot -Environment bootstrap-test -Build } *>&1)
            @($output | Where-Object { $_ -is [int] })[-1] | Should Be 1
            @($script:EnvironmentStartupCalls | Where-Object { $_.Arguments -contains 'build' -or $_.Arguments -contains 'up' -or $_.Arguments -contains 'start' }).Count | Should Be 0
            Assert-MockCalled Get-ServiceContainer -Times 0 -Exactly -Scope It
        }
    }
}

Invoke-EnvironmentPreparationTests
