# Pester 3.4 / Windows PowerShell 5.1. No real Docker, services or Python.
$p62ProjectRoot = Split-Path -Parent $PSScriptRoot
. (Join-Path $p62ProjectRoot 'scripts/bootstrap.ps1')

function New-P62FixtureModel {
    param([string]$Root, [switch]$Protected, [switch]$Clean)
    $project = 'nyc-taxi-bootstrap-test'
    $runtime = Join-Path $Root '.sandbox/bootstrap-test'
    $image = 'nyc-taxi-worker:p6-test'
    $dockerfile = 'Dockerfile.bootstrap-test'
    if ($Clean) { $image = 'nyc-taxi-worker:p6-clean'; $dockerfile = 'Dockerfile.spark' }
    if ($Protected) { $project = 'nyc-taxi-bigdata'; $runtime = $Root; $image = 'nyc-taxi-worker:p2' }
    $binds = @()
    foreach ($directory in @('data', 'logs', 'models', 'reports')) {
        $binds += @{ type = 'bind'; source = (Join-Path $runtime $directory); target = "/app/$directory"; read_only = $false }
    }
    $model = @{
        name = $project
        services = @{
            postgres = @{
                image = 'postgres:16'; container_name = "$project-postgres"
                volumes = @(
                    @{ type = 'volume'; source = 'postgres-data'; target = '/var/lib/postgresql/data' }
                    @{ type = 'bind'; source = (Join-Path $Root 'sql'); target = '/docker-entrypoint-initdb.d'; read_only = $true }
                )
                networks = @{ default = $null }
            }
            mlflow = @{
                image = 'ghcr.io/mlflow/mlflow:v3.5.1'; container_name = "$project-mlflow"
                volumes = @(@{ type = 'volume'; source = 'mlflow-data'; target = '/mlflow' })
                networks = @{ default = $null }
            }
            worker = @{
                image = $image; profiles = @('jobs'); volumes = $binds
                build = @{ context = $Root; dockerfile = $dockerfile }
                environment = @{ POSTGRES_HOST = 'postgres'; POSTGRES_PORT = '5432'; MLFLOW_TRACKING_URI = 'http://mlflow:5000' }
                networks = @{ default = $null }
            }
        }
        volumes = @{
            'postgres-data' = @{ name = "${project}_postgres-data" }
            'mlflow-data' = @{ name = "${project}_mlflow-data" }
        }
        networks = @{ default = @{ name = "${project}_default" } }
    }
    $model | ConvertTo-Json -Depth 20 -Compress | ConvertFrom-Json
}

Describe 'P6.2 clean worker selection and safety (metadata mocked)' {
    BeforeEach {
        $script:P62Root = Join-Path $TestDrive 'clean build project with spaces'
        foreach ($directory in @('', 'sql', 'data', 'logs', 'models', 'reports', '.sandbox/bootstrap-test')) {
            [IO.Directory]::CreateDirectory((Join-Path $script:P62Root $directory)) | Out-Null
        }
        foreach ($directory in @('data', 'logs', 'models', 'reports')) {
            [IO.Directory]::CreateDirectory((Join-Path $script:P62Root ".sandbox/bootstrap-test/$directory")) | Out-Null
        }
        foreach ($relative in @('docker-compose.yml', 'docker-compose.bootstrap-test.yml', 'docker-compose.bootstrap-clean.yml',
            '.env', '.sandbox/bootstrap-test/compose.env', 'requirements.txt', 'pyproject.toml', 'config/config.yaml',
            'scripts/start.ps1', 'scripts/create_calendar.py', 'scripts/bootstrap_calendar.py', 'scripts/download_raw_data.py',
            'scripts/build_bronze.py', 'scripts/build_silver.py', 'scripts/build_gold.py', 'scripts/load_postgres.py')) {
            $path = Join-Path $script:P62Root $relative
            [IO.Directory]::CreateDirectory((Split-Path -Parent $path)) | Out-Null
            [IO.File]::WriteAllText($path, '# test fixture only')
        }
        $script:P62Model = New-P62FixtureModel -Root $script:P62Root -Clean
        $script:P62Protected = New-P62FixtureModel -Root $script:P62Root -Protected
        $script:P62ComposeCalls = New-Object 'System.Collections.Generic.List[object]'
        Mock Get-ProtectedEnvironmentModel { $script:P62Protected }
        Mock Get-EnvironmentDockerInventory { [pscustomobject]@{ Containers = @(); Volumes = @(); Networks = @() } }
        Mock Resolve-DockerCli { 'mock-docker.exe' }
        Mock Invoke-StartupDocker { 'fixture-version' }
        Mock Invoke-StartupCompose {
            param($Context, $Arguments, $FailureMessage, $TimeoutSeconds)
            $script:P62ComposeCalls.Add([pscustomobject]@{ Context = $Context; Arguments = @($Arguments) })
            if ($Arguments -contains '--format') { return ($script:P62Model | ConvertTo-Json -Depth 20 -Compress) }
            return ''
        }
        Mock Assert-BootstrapServiceHealth { }
        Mock Get-BootstrapWorkerImage { [pscustomobject]@{ ID = 'sha256:clean-worker'; WorkingDir = '/app' } }
        Mock Test-BootstrapWorkerFreshness { throw 'PLAN must not run a worker hash probe' }
        Mock Invoke-DockerCommand { throw 'Unexpected external call: all Docker operations must be mocked' }
    }

    It 'preserves the current environment default without the clean override' {
        $context = New-ProjectEnvironmentContext -ProjectRoot $script:P62Root
        $context.Environment | Should Be 'current'
        $context.RuntimeRoot | Should Be $script:P62Root
        $context.EnvPath | Should Be (Join-Path $script:P62Root '.env')
        @($context.ComposeArguments) -contains (Join-Path $script:P62Root 'docker-compose.bootstrap-clean.yml') | Should Be $false
    }

    It 'preserves the existing refresh variant for bootstrap-test by default' {
        $context = New-ProjectEnvironmentContext -ProjectRoot $script:P62Root -Environment bootstrap-test
        $context.ExpectedWorkerImage | Should Be 'nyc-taxi-worker:p6-test'
        @($context.ComposeArguments | Where-Object { $_ -eq '--file' }).Count | Should Be 2
        @($context.ComposeArguments) -contains (Join-Path $script:P62Root 'docker-compose.bootstrap-clean.yml') | Should Be $false
    }

    It 'selects only the clean image and appends its override after the isolation override' {
        $context = New-ProjectEnvironmentContext -ProjectRoot $script:P62Root -Environment bootstrap-test -WorkerVariant clean
        $context.ExpectedWorkerImage | Should Be 'nyc-taxi-worker:p6-clean'
        $context.ExpectedProject | Should Be 'nyc-taxi-bootstrap-test'
        $context.RuntimeRoot | Should Be (Join-Path $script:P62Root '.sandbox/bootstrap-test')
        $arguments = @($context.ComposeArguments)
        @($arguments | Where-Object { $_ -eq '--file' }).Count | Should Be 3
        $clean = [Array]::IndexOf($arguments, (Join-Path $script:P62Root 'docker-compose.bootstrap-clean.yml'))
        $isolation = [Array]::IndexOf($arguments, (Join-Path $script:P62Root 'docker-compose.bootstrap-test.yml'))
        ($clean -gt $isolation) | Should Be $true
    }

    It 'rejects the clean variant for the current environment before calling Docker' {
        { New-ProjectEnvironmentContext -ProjectRoot $script:P62Root -WorkerVariant clean } | Should Throw
        Assert-MockCalled Invoke-StartupDocker -Times 0 -Exactly -Scope It
        Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
    }

    It 'allows clean image metadata only with the standard worker Dockerfile' {
        $context = New-ProjectEnvironmentContext -ProjectRoot $script:P62Root -Environment bootstrap-test -WorkerVariant clean
        { Assert-ProjectEnvironmentIsolation -Context $context -Model $script:P62Model } | Should Not Throw
    }

    It 'rejects the old refresh tag when the clean variant was selected' {
        $context = New-ProjectEnvironmentContext -ProjectRoot $script:P62Root -Environment bootstrap-test -WorkerVariant clean
        $script:P62Model.services.worker.image = 'nyc-taxi-worker:p6-test'
        { Assert-ProjectEnvironmentIsolation -Context $context -Model $script:P62Model } | Should Throw
    }

    It 'rejects the clean tag when the default refresh variant was selected' {
        $context = New-ProjectEnvironmentContext -ProjectRoot $script:P62Root -Environment bootstrap-test
        { Assert-ProjectEnvironmentIsolation -Context $context -Model $script:P62Model } | Should Throw
    }

    It 'rejects a P2-derived Dockerfile for the clean tag' {
        $context = New-ProjectEnvironmentContext -ProjectRoot $script:P62Root -Environment bootstrap-test -WorkerVariant clean
        $script:P62Model.services.worker.build.dockerfile = 'Dockerfile.bootstrap-test'
        { Assert-ProjectEnvironmentIsolation -Context $context -Model $script:P62Model } | Should Throw
    }

    It 'rejects extra build tags that could overwrite protected images' {
        $context = New-ProjectEnvironmentContext -ProjectRoot $script:P62Root -Environment bootstrap-test -WorkerVariant clean
        $script:P62Model.services.worker.build | Add-Member -MemberType NoteProperty -Name tags -Value @('nyc-taxi-worker:p2')
        { Assert-ProjectEnvironmentIsolation -Context $context -Model $script:P62Model } | Should Throw
    }

    It 'rejects an inline Dockerfile for the clean variant' {
        $context = New-ProjectEnvironmentContext -ProjectRoot $script:P62Root -Environment bootstrap-test -WorkerVariant clean
        $script:P62Model.services.worker.build | Add-Member -MemberType NoteProperty -Name dockerfile_inline -Value 'FROM nyc-taxi-worker:p2'
        { Assert-ProjectEnvironmentIsolation -Context $context -Model $script:P62Model } | Should Throw
    }

    It 'runs clean PLAN with all profiles included but without worker, bootstrap or lifecycle commands' {
        $output = @(& { Invoke-DataBootstrap -ProjectRoot $script:P62Root -Environment bootstrap-test -WorkerVariant clean } *>&1)
        @($output | Where-Object { $_ -is [int] })[-1] | Should Be 0
        $modelCall = @($script:P62ComposeCalls | Where-Object { $_.Arguments -contains '--format' })[0]
        $modelCall.Arguments -contains '--profile' | Should Be $true
        $modelCall.Arguments -contains '*' | Should Be $true
        $modelCall.Context.ExpectedWorkerImage | Should Be 'nyc-taxi-worker:p6-clean'
        @($script:P62ComposeCalls | Where-Object { @($_.Arguments | Where-Object { $_ -in @('run', 'up', 'start', 'build', 'pull', 'down') }).Count -gt 0 }).Count | Should Be 0
        Assert-MockCalled Get-BootstrapWorkerImage -Times 1 -Exactly -Scope It -ParameterFilter { $Image -eq 'nyc-taxi-worker:p6-clean' }
        Assert-MockCalled Test-BootstrapWorkerFreshness -Times 0 -Exactly -Scope It
        Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
    }

    It 'keeps refresh PLAN using its existing image without a clean override' {
        $script:P62Model = New-P62FixtureModel -Root $script:P62Root
        $output = @(& { Invoke-DataBootstrap -ProjectRoot $script:P62Root -Environment bootstrap-test } *>&1)
        @($output | Where-Object { $_ -is [int] })[-1] | Should Be 0
        Assert-MockCalled Get-BootstrapWorkerImage -Times 1 -Exactly -Scope It -ParameterFilter { $Image -eq 'nyc-taxi-worker:p6-test' }
        Assert-MockCalled Test-BootstrapWorkerFreshness -Times 0 -Exactly -Scope It
        Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
    }

    It 'returns a nonzero status for current plus clean without contacting Docker' {
        $output = @(& { Invoke-DataBootstrap -ProjectRoot $script:P62Root -WorkerVariant clean } *>&1)
        @($output | Where-Object { $_ -is [int] })[-1] | Should Be 1
        Assert-MockCalled Resolve-DockerCli -Times 0 -Exactly -Scope It
        Assert-MockCalled Invoke-StartupDocker -Times 0 -Exactly -Scope It
        Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
    }
}

Describe 'P6.2 standard Dockerfile dependency-download contract (static checks)' {
    BeforeEach {
        $script:P62Dockerfile = [IO.File]::ReadAllText((Join-Path $p62ProjectRoot 'Dockerfile.spark'))
    }

    It 'builds from the supported Python base rather than any existing worker runtime' {
        $script:P62Dockerfile | Should Match '(?m)^FROM python:3\.11-slim-bookworm\s*$'
        $script:P62Dockerfile | Should Not Match '(?im)^FROM\s+nyc-taxi-worker:'
    }

    It 'keeps a fresh pip download with finite socket timeout and connection retries' {
        $script:P62Dockerfile | Should Match '--no-cache-dir'
        $script:P62Dockerfile | Should Match '--timeout(?:=|\s+)120'
        $script:P62Dockerfile | Should Match '--retries(?:=|\s+)5'
        $script:P62Dockerfile | Should Match '-r requirements\.txt'
    }

    It 'bounds the whole-install retry loop and preserves TLS and package hash validation' {
        $script:P62Dockerfile | Should Match '(?is)(?:attempt[^\r\n]*(?:-ge|>=)[^\r\n]*3|for\s+\w+\s+in\s+1\s+2\s+3)'
        $script:P62Dockerfile | Should Not Match '(?i)--trusted-host|PIP_TRUSTED_HOST|--ignore-hash|--skip-hash'
        $script:P62Dockerfile | Should Match 'pip check'
    }

    It 'preserves the three observed P2 transitive dependency versions through narrow constraints' {
        $path = Join-Path $p62ProjectRoot 'requirements-worker-constraints.txt'
        Test-Path -LiteralPath $path -PathType Leaf | Should Be $true
        $pins = @([IO.File]::ReadAllLines($path) | ForEach-Object { $_.Trim() } | Where-Object { $_ -and -not $_.StartsWith('#') })
        $pins.Count | Should Be 3
        $pins -contains 'fastapi==0.148.0' | Should Be $true
        $pins -contains 'certifi==2026.10.18' | Should Be $true
        $pins -contains 'wcwidth==0.9.2' | Should Be $true
    }

    It 'copies and applies the dependency constraints during the standard full build' {
        $script:P62Dockerfile | Should Match '(?m)^COPY\s+[^\r\n]*requirements-worker-constraints\.txt[^\r\n]*\./\s*$'
        $script:P62Dockerfile | Should Match '(?:-c|--constraint)(?:=|\s+)requirements-worker-constraints\.txt'
    }
}
