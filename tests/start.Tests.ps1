# Pester 3.4 / Windows PowerShell 5.1 tests. All Docker calls are mocked.
$startupScript = Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts/start.ps1'
. $startupScript

function New-FakeDockerResult {
    param([string]$StdOut = '', [int]$ExitCode = 0, [string]$StdErr = '')
    [pscustomobject]@{ ExitCode = $ExitCode; StdOut = $StdOut; StdErr = $StdErr }
}

function Get-FakeService {
    param([string[]]$Arguments)
    foreach ($service in @('postgres', 'mlflow', 'dashboard')) {
        if (($Arguments -contains $service) -or
            (($Arguments -join ' ') -match "(?:service=|name=\^/nyc-taxi-)$service(?:\$|\s|$)")) {
            return $service
        }
    }
    return $null
}

function Get-FakeContainerJson {
    param([string]$Service)
    $state = 'running'
    if ($script:Scenario.StoppedService -eq $Service -and -not $script:Scenario.Started) {
        $state = 'exited'
    }
    $health = 'healthy'
    if ($script:Scenario.UnhealthyService -eq $Service) { $health = 'unhealthy' }
    if ($script:Scenario.StartingService -eq $Service) { $health = 'starting' }
    $project = 'nyc-taxi-bigdata'
    if ($script:Scenario.WrongProjectService -eq $Service) { $project = 'other-project' }
    $hash = "config-$Service"
    if ($script:Scenario.ConfigDriftService -eq $Service) { $hash = 'old-config' }
    $imageId = "sha256:image-$Service"
    if ($script:Scenario.ImageDriftService -eq $Service) { $imageId = 'sha256:old-image' }
    $container = [pscustomobject]@{
        ID = "$Service-id"; ImageID = $imageId; ImageReference = $script:Images[$Service]
        Status = $state; StartedAt = '2026-10-19T13:21:05.000000000Z'; Health = $health
        Project = $project; Service = $Service; ConfigHash = $hash
    }
    ConvertTo-Json -InputObject $container -Depth 10 -Compress
}

function Invoke-FakeDocker {
    param([string[]]$Arguments, [string]$InputText)
    $script:DockerCalls.Add([pscustomobject]@{ Arguments = @($Arguments); InputText = $InputText })
    $joined = $Arguments -join ' '
    if ($Arguments[0] -eq 'info') {
        if ($script:Scenario.DaemonMissing) {
            return New-FakeDockerResult -ExitCode 1 -StdErr 'daemon unavailable fixture-secret'
        }
        return New-FakeDockerResult -StdOut '29.3.0'
    }
    if ($joined -match '^compose version') {
        if ($script:Scenario.ComposeMissing) { return New-FakeDockerResult -ExitCode 1 }
        return New-FakeDockerResult -StdOut '5.6.0'
    }
    if ($Arguments[0] -eq 'compose') {
        if ($Arguments -contains 'config') {
            if ($Arguments -contains '--quiet') {
                if ($script:Scenario.ConfigInvalid) { return New-FakeDockerResult -ExitCode 1 }
                return New-FakeDockerResult
            }
            if ($Arguments -contains '--format') {
                $services = @{}
                foreach ($service in @('postgres', 'mlflow', 'dashboard')) {
                    $services[$service] = @{ container_name = "nyc-taxi-$service"; image = $script:Images[$service] }
                }
                if ($script:Scenario.AmbiguousImageListing) {
                    $services.dashboard.image = '${UNRESOLVED_DASHBOARD_IMAGE}'
                }
                $model = @{ name = 'nyc-taxi-bigdata'; services = $services }
                return New-FakeDockerResult -StdOut ($model | ConvertTo-Json -Depth 10 -Compress)
            }
            $service = $Arguments[-1]
            if ($Arguments -contains '--images') {
                if ($service -eq 'dashboard' -and
                    ($script:Scenario.DependencyImageListing -or $script:Scenario.AmbiguousImageListing)) {
                    return New-FakeDockerResult -StdOut "postgres:16`nnyc-taxi-dashboard:p4"
                }
                return New-FakeDockerResult -StdOut $script:Images[$service]
            }
            if ($Arguments -contains '--hash') {
                if ($service -eq 'dashboard' -and $script:Scenario.DependencyHashListing) {
                    return New-FakeDockerResult -StdOut "postgres config-postgres`ndashboard config-dashboard"
                }
                return New-FakeDockerResult -StdOut "$service config-$service"
            }
        }
        if ($Arguments -contains 'build') {
            if ($script:Scenario.BuildFails) { return New-FakeDockerResult -ExitCode 1 }
            $script:Scenario.Built = $true
            return New-FakeDockerResult
        }
        if ($Arguments -contains 'up') {
            $script:Scenario.Created = $true
            return New-FakeDockerResult
        }
    }
    if ($joined -match '^container ls') {
        $service = Get-FakeService -Arguments $Arguments
        if (-not $service) { throw "Unrecognized mock container listing: $joined" }
        if ($joined -match 'name=') {
            if ($script:Scenario.ForeignNameService -eq $service) {
                return New-FakeDockerResult -StdOut 'foreign-id'
            }
            return New-FakeDockerResult
        }
        if ($script:Scenario.AbsentService -eq $service -and -not $script:Scenario.Created) {
            return New-FakeDockerResult
        }
        if ($script:Scenario.MultipleService -eq $service) {
            return New-FakeDockerResult -StdOut "$service-id`n$service-extra-id"
        }
        return New-FakeDockerResult -StdOut "$service-id"
    }
    if ($joined -match '^container inspect') {
        $service = $Arguments[-1] -replace '-id$', ''
        return New-FakeDockerResult -StdOut (Get-FakeContainerJson -Service $service)
    }
    if ($joined -match '^image inspect') {
        $image = $Arguments[-1]
        $service = @($script:Images.Keys | Where-Object { $script:Images[$_] -eq $image })[0]
        if ($script:Scenario.MissingImageService -eq $service -and -not $script:Scenario.Built) {
            return New-FakeDockerResult -ExitCode 1
        }
        return New-FakeDockerResult -StdOut "sha256:image-$service"
    }
    if ($joined -match '^container start') {
        $script:Scenario.Started = $true
        return New-FakeDockerResult -StdOut $Arguments[-1]
    }
    if ($Arguments[0] -eq 'exec') {
        if ($script:Scenario.DatabaseError) {
            return New-FakeDockerResult -ExitCode 1 -StdErr 'database fixture-secret'
        }
        if ($script:Scenario.MalformedDatabaseJson) {
            return New-FakeDockerResult -StdOut 'invalid json fixture-secret'
        }
        $data = @{
            schema_present = $true; missing_tables = @(); dimensions_have_rows = $true
            fact_has_rows = $true; log_entries = 6; loaded_partitions = 6; unfinished_partitions = 0
        }
        if ($script:Scenario.EmptyDatabase) {
            $data.fact_has_rows = $false; $data.dimensions_have_rows = $false
            $data.log_entries = 0; $data.loaded_partitions = 0
        }
        if ($script:Scenario.MissingSchema) {
            $data.schema_present = $false
            $data.missing_tables = @('fact_trip', 'dim_location', 'dim_calendar', 'dim_weather', 'etl_load_log')
            $data.fact_has_rows = $false; $data.dimensions_have_rows = $false
            $data.log_entries = 0; $data.loaded_partitions = 0
        }
        return New-FakeDockerResult -StdOut ($data | ConvertTo-Json -Compress)
    }
    throw "Unexpected Docker call (real Docker is never used): $joined"
}

function Invoke-TestStartup {
    param([switch]$Build, [int]$HealthTimeoutSeconds = 120)
    $parameters = @{
        ProjectRoot = $script:FixtureRoot; HealthTimeoutSeconds = $HealthTimeoutSeconds
        CommandTimeoutSeconds = 3; BuildTimeoutSeconds = 30; PollIntervalSeconds = 2
    }
    if ($Build) { $parameters.Build = $true }
    $script:StartupOutput = @(& { Invoke-ProjectStart @parameters } *>&1)
    $integers = @($script:StartupOutput | Where-Object { $_ -is [int] })
    $script:StartupResult = $integers[-1]
    $script:StartupText = $script:StartupOutput | Out-String
}

function Get-LifecycleCalls {
    @($script:DockerCalls | Where-Object {
        ($_.Arguments -contains 'up') -or ($_.Arguments -contains 'start') -or
        ($_.Arguments -contains 'restart') -or ($_.Arguments -contains 'down') -or
        ($_.Arguments -contains 'stop') -or ($_.Arguments -contains 'rm')
    })
}

Describe 'P5 service startup safety (mock Docker only)' {
    BeforeEach {
        $script:FixtureRoot = Join-Path $TestDrive 'project with spaces'
        [System.IO.Directory]::CreateDirectory($script:FixtureRoot) | Out-Null
        [System.IO.File]::WriteAllText((Join-Path $script:FixtureRoot 'docker-compose.yml'), 'services: {}')
        [System.IO.File]::WriteAllText((Join-Path $script:FixtureRoot '.env.example'), 'POSTGRES_PASSWORD=fixture-secret')
        [System.IO.File]::WriteAllText((Join-Path $script:FixtureRoot '.env'), 'POSTGRES_PASSWORD=existing-fixture-secret')
        $script:Scenario = @{
            AbsentService = ''; AdvanceClock = $false; BuildFails = $false; Built = $false
            ComposeMissing = $false; ConfigDriftService = ''; ConfigInvalid = $false
            Created = $false; DaemonMissing = $false; DatabaseError = $false
            EmptyDatabase = $false; ForeignNameService = ''; ImageDriftService = ''
            MalformedDatabaseJson = $false; MissingImageService = ''; MissingSchema = $false
            MultipleService = ''; Started = $false; StartingService = ''; StoppedService = ''
            UnhealthyService = ''; WrongProjectService = ''; DependencyImageListing = $false
            DependencyHashListing = $false; AmbiguousImageListing = $false
        }
        $script:DockerCalls = New-Object 'System.Collections.Generic.List[object]'
        $script:Images = @{
            postgres = 'postgres:16'; mlflow = 'ghcr.io/mlflow/mlflow:v3.5.1'
            dashboard = 'nyc-taxi-dashboard:p4'
        }
        $script:Clock = [DateTime]::UtcNow
        Mock Resolve-DockerCli { 'mock-docker.exe' }
        Mock Invoke-DockerCommand {
            param($Arguments, $WorkingDirectory, $InputText, $TimeoutSeconds)
            if ($WorkingDirectory -ne $script:FixtureRoot) { throw 'Wrong project working directory' }
            Invoke-FakeDocker -Arguments $Arguments -InputText $InputText
        }
        Mock Get-StartupTime {
            if ($script:Scenario.AdvanceClock) { $script:Clock = $script:Clock.AddSeconds(10) }
            $script:Clock
        }
        Mock Pause-StartupPolling { }
    }

    It 'reuses healthy containers without lifecycle commands or builds' {
        Invoke-TestStartup
        $script:StartupResult | Should Be 0
        @(Get-LifecycleCalls).Count | Should Be 0
        @($script:DockerCalls | Where-Object { $_.Arguments -contains 'build' }).Count | Should Be 0
        $script:StartupText | Should Not Match 'fixture-secret'
    }

    It 'can run twice without lifecycle commands' {
        Invoke-TestStartup
        Invoke-TestStartup
        $script:StartupResult | Should Be 0
        @(Get-LifecycleCalls).Count | Should Be 0
    }

    It 'anchors every Compose command to the requested root independently of cwd' {
        Push-Location $TestDrive
        try { Invoke-TestStartup } finally { Pop-Location }
        $script:StartupResult | Should Be 0
        $composeCalls = @($script:DockerCalls | Where-Object {
            $_.Arguments[0] -eq 'compose' -and $_.Arguments[1] -ne 'version'
        })
        foreach ($call in $composeCalls) {
            ($call.Arguments -contains '--project-directory') | Should Be $true
            ($call.Arguments -contains $script:FixtureRoot) | Should Be $true
            ($call.Arguments -contains (Join-Path $script:FixtureRoot 'docker-compose.yml')) | Should Be $true
        }
    }

    It 'preserves an existing env file exactly' {
        $path = Join-Path $script:FixtureRoot '.env'
        $before = [System.IO.File]::ReadAllText($path)
        Invoke-TestStartup
        [System.IO.File]::ReadAllText($path) | Should Be $before
    }

    It 'copies the example only when env is absent without printing its contents' {
        Remove-Item -LiteralPath (Join-Path $script:FixtureRoot '.env')
        Invoke-TestStartup
        [System.IO.File]::ReadAllText((Join-Path $script:FixtureRoot '.env')) | Should Be 'POSTGRES_PASSWORD=fixture-secret'
        $script:StartupText | Should Not Match 'fixture-secret'
        $script:StartupResult | Should Be 0
    }

    It 'fails if neither env nor its example exists' {
        Remove-Item -LiteralPath (Join-Path $script:FixtureRoot '.env')
        Remove-Item -LiteralPath (Join-Path $script:FixtureRoot '.env.example')
        Invoke-TestStartup
        $script:StartupResult | Should Be 1
        @(Get-LifecycleCalls).Count | Should Be 0
    }

    It 'fails clearly when Docker CLI is unavailable' {
        Mock Resolve-DockerCli { $null }
        Invoke-TestStartup
        $script:StartupResult | Should Be 1
        $script:DockerCalls.Count | Should Be 0
    }

    It 'fails without mutations when Docker daemon is unavailable' {
        $script:Scenario.DaemonMissing = $true
        Invoke-TestStartup
        $script:StartupResult | Should Be 1
        @(Get-LifecycleCalls).Count | Should Be 0
        $script:StartupText | Should Not Match 'fixture-secret'
    }

    It 'fails when Compose is unavailable' {
        $script:Scenario.ComposeMissing = $true
        Invoke-TestStartup
        $script:StartupResult | Should Be 1
        @(Get-LifecycleCalls).Count | Should Be 0
    }

    It 'validates configuration quietly before lifecycle actions' {
        $script:Scenario.ConfigInvalid = $true
        Invoke-TestStartup
        $script:StartupResult | Should Be 1
        @(Get-LifecycleCalls).Count | Should Be 0
        @($script:DockerCalls | Where-Object { $_.Arguments -contains '--quiet' }).Count | Should Be 1
    }

    It 'starts exactly an existing stopped container and never recreates it' {
        $script:Scenario.StoppedService = 'mlflow'
        Invoke-TestStartup
        $script:StartupResult | Should Be 0
        $calls = @(Get-LifecycleCalls)
        $calls.Count | Should Be 1
        ($calls[0].Arguments -join ' ') | Should Be 'container start mlflow-id'
    }

    It 'creates only a missing service with no dependency, pull, build, or recreate' {
        $script:Scenario.AbsentService = 'dashboard'
        Invoke-TestStartup
        $script:StartupResult | Should Be 0
        $calls = @(Get-LifecycleCalls)
        $calls.Count | Should Be 1
        foreach ($flag in @('up', '-d', '--no-deps', '--no-recreate', '--no-build', '--pull', 'never')) {
            ($calls[0].Arguments -contains $flag) | Should Be $true
        }
        $calls[0].Arguments[-1] | Should Be 'dashboard'
    }

    It 'reports config drift and preserves all containers' {
        $script:Scenario.ConfigDriftService = 'postgres'
        Invoke-TestStartup
        $script:StartupResult | Should Be 2
        @(Get-LifecycleCalls).Count | Should Be 0
    }

    It 'reports image drift and preserves all containers' {
        $script:Scenario.ImageDriftService = 'dashboard'
        Invoke-TestStartup
        $script:StartupResult | Should Be 2
        @(Get-LifecycleCalls).Count | Should Be 0
    }

    It 'selects the dashboard image when Compose also lists its dependency image' {
        $script:Scenario.DependencyImageListing = $true
        Invoke-TestStartup
        $script:StartupResult | Should Be 0
        $imageCalls = @($script:DockerCalls | Where-Object {
            $_.Arguments[0] -eq 'image' -and $_.Arguments[1] -eq 'inspect'
        })
        $imageCalls.Count | Should Be 3
        $imageCalls[-1].Arguments[-1] | Should Be 'nyc-taxi-dashboard:p4'
        @(Get-LifecycleCalls).Count | Should Be 0
    }

    It 'selects the service hash when Compose also reports dependency hashes' {
        $script:Scenario.DependencyHashListing = $true
        Invoke-TestStartup
        $script:StartupResult | Should Be 0
        @(Get-LifecycleCalls).Count | Should Be 0
    }

    It 'rejects ambiguous image listings without a matching public service image' {
        $script:Scenario.AmbiguousImageListing = $true
        Invoke-TestStartup
        $script:StartupResult | Should Be 1
        @(Get-LifecycleCalls).Count | Should Be 0
    }

    It 'refuses to start an existing stopped container when config differs' {
        $script:Scenario.StoppedService = 'postgres'
        $script:Scenario.ConfigDriftService = 'postgres'
        Invoke-TestStartup
        $script:StartupResult | Should Be 2
        @(Get-LifecycleCalls).Count | Should Be 0
    }

    It 'rejects wrong-project container labels' {
        $script:Scenario.WrongProjectService = 'postgres'
        Invoke-TestStartup
        $script:StartupResult | Should Be 1
        @(Get-LifecycleCalls).Count | Should Be 0
    }

    It 'rejects ambiguous multiple containers for one service' {
        $script:Scenario.MultipleService = 'postgres'
        Invoke-TestStartup
        $script:StartupResult | Should Be 1
        @(Get-LifecycleCalls).Count | Should Be 0
    }

    It 'rejects a foreign container using a required explicit name' {
        $script:Scenario.AbsentService = 'postgres'
        $script:Scenario.ForeignNameService = 'postgres'
        Invoke-TestStartup
        $script:StartupResult | Should Be 1
        @(Get-LifecycleCalls).Count | Should Be 0
    }

    It 'does not create a missing service whose local image is unavailable' {
        $script:Scenario.AbsentService = 'dashboard'
        $script:Scenario.MissingImageService = 'dashboard'
        Invoke-TestStartup
        $script:StartupResult | Should Be 1
        @(Get-LifecycleCalls).Count | Should Be 0
    }

    It 'builds worker and dashboard only when explicitly requested' {
        Invoke-TestStartup -Build
        $script:StartupResult | Should Be 0
        $calls = @($script:DockerCalls | Where-Object { $_.Arguments -contains 'build' })
        $calls.Count | Should Be 1
        ($calls[0].Arguments -join ' ') | Should Match '--profile jobs build worker dashboard$'
        @(Get-LifecycleCalls).Count | Should Be 0
    }

    It 'allows explicit build to provide the missing dashboard image' {
        $script:Scenario.AbsentService = 'dashboard'
        $script:Scenario.MissingImageService = 'dashboard'
        Invoke-TestStartup -Build
        $script:StartupResult | Should Be 0
        $script:Scenario.Built | Should Be $true
        $script:Scenario.Created | Should Be $true
    }

    It 'stops on a failed explicit build without service lifecycle changes' {
        $script:Scenario.BuildFails = $true
        Invoke-TestStartup -Build
        $script:StartupResult | Should Be 1
        @(Get-LifecycleCalls).Count | Should Be 0
    }

    It 'fails when a service is unhealthy without restarting it' {
        $script:Scenario.UnhealthyService = 'postgres'
        Invoke-TestStartup
        $script:StartupResult | Should Be 1
        @(Get-LifecycleCalls).Count | Should Be 0
    }

    It 'times out while health remains starting without lifecycle changes' {
        $script:Scenario.StartingService = 'postgres'
        $script:Scenario.AdvanceClock = $true
        Invoke-TestStartup -HealthTimeoutSeconds 1
        $script:StartupResult | Should Be 1
        @(Get-LifecycleCalls).Count | Should Be 0
    }

    It 'reports empty database metadata without loading data or claiming a full count' {
        $script:Scenario.EmptyDatabase = $true
        Invoke-TestStartup
        $script:StartupResult | Should Be 0
        @(Get-LifecycleCalls).Count | Should Be 0
        $dbCalls = @($script:DockerCalls | Where-Object { $_.Arguments[0] -eq 'exec' })
        $dbCalls.Count | Should Be 1
        $dbCalls[0].InputText | Should Match 'READ ONLY'
        $dbCalls[0].InputText | Should Not Match '(?is)COUNT\s*\(\s*\*\s*\)\s+FROM\s+analytics\.fact_trip'
    }

    It 'treats missing analytics objects as bootstrap required, not a startup mutation' {
        $script:Scenario.MissingSchema = $true
        Invoke-TestStartup
        $script:StartupResult | Should Be 0
        @(Get-LifecycleCalls).Count | Should Be 0
    }

    It 'returns failure for database check errors without leaking diagnostics' {
        $script:Scenario.DatabaseError = $true
        Invoke-TestStartup
        $script:StartupResult | Should Be 1
        $script:StartupText | Should Not Match 'fixture-secret'
        @(Get-LifecycleCalls).Count | Should Be 0
    }

    It 'rejects malformed database metadata without leaking its contents' {
        $script:Scenario.MalformedDatabaseJson = $true
        Invoke-TestStartup
        $script:StartupResult | Should Be 1
        $script:StartupText | Should Not Match 'fixture-secret'
        @(Get-LifecycleCalls).Count | Should Be 0
    }
}
