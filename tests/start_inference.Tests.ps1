# Pester 4.10.1 / Windows PowerShell 5.1. Native Docker and artifact reads are mocked.
. (Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts/start.ps1')

function New-InferenceDockerResult {
    param([string]$Text = '', [int]$ExitCode = 0)
    [pscustomobject]@{ ExitCode = $ExitCode; StdOut = $Text; StdErr = 'private-error-sentinel' }
}

function Get-InferenceFakeService {
    param([string[]]$Arguments)
    foreach ($service in @('postgres', 'mlflow', 'fare-inference', 'dashboard')) {
        if (($Arguments -contains $service) -or
            (($Arguments -join ' ') -match ('(?:service=|name=\^/nyc-taxi-)' + [regex]::Escape($service) + '(?:\$|\s|$)'))) {
            return $service
        }
    }
    return $null
}

function Get-InferenceFakeSnapshot {
    param([string]$Service)
    $status = 'running'
    if ($script:InferenceScenario.StoppedService -eq $Service -and -not $script:InferenceScenario.Started.ContainsKey($Service)) { $status = 'exited' }
    $health = 'healthy'
    if ($script:InferenceScenario.HealthStarting -eq $Service) { $health = 'starting' }
    $hash = 'config-' + $Service
    if ($script:InferenceScenario.ConfigDrift -eq $Service) { $hash = 'old-config' }
    $imageId = 'sha256:image-' + $Service
    if ($script:InferenceScenario.ImageDrift -eq $Service) { $imageId = 'sha256:old-image' }
    [pscustomobject]@{
        ID = ($Service + '-id'); ImageID = $imageId; ImageReference = $script:InferenceImages[$Service]
        Status = $status; StartedAt = '2026-10-23T01:00:00Z'; Health = $health
        Project = 'nyc-taxi-bigdata'; Service = $Service; ConfigHash = $hash
    } | ConvertTo-Json -Compress
}

function Invoke-InferenceFakeDocker {
    param([string[]]$Arguments, [string]$InputText, [int]$TimeoutSeconds)
    $script:InferenceCalls.Add([pscustomobject]@{ Arguments = @($Arguments); InputText = $InputText; TimeoutSeconds = $TimeoutSeconds })
    if ($Arguments[0] -eq 'info') { return New-InferenceDockerResult 'mock-engine' }
    if ($Arguments[0] -eq 'compose' -and $Arguments[1] -eq 'version') { return New-InferenceDockerResult 'mock-compose' }
    if ($Arguments[0] -eq 'compose') {
        if ($Arguments -contains 'config') {
            if ($Arguments -contains '--quiet') { return New-InferenceDockerResult }
            if ($Arguments -contains '--format') {
                $services = @{}
                foreach ($service in @('postgres', 'mlflow', 'fare-inference', 'dashboard')) {
                    $services[$service] = @{ container_name = ('nyc-taxi-' + $service); image = $script:InferenceImages[$service] }
                }
                return New-InferenceDockerResult (@{ name = 'nyc-taxi-bigdata'; services = $services } | ConvertTo-Json -Depth 10 -Compress)
            }
            $service = $Arguments[-1]
            if ($Arguments -contains '--images') { return New-InferenceDockerResult $script:InferenceImages[$service] }
            if ($Arguments -contains '--hash') { return New-InferenceDockerResult ($service + ' config-' + $service) }
        }
        if ($Arguments -contains 'up') { $script:InferenceScenario.Created[$Arguments[-1]] = $true; return New-InferenceDockerResult }
        if ($Arguments -contains 'build') { $script:InferenceScenario.Built = $true; return New-InferenceDockerResult }
    }
    if ($Arguments[0] -eq 'container' -and $Arguments[1] -eq 'ls') {
        $service = Get-InferenceFakeService $Arguments
        if (-not $service) { throw 'Unknown container listing in offline mock.' }
        if (($Arguments -join ' ') -match 'name=') { return New-InferenceDockerResult }
        if ($script:InferenceScenario.AbsentService -eq $service -and -not $script:InferenceScenario.Created.ContainsKey($service)) { return New-InferenceDockerResult }
        return New-InferenceDockerResult ($service + '-id')
    }
    if ($Arguments[0] -eq 'container' -and $Arguments[1] -eq 'inspect') {
        return New-InferenceDockerResult (Get-InferenceFakeSnapshot ($Arguments[-1] -replace '-id$', ''))
    }
    if ($Arguments[0] -eq 'container' -and $Arguments[1] -eq 'start') {
        $script:InferenceScenario.Started[$Arguments[-1] -replace '-id$', ''] = $true
        return New-InferenceDockerResult $Arguments[-1]
    }
    if ($Arguments[0] -eq 'image' -and $Arguments[1] -eq 'inspect') {
        $service = @($script:InferenceImages.Keys | Where-Object { $script:InferenceImages[$_] -eq $Arguments[-1] })[0]
        if ($script:InferenceScenario.MissingImage -eq $service -and -not $script:InferenceScenario.Built) { return New-InferenceDockerResult -ExitCode 1 }
        return New-InferenceDockerResult ('sha256:image-' + $service)
    }
    if ($Arguments[0] -eq 'exec') {
        if ($Arguments -contains 'fare-inference-id') {
            if ($script:InferenceScenario.ReadinessCommandFailure) { return New-InferenceDockerResult -ExitCode 124 }
            if ($script:InferenceScenario.ReadinessMalformed) { return New-InferenceDockerResult 'private-readiness-sentinel' }
            return New-InferenceDockerResult ($script:InferenceReadiness | ConvertTo-Json -Compress)
        }
        return New-InferenceDockerResult (@{ schema_present = $true; missing_tables = @(); fact_has_rows = $true
            dimensions_have_rows = $true; loaded_partitions = 6; log_entries = 6; unfinished_partitions = 0 } | ConvertTo-Json -Compress)
    }
    throw 'Unexpected command in offline Docker mock; no real Docker is permitted.'
}

function Invoke-InferenceTestStart {
    param([switch]$Default, [switch]$Build, [string]$Environment = 'current', [int]$HealthTimeoutSeconds = 120)
    $arguments = @{ ProjectRoot = $script:InferenceRoot; Environment = $Environment; HealthTimeoutSeconds = $HealthTimeoutSeconds
        CommandTimeoutSeconds = 3; BuildTimeoutSeconds = 60; PollIntervalSeconds = 1 }
    if (-not $Default) { $arguments.WithInference = $true }
    if ($Build) { $arguments.Build = $true }
    $script:InferenceOutput = @(& { Invoke-ProjectStart @arguments } *>&1)
    $script:InferenceExit = @($script:InferenceOutput | Where-Object { $_ -is [int] })[-1]
    $script:InferenceText = $script:InferenceOutput | Out-String
}

function Get-InferenceLifecycleCalls {
    @($script:InferenceCalls | Where-Object {
        @($_.Arguments | Where-Object { $_ -in @('up', 'start', 'restart', 'stop', 'down', 'rm') }).Count -gt 0
    })
}

Describe 'ML P5 optional inference startup (mock-only)' {
    BeforeEach {
        $script:InferenceRoot = Join-Path $TestDrive 'project with spaces'
        [IO.Directory]::CreateDirectory($script:InferenceRoot) | Out-Null
        foreach ($file in @('docker-compose.yml', 'docker-compose.inference.yml')) {
            [IO.File]::WriteAllText((Join-Path $script:InferenceRoot $file), 'services: {}')
        }
        [IO.File]::WriteAllText((Join-Path $script:InferenceRoot '.env'), 'PLACEHOLDER=private-env-sentinel')
        [IO.File]::WriteAllText((Join-Path $script:InferenceRoot '.env.example'), 'PLACEHOLDER=public-example-sentinel')
        $script:InferenceImages = @{ postgres = 'postgres:16'; mlflow = 'ghcr.io/mlflow/mlflow:v3.5.1'
            dashboard = 'nyc-taxi-dashboard:inference'; 'fare-inference' = 'nyc-taxi-inference:ml-p4' }
        $script:InferenceScenario = @{ StoppedService = ''; HealthStarting = ''; ConfigDrift = ''; ImageDrift = ''; AbsentService = ''
            Started = @{}; Created = @{}; Built = $false; MissingImage = ''; ReadinessCommandFailure = $false; ReadinessMalformed = $false; AdvanceClock = $false }
        $script:InferenceReadiness = @{ ready = $true; status = 'ready'; model_load_count = 1; spark_application_id = 'local-offline'
            training_run_id = 'd2772f1904d7472490a73b0ac1c0c5d4'; evaluation_run_id = '58d5008f0ed048f58599a859877ae336'
            model_sha256 = '759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee'; startup_seconds = 1.0 }
        $script:InferenceCalls = New-Object 'System.Collections.Generic.List[object]'
        $script:InferenceClock = [DateTime]::UtcNow
        Mock Assert-FareArtifacts { [pscustomobject]@{ Verified = $true
            ModelSHA256 = '759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee'
            TrainingRunId = 'd2772f1904d7472490a73b0ac1c0c5d4'; EvaluationRunId = '58d5008f0ed048f58599a859877ae336' } }
        Mock Resolve-DockerCli { 'mock-docker.exe' }
        Mock Invoke-DockerCommand {
            param($Arguments, $WorkingDirectory, $InputText, $TimeoutSeconds)
            if ($WorkingDirectory -ne $script:InferenceRoot) { throw 'Project cwd was not pinned.' }
            Invoke-InferenceFakeDocker -Arguments $Arguments -InputText $InputText -TimeoutSeconds $TimeoutSeconds
        }
        Mock Get-StartupTime {
            if ($script:InferenceScenario.AdvanceClock) { $script:InferenceClock = $script:InferenceClock.AddSeconds(10) }
            $script:InferenceClock
        }
        Mock Pause-StartupPolling { }
    }

    It 'keeps ordinary startup independent of inference artifacts and override' {
        Invoke-InferenceTestStart -Default
        $script:InferenceExit | Should Be 0
        Assert-MockCalled Assert-FareArtifacts -Times 0 -Exactly -Scope It
        @($script:InferenceCalls | Where-Object { $_.Arguments -contains (Join-Path $script:InferenceRoot 'docker-compose.inference.yml') }).Count | Should Be 0
        @($script:InferenceCalls | Where-Object { $_.Arguments -contains 'fare-inference-id' }).Count | Should Be 0
    }

    It 'uses the inference override and fixed project on every selected Compose command' {
        Push-Location $TestDrive
        try { Invoke-InferenceTestStart } finally { Pop-Location }
        $script:InferenceExit | Should Be 0
        Assert-MockCalled Assert-FareArtifacts -Times 1 -Exactly -Scope It -ParameterFilter { $ProjectRoot -eq $script:InferenceRoot }
        $compose = @($script:InferenceCalls | Where-Object { $_.Arguments[0] -eq 'compose' -and $_.Arguments[1] -ne 'version' })
        foreach ($call in $compose) {
            $call.Arguments -contains (Join-Path $script:InferenceRoot 'docker-compose.yml') | Should Be $true
            $call.Arguments -contains (Join-Path $script:InferenceRoot 'docker-compose.inference.yml') | Should Be $true
            $call.Arguments -contains '--project-name' | Should Be $true
            $call.Arguments -contains 'nyc-taxi-bigdata' | Should Be $true
        }
    }

    It 'reuses four healthy services twice without any lifecycle or default build' {
        Invoke-InferenceTestStart
        Invoke-InferenceTestStart
        $script:InferenceExit | Should Be 0
        @(Get-InferenceLifecycleCalls).Count | Should Be 0
        @($script:InferenceCalls | Where-Object { $_.Arguments -contains 'build' }).Count | Should Be 0
        Assert-MockCalled Assert-FareArtifacts -Times 2 -Exactly -Scope It
        $script:InferenceText | Should Not Match 'private-.*sentinel'
    }

    It 'waits for PostgreSQL MLflow inference then dashboard in that order' {
        Invoke-InferenceTestStart
        $script:InferenceExit | Should Be 0
        $inspects = @($script:InferenceCalls | Where-Object { $_.Arguments[0] -eq 'container' -and $_.Arguments[1] -eq 'inspect' })
        ($inspects[-4..-1] | ForEach-Object { $_.Arguments[-1] }) -join ',' | Should Be 'postgres-id,mlflow-id,fare-inference-id,dashboard-id'
    }

    It 'starts only an existing stopped inference container without recreating it' {
        $script:InferenceScenario.StoppedService = 'fare-inference'
        Invoke-InferenceTestStart
        $script:InferenceExit | Should Be 0
        $calls = @(Get-InferenceLifecycleCalls)
        $calls.Count | Should Be 1
        $calls[0].Arguments -join ' ' | Should Be 'container start fare-inference-id'
    }

    It 'creates only missing inference with explicit no-deps no-recreate no-build pull-never flags' {
        $script:InferenceScenario.AbsentService = 'fare-inference'
        Invoke-InferenceTestStart
        $script:InferenceExit | Should Be 0
        $calls = @(Get-InferenceLifecycleCalls)
        $calls.Count | Should Be 1
        foreach ($flag in @('up', '-d', '--no-deps', '--no-recreate', '--no-build', '--pull', 'never')) {
            $calls[0].Arguments -contains $flag | Should Be $true
        }
        $calls[0].Arguments[-1] | Should Be 'fare-inference'
    }

    It 'builds the three application images only on explicit Build with the inference context' {
        Invoke-InferenceTestStart -Build
        $script:InferenceExit | Should Be 0
        $calls = @($script:InferenceCalls | Where-Object { $_.Arguments -contains 'build' })
        $calls.Count | Should Be 1
        $calls[0].Arguments -join ' ' | Should Match 'build worker dashboard fare-inference$'
        @(Get-InferenceLifecycleCalls).Count | Should Be 0
    }

    It 'rejects missing artifacts before env creation build or lifecycle' {
        [IO.File]::Delete((Join-Path $script:InferenceRoot '.env'))
        Mock Assert-FareArtifacts { throw 'Missing artifact private-artifact-sentinel' }
        Invoke-InferenceTestStart -Build
        $script:InferenceExit | Should Be 1
        Test-Path -LiteralPath (Join-Path $script:InferenceRoot '.env') | Should Be $false
        @(Get-InferenceLifecycleCalls).Count | Should Be 0
        @($script:InferenceCalls | Where-Object { $_.Arguments -contains 'build' }).Count | Should Be 0
        $script:InferenceText | Should Not Match 'private-artifact-sentinel'
    }

    It 'rejects wrong artifact hashes without overwriting configuration or building' {
        $before = [IO.File]::ReadAllBytes((Join-Path $script:InferenceRoot '.env'))
        Mock Assert-FareArtifacts { throw 'Wrong full hash private-artifact-sentinel' }
        Invoke-InferenceTestStart -Build
        $script:InferenceExit | Should Be 1
        [Convert]::ToBase64String([IO.File]::ReadAllBytes((Join-Path $script:InferenceRoot '.env'))) | Should Be ([Convert]::ToBase64String($before))
        @(Get-InferenceLifecycleCalls).Count | Should Be 0
        @($script:InferenceCalls | Where-Object { $_.Arguments -contains 'build' }).Count | Should Be 0
    }

    It 'refuses a missing local inference image instead of pulling or building implicitly' {
        $script:InferenceScenario.AbsentService = 'fare-inference'
        $script:InferenceScenario.MissingImage = 'fare-inference'
        Invoke-InferenceTestStart
        $script:InferenceExit | Should Be 1
        @(Get-InferenceLifecycleCalls).Count | Should Be 0
        @($script:InferenceCalls | Where-Object { $_.Arguments -contains 'build' -or $_.Arguments -contains 'pull' }).Count | Should Be 0
    }

    It 'reports running inference config drift as exit2 and prints the override update command without executing it' {
        $script:InferenceScenario.ConfigDrift = 'fare-inference'
        Invoke-InferenceTestStart
        $script:InferenceExit | Should Be 2
        @(Get-InferenceLifecycleCalls).Count | Should Be 0
        $script:InferenceText | Should Match 'docker-compose\.inference\.yml'
        $script:InferenceText | Should Match 'up -d --no-deps'
    }

    It 'reports dashboard image drift without silently recreating the dashboard' {
        $script:InferenceScenario.ImageDrift = 'dashboard'
        Invoke-InferenceTestStart
        $script:InferenceExit | Should Be 2
        @(Get-InferenceLifecycleCalls).Count | Should Be 0
        $script:InferenceText | Should Match 'docker-compose\.inference\.yml'
    }

    It 'prints the exact package-test layers project and env file as one safely quoted command' {
        $root = 'C:\fixtures\demo''s $cash `token; path with spaces'
        $compose = @('compose', '--project-directory', $root,
            '--file', (Join-Path $root 'docker-compose.yml'),
            '--file', (Join-Path $root 'docker-compose.inference.yml'),
            '--file', (Join-Path $root 'docker-compose.package.yml'),
            '--file', (Join-Path $root 'docker-compose.package-test.yml'),
            '--env-file', (Join-Path $root '.sandbox/package-test/compose.env'),
            '--project-name', 'nyc-taxi-package-test')
        $context = [pscustomobject]@{ Root = $root; IsPackage = $true; ComposeArguments = $compose }
        $command = Get-InferenceUpdateCommand -Context $context -Service dashboard
        $tokens = $null
        $errors = $null
        $parsed = [Management.Automation.Language.Parser]::ParseInput($command, [ref]$tokens, [ref]$errors)
        @($errors).Count | Should Be 0
        @($parsed.EndBlock.Statements).Count | Should Be 1
        @($parsed.EndBlock.Statements[0].PipelineElements).Count | Should Be 1
        $actual = @($parsed.EndBlock.Statements[0].PipelineElements[0].CommandElements | ForEach-Object { $_.Value })
        $expected = @('docker') + $compose + @('up', '-d', '--no-deps', '--no-build', '--pull', 'never', 'dashboard')
        ($actual -join "`n") | Should BeExactly ($expected -join "`n")
        $script:InferenceCalls.Count | Should Be 0
    }

    It 'keeps the current-package update command on its selected package layers without borrowing test or legacy paths' {
        $compose = @('compose', '--project-directory', $script:InferenceRoot,
            '-f', (Join-Path $script:InferenceRoot 'docker-compose.yml'),
            '-f', (Join-Path $script:InferenceRoot 'docker-compose.inference.yml'),
            '-f', (Join-Path $script:InferenceRoot 'docker-compose.package.yml'),
            '--env-file', (Join-Path $script:InferenceRoot '.env'), '--project-name', 'nyc-taxi-bigdata')
        $context = [pscustomobject]@{ Root = $script:InferenceRoot; IsPackage = $true; ComposeArguments = $compose }
        $command = Get-InferenceUpdateCommand -Context $context -Service fare-inference
        $tokens = $null
        $errors = $null
        $parsed = [Management.Automation.Language.Parser]::ParseInput($command, [ref]$tokens, [ref]$errors)
        @($errors).Count | Should Be 0
        $actual = @($parsed.EndBlock.Statements[0].PipelineElements[0].CommandElements | ForEach-Object { $_.Value })
        ($actual -join "`n") | Should BeExactly ((@('docker') + $compose + @('up', '-d', '--no-deps', '--no-build', '--pull', 'never', 'fare-inference')) -join "`n")
        $command | Should Not Match 'nyc-taxi-package-test'
        $script:InferenceCalls.Count | Should Be 0
    }

    It 'does not start a stopped inference container whose configuration drifted' {
        $script:InferenceScenario.StoppedService = 'fare-inference'
        $script:InferenceScenario.ConfigDrift = 'fare-inference'
        Invoke-InferenceTestStart
        $script:InferenceExit | Should Be 2
        @(Get-InferenceLifecycleCalls).Count | Should Be 0
    }

    It 'refuses inference selection in bootstrap-test without native calls' {
        Invoke-InferenceTestStart -Environment bootstrap-test
        $script:InferenceExit | Should Be 1
        $script:InferenceCalls.Count | Should Be 0
    }

    It 'rejects a short model hash despite healthy Docker health state' {
        $script:InferenceReadiness.model_sha256 = $script:InferenceReadiness.model_sha256.Substring(0, 16)
        Invoke-InferenceTestStart
        $script:InferenceExit | Should Be 1
        @(Get-InferenceLifecycleCalls).Count | Should Be 0
    }

    It 'rejects the wrong training identity despite healthy Docker health state' {
        $script:InferenceReadiness.training_run_id = ('0' * 32)
        Invoke-InferenceTestStart
        $script:InferenceExit | Should Be 1
        @(Get-InferenceLifecycleCalls).Count | Should Be 0
    }

    It 'rejects the wrong evaluation identity despite healthy Docker health state' {
        $script:InferenceReadiness.evaluation_run_id = ('0' * 32)
        Invoke-InferenceTestStart
        $script:InferenceExit | Should Be 1
    }

    It 'rejects a service that has loaded the model more than once' {
        $script:InferenceReadiness.model_load_count = 2
        Invoke-InferenceTestStart
        $script:InferenceExit | Should Be 1
    }

    It 'rejects an HTTP readiness result that is not actually ready' {
        $script:InferenceReadiness.ready = $false
        $script:InferenceReadiness.status = 'model_load_failed'
        Invoke-InferenceTestStart
        $script:InferenceExit | Should Be 1
    }

    It 'rejects string true instead of a JSON readiness boolean' {
        $script:InferenceReadiness.ready = 'true'
        Invoke-InferenceTestStart
        $script:InferenceExit | Should Be 1
    }

    It 'rejects a string load count instead of the JSON integer one' {
        $script:InferenceReadiness.model_load_count = '1'
        Invoke-InferenceTestStart
        $script:InferenceExit | Should Be 1
    }

    It 'rejects non-ready status even when the ready field is true' {
        $script:InferenceReadiness.status = 'prediction_timeout'
        Invoke-InferenceTestStart
        $script:InferenceExit | Should Be 1
    }

    It 'rejects an unverified artifact result before env creation or native calls' {
        [IO.File]::Delete((Join-Path $script:InferenceRoot '.env'))
        Mock Assert-FareArtifacts { [pscustomobject]@{ Verified = $false } }
        Invoke-InferenceTestStart -Build
        $script:InferenceExit | Should Be 1
        Test-Path -LiteralPath (Join-Path $script:InferenceRoot '.env') | Should Be $false
        $script:InferenceCalls.Count | Should Be 0
    }

    It 'withholds malformed readiness output and never restarts a service' {
        $script:InferenceScenario.ReadinessMalformed = $true
        Invoke-InferenceTestStart
        $script:InferenceExit | Should Be 1
        $script:InferenceText | Should Not Match 'private-readiness-sentinel'
        @(Get-InferenceLifecycleCalls).Count | Should Be 0
    }

    It 'fails clearly on a bounded readiness command timeout without lifecycle actions' {
        $script:InferenceScenario.ReadinessCommandFailure = $true
        Invoke-InferenceTestStart
        $script:InferenceExit | Should Be 1
        @(Get-InferenceLifecycleCalls).Count | Should Be 0
        $script:InferenceText | Should Not Match 'private-error-sentinel'
    }

    It 'times out starting health using a fake clock without sleeping or restarting' {
        $script:InferenceScenario.HealthStarting = 'fare-inference'
        $script:InferenceScenario.AdvanceClock = $true
        Invoke-InferenceTestStart -HealthTimeoutSeconds 1
        $script:InferenceExit | Should Be 1
        @(Get-InferenceLifecycleCalls).Count | Should Be 0
    }
}
