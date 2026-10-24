# PACKAGE P1 orchestration only. Pester 4.10.1; all external and lifecycle seams mocked.
. (Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts/setup_system.ps1')

function Add-PackageTestCall {
    param([string]$Name, [string]$Phase = '', $Value = $null)
    $script:PackageCalls.Add([pscustomobject]@{ Name = $Name; Phase = $Phase; Value = $Value })
}

function Invoke-PackageTestSetup {
    param([switch]$Execute, [switch]$PrepareTest, [string]$Environment = 'package-test')
    $parameters = @{ ProjectRoot = $script:PackageRoot; Environment = $Environment
        BundleDirectory = $script:PackageBundle; CommandTimeoutSeconds = 3; StepTimeoutSeconds = 60 }
    if ($Execute) { $parameters.Execute = $true }
    if ($PrepareTest) { $parameters.PrepareTest = $true }
    $script:PackageOutput = @(& { Invoke-SystemSetup @parameters } *>&1)
    $script:PackageExit = @($script:PackageOutput | Where-Object { $_ -is [int] })[-1]
    $script:PackageText = $script:PackageOutput | Out-String
}

function Get-PackageTestMutations {
    @($script:PackageCalls | Where-Object { $_.Name -in @('initialize', 'images', 'start', 'bootstrap', 'install', 'validation', 'checkpoint-write') })
}

Describe 'PACKAGE P1 system setup orchestration (all external work mocked)' {
    BeforeEach {
        $script:PackageRoot = Join-Path $TestDrive 'project with spaces'
        $script:PackageBundle = Join-Path $TestDrive 'public frozen bundle'
        [IO.Directory]::CreateDirectory($script:PackageRoot) | Out-Null
        [IO.Directory]::CreateDirectory($script:PackageBundle) | Out-Null
        $script:PackageCalls = New-Object 'System.Collections.Generic.List[object]'
        $script:PackageScenario = @{ PreflightFailure = ''; BundleFailure = ''; ImageFailure = $false
            InfrastructureFailure = $false; ApplicationFailure = $false; BootstrapExit = 0
            InstallFailure = $false; ValidationFailure = $false; ValidationVerified = $true
            ExistingCheckpoint = $null; DriftOnSecondPreflight = $false; CheckpointWriteFailure = $false
            OwnershipReady = $true; PreflightVerified = $true; BundleVerified = $true }
        $script:PackagePreflightCount = 0
        $script:PackageContext = [pscustomobject]@{
            Root = $script:PackageRoot; RuntimeRoot = (Join-Path $script:PackageRoot '.sandbox/package-test')
            ModelRuntimeRoot = (Join-Path $script:PackageRoot '.sandbox/package-test/model-bundle')
            EnvPath = (Join-Path $script:PackageRoot '.sandbox/package-test/compose.env')
            StateRoot = (Join-Path $script:PackageRoot '.sandbox/package-test/package-state')
            Project = 'nyc-taxi-package-test'; ExpectedProject = 'nyc-taxi-package-test'; Environment = 'package-test'
            IsPackage = $true; OwnershipReady = $true; Model = [pscustomobject]@{ name = 'nyc-taxi-package-test'; services = [pscustomobject]@{} }
            ComposeArguments = @('compose', '--project-name', 'nyc-taxi-package-test')
            Services = @('postgres', 'mlflow'); BuildServices = @('worker', 'dashboard', 'fare-inference')
            CommandTimeoutSeconds = 3; StepTimeoutSeconds = 60; HealthTimeoutSeconds = 120; PollIntervalSeconds = 1
        }
        Mock New-PackageEnvironmentContext {
            param($ProjectRoot, $Environment, $CommandTimeoutSeconds, $StepTimeoutSeconds)
            Add-PackageTestCall 'context' '' $Environment
            if ($ProjectRoot -ne $script:PackageRoot) { throw 'Project root was not forwarded.' }
            $script:PackageContext.Environment = $Environment
            return $script:PackageContext
        }
        Mock Assert-PackageEnvironmentIsolation { Add-PackageTestCall 'isolation' }
        Mock Get-PackagePreflight {
            param($Context)
            Add-PackageTestCall 'preflight'
            $script:PackagePreflightCount++
            if ($script:PackageScenario.PreflightFailure) { throw ($script:PackageScenario.PreflightFailure + ' private-preflight-sentinel') }
            if ($script:PackageScenario.DriftOnSecondPreflight -and $script:PackagePreflightCount -gt 1) { throw 'Ownership drift private-preflight-sentinel' }
            return [pscustomobject]@{ Model = $Context.Model; OwnershipReady = $script:PackageScenario.OwnershipReady
                Verified = $script:PackageScenario.PreflightVerified
                CPUs = 4; MemoryBytes = 8GB; HostFreeBytes = 100GB; DockerFreeBytes = 100GB }
        }
        Mock Initialize-PackageConfiguration {
            param($Context)
            Add-PackageTestCall 'initialize'
            [IO.Directory]::CreateDirectory((Split-Path $Context.EnvPath -Parent)) | Out-Null
            if (-not [IO.File]::Exists($Context.EnvPath)) { [IO.File]::WriteAllText($Context.EnvPath, 'PACKAGE_PLACEHOLDER=private-config-sentinel') }
        }
        Mock Get-PackageBundleSelection {
            param($ProjectRoot, $BundleDirectory)
            Add-PackageTestCall 'bundle-select'
            if ($BundleDirectory -ne $script:PackageBundle) { throw 'Bundle directory was not forwarded.' }
            if ($script:PackageScenario.BundleFailure) { throw ($script:PackageScenario.BundleFailure + ' private-bundle-sentinel') }
            return [pscustomobject]@{ Verified = $script:PackageScenario.BundleVerified; Available = $true; BundleDirectory = $script:PackageBundle
                Directory = $script:PackageBundle; Mode = 'bundle'; Selection = 'bundle'
                BundlePath = (Join-Path $script:PackageBundle 'ml-p4-v1.zip')
                ManifestPath = (Join-Path $script:PackageBundle 'ml-p4-v1.manifest.json')
                ManifestSHA256 = ('a' * 64)
                ModelSHA256 = '759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee' }
        }
        Mock Ensure-PackageImages {
            param($Context)
            Add-PackageTestCall 'images'
            if ($script:PackageScenario.ImageFailure) { throw 'Image mismatch private-image-sentinel' }
            return [pscustomobject]@{ Verified = $true; WorkerImageId = 'sha256:mock-worker' }
        }
        Mock Start-PackagePhase {
            param($Context, $Phase)
            Add-PackageTestCall 'start' $Phase
            if (($Phase -eq 'infrastructure' -and $script:PackageScenario.InfrastructureFailure) -or
                ($Phase -eq 'application' -and $script:PackageScenario.ApplicationFailure)) { throw 'Service error private-service-sentinel' }
            return 0
        }
        Mock Invoke-DataBootstrap {
            param($Context, $Execute, $ProjectRoot, $Environment, $StepTimeoutSeconds)
            if (-not $Execute) { throw 'An execution wrapper invoked bootstrap without the explicit Execute flag.' }
            Add-PackageTestCall 'bootstrap' '' ([bool]$Execute)
            return $script:PackageScenario.BootstrapExit
        }
        Mock Install-PackageModelBundle {
            Add-PackageTestCall 'install'
            if ($script:PackageScenario.InstallFailure) { throw 'Model bundle error private-install-sentinel' }
            return [pscustomobject]@{ Verified = $true; Installed = $true; Reused = $false }
        }
        Mock Test-PackageFullValidation {
            param($Context)
            Add-PackageTestCall 'validation'
            if ($script:PackageScenario.ValidationFailure) { throw 'Aggregate query failed private-validation-sentinel' }
            return [pscustomobject]@{ Verified = $script:PackageScenario.ValidationVerified; TotalTrips = 23886265 }
        }
        Mock Read-PackageCheckpoint {
            Add-PackageTestCall 'checkpoint-read'
            return $script:PackageScenario.ExistingCheckpoint
        }
        Mock Write-PackageCheckpoint {
            param($Context, $State)
            if ($script:PackageScenario.CheckpointWriteFailure) { throw 'Checkpoint error private-checkpoint-sentinel' }
            $copy = $null
            if ($null -ne $State) { $copy = $State | ConvertTo-Json -Depth 30 -Compress | ConvertFrom-Json }
            Add-PackageTestCall 'checkpoint-write' '' $copy
        }
        # Defense in depth: an unmocked orchestration path must fail, never use native Docker.
        Mock Invoke-DockerCommand { throw 'Unexpected native Docker path in PACKAGE mock tests.' }
    }

    It 'defaults to a non-mutating plan with no configuration build lifecycle bootstrap or model install' {
        Invoke-PackageTestSetup
        $script:PackageExit | Should Be 0
        @(Get-PackageTestMutations).Count | Should Be 0
        Test-Path -LiteralPath $script:PackageContext.EnvPath | Should Be $false
        $script:PackageText | Should Match 'PLAN'
        Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
    }

    It 'anchors setup to the supplied root and selected isolated environment independently of cwd' {
        Push-Location $TestDrive
        try { Invoke-PackageTestSetup } finally { Pop-Location }
        $script:PackageExit | Should Be 0
        Assert-MockCalled New-PackageEnvironmentContext -Times 1 -Exactly -Scope It -ParameterFilter {
            $ProjectRoot -eq $script:PackageRoot -and $Environment -eq 'package-test'
        }
    }

    It 'permits explicit package-test preparation but still does not execute data or services' {
        Invoke-PackageTestSetup -PrepareTest
        $script:PackageExit | Should Be 0
        @($script:PackageCalls | Where-Object Name -eq 'initialize').Count | Should Be 1
        @($script:PackageCalls | Where-Object { $_.Name -in @('images', 'start', 'bootstrap', 'install', 'validation') }).Count | Should Be 0
    }

    It 'executes infrastructure data model application and full validation only after explicit Execute' {
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 0
        $operations = @($script:PackageCalls | Where-Object { $_.Name -in @('initialize', 'images', 'start', 'bootstrap', 'install', 'validation') })
        ($operations | ForEach-Object { $_.Name + ':' + $_.Phase }) -join ',' | Should Be 'initialize:,images:,start:infrastructure,bootstrap:,install:,start:application,validation:'
        Assert-MockCalled Invoke-DataBootstrap -Times 1 -Exactly -Scope It -ParameterFilter { $Execute }
    }

    It 'verifies resources ownership and model selection before any configuration or lifecycle operation' {
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 0
        $names = @($script:PackageCalls | ForEach-Object { $_.Name })
        [Array]::IndexOf($names, 'preflight') | Should BeLessThan ([Array]::IndexOf($names, 'initialize'))
        [Array]::IndexOf($names, 'bundle-select') | Should BeLessThan ([Array]::IndexOf($names, 'initialize'))
        $script:PackagePreflightCount | Should BeGreaterThan 1
    }

    It 'propagates insufficient RAM preflight rejection before writes builds or services' {
        $script:PackageScenario.PreflightFailure = 'Insufficient Docker RAM'
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 1
        @(Get-PackageTestMutations).Count | Should Be 0
        $script:PackageText | Should Not Match 'private-preflight-sentinel'
    }

    It 'propagates insufficient host or Docker disk preflight rejection without initializing data' {
        $script:PackageScenario.PreflightFailure = 'Insufficient host or Docker disk'
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 1
        @($script:PackageCalls | Where-Object { $_.Name -in @('initialize', 'images', 'start', 'bootstrap', 'install') }).Count | Should Be 0
    }

    It 'propagates a host port collision without starting or recreating services' {
        $script:PackageScenario.PreflightFailure = 'Required host port is occupied'
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 1
        @($script:PackageCalls | Where-Object { $_.Name -in @('initialize', 'images', 'start', 'bootstrap') }).Count | Should Be 0
    }

    It 'rejects existing legacy ownership before invoking bootstrap on a current environment' {
        $script:PackageScenario.PreflightFailure = 'Existing current resources have no package ownership'
        Invoke-PackageTestSetup -Execute -Environment current
        $script:PackageExit | Should Be 1
        @($script:PackageCalls | Where-Object { $_.Name -in @('initialize', 'images', 'start', 'bootstrap') }).Count | Should Be 0
    }

    It 'refuses Execute when preflight reports verified but unowned existing resources' {
        $script:PackageScenario.OwnershipReady = $false
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 1
        @(Get-PackageTestMutations).Count | Should Be 0
    }

    It 'permits a read-only plan for unowned legacy resources without adopting them' {
        $script:PackageScenario.OwnershipReady = $false
        Invoke-PackageTestSetup
        $script:PackageExit | Should Be 0
        @(Get-PackageTestMutations).Count | Should Be 0
    }

    It 'rejects an unverified preflight before configuration or bundle installation' {
        $script:PackageScenario.PreflightVerified = $false
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 1
        @(Get-PackageTestMutations).Count | Should Be 0
    }

    It 'rejects a missing model bundle before configuration build or any lifecycle work' {
        $script:PackageScenario.BundleFailure = 'Model bundle missing'
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 1
        @($script:PackageCalls | Where-Object { $_.Name -in @('initialize', 'images', 'start', 'bootstrap', 'install') }).Count | Should Be 0
        $script:PackageText | Should Not Match 'private-bundle-sentinel'
    }

    It 'rejects a wrong pinned bundle identity without replacing configuration or fetching a model' {
        $script:PackageScenario.BundleFailure = 'Model bundle full hash mismatch'
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 1
        @($script:PackageCalls | Where-Object { $_.Name -in @('initialize', 'images', 'start', 'bootstrap', 'install') }).Count | Should Be 0
    }

    It 'rejects a bundle selection that returned without proving verification' {
        $script:PackageScenario.BundleVerified = $false
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 1
        @(Get-PackageTestMutations).Count | Should Be 0
    }

    It 'rechecks actual ownership after configuration and stops before images if it drifted' {
        $script:PackageScenario.DriftOnSecondPreflight = $true
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 1
        @($script:PackageCalls | Where-Object Name -eq 'initialize').Count | Should Be 1
        @($script:PackageCalls | Where-Object { $_.Name -in @('images', 'start', 'bootstrap', 'install') }).Count | Should Be 0
    }

    It 'stops at an image verification or build failure without starting infrastructure' {
        $script:PackageScenario.ImageFailure = $true
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 1
        @($script:PackageCalls | Where-Object { $_.Name -in @('start', 'bootstrap', 'install', 'validation') }).Count | Should Be 0
        $script:PackageText | Should Not Match 'private-image-sentinel'
    }

    It 'stops after infrastructure failure without bootstrap or application startup' {
        $script:PackageScenario.InfrastructureFailure = $true
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 1
        @($script:PackageCalls | Where-Object { $_.Name -in @('bootstrap', 'install', 'validation') }).Count | Should Be 0
        @($script:PackageCalls | Where-Object { $_.Name -eq 'start' -and $_.Phase -eq 'application' }).Count | Should Be 0
    }

    It 'treats a nonzero bootstrap exit as failure and does not install a model or start applications' {
        $script:PackageScenario.BootstrapExit = 1
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 1
        @($script:PackageCalls | Where-Object Name -eq 'bootstrap').Count | Should Be 1
        @($script:PackageCalls | Where-Object { $_.Name -in @('install', 'validation') }).Count | Should Be 0
        @($script:PackageCalls | Where-Object { $_.Name -eq 'start' -and $_.Phase -eq 'application' }).Count | Should Be 0
    }

    It 'does not continue after a timed-out bootstrap whose worker may still exist' {
        $script:PackageScenario.BootstrapExit = 124
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 1
        @($script:PackageCalls | Where-Object { $_.Name -in @('install', 'validation') }).Count | Should Be 0
    }

    It 'stops after model installation failure and never fabricates application readiness' {
        $script:PackageScenario.InstallFailure = $true
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 1
        @($script:PackageCalls | Where-Object { $_.Name -eq 'start' -and $_.Phase -eq 'application' }).Count | Should Be 0
        @($script:PackageCalls | Where-Object Name -eq 'validation').Count | Should Be 0
        $script:PackageText | Should Not Match 'private-install-sentinel'
    }

    It 'does not validate or claim ready when application startup failed' {
        $script:PackageScenario.ApplicationFailure = $true
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 1
        @($script:PackageCalls | Where-Object Name -eq 'validation').Count | Should Be 0
    }

    It 'rejects an unverified final aggregate index view or lineage result' {
        $script:PackageScenario.ValidationVerified = $false
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 1
        @($script:PackageCalls | Where-Object Name -eq 'validation').Count | Should Be 1
    }

    It 'rejects failed actual COUNT validation without printing raw database details' {
        $script:PackageScenario.ValidationFailure = $true
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 1
        $script:PackageText | Should Not Match 'private-validation-sentinel'
    }

    It 'records phase checkpoints only for execution and retains failure state' {
        $script:PackageScenario.BootstrapExit = 1
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 1
        $writes = @($script:PackageCalls | Where-Object Name -eq 'checkpoint-write')
        $writes.Count | Should BeGreaterThan 0
        ($writes[-1].Value | ConvertTo-Json -Depth 30 -Compress) | Should Match '(?i)fail'
    }

    It 'does not blindly skip bootstrap or final validation because an old checkpoint says ready' {
        $script:PackageScenario.ExistingCheckpoint = [pscustomobject]@{ SchemaVersion = 1; Owner = 'setup_system_v1'; Project = $script:PackageContext.Project
            RuntimeRoot = $script:PackageContext.RuntimeRoot; Status = 'ready'; Stage = 'ready'
            CompletedStages = @('images', 'infrastructure', 'bootstrap', 'model', 'application', 'validation') }
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 0
        @($script:PackageCalls | Where-Object Name -eq 'bootstrap').Count | Should Be 1
        @($script:PackageCalls | Where-Object Name -eq 'validation').Count | Should Be 1
    }

    It 'reruns the real bootstrap seam so existing manifest lineage controls idempotency' {
        Invoke-PackageTestSetup -Execute
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 0
        @($script:PackageCalls | Where-Object Name -eq 'bootstrap').Count | Should Be 2
        @($script:PackageCalls | Where-Object Name -eq 'validation').Count | Should Be 2
        Assert-MockCalled Invoke-DataBootstrap -Times 2 -Exactly -Scope It -ParameterFilter { $Execute }
        $script:PackageText | Should Not Match 'private-.*sentinel'
    }

    It 'rejects PrepareTest for current instead of creating configuration there' {
        Invoke-PackageTestSetup -PrepareTest -Environment current
        $script:PackageExit | Should Be 1
        $script:PackageCalls.Count | Should Be 0
    }

    It 'rejects combined PrepareTest and Execute before any model or data operation' {
        Invoke-PackageTestSetup -PrepareTest -Execute
        $script:PackageExit | Should Be 1
        $script:PackageCalls.Count | Should Be 0
    }

    It 'fails safely if the first execution checkpoint cannot be persisted' {
        $script:PackageScenario.CheckpointWriteFailure = $true
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 1
        @($script:PackageCalls | Where-Object { $_.Name -in @('images', 'start', 'bootstrap', 'install', 'validation') }).Count | Should Be 0
        $script:PackageText | Should Not Match 'private-checkpoint-sentinel'
    }

    It 'rejects a concurrent execution while another process holds the state lock' {
        $held=Enter-PackageExecutionLock $script:PackageContext
        try {
            Invoke-PackageTestSetup -Execute
            $script:PackageExit | Should Be 1
            @($script:PackageCalls | Where-Object { $_.Name -in @('images','start','bootstrap','install','validation','checkpoint-write') }).Count | Should Be 0
        } finally {$held.Dispose()}
    }

    It 'releases the execution lock on failure without deleting state or old outputs' {
        $script:PackageScenario.BootstrapExit=1
        Invoke-PackageTestSetup -Execute
        $script:PackageExit | Should Be 1
        $lock=Enter-PackageExecutionLock $script:PackageContext
        $lock.Dispose()
        Test-Path -LiteralPath (Join-Path $script:PackageContext.StateRoot 'execution.lock') | Should Be $true
    }
}
