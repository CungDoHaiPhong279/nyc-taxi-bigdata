# Pester 4, PowerShell 5.1. Docker/build/network/model/lifecycle are mocked.
. (Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts/setup_pretrip_v22.ps1')

function New-PretripAppTestContainer {
    param([string]$Service='dashboard',[string]$Status='running',[string]$Health='healthy')
    $spec=Get-PretripAppSpec -ProjectRoot $script:AppRoot
    [pscustomobject]@{ID=($Service+'-id');Service=$Service;Project=$spec.Project;Owner=$spec.Owner;
        Status=$Status;Health=$Health;ConfigHash=('a'*64);ImageID=($Service+'-image');
        ImageReference=if($Service -eq 'dashboard'){'test-dashboard:fixed'}else{'test-inference:fixed'};
        StartedAt='2026-01-01T00:00:00Z'}
}

Describe 'Standalone pretrip V2.2 setup orchestration' {
    BeforeEach {
        $script:AppRoot=Join-Path $TestDrive 'standalone'
        $null=New-Item -ItemType Directory -Path (Join-Path $script:AppRoot 'config') -Force
        [IO.File]::WriteAllText((Join-Path $script:AppRoot 'docker-compose.pretrip-app.yml'),'services: {}')
        [IO.File]::WriteAllText((Join-Path $script:AppRoot 'config/pretrip_app.env'),'# public, no secrets')
        $script:Stages=New-Object 'Collections.Generic.List[string]'
        $script:NativeCalls=New-Object 'Collections.Generic.List[object]'
        $script:AppOwned=@{}
        Mock Resolve-DockerCli {'mock-docker'}
        Mock Invoke-PretripAppDocker {
            param($Context,$Arguments,$FailureMessage,$TimeoutSeconds)
            $script:NativeCalls.Add(@($Arguments))
            ''
        }
        Mock Get-PretripAppImagePlan {
            [pscustomobject]@{InferenceImage='test-inference:fixed';DashboardImage='test-dashboard:fixed'}
        }
        Mock Get-PretripAppContainers {return $script:AppOwned}
        Mock Assert-PretripAppNetwork {}
        Mock Assert-PretripAppPort {}
        Mock Get-PretripAppConfigHash {return ('a'*64)}
        Mock Install-PretripV22Distribution {
            param($ProjectRoot,$Execute)
            $script:Stages.Add('bundle')
            [pscustomobject]@{Verified=$true;ModelSHA256=(Get-PretripV22Identity).ModelSHA256;Writes=[bool]$Execute}
        }
        Mock Assert-PretripAppImages {
            param($Context,$Execute)
            $script:Stages.Add('images')
            [pscustomobject]@{InferenceImage='test-inference:fixed';DashboardImage='test-dashboard:fixed';
                InferenceImageID='fare-inference-image';DashboardImageID='dashboard-image';Writes=[bool]$Execute;
                SourceSHA256=('c'*64);DependencySHA256=('d'*64);
                Images=@([pscustomobject]@{Role='inference';Tag='test-inference:fixed';Status='reuse-provenance-checked';ImageID='fare-inference-image'},
                         [pscustomobject]@{Role='dashboard';Tag='test-dashboard:fixed';Status='reuse-provenance-checked';ImageID='dashboard-image'});
                ColdBuild='mock cold-build plan'}
        }
        Mock Assert-PretripV22InstalledRoot {[pscustomobject]@{Verified=$true}}
        Mock Start-PretripAppService {
            param($Context,$Service,$ImageID,$ConfigHash)
            $script:Stages.Add($Service)
            New-PretripAppTestContainer -Service $Service
        }
        Mock Test-PretripAppReadiness {$script:Stages.Add('full-readiness');[pscustomobject]@{ready=$true}}
    }

    Context 'isolated case 1' {
        It 'PLAN is default, read-only and stops before any service or full readiness request' {
            $snapshot={@(Get-ChildItem $script:AppRoot -Recurse -Force | Sort-Object FullName | ForEach-Object {$_.FullName+'|'+$_.LastWriteTimeUtc.Ticks+'|'+$(if($_ -is [IO.FileInfo]){(Get-FileHash $_.FullName).Hash}else{''})}) -join "`n"}
            # Windows can finish a directory timestamp update after fixture
            # writes return. Establish two equal baselines before testing PLAN.
            $before=& $snapshot
            $stable=$false
            foreach($attempt in 1..20){
                Start-Sleep -Milliseconds 20
                $next=& $snapshot
                if($next -ceq $before){$stable=$true;break}
                $before=$next
            }
            if(-not $stable){throw 'Test fixture timestamps did not stabilize before PLAN.'}
            $result=Invoke-PretripAppSetup -ProjectRoot $script:AppRoot
            $after=& $snapshot
            $result.Status | Should Be 'PLAN'
            $result.Writes | Should Be $false
            $after | Should BeExactly $before
            ($script:Stages -join ',') | Should Be 'bundle,images'
            Assert-MockCalled Start-PretripAppService -Scope It -Times 0 -Exactly
            Assert-MockCalled Install-PretripV22Distribution -Scope It -Times 1 -Exactly -ParameterFilter {-not $Execute}
            Assert-MockCalled Assert-PretripAppImages -Scope It -Times 1 -Exactly -ParameterFilter {-not $Execute}
            @($script:NativeCalls | Where-Object {$_ -contains 'up' -or $_ -contains 'start' -or $_ -contains 'run' -or $_ -contains 'build' -or $_ -contains 'pull'}).Count | Should Be 0
        }
    }

    Context 'isolated case 2' {
        It 'Execute follows bundle images inference full-readiness dashboard order' {
            $result=Invoke-PretripAppSetup -ProjectRoot $script:AppRoot -Execute
            $result.Status | Should Be 'READY'
            ($script:Stages -join ',') | Should Be 'bundle,images,fare-inference,full-readiness,dashboard'
            Assert-MockCalled Install-PretripV22Distribution -Scope It -Times 1 -Exactly -ParameterFilter {$Execute}
            Assert-MockCalled Assert-PretripAppImages -Scope It -Times 1 -Exactly -ParameterFilter {$Execute}
            @($script:NativeCalls | Where-Object {($_ -join ' ') -match 'postgres|mlflow|worker|bootstrap'}).Count | Should Be 0
        }
    }

    foreach($failure in @('missing','corrupt')) {
        Context 'isolated case 3' {
            It "stops after $failure bundle without images or services" {
                Mock Install-PretripV22Distribution {throw 'mock bounded bundle rejection'}
                {Invoke-PretripAppSetup -ProjectRoot $script:AppRoot -Execute} | Should Throw
                Assert-MockCalled Assert-PretripAppImages -Scope It -Times 0 -Exactly
                Assert-MockCalled Start-PretripAppService -Scope It -Times 0 -Exactly
            }
        }
    }

    Context 'isolated case 4' {
        It 'stops immediately on image build/identity failure' {
            Mock Assert-PretripAppImages {throw 'mock image failure'}
            {Invoke-PretripAppSetup -ProjectRoot $script:AppRoot -Execute} | Should Throw
            Assert-MockCalled Start-PretripAppService -Scope It -Times 0 -Exactly
        }
    }

    Context 'isolated case 5' {
        It 'does not start dashboard when inference health/start fails' {
            Mock Start-PretripAppService {throw 'mock inference failure'} -ParameterFilter {$Service -eq 'fare-inference'}
            {Invoke-PretripAppSetup -ProjectRoot $script:AppRoot -Execute} | Should Throw
            Assert-MockCalled Test-PretripAppReadiness -Scope It -Times 0 -Exactly
            Assert-MockCalled Start-PretripAppService -Scope It -Times 0 -Exactly -ParameterFilter {$Service -eq 'dashboard'}
        }
    }

    Context 'isolated case 6' {
        It 'does not start dashboard when full frozen readiness fails' {
            Mock Test-PretripAppReadiness {throw 'mock identity rejection'}
            {Invoke-PretripAppSetup -ProjectRoot $script:AppRoot -Execute} | Should Throw
            Assert-MockCalled Start-PretripAppService -Scope It -Times 0 -Exactly -ParameterFilter {$Service -eq 'dashboard'}
        }
    }

    Context 'isolated case 7' {
        It 'requires Docker CLI before any bundle installation' {
            Mock Resolve-DockerCli {$null}
            {Invoke-PretripAppSetup -ProjectRoot $script:AppRoot -Execute} | Should Throw
            Assert-MockCalled Install-PretripV22Distribution -Scope It -Times 0 -Exactly
        }
    }

    Context 'isolated case 8' {
        It 'fails when daemon is unavailable and does not install or build' {
            Mock Invoke-PretripAppDocker {throw 'mock daemon unavailable'}
            {Invoke-PretripAppSetup -ProjectRoot $script:AppRoot -Execute} | Should Throw
            Assert-MockCalled Install-PretripV22Distribution -Scope It -Times 0 -Exactly
        }
    }

    Context 'isolated case 9' {
        It 'rejects pre-existing config drift before bundle or build for PLAN too' {
            $item=New-PretripAppTestContainer
            $item.ConfigHash='b'*64
            $script:AppOwned=@{dashboard=$item}
            {Invoke-PretripAppSetup -ProjectRoot $script:AppRoot} | Should Throw
            Assert-MockCalled Install-PretripV22Distribution -Scope It -Times 0 -Exactly
            Assert-MockCalled Assert-PretripAppImages -Scope It -Times 0 -Exactly
        }
    }

    Context 'isolated case 10' {
        It 'rejects pre-existing image tag drift before bundle or build' {
            $item=New-PretripAppTestContainer
            $item.ImageReference='foreign:old'
            $script:AppOwned=@{dashboard=$item}
            {Invoke-PretripAppSetup -ProjectRoot $script:AppRoot -Execute} | Should Throw
            Assert-MockCalled Assert-PretripAppImages -Scope It -Times 0 -Exactly
        }
    }

    Context 'isolated case 11' {
        It 'rejects same tag with changed image ID before service lifecycle' {
            $item=New-PretripAppTestContainer
            $item.ImageID='foreign-image'
            $script:AppOwned=@{dashboard=$item}
            {Invoke-PretripAppSetup -ProjectRoot $script:AppRoot -Execute} | Should Throw
            Assert-MockCalled Start-PretripAppService -Scope It -Times 0 -Exactly
        }
    }

    Context 'isolated case 12' {
        It 'restores caller process environment even after failure' {
            $old=$env:PRETRIP_APP_PROJECT
            try {
                $env:PRETRIP_APP_PROJECT='caller-owned-value'
                Mock Install-PretripV22Distribution {throw 'mock bundle failure'}
                {Invoke-PretripAppSetup -ProjectRoot $script:AppRoot -Execute} | Should Throw
                $env:PRETRIP_APP_PROJECT | Should Be 'caller-owned-value'
            } finally {$env:PRETRIP_APP_PROJECT=$old}
        }
    }

    Context 'isolated case 13' {
        It 'supports a clean root with no .env data or author candidate and an explicit public env file' {
            $result=Invoke-PretripAppSetup -ProjectRoot $script:AppRoot
            $result.Status | Should Be 'PLAN'
            Test-Path (Join-Path $script:AppRoot '.env') | Should Be $false
            $spec=Get-PretripAppSpec -ProjectRoot $script:AppRoot
            ($spec.ComposeArguments -join ' ') | Should Match 'pretrip_app.env'
            ($spec.ComposeArguments -join ' ') | Should Not Match 'docker-compose.yml'
        }
    }

    Context 'isolated case 14' {
        It 'does not depend on caller cwd and gives test a distinct project port runtime and owner' {
            $current=Get-Location
            try {
                Set-Location $TestDrive
                $app=Get-PretripAppSpec -ProjectRoot $script:AppRoot
                $test=Get-PretripAppSpec -ProjectRoot $script:AppRoot -Environment test
                $app.Project | Should Be 'nyc-taxi-pretrip-v22'
                $test.Project | Should Be 'nyc-taxi-pretrip-v22-test'
                $app.Port | Should Be 18522
                $test.Port | Should Be 28522
                $app.RuntimeRoot | Should Not Be $test.RuntimeRoot
                $app.Owner | Should Not Be $test.Owner
                $app.Project | Should Not Be 'nyc-taxi-bigdata'
            } finally {Set-Location $current}
        }
    }
}

Describe 'Standalone pretrip ownership and lifecycle guards' {
    BeforeEach {
        $script:AppRoot=Join-Path $TestDrive 'guard-root'
        $null=New-Item -ItemType Directory -Path (Join-Path $script:AppRoot 'config') -Force
        [IO.File]::WriteAllText((Join-Path $script:AppRoot 'docker-compose.pretrip-app.yml'),'services: {}')
        [IO.File]::WriteAllText((Join-Path $script:AppRoot 'config/pretrip_app.env'),'# empty public config')
        $script:AppSpec=Get-PretripAppSpec -ProjectRoot $script:AppRoot
        $script:LifecycleCalls=New-Object 'Collections.Generic.List[object]'
        Mock Invoke-PretripAppDocker {
            param($Context,$Arguments,$FailureMessage,$TimeoutSeconds)
            $script:LifecycleCalls.Add(@($Arguments))
            ''
        }
    }

    Context 'isolated case 15' {
        It 'rejects a container with matching project but foreign checkout owner' {
            $item=New-PretripAppTestContainer
            $item.Owner='foreign'
            Mock Invoke-PretripAppDocker {'owned-id'} -ParameterFilter {$Arguments[0] -eq 'container' -and $Arguments[1] -eq 'ls'}
            Mock Get-PretripAppContainer {$item}
            {Get-PretripAppContainers $script:AppSpec} | Should Throw
        }
    }

    Context 'isolated case 16' {
        It 'rejects an unexpected service in the isolated project' {
            $item=New-PretripAppTestContainer
            $item.Service='postgres'
            Mock Invoke-PretripAppDocker {'owned-id'} -ParameterFilter {$Arguments[0] -eq 'container' -and $Arguments[1] -eq 'ls'}
            Mock Get-PretripAppContainer {$item}
            {Get-PretripAppContainers $script:AppSpec} | Should Throw
        }
    }

    Context 'isolated case 17' {
        It 'rejects a foreign network instead of adopting it' {
            Mock Invoke-PretripAppDocker {'network-id'} -ParameterFilter {$Arguments[0] -eq 'network' -and $Arguments[1] -eq 'ls'}
            Mock Invoke-PretripAppDocker {'{"Name":"nyc-taxi-pretrip-v22_default","Project":"other","Owner":"other"}'} -ParameterFilter {$Arguments[0] -eq 'network' -and $Arguments[1] -eq 'inspect'}
            {Assert-PretripAppNetwork $script:AppSpec} | Should Throw
        }
    }

    Context 'isolated case 18' {
        It 'rejects a foreign Docker-published host port' {
            Mock Invoke-PretripAppDocker {'foreign-id'} -ParameterFilter {$Arguments[0] -eq 'container' -and $Arguments[1] -eq 'ls'}
            Mock Get-PretripAppHostListeners {@()}
            {Assert-PretripAppPort $script:AppSpec @{}} | Should Throw
        }
    }

    Context 'isolated case 19' {
        It 'rejects a host-only port collision' {
            Mock Get-PretripAppHostListeners {@([pscustomobject]@{Port=18522})}
            {Assert-PretripAppPort $script:AppSpec @{}} | Should Throw
        }
    }

    Context 'isolated case 20' {
        It 'allows the port of the exact owned dashboard' {
            Mock Invoke-PretripAppDocker {'dashboard-id'} -ParameterFilter {$Arguments[0] -eq 'container' -and $Arguments[1] -eq 'ls'}
            Mock Get-PretripAppHostListeners {@([pscustomobject]@{Port=18522})}
            {Assert-PretripAppPort $script:AppSpec @{dashboard=(New-PretripAppTestContainer)}} | Should Not Throw
        }
    }

    Context 'isolated case 21' {
        It 'rejects owner image and configuration drift without lifecycle' {
            foreach($property in @('Owner','ImageID','ConfigHash')){
                $item=New-PretripAppTestContainer
                $item.$property='foreign'
                {Assert-PretripAppContainerIdentity $script:AppSpec $item 'dashboard-image' ('a'*64)} | Should Throw
            }
            $script:LifecycleCalls.Count | Should Be 0
        }
    }

    Context 'isolated case 22' {
        It 'reuses healthy exact containers and two reruns never start or create' {
            $item=New-PretripAppTestContainer
            Mock Get-PretripAppContainers {@{dashboard=$item}}
            Mock Assert-PretripAppNetwork {}
            Mock Assert-PretripAppPort {}
            Mock Wait-PretripAppHealth {$item}
            1..2 | ForEach-Object { $null=Start-PretripAppService $script:AppSpec 'dashboard' 'dashboard-image' ('a'*64) }
            @($script:LifecycleCalls | Where-Object {$_ -contains 'start' -or $_ -contains 'up'}).Count | Should Be 0
        }
    }

    Context 'isolated case 23' {
        It 'starts an exact stopped container by ID once without recreating' {
            $item=New-PretripAppTestContainer -Status exited
            Mock Get-PretripAppContainers {@{dashboard=$item}}
            Mock Assert-PretripAppNetwork {}
            Mock Assert-PretripAppPort {}
            Mock Wait-PretripAppHealth {$item}
            $null=Start-PretripAppService $script:AppSpec 'dashboard' 'dashboard-image' ('a'*64)
            @($script:LifecycleCalls | Where-Object {($_ -join ' ') -eq 'start dashboard-id'}).Count | Should Be 1
            @($script:LifecycleCalls | Where-Object {$_ -contains 'up'}).Count | Should Be 0
        }
    }

    Context 'isolated case 24' {
        It 'creates only a missing named service without dependencies build pull or recreation' {
            $script:ContainerReads=0
            $script:CreatedContainer=New-PretripAppTestContainer
            Mock Get-PretripAppContainers {$script:ContainerReads++;if($script:ContainerReads -eq 1){@{}}else{@{dashboard=$script:CreatedContainer}}}
            Mock Assert-PretripAppNetwork {}
            Mock Assert-PretripAppPort {}
            Mock Wait-PretripAppHealth {$script:CreatedContainer}
            $null=Start-PretripAppService $script:AppSpec 'dashboard' 'dashboard-image' ('a'*64)
            $script:ContainerReads | Should Be 2
            $call=@($script:LifecycleCalls | Where-Object {$_ -contains 'up'})
            $call.Count | Should Be 1
            ($call[0] -join ' ') | Should Match 'up -d --no-deps --no-recreate --no-build --pull never dashboard$'
        }
    }

    Context 'isolated case 25' {
        It 'refuses unhealthy or non-running containers without retry' {
            $item=New-PretripAppTestContainer -Health unhealthy
            Mock Get-PretripAppContainer {$item}
            {Wait-PretripAppHealth $script:AppSpec dashboard 'dashboard-id'} | Should Throw
            $script:LifecycleCalls.Count | Should Be 0
        }
    }

    Context 'isolated case 26' {
        It 'has a finite health wait and performs no recovery restart' {
            $item=New-PretripAppTestContainer -Health starting
            Mock Get-PretripAppContainer {$item}
            $script:Clock=[DateTime]::UtcNow
            Mock Get-StartupTime {$script:Clock=$script:Clock.AddSeconds(181);$script:Clock}
            Mock Pause-StartupPolling {}
            {Wait-PretripAppHealth $script:AppSpec dashboard 'dashboard-id'} | Should Throw
            Assert-MockCalled Pause-StartupPolling -Scope It -Times 0 -Exactly
        }
    }

    Context 'isolated case 27' {
        It 'uses the existing full readiness helper with explicit V2 artifacts only' {
            $item=New-PretripAppTestContainer -Service fare-inference
            $expected=[pscustomobject]@{Verified=$true;ModelSHA256='pinned'}
            Mock Test-InferenceReadiness {[pscustomobject]@{ready=$true}}
            $null=Test-PretripAppReadiness $script:AppSpec $item $expected
            Assert-MockCalled Test-InferenceReadiness -Scope It -Times 1 -Exactly -ParameterFilter {$Context.ModelVersion -ceq 'v2.2' -and $Context.InferenceArtifacts -eq $expected -and $Context.Project -ceq 'nyc-taxi-pretrip-v22'}
        }
    }
}
