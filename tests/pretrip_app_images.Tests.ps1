# Offline only: every Docker/build/network/runtime verification is mocked.
. (Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts/pretrip_app_images.ps1')
function Invoke-PretripAppDocker {
    param($Context,[string[]]$Arguments,[string]$FailureMessage,[int]$TimeoutSeconds,[string]$InputText)
    throw 'Real Docker transport is forbidden by these tests.'
}
function New-ImageTestPlan {
    $images=@()
    foreach($role in @('worker','inference','dashboard')) {
        $images+=[pscustomobject]@{Role=$role;Tag=('fixture-'+$role+':v22');SourceSHA256=('a'*64);DependencySHA256=('b'*64);
            Files=@([pscustomobject]@{path='src/fixture.py';bytes=1;sha256=('c'*64)});Dockerfile=('Dockerfile.'+$role);Status='unchecked';ImageID=$null}
    }
    [pscustomobject]@{WorkerImage=$images[0].Tag;InferenceImage=$images[1].Tag;DashboardImage=$images[2].Tag;
        SourceSHA256=('a'*64);DependencySHA256=('b'*64);Images=$images;InferenceImageID=$null;DashboardImageID=$null;ColdBuild='cold unmeasured'}
}
Describe 'Standalone pretrip image build and reuse guards' {
    BeforeEach {
        $script:ImgPlan=New-ImageTestPlan
        $script:ImgContext=[pscustomobject]@{Root=$TestDrive;BuildTimeoutSeconds=1800}
        $script:ImgPresent=@{}; $script:ImgCalls=New-Object 'Collections.Generic.List[object]'
        $script:ImgBadLabel=$false; $script:ImgInspectFailure=$false; $script:ImgBuildFailure=$false; $script:ImgBadRuntime=$false
        Mock Get-PretripAppImagePlan {return $script:ImgPlan}
        Mock Invoke-PretripAppDocker {
            $script:ImgCalls.Add([pscustomobject]@{Arguments=@($Arguments);InputText=$InputText})
            if($Arguments[0] -eq 'build') {
                if($script:ImgBuildFailure){throw 'safe build failure'}
                $tag=$Arguments[([Array]::IndexOf($Arguments,'--tag')+1)]
                $script:ImgPresent[$tag]=$true;return ''
            }
            if($Arguments[0] -eq 'run') {
                if($script:ImgBadRuntime){return 'NOT_VERIFIED'}
                return 'PRETRIP_IMAGE_RUNTIME_VERIFIED'
            }
            if($Arguments[0] -eq 'image' -and $Arguments[1] -eq 'ls') {
                $filter=$Arguments[([Array]::IndexOf($Arguments,'--filter')+1)]
                $tag=$filter.Substring('reference='.Length)
                if($script:ImgPresent[$tag]){return ('sha256:'+('d'*64))};return ''
            }
            if($Arguments[0] -eq 'image' -and $Arguments[1] -eq 'inspect') {
                if($script:ImgInspectFailure){throw 'safe inspect failure'}
                $tag=$Arguments[-1];$image=@($script:ImgPlan.Images | Where-Object {$_.Tag -eq $tag})[0]
                $source=$image.SourceSHA256;if($script:ImgBadLabel){$source='foreign'}
                return (ConvertTo-Json -Compress -Depth 5 -InputObject @{ID=('sha256:'+('d'*64));Labels=@{
                    'com.nyc-taxi.pretrip-app.schema'='1';'com.nyc-taxi.pretrip-app.role'=$image.Role;
                    'com.nyc-taxi.pretrip-app.source-sha256'=$source;'com.nyc-taxi.pretrip-app.dependency-sha256'=$image.DependencySHA256}})
            }
            throw 'Unexpected mocked Docker command'
        }
    }
    It 'PLAN reports missing images without builds or runtime containers' {
        $result=Assert-PretripAppImages $script:ImgContext
        @($result.Images | Where-Object {$_.Status -eq 'build-required'}).Count | Should Be 3
        @($script:ImgCalls | Where-Object {$_.Arguments[0] -in @('build','run')}).Count | Should Be 0
    }
    It 'PLAN checks exact image provenance but does not run reused images' {
        foreach($image in $script:ImgPlan.Images){$script:ImgPresent[$image.Tag]=$true}
        $result=Assert-PretripAppImages $script:ImgContext
        $result.Images[1].Status | Should Be 'reuse-provenance-checked'
        @($script:ImgCalls | Where-Object {$_.Arguments[0] -in @('build','run')}).Count | Should Be 0
    }
    It 'builds missing worker then inference then dashboard in Execute' {
        $result=Assert-PretripAppImages $script:ImgContext -Execute
        $builds=@($script:ImgCalls | Where-Object {$_.Arguments[0] -eq 'build'})
        $builds.Count | Should Be 3
        ($builds[0].Arguments -join ' ') | Should Match 'fixture-worker:v22'
        ($builds[1].Arguments -join ' ') | Should Match 'WORKER_RUNTIME_IMAGE=fixture-worker:v22'
        ($builds[2].Arguments -join ' ') | Should Match '--target dashboard'
        $result.InferenceImageID | Should Be ('sha256:'+('d'*64))
        $result.DashboardImageID | Should Be ('sha256:'+('d'*64))
    }
    It 'never uses P6 or author local image tags in build commands' {
        $null=Assert-PretripAppImages $script:ImgContext -Execute
        $calls=($script:ImgCalls | ForEach-Object {$_.Arguments -join ' '}) -join ' '
        $calls | Should Not Match 'p6-clean|p6-test|ml-p5|ml-p4'
        @($script:ImgCalls | Where-Object {$_.Arguments[0] -eq 'build' -and '--pull=false' -in $_.Arguments}).Count | Should Be 3
    }
    It 'reuses all correct images and verifies runtime bytes without a build' {
        foreach($image in $script:ImgPlan.Images){$script:ImgPresent[$image.Tag]=$true}
        $result=Assert-PretripAppImages $script:ImgContext -Execute
        @($script:ImgCalls | Where-Object {$_.Arguments[0] -eq 'build'}).Count | Should Be 0
        @($script:ImgCalls | Where-Object {$_.Arguments[0] -eq 'run'}).Count | Should Be 2
        $result.Images[2].Status | Should Be 'runtime-verified'
    }
    It 'does not build an absent worker if inference and dashboard can be reused' {
        $script:ImgPresent[$script:ImgPlan.InferenceImage]=$true;$script:ImgPresent[$script:ImgPlan.DashboardImage]=$true
        $result=Assert-PretripAppImages $script:ImgContext -Execute
        $result.Images[0].Status | Should Be 'not-needed'
        @($script:ImgCalls | Where-Object {$_.Arguments[0] -eq 'build'}).Count | Should Be 0
    }
    It 'rejects a foreign versioned image without overwriting it' {
        $script:ImgPresent[$script:ImgPlan.WorkerImage]=$true;$script:ImgBadLabel=$true
        {Assert-PretripAppImages $script:ImgContext -Execute} | Should Throw
        @($script:ImgCalls | Where-Object {$_.Arguments[0] -in @('build','run')}).Count | Should Be 0
    }
    It 'does not treat failed inspect as a missing image' {
        $script:ImgPresent[$script:ImgPlan.WorkerImage]=$true;$script:ImgInspectFailure=$true
        {Assert-PretripAppImages $script:ImgContext -Execute} | Should Throw
        @($script:ImgCalls | Where-Object {$_.Arguments[0] -eq 'build'}).Count | Should Be 0
    }
    It 'stops after a failed build before inference or dashboard builds' {
        $script:ImgBuildFailure=$true
        {Assert-PretripAppImages $script:ImgContext -Execute} | Should Throw
        @($script:ImgCalls | Where-Object {$_.Arguments[0] -eq 'build'}).Count | Should Be 1
        @($script:ImgCalls | Where-Object {$_.Arguments[0] -eq 'run'}).Count | Should Be 0
    }
    It 'stops on byte verifier failure even if provenance labels match' {
        foreach($image in $script:ImgPlan.Images){$script:ImgPresent[$image.Tag]=$true};$script:ImgBadRuntime=$true
        {Assert-PretripAppImages $script:ImgContext -Execute} | Should Throw
        @($script:ImgCalls | Where-Object {$_.Arguments[0] -eq 'run'}).Count | Should Be 1
    }
    It 'offline verifier uses stdin and no model or source mount' {
        foreach($image in $script:ImgPlan.Images){$script:ImgPresent[$image.Tag]=$true}
        $null=Assert-PretripAppImages $script:ImgContext -Execute
        foreach($run in @($script:ImgCalls | Where-Object {$_.Arguments[0] -eq 'run'})) {
            $command=$run.Arguments -join ' '
            $command | Should Match '--network none'
            $command | Should Match '--read-only'
            $command | Should Not Match '--mount|--volume|/model'
            '-i' -in $run.Arguments | Should Be $true
            @($run.InputText | ConvertFrom-Json).Count | Should Be 1
        }
    }
    It 'rerun of correct images does not build or replace their IDs' {
        $first=Assert-PretripAppImages $script:ImgContext -Execute
        $before=@($script:ImgCalls | Where-Object {$_.Arguments[0] -eq 'build'}).Count
        $script:ImgPlan=New-ImageTestPlan
        $second=Assert-PretripAppImages $script:ImgContext -Execute
        @($script:ImgCalls | Where-Object {$_.Arguments[0] -eq 'build'}).Count | Should Be $before
        $second.InferenceImageID | Should Be $first.InferenceImageID
    }
}
Describe 'Repository build input identities' {
    It 'rejects missing required input without writing output' {
        {Get-PretripAppInputFiles $TestDrive @('missing.txt')} | Should Throw
        @(Get-ChildItem -LiteralPath $TestDrive -Force).Count | Should Be 0
    }
    It 'hashes exact bytes and deduplicates an input used by two COPY trees' {
        $root=Join-Path $TestDrive 'inputs';$null=New-Item -ItemType Directory -Path (Join-Path $root 'src') -Force
        [IO.File]::WriteAllText((Join-Path $root 'src/a.py'),'x',[Text.Encoding]::ASCII)
        $files=Get-PretripAppInputFiles $root @('src','src/a.py')
        @($files).Count | Should Be 1
        $files[0].bytes | Should Be 1
        $files[0].sha256 | Should Be (Get-PretripAppSHA256 ([Text.Encoding]::ASCII.GetBytes('x')))
    }
    It 'does not include ignored env files or pycache under a COPY tree' {
        $root=Join-Path $TestDrive 'ignored';$null=New-Item -ItemType Directory -Path (Join-Path $root 'src/__pycache__') -Force
        $null=New-Item -ItemType Directory -Path (Join-Path $root 'src/local.egg-info') -Force
        foreach($relative in @('src/a.py','src/.env','src/__pycache__/x.pyc','src/local.egg-info/PKG-INFO')){[IO.File]::WriteAllText((Join-Path $root $relative),'x')}
        $files=Get-PretripAppInputFiles $root @('src')
        @($files).Count | Should Be 1;$files[0].path | Should Be 'src/a.py'
    }
    It 'changes inference provenance when the worker Dockerfile changes' {
        $script:FixtureSparkChange=$false
        Mock Get-PretripAppInputFiles {
            @($Paths | Sort-Object -Unique | ForEach-Object {
                $value='stable:'+$_
                if($_ -eq 'Dockerfile.spark' -and $script:FixtureSparkChange){$value+=':changed'}
                [pscustomobject]@{path=$_;bytes=$value.Length;sha256=(Get-PretripAppSHA256 ([Text.Encoding]::UTF8.GetBytes($value)))}
            })
        }
        $first=Get-PretripAppImagePlan $TestDrive
        $script:FixtureSparkChange=$true
        $second=Get-PretripAppImagePlan $TestDrive
        $first.WorkerImage | Should Not Be $second.WorkerImage
        $first.InferenceImage | Should Not Be $second.InferenceImage
        $first.DashboardImage | Should Be $second.DashboardImage
    }
}
