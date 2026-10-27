# Pester 4: model-version/readiness guards are pure mocks, no Docker or model.
. (Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts/start.ps1')

Describe 'Explicit V2.2 model selection and compatibility guards' {
    BeforeEach {
        $script:SelectionRoot=Join-Path $TestDrive 'selection'
        $null=New-Item -ItemType Directory -Path $script:SelectionRoot -Force
        foreach($file in @('docker-compose.inference.yml','docker-compose.pretrip-v22.yml')) {
            [IO.File]::WriteAllText((Join-Path $script:SelectionRoot $file),'services: {}')
        }
        $script:SelectionContext=[pscustomobject]@{Root=$script:SelectionRoot;Environment='current';
            ComposeArguments=@('compose','--file',(Join-Path $script:SelectionRoot 'docker-compose.yml'));
            ExpectedProject='nyc-taxi-bigdata';Services=@();BuildServices=@()}
        $pin=Get-PretripV22Identity
        $script:SelectionExpected=[pscustomobject]@{Verified=$true;ModelVersion='v2.2';ModelSHA256=$pin.ModelSHA256;
            ArtifactManifestSHA256=$pin.ArtifactManifestSHA256;DictionarySHA256=$pin.DictionarySHA256;
            ContractVersion=$pin.ContractVersion;CalendarVersion=$pin.CalendarVersion;
            CalendarSHA256=$pin.CalendarSHA256;TzifSHA256=$pin.TzifSHA256}
        $script:SelectionReady=[pscustomobject]@{ready=$true;status='ready';model_load_count=1;fit_calls=0;model_version='v2.2';
            model_sha256=$pin.ModelSHA256;artifact_manifest_sha256=$pin.ArtifactManifestSHA256;dictionary_sha256=$pin.DictionarySHA256;
            contract_version=$pin.ContractVersion;calendar_version=$pin.CalendarVersion;
            calendar_sha256=$pin.CalendarSHA256;tzdb_sha256=$pin.TzifSHA256}
    }

    It 'keeps v1 startup as the default without V2.2 layering' {
        Enable-InferenceStartupContext $script:SelectionContext
        $script:SelectionContext.ModelVersion | Should Be 'v1'
        ($script:SelectionContext.ComposeArguments -join ' ') | Should Not Match 'pretrip-v22'
    }

    It 'hashes pipeline-prefixed inventory while preserving the original v1 algorithm' {
        $pipeline=Join-Path $TestDrive 'prefix-pipeline'
        $null=New-Item -ItemType Directory -Path $pipeline -Force
        $file=Join-Path $pipeline 'metadata.json'
        [IO.File]::WriteAllText($file,'{}',[Text.Encoding]::ASCII)
        $original=Get-FareModelTree $pipeline
        $observed=Get-PretripV22PipelineTree $pipeline
        $expected='[{"bytes":2,"path":"pipeline/metadata.json","sha256":"'+(Get-FileHash $file -Algorithm SHA256).Hash.ToLowerInvariant()+'"}]'
        $algorithm=[Security.Cryptography.SHA256]::Create()
        try {$hash=[BitConverter]::ToString($algorithm.ComputeHash([Text.Encoding]::UTF8.GetBytes($expected))).Replace('-','').ToLowerInvariant()}
        finally {$algorithm.Dispose()}
        $observed.sha256 | Should Be $hash
        $observed.sha256 | Should Not Be $original.sha256
        $observed.files[0].path | Should Be 'pipeline/metadata.json'
        $original.files[0].path | Should Be 'metadata.json'
    }

    It 'layers explicit V2.2 on the same service/project, never a second model service' {
        Enable-InferenceStartupContext $script:SelectionContext -ModelVersion 'v2.2'
        $script:SelectionContext.Services.Count | Should Be 4
        @($script:SelectionContext.Services | Where-Object {$_ -eq 'fare-inference'}).Count | Should Be 1
        $script:SelectionContext.ModelVersion | Should Be 'v2.2'
        $script:SelectionContext.ComposeArguments[-1] | Should Match 'docker-compose.pretrip-v22.yml$'
    }

    It 'includes all three layers in its separate drift-update instruction' {
        Enable-InferenceStartupContext $script:SelectionContext -ModelVersion 'v2.2'
        $command=Get-InferenceUpdateCommand $script:SelectionContext 'dashboard'
        $command | Should Match 'docker-compose.yml'
        $command | Should Match 'docker-compose.inference.yml'
        $command | Should Match 'docker-compose.pretrip-v22.yml'
        $command | Should Match '--no-deps --no-build --pull never dashboard'
    }

    It 'accepts exact V2.2 readiness only after artifact verification' {
        (Assert-PretripV22Readiness $script:SelectionExpected $script:SelectionReady).ready | Should Be $true
    }

    foreach($field in @('model_sha256','artifact_manifest_sha256','dictionary_sha256','model_version',
        'contract_version','calendar_version','calendar_sha256','tzdb_sha256')) {
        It ('rejects mismatched readiness '+$field+' without v1 fallback') {
            param($identityField)
            $script:SelectionReady.$identityField='different'
            {Assert-PretripV22Readiness $script:SelectionExpected $script:SelectionReady} | Should Throw
        } -TestCases @(@{identityField=$field})
    }

    It 'rejects false readiness, unverified artifacts, nonnumeric load or fit counts and any refit' {
        $script:SelectionReady.ready=$false
        {Assert-PretripV22Readiness $script:SelectionExpected $script:SelectionReady} | Should Throw
        $script:SelectionReady.ready=$true
        $script:SelectionExpected.Verified=$false
        {Assert-PretripV22Readiness $script:SelectionExpected $script:SelectionReady} | Should Throw
        $script:SelectionExpected.Verified=$true
        $script:SelectionReady.model_load_count='1'
        {Assert-PretripV22Readiness $script:SelectionExpected $script:SelectionReady} | Should Throw
        $script:SelectionReady.model_load_count=1
        $script:SelectionReady.fit_calls='0'
        {Assert-PretripV22Readiness $script:SelectionExpected $script:SelectionReady} | Should Throw
        $script:SelectionReady.fit_calls=1
        {Assert-PretripV22Readiness $script:SelectionExpected $script:SelectionReady} | Should Throw
        $script:SelectionReady.fit_calls=0
        (Assert-PretripV22Readiness $script:SelectionExpected $script:SelectionReady).ready | Should Be $true
    }

    It 'rejects a missing bundle before any Docker calls or v1 artifact check' {
        Mock Resolve-DockerCli {'docker.exe'}
        Mock Invoke-DockerCommand {throw 'No Docker call allowed'}
        Mock Assert-FareArtifacts {throw 'No v1 fallback allowed'}
        $null=New-Item -ItemType File -Path (Join-Path $script:SelectionRoot 'docker-compose.yml') -Force
        $result=Invoke-ProjectStart -ProjectRoot $script:SelectionRoot -WithInference -ModelVersion 'v2.2'
        $result | Should Be 1
        Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly
        Assert-MockCalled Assert-FareArtifacts -Times 0 -Exactly
    }

    It 'rejects a modified version lock rather than trusting its replacement model hash' {
        $config=Join-Path $script:SelectionRoot 'config'
        $null=New-Item -ItemType Directory -Path $config -Force
        [IO.File]::WriteAllText((Join-Path $config 'model_bundle_v22.lock.json'),'{"model_version":"v2.2","model_tree_sha256":"different"}')
        {Assert-PretripV22Artifacts -ProjectRoot $script:SelectionRoot} | Should Throw
    }
}

Describe 'V2.2 package selection refuses unsupported bootstrap' {
    . (Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts/setup_system.ps1')

    It 'rejects V2.2 before package context, Docker, configuration, bootstrap or model installation' {
        Mock New-PackageEnvironmentContext {throw 'Package context must not be constructed'}
        Mock Resolve-DockerCli {throw 'Docker must not be resolved'}
        Mock Invoke-DockerCommand {throw 'Docker must not be called'}
        Mock Get-PackagePreflight {throw 'Package preflight must not run'}
        Mock Get-PackageBundleSelection {throw 'Package bundle selection must not run'}
        Mock Initialize-PackageConfiguration {throw 'No environment/configuration writes allowed'}
        Mock Enter-PackageExecutionLock {throw 'No execution lock allowed'}
        Mock Read-PackageCheckpoint {throw 'No checkpoint may be read'}
        Mock Write-PackageCheckpoint {throw 'No checkpoint may be written'}
        Mock Ensure-PackageImages {throw 'No image operation allowed'}
        Mock Start-PackagePhase {throw 'No service operation allowed'}
        Mock Invoke-DataBootstrap {throw 'No bootstrap allowed'}
        Mock Install-PackageModelBundle {throw 'No model installation allowed'}
        Mock Test-PackageFullValidation {throw 'No database/model validation allowed'}
        $absentRoot=Join-Path $TestDrive 'unsupported-v22-new-machine'
        Invoke-SystemSetup -ProjectRoot $absentRoot -ModelVersion 'v2.2' -Execute | Should Be 1
        Test-Path -LiteralPath $absentRoot | Should Be $false
        foreach($commandName in @('New-PackageEnvironmentContext','Resolve-DockerCli','Invoke-DockerCommand',
            'Get-PackagePreflight','Get-PackageBundleSelection','Initialize-PackageConfiguration',
            'Enter-PackageExecutionLock','Read-PackageCheckpoint','Write-PackageCheckpoint','Ensure-PackageImages',
            'Start-PackagePhase','Invoke-DataBootstrap','Install-PackageModelBundle','Test-PackageFullValidation')) {
            Assert-MockCalled -CommandName $commandName -Times 0 -Exactly -Scope It
        }
    }
}
