# PACKAGE context/guards only. No actions while dot-sourcing.
function New-PackageEnvironmentContext {
    param([string]$ProjectRoot,[ValidateSet('current','package-test')][string]$Environment='current',
        [int]$CommandTimeoutSeconds=30,[int]$StepTimeoutSeconds=21600)
    $ctx = New-ProjectEnvironmentContext -ProjectRoot $ProjectRoot -Environment $Environment -CommandTimeoutSeconds $CommandTimeoutSeconds -HealthTimeoutSeconds 180
    if ($Environment -eq 'current') {
        $ctx.ComposeArguments += @('-f',(Join-Path $ctx.Root 'docker-compose.inference.yml'),'-f',(Join-Path $ctx.Root 'docker-compose.package.yml'),'--project-name','nyc-taxi-bigdata')
        $ctx.ExpectedProject='nyc-taxi-bigdata'
        $ctx.ExpectedWorkerImage='nyc-taxi-worker:package-p1-standard'
        $ctx.Services=@('postgres','mlflow')
        $ctx.BuildServices=@('worker','dashboard','fare-inference')
        $ctx | Add-Member ModelRuntimeRoot (Join-Path $ctx.Root '.sandbox/package-model') -Force
    }
    $ctx.Project=$ctx.ExpectedProject
    $ctx | Add-Member IsPackage $true -Force
    $ctx | Add-Member StateRoot (Join-Path $ctx.Root ('.sandbox/' + $(if ($Environment -eq 'current') {'package-current'} else {'package-test'}) + '/package-state')) -Force
    $ctx | Add-Member StepTimeoutSeconds $StepTimeoutSeconds -Force
    $ctx | Add-Member InferenceArtifacts $null -Force
    return $ctx
}

function Assert-PackageEnvironmentIsolation {
    param($Context,$Model)
    $test = $Context.Environment -eq 'package-test'
    $project = if ($test) {'nyc-taxi-package-test'} else {'nyc-taxi-bigdata'}
    $runtime = if ($test) {Join-Path $Context.Root '.sandbox/package-test'} else {$Context.Root}
    $modelRoot = if ($test) {Join-Path $runtime 'model-bundle'} else {Join-Path $Context.Root '.sandbox/package-model'}
    if ($Model.name -ne $project -or $Context.ExpectedProject -ne $project -or
        (Get-NormalizedEnvironmentPath $Context.RuntimeRoot) -ne (Get-NormalizedEnvironmentPath $runtime) -or
        (Get-NormalizedEnvironmentPath $Context.ModelRuntimeRoot) -ne (Get-NormalizedEnvironmentPath $modelRoot)) { Stop-Startup 'Package project/runtime/model root is not the dedicated reviewed layout.' }
    foreach ($path in @($runtime,$modelRoot,$Context.EnvPath,$Context.StateRoot)) { Assert-NoEnvironmentReparsePoint $path }
    $inventory = Get-EnvironmentDockerInventory $Context
    $serviceNames=@($Model.services.PSObject.Properties | ForEach-Object Name)
    if ($serviceNames.Count -ne 5 -or @($serviceNames | Where-Object {$_ -notin @('postgres','mlflow','worker','dashboard','fare-inference')}).Count) { Stop-Startup 'Package must contain exactly its five reviewed services.' }
    $worker=$Model.services.worker
    $build=Get-OptionalProperty $worker 'build'
    if($worker.image -ne 'nyc-taxi-worker:package-p1-standard' -or $null -eq $build -or
        (Get-NormalizedEnvironmentPath $build.context) -ne (Get-NormalizedEnvironmentPath $Context.Root) -or
        (Get-OptionalProperty $build 'dockerfile' '') -ne 'Dockerfile.spark' -or (Get-OptionalProperty $build 'tags') -or
        (Get-OptionalProperty $build 'dockerfile_inline')) {Stop-Startup 'Package worker image/build context differs from the standard reviewed build.'}
    $targets=@{'/model'='models/ml-p2/20261023-pilot02/fitted_model';'/model-contract/candidate_lock.json'='models/ml-p3/20261023-june01/candidate_lock.json';'/model-contract/feature_contract.json'='models/ml-p2/20261023-pilot02/feature_contract.json';'/model-contract/zones.csv'='data/raw/taxi_zones/taxi_zone_lookup.csv'}
    # Labels alone are not proof of isolation: inspect actual attachments before
    # reusing any existing container, including stopped/one-off containers.
    foreach($live in @($inventory.Containers | Where-Object Project -eq $project)) {
        $service=Get-OptionalProperty $live 'Service' ''
        if($service -notin $serviceNames){Stop-Startup 'Existing package container has an unknown service.'}
        $actualMounts=@(Get-OptionalProperty $live 'Mounts' @())
        $persistent=@($actualMounts | Where-Object Type -ne 'tmpfs')
        $expectedCount=switch($service){'worker'{4};'fare-inference'{4};'postgres'{2};'mlflow'{1};default{0}}
        if($persistent.Count -ne $expectedCount -or @($actualMounts | Group-Object Destination | Where-Object Count -gt 1).Count){Stop-Startup 'Existing package container mount count/targets differ.'}
        foreach($mount in $actualMounts){
            $destination=Get-OptionalProperty $mount 'Destination' ''
            if($mount.Type -eq 'volume'){
                $shortName=if($service -eq 'postgres'){'postgres-data'}elseif($service -eq 'mlflow'){'mlflow-data'}else{''}
                $target=if($service -eq 'postgres'){'/var/lib/postgresql/data'}elseif($service -eq 'mlflow'){'/mlflow'}else{''}
                if(-not $shortName -or (Get-OptionalProperty $mount 'Name' '') -ne ($project+'_'+$shortName) -or $destination -ne $target -or -not $mount.RW){Stop-Startup 'Existing package container has a foreign/shared volume or unexpected volume target.'}
            }elseif($mount.Type -eq 'bind'){
                $source=Get-NormalizedEnvironmentPath $mount.Source
                Assert-NoEnvironmentReparsePoint $source
                if($service -eq 'worker'){
                    if($destination -notin @('/app/data','/app/logs','/app/models','/app/reports') -or -not $mount.RW -or
                        $source -ne (Get-NormalizedEnvironmentPath (Join-Path $runtime $destination.Substring(5)))){Stop-Startup 'Existing package worker has a non-runtime bind.'}
                }elseif($service -eq 'fare-inference'){
                    if(-not $targets.ContainsKey($destination) -or $mount.RW -or
                        $source -ne (Get-NormalizedEnvironmentPath (Join-Path $modelRoot $targets[$destination]))){Stop-Startup 'Existing package inference has a non-model/read-write bind.'}
                }elseif($service -ne 'postgres' -or $mount.RW -or $destination -ne '/docker-entrypoint-initdb.d' -or
                    $source -ne (Get-NormalizedEnvironmentPath (Join-Path $Context.Root 'sql'))){Stop-Startup 'Existing package container has an unexpected bind.'}
            }elseif($mount.Type -ne 'tmpfs' -or $service -ne 'fare-inference' -or $destination -ne '/tmp'){
                Stop-Startup 'Existing package container has an unsupported mount.'
            }
        }
        $expectedNetworks=@(if($service -eq 'fare-inference'){$project+'_inference'}elseif($service -eq 'dashboard'){$project+'_default';$project+'_inference'}else{$project+'_default'})
        $attached=Get-OptionalProperty $live 'Networks'
        $actualNetworks=@($(if($null -ne $attached){$attached.PSObject.Properties | ForEach-Object Name}))
        if($actualNetworks.Count -ne $expectedNetworks.Count -or @($actualNetworks | Where-Object {$_ -notin $expectedNetworks}).Count){Stop-Startup 'Existing package container has a foreign/shared network.'}
        $ports=Get-OptionalProperty $live 'Ports'
        $bindings=@($(if($null -ne $ports){$ports.PSObject.Properties}))
        $allowed=if($test){@{mlflow='15000';dashboard='18501'}}else{@{mlflow='5000';dashboard='8501'}}
        $portTargets=@{mlflow='5000/tcp';dashboard='8501/tcp'}
        if($bindings.Count -ne $(if($allowed.ContainsKey($service)){1}else{0})){Stop-Startup 'Existing package container publishes unexpected ports.'}
        foreach($binding in $bindings){
            if(-not $allowed.ContainsKey($service) -or $binding.Name -ne $portTargets[$service] -or @($binding.Value).Count -ne 1){Stop-Startup 'Existing package container has unexpected port bindings.'}
            foreach($value in @($binding.Value)){
                if([string]$value.HostPort -ne $allowed[$service] -or (Get-OptionalProperty $value 'HostIp' '') -ne '127.0.0.1'){Stop-Startup 'Existing package container publishes foreign/non-loopback ports.'}
            }
        }
    }
    $expectedVolumes=@('postgres-data','mlflow-data')
    if (@($Model.volumes.PSObject.Properties).Count -ne 2 -or @($Model.networks.PSObject.Properties).Count -ne 2) { Stop-Startup 'Package must use exactly two volumes and two project networks.' }
    foreach ($entry in $Model.volumes.PSObject.Properties) {
        $name = $project + '_' + $entry.Name
        if ($entry.Name -notin $expectedVolumes -or $entry.Value.name -ne $name -or
            (Get-OptionalProperty $entry.Value 'external' $false) -or (Get-OptionalProperty $entry.Value 'driver_opts') -or
            (Get-OptionalProperty $entry.Value 'driver' 'local') -ne 'local') { Stop-Startup 'Package volume is external/shared/redirected.' }
        foreach ($live in @($inventory.Volumes | Where-Object Name -eq $name)) {
            if ($live.Project -ne $project -or $live.HasOptions -or (Get-OptionalProperty $live 'Driver' 'local') -ne 'local') { Stop-Startup 'Package volume ownership/driver differs.' }
        }
        foreach($foreign in @($inventory.Containers | Where-Object Project -ne $project)){
            if(@($foreign.Mounts | Where-Object {$_.Type -eq 'volume' -and $_.Name -eq $name}).Count){Stop-Startup 'Package named volume is attached outside its project.'}
        }
    }
    foreach ($entry in $Model.networks.PSObject.Properties) {
        if ($entry.Name -notin @('default','inference') -or $entry.Value.name -ne ($project+'_'+$entry.Name) -or
            (Get-OptionalProperty $entry.Value 'external' $false) -or (Get-OptionalProperty $entry.Value 'driver_opts') -or
            (Get-OptionalProperty $entry.Value 'driver' 'bridge') -ne 'bridge') { Stop-Startup 'Package network is shared/redirected.' }
        foreach ($live in @($inventory.Networks | Where-Object Name -eq $entry.Value.name)) {
            if ($live.Project -ne $project -or (Get-OptionalProperty $live 'Driver' 'bridge') -ne 'bridge' -or ($live.HasOptions -and -not $live.OnlyIPFamilyOptions)) { Stop-Startup 'Package network ownership/options differ.' }
        }
    }
    $inferenceNetwork=Get-OptionalProperty $Model.networks 'inference'
    if (-not (Get-OptionalProperty $inferenceNetwork 'internal' $false)) { Stop-Startup 'Inference network must remain internal-only.' }
    foreach ($entry in $Model.services.PSObject.Properties) {
        $service=$entry.Name; $settings=$entry.Value
        if ($service -notin @('postgres','mlflow','worker','dashboard','fare-inference')) { Stop-Startup 'Unknown service in package model.' }
        $name=Get-OptionalProperty $settings 'container_name' ''
        if ($test -and $name) { Stop-Startup 'Package-test must not inherit fixed container names.' }
        foreach ($foreign in @($inventory.Containers | Where-Object {$_.Project -ne $project -and $name -and $_.Name -eq $name})) { Stop-Startup 'Package container name collides with another project.' }
        foreach ($key in @('network_mode','volumes_from','devices','privileged','external_links','pid','ipc','configs','secrets')) {
            if (Get-OptionalProperty $settings $key) { Stop-Startup 'Unsupported host/shared option in package service.' }
        }
        $attached=Get-OptionalProperty $settings 'networks'
        $expectedNetworks=@(if($service -eq 'fare-inference'){'inference'}elseif($service -eq 'dashboard'){'default';'inference'}else{'default'})
        $actualNetworks=@($(if($null -ne $attached){$attached.PSObject.Properties | ForEach-Object Name}))
        if ($actualNetworks.Count -ne $expectedNetworks.Count -or @($actualNetworks | Where-Object {$_ -notin $expectedNetworks}).Count) { Stop-Startup 'Package service attaches to an unexpected network.' }
        $mounts=@(Get-OptionalProperty $settings 'volumes' @())
        $expectedMountCount=switch($service){'worker'{4};'fare-inference'{4};'postgres'{2};'mlflow'{1};default{0}}
        if($mounts.Count -ne $expectedMountCount -or @($mounts | Group-Object target | Where-Object Count -gt 1).Count){Stop-Startup 'Package mount count/target uniqueness differs.'}
        foreach ($mount in @(Get-OptionalProperty $settings 'volumes' @())) {
            if ($mount.type -eq 'volume') {
                if (($service -eq 'postgres' -and ($mount.source -ne 'postgres-data' -or $mount.target -ne '/var/lib/postgresql/data')) -or
                    ($service -eq 'mlflow' -and ($mount.source -ne 'mlflow-data' -or $mount.target -ne '/mlflow')) -or
                    $service -notin @('postgres','mlflow') -or (Get-OptionalProperty $mount 'read_only' $false)) { Stop-Startup 'Unexpected package named volume service/target.' }
            } elseif ($mount.type -eq 'bind') {
                $source=Get-NormalizedEnvironmentPath $mount.source
                Assert-NoEnvironmentReparsePoint $source
                $readOnly=Get-OptionalProperty $mount 'read_only' $false
                if ($service -eq 'worker') {
                    if ($mount.target -notin @('/app/data','/app/logs','/app/models','/app/reports') -or $readOnly -or
                        $source -ne (Get-NormalizedEnvironmentPath (Join-Path $runtime ($mount.target.Substring(5))))) { Stop-Startup 'Worker bind is not an approved runtime directory.' }
                } elseif ($service -eq 'fare-inference') {
                    if (-not $targets.ContainsKey($mount.target) -or -not $readOnly -or
                        $source -ne (Get-NormalizedEnvironmentPath (Join-Path $modelRoot $targets[$mount.target]))) { Stop-Startup 'Inference artifact bind is not pinned read-only model storage.' }
                } elseif ($service -ne 'postgres' -or -not $readOnly -or $source -ne (Get-NormalizedEnvironmentPath (Join-Path $Context.Root 'sql'))) { Stop-Startup 'Unexpected package bind mount.' }
                if ($test -and -not $readOnly) {
                    foreach ($foreign in @($inventory.Containers | Where-Object Project -ne $project)) {
                        foreach ($other in @($foreign.Mounts | Where-Object {$_.Type -eq 'bind' -and $_.RW})) {
                            if (Test-EnvironmentPathOverlap $mount.source $other.Source) { Stop-Startup 'Package writable directory overlaps another project.' }
                        }
                    }
                    foreach ($protected in @('data','logs','models','reports','.sandbox/bootstrap-test')) {
                        if (Test-EnvironmentPathOverlap $mount.source (Join-Path $Context.Root $protected)) { Stop-Startup 'Package-test overlaps live/fixture runtime.' }
                    }
                }
            } else { Stop-Startup 'Unsupported package mount type.' }
        }
        foreach ($port in @(Get-OptionalProperty $settings 'ports' @())) {
            $allowed=if ($test) {@{mlflow='15000';dashboard='18501'}} else {@{mlflow='5000';dashboard='8501'}}
            $portTargets=@{mlflow=5000;dashboard=8501}
            if (-not $allowed.ContainsKey($service) -or [string]$port.published -ne $allowed[$service] -or
                [int](Get-OptionalProperty $port 'target' 0) -ne $portTargets[$service] -or (Get-OptionalProperty $port 'protocol' 'tcp') -ne 'tcp' -or
                (Get-OptionalProperty $port 'host_ip' '') -ne '127.0.0.1') { Stop-Startup 'Unexpected package published port.' }
            foreach ($foreign in @($inventory.Containers | Where-Object Project -ne $project)) {
                foreach ($binding in @($(if ($null -ne $foreign.Ports) {$foreign.Ports.PSObject.Properties}))) {
                    if (@($binding.Value | Where-Object HostPort -eq ([string]$port.published)).Count) { Stop-Startup 'Package host port collides with another container.' }
                }
            }
        }
    }
    if (@($Model.services.worker.volumes).Count -ne 4 -or @($Model.services.'fare-inference'.volumes).Count -ne 4) { Stop-Startup 'Package runtime/artifact mount count differs.' }
    return
}

function Initialize-PackageConfiguration {
    param($Context)
    foreach ($path in @($Context.RuntimeRoot,$Context.StateRoot,(Split-Path $Context.EnvPath -Parent))) { Assert-NoEnvironmentReparsePoint $path; $null=New-Item -ItemType Directory -Path $path -Force }
    foreach ($name in @('data','logs','models','reports')) {
        $path=Join-Path $Context.RuntimeRoot $name
        Assert-NoEnvironmentReparsePoint $path
        $null=New-Item -ItemType Directory -Path $path -Force
    }
    if (Test-Path -LiteralPath $Context.EnvPath -PathType Leaf) { Write-Host 'Reusing configuration without printing/overwriting values.'; return }
    if (Test-Path -LiteralPath $Context.EnvPath) { Stop-Startup 'Configuration path is not a regular file.' }
    $rng=[Security.Cryptography.RandomNumberGenerator]::Create(); $bytes=New-Object byte[] 32
    try {$rng.GetBytes($bytes)} finally {$rng.Dispose()}
    $password=[BitConverter]::ToString($bytes).Replace('-','').ToLowerInvariant()
    if ($Context.Environment -eq 'package-test') { $text="PACKAGE_POSTGRES_PASSWORD=$password`n" }
    else { $text=[IO.File]::ReadAllText((Join-Path $Context.Root '.env.example')); $text=[regex]::Replace($text,'(?m)^POSTGRES_PASSWORD=.*$',('POSTGRES_PASSWORD='+$password)) }
    $stream=[IO.File]::Open($Context.EnvPath,[IO.FileMode]::CreateNew,[IO.FileAccess]::Write,[IO.FileShare]::None)
    try {$raw=[Text.UTF8Encoding]::new($false).GetBytes($text);$stream.Write($raw,0,$raw.Length)} finally {$stream.Dispose()}
    Write-Host 'Created missing local configuration with a generated password; no secret values printed.'
}

function Get-PackagePreflight {
    param($Context)
    $script:DockerCliPath=Resolve-DockerCli
    if (-not $script:DockerCliPath) { Stop-Startup 'Docker CLI missing. Install/open Docker Desktop before setup.' }
    $info=ConvertFrom-StartupJson (Invoke-StartupDocker $Context @('info','--format','{"CPUs":{{.NCPU}},"MemoryBytes":{{.MemTotal}},"OSType":{{json .OSType}}}') 'Docker Desktop daemon unavailable; open it and retry') 'Docker resources'
    $version=(Invoke-StartupDocker $Context @('compose','version','--short') 'Compose unavailable').Trim()
    if ($version -notmatch '^v?(\d+\.\d+\.\d+)' -or [version]$Matches[1] -lt [version]'2.24.4') { Stop-Startup 'Compose >=2.24.4 required.' }
    $drive=[IO.DriveInfo]::new([IO.Path]::GetPathRoot($Context.RuntimeRoot))
    $hostFree=$drive.AvailableFreeSpace
    $hostRam=[long](Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory
    Write-Host ('Measured: host free {0:N2} GiB; host RAM {1:N2} GiB; Docker RAM {2:N2} GiB / {3} CPUs.' -f ($hostFree/1GB),($hostRam/1GB),($info.MemoryBytes/1GB),$info.CPUs)
    Write-Host 'ESTIMATE full Jan-Jun bootstrap: reserve >=60 GiB physical free; 3-8+ hours, network/WAL/shuffle dependent. Not a measured E2E promise.'
    if ($info.OSType -ne 'linux' -or $info.CPUs -lt 2 -or $info.MemoryBytes -lt 7GB -or $hostRam -lt 8GB -or $hostFree -lt 60GB) { Stop-Startup 'Insufficient full-bootstrap budget (Linux engine, >=2 CPUs, >=7GiB Docker/8GiB host RAM and >=60GiB physical host free).' }
    $probe=$Context.PSObject.Copy()
    if (-not (Test-Path -LiteralPath $Context.EnvPath -PathType Leaf)) {
        if ($Context.Environment -eq 'package-test') { Stop-Startup 'Package-test config absent. Use -Environment package-test -PrepareTest (configuration only) first.' }
        $probe.ComposeArguments += @('--env-file',(Join-Path $Context.Root '.env.example'))
    }
    $null=Invoke-StartupCompose $probe @('--profile','*','config','--quiet') 'Package Compose validation failed; resolved values withheld'
    $model=ConvertFrom-StartupJson (Invoke-StartupCompose $probe @('--profile','*','config','--format','json') 'Cannot resolve package model') 'package Compose model'
    $null=Assert-PackageEnvironmentIsolation $Context $model
    $Context | Add-Member Model $model -Force
    $inventory=Get-EnvironmentDockerInventory $Context
    if (@($inventory.Containers | Where-Object {$_.Project -eq $Context.Project -and $_.Service -eq 'worker'}).Count -gt 0) { Stop-Startup 'A previous package worker exists. Inspect completion/rollback and ownership before retry; no overlapping data jobs are allowed.' }
    $owned=@($inventory.Containers | Where-Object Project -eq $Context.Project)
    $ownerPath=Join-Path $Context.StateRoot 'checkpoint.json'
    $legacy=($owned.Count -gt 0 -or @($inventory.Volumes | Where-Object Name -like ($Context.Project+'_*')).Count -gt 0) -and -not (Test-Path -LiteralPath $ownerPath)
    if ($legacy) { Write-Warning 'Existing resources have no package ownership checkpoint. PLAN only; Execute will refuse adoption/bootstrap. Use start.ps1 for the already prepared system.' }
    $listeners=@([Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners() | ForEach-Object Port)
    foreach ($service in $model.services.PSObject.Properties) {
        foreach ($port in @(Get-OptionalProperty $service.Value 'ports' @())) {
            if ($listeners -contains [int]$port.published -and @($owned | Where-Object Service -eq $service.Name).Count -eq 0) { Stop-Startup 'Required host port is occupied outside the selected service.' }
        }
    }
    return [pscustomobject]@{Verified=$true;Model=$model;OwnershipReady=(-not $legacy);CPUs=$info.CPUs;MemoryBytes=$info.MemoryBytes;HostFreeBytes=$hostFree;HostRamBytes=$hostRam}
}
