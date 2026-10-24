# Pester 3.4 / Windows PowerShell 5.1. All Docker access is mocked; no services run.
$packageTestProject = Split-Path -Parent $PSScriptRoot
. (Join-Path $packageTestProject 'scripts/start.ps1')

function ConvertTo-PackageFixture {
    param($Value)
    $Value | ConvertTo-Json -Depth 30 -Compress | ConvertFrom-Json
}

function New-PackageFixtureModel {
    param($Context)
    $runtime = $Context.RuntimeRoot
    $modelRoot = $Context.ModelRuntimeRoot
    $project = $Context.ExpectedProject
    $binds = @(foreach ($directory in @('data', 'logs', 'models', 'reports')) {
        @{ type='bind'; source=(Join-Path $runtime $directory); target="/app/$directory"; read_only=$false }
    })
    $targets = [ordered]@{
        '/model'='models/ml-p2/20261023-pilot02/fitted_model'
        '/model-contract/candidate_lock.json'='models/ml-p3/20261023-june01/candidate_lock.json'
        '/model-contract/feature_contract.json'='models/ml-p2/20261023-pilot02/feature_contract.json'
        '/model-contract/zones.csv'='data/raw/taxi_zones/taxi_zone_lookup.csv'
    }
    $artifacts = @(foreach ($target in $targets.Keys) {
        @{ type='bind'; source=(Join-Path $modelRoot $targets[$target]); target=$target; read_only=$true; bind=@{create_host_path=$false} }
    })
    $environment = @{
        POSTGRES_HOST='postgres'; POSTGRES_PORT='5432'; POSTGRES_DB='nyc_taxi_package_test'
        POSTGRES_USER='package_test'; POSTGRES_PASSWORD='package-fixture-only'
        MLFLOW_TRACKING_URI='http://mlflow:5000'
        JAVA_TOOL_OPTIONS='-XX:ActiveProcessorCount=2'
        PYSPARK_SUBMIT_ARGS='--driver-memory 1536m --conf spark.ui.enabled=false pyspark-shell'
        OPENBLAS_NUM_THREADS='1'; OMP_NUM_THREADS='1'
    }
    $value = @{
        name=$project
        services=@{
            postgres=@{
                image='postgres:16@sha256:1a6ab3f5345eb6dbe04a1349529caabdb0ab09293a09590fad07b2246bfa4b54'
                ports=@(); networks=@{default=$null}; restart='no'
                environment=@{POSTGRES_DB='nyc_taxi_package_test';POSTGRES_USER='package_test';POSTGRES_PASSWORD='package-fixture-only'}
                volumes=@(
                    @{type='volume';source='postgres-data';target='/var/lib/postgresql/data'}
                    @{type='bind';source=(Join-Path $Context.Root 'sql');target='/docker-entrypoint-initdb.d';read_only=$true}
                )
            }
            mlflow=@{
                image='ghcr.io/mlflow/mlflow:v3.5.1@sha256:7a8c21cd553b84227391ad7daa39c029dab64328da7607c372370cfa07ef43d9'
                restart='no';networks=@{default=$null}
                command=@('mlflow','server','--backend-store-uri','sqlite:////mlflow/mlflow.db','--default-artifact-root','/mlflow/artifacts','--host','0.0.0.0','--port','5000')
                ports=@(@{target=5000;published='15000';host_ip='127.0.0.1';protocol='tcp'})
                volumes=@(@{type='volume';source='mlflow-data';target='/mlflow'})
            }
            worker=@{
                image='nyc-taxi-worker:package-p1-standard';profiles=@('jobs');working_dir='/app'
                build=@{context=$Context.Root;dockerfile='Dockerfile.spark'}
                cpus=2.0;mem_limit=3221225472;environment=$environment
                command=@('python','scripts/check_worker_services.py');restart='no'
                volumes=$binds;ports=@();networks=@{default=$null}
            }
            dashboard=@{
                image='nyc-taxi-dashboard:package-p1'
                build=@{context=$Context.Root;dockerfile='Dockerfile.dashboard'}
                ports=@(@{target=8501;published='18501';host_ip='127.0.0.1';protocol='tcp'})
                environment=@{POSTGRES_HOST='postgres';POSTGRES_PORT='5432';POSTGRES_DB='nyc_taxi_package_test';POSTGRES_USER='package_test';POSTGRES_PASSWORD='package-fixture-only';FARE_INFERENCE_URL='http://fare-inference:8000'}
                volumes=@();networks=@{default=$null;inference=$null}
            }
            'fare-inference'=@{
                image='nyc-taxi-inference:package-p1'
                build=@{context=$Context.Root;dockerfile='Dockerfile.inference';args=@{WORKER_RUNTIME_IMAGE='nyc-taxi-worker:package-p1-standard'}}
                cpus=2.0;mem_limit=3221225472;read_only=$true;cap_drop=@('ALL')
                environment=@{JAVA_TOOL_OPTIONS='-XX:ActiveProcessorCount=2';PYSPARK_SUBMIT_ARGS='--driver-memory 1536m --conf spark.ui.enabled=false pyspark-shell';SPARK_LOCAL_IP='127.0.0.1';SPARK_LOCAL_DIRS='/tmp/ml-p4-spark';OPENBLAS_NUM_THREADS='1';OMP_NUM_THREADS='1'}
                tmpfs=@('/tmp:rw,exec,nosuid,nodev,size=1073741824,mode=1777');restart='no'
                security_opt=@('no-new-privileges:true');ports=@();networks=@{inference=$null};volumes=$artifacts
            }
        }
        volumes=@{
            'postgres-data'=@{name=($project+'_postgres-data');driver='local';external=$false}
            'mlflow-data'=@{name=($project+'_mlflow-data');driver='local';external=$false}
        }
        networks=@{
            default=@{name=($project+'_default');driver='bridge';external=$false;internal=$false}
            inference=@{name=($project+'_inference');driver='bridge';external=$false;internal=$true}
        }
    }
    if ($Context.Environment -eq 'current') {
        $value.services.postgres.container_name='nyc-taxi-postgres'
        $value.services.mlflow.container_name='nyc-taxi-mlflow'
        $value.services.mlflow.ports[0].published='5000'
        $value.services.mlflow.ports[0].host_ip='127.0.0.1'
        $value.services.dashboard.ports[0].published='8501'
    }
    ConvertTo-PackageFixture $value
}

function New-PackageForeignContainer {
    param([string]$Project='nyc-taxi-bigdata',[string]$Source,[string]$HostPort)
    $ports=@{}
    if ($HostPort) {$ports['5000/tcp']=@(@{HostPort=$HostPort;HostIp='127.0.0.1'})}
    $mounts=@()
    if ($Source) {$mounts=@(@{Type='bind';Source=$Source;RW=$true})}
    ConvertTo-PackageFixture @{
        ID='foreign-fixture-id';Name='foreign-fixture-container';Project=$Project;Service='mlflow'
        Mounts=$mounts;Ports=$ports;Networks=@{($Project+'_default')=@{}}
    }
}

function New-PackageOwnedContainer {
    param([string]$Service)
    $settings=$script:PackageModel.services.$Service
    $project=$script:PackageContext.ExpectedProject
    $mounts=@(foreach($mount in @(Get-OptionalProperty $settings 'volumes' @())){
        $snapshot=@{Type=$mount.type;Destination=$mount.target;RW=(-not (Get-OptionalProperty $mount 'read_only' $false))}
        if($mount.type -eq 'bind'){$snapshot.Source=$mount.source}else{$snapshot.Name=$project+'_'+$mount.source}
        $snapshot
    })
    if($Service -eq 'fare-inference'){$mounts+=@{Type='tmpfs';Destination='/tmp';RW=$true}}
    $networks=@{}
    foreach($network in $settings.networks.PSObject.Properties){$networks[$project+'_'+$network.Name]=@{}}
    $ports=@{}
    foreach($port in @(Get-OptionalProperty $settings 'ports' @())){
        $ports[([string]$port.target)+'/tcp']=@(@{HostPort=[string]$port.published;HostIp=$port.host_ip})
    }
    ConvertTo-PackageFixture @{
        ID=('owned-'+$Service);Name=($project+'-'+$Service+'-1');Project=$project;Service=$Service
        Mounts=$mounts;Networks=$networks;Ports=$ports
    }
}

Describe 'PACKAGE P1 shared context and isolation safety (Docker metadata mocked)' {
    BeforeEach {
        $script:PackageFixtureRoot=Join-Path $TestDrive 'package project with spaces'
        foreach ($directory in @('sql','data','logs','models','reports','.sandbox/package-test','.sandbox/bootstrap-test','.sandbox/fixture-test')) {
            [IO.Directory]::CreateDirectory((Join-Path $script:PackageFixtureRoot $directory)) | Out-Null
        }
        $script:PackageContext=New-PackageEnvironmentContext -ProjectRoot $script:PackageFixtureRoot -Environment package-test
        $script:PackageModel=New-PackageFixtureModel $script:PackageContext
        $script:PackageInventory=[pscustomobject]@{Containers=@();Volumes=@();Networks=@()}
        $script:PackageBlockedReparsePath=$null
        Mock Get-EnvironmentDockerInventory {$script:PackageInventory}
        Mock Test-Path {$true} -ParameterFilter {$script:PackageBlockedReparsePath -and $LiteralPath -eq $script:PackageBlockedReparsePath}
        Mock Get-Item {[pscustomobject]@{Attributes=[IO.FileAttributes]::ReparsePoint}} -ParameterFilter {$script:PackageBlockedReparsePath -and $LiteralPath -eq $script:PackageBlockedReparsePath}
        Mock Invoke-DockerCommand {throw 'Real Docker must never run in package safety tests.'}
        Mock Invoke-StartupDocker {throw 'Real Docker must never run in package safety tests.'}
        Mock Invoke-StartupCompose {throw 'Real Compose must never run in package safety tests.'}
    }

    It 'preserves the ordinary current context without package layers or new services' {
        $context=New-ProjectEnvironmentContext -ProjectRoot $script:PackageFixtureRoot
        $context.Environment | Should Be 'current'
        $context.RuntimeRoot | Should Be $script:PackageFixtureRoot
        $context.EnvPath | Should Be (Join-Path $script:PackageFixtureRoot '.env')
        $context.IsPackage | Should Be $false
        ($context.Services -join ',') | Should Be 'postgres,mlflow,dashboard'
        ($context.ComposeArguments -join ' ') | Should Not Match 'docker-compose.package'
    }

    It 'selects four ordered package-test layers and dedicated roots without creating configuration' {
        $context=$script:PackageContext
        $context.Project | Should Be 'nyc-taxi-package-test'
        $context.RuntimeRoot | Should Be (Join-Path $script:PackageFixtureRoot '.sandbox/package-test')
        $context.ModelRuntimeRoot | Should Be (Join-Path $context.RuntimeRoot 'model-bundle')
        $context.StateRoot | Should Be (Join-Path $context.RuntimeRoot 'package-state')
        $context.ExpectedWorkerImage | Should Be 'nyc-taxi-worker:package-p1-standard'
        $context.IsPackage | Should Be $true
        ($context.Services -join ',') | Should Be 'postgres,mlflow'
        $files=@('docker-compose.yml','docker-compose.inference.yml','docker-compose.package.yml','docker-compose.package-test.yml')
        $positions=@($files | ForEach-Object {[Array]::IndexOf(@($context.ComposeArguments),(Join-Path $script:PackageFixtureRoot $_))})
        ($positions[0] -ge 0 -and $positions[1] -gt $positions[0] -and $positions[2] -gt $positions[1] -and $positions[3] -gt $positions[2]) | Should Be $true
        (Test-Path -LiteralPath $context.EnvPath) | Should Be $false
        Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
    }

    It 'wraps the explicit current package context without changing its project or runtime data root' {
        $context=New-PackageEnvironmentContext -ProjectRoot $script:PackageFixtureRoot
        $context.Project | Should Be 'nyc-taxi-bigdata'
        $context.RuntimeRoot | Should Be $script:PackageFixtureRoot
        $context.ModelRuntimeRoot | Should Be (Join-Path $script:PackageFixtureRoot '.sandbox/package-model')
        $context.IsPackage | Should Be $true
        ($context.ComposeArguments -join ' ') | Should Match 'docker-compose.package.yml'
        ($context.ComposeArguments -join ' ') | Should Not Match 'docker-compose.package-test.yml'
    }

    It 'rejects the old clean variant for package-test rather than retagging p6-clean' {
        {New-ProjectEnvironmentContext -ProjectRoot $script:PackageFixtureRoot -Environment package-test -WorkerVariant clean} | Should Throw
        Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
    }

    It 'accepts the complete fresh isolated fixture and the shared guard dispatcher' {
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Not Throw
        {Assert-ProjectEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Not Throw
        Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
    }

    It 'accepts a reviewed current package model with preserved project storage and internal PostgreSQL' {
        $current=New-PackageEnvironmentContext -ProjectRoot $script:PackageFixtureRoot
        $model=New-PackageFixtureModel $current
        {Assert-PackageEnvironmentIsolation $current $model} | Should Not Throw
        Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
    }

    It 'accepts existing owned containers only with their actual isolated attachments' {
        $script:PackageInventory.Containers=@('postgres','mlflow','worker','dashboard','fare-inference' | ForEach-Object {New-PackageOwnedContainer $_})
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Not Throw
    }

    It 'rejects junctions in every runtime bind leaf despite a safe runtime root' {
        foreach($directory in @('data','logs','models','reports')){
            $script:PackageBlockedReparsePath=Get-NormalizedEnvironmentPath (Join-Path $script:PackageContext.RuntimeRoot $directory)
            {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
        }
        Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
    }

    It 'rejects an existing owned bind junction using Docker Desktop Linux path spelling' {
        $owned=New-PackageOwnedContainer 'worker'
        $script:PackageBlockedReparsePath=Get-NormalizedEnvironmentPath $owned.Mounts[0].Source
        $path=$script:PackageBlockedReparsePath
        $owned.Mounts[0].Source='/run/desktop/mnt/host/'+$path.Substring(0,1)+'/'+$path.Substring(3).Replace('\','/')
        $script:PackageInventory.Containers=@($owned)
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
        Assert-MockCalled Get-Item -Times 1 -Exactly -Scope It -ParameterFilter {$LiteralPath -eq $script:PackageBlockedReparsePath}
    }

    It 'rejects an initializer runtime child junction before creating credentials' {
        $script:PackageBlockedReparsePath=Join-Path $script:PackageContext.RuntimeRoot 'data'
        {Initialize-PackageConfiguration $script:PackageContext} | Should Throw
        (Test-Path -LiteralPath $script:PackageContext.EnvPath) | Should Be $false
        Assert-MockCalled Get-Item -Times 1 -Exactly -Scope It -ParameterFilter {$LiteralPath -eq $script:PackageBlockedReparsePath}
    }

    It 'rejects a model project different from the dedicated package project' {
        $script:PackageModel.name='nyc-taxi-bigdata'
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects a runtime root borrowed from bootstrap-test or fixture data' {
        $script:PackageContext.RuntimeRoot=Join-Path $script:PackageFixtureRoot '.sandbox/bootstrap-test'
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects a model root outside the package-test model bundle' {
        $script:PackageContext.ModelRuntimeRoot=Join-Path $script:PackageFixtureRoot 'models'
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects an external network' {
        $script:PackageModel.networks.default.external=$true
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects a non-internal inference network' {
        $script:PackageModel.networks.inference.internal=$false
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects a network named for the live project' {
        $script:PackageModel.networks.default.name='nyc-taxi-bigdata_default'
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects network driver options and custom drivers' {
        $script:PackageModel.networks.default | Add-Member driver_opts @{parent='foreign-interface'} -Force
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
        $script:PackageModel=New-PackageFixtureModel $script:PackageContext
        $script:PackageModel.networks.default.driver='overlay'
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects an existing network owned by another project' {
        $script:PackageInventory.Networks=@([pscustomobject]@{Name='nyc-taxi-package-test_default';Project='foreign';Driver='bridge';HasOptions=$false;OnlyIPFamilyOptions=$true})
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects external or foreign-named volumes' {
        $script:PackageModel.volumes.'postgres-data'.external=$true
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
        $script:PackageModel=New-PackageFixtureModel $script:PackageContext
        $script:PackageModel.volumes.'postgres-data'.name='nyc-taxi-bigdata_postgres-data'
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects redirected volumes and non-local drivers' {
        $script:PackageModel.volumes.'postgres-data' | Add-Member driver_opts @{device='foreign-path'} -Force
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
        $script:PackageModel=New-PackageFixtureModel $script:PackageContext
        $script:PackageModel.volumes.'postgres-data'.driver='foreign-plugin'
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects existing package volume names with foreign ownership' {
        $script:PackageInventory.Volumes=@([pscustomobject]@{Name='nyc-taxi-package-test_postgres-data';Project='nyc-taxi-bigdata';Driver='local';HasOptions=$false})
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects fixed container names in package-test' {
        $script:PackageModel.services.postgres | Add-Member container_name 'nyc-taxi-postgres' -Force
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects public-interface ports and unexpected inference publication' {
        $script:PackageModel.services.dashboard.ports[0].host_ip='0.0.0.0'
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
        $script:PackageModel=New-PackageFixtureModel $script:PackageContext
        $script:PackageModel.services.'fare-inference'.ports=@([pscustomobject]@{target=8000;published='18000';host_ip='127.0.0.1';protocol='tcp'})
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects a host port occupied by another project' {
        $script:PackageInventory.Containers=@(New-PackageForeignContainer -HostPort '18501')
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects incorrect container port targets or non-TCP publication' {
        $script:PackageModel.services.dashboard.ports[0].target=5432
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
        $script:PackageModel=New-PackageFixtureModel $script:PackageContext
        $script:PackageModel.services.dashboard.ports[0].protocol='udp'
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects a worker writable bind pointing at live data' {
        $script:PackageModel.services.worker.volumes[0].source=Join-Path $script:PackageFixtureRoot 'data'
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects a worker writable bind pointing at fixture/bootstrap-test data' {
        $script:PackageModel.services.worker.volumes[0].source=Join-Path $script:PackageFixtureRoot '.sandbox/bootstrap-test/data'
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects test runtime overlaps with a foreign writable parent mount' {
        $script:PackageInventory.Containers=@(New-PackageForeignContainer -Source $script:PackageContext.RuntimeRoot)
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'keeps distinct foreign fixture/bootstrap mounts isolated without false collisions' {
        $script:PackageInventory.Containers=@(New-PackageForeignContainer -Project 'nyc-taxi-bootstrap-test' -Source (Join-Path $script:PackageFixtureRoot '.sandbox/bootstrap-test/data'))
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Not Throw
    }
    It 'rejects model artifacts outside the dedicated read-only bundle' {
        $script:PackageModel.services.'fare-inference'.volumes[0].source=Join-Path $script:PackageFixtureRoot 'models/ml-p2/20261023-pilot02/fitted_model'
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects writable model artifacts' {
        $script:PackageModel.services.'fare-inference'.volumes[0].read_only=$false
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects absent worker or inference mounts' {
        $script:PackageModel.services.worker.volumes=@($script:PackageModel.services.worker.volumes | Select-Object -First 3)
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
        $script:PackageModel=New-PackageFixtureModel $script:PackageContext
        $script:PackageModel.services.'fare-inference'.volumes=@()
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects database volumes mounted into a worker runtime slot' {
        $script:PackageModel.services.worker.volumes[0]=[pscustomobject]@{type='volume';source='postgres-data';target='/app/data'}
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects database volumes mounted into an inference artifact slot' {
        $script:PackageModel.services.'fare-inference'.volumes[0]=[pscustomobject]@{type='volume';source='mlflow-data';target='/model';read_only=$true}
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects duplicate runtime targets even when total mount count is four' {
        $script:PackageModel.services.worker.volumes[1]=$script:PackageModel.services.worker.volumes[0]
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects duplicate artifact targets even when total mount count is four' {
        $script:PackageModel.services.'fare-inference'.volumes[1]=$script:PackageModel.services.'fare-inference'.volumes[0]
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects a service attached to a foreign or undeclared network' {
        $script:PackageModel.services.worker.networks=[pscustomobject]@{'nyc-taxi-bigdata_default'=$null}
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects inference on the database network even if only known networks are used' {
        $script:PackageModel.services.'fare-inference'.networks=[pscustomobject]@{default=$null;inference=$null}
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects unknown extra services and missing required services' {
        $script:PackageModel.services | Add-Member surprise ([pscustomobject]@{image='fixture-only'}) -Force
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
        $script:PackageModel=New-PackageFixtureModel $script:PackageContext
        $script:PackageModel.services.PSObject.Properties.Remove('mlflow')
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects privileged service execution' {
        $script:PackageModel.services.worker | Add-Member privileged $true -Force
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects an arbitrary worker image or redirected build context' {
        $script:PackageModel.services.worker.image='nyc-taxi-worker:p2'
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
        $script:PackageModel=New-PackageFixtureModel $script:PackageContext
        $script:PackageModel.services.worker.build.context=Join-Path $TestDrive 'foreign-source'
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }
    It 'rejects an existing same-project container with a writable live-data attachment' {
        $owned=New-PackageForeignContainer -Project 'nyc-taxi-package-test' -Source (Join-Path $script:PackageFixtureRoot 'data')
        $script:PackageInventory.Containers=@($owned)
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }

    It 'rejects an actual live PostgreSQL volume attached behind package ownership labels' {
        $owned=New-PackageOwnedContainer 'postgres'
        $owned.Mounts[0].Name='nyc-taxi-bigdata_postgres-data'
        $script:PackageInventory.Containers=@($owned)
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }

    It 'rejects actual foreign network attachments despite matching package labels' {
        $owned=New-PackageOwnedContainer 'mlflow'
        $owned.Networks=[pscustomobject]@{'nyc-taxi-bigdata_default'=[pscustomobject]@{}}
        $script:PackageInventory.Containers=@($owned)
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }

    It 'rejects actual non-loopback published ports despite matching package labels' {
        $owned=New-PackageOwnedContainer 'dashboard'
        $owned.Ports.'8501/tcp'[0].HostIp='0.0.0.0'
        $script:PackageInventory.Containers=@($owned)
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }

    It 'rejects package volume names simultaneously mounted by a foreign project' {
        $foreign=New-PackageForeignContainer
        $foreign.Mounts=@([pscustomobject]@{Type='volume';Name='nyc-taxi-package-test_postgres-data';Destination='/foreign';RW=$true})
        $script:PackageInventory.Containers=@($foreign)
        {Assert-PackageEnvironmentIsolation $script:PackageContext $script:PackageModel} | Should Throw
    }

    It 'requires loopback publication in the current package model too' {
        $current=New-PackageEnvironmentContext -ProjectRoot $script:PackageFixtureRoot
        $model=New-PackageFixtureModel $current
        $model.services.mlflow.ports[0].host_ip='0.0.0.0'
        {Assert-PackageEnvironmentIsolation $current $model} | Should Throw
    }
}

Describe 'PACKAGE P1 preflight prerequisites (no native Docker or resource processes)' {
    BeforeEach {
        $script:PackagePrerequisiteRoot=Join-Path $TestDrive 'package prerequisites'
        [IO.Directory]::CreateDirectory($script:PackagePrerequisiteRoot) | Out-Null
        $script:PackagePrerequisiteContext=New-PackageEnvironmentContext -ProjectRoot $script:PackagePrerequisiteRoot -Environment package-test
        Mock Invoke-DockerCommand {throw 'Real Docker is prohibited in prerequisite tests.'}
        Mock Invoke-StartupCompose {throw 'Compose resolution must not run before prerequisite acceptance.'}
        Mock Get-CimInstance {throw 'Host resources must not be queried for these rejected prerequisites.'}
    }

    It 'rejects a missing Docker CLI before daemon/configuration/resource checks' {
        Mock Resolve-DockerCli {$null}
        Mock Invoke-StartupDocker {throw 'Daemon checks must not run when the CLI is absent.'}
        {Get-PackagePreflight $script:PackagePrerequisiteContext} | Should Throw
        Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
        Assert-MockCalled Invoke-StartupDocker -Times 0 -Exactly -Scope It
        Assert-MockCalled Get-CimInstance -Times 0 -Exactly -Scope It
    }

    It 'rejects Compose lacking required merge tags before host resource/configuration checks' {
        Mock Resolve-DockerCli {'mock-docker.exe'}
        Mock Invoke-StartupDocker {
            param($Context,$Arguments,$FailureMessage,$InputText,$TimeoutSeconds)
            if ($Arguments[0] -eq 'info') {return '{"CPUs":2,"MemoryBytes":8589934592,"OSType":"linux"}'}
            if ($Arguments[0] -eq 'compose' -and $Arguments[1] -eq 'version') {return '2.23.0'}
            throw 'Unexpected mocked prerequisite command.'
        }
        {Get-PackagePreflight $script:PackagePrerequisiteContext} | Should Throw
        Assert-MockCalled Invoke-DockerCommand -Times 0 -Exactly -Scope It
        Assert-MockCalled Invoke-StartupCompose -Times 0 -Exactly -Scope It
        Assert-MockCalled Get-CimInstance -Times 0 -Exactly -Scope It
    }
}
