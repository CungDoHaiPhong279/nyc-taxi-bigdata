#requires -Version 5.1
<# Standalone pretrip V2.2 only. Dot-source for tests; PLAN performs read-only checks.
   Docker is the only host runtime. No database, bootstrap, worker job or training. #>
[CmdletBinding()]
param(
    [ValidateSet('app','test')][string]$Environment='app',
    [switch]$Execute,
    [ValidateRange(1,1800)][int]$HealthTimeoutSeconds=180,
    [ValidateRange(1,300)][int]$CommandTimeoutSeconds=30,
    [ValidateRange(60,7200)][int]$BuildTimeoutSeconds=1800,
    [ValidateRange(1,30)][int]$PollIntervalSeconds=2
)

$script:PretripAppCliOptions=@{Environment=$Environment;Execute=[bool]$Execute;HealthTimeoutSeconds=$HealthTimeoutSeconds;CommandTimeoutSeconds=$CommandTimeoutSeconds;BuildTimeoutSeconds=$BuildTimeoutSeconds;PollIntervalSeconds=$PollIntervalSeconds}
$script:PretripAppDefaultRoot=[IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
. (Join-Path $PSScriptRoot 'start.ps1')
. (Join-Path $PSScriptRoot 'install_pretrip_v22_bundle.ps1')
. (Join-Path $PSScriptRoot 'pretrip_app_images.ps1')

function Get-PretripAppSpec {
    param([string]$ProjectRoot=$script:PretripAppDefaultRoot,
        [ValidateSet('app','test')][string]$Environment='app',
        [int]$HealthTimeoutSeconds=180,[int]$CommandTimeoutSeconds=30,
        [int]$BuildTimeoutSeconds=1800,[int]$PollIntervalSeconds=2)
    $root=[IO.Path]::GetFullPath($ProjectRoot).TrimEnd('\','/')
    $null=Assert-PackageLocalPath $root -Directory
    $project=if($Environment -eq 'test'){'nyc-taxi-pretrip-v22-test'}else{'nyc-taxi-pretrip-v22'}
    $port=if($Environment -eq 'test'){28522}else{18522}
    $canonical=$root.ToLowerInvariant()+'|nyc-taxi-pretrip-app-v1|'+$Environment
    $owner=Get-PackageBytesHash ([Text.Encoding]::UTF8.GetBytes($canonical))
    $file=Join-Path $root 'docker-compose.pretrip-app.yml'
    $envFile=Join-Path $root 'config/pretrip_app.env'
    foreach($path in @($file,$envFile)){$null=Assert-PackageLocalPath $path}
    [pscustomobject]@{
        Root=$root;Project=$project;Port=$port;Owner=$owner;Environment=$Environment
        RuntimeRoot=(Join-Path $root ('.sandbox/pretrip-app/'+$Environment))
        ModelRoot=(Join-Path $root (Get-PretripV22DistributionSpec).Runtime)
        ComposeArguments=@('compose','--project-directory',$root,'--file',$file,'--env-file',$envFile,'--project-name',$project)
        CommandTimeoutSeconds=$CommandTimeoutSeconds;BuildTimeoutSeconds=$BuildTimeoutSeconds
        HealthTimeoutSeconds=$HealthTimeoutSeconds;PollIntervalSeconds=$PollIntervalSeconds
    }
}

function Invoke-PretripAppDocker {
    param($Context,[string[]]$Arguments,[string]$FailureMessage='Standalone Docker operation failed',[int]$TimeoutSeconds=0,[AllowNull()][string]$InputText)
    if(-not $TimeoutSeconds){$TimeoutSeconds=$Context.CommandTimeoutSeconds}
    $result=Invoke-DockerCommand -Arguments $Arguments -WorkingDirectory $Context.Root -InputText $InputText -TimeoutSeconds $TimeoutSeconds
    if($result.ExitCode -ne 0){Stop-Startup ($FailureMessage+' (exit '+$result.ExitCode+'); raw output withheld.')}
    return [string]$result.StdOut
}

function Invoke-PretripAppCompose {
    param($Context,[string[]]$Arguments,[string]$FailureMessage='Standalone Compose operation failed')
    Invoke-PretripAppDocker -Context $Context -Arguments ($Context.ComposeArguments+$Arguments) -FailureMessage $FailureMessage
}

function Get-PretripAppContainer {
    param($Context,[string]$Id)
    $template='{"ID":{{json .Id}},"ImageID":{{json .Image}},"ImageReference":{{json .Config.Image}},"Status":{{json .State.Status}},"StartedAt":{{json .State.StartedAt}},"Health":{{if .State.Health}}{{json .State.Health.Status}}{{else}}null{{end}},"Project":{{json (index .Config.Labels "com.docker.compose.project")}},"Service":{{json (index .Config.Labels "com.docker.compose.service")}},"ConfigHash":{{json (index .Config.Labels "com.docker.compose.config-hash")}},"Owner":{{json (index .Config.Labels "com.nyc-taxi.pretrip-app.owner")}}}'
    $json=Invoke-PretripAppDocker $Context @('container','inspect','--format',$template,$Id) 'Cannot inspect standalone container ownership'
    ConvertFrom-StartupJson $json 'standalone container metadata'
}

function Get-PretripAppContainers {
    param($Context)
    $ids=Invoke-PretripAppDocker $Context @('container','ls','-a','-q','--filter',('label=com.docker.compose.project='+$Context.Project)) 'Cannot inspect standalone project ownership'
    $seen=@{}
    foreach($id in @($ids -split '\s+' | Where-Object {$_})){
        $item=Get-PretripAppContainer $Context $id
        if($item.Project -cne $Context.Project -or $item.Owner -cne $Context.Owner -or
            $item.Service -notin @('fare-inference','dashboard') -or $seen.ContainsKey($item.Service)){
            Stop-Startup 'Standalone project has foreign/duplicate containers or belongs to another checkout; no adoption or lifecycle action.'
        }
        $seen[$item.Service]=$item
    }
    return $seen
}

function Assert-PretripAppNetwork {
    param($Context)
    $name=$Context.Project+'_app'
    $ids=Invoke-PretripAppDocker $Context @('network','ls','-q','--filter',('name=^'+[regex]::Escape($name)+'$')) 'Cannot inspect standalone network'
    if(-not $ids.Trim()){return}
    if(@($ids -split '\s+' | Where-Object {$_}).Count -ne 1){Stop-Startup 'Standalone network name is ambiguous.'}
    $template='{"Name":{{json .Name}},"Project":{{json (index .Labels "com.docker.compose.project")}},"Owner":{{json (index .Labels "com.nyc-taxi.pretrip-app.owner")}}}'
    $json=Invoke-PretripAppDocker $Context @('network','inspect','--format',$template,$ids.Trim()) 'Cannot inspect standalone network labels'
    $network=ConvertFrom-StartupJson $json 'standalone network metadata'
    if($network.Name -cne $name -or $network.Project -cne $Context.Project -or $network.Owner -cne $Context.Owner){
        Stop-Startup 'Standalone network is foreign; no adoption or overwrite.'
    }
}

function Get-PretripAppHostListeners {
    param([int]$Port)
    # Works on Windows PS5.1 without external modules; inspect only the selected port.
    return @([Net.NetworkInformation.IPGlobalProperties]::GetIPGlobalProperties().GetActiveTcpListeners() | Where-Object {$_.Port -eq $Port})
}

function Assert-PretripAppPort {
    param($Context,$Containers)
    $text=Invoke-PretripAppDocker $Context @('container','ls','-q','--filter',('publish='+$Context.Port)) 'Cannot inspect standalone host-port ownership'
    $ids=@($text -split '\s+' | Where-Object {$_})
    $own=if($Containers.ContainsKey('dashboard')){$Containers.dashboard.ID}else{''}
    foreach($id in $ids){
        if(-not $own -or (-not $own.StartsWith($id) -and -not $id.StartsWith($own))){Stop-Startup 'Standalone dashboard port is published by a foreign container; select the isolated test environment or resolve separately.'}
    }
    if(@(Get-PretripAppHostListeners -Port $Context.Port).Count -gt 0 -and ($ids.Count -eq 0 -or -not $own)){
        Stop-Startup 'Standalone dashboard port is already listening outside the owned dashboard; no service changes.'
    }
}

function Get-PretripAppConfigHash {
    param($Context,[string]$Service)
    $line=(Invoke-PretripAppCompose $Context @('config','--hash',$Service) 'Cannot compute standalone configuration identity').Trim()
    $parts=@($line -split '\s+')
    if($parts.Count -eq 2 -and $parts[0] -ceq $Service -and $parts[1] -cmatch '^[0-9a-f]{64}$'){return $parts[1]}
    if($parts.Count -eq 1 -and $parts[0] -cmatch '^[0-9a-f]{64}$'){return $parts[0]}
    Stop-Startup 'Compose configuration hash is invalid; no lifecycle action.'
}

function Assert-PretripAppContainerIdentity {
    param($Context,$Container,[string]$ImageID,[string]$ConfigHash)
    if($Container.Project -cne $Context.Project -or $Container.Owner -cne $Context.Owner -or
        $Container.ImageID -cne $ImageID -or $Container.ConfigHash -cne $ConfigHash){
        Stop-Startup ('Standalone '+$Container.Service+' image/config/ownership drift; update it separately after review. No automatic recreate.')
    }
}

function Wait-PretripAppHealth {
    param($Context,[string]$Service,[string]$Id)
    $deadline=(Get-StartupTime).AddSeconds($Context.HealthTimeoutSeconds)
    while($true){
        $item=Get-PretripAppContainer $Context $Id
        if($item.Project -cne $Context.Project -or $item.Owner -cne $Context.Owner -or $item.Service -cne $Service){Stop-Startup 'Standalone ownership changed while waiting for health.'}
        if($item.Status -ne 'running'){Stop-Startup ($Service+' is '+$item.Status+'; no retry/restart attempted.')}
        if($item.Health -eq 'healthy'){return $item}
        if($item.Health -ne 'starting'){Stop-Startup ($Service+' has no valid healthy/starting healthcheck; inspect separately.')}
        if((Get-StartupTime) -ge $deadline){Stop-Startup ('Timed out waiting for standalone '+$Service+' readiness.')}
        Pause-StartupPolling -Seconds $Context.PollIntervalSeconds
    }
}

function Start-PretripAppService {
    param($Context,[string]$Service,[string]$ImageID,[string]$ConfigHash)
    $containers=Get-PretripAppContainers $Context
    Assert-PretripAppNetwork $Context
    Assert-PretripAppPort $Context $containers
    $item=$containers[$Service]
    if($null -ne $item){
        Assert-PretripAppContainerIdentity $Context $item $ImageID $ConfigHash
        if($item.Status -eq 'exited'){
            $null=Invoke-PretripAppDocker $Context @('start',$item.ID) ('Cannot start existing standalone '+$Service)
        } elseif($item.Status -ne 'running') {Stop-Startup ('Standalone '+$Service+' is '+$item.Status+'; no automatic reconciliation.')}
    } else {
        $null=Invoke-PretripAppCompose $Context @('up','-d','--no-deps','--no-recreate','--no-build','--pull','never',$Service) ('Cannot create missing standalone '+$Service)
        $containers=Get-PretripAppContainers $Context
        $item=$containers[$Service]
        if($null -eq $item){Stop-Startup ('Standalone '+$Service+' was not created.')}
        Assert-PretripAppContainerIdentity $Context $item $ImageID $ConfigHash
    }
    Wait-PretripAppHealth $Context $Service $item.ID
}

function Test-PretripAppReadiness {
    param($Context,$Container,$Artifacts)
    # Reuse the full frozen model/calendar/TZif/dictionary guard and bounded probe.
    $contextForProbe=[pscustomobject]@{Root=$Context.Root;Project=$Context.Project;CommandTimeoutSeconds=$Context.CommandTimeoutSeconds;ModelVersion='v2.2';InferenceArtifacts=$Artifacts}
    Test-InferenceReadiness -Context $contextForProbe -Container $Container
}

function Invoke-PretripAppSetup {
    [CmdletBinding()]
    param([string]$ProjectRoot=$script:PretripAppDefaultRoot,[ValidateSet('app','test')][string]$Environment='app',
        [switch]$Execute,[int]$HealthTimeoutSeconds=180,[int]$CommandTimeoutSeconds=30,
        [int]$BuildTimeoutSeconds=1800,[int]$PollIntervalSeconds=2)
    $keys=@('PRETRIP_APP_PROJECT','PRETRIP_APP_PORT','PRETRIP_APP_OWNER','PRETRIP_APP_MODEL_ROOT','PRETRIP_APP_INFERENCE_IMAGE','PRETRIP_APP_DASHBOARD_IMAGE')
    $saved=@{};foreach($key in $keys){$saved[$key]=[Environment]::GetEnvironmentVariable($key,'Process')}
    $stage='preflight';$completed=New-Object 'Collections.Generic.List[string]'
    try {
        $context=Get-PretripAppSpec -ProjectRoot $ProjectRoot -Environment $Environment -HealthTimeoutSeconds $HealthTimeoutSeconds -CommandTimeoutSeconds $CommandTimeoutSeconds -BuildTimeoutSeconds $BuildTimeoutSeconds -PollIntervalSeconds $PollIntervalSeconds
        $script:DockerCliPath=Resolve-DockerCli
        if(-not $script:DockerCliPath){Stop-Startup 'Docker CLI missing. Install/open Docker Desktop, wait for its engine, then rerun; no host Python/Java is required.'}
        $null=Invoke-PretripAppDocker $context @('info','--format','{{.ServerVersion}}') 'Docker daemon unavailable. Open Docker Desktop and wait for the engine'
        $null=Invoke-PretripAppDocker $context @('compose','version','--short') 'Docker Compose unavailable'
        $images=Get-PretripAppImagePlan -ProjectRoot $context.Root
        $vars=@{PRETRIP_APP_PROJECT=$context.Project;PRETRIP_APP_PORT=[string]$context.Port;PRETRIP_APP_OWNER=$context.Owner;PRETRIP_APP_MODEL_ROOT=$context.ModelRoot;PRETRIP_APP_INFERENCE_IMAGE=$images.InferenceImage;PRETRIP_APP_DASHBOARD_IMAGE=$images.DashboardImage}
        foreach($key in $keys){[Environment]::SetEnvironmentVariable($key,[string]$vars[$key],'Process')}
        $null=Invoke-PretripAppCompose $context @('config','--quiet') 'Standalone Compose validation failed'
        $containers=Get-PretripAppContainers $context
        Assert-PretripAppNetwork $context
        Assert-PretripAppPort $context $containers
        $hashes=@{};foreach($service in @('fare-inference','dashboard')){
            $hashes[$service]=Get-PretripAppConfigHash $context $service
            if($containers.ContainsKey($service)){
                $expectedTag=if($service -eq 'fare-inference'){$images.InferenceImage}else{$images.DashboardImage}
                if($containers[$service].ConfigHash -cne $hashes[$service] -or $containers[$service].ImageReference -cne $expectedTag){
                    Stop-Startup ('Standalone '+$service+' configuration/image tag drift; review a separate update, no recreate or build attempted.')
                }
            }
        }
        $completed.Add('preflight')
        $stage='bundle'
        $bundle=Install-PretripV22Distribution -ProjectRoot $context.Root -Execute:$Execute
        if(-not $bundle.Verified){Stop-Startup 'Repository model bundle did not verify; no images or services attempted.'}
        $completed.Add('bundle')
        $stage='images'
        $images=Assert-PretripAppImages -Context $context -Execute:$Execute
        $completed.Add('images')
        if(-not $Execute){
            return [pscustomobject]@{Status='PLAN';Writes=$false;Environment=$Environment;Project=$context.Project;Port=$context.Port;Stages=@('preflight','bundle','images','fare-inference','full-readiness','dashboard');ImagePlan=[pscustomobject]@{SourceSHA256=$images.SourceSHA256;DependencySHA256=$images.DependencySHA256;Images=@($images.Images | Select-Object Role,Tag,Status,ImageID);ColdBuild=$images.ColdBuild};ModelSHA256=$bundle.ModelSHA256;NetworkEnabled=($env:FARE_ADDRESS_ALLOW_NETWORK -eq '1');Note='No writes/build/pull/container lifecycle. Cold image build/download sizes and duration unmeasured. Only pretrip; no analytics database/bootstrap.'}
        }
        # Image helper returns exact verified image IDs for both services.
        $stage='container-drift'
        foreach($service in @('fare-inference','dashboard')){
            if($containers.ContainsKey($service)){
                $imageId=if($service -eq 'fare-inference'){$images.InferenceImageID}else{$images.DashboardImageID}
                Assert-PretripAppContainerIdentity $context $containers[$service] $imageId $hashes[$service]
            }
        }
        $artifacts=Assert-PretripV22InstalledRoot -ProjectRoot $context.Root
        $stage='fare-inference'
        $inference=Start-PretripAppService $context 'fare-inference' $images.InferenceImageID $hashes['fare-inference']
        $completed.Add('fare-inference')
        $stage='full-readiness'
        $null=Test-PretripAppReadiness $context $inference $artifacts
        $completed.Add('full-readiness')
        $stage='dashboard'
        $dashboard=Start-PretripAppService $context 'dashboard' $images.DashboardImageID $hashes.dashboard
        $completed.Add('dashboard')
        [pscustomobject]@{Status='READY';Project=$context.Project;URL=('http://127.0.0.1:'+$context.Port);ModelVersion='v2.2';ModelSHA256=$bundle.ModelSHA256;Completed=@($completed.ToArray());InferenceID=$inference.ID;DashboardID=$dashboard.ID;Note='Pretrip only; no historical data/bootstrap/training or production lifecycle.'}
    } catch {
        $safe=$_.Exception.Data['StartupSafeMessage']
        if(-not $safe){$safe=$_.Exception.Data['PackageSafeMessage']}
        if(-not $safe){$safe='Standalone setup failed; raw details withheld. Existing resources retained; rerun after resolving this stage.'}
        $failure=New-Object Exception(('Stage '+$stage+' failed: '+$safe))
        $failure.Data['PretripAppSafeMessage']=$failure.Message
        $failure.Data['CompletedStages']=@($completed.ToArray())
        throw $failure
    } finally {
        foreach($key in $keys){[Environment]::SetEnvironmentVariable($key,$saved[$key],'Process')}
    }
}

if($MyInvocation.InvocationName -ne '.'){
    try {
        $result=Invoke-PretripAppSetup -ProjectRoot $script:PretripAppDefaultRoot @script:PretripAppCliOptions
        $result | ConvertTo-Json -Depth 8
        if(-not $script:PretripAppCliOptions.Execute){Write-Host 'PLAN only. Pass -Execute explicitly to install bundle, build missing images and start the isolated pretrip application.'}
        exit 0
    } catch {
        $safe=$_.Exception.Data['PretripAppSafeMessage'];if(-not $safe){$safe='Standalone pretrip setup failed; raw details withheld.'}
        Write-Host ('ERROR: '+$safe)
        exit 1
    }
}