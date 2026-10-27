#requires -Version 5.1
<#
Start services without reconciling/recreating existing containers. Data bootstrap
and explicit image/config updates are separate operations. No host Python needed.
#>
[CmdletBinding()]
param(
    [switch]$Build,
    [switch]$WithInference,
    [ValidateSet('v1', 'v2.2')][string]$ModelVersion = 'v1',
    [ValidateSet('current', 'bootstrap-test', 'package-test')][string]$Environment = 'current',
    [ValidateRange(1, 1800)][int]$HealthTimeoutSeconds = 120,
    [ValidateRange(1, 300)][int]$CommandTimeoutSeconds = 30,
    [ValidateRange(60, 7200)][int]$BuildTimeoutSeconds = 1800,
    [ValidateRange(1, 30)][int]$PollIntervalSeconds = 2
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$script:StartupProjectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))

function Stop-Startup {
    param([string]$Message)
    $failure = New-Object System.Exception($Message)
    $failure.Data['StartupSafeMessage'] = $Message
    throw $failure
}

function Resolve-DockerCli {
    # Docker Desktop can expose both docker.exe and a Unix 'docker' shim.
    $command = Get-Command docker.exe -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($null -eq $command) { return $null }
    return $command.Source
}

function ConvertTo-NativeArgument {
    param([AllowEmptyString()][string]$Value)
    # Windows CommandLineToArgvW/MSVC quoting; no shell interpolation.
    $escaped = [regex]::Replace($Value, '(\\*)"', '$1$1\"')
    $escaped = [regex]::Replace($escaped, '(\\+)$', '$1$1')
    return '"' + $escaped + '"'
}

function Invoke-DockerCommand {
    param(
        [string[]]$Arguments,
        [string]$WorkingDirectory,
        [AllowNull()][string]$InputText,
        [int]$TimeoutSeconds = 30
    )
    # Capture both streams: config/build/daemon errors must not expose secrets.
    $process = New-Object System.Diagnostics.Process
    try {
        $process.StartInfo.FileName = $script:DockerCliPath
        $process.StartInfo.WorkingDirectory = $WorkingDirectory
        $process.StartInfo.Arguments = ($Arguments | ForEach-Object { ConvertTo-NativeArgument $_ }) -join ' '
        $process.StartInfo.UseShellExecute = $false
        $process.StartInfo.CreateNoWindow = $true
        $process.StartInfo.RedirectStandardOutput = $true
        $process.StartInfo.RedirectStandardError = $true
        $process.StartInfo.RedirectStandardInput = $true
        $process.StartInfo.StandardOutputEncoding = [Text.Encoding]::UTF8
        $process.StartInfo.StandardErrorEncoding = [Text.Encoding]::UTF8
        $null = $process.Start()
        $stdout = $process.StandardOutput.ReadToEndAsync()
        $stderr = $process.StandardError.ReadToEndAsync()
        if ($InputText) { $process.StandardInput.Write($InputText) }
        $process.StandardInput.Close()
        if (-not $process.WaitForExit($TimeoutSeconds * 1000)) {
            # Kill only this CLI client, never a container or service.
            $process.Kill()
            $null = $process.WaitForExit(1000)
            return [pscustomobject]@{ ExitCode = 124; StdOut = ''; StdErr = '' }
        }
        return [pscustomobject]@{
            ExitCode = $process.ExitCode
            StdOut = $stdout.GetAwaiter().GetResult()
            StdErr = $stderr.GetAwaiter().GetResult()
        }
    } catch {
        return [pscustomobject]@{ ExitCode = 127; StdOut = ''; StdErr = '' }
    } finally {
        $process.Dispose()
    }
}

function Invoke-StartupDocker {
    param($Context, [string[]]$Arguments, [string]$FailureMessage, [string]$InputText, [int]$TimeoutSeconds = 0)
    if ($TimeoutSeconds -eq 0) { $TimeoutSeconds = $Context.CommandTimeoutSeconds }
    $result = Invoke-DockerCommand -Arguments $Arguments -WorkingDirectory $Context.Root -InputText $InputText -TimeoutSeconds $TimeoutSeconds
    if ($result.ExitCode -ne 0) { Stop-Startup ($FailureMessage + ' (exit ' + $result.ExitCode + ').') }
    return [string]$result.StdOut
}

function Invoke-StartupCompose {
    param($Context, [string[]]$Arguments, [string]$FailureMessage, [int]$TimeoutSeconds = 0)
    return Invoke-StartupDocker -Context $Context -Arguments ($Context.ComposeArguments + $Arguments) -FailureMessage $FailureMessage -TimeoutSeconds $TimeoutSeconds
}

function Get-OptionalProperty {
    param($Object, [string]$Name, $Default = $null)
    if ($null -eq $Object) { return $Default }
    $property = $Object.PSObject.Properties[$Name]
    if ($null -eq $property) { return $Default }
    return $property.Value
}

function ConvertFrom-StartupJson {
    param([string]$Json, [string]$Description)
    try { return ConvertFrom-Json -InputObject $Json }
    catch { Stop-Startup ('Cannot read ' + $Description + '; raw output withheld.') }
}

function Get-ContainerSnapshot {
    param($Context, [string]$Id)
    # Inspect emits ONLY this whitelist; never fetch Config.Env or health logs.
    $template = '{"ID":{{json .Id}},"ImageID":{{json .Image}},"ImageReference":{{json .Config.Image}},"Status":{{json .State.Status}},"StartedAt":{{json .State.StartedAt}},"Health":{{if .State.Health}}{{json .State.Health.Status}}{{else}}null{{end}},"Project":{{json (index .Config.Labels "com.docker.compose.project")}},"Service":{{json (index .Config.Labels "com.docker.compose.service")}},"ConfigHash":{{json (index .Config.Labels "com.docker.compose.config-hash")}}}'
    $json = Invoke-StartupDocker $Context @('container', 'inspect', '--format', $template, $Id) 'Cannot inspect service container'
    return ConvertFrom-StartupJson $json 'container metadata'
}

function Get-ServiceContainer {
    param($Context, [string]$Service, [string]$ContainerName)
    $idsText = Invoke-StartupDocker $Context @(
        'container', 'ls', '-a', '-q',
        '--filter', ('label=com.docker.compose.project=' + $Context.Project),
        '--filter', ('label=com.docker.compose.service=' + $Service),
        '--filter', 'label=com.docker.compose.oneoff=False'
    ) ('Cannot find ' + $Service + ' container')
    $ids = @($idsText -split '\s+' | Where-Object { $_ })
    if ($ids.Count -gt 1) { Stop-Startup ('Multiple containers found for ' + $Service + '; review separately.') }
    if ($ids.Count -eq 0) {
        if ($ContainerName) {
            $collision = Invoke-StartupDocker $Context @('container', 'ls', '-a', '-q', '--filter', ('name=^/' + [regex]::Escape($ContainerName) + '$')) 'Cannot check container name ownership'
            if ($collision.Trim()) { Stop-Startup ('Existing named container for ' + $Service + ' belongs to another project/service or has unexpected labels; no creation attempted.') }
        }
        return $null
    }
    $container = Get-ContainerSnapshot $Context $ids[0]
    if ($container.Project -ne $Context.Project -or $container.Service -ne $Service) {
        Stop-Startup ('Container project/service mismatch for ' + $Service + '; no lifecycle action attempted.')
    }
    return $container
}

function Get-StartupTime { return [DateTime]::UtcNow }
function Pause-StartupPolling { param([int]$Seconds) Start-Sleep -Seconds $Seconds }

function Wait-ServiceHealth {
    param($Context, [string]$Service, [string]$Id)
    $deadline = (Get-StartupTime).AddSeconds($Context.HealthTimeoutSeconds)
    while ($true) {
        $container = Get-ContainerSnapshot $Context $Id
        if ($container.Project -ne $Context.Project -or $container.Service -ne $Service) {
            Stop-Startup ('Container ownership changed for ' + $Service + '.')
        }
        if ($container.Status -ne 'running') { Stop-Startup ($Service + ' is ' + $container.Status + '; inspect it separately.') }
        if ($container.Health -eq 'healthy') { return $container }
        if ($container.Health -eq 'unhealthy') { Stop-Startup ($Service + ' healthcheck is unhealthy; no restart/recreate attempted.') }
        if (-not $container.Health) { Stop-Startup ($Service + ' has no healthcheck; update its image/config separately.') }
        if ((Get-StartupTime) -ge $deadline) { Stop-Startup ('Timed out waiting for ' + $Service + ' health.') }
        Pause-StartupPolling -Seconds $Context.PollIntervalSeconds
    }
}

function Test-DatabasePresence {
    param($Context, [string]$PostgresId)
    # psql uses container-local credentials; no password/URI goes into arguments.
    # EXISTS reads at most one qualifying row; COUNT is only on the tiny load log.
    $sql = @'
\set ON_ERROR_STOP on
BEGIN READ ONLY;
SET LOCAL statement_timeout = '5s';
SET LOCAL lock_timeout = '2s';
SELECT bool_and(to_regclass('analytics.' || name) IS NOT NULL) AS all_tables_present
FROM (VALUES ('dim_location'), ('dim_calendar'), ('dim_weather'), ('fact_trip'), ('etl_load_log')) AS required(name) \gset
\if :all_tables_present
SELECT json_build_object(
    'schema_present', true, 'missing_tables', '[]'::json,
    'fact_has_rows', EXISTS (SELECT 1 FROM analytics.fact_trip),
    'dimensions_have_rows', EXISTS (SELECT 1 FROM analytics.dim_location)
        AND EXISTS (SELECT 1 FROM analytics.dim_calendar) AND EXISTS (SELECT 1 FROM analytics.dim_weather),
    'log_entries', COUNT(*),
    'loaded_partitions', COUNT(*) FILTER (WHERE status = 'loaded' AND completed_at IS NOT NULL
        AND loaded_row_count > 0 AND loaded_row_count = source_row_count),
    'unfinished_partitions', COUNT(*) FILTER (WHERE status IS DISTINCT FROM 'loaded'
        OR completed_at IS NULL OR loaded_row_count IS NULL OR loaded_row_count <= 0
        OR loaded_row_count <> source_row_count)
) FROM analytics.etl_load_log WHERE dataset = 'fact_trip';
\else
SELECT json_build_object(
    'schema_present', EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'analytics'),
    'missing_tables', COALESCE(json_agg(name), '[]'::json),
    'fact_has_rows', false, 'dimensions_have_rows', false,
    'log_entries', 0, 'loaded_partitions', 0, 'unfinished_partitions', 0
) FROM (VALUES ('dim_location'), ('dim_calendar'), ('dim_weather'), ('fact_trip'), ('etl_load_log')) AS required(name)
WHERE to_regclass('analytics.' || name) IS NULL;
\endif
ROLLBACK;
'@
    $shell = 'exec psql -X -qAt -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
    $json = Invoke-StartupDocker -Context $Context -Arguments @('exec', '-i', $PostgresId, 'sh', '-c', $shell) -InputText $sql -FailureMessage 'Read-only database presence check failed; data status is unverified. Review credentials/schema and perform any bootstrap separately'
    $presence = ConvertFrom-StartupJson $json 'database presence result'
    if (-not $presence.schema_present -or @($presence.missing_tables).Count -gt 0 -or
        -not $presence.fact_has_rows -or -not $presence.dimensions_have_rows -or
        $presence.loaded_partitions -eq 0 -or $presence.unfinished_partitions -gt 0) {
        Write-Warning 'Data is not initialized or load metadata is incomplete. A separate reviewed data bootstrap/load is required; this script never loads data.'
    }
    Write-Host ('Data presence: schema=' + $presence.schema_present + '; fact has rows=' + $presence.fact_has_rows + '; dimensions have rows=' + $presence.dimensions_have_rows + '.')
    Write-Host ('Load metadata: ' + $presence.loaded_partitions + ' loaded / ' + $presence.log_entries + ' entries; ' + $presence.unfinished_partitions + ' unfinished. No full fact count or data validation was performed.')
    if (@($presence.missing_tables).Count -gt 0) { Write-Host ('Missing analytics tables: ' + ($presence.missing_tables -join ', ') + '.') }
}

function Invoke-ProjectStart {
    param(
        [string]$ProjectRoot = $script:StartupProjectRoot,
        [switch]$Build,
        [switch]$WithInference,
        [ValidateSet('v1', 'v2.2')][string]$ModelVersion = 'v1',
        [ValidateSet('current', 'bootstrap-test', 'package-test')][string]$Environment = 'current',
        [int]$HealthTimeoutSeconds = 120,
        [int]$CommandTimeoutSeconds = 30,
        [int]$BuildTimeoutSeconds = 1800,
        [int]$PollIntervalSeconds = 2,
        $Context = $null
    )
    try {
        $package = $null -ne $Context -and (Get-OptionalProperty $Context 'IsPackage' $false)
        if ($ModelVersion -ne 'v1' -and (-not $WithInference -or $package)) { Stop-Startup 'V2.2 requires explicit -WithInference in the current environment. Package bootstrap stays v1; prepare V2.2 separately.' }
        if ($WithInference -and $Environment -ne 'current' -and -not $package) { Stop-Startup '-WithInference cannot be combined with bootstrap-test; use the current frozen-model environment explicitly.' }
        $script:DockerCliPath = Resolve-DockerCli
        if (-not $script:DockerCliPath) { Stop-Startup 'Docker CLI is missing. Install Docker Desktop, open it, wait for the engine, then run this script again.' }
        $composeFile = Join-Path $ProjectRoot 'docker-compose.yml'
        if (-not (Test-Path -LiteralPath $composeFile -PathType Leaf)) { Stop-Startup 'docker-compose.yml is missing beside the scripts directory.' }
        if ($null -eq $Context) {
            $context = New-ProjectEnvironmentContext -ProjectRoot $ProjectRoot -Environment $Environment -CommandTimeoutSeconds $CommandTimeoutSeconds -HealthTimeoutSeconds $HealthTimeoutSeconds -PollIntervalSeconds $PollIntervalSeconds
        } elseif ((Get-NormalizedEnvironmentPath $Context.Root) -ne (Get-NormalizedEnvironmentPath $ProjectRoot) -or $Context.Environment -ne $Environment) {
            Stop-Startup 'Supplied startup context differs from the requested root/environment.'
        } else { $context = $Context }
        if ($WithInference) {
            if ($package) {
                $context | Add-Member WithInference $true -Force
            } else { Enable-InferenceStartupContext -Context $context -ModelVersion $ModelVersion }
            # Host-only hashes; fail before builds, container lifecycle work, or
            # even creating a missing .env. Never train/bootstrap/fetch a model.
            try {
                if ($ModelVersion -eq 'v2.2') { $artifacts = Assert-PretripV22Artifacts -ProjectRoot $context.Root }
                elseif ($package) { $artifacts = Assert-FareArtifacts -ProjectRoot $context.Root -RuntimeRoot $context.ModelRuntimeRoot }
                else { $artifacts = Assert-FareArtifacts -ProjectRoot $context.Root }
            }
            catch {
                $safeArtifactMessage = $_.Exception.Data['StartupSafeMessage']
                if ($safeArtifactMessage) { throw }
                Stop-Startup 'Frozen inference artifacts are missing or mismatched. Provision the reviewed P2.1/P3 artifacts separately; no build, service lifecycle, or automatic training was attempted.'
            }
            if ((Get-OptionalProperty $artifacts 'Verified' $false) -ne $true) { Stop-Startup 'Frozen inference artifacts are not verified; no service lifecycle or automatic training was attempted.' }
            $context.InferenceArtifacts = $artifacts
        }
        $null = Invoke-StartupDocker $context @('info', '--format', '{{.ServerVersion}}') 'Docker daemon is unavailable. Open Docker Desktop, wait until the engine is running, then retry'
        $null = Invoke-StartupDocker $context @('compose', 'version', '--short') 'Docker Compose plugin is unavailable. Enable/install it through Docker Desktop'

        $envPath = $context.EnvPath
        if (-not (Test-Path -LiteralPath $envPath -PathType Leaf)) {
            if ($Environment -ne 'current' -or $package) { Stop-Startup 'Selected isolated/package configuration is missing. Prepare it through its explicit setup script; existing .env is never overwritten.' }
            if (Test-Path -LiteralPath $envPath) { Stop-Startup '.env exists but is not a regular file.' }
            $example = Join-Path $ProjectRoot '.env.example'
            if (-not (Test-Path -LiteralPath $example -PathType Leaf)) { Stop-Startup '.env and .env.example are missing. Create local configuration separately.' }
            try { [IO.File]::Copy($example, $envPath, $false) }
            catch { if (-not (Test-Path -LiteralPath $envPath -PathType Leaf)) { Stop-Startup 'Could not create .env from .env.example.' } }
            Write-Host 'Created .env from .env.example without overwriting an existing file. Review local settings; values are not printed.'
        } elseif ($Environment -eq 'current') { Write-Host 'Reusing existing .env; no values printed or overwritten.' }
        else { Write-Host 'Reusing isolated bootstrap-test configuration; no values printed or overwritten.' }

        $null = Invoke-StartupCompose $context @('config', '--quiet') 'Compose validation failed; check local .env and docker-compose.yml (resolved values withheld)'
        $modelArguments = @('config', '--no-interpolate', '--format', 'json')
        if ($Environment -ne 'current' -or $package) { $modelArguments = @('--profile', '*', 'config', '--format', 'json') }
        $modelJson = Invoke-StartupCompose $context $modelArguments 'Cannot read Compose metadata; resolved values withheld'
        $model = ConvertFrom-StartupJson $modelJson 'public Compose metadata'
        $modelProject = Get-OptionalProperty $model 'name'
        if ($Environment -eq 'bootstrap-test' -and $modelProject -ne $context.ExpectedProject) { Stop-Startup 'Resolved bootstrap-test Compose project differs from the required isolated project.' }
        if ($WithInference -and $modelProject -ne $context.ExpectedProject) { Stop-Startup 'Inference Compose project differs from the required existing project.' }
        $context.Project = $modelProject
        if (-not $context.Project -or $context.Project -notmatch '^[a-z0-9][a-z0-9_-]*$') { Stop-Startup 'Cannot determine the Compose project name safely.' }
        # Isolation must pass before either image builds or service lifecycle work.
        $null = Assert-ProjectEnvironmentIsolation -Context $context -Model $model
        if ($Build) {
            Write-Host ('Building only ' + ($context.BuildServices -join '/') + '; existing containers will not be recreated (bounded build wait).')
            $null = Invoke-StartupCompose $context (@('--profile', 'jobs', 'build') + @($context.BuildServices)) 'Explicit application image build failed; output withheld' -TimeoutSeconds $BuildTimeoutSeconds
        }

        # Complete all ownership/image preflight checks before lifecycle actions.
        $plans = @()
        $updateNeeded = $false
        foreach ($service in $context.Services) {
            $serviceModel = Get-OptionalProperty (Get-OptionalProperty $model 'services') $service
            if ($null -eq $serviceModel) { Stop-Startup ('Compose service is missing: ' + $service + '.') }
            $containerName = Get-OptionalProperty $serviceModel 'container_name' ''
            $container = Get-ServiceContainer $context $service $containerName
            # --images can also list dependencies. Never mistake postgres's image
            # for dashboard's; prefer the exact public service image reference.
            $imageText = Invoke-StartupCompose $context @('config', '--images', $service) ('Cannot resolve image for ' + $service)
            $imageNames = @($imageText -split '\s+' | Where-Object { $_ } | Select-Object -Unique)
            $publicImage = Get-OptionalProperty $serviceModel 'image' ''
            if ($publicImage -and $imageNames -contains $publicImage) { $imageName = $publicImage }
            elseif ($imageNames.Count -eq 1) { $imageName = $imageNames[0] }
            else { Stop-Startup ('Cannot uniquely resolve the image reference for ' + $service + '; review its Compose image configuration separately.') }
            $hashText = Invoke-StartupCompose $context @('config', '--hash', $service) ('Cannot resolve config hash for ' + $service)
            $hashLines = @($hashText -split '\r?\n' | Where-Object { $_ -match ('^' + [regex]::Escape($service) + '\s+') })
            if ($hashLines.Count -ne 1) { Stop-Startup ('Unexpected config hash result for ' + $service + '.') }
            $hashParts = @($hashLines[0].Trim() -split '\s+')
            if ($hashParts.Count -ne 2) { Stop-Startup ('Unexpected config hash result for ' + $service + '.') }
            $image = Invoke-DockerCommand -Arguments @('image', 'inspect', '--format', '{{.Id}}', $imageName) -WorkingDirectory $context.Root -TimeoutSeconds $CommandTimeoutSeconds
            $imageId = ''
            if ($image.ExitCode -eq 0) { $imageId = $image.StdOut.Trim() }
            if ($null -eq $container -and -not $imageId) {
                if ($WithInference -and $service -in @('fare-inference', 'dashboard')) {
                    $selectionHint = ''
                    if ($ModelVersion -eq 'v2.2') { $selectionHint = ' -ModelVersion v2.2' }
                    Stop-Startup ($service + ' image is missing. Run scripts/start.ps1 -WithInference' + $selectionHint + ' -Build explicitly; no automatic build, pull, or model training was attempted.')
                }
                if ($service -eq 'dashboard') { Stop-Startup 'Dashboard image is missing. Run scripts/start.ps1 -Build first; no automatic build or pull was attempted.' }
                Stop-Startup ($service + ' image is missing. Obtain the exact configured image explicitly, then retry. -Build never builds/pulls postgres or mlflow.')
            }
            $drift = @()
            if ($null -ne $container) {
                if ($container.ConfigHash -ne $hashParts[1]) { $drift += 'configuration hash' }
                if (-not $imageId -or $container.ImageID -ne $imageId -or $container.ImageReference -ne $imageName) { $drift += 'local image/reference' }
                if ($drift.Count -gt 0) {
                    $updateNeeded = $true
                    Write-Warning ($service + ' differs in ' + ($drift -join ', ') + '. Existing container is preserved; a separate reviewed update is required. This script does not recreate it.')
                    if ($WithInference -and $service -in @('fare-inference', 'dashboard')) {
                        Write-Host ('Separate reviewed update command (not executed): ' + (Get-InferenceUpdateCommand -Context $context -Service $service))
                    }
                }
            }
            $plans += [pscustomobject]@{ Service = $service; Container = $container; ContainerName = $containerName; Drift = ($drift.Count -gt 0) }
        }
        foreach ($plan in $plans) {
            if ($null -ne $plan.Container -and $plan.Container.Status -in @('created', 'exited') -and $plan.Drift) {
                Write-Warning ($plan.Service + ' is stopped with config/image drift. Update separately before starting; no lifecycle action was attempted.')
                return 2
            }
        }

        $final = @()
        foreach ($plan in $plans) {
            $service = $plan.Service
            $container = $plan.Container
            if ($null -eq $container) {
                Write-Host ('Creating missing service only: ' + $service + ' (no deps/recreate/build/pull).')
                $null = Invoke-StartupCompose $context @('up', '-d', '--no-deps', '--no-recreate', '--no-build', '--pull', 'never', $service) ('Cannot create missing service ' + $service)
                $container = Get-ServiceContainer $context $service $plan.ContainerName
                if ($null -eq $container) { Stop-Startup ('No container found after creating ' + $service + '.') }
            } elseif ($container.Status -in @('created', 'exited')) {
                Write-Host ('Starting existing stopped container: ' + $service + '.')
                $null = Invoke-StartupDocker $context @('container', 'start', $container.ID) ('Cannot start ' + $service)
            } elseif ($container.Status -eq 'running') {
                Write-Host ('Reusing running container: ' + $service + '.')
            } else { Stop-Startup ($service + ' is ' + $container.Status + '; review separately. No restart/recreate attempted.') }
            $healthy = Wait-ServiceHealth $context $service $container.ID
            if ($WithInference -and $service -eq 'fare-inference') { $null = Test-InferenceReadiness -Context $context -Container $healthy }
            $final += $healthy
        }
        Write-Host ('Compose project: ' + $context.Project)
        foreach ($container in $final) {
            Write-Host ($container.Service + ': healthy; ID=' + $container.ID.Substring(0, [Math]::Min(12, $container.ID.Length)) + '; StartedAt=' + $container.StartedAt)
        }
        Test-DatabasePresence $context ($final | Where-Object Service -eq 'postgres').ID
        if ($Environment -eq 'current') {
            Write-Host 'Dashboard: http://127.0.0.1:8501'
            Write-Host 'MLflow: http://localhost:5000'
            if ($WithInference) { Write-Host 'Fare inference: internal-only, verified frozen model; open the dashboard fare-estimate page.' }
        } else {
            Write-Host 'Bootstrap-test PostgreSQL/MLflow are internal-only; no host ports or dashboard are started.'
        }
        Write-Host 'Service startup only. Fresh-clone data bootstrap is NOT completed by this script. Worker/jobs were not started.'
        if ($updateNeeded) { Write-Warning 'Services are healthy, but config/image updates remain pending (exit 2).'; return 2 }
        return 0
    } catch {
        $safeMessage = $_.Exception.Data['StartupSafeMessage']
        if (-not $safeMessage) { $safeMessage = 'Unexpected startup check failure; raw details withheld to protect local configuration. No automatic recreate/restart or data loading was attempted.' }
        Write-Host ('ERROR: ' + $safeMessage) -ForegroundColor Red
        return 1
    }
}

. (Join-Path $PSScriptRoot 'docker_environment.ps1')
. (Join-Path $PSScriptRoot 'start_inference.ps1')
. (Join-Path $PSScriptRoot 'model_artifacts.ps1')
. (Join-Path $PSScriptRoot 'pretrip_model_selection.ps1')
. (Join-Path $PSScriptRoot 'package_environment.ps1')

if ($MyInvocation.InvocationName -ne '.') {
    exit (Invoke-ProjectStart -Build:$Build -WithInference:$WithInference -ModelVersion $ModelVersion -Environment $Environment -HealthTimeoutSeconds $HealthTimeoutSeconds -CommandTimeoutSeconds $CommandTimeoutSeconds -BuildTimeoutSeconds $BuildTimeoutSeconds -PollIntervalSeconds $PollIntervalSeconds)
}
