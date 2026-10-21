# Shared selection and fail-closed isolation checks; no actions on dot-source.
# Native Docker/JSON helpers are provided by start.ps1.

function New-ProjectEnvironmentContext {
    param(
        [string]$ProjectRoot,
        [ValidateSet('current', 'bootstrap-test')][string]$Environment = 'current',
        [ValidateSet('refresh', 'clean')][string]$WorkerVariant = 'refresh',
        [int]$CommandTimeoutSeconds = 30,
        [int]$HealthTimeoutSeconds = 120,
        [int]$PollIntervalSeconds = 2
    )
    if ($WorkerVariant -eq 'clean' -and $Environment -ne 'bootstrap-test') {
        Stop-Startup 'The clean worker variant is available only in bootstrap-test; the current environment is never overridden.'
    }
    $root = [IO.Path]::GetFullPath($ProjectRoot)
    $arguments = @('compose', '--project-directory', $root, '--file', (Join-Path $root 'docker-compose.yml'))
    $runtime = $root
    $envPath = Join-Path $root '.env'
    $services = @('postgres', 'mlflow', 'dashboard')
    $buildServices = @('worker', 'dashboard')
    $expectedProject = ''
    $expectedImage = ''
    if ($Environment -eq 'bootstrap-test') {
        $runtime = Join-Path $root '.sandbox/bootstrap-test'
        $envPath = Join-Path $runtime 'compose.env'
        $expectedProject = 'nyc-taxi-bootstrap-test'
        $expectedImage = 'nyc-taxi-worker:p6-test'
        $arguments += @('--file', (Join-Path $root 'docker-compose.bootstrap-test.yml'), '--env-file', $envPath, '--project-name', $expectedProject)
        if ($WorkerVariant -eq 'clean') {
            $expectedImage = 'nyc-taxi-worker:p6-clean'
            $arguments += @('--file', (Join-Path $root 'docker-compose.bootstrap-clean.yml'))
        }
        $services = @('postgres', 'mlflow')
        $buildServices = @('worker')
    }
    return [pscustomobject]@{
        Root = $root; RuntimeRoot = [IO.Path]::GetFullPath($runtime); EnvPath = $envPath
        Environment = $Environment; WorkerVariant = $WorkerVariant; ComposeArguments = $arguments; Project = ''
        ExpectedProject = $expectedProject; ExpectedWorkerImage = $expectedImage
        Services = $services; BuildServices = $buildServices
        CommandTimeoutSeconds = $CommandTimeoutSeconds; HealthTimeoutSeconds = $HealthTimeoutSeconds
        PollIntervalSeconds = $PollIntervalSeconds
    }
}

function Get-NormalizedEnvironmentPath {
    param([string]$Path)
    # Docker Desktop sometimes reports its Linux-side spelling of a Windows bind.
    $value = [regex]::Replace($Path, '^/(?:run/desktop/mnt/host|host_mnt)/([a-zA-Z])/', '${1}:/')
    if (-not $value -or -not [IO.Path]::IsPathRooted($value)) { Stop-Startup 'A resolved bind source must be an absolute path.' }
    return [IO.Path]::GetFullPath($value).TrimEnd('\', '/').ToLowerInvariant()
}

function Test-EnvironmentPathOverlap {
    param([string]$First, [string]$Second)
    $a = Get-NormalizedEnvironmentPath $First
    $b = Get-NormalizedEnvironmentPath $Second
    return $a -eq $b -or $a.StartsWith($b + '\') -or $b.StartsWith($a + '\')
}

function Assert-NoEnvironmentReparsePoint {
    param([string]$Path)
    $candidate = [IO.Path]::GetFullPath($Path)
    while ($candidate) {
        if (Test-Path -LiteralPath $candidate) {
            $item = Get-Item -LiteralPath $candidate -Force
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                Stop-Startup 'Sandbox runtime paths must not traverse a symlink/junction/reparse point.'
            }
        }
        $parent = [IO.Path]::GetDirectoryName($candidate.TrimEnd('\', '/'))
        if (-not $parent -or $parent -eq $candidate) { break }
        $candidate = $parent
    }
}

function Get-ProtectedEnvironmentModel {
    param($Context)
    $current = New-ProjectEnvironmentContext -ProjectRoot $Context.Root -Environment current -CommandTimeoutSeconds $Context.CommandTimeoutSeconds
    $json = Invoke-StartupCompose $current @('--profile', '*', 'config', '--format', 'json') 'Cannot resolve the protected current environment; isolation is unverified'
    return ConvertFrom-StartupJson $json 'protected Compose metadata'
}

function Get-EnvironmentDockerInventory {
    param($Context)
    $containers = @()
    $ids = Invoke-StartupDocker $Context @('container', 'ls', '-a', '-q') 'Cannot list containers for collision checks'
    # No Config.Env or health logs. Only resource ownership and attachment metadata.
    $template = '{"ID":{{json .Id}},"Name":{{json .Name}},"Project":{{json (index .Config.Labels "com.docker.compose.project")}},"Service":{{json (index .Config.Labels "com.docker.compose.service")}},"Mounts":{{json .Mounts}},"Networks":{{json .NetworkSettings.Networks}},"Ports":{{json .HostConfig.PortBindings}}}'
    foreach ($id in @($ids -split '\s+' | Where-Object { $_ })) {
        $json = Invoke-StartupDocker $Context @('container', 'inspect', '--format', $template, $id) 'Cannot inspect container collision metadata'
        $record = ConvertFrom-StartupJson $json 'container attachments'
        $record.Name = $record.Name.TrimStart('/')
        $containers += $record
    }
    $volumes = @()
    $names = Invoke-StartupDocker $Context @('volume', 'ls', '-q') 'Cannot list volume ownership'
    foreach ($name in @($names -split '\s+' | Where-Object { $_ })) {
        $json = Invoke-StartupDocker $Context @('volume', 'inspect', '--format', '{"Name":{{json .Name}},"Project":{{json (index .Labels "com.docker.compose.project")}},"Driver":{{json .Driver}},"HasOptions":{{if .Options}}true{{else}}false{{end}}}', $name) 'Cannot inspect volume ownership'
        $volumes += ConvertFrom-StartupJson $json 'volume ownership'
    }
    $networks = @()
    $names = Invoke-StartupDocker $Context @('network', 'ls', '--format', '{{.Name}}') 'Cannot list network ownership'
    foreach ($name in @($names -split '\s+' | Where-Object { $_ })) {
        $template = '{"Name":{{json .Name}},"Project":{{json (index .Labels "com.docker.compose.project")}},"Driver":{{json .Driver}},"HasOptions":{{if .Options}}true{{else}}false{{end}},"OptionCount":{{if .Options}}{{len .Options}}{{else}}0{{end}},"IPv4Flag":{{if .Options}}{{json (index .Options "com.docker.network.enable_ipv4")}}{{else}}null{{end}},"IPv6Flag":{{if .Options}}{{json (index .Options "com.docker.network.enable_ipv6")}}{{else}}null{{end}}}'
        $json = Invoke-StartupDocker $Context @('network', 'inspect', '--format', $template, $name) 'Cannot inspect network ownership'
        $record = ConvertFrom-StartupJson $json 'network ownership'
        # Docker 29 adds these boolean IP-family flags to ordinary bridge networks.
        # Permit only those exact flags, not arbitrary driver options or their values.
        $flags = @(@($record.IPv4Flag, $record.IPv6Flag) | Where-Object { $null -ne $_ })
        $onlyFlags = $record.OptionCount -eq $flags.Count -and @($flags | Where-Object { $_ -notin @('true', 'false') }).Count -eq 0
        $record | Add-Member -MemberType NoteProperty -Name OnlyIPFamilyOptions -Value $onlyFlags
        $networks += $record
    }
    return [pscustomobject]@{ Containers = $containers; Volumes = $volumes; Networks = $networks }
}

function Assert-ProjectEnvironmentIsolation {
    param($Context, $Model)
    if ($Context.Environment -eq 'current') { return }
    $testProject = 'nyc-taxi-bootstrap-test'
    if ($Model.name -ne $testProject -or $Context.ExpectedProject -ne $testProject) { Stop-Startup 'Test project name is not the dedicated bootstrap-test project.' }
    $runtime = Get-NormalizedEnvironmentPath $Context.RuntimeRoot
    $expectedRuntime = Get-NormalizedEnvironmentPath (Join-Path $Context.Root '.sandbox/bootstrap-test')
    if ($runtime -ne $expectedRuntime) { Stop-Startup 'Unexpected sandbox runtime root.' }
    Assert-NoEnvironmentReparsePoint $Context.RuntimeRoot
    Assert-NoEnvironmentReparsePoint $Context.EnvPath
    $protected = Get-ProtectedEnvironmentModel -Context $Context
    if ($protected.name -eq $testProject) { Stop-Startup 'Test project collides with the current project.' }
    $inventory = Get-EnvironmentDockerInventory -Context $Context
    $protectedNames = @()
    $protectedVolumes = @()
    $protectedPaths = @('data', 'logs', 'models', 'reports') | ForEach-Object { Join-Path $Context.Root $_ }
    foreach ($service in $protected.services.PSObject.Properties) {
        $name = Get-OptionalProperty $service.Value 'container_name'
        if ($name) { $protectedNames += $name }
        foreach ($mount in @(Get-OptionalProperty $service.Value 'volumes' @())) {
            if ((Get-OptionalProperty $mount 'type') -eq 'bind' -and -not (Get-OptionalProperty $mount 'read_only' $false)) { $protectedPaths += $mount.source }
        }
    }
    foreach ($volume in $protected.volumes.PSObject.Properties) { $protectedVolumes += $volume.Value.name }
    foreach ($container in @($inventory.Containers)) {
        if ($container.Project -ne $testProject) {
            $protectedNames += $container.Name
            foreach ($mount in @(Get-OptionalProperty $container 'Mounts' @())) {
                if ($mount.Type -eq 'volume') { $protectedVolumes += $mount.Name }
                elseif ($mount.Type -eq 'bind' -and $mount.RW) { $protectedPaths += $mount.Source }
            }
        }
    }
    foreach ($path in $protectedPaths) {
        Assert-NoEnvironmentReparsePoint (Get-NormalizedEnvironmentPath $path)
        if (Test-EnvironmentPathOverlap $Context.RuntimeRoot $path) { Stop-Startup 'Sandbox runtime root overlaps a protected writable bind.' }
    }
    $testVolumes = @()
    $definitions = Get-OptionalProperty $Model 'volumes'
    if ($null -eq $definitions -or @($definitions.PSObject.Properties).Count -ne 2) { Stop-Startup 'Test must use exactly the two dedicated named volumes.' }
    foreach ($volume in $definitions.PSObject.Properties) {
        $definition = $volume.Value
        $name = Get-OptionalProperty $definition 'name' ''
        if ($volume.Name -notin @('postgres-data', 'mlflow-data') -or $name -ne ($testProject + '_' + $volume.Name) -or
            (Get-OptionalProperty $definition 'external' $false) -or (Get-OptionalProperty $definition 'driver_opts') -or
            (Get-OptionalProperty $definition 'driver' 'local') -ne 'local' -or $protectedVolumes -contains $name) {
            Stop-Startup 'Test volume definition is shared/external/redirected or not dedicated.'
        }
        foreach ($live in @($inventory.Volumes | Where-Object { $_.Name -eq $name })) {
            if ($live.Project -ne $testProject -or (Get-OptionalProperty $live 'Driver' 'local') -ne 'local' -or (Get-OptionalProperty $live 'HasOptions' $false)) { Stop-Startup 'A test volume name is already owned outside the test project or redirected.' }
        }
        $testVolumes += $name
    }
    $networks = Get-OptionalProperty $Model 'networks'
    if ($null -eq $networks -or @($networks.PSObject.Properties).Count -ne 1) { Stop-Startup 'Test must use only its dedicated default network.' }
    $network = Get-OptionalProperty $networks 'default'
    if ($null -eq $network -or $network.name -ne ($testProject + '_default') -or
        (Get-OptionalProperty $network 'external' $false) -or (Get-OptionalProperty $network 'driver_opts') -or
        (Get-OptionalProperty $network 'driver' 'bridge') -ne 'bridge') { Stop-Startup 'Test network is not isolated.' }
    foreach ($live in @($inventory.Networks | Where-Object { $_.Name -eq $network.name })) {
        if ($live.Project -ne $testProject -or (Get-OptionalProperty $live 'Driver' 'bridge') -ne 'bridge' -or
            ((Get-OptionalProperty $live 'HasOptions' $false) -and -not (Get-OptionalProperty $live 'OnlyIPFamilyOptions' $false))) { Stop-Startup 'Test network name is already owned outside the test project or redirected.' }
    }
    $worker = Get-OptionalProperty $Model.services 'worker'
    $expectedWorkerImage = Get-OptionalProperty $Context 'ExpectedWorkerImage' ''
    if ($expectedWorkerImage -notin @('nyc-taxi-worker:p6-test', 'nyc-taxi-worker:p6-clean') -or
        $null -eq $worker -or $worker.image -ne $expectedWorkerImage) {
        Stop-Startup 'Test worker image tag must match the explicitly selected dedicated worker variant.'
    }
    $build = Get-OptionalProperty $worker 'build'
    if ($null -ne $build -and ((Get-NormalizedEnvironmentPath $build.context) -ne (Get-NormalizedEnvironmentPath $Context.Root) -or
        (Get-OptionalProperty $build 'dockerfile' 'Dockerfile') -notin @('Dockerfile.spark', 'Dockerfile.bootstrap-test') -or
        (Get-OptionalProperty $build 'tags') -or (Get-OptionalProperty $build 'dockerfile_inline'))) { Stop-Startup 'Test worker build must reuse an approved worker Dockerfile without additional image tags.' }
    if ($expectedWorkerImage -eq 'nyc-taxi-worker:p6-clean' -and
        ($null -eq $build -or (Get-OptionalProperty $build 'dockerfile' 'Dockerfile') -ne 'Dockerfile.spark')) {
        Stop-Startup 'The clean worker variant must build from Dockerfile.spark, never the runtime refresh Dockerfile.'
    }
    foreach ($service in $Model.services.PSObject.Properties) {
        $settings = $service.Value
        $containerName = Get-OptionalProperty $settings 'container_name' ''
        if ($containerName -and ($containerName -ne ($testProject + '-' + $service.Name) -or $protectedNames -contains $containerName)) { Stop-Startup 'Test container name collides or is not dedicated.' }
        foreach ($live in @($inventory.Containers | Where-Object { $_.Name -eq $containerName -and $containerName })) {
            if ($live.Project -ne $testProject -or $live.Service -ne $service.Name) { Stop-Startup 'Test container name is already owned outside the expected project/service.' }
        }
        if (@(Get-OptionalProperty $settings 'ports' @()).Count -gt 0) { Stop-Startup 'Bootstrap-test must not publish any host ports.' }
        foreach ($option in @('network_mode', 'volumes_from', 'devices', 'privileged', 'external_links', 'pid', 'ipc', 'configs', 'secrets')) {
            if (Get-OptionalProperty $settings $option) { Stop-Startup 'Test service has an unsupported host/shared-resource option.' }
        }
        $attached = Get-OptionalProperty $settings 'networks'
        if ($null -ne $attached -and @($attached.PSObject.Properties | Where-Object { $_.Name -ne 'default' }).Count -gt 0) { Stop-Startup 'Test service attaches to a non-test network.' }
        foreach ($mount in @(Get-OptionalProperty $settings 'volumes' @())) {
            if ($mount.type -eq 'volume') {
                if ($mount.source -notin @('postgres-data', 'mlflow-data')) { Stop-Startup 'Test service uses an unexpected named volume.' }
            } elseif ($mount.type -eq 'bind') {
                $source = Get-NormalizedEnvironmentPath $mount.source
                if (-not (Get-OptionalProperty $mount 'read_only' $false)) {
                    if (-not $source.StartsWith($runtime + '\')) { Stop-Startup 'Writable test bind is outside the sandbox runtime root.' }
                    Assert-NoEnvironmentReparsePoint $mount.source
                    foreach ($path in $protectedPaths) { if (Test-EnvironmentPathOverlap $mount.source $path) { Stop-Startup 'Writable test bind overlaps protected data.' } }
                } elseif ($source -ne (Get-NormalizedEnvironmentPath (Join-Path $Context.Root 'sql'))) {
                    Stop-Startup 'Only public SQL may be bound read-only from outside the sandbox.'
                }
            } else { Stop-Startup 'Unsupported mount type in the test environment.' }
        }
    }
    # Do not trust existing test labels alone if attachments were changed manually.
    foreach ($container in @($inventory.Containers | Where-Object { $_.Project -eq $testProject })) {
        $ports = Get-OptionalProperty $container 'Ports'
        if ($null -ne $ports -and @($ports.PSObject.Properties).Count -gt 0) { Stop-Startup 'Existing test container publishes host ports.' }
        foreach ($mount in @(Get-OptionalProperty $container 'Mounts' @())) {
            if ($mount.Type -eq 'volume' -and $mount.Name -notin $testVolumes) { Stop-Startup 'Existing test container has a non-test volume attached.' }
            if ($mount.Type -eq 'bind' -and $mount.RW) {
                $source = Get-NormalizedEnvironmentPath $mount.Source
                if (-not $source.StartsWith($runtime + '\')) { Stop-Startup 'Existing test container has a writable bind outside the sandbox.' }
                Assert-NoEnvironmentReparsePoint $mount.Source
            }
        }
        $attached = Get-OptionalProperty $container 'Networks'
        if ($null -ne $attached -and @($attached.PSObject.Properties | Where-Object { $_.Name -ne $network.name }).Count -gt 0) { Stop-Startup 'Existing test container has a shared network attached.' }
    }
}

function Initialize-BootstrapTestRuntime {
    param($Context)
    if ($Context.Environment -ne 'bootstrap-test') { Stop-Startup 'Runtime initialization is allowed only for bootstrap-test.' }
    Assert-NoEnvironmentReparsePoint $Context.RuntimeRoot
    Assert-NoEnvironmentReparsePoint $Context.EnvPath
    foreach ($directory in @('', 'data', 'logs', 'models', 'reports')) {
        $path = $Context.RuntimeRoot
        if ($directory) { $path = Join-Path $path $directory }
        Assert-NoEnvironmentReparsePoint $path
        $null = [IO.Directory]::CreateDirectory($path)
    }
    if (-not (Test-Path -LiteralPath $Context.EnvPath)) {
        $bytes = New-Object byte[] 32
        $random = [Security.Cryptography.RandomNumberGenerator]::Create()
        try { $random.GetBytes($bytes) } finally { $random.Dispose() }
        $password = [BitConverter]::ToString($bytes).Replace('-', '').ToLowerInvariant()
        $text = 'P61_POSTGRES_PASSWORD=' + $password + [Environment]::NewLine
        # CreateNew prevents concurrent invocations from replacing an existing file.
        $stream = New-Object IO.FileStream($Context.EnvPath, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write)
        try { $encoded = [Text.Encoding]::ASCII.GetBytes($text); $stream.Write($encoded, 0, $encoded.Length) } finally { $stream.Dispose() }
        Write-Host 'Created dedicated test credentials; values withheld. Existing current .env was not copied or changed.'
    } elseif (-not (Test-Path -LiteralPath $Context.EnvPath -PathType Leaf)) { Stop-Startup 'Test environment file is not a regular file.' }
}
