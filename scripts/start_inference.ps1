# Optional inference startup helpers; dot-sourcing performs no checks or actions.

function Enable-InferenceStartupContext {
    param($Context)
    if ($Context.Environment -ne 'current') { Stop-Startup '-WithInference is available only for the current environment, never bootstrap-test.' }
    $override = Join-Path $Context.Root 'docker-compose.inference.yml'
    if (-not (Test-Path -LiteralPath $override -PathType Leaf)) { Stop-Startup 'docker-compose.inference.yml is missing; inference startup is unavailable.' }
    $Context.ComposeArguments += @('--file', $override, '--project-name', 'nyc-taxi-bigdata')
    $Context.ExpectedProject = 'nyc-taxi-bigdata'
    $Context.Services = @('postgres', 'mlflow', 'fare-inference', 'dashboard')
    $Context.BuildServices = @('worker', 'dashboard', 'fare-inference')
    $Context | Add-Member -MemberType NoteProperty -Name WithInference -Value $true -Force
    $Context | Add-Member -MemberType NoteProperty -Name InferenceArtifacts -Value $null -Force
}

function Get-InferenceUpdateCommand {
    param($Context, [ValidateSet('fare-inference', 'dashboard')][string]$Service)
    # This is a review instruction, never executed. Single-quoted PowerShell
    # literals preserve paths containing spaces, $, backticks or apostrophes.
    $project = "'" + $Context.Root.Replace("'", "''") + "'"
    $base = "'" + (Join-Path $Context.Root 'docker-compose.yml').Replace("'", "''") + "'"
    $override = "'" + (Join-Path $Context.Root 'docker-compose.inference.yml').Replace("'", "''") + "'"
    return ('docker compose --project-directory ' + $project + ' -f ' + $base + ' -f ' + $override +
        ' --project-name nyc-taxi-bigdata up -d --no-deps --no-build --pull never ' + $Service)
}

function Test-InferenceReadiness {
    param($Context, $Container)
    if ($null -eq $Container -or $Container.Service -ne 'fare-inference' -or
        $Container.Project -ne $Context.Project -or $Container.Status -ne 'running' -or $Container.Health -ne 'healthy') {
        Stop-Startup 'Inference readiness requires the expected healthy current-project container.'
    }
    $expected = Get-OptionalProperty $Context 'InferenceArtifacts'
    if ($null -eq $expected -or (Get-OptionalProperty $expected 'Verified' $false) -ne $true) {
        Stop-Startup 'Frozen artifacts were not verified before the inference readiness check.'
    }
    foreach ($pin in @(@{Name='ModelSHA256';Pattern='^[0-9a-f]{64}$'},
                       @{Name='TrainingRunId';Pattern='^[0-9a-f]{32}$'},
                       @{Name='EvaluationRunId';Pattern='^[0-9a-f]{32}$'})) {
        $value = Get-OptionalProperty $expected $pin.Name
        if ($value -isnot [string] -or $value -cnotmatch $pin.Pattern) {
            Stop-Startup 'Verified artifact identity is incomplete; inference readiness cannot be accepted.'
        }
    }
    $probe = @'
import json, urllib.request
with urllib.request.urlopen('http://127.0.0.1:8000/ready', timeout=3) as response:
    body = response.read(16385)
    if response.status != 200 or len(body) > 16384:
        raise ValueError('Readiness response rejected')
value = json.loads(body)
print(json.dumps(value, allow_nan=False))
'@
    $timeout = [Math]::Min(10, $Context.CommandTimeoutSeconds)
    $json = Invoke-StartupDocker -Context $Context -Arguments @('exec', $Container.ID, 'python', '-I', '-B', '-c', $probe) -TimeoutSeconds $timeout -FailureMessage 'Inference readiness request failed; no prediction or fallback was requested. Inspect the service separately'
    $ready = ConvertFrom-StartupJson $json 'inference readiness response'
    $readyFlag = Get-OptionalProperty $ready 'ready'
    $loadCount = Get-OptionalProperty $ready 'model_load_count'
    if ($readyFlag -isnot [bool] -or $readyFlag -ne $true -or
        (Get-OptionalProperty $ready 'status') -ne 'ready' -or
        ($loadCount -isnot [int] -and $loadCount -isnot [long]) -or $loadCount -ne 1 -or
        (Get-OptionalProperty $ready 'model_sha256') -ne (Get-OptionalProperty $expected 'ModelSHA256') -or
        (Get-OptionalProperty $ready 'training_run_id') -ne (Get-OptionalProperty $expected 'TrainingRunId') -or
        (Get-OptionalProperty $ready 'evaluation_run_id') -ne (Get-OptionalProperty $expected 'EvaluationRunId')) {
        Stop-Startup 'Inference readiness/identity differs from the verified frozen model. No price or fallback is available; review separately.'
    }
    Write-Host ('Inference readiness: verified frozen model; model_load_count=1; model SHA256=' + $ready.model_sha256 + '.')
    return $ready
}
