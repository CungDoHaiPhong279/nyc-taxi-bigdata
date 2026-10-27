#requires -Version 5.1
<# Full real Jan-Jun/2025 setup, explicit Execute. Default PLAN never runs data.
   The reviewed fixed model bundle is supplied with the repository. #>
[CmdletBinding()]
param([switch]$Execute,[switch]$PrepareTest,
    [ValidateSet('v1','v2.2')][string]$ModelVersion='v1',
    [ValidateSet('current','package-test')][string]$Environment='current',
    [string]$BundleDirectory,[ValidateRange(1,300)][int]$CommandTimeoutSeconds=30,
    [ValidateRange(60,86400)][int]$StepTimeoutSeconds=21600)
$script:SetupInvocation=@{Execute=[bool]$Execute;PrepareTest=[bool]$PrepareTest;ModelVersion=$ModelVersion;Environment=$Environment;BundleDirectory=$BundleDirectory;CommandTimeoutSeconds=$CommandTimeoutSeconds;StepTimeoutSeconds=$StepTimeoutSeconds}
. (Join-Path $PSScriptRoot 'bootstrap.ps1')
. (Join-Path $PSScriptRoot 'model_bundle.ps1')
. (Join-Path $PSScriptRoot 'package_operations.ps1')
$script:SetupRoot=[IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))

function Invoke-SystemSetup {
    param([string]$ProjectRoot=$script:SetupRoot,[ValidateSet('current','package-test')][string]$Environment='current',
        [ValidateSet('v1','v2.2')][string]$ModelVersion='v1',
        [switch]$Execute,[switch]$PrepareTest,[string]$BundleDirectory,[int]$CommandTimeoutSeconds=30,[int]$StepTimeoutSeconds=21600)
    $ctx=$null; $state=$null; $canCheckpoint=$false; $executionLock=$null
    try {
        if($ModelVersion -ne 'v1') { Stop-Startup 'Full package bootstrap remains v1 and will not silently switch models. For V2.2: prepare_pretrip_serving_bundle.py --execute, verify model selection, then start.ps1 -WithInference -ModelVersion v2.2. No bootstrap or lifecycle action attempted.' }
        if ($PrepareTest -and ($Environment -ne 'package-test' -or $Execute)) { Stop-Startup '-PrepareTest is configuration-only, requires package-test and cannot be combined with Execute.' }
        $ctx=New-PackageEnvironmentContext -ProjectRoot $ProjectRoot -Environment $Environment -CommandTimeoutSeconds $CommandTimeoutSeconds -StepTimeoutSeconds $StepTimeoutSeconds
        Write-Host ('PACKAGE P2 '+$(if($Execute){'EXECUTE requested'}else{'PLAN only'})+'; '+$Environment+'; real data Jan-Jun/2025, not fixtures.')
        Write-Host 'Stages: configuration -> images -> infrastructure -> existing bootstrap Calendar/Raw/Bronze/Silver/Gold/dimensions/facts/indexes/view/actual SQL validation -> verified model install -> applications -> readiness.'
        if ($PrepareTest) { Initialize-PackageConfiguration $ctx }
        $preflight=Get-PackagePreflight $ctx
        if (-not (Get-OptionalProperty $preflight 'Verified' $false)) { Stop-Startup 'Package preflight is not verified.' }
        $bundle=Get-PackageBundleSelection -ProjectRoot $ctx.Root -BundleDirectory $BundleDirectory
        if (-not (Get-OptionalProperty $bundle 'Verified' $false)) { Stop-Startup 'Pinned model bundle is not verified.' }
        if (-not $Execute) {
            Write-Host 'PLAN/preflight complete; no image build/pull, service startup, model install, worker, download or bootstrap ran.'
            Write-Host 'Pinned repository model bundle verified; no model download or training required. Full new-machine bootstrap is NOT end-to-end verified.'
            return 0
        }
        if (-not (Get-OptionalProperty $preflight 'OwnershipReady' $false)) { Stop-Startup 'Existing resources are not owned by this package checkpoint. Refusing to bootstrap/adopt the live or fixture database.' }
        Initialize-PackageConfiguration $ctx
        $executionLock=Enter-PackageExecutionLock $ctx
        $previous=Read-PackageCheckpoint $ctx
        $ctx | Add-Member ValidationNotBeforeUTC ([DateTime]::UtcNow) -Force
        $state=[ordered]@{SchemaVersion=1;Owner='setup_system_v1';Project=$ctx.Project;RuntimeRoot=$ctx.RuntimeRoot;
            ModelSHA256=(Get-FareModelIdentity).ModelSHA256;Stage='preflight';Status='running';UpdatedAtUTC='';CompletedStages=@()}
        # A checkpoint is progress/ownership only, never proof of ready data or a
        # reason to skip lineage checks/full validation on the next Execute.
        $canCheckpoint=$true; Write-PackageCheckpoint $ctx $state
        $second=Get-PackagePreflight $ctx
        if (-not (Get-OptionalProperty $second 'Verified' $false) -or -not (Get-OptionalProperty $second 'OwnershipReady' $false)) { Stop-Startup 'Package ownership/config changed before execution.' }
        foreach ($stage in @('images','infrastructure','bootstrap','model','application','validation')) {
            $state.Stage=$stage;Write-PackageCheckpoint $ctx $state
            $watch=[Diagnostics.Stopwatch]::StartNew();Write-Host ('START '+$stage)
            switch ($stage) {
                'images' {$result=Ensure-PackageImages $ctx;if(-not(Get-OptionalProperty $result 'Verified' $false)){Stop-Startup 'Image verification failed.'}}
                'infrastructure' {$rc=Start-PackagePhase $ctx 'infrastructure';if($rc -ne 0){Stop-Startup 'Infrastructure startup failed.'}}
                'bootstrap' {$rc=Invoke-DataBootstrap -ProjectRoot $ctx.Root -Environment $ctx.Environment -Context $ctx -Execute -StepTimeoutSeconds $StepTimeoutSeconds;if($rc -ne 0){Stop-Startup ('Bootstrap failed/timeout (exit '+$rc+'). A worker may still be active; inspect before retry. No automatic force or retry.')}}
                'model' {$result=Install-PackageModelBundle -ProjectRoot $ctx.Root -RuntimeRoot $ctx.ModelRuntimeRoot -BundlePath $bundle.BundlePath -ManifestPath $bundle.ManifestPath -ManifestSHA256 $bundle.ManifestSHA256;if(-not(Get-OptionalProperty $result 'Verified' $false)){Stop-Startup 'Model install is not verified.'}}
                'application' {$rc=Start-PackagePhase $ctx 'application';if($rc -ne 0){Stop-Startup 'Application startup failed.'}}
                'validation' {$result=Test-PackageFullValidation $ctx;if(-not(Get-OptionalProperty $result 'Verified' $false)){Stop-Startup 'Actual full validation/model readiness failed.'}}
            }
            $watch.Stop();$state.CompletedStages+=@($stage);Write-PackageCheckpoint $ctx $state
            Write-Host ('RESULT '+$stage+': success, '+[Math]::Round($watch.Elapsed.TotalSeconds,1)+'s')
        }
        $state.Status='ready';$state.Stage='ready';Write-PackageCheckpoint $ctx $state
        Write-Host 'READY: real six-month SQL counts/lineage/indexes/view and pinned model readiness verified, not load-log-only.'
        if($Environment -eq 'package-test'){Write-Host 'Dashboard http://127.0.0.1:18501 ; MLflow http://127.0.0.1:15000'}else{Write-Host 'Dashboard http://127.0.0.1:8501 ; MLflow http://127.0.0.1:5000'}
        return 0
    } catch {
        if ($canCheckpoint -and $null -ne $state) {
            $state.Status='failed'
            try {Write-PackageCheckpoint $ctx $state} catch {Write-Warning 'Could not save failure checkpoint; existing state retained. Inspect one-off workers before rerun.'}
        }
        $safe=$_.Exception.Data['StartupSafeMessage'];if(-not $safe){$safe=$_.Exception.Data['PackageSafeMessage']}
        if(-not $safe){$safe='Package setup stopped. Raw details withheld; inspect the failed stage/checkpoint and validate paths/resources/model pins. No later stage or automatic training was run.'}
        Write-Warning $safe
        Write-Host 'Resume: fix the reported cause, confirm no previous worker/transaction is active, rerun the SAME environment/bundle command. Existing CLI lineage/skip checks remain authoritative.'
        return 1
    } finally {
        if($null -ne $executionLock){$executionLock.Dispose()}
    }
}
if($MyInvocation.InvocationName -ne '.'){exit(Invoke-SystemSetup @script:SetupInvocation)}
