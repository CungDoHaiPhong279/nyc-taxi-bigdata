#requires -Version 5.1
[CmdletBinding()]
param(
    # Optional manifest/directory for an isolated Pester download; never installs a module.
    [string]$ModulePath
)

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
Set-StrictMode -Version Latest

try {
    $projectRoot = Split-Path -Parent $PSScriptRoot
    $expectedSuites = @(
        'bootstrap.Tests.ps1'
        'environment.Tests.ps1'
        'fixture_pipeline.Tests.ps1'
        'model_artifacts.Tests.ps1'
        'p62.Tests.ps1'
        'start.Tests.ps1'
        'start_inference.Tests.ps1'
    )
    $testsRoot = Join-Path $projectRoot 'tests'
    $testFiles = @(Get-ChildItem -LiteralPath $testsRoot -Filter '*.Tests.ps1' -File -Recurse | Sort-Object FullName)
    $actualNames = @($testFiles | ForEach-Object { $_.Name } | Sort-Object)
    if ($testFiles.Count -ne $expectedSuites.Count -or
        @(Compare-Object -ReferenceObject $expectedSuites -DifferenceObject $actualNames).Count -ne 0) {
        throw 'PowerShell mock suite inventory changed: classify every new/missing *.Tests.ps1 before updating CI.'
    }

    $scripts = @(Get-ChildItem -LiteralPath (Join-Path $projectRoot 'scripts') -Filter '*.ps1' -File -Recurse)
    $parseCount = 0
    foreach ($file in @($scripts) + @($testFiles)) {
        $tokens = $null
        $parseErrors = $null
        $null = [Management.Automation.Language.Parser]::ParseFile($file.FullName, [ref]$tokens, [ref]$parseErrors)
        if (@($parseErrors).Count -gt 0) {
            foreach ($parseError in $parseErrors) {
                Write-Host ('Syntax error in {0}:{1}: {2}' -f $file.Name, $parseError.Extent.StartLineNumber, $parseError.Message)
            }
            throw 'PowerShell parser check failed.'
        }
        $parseCount++
    }
    Write-Host ('PowerShell parser: {0} files passed.' -f $parseCount)

    if ($ModulePath) {
        $module = Import-Module -Name $ModulePath -Force -PassThru
    } else {
        $module = Import-Module -Name Pester -RequiredVersion 4.10.1 -Force -PassThru
    }
    $selected = @($module | Where-Object { $_.Name -eq 'Pester' })
    if ($selected.Count -ne 1 -or $selected[0].Version -ne [version]'4.10.1') {
        throw 'CI requires exactly Pester 4.10.1; use an isolated module download or install that exact version.'
    }

    $timer = [Diagnostics.Stopwatch]::StartNew()
    $result = Invoke-Pester -Script @($testFiles.FullName) -PassThru -Strict
    $timer.Stop()
    if (-not $result -or $result.TotalCount -eq 0) {
        throw 'Pester did not execute any tests.'
    }
    Write-Host ('Pester {0}: suites={1}, total={2}, passed={3}, failed={4}, skipped={5}, pending={6}, seconds={7:N2}.' -f
        $selected[0].Version, $testFiles.Count, $result.TotalCount, $result.PassedCount, $result.FailedCount,
        $result.SkippedCount, $result.PendingCount, $timer.Elapsed.TotalSeconds)
    if ($result.FailedCount -gt 0 -or $result.SkippedCount -gt 0 -or $result.PendingCount -gt 0) {
        throw 'PowerShell mock tests failed, were skipped, or remained pending.'
    }
    exit 0
} catch {
    Write-Error ('CI PowerShell checks failed: {0}' -f $_.Exception.Message) -ErrorAction Continue
    exit 1
}
