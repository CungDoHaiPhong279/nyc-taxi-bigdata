# Offline Pester 4.10.1: synthetic inventory + mocked pinned manifest, no model bundle.
. (Join-Path (Split-Path -Parent $PSScriptRoot) 'scripts/model_artifacts.ps1')

Describe 'Frozen model artifact preflight' {
    BeforeEach {
        $script:ArtifactRoot = Join-Path $TestDrive 'project'
        $script:ArtifactLock = Join-Path $script:ArtifactRoot 'models/ml-p3/20261023-june01/candidate_lock.json'
        $null = New-Item -ItemType Directory -Path (Split-Path $script:ArtifactLock -Parent) -Force
        $script:ArtifactManifest = [ordered]@{
            training_run_id='d2772f1904d7472490a73b0ac1c0c5d4'
            model_tree=@{sha256='759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee'}
            feature_contract_sha256=('a' * 64); zone_lookup_sha256=('b' * 64)
            core_file_sha256=@{'src/frozen.py'=('c' * 64)}
        }
        foreach ($relative in @('models/ml-p2/20261023-pilot02/fitted_model/stage',
            'models/ml-p2/20261023-pilot02/feature_contract.json','data/raw/taxi_zones/taxi_zone_lookup.csv','src/frozen.py')) {
            $path = Join-Path $script:ArtifactRoot $relative
            $null = New-Item -ItemType Directory -Path (Split-Path $path -Parent) -Force
            [IO.File]::WriteAllText($path, 'offline fixture')
        }
        $script:ArtifactManifest | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $script:ArtifactLock -Encoding UTF8
        Mock Get-FileHash {
            param($LiteralPath, $Algorithm)
            $hash = 'c' * 64
            if ($LiteralPath -like '*candidate_lock.json') { $hash = '12442b5684943bb81169bb084cffc8c1b54043882d75711e295a0f1a0ff7338d' }
            if ($LiteralPath -like '*feature_contract.json') { $hash = 'a' * 64 }
            if ($LiteralPath -like '*taxi_zone_lookup.csv') { $hash = 'b' * 64 }
            [pscustomobject]@{Hash=$hash}
        }
        Mock Get-FareModelTree { [pscustomobject]@{sha256='759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee';files=@(1..40)} }
    }

    It 'pins all three full identities independently of local artifacts' {
        $pin = Get-FareModelIdentity
        $pin.ModelSHA256 | Should -BeExactly '759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee'
        $pin.TrainingRunId | Should -BeExactly 'd2772f1904d7472490a73b0ac1c0c5d4'
        $pin.EvaluationRunId | Should -BeExactly '58d5008f0ed048f58599a859877ae336'
    }
    It 'accepts a fully verified manifest and returns the startup contract' {
        $result = Assert-FareArtifacts $script:ArtifactRoot
        $result.Verified | Should -Be $true
        $result.ModelDirectory | Should -Be (Join-Path $script:ArtifactRoot 'models/ml-p2/20261023-pilot02/fitted_model')
    }
    It 'rejects missing artifact paths without creating them' {
        { Assert-FareArtifacts (Join-Path $TestDrive 'missing') } | Should -Throw 'Missing artifact/source'
        Test-Path (Join-Path $TestDrive 'missing') | Should -Be $false
    }
    It 'rejects a wrong candidate-lock hash before trusting its JSON' {
        Mock Get-FileHash { [pscustomobject]@{Hash=('0' * 64)} }
        { Assert-FareArtifacts $script:ArtifactRoot } | Should -Throw 'candidate_lock.json SHA256 mismatch'
        Assert-MockCalled Get-FareModelTree -Times 0 -Exactly -Scope It
    }
    It 'rejects modified model files even if the local lock still claims the pinned hash' {
        Mock Get-FareModelTree { [pscustomobject]@{sha256=('0' * 64);files=@(1..40)} }
        { Assert-FareArtifacts $script:ArtifactRoot } | Should -Throw 'tree SHA256 mismatch'
    }
    It 'rejects a missing or extra file inventory' {
        Mock Get-FareModelTree { [pscustomobject]@{sha256='759337ab73d6d6cd171edb4b0b42ff325f991cc7cda43e722905ce28c5c947ee';files=@(1..39)} }
        { Assert-FareArtifacts $script:ArtifactRoot } | Should -Throw 'exactly 40 files'
    }
    It 'rejects a different training run even with the expected model hash' {
        $script:ArtifactManifest.training_run_id = '00000000000000000000000000000000'
        $script:ArtifactManifest | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $script:ArtifactLock -Encoding UTF8
        { Assert-FareArtifacts $script:ArtifactRoot } | Should -Throw 'Frozen model identity'
    }
    It 'rejects mismatched feature contract and zone bytes' -TestCases @(
        @{Name='feature_contract_sha256'; Expected='feature_contract.json SHA256 mismatch'}
        @{Name='zone_lookup_sha256'; Expected='zone lookup CSV SHA256 mismatch'}
    ) {
        param($Name,$Expected)
        $script:ArtifactManifest[$Name] = '0' * 64
        $script:ArtifactManifest | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $script:ArtifactLock -Encoding UTF8
        { Assert-FareArtifacts $script:ArtifactRoot } | Should -Throw $Expected
    }
    It 'rejects changed frozen preprocessing bytes without updating the lock' {
        $script:ArtifactManifest.core_file_sha256['src/frozen.py'] = '0' * 64
        $script:ArtifactManifest | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $script:ArtifactLock -Encoding UTF8
        { Assert-FareArtifacts $script:ArtifactRoot } | Should -Throw 'Frozen preprocessing/dependency source mismatch'
    }
    It 'rejects lock paths outside the project root' {
        { Resolve-FareArtifactPath $script:ArtifactRoot '../outside' } | Should -Throw 'escapes the project root'
    }
}

Describe 'Cross-language canonical model tree hash' {
    It 'does not collapse distinct paths that differ only by letter case' {
        $root = Join-Path $TestDrive 'case-sensitive-inventory'
        $null = New-Item -ItemType Directory -Path $root -Force
        [IO.File]::WriteAllBytes((Join-Path $root 'a.txt'), [byte[]]@(65))
        # FileInfo paths emulate a case-sensitive directory without needing NTFS
        # configuration changes, symlink privileges or real model artifacts.
        $script:CaseInventory = @([IO.FileInfo]::new((Join-Path $root 'A.txt')), [IO.FileInfo]::new((Join-Path $root 'a.txt')))
        $script:CaseInventoryRoot = $root
        Mock Get-ChildItem { $script:CaseInventory } -ParameterFilter { $LiteralPath -eq $script:CaseInventoryRoot }
        $tree = Get-FareModelTree $root
        @($tree.files).Count | Should -Be 2
        $tree.files[0].path | Should -BeExactly 'A.txt'
        $tree.files[1].path | Should -BeExactly 'a.txt'
    }

    It 'matches the independently calculated Python JSON hash including hidden zero-byte files' {
        $root = Join-Path $TestDrive 'canonical'
        $null = New-Item -ItemType Directory -Path (Join-Path $root 'nested') -Force
        [IO.File]::WriteAllBytes((Join-Path $root 'a.txt'), [byte[]]@(65))
        [IO.File]::WriteAllBytes((Join-Path $root 'nested/.zero'), [byte[]]@())
        $tree = Get-FareModelTree $root
        @($tree.files).Count | Should -Be 2
        $tree.sha256 | Should -BeExactly '94a44af58e16a8c0325d3d088529a118daf02c585b5f0ba64d2f55c42d7aee97'
        $tree.files[1].path | Should -BeExactly 'nested/.zero'
    }
}
