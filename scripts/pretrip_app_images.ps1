# Standalone pretrip image provenance. Dot-source performs no Docker action.
function Stop-PretripAppImage {
    param([string]$Message)
    $failure=New-Object System.Exception ('Pretrip image guard: '+$Message)
    $failure.Data['StartupSafeMessage']=$failure.Message
    throw $failure
}
function Get-PretripAppSHA256 {
    param([byte[]]$Bytes)
    $algorithm=[Security.Cryptography.SHA256]::Create()
    try { return [BitConverter]::ToString($algorithm.ComputeHash($Bytes)).Replace('-','').ToLowerInvariant() }
    finally { $algorithm.Dispose() }
}
function Assert-PretripAppImageLocalPath {
    param([string]$Path)
    $current=[IO.Path]::GetFullPath($Path)
    while($current) {
        if(Test-Path -LiteralPath $current) {
            if(((Get-Item -LiteralPath $current -Force).Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                Stop-PretripAppImage 'Build inputs cannot traverse a symlink/junction/reparse point.'
            }
        }
        $parent=[IO.Path]::GetDirectoryName($current.TrimEnd('\','/'))
        if(-not $parent -or $parent -eq $current){break}; $current=$parent
    }
}
function Get-PretripAppInputFiles {
    param([string]$ProjectRoot,[string[]]$Paths)
    $root=[IO.Path]::GetFullPath($ProjectRoot).TrimEnd('\','/')
    $found=@{}
    foreach($relative in $Paths) {
        $path=Join-Path $root $relative
        Assert-PretripAppImageLocalPath $path
        if(-not (Test-Path -LiteralPath $path)){Stop-PretripAppImage 'A required repository build input is missing.'}
        $pending=New-Object 'Collections.Generic.Stack[string]'; $pending.Push($path)
        while($pending.Count) {
            $current=$pending.Pop(); Assert-PretripAppImageLocalPath $current
            $item=Get-Item -LiteralPath $current -Force
            $rel=$item.FullName.Substring($root.Length+1).Replace('\','/')
            if($rel -match '(^|/)(\.git|\.venv|venv|__pycache__|\.pytest_cache)(/|$)' -or
               $item.Name -like '.env*' -or $item.Name -like '*.egg-info' -or $item.Name -like '*.pyc'){continue}
            if($item -is [IO.DirectoryInfo]) {
                foreach($child in Get-ChildItem -LiteralPath $current -Force){$pending.Push($child.FullName)}
            } else {
                $found[$rel]=[pscustomobject]@{path=$rel;bytes=[long]$item.Length;sha256=(Get-FileHash -LiteralPath $current -Algorithm SHA256).Hash.ToLowerInvariant()}
            }
        }
    }
    return @($found.Values | Sort-Object path)
}
function Get-PretripAppImagePlan {
    param([string]$ProjectRoot)
    $root=[IO.Path]::GetFullPath($ProjectRoot)
    $workerPaths=@('requirements.txt','requirements-worker-constraints.txt','pyproject.toml','README.md','src','scripts','config/config.yaml','sql','tests')
    $inferencePaths=$workerPaths+@('config/pretrip_v2_contract.json','config/pretrip_v2_calendar.json','config/model_bundle_v22.lock.json','Dockerfile.inference','docker-compose.inference.yml','docker-compose.pretrip-v22.yml')
    $webPaths=@('requirements-dashboard.txt','src/nyc_taxi/__init__.py','src/nyc_taxi/common/__init__.py','src/nyc_taxi/common/config.py',
        'src/nyc_taxi/database/__init__.py','src/nyc_taxi/database/postgres.py','src/nyc_taxi/dashboard','src/nyc_taxi/ml/__init__.py',
        'src/nyc_taxi/ml/pretrip_features.py','src/nyc_taxi/ml/pretrip_serving_contract.py','assets/taxi_zones','config/config.yaml',
        'config/pretrip_v2_contract.json','config/pretrip_v2_calendar.json','dashboard/app.py','scripts/check_dashboard_package.py')
    $dependencyFiles=Get-PretripAppInputFiles $root @('requirements.txt','requirements-worker-constraints.txt','pyproject.toml','requirements-dashboard.txt')
    $dependencyHash=Get-PretripAppSHA256 ([Text.Encoding]::UTF8.GetBytes((ConvertTo-Json -InputObject @($dependencyFiles) -Compress -Depth 5)))
    $images=@()
    foreach($role in @('worker','inference','dashboard')) {
        $paths=$workerPaths; $dockerfile='Dockerfile.spark'
        if($role -eq 'inference'){$paths=$inferencePaths;$dockerfile='Dockerfile.inference'}
        if($role -eq 'dashboard'){$paths=$webPaths;$dockerfile='Dockerfile.dashboard'}
        $files=Get-PretripAppInputFiles $root $paths
        $buildInputs=Get-PretripAppInputFiles $root @($dockerfile,'.dockerignore')
        $identity=[ordered]@{schema='pretrip-app-image-v1';role=$role;files=@($files);build_inputs=@($buildInputs);dependency_sha256=$dependencyHash}
        if($role -eq 'inference'){$identity['parent_worker_source_sha256']=$images[0].SourceSHA256}
        $sourceHash=Get-PretripAppSHA256 ([Text.Encoding]::UTF8.GetBytes((ConvertTo-Json -InputObject $identity -Compress -Depth 8)))
        $tag='nyc-taxi-'+$role+':pretrip-v22-app-'+$sourceHash.Substring(0,24)
        $images+= [pscustomobject]@{Role=$role;Tag=$tag;SourceSHA256=$sourceHash;DependencySHA256=$dependencyHash;
            Files=@($files);Dockerfile=$dockerfile;Status='unchecked';ImageID=$null}
    }
    $combined=Get-PretripAppSHA256 ([Text.Encoding]::UTF8.GetBytes((@($images | ForEach-Object {$_.Role+':'+$_.SourceSHA256}) -join "`n")))
    return [pscustomobject]@{WorkerImage=$images[0].Tag;InferenceImage=$images[1].Tag;DashboardImage=$images[2].Tag;
        SourceSHA256=$combined;DependencySHA256=$dependencyHash;Images=$images;InferenceImageID=$null;DashboardImageID=$null;
        ColdBuild='Missing images require the repository Dockerfiles, Python base, Java and pinned Python dependencies to download/build. Cold disk/time are not measured; PLAN does not download.'}
}
function Get-PretripAppImageState {
    param($Context,$Image)
    # Failed inspect is never reclassified as missing.
    $idsText=Invoke-PretripAppDocker -Context $Context -Arguments @('image','ls','--no-trunc','--filter',('reference='+$Image.Tag),'--format','{{.ID}}') -FailureMessage 'Cannot list the exact standalone image tag.'
    $ids=@($idsText -split '\s+' | Where-Object {$_})
    if($ids.Count -eq 0){return $null}
    if($ids.Count -ne 1 -or $ids[0] -cnotmatch '^sha256:[0-9a-f]{64}$'){Stop-PretripAppImage 'Ambiguous image identity; review separately.'}
    $json=Invoke-PretripAppDocker -Context $Context -Arguments @('image','inspect','--format','{"ID":{{json .Id}},"Labels":{{json .Config.Labels}}}',$Image.Tag) -FailureMessage 'Cannot inspect standalone image provenance.'
    try {$record=$json | ConvertFrom-Json} catch {Stop-PretripAppImage 'Image provenance is not valid JSON.'}
    if($record.ID -cne $ids[0] -or $null -eq $record.Labels){Stop-PretripAppImage 'Image provenance is missing or inconsistent.'}
    $expected=@{'com.nyc-taxi.pretrip-app.schema'='1';'com.nyc-taxi.pretrip-app.role'=$Image.Role;
        'com.nyc-taxi.pretrip-app.source-sha256'=$Image.SourceSHA256;'com.nyc-taxi.pretrip-app.dependency-sha256'=$Image.DependencySHA256}
    foreach($name in $expected.Keys) {
        $property=$record.Labels.PSObject.Properties[$name]
        if($null -eq $property -or $property.Value -cne $expected[$name]){Stop-PretripAppImage 'The versioned tag contains foreign or drifted image provenance; no replacement attempted.'}
    }
    return $record.ID
}
function Test-PretripAppImageRuntime {
    param($Context,$Image)
    # No model mounts, network, training or SparkContext; expected bytes in memory.
    $inventory=ConvertTo-Json -InputObject @($Image.Files) -Compress -Depth 5
    $probe=@'
import hashlib, importlib, json, pathlib, subprocess, sys
files=json.load(sys.stdin); role=sys.argv[1]
for item in files:
    path=pathlib.Path('/app')/item['path']
    data=path.read_bytes()
    if len(data)!=item['bytes'] or hashlib.sha256(data).hexdigest()!=item['sha256']:
        raise SystemExit('Image runtime input hash mismatch')
sys.path.insert(0,'/app/src')
subprocess.run([sys.executable,'-m','pip','check'],capture_output=True,check=True,timeout=60)
modules=('pyspark','pyarrow','psycopg','nyc_taxi.ml.inference_server') if role!='dashboard' else ('streamlit','nyc_taxi.dashboard.pretrip_prediction')
for name in modules: importlib.import_module(name)
if role=='dashboard':
    import runpy
    runpy.run_path('/app/scripts/check_dashboard_package.py')['check_package'](pathlib.Path('/app'))
print('PRETRIP_IMAGE_RUNTIME_VERIFIED')
'@
    $arguments=@('run','--rm','-i','--network','none','--read-only','--cpus','1','--memory','256m','--memory-swap','256m','--pids-limit','64',
        '--tmpfs','/tmp:rw,nosuid,nodev,size=16777216,mode=1777','--entrypoint','python',$Image.ImageID,'-I','-B','-c',$probe,$Image.Role)
    $result=Invoke-PretripAppDocker -Context $Context -Arguments $arguments -InputText $inventory -FailureMessage 'Offline image runtime/hash/dependency verification failed; no model was loaded.' -TimeoutSeconds 120
    if($result.Trim() -cne 'PRETRIP_IMAGE_RUNTIME_VERIFIED'){Stop-PretripAppImage 'Offline image verification did not return its exact success marker.'}
}
function Assert-PretripAppImages {
    param($Context,[switch]$Execute)
    $plan=Get-PretripAppImagePlan -ProjectRoot $Context.Root
    foreach($image in $plan.Images) {
        $id=Get-PretripAppImageState -Context $Context -Image $image
        if($null -eq $id){$image.Status='build-required'}else{$image.ImageID=$id;$image.Status='reuse-provenance-checked'}
    }
    foreach($image in $plan.Images) {
        if($image.Role -eq 'worker' -and $plan.Images[1].Status -ne 'build-required') {
            if($null -eq $image.ImageID){$image.Status='not-needed'}; continue
        }
        if($image.Status -eq 'build-required' -and $Execute) {
            $args=@('build','--pull=false','--file',(Join-Path $Context.Root $image.Dockerfile),'--tag',$image.Tag)
            foreach($pair in @('schema=1',('role='+$image.Role),('source-sha256='+$image.SourceSHA256),('dependency-sha256='+$image.DependencySHA256))) {
                $args+=@('--label',('com.nyc-taxi.pretrip-app.'+$pair))
            }
            if($image.Role -eq 'inference'){$args+=@('--build-arg',('WORKER_RUNTIME_IMAGE='+$plan.WorkerImage))}
            if($image.Role -eq 'dashboard'){$args+=@('--target','dashboard')}
            $args+=$Context.Root
            $null=Invoke-PretripAppDocker -Context $Context -Arguments $args -FailureMessage 'Standalone image build failed; outputs retained, no services started.' -TimeoutSeconds $Context.BuildTimeoutSeconds
            $image.ImageID=Get-PretripAppImageState -Context $Context -Image $image
            if($null -eq $image.ImageID){Stop-PretripAppImage 'Build returned without a verifiable versioned image.'}
            $image.Status='built'
        }
        if($Execute -and $null -ne $image.ImageID){Test-PretripAppImageRuntime -Context $Context -Image $image;$image.Status='runtime-verified'}
    }
    if($Execute){$plan.InferenceImageID=$plan.Images[1].ImageID;$plan.DashboardImageID=$plan.Images[2].ImageID}
    return $plan
}
