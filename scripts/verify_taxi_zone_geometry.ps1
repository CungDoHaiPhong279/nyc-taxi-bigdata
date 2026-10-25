#requires -Version 5.1
<#
Verify downloaded Taxi Zone topology without installing GIS dependencies.
Uses the host's existing SQL Server spatial assembly in-process; no database is
opened. Outputs a small JSON summary to stdout and never repairs geometry.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$GeoJsonPath,
    [ValidateRange(1, 10000)][int]$ExpectedFeatureCount = 263,
    [string]$SqlGeometryAssemblyPath
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function ConvertTo-GeometryPointText {
    param([object]$Position)
    if ($Position -isnot [array] -or $Position.Count -ne 2) {
        throw 'Geometry position must contain two WGS84 numbers.'
    }
    $parts = [System.Collections.Generic.List[string]]::new()
    for ($coordinateIndex = 0; $coordinateIndex -lt 2; $coordinateIndex++) {
        $number = $Position[$coordinateIndex]
        if ($number -is [bool] -or $number -is [string] -or $number -isnot [ValueType]) {
            throw 'Geometry coordinate must be numeric.'
        }
        $value = [double]$number
        $limit = if ($coordinateIndex -eq 0) { 180 } else { 90 }
        if ([double]::IsNaN($value) -or [double]::IsInfinity($value) -or [Math]::Abs($value) -gt $limit) {
            throw 'Geometry coordinate is outside WGS84 bounds.'
        }
        $parts.Add($value.ToString('R', [Globalization.CultureInfo]::InvariantCulture))
    }
    return $parts -join ' '
}

function ConvertTo-GeometryPolygonText {
    param([object]$Rings)
    if ($Rings -isnot [array] -or $Rings.Count -lt 1) {
        throw 'Geometry polygon must contain an outer ring.'
    }
    $ringTexts = [System.Collections.Generic.List[string]]::new()
    foreach ($ring in $Rings) {
        if ($ring -isnot [array] -or $ring.Count -lt 4) {
            throw 'Geometry ring must contain at least four positions.'
        }
        $pointTexts = [System.Collections.Generic.List[string]]::new()
        foreach ($position in $ring) {
            $pointTexts.Add((ConvertTo-GeometryPointText -Position $position))
        }
        if ($pointTexts[0] -cne $pointTexts[$pointTexts.Count - 1]) {
            throw 'Geometry ring must be closed.'
        }
        $ringTexts.Add('(' + ($pointTexts -join ', ') + ')')
    }
    return '(' + ($ringTexts -join ', ') + ')'
}

try {
    if (-not $SqlGeometryAssemblyPath) {
        $assemblyRoot = Join-Path $env:WINDIR 'Microsoft.NET\assembly\GAC_MSIL\Microsoft.SqlServer.Types'
        $assembly = Get-ChildItem -LiteralPath $assemblyRoot -Filter Microsoft.SqlServer.Types.dll -File -Recurse |
            Sort-Object FullName -Descending | Select-Object -First 1
        if ($null -eq $assembly) { throw 'Existing spatial assembly was not found.' }
        $SqlGeometryAssemblyPath = $assembly.FullName
    }
    Add-Type -Path $SqlGeometryAssemblyPath
    $file = Get-Item -LiteralPath $GeoJsonPath
    if ($file.PSIsContainer -or $file.Length -gt 32MB) { throw 'Geometry input size is invalid.' }
    $bytes = [IO.File]::ReadAllBytes($file.FullName)
    $document = ([Text.UTF8Encoding]::new($false, $true)).GetString($bytes) | ConvertFrom-Json
    if ($document.type -ne 'FeatureCollection' -or $document.features -isnot [array] -or $document.features.Count -ne $ExpectedFeatureCount) {
        throw 'Geometry feature collection/count is invalid.'
    }
    $crsProperty = $document.PSObject.Properties['crs']
    if ($null -ne $crsProperty -and $null -ne $crsProperty.Value) {
        $allowedCrs = @('EPSG:4326', 'urn:ogc:def:crs:EPSG::4326', 'urn:ogc:def:crs:OGC:1.3:CRS84', 'urn:ogc:def:crs:OGC::CRS84', 'OGC:CRS84')
        if ($document.crs.type -ne 'name' -or $document.crs.properties.name -cnotin $allowedCrs) {
            throw 'Geometry must declare WGS84 or omit CRS under RFC 7946.'
        }
    }
    $ids = [System.Collections.Generic.HashSet[int]]::new()
    $invalid = [System.Collections.Generic.List[object]]::new()
    $polygonCount = 0
    $holeCount = 0
    foreach ($feature in $document.features) {
        if ($feature.type -ne 'Feature') { throw 'Geometry feature is invalid.' }
        $idProperty = @('LocationID', 'location_id', 'locationid') |
            Where-Object { $null -ne $feature.properties.PSObject.Properties[$_] } | Select-Object -First 1
        if (-not $idProperty) { throw 'Geometry LocationID is missing.' }
        $rawId = [string]$feature.properties.$idProperty
        if ($rawId -cnotmatch '^[0-9]{1,3}$') { throw 'Geometry LocationID is invalid.' }
        $locationId = [int]$rawId
        if ($locationId -lt 1 -or $locationId -gt 263 -or -not $ids.Add($locationId)) {
            throw 'Geometry LocationID is outside 1..263 or duplicated.'
        }
        $geometry = $feature.geometry
        if ($geometry.type -eq 'Polygon') {
            $wkt = 'POLYGON ' + (ConvertTo-GeometryPolygonText -Rings $geometry.coordinates)
            $polygonCount++
            $holeCount += $geometry.coordinates.Count - 1
        } elseif ($geometry.type -eq 'MultiPolygon') {
            if ($geometry.coordinates -isnot [array] -or $geometry.coordinates.Count -lt 1) {
                throw 'Geometry MultiPolygon is empty.'
            }
            $polygonTexts = [System.Collections.Generic.List[string]]::new()
            foreach ($polygon in $geometry.coordinates) {
                $polygonTexts.Add((ConvertTo-GeometryPolygonText -Rings $polygon))
                $polygonCount++
                $holeCount += $polygon.Count - 1
            }
            $wkt = 'MULTIPOLYGON (' + ($polygonTexts -join ', ') + ')'
        } else {
            throw 'Only Polygon and MultiPolygon geometry is supported.'
        }
        try {
            $spatial = [Microsoft.SqlServer.Types.SqlGeometry]::STGeomFromText([System.Data.SqlTypes.SqlChars]::new($wkt), 4326)
            if (-not $spatial.STIsValid().Value) {
                # IsValidDetailed may include coordinates. Retain its public code
                # only, never the original message or WKT in output/errors.
                $detail = $spatial.IsValidDetailed().ToString()
                $reason = 'Invalid polygon topology.'
                if ($detail -match '^([0-9]{5}):') { $reason = 'SQL spatial validation code ' + $Matches[1] + '.' }
                $invalid.Add([pscustomobject]@{ location_id = $locationId; reason = $reason })
            }
        } catch {
            $invalid.Add([pscustomobject]@{ location_id = $locationId; reason = 'Spatial geometry parsing/validation failed.' })
        }
    }
    $summary = [ordered]@{
        schema_version = 1
        geojson_sha256 = (Get-FileHash -LiteralPath $file.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
        feature_count = $document.features.Count
        invalid = @($invalid.ToArray())
        polygon_count = $polygonCount
        hole_count = $holeCount
        validator = [ordered]@{
            name = 'Microsoft.SqlServer.Types.SqlGeometry.STIsValid'
            assembly_version = ([Microsoft.SqlServer.Types.SqlGeometry].Assembly.GetName().Version).ToString()
            assembly_sha256 = (Get-FileHash -LiteralPath $SqlGeometryAssemblyPath -Algorithm SHA256).Hash.ToLowerInvariant()
            coordinates = 'WGS84 longitude/latitude; planar OGC polygon topology'
            repaired_geometry = $false
        }
    }
    [Console]::Out.WriteLine(($summary | ConvertTo-Json -Depth 8))
    if ($invalid.Count -gt 0) { exit 1 }
} catch {
    # Exceptions may embed coordinates/WKT, so do not print their messages.
    [Console]::Out.WriteLine('{"schema_version":1,"error":"Geometry verification could not complete; check input structure, feature count, CRS and existing spatial assembly."}')
    exit 1
}
