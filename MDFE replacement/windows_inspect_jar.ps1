<#
Windows-friendly JAR inspection script for MDFE

Run on your Windows work machine (PowerShell). This script provides multiple
ways to inspect a Spark JAR: using the `jar` tool (if Java is installed), using
.NET's ZipFile APIs to list or extract files, printing the MANIFEST.MF,
searching for likely config files (`application.conf`, `reference.conf`, `*.properties`),
calculating a SHA-256 checksum and attempting a `--help` probe.

Usage:
  1. Open PowerShell (recommended as Administrator only if you need extraction to protected folders).
  2. Edit the `$jarPath` variable below to point to your downloaded JAR file.
  3. Run: `powershell -ExecutionPolicy Bypass -File .\windows_inspect_jar.ps1`

Notes:
  - Do NOT paste the JAR content here. Run locally and paste only sanitized outputs.
  - The script is defensive: it tries `jar` first, then falls back to .NET Zip APIs.
  - Replace placeholder blob paths or filenames before running commands that upload or share outputs.
#>

param(
    [string]$jarPath = "C:\path\to\mfde.jar",
    [string]$extractDir = "$env:TEMP\mfde_jarextract",
    [switch]$DoHelpProbe
)

Write-Host "Inspecting JAR at: $jarPath`n" -ForegroundColor Cyan

if (-not (Test-Path $jarPath)) {
    Write-Host "ERROR: JAR not found at path: $jarPath" -ForegroundColor Red
    exit 2
}

# 1) SHA-256 checksum
Write-Host "SHA-256 checksum:" -ForegroundColor Green
Get-FileHash -Algorithm SHA256 -Path $jarPath | Format-List


# 2) Try Java 'jar' tool listing if available
try {
    $java = Get-Command jar -ErrorAction SilentlyContinue
    if ($null -ne $java) {
        Write-Host "\nUsing 'jar' command to list top-level contents (first 200 lines):" -ForegroundColor Green
        & jar tf $jarPath | Select-Object -First 200 | ForEach-Object { Write-Output $_ }
    }
    else {
        Write-Host "\n'jar' command not found in PATH. Falling back to .NET Zip listing." -ForegroundColor Yellow
    }
}
catch {
    Write-Host "Warning: couldn't run 'jar' command: $_" -ForegroundColor Yellow
}


# 3) List entries using .NET ZipFile (safe fallback)
Add-Type -AssemblyName System.IO.Compression.FileSystem
[System.IO.Path]::GetFullPath($jarPath) | Out-Null
Write-Host "\nListing JAR entries (first 300):" -ForegroundColor Green
[System.IO.Compression.ZipFile]::OpenRead($jarPath).Entries | Select-Object -First 300 | ForEach-Object { Write-Output $_.FullName }


# 4) Print MANIFEST.MF if present
Write-Host "\nMANIFEST.MF (if present):" -ForegroundColor Green
$zip = [System.IO.Compression.ZipFile]::OpenRead($jarPath)
$manifest = $zip.Entries | Where-Object { $_.FullName -match 'META-INF/MANIFEST.MF' }
if ($manifest) {
    $r = $manifest.Open()
    $sr = New-Object System.IO.StreamReader($r)
    $content = $sr.ReadToEnd()
    $sr.Close(); $r.Close()
    $content -split "\r?\n" | Select-Object -First 200 | ForEach-Object { Write-Output $_ }
} else {
    Write-Host "  No MANIFEST.MF found inside JAR." -ForegroundColor Yellow
}


# 5) Search for config files (application.conf, reference.conf, *.properties, log4j*) and print a snippet
Write-Host "\nLooking for likely config files (application.conf, reference.conf, *.properties, log4j*):" -ForegroundColor Green
$candidates = $zip.Entries | Where-Object { $_.FullName -match '(application\.conf|reference\.conf|\.properties$|log4j|log4j2)' }
if ($candidates.Count -eq 0) {
    Write-Host "  No obvious config files found." -ForegroundColor Yellow
} else {
    foreach ($e in $candidates) {
        Write-Host "--- $($e.FullName) ---" -ForegroundColor Cyan
        try {
            $r = $e.Open(); $sr = New-Object System.IO.StreamReader($r)
            $sr.ReadToEnd().Split("`n") | Select-Object -First 200 | ForEach-Object { Write-Output $_ }
            $sr.Close(); $r.Close()
        } catch {
            Write-Host "  (binary or unreadable)" -ForegroundColor Yellow
        }
    }
}


# 6) Quick textual scan across text entries for 'spark.' or 'jdbc' hints
Write-Host "\nScanning text resources for 'spark.' or 'jdbc' or 'sqlserver' occurrences (first 200 matches):" -ForegroundColor Green
$matches = @()
foreach ($entry in $zip.Entries) {
    if ($entry.Length -lt 5MB -and $entry.FullName -match '\.(conf|properties|xml|txt|sql)$') {
        try {
            $r = $entry.Open(); $sr = New-Object System.IO.StreamReader($r)
            $text = $sr.ReadToEnd()
            $sr.Close(); $r.Close()
            if ($text -match '(?i)spark\.|jdbc|sqlserver|wasb|wasbs|account|connectionString') {
                $excerpt = ($text -split "\r?\n") | Select-Object -First 20
                $matches += @{file=$entry.FullName; excerpt=$excerpt}
            }
        } catch { }
    }
}
if ($matches.Count -gt 0) {
    foreach ($m in $matches) {
        Write-Host "--- $($m.file) ---" -ForegroundColor Cyan
        $m.excerpt | ForEach-Object { Write-Output $_ }
    }
} else {
    Write-Host "  No quick textual hints found inside small text files." -ForegroundColor Yellow
}


# 7) Optional: extract JAR to a temp folder for deeper inspection
Write-Host "\nExtracting JAR to: $extractDir (first 100 files listed)" -ForegroundColor Green
if (Test-Path $extractDir) { Remove-Item -Recurse -Force $extractDir }
[System.IO.Compression.ZipFile]::ExtractToDirectory($jarPath, $extractDir)
Get-ChildItem -Path $extractDir -Recurse | Select-Object FullName -First 100 | ForEach-Object { Write-Output $_.FullName }


# 8) Optional: attempt to run `java -jar <jar> --help` to get CLI usage (do NOT share binary output; paste sanitized text)
if ($DoHelpProbe) {
    Write-Host "\nAttempting 'java -jar <jar> --help' probe (may run code) -- use with care" -ForegroundColor Yellow
    try {
        & java -jar $jarPath --help 2>&1 | Select-Object -First 200 | ForEach-Object { Write-Output $_ }
    } catch {
        Write-Host "  Could not run java -jar. Ensure Java is installed and in PATH, or skip this step." -ForegroundColor Yellow
    }
}

Write-Host "\nDone. Review outputs above. Copy/paste any sanitized snippets you want me to analyze." -ForegroundColor Cyan
