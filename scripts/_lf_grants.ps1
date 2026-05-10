$execRole = "arn:aws:iam::560503716668:role/lakehouse-at-scale-emr-execution-20260510061545521500000001"
$region   = "ap-southeast-1"
$dbs      = @("default", "raw", "staging", "intermediate", "marts")
$utf8NoBom = New-Object System.Text.UTF8Encoding $false

foreach ($db in $dbs) {
    Write-Host "--- grant on db: $db ---"

    $dbResource = @{ Database = @{ Name = $db } } | ConvertTo-Json -Compress
    $tmpDb = New-TemporaryFile
    [System.IO.File]::WriteAllText($tmpDb.FullName, $dbResource, $utf8NoBom)
    aws lakeformation grant-permissions `
        --region $region `
        --principal "DataLakePrincipalIdentifier=$execRole" `
        --resource "file://$($tmpDb.FullName)" `
        --permissions DESCRIBE CREATE_TABLE ALTER DROP
    Remove-Item $tmpDb.FullName -Force

    $tableResource = @{ Table = @{ DatabaseName = $db; TableWildcard = @{} } } | ConvertTo-Json -Compress
    $tmpTbl = New-TemporaryFile
    [System.IO.File]::WriteAllText($tmpTbl.FullName, $tableResource, $utf8NoBom)
    aws lakeformation grant-permissions `
        --region $region `
        --principal "DataLakePrincipalIdentifier=$execRole" `
        --resource "file://$($tmpTbl.FullName)" `
        --permissions SELECT INSERT DELETE DESCRIBE ALTER DROP
    Remove-Item $tmpTbl.FullName -Force
}
