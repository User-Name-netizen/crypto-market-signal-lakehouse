$ErrorActionPreference = "Stop"

# Run from anywhere: script changes to project root based on its own location.
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Resolve-Path (Join-Path $ScriptDir "..")
Set-Location $ProjectRoot

$containerName = "lakehouse-trino"
$catalog = "delta"

$queries = @(
    "CREATE SCHEMA IF NOT EXISTS delta.gold WITH (location = 's3://lakehouse/gold/')",
    @"
CALL delta.system.register_table(
  schema_name => 'gold',
  table_name => 'ohlc_1min',
  table_location => 's3://lakehouse/gold/OHLC_1Min'
)
"@,
    @"
CALL delta.system.register_table(
  schema_name => 'gold',
  table_name => 'whale_alert',
  table_location => 's3://lakehouse/gold/Whale_Alert'
)
"@,
    @"
CALL delta.system.register_table(
  schema_name => 'gold',
  table_name => 'maker_taker_flow_1min',
  table_location => 's3://lakehouse/gold/maker_taker_flow_1min'
)
"@,
    @"
CALL delta.system.register_table(
  schema_name => 'gold',
  table_name => 'vwap_1min',
  table_location => 's3://lakehouse/gold/VWAP_1Min'
)
"@,
    "SHOW TABLES FROM delta.gold",
    @"
SELECT symbol, candle_time, close_price
FROM delta.gold.ohlc_1min
ORDER BY candle_time DESC
LIMIT 20
"@
)

Write-Host "== Checking Trino container =="
$runningContainers = docker ps --format "{{.Names}}"
if ($LASTEXITCODE -ne 0 -or -not ($runningContainers -contains $containerName)) {
    throw "Container '$containerName' is not running. Start docker compose first."
}

for ($i = 0; $i -lt $queries.Count; $i++) {
    $step = $i + 1
    Write-Host "`n== Step $step/$($queries.Count) =="
    Write-Host $queries[$i]

    docker exec $containerName trino --catalog $catalog --execute $queries[$i]
    if ($LASTEXITCODE -ne 0) {
        throw "Step $step failed with exit code $LASTEXITCODE"
    }
}

Write-Host "`nDone: schema created/verified, tables registered, and sample query executed."
