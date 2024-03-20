. "$PSScriptRoot\Get-LogicMonitor-Request.ps1"

Function Get-LogicMonitor-Ping {
    Param(
        $ReportDate = $null
        , $DeviceId = $null
        
    )

    Write-Host -f Yellow "Collecting Ping data..."

    $dateOrigin = Get-Date -Date "01/01/1970"
    $ReportStartUnixTime = (New-TimeSpan -Start $dateOrigin -End $ReportDate).TotalSeconds
    $ReportEndUnixTime = (New-TimeSpan -Start $dateOrigin -End $ReportDate.AddMonths(1)).TotalSeconds    
    Write-Host -f Yellow "Collecting datasources..."
    $ResourcePath = "/device/devices/$($deviceId)/devicedatasources/"
    $queryParams = "?size=1000"
    $datasourcesData = Get-LogicMonitor-Request -ResourcePath $ResourcePath -QueryParams $queryParams
    $datasources = ($datasourcesData.content | convertFrom-Json).data.items
    $DataSourceId = ($datasources | where-object { $_.dataSourceName -eq "Ping" }).id

    Write-Host -f Yellow "Collecting instances..."
    $ResourcePath = "/device/devices/$($deviceId)/devicedatasources/$($DataSourceId)/instances"
    $instancesData = Get-LogicMonitor-Request -ResourcePath $ResourcePath
    $instances = ($instancesData.content | convertFrom-Json).data.items

    Write-Host -f Yellow "...Collecting packetloss and latency data..."
    $dataPoints = "average,PingLossPercent"
    $ResourcePath = "/device/devices/$($deviceId)/devicedatasources/$($DataSourceId)/instances/$($instances.id)/data"
    $queryParams = "?start=$ReportStartUnixTime&end=$ReportEndUnixTime&datapoints=$($dataPoints)"
    $datas = @()
    do {
        $rawData = Get-LogicMonitor-Request -ResourcePath $ResourcePath -QueryParams $queryParams
        $data = ($rawData.content | convertFrom-Json).data
        $datas += $data
        $queryParams = "?$(($rawdata.content | convertfrom-json).nextPageParams)&datapoints=$($dataPoints)"
    } while (![string]::IsNullOrEmpty(($rawdata.content | convertfrom-json).nextPageParams))

    $AveragePacketLoss = ($datas.values | foreach-object { $_[$data.datapoints.indexOf("PingLossPercent")] | where-object { $_ -notlike "No Data" } } | Measure-Object -Average).Average
    $AverageLatency = ($datas.values | foreach-object { $_[$data.datapoints.indexOf("average")] | where-object { $_ -notlike "No Data" } } | Measure-Object -Average).Average
    
    $PingData = New-Object -TypeName PSObject -Property @{
        "AveragePacketLoss" = $AveragePacketLoss
        "AverageLatency"    = $AverageLatency
    }
    Return  $PingData
}

