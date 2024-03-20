. "$PSScriptRoot\Get-LogicMonitor-Request.ps1"

Function Get-LogicMonitor-Bandwidth {
    Param(
        $ReportDate = $null
        , $DeviceId = $null

    )

    Write-Host -f Yellow "Collecting Bandwidth data..."
    $dateOrigin = Get-Date -Date "01/01/1970"
    $ReportStartUnixTime = (New-TimeSpan -Start $dateOrigin -End $ReportDate).TotalSeconds
    $ReportEndUnixTime = (New-TimeSpan -Start $dateOrigin -End $ReportDate.AddMonths(1)).TotalSeconds    

    $ResourcePath = "/device/devices/$($deviceId)/devicedatasources/"
    $queryParams = "?size=1000"
    $datasourcesData = Get-LogicMonitor-Request -ResourcePath $ResourcePath -QueryParams $queryParams
    $datasources = ($datasourcesData.content | convertFrom-Json).data.items

    $is64Bit = $true
  
    $DataSourceId = ($datasources | where-object { $_.dataSourceName -eq "snmp64_If-" }).id
    $ResourcePath = "/device/devices/$($deviceId)/devicedatasources/$($DataSourceId)/instances"
    $instancesData = Get-LogicMonitor-Request -ResourcePath $ResourcePath
    
    if (($instancesdata.content | convertfrom-json).data.total -eq 0) {
        $is64Bit = $false
    }

    if (!$is64Bit) {
        Write-Host -f Cyan "...non 64bit detected"
        $DataSourceId = ($datasources | where-object { $_.dataSourceName -eq "snmpIf-" }).id
        $ResourcePath = "/device/devices/$($deviceId)/devicedatasources/$($DataSourceId)/instances"
        $instancesData = Get-LogicMonitor-Request -ResourcePath $ResourcePath
    }
   
    if ($null -ne $instancesData) {
        $instances = ($instancesData.content | convertFrom-Json).data.items
        $dataPoints = "InOctets,OutOctets,InUtilizationPercent,OutUtilizationPercent"

        $datas = @()
        foreach ($instance in $instances) {
            Write-Host -f Magenta "...Collecting bandwidth for $($instance.displayName)"
            $ResourcePath = "/device/devices/$($deviceId)/devicedatasources/$($DataSourceId)/instances/$($instance.id)/data"
            $queryParams = "?start=$ReportStartUnixTime&end=$ReportEndUnixTime&datapoints=$($dataPoints)"
        
            do {
                $rawData = Get-LogicMonitor-Request -ResourcePath $ResourcePath -QueryParams $queryParams
                $data = ($rawData.content | convertFrom-Json).data
                $datas += $data
                $queryParams = "?$(($rawdata.content | convertfrom-json).nextPageParams)&datapoints=$($dataPoints)"
            } while (![string]::IsNullOrEmpty(($rawdata.content | convertfrom-json).nextPageParams))
        }

        $TotalOutOctets = ($datas.values | foreach-object { $_[$data.datapoints.indexOf("OutOctets")] | where-object { $_ -notlike "No Data" } } | Measure-Object -Sum).Sum
        $TotalInOctets = ($datas.values | foreach-object { $_[$data.datapoints.indexOf("InOctets")] | where-object { $_ -notlike "No Data" } } | Measure-Object -Sum).Sum
        
        
        $AverageOutUtilizationPercent = ($datas.values | foreach-object { $_[$data.datapoints.indexOf("OutUtilizationPercent")] | where-object { $_ -notlike "No Data" -and $_ -notlike "Infinity" } } | Measure-Object -Average).Average
        $AverageInUtilizationPercent = ($datas.values | foreach-object { $_[$data.datapoints.indexOf("InUtilizationPercent")] | where-object { $_ -notlike "No Data" -and $_ -notlike "Infinity" } } | Measure-Object -Average).Average
    

        $BandwidthData = New-Object -TypeName PSObject -Property @{
            "Transmit_TotalConsumption" = $TotalOutOctets / 1MB
            "Receive_TotalConsumption"  = $TotalInOctets / 1MB
            "Transmit_AvgUtilization"   = $AverageOutUtilizationPercent
            "Receive_AvgUtilization"    = $AverageInUtilizationPercent 
            "Ports_Count"               = $instances.count
        }

        Return $BandwidthData

    }
}


