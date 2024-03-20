. "$PSScriptRoot\Get-LogicMonitor-Request.ps1"

Function Get-LogicMonitor-HostStatus {
    Param(
        $ReportDate = $null
        , $DeviceId = $null
        
    )


    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    $DownTimeThreshold = 360
    Write-Host -f Yellow "Collecting HostStatus data..."
    $dateOrigin = Get-Date -Date "01/01/1970"
    $ReportStartUnixTime = (New-TimeSpan -Start $dateOrigin -End $ReportDate).TotalSeconds
    $ReportEndUnixTime = (New-TimeSpan -Start $dateOrigin -End $ReportDate.AddMonths(1)).TotalSeconds    

    $ResourcePath = "/device/devices/$($deviceId)/devicedatasources/"
    $queryParams = "?size=1000"
    $datasourcesData = Get-LogicMonitor-Request -ResourcePath $ResourcePath -QueryParams $queryParams
    $datasources = ($datasourcesData.content | convertFrom-Json).data.items
    $DataSourceId = ($datasources | where-object { $_.dataSourceName -eq "HostStatus" }).id


    $ResourcePath = "/device/devices/$($deviceId)/devicedatasources/$($DataSourceId)/instances"
    $instancesData = Get-LogicMonitor-Request -ResourcePath $ResourcePath
    $instances = ($instancesData.content | convertFrom-Json).data.items


    $ResourcePath = "/device/devices/$($deviceId)/devicedatasources/$($DataSourceId)/instances/$($instances.id)/data"
    $queryParams = "?start=$ReportStartUnixTime&end=$ReportEndUnixTime"
    $datas = @()
    do {
        $rawData = Get-LogicMonitor-Request -ResourcePath $ResourcePath -QueryParams $queryParams
        $data = ($rawData.content | convertFrom-Json).data
        $datas += $data
        $queryParams = "?$(($rawdata.content | convertfrom-json).nextPageParams)"
            
    } while (![string]::IsNullOrEmpty(($rawdata.content | convertfrom-json).nextPageParams))
  
    $totalUpTimes = $datas.values | foreach-object { $_[0] }
    $downtimes = $datas.values | Foreach-object { if ($_[1] -ge $DownTimeThreshold) { $_[0] } }

    $Events = @()
    foreach ($dataBatch in $datas) {
        $batchCount = $null
        $batchCount = $databatch.time.Count
        for ($i = 0; $i -lt $batchCount; $i++) {

            #Write-Host $i
            #Write-Host $databatch.time[$i]

            $Event = New-Object -TypeName PSObject -Property @{
                "DateTime" = $dateOrigin.addSeconds($databatch.time[$i] / 1000)
                "Status"   = if ($databatch.values[$i][1] -lt $DownTimeThreshold) { "Up" } else { "Down" }
            }
            $Events += $Event
        }
    }

    $Events = $Events | Sort-Object -Property Datetime

    $DownReports = @()
    $prevousDetectedDownEvent = $null
    foreach ($event in $Events) {
        if ($event.status -eq "Down" -and [string]::IsNullOrEmpty($prevousDetectedDownEvent)) {
            $detectedDownTime = $event.DateTime
            $prevousDetectedDownEvent = $detectedDownTime
        }

        if ($event.status -eq "Up" -and $event.DateTime -ge $detectedDownTime -and ![string]::IsNullOrEmpty($detectedDownTime)) {
            #Write-Host -f Cyan "Initial Up Detect: $($event.DateTime)"
            $DownReport = New-Object -TypeName PSObject -Property @{
                "DownEvent" = $detectedDownTime
                "UpEvent"   = $event.DateTime
                "DownTime"  = "$($(New-TimeSpan -Start $detectedDownTime -End $event.DateTime).Days)d $($(New-TimeSpan -Start $detectedDownTime -End $event.DateTime).Hours)h $($(New-TimeSpan -Start $detectedDownTime -End $event.DateTime).Minutes)m $($(New-TimeSpan -Start $detectedDownTime -End $event.DateTime).Seconds)s"
            }
            $DownReports += $DownReport
            $prevousDetectedDownEvent = $null
            $detectedDownTime = $null
        }

        if ($events[-1].DateTime -eq $event.DateTime -and $event.Status -eq "Down") {
            #Write-Host -f Cyan " Final status is down: $($event.DateTime)"
            $DownReport = New-Object -TypeName PSObject -Property @{
                "DownEvent" = $detectedDownTime
                "UpEvent"   = $event.DateTime
                "DownTime"  = "$($(New-TimeSpan -Start $detectedDownTime -End $event.DateTime).Days)d $($(New-TimeSpan -Start $detectedDownTime -End $event.DateTime).Hours)h $($(New-TimeSpan -Start $detectedDownTime -End $event.DateTime).Minutes)m $($(New-TimeSpan -Start $detectedDownTime -End $event.DateTime).Seconds)s"
            }
            $DownReports += $DownReport
        }
    }
   



   
    $Availability = New-Object -TypeName PSObject -Property @{
        "Availability" = 100 - ($downtimes.count / $totaluptimes.count) * 100
        "DownTimes"    = $DownReports | Foreach-object { "$($_.DownEvent) - $($_.UpEvent) $($_.DownTime)" }
    }

    $stopwatch.Elapsed.TotalSeconds

    Return $Availability

}


