Start-Transcript "$PSScriptRoot\..\..\..\Transcripts\$($($MyInvocation.MyCommand.Name).split('.')[0])\$($MyInvocation.MyCommand.Name)_$(get-date -f 'dd-MM-yyyy_hh.mm.ss').txt"

. "$PSScriptRoot\..\..\Functions\Get-LogicMonitor-Request.ps1"
. "$PSScriptRoot\..\..\Functions\Get-LogicMonitor-HostStatus.ps1"
. "$PSScriptRoot\..\..\Functions\Get-LogicMonitor-Ping.ps1"
. "$PSScriptRoot\..\..\Functions\Get-LogicMonitor-Alerts.ps1"
. "$PSScriptRoot\..\..\Functions\Get-LogicMonitor-Bandwidth.ps1"
. "$PSScriptRoot\..\..\Functions\Set-CreateSQLTable.ps1"
. "$PSScriptRoot\..\..\Functions\Set-InsertToSQL.ps1"
. "$PSScriptRoot\..\..\Functions\Set-UpdateToSQL.ps1"

$stopwatch = [System.Diagnostics.Stopwatch]::StartNew()


$CustomerName = "Customer"
$ServiceNowId = "CUS999"

$SQLTable = "[LogicMonitor].[dbo].$($CustomerName)_Network]"
$PrimaryKey

$MonthAgo = -1
$ReportDate = (Get-Date -Day 1).Date.addMonths($MonthAgo)

$devices = @()
$deviceResponse = $null
$deviceOffset = 0
$deviceSize = 1000
$deviceResourcePath = "/device/devices"

$deviceFilter = "inheritedProperties.name:cmdb.customer_code,inheritedProperties.value:$($ServiceNowId)
                ,systemProperties.name:system.staticgroups,systemProperties.value:Customers/Nexon Clients/Retail Apparel Group/Network/WAN|Customers/Nexon Clients/Retail Apparel Group/Network/Stores"
               

$deviceFields = "id,name,displayName,systemProperties"


do {
    $deviceQueryParams = "?size=$($deviceSize)&offset=$($deviceOffset)&filter=$($deviceFilter)&fields=$($deviceFields)"
    $deviceResponse = Get-LogicMonitor-Request -ResourcePath $deviceResourcePath -QueryParams $deviceQueryParams
    $devices += ($deviceResponse.content | convertfrom-json).data.items
    $deviceOffset += $deviceSize

} while (($deviceResponse.content | ConvertFrom-json).data.items -ne "{}")


$counter = 0
$Reports = @()
foreach ($device in $devices) {
    Write-Host -f Yellow $device.id
    $counter = $counter + 1

    Write-Host "$($counter) / $($devices.count)"

    Write-Host -f Cyan $device.displayName

    $Availability = $Null 
    $PingData = $Null
    $Availability = Get-LogicMonitor-HostStatus -ReportDate $ReportDate -DeviceId $device.id
    $PingData = Get-LogicMonitor-Ping -ReportDate $ReportDate -DeviceId $device.id
    $AlertsCount = Get-LogicMonitor-Alerts -ReportDate $ReportDate -DeviceId $device.id
    $BandwidthData = Get-LogicMonitor-Bandwidth -ReportDate $ReportDate -DeviceId $device.id

    Write-Host -f Yellow "Restructuring data..."
    $Report = New-Object -TypeName PSObject -Property @{
        "ReportId"                         = "$(Get-date $ReportDate -Format "dd_MM_yyyy")_$($device.name)"
        "CustomerName"                     = $CustomerName
        "ReportDate"                       = $ReportDate
        "Availability"                     = [nullable [Double]]$availability.availability
        "AveragePacketLoss"                = [Double]$PingData.AveragePacketLoss 
        "AverageLatency"                   = [Double]$PingData.AverageLatency
        "Alerts"                           = [Double]$AlertsCount
        "Bandwidth Consumption (Transmit)" = [Double]$BandwidthData.Transmit_TotalConsumption
        "Bandwidth Consumption (Receive)"  = [Double]$BandwidthData.Receive_TotalConsumption
        "Bandwidth Utilization (Transmit)" = [Double]$BandwidthData.Transmit_AvgUtilization
        "Bandwidth Utilization (Receive)"  = [Double]$BandwidthData.Receive_AvgUtilization
        "Name"                             = $device.name
        "DisplayName"                      = $device.displayName             
    }
    $Reports += $Report
}


Try {
    Write-host -f Cyan "Creating Database..."
    Set-CreateSQLTable -Report $Reports -SQLTable $SQLTable -ErrorAction Stop
    
}
catch { Write-Host -f Yellow "Database already exist" }

Set-InsertToSQL -Report $Reports -SQLTable $SQLTable

Write-Host -f Cyan "updating data...."
Set-UpdateToSQL -report $Reports -sqltable $SQLTable -PrimaryKey $PrimaryKey -EmailAlerts $false

$stopwatch.Elapsed.TotalSeconds

Stop-Transcript