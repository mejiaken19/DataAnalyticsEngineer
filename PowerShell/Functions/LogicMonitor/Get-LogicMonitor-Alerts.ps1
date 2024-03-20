. "$PSScriptRoot\Get-LogicMonitor-Request.ps1"

Function Get-LogicMonitor-Alerts {
    Param(
        $ReportDate = $null
        , $DeviceId = $null
        
    )

    Write-Host -f Yellow "Collecting Alerts data..."

    $dateOrigin = Get-Date -Date "01/01/1970"
    $ReportStartUnixTime = (New-TimeSpan -Start $dateOrigin -End $ReportDate).TotalSeconds
    $ReportEndUnixTime = (New-TimeSpan -Start $dateOrigin -End $ReportDate.AddMonths(1)).TotalSeconds
    #$ReportEndUnixTime = (New-TimeSpan -Start $dateOrigin -End $ReportDate.Addhours(1)).TotalSeconds
    
    Write-Host -f Yellow "Collecting device alerts..."
    $ResourcePath = "/device/devices/$($deviceId)/alerts"
    $queryParams = "?start=$ReportStartUnixTime&end=$ReportEndUnixTime&fields=id"
    $alertsData = Get-LogicMonitor-Request -ResourcePath $ResourcePath -QueryParams $queryParams
    
    $alertsCount = ($alertsData.content | convertfrom-json).data.total

    Return  $alertsCount
}