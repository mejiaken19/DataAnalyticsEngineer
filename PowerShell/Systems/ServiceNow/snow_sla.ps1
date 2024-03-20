Start-Transcript "$PSScriptRoot\..\..\..\Transcripts\$($($MyInvocation.MyCommand.Name).split('.')[0])\$($MyInvocation.MyCommand.Name)_$(get-date -f 'dd-MM-yyyy_hh.mm.ss').txt"

. "$PSScriptRoot\..\..\Functions\Set-CSA_SNOWDataToLocalSQL.ps1"

$Method = "GET"
   
$entity = "task_sla"
$SQLDatabase = "CSA"
$SQLTable = "[$($SQLDatabase)].[dbo].[$($entity)]"
$PrimaryKey = "sys_id"


$Fields = @("All")

$DateTimeToAESTs = @("sys_created_on", "sys_updated_on", "planned_end_time")
$Fields += $DateTimeToAESTs

$IndexExemption = "sys_id" 

$Indexes = $Fields | where-object { $_ -notin $IndexExemption } | ForEach-Object { $_ + ".display_value" }
$Indexes += $Fields | where-object { $_ -notin $IndexExemption } | ForEach-Object { $_ + ".value" }

$fieldsquery = if ($Fields.count -eq 0) { $null } else { "&sysparm_fields=$($fields -join ",")" } 

$i = 0
$limit = 1000

do {

    Write-Host -f Red "offset = $($i)"

    $endpoint = "/api/now/v1/table/$($entity)?sysparm_limit=$limit&sysparm_offset=$i&sysparm_query=stage=in_progress^ORsys_updated_onRELATIVEGE@hour@ago@2&sysparm_display_value=all" `
        + "$($fieldsquery)"

    $rawresult = Set-CSA_SNOWDataToLocalSQL -Endpoint $endpoint `
        -Method $Method `
        -SQLDatabase $SQLDatabase `
        -SQLTable $SQLTable `
        -PrimaryKey $PrimaryKey `
        -Fields $Fields `
        -Raw $false `
        -DateTimeToAESTs $DateTimeToAESTs `
        -Indexes $Indexes

    Start-Sleep -Milliseconds 300
    $i += $limit
} while ($null -ne $RawResult)




Stop-Transcript