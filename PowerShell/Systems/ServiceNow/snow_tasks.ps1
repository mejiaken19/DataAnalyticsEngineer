Start-Transcript "$PSScriptRoot\..\..\..\Transcripts\$($($MyInvocation.MyCommand.Name).split('.')[0])\$($MyInvocation.MyCommand.Name)_$(get-date -f 'dd-MM-yyyy_hh.mm.ss').txt"

. "$PSScriptRoot\..\Functions\Set-CSA_SNOWDataToLocalSQL.ps1"

$Method = "GET"
   
$entity = "task"
$SQLDatabase = "CSA"
$SQLTable = "[$($SQLDatabase)].[dbo].[$($entity)]"
$PrimaryKey = "sys_id"



$Fields = @("All")
$DateTimeToAESTs = @("sys_created_on","sys_updated_on","closed_at","opened_at")
$Fields += $DateTimeToAESTs

$IndexExemption = "sys_id","comments","description","work_notes","comments_and_work_notes","u_shared_work_notes","close_notes","approval_history" ##Default:sys_id should always be included for Primary key configuration

$LongTextFields = "comments","description","work_notes","comments_and_work_notes","u_shared_work_notes","close_notes","approval_history"

$Indexes = $Fields | where-object {$_ -notin $IndexExemption} | ForEach-Object {$_+".display_value"}
$Indexes += $Fields | where-object {$_ -notin $IndexExemption} | ForEach-Object {$_+".value"}

$fieldsquery = if ($Fields.count -eq 0) {$null} else  {"&sysparm_fields=$($fields -join ",")"} 

$i = 0
$limit = 300

do {

    Write-Host -f Red "offset = $($i)"

    $endpoint = "/api/now/v1/table/$($entity)?sysparm_limit=$limit&sysparm_offset=$i&sysparm_query=sys_updated_onRELATIVEGE@hour@ago@1&sysparm_display_value=all" `
                + "$($fieldsquery)" + "&sysparm_suppress_pagination_header=true"

    $rawresult = Set-CSA_SNOWDataToLocalSQL -Endpoint $endpoint `
        -Method $Method `
        -SQLDatabase $SQLDatabase `
        -SQLTable $SQLTable `
        -PrimaryKey $PrimaryKey `
        -Fields $Fields `
        -Raw $false `
        -DateTimeToAESTs $DateTimeToAESTs `
        -Indexes $Indexes -LongTextFields $LongTextFields

        Start-Sleep -Milliseconds 300
        $i += $limit
    } while ($null -ne $RawResult)


Stop-Transcript