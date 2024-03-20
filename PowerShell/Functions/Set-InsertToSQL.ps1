. "$PSScriptRoot\..\ErrorHandlers\send-mailalerts.ps1"

Function Set-InsertToSQL {
    Param(
        $Report, 
        $SQLTable, 
        $EmailAlerts = $true
    )

    #$sqlServer = '.'
    $params = @{'server' = 'POWERBI_SERVER'; 'Database' = $SQLTable.split(".")[0].trim("[]") }
    $TimeStamp = "{0:yyyy-MM-dd HH:mm:ss.fffffff}" -f (get-date)
    $Errors = @()
    foreach ($object in $Report) { 
        $propertyNamesNoBracket = @()
        $propertyNames = @()
        $propertyValues = @()

        #Write-Host -f Yellow $object.Date
        foreach ($properties in $object.PSObject.Properties) {
            if ($ExcludedFields -notcontains $properties.name ) {
                $propertyNamesNoBracket += $($properties.name)
                $propertyNames += "[$($properties.name)]"
            }
        }

        foreach ($property in $propertyNamesNoBracket) {
            if ([string]::IsNullOrEmpty("$($object."$($property)")")) {
                $propertyValues += "NULL"
            }
            if (![string]::IsNullOrEmpty("$($object."$($property)")")) {

                $propertyRawValues = $null
                $propertyRawValues += "$($object."$($property)")" -replace "'", "''"
                $propertyValues += "`'$($propertyRawValues)`'"
                
                if ($propertyRawValues.contains("$")) {
                }
                
            }
        }

        $InsertResults = "
        INSERT INTO $($SQLTable)
        (
            $($propertyNames -join ",")
            ,[DataTimeStamp]
        )
        Values
        (
            $($propertyValues -join ",")
            ,'$($TimeStamp)'
        )
        "
        Try { 
            Invoke-sqlcmd @params -Query $InsertResults -ConnectionTimeout 65534 -QueryTimeout 65535 -DisableVariables -ErrorAction Stop
        }
        catch {
            if ($_.Exception.Message.contains("PRIMARY KEY constraint")) {
                $Errors += "<html> <br> $($object.site) is already in for $($object.ReportMonthYear) </html>"
                $inBetween = "The duplicate key value is .*"
                $parsedItem = [regex]::Match($_.Exception.Message, $inBetween).Groups[0].Value
                $parsedItem >> "$PSScriptRoot\..\..\Logs\SQL_Errors_Reports\Errors.txt"
            }
            if ($_.Exception.Message.contains("String or binary data would be truncated")) {
                "Truncated: $($InsertResults)" >> "$PSScriptRoot\..\..\Logs\SQL_Errors_Reports\Errors.txt"
                $Errors += "Truncated: $($InsertResults)"
            }
            else {
                $_.Exception.Message >> "$PSScriptRoot\..\..\Logs\SQL_Errors_Reports\Errors.txt"
                $Errors += $_.Exception.Message 
            }
        }
    }
    
    if ($EmailAlerts) {
        if (!([string]::IsNullOrEmpty($Errors))) {
            Send-MailAlert -Subject ("$($MyInvocation.MyCommand.Name) : $($SQLTable)") -Body $Errors
        }
    }
    else {
        Write-Host -f Yellow "Email not sent" 
    }
}

