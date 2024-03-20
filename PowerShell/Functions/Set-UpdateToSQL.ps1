. "$PSScriptRoot\..\ErrorHandlers\send-mailalerts.ps1"

Function Set-UpdateToSQL ($Report, $SQLTable, $PrimaryKey, $EmailAlerts = $true) {

    #$sqlServer = '.'
    $params = @{'server' = 'POWERBI_Server'; 'Database' = $SQLTable.split(".")[0].trim("[]") }
    $TimeStamp = "{0:yyyy-MM-dd HH:mm:ss.fffffff}" -f (get-date)
    $Errors = @()

    
    foreach ($object in $Report) { 
        $propertyNamesNoBracket = @()
        $propertyNames = @()
        $propertyValues = @()

        foreach ($properties in $object.PSObject.Properties) {
            $propertyNamesNoBracket += $($properties.name)
            $propertyNames += "[$($properties.name)]"
        }

        foreach ($property in $propertyNamesNoBracket) {

            if ([string]::IsNullOrEmpty("$($object."$($property)")")) {
                $propertyValues += "NULL"
            }
            if (![string]::IsNullOrEmpty("$($object."$($property)")")) {

                $propertyRawValues = $null
                $propertyRawValues += "$($object."$($property)")" -replace "'", "''"
                $propertyValues += "`'$($propertyRawValues)`'"
            }
        }

        $propertyNameValue = @()
        for ($i = 0; $i -lt $propertyNames.count; $i++) {
            $propertyNameValue += "$($propertyNames[$i]) = $($propertyValues[$i])"
        }

        
        $UpdateResults = "
        Update $($SQLTable)
        Set
        $($propertyNameValue -join ",")
        ,[DataTimeStamp] = `'$($Timestamp)`'
        WHERE $($propertynames | Where-object {$_.contains($($PrimaryKey)) `
            -and $_ -ne "[Resource_Group_No]" `
            -and $_ -ne "[orig_sys_id]" `
            -and $_ -ne "[Bill_to_Customer_No]" `
        }) = `'$($object."$($PrimaryKey)")`'
        
        "

        Try { 
            Invoke-sqlcmd @params -Query $UpdateResults -ConnectionTimeout 65534 -QueryTimeout 65535 -DisableVariables -ErrorAction Stop
        }
        catch {
            if ($_.Exception.Message.contains("PRIMARY KEY")) {
                $Errors += $_.Exception.Message
            }
            if ($_.Exception.Message.contains("String or binary data would be truncated")) {
                "Truncated: $($UpdateResults)" >> "$PSScriptRoot\..\..\Logs\SQL_Errors_Reports\Errors.txt"
                $Errors += "Truncated: $($UpdateResults)"
            }
            else {
                $_.Exception.Message >> "$PSScriptRoot\..\..\Logs\SQL_Errors_Reports\Errors.txt"
                $Errors = $_.Exception 
            }
        }    
    }
    
    
    if (!([string]::IsNullOrEmpty($Errors)) -and $EmailAlerts -eq $true) {
        Send-MailAlert -Subject ("$($MyInvocation.MyCommand.Name) : $($SQLTable)") -Body $Errors
    }
    else {
        Write-Host "email not sent"
    }
}

