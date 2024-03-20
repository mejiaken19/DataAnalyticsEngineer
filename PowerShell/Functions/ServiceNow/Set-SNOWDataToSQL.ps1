
. "$PSScriptRoot\Set-CreateSQLTableWithIndex.ps1"
. "$PSScriptRoot\Get-QueryLocalDataWarehouse.ps1"
. "$PSScriptRoot\Get-RetryCommand.ps1"
. "$PSScriptRoot\Set-SnowToSQL.ps1"

Function Set-CSA_SNOWDataToLocalSQL () {
    Param(
        $domain = "https://csa.service-now.com"
        , $endpoint
        , $Method
        , $SQLDatabase
        , $SQLTable
        , $PrimaryKey
        , $Fields
        , $DisplayValues
        , $Test = $false
        , $ParseDateFields
        , $Raw = $true
        , $DateTimeToAESTs = @()
        , $Indexes
        , $DateTypeToDate
        , $LongTextFields
    )

    $uri = "$($domain)/$($endpoint)"

    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    $user = "operations.bi"
    $PlainPassword = Get-Content "E:\POWER_BI_SERVER\vault\SNOW_APItxt"
    $SecurePassword = ConvertTo-SecureString $PlainPassword
    $BSTR = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($SecurePassword)
    $pass = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($BSTR)
    
    $base64AuthInfo = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes(("{0}:{1}" -f $user, $pass)))
    $headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
    $headers.Add('Authorization', ('Basic {0}' -f $base64AuthInfo))
    $headers.Add('Accept', 'application/json')
 
    $utcTZ = [System.TimeZoneInfo]::GetSystemTimeZones() | where-object { $_.id -eq "UTC" }
    $aestTZ = [System.TimeZoneInfo]::GetSystemTimeZones() | where-object { $_.id -eq "AUS Eastern Standard Time" }


    $Stoploop = $false
    Do {
        try {
            Write-Host -f Yellow "Working?"
            $response = Invoke-Webrequest -Headers $headers -Method $method -Uri $uri -EA Stop
            $Stoploop = $true
            $responseError = $null

            Write-Host -f Green "Up to Here?"
        }
        catch {
            if ($_.Exception.Message -eq "The remote server returned an error: (404) Not Found.") {
                Write-Host -f Green "No more data"
                Write-Host -f Green $_.Exception.Message
                $responseError = "No Data"
                $Stoploop = $true
            }
            else {
                Write-Host -f Magenta "request failed, retrying..."
                Write-Host -f Magenta "$($_)"
                $responseError = "Error"
                $_.Exception.Message
                Start-Sleep -Seconds 5
            }
        }
    }
    While ($Stoploop -eq $false)


    
    if ($null -eq $responseError) {
        $report = $null
        if ($null -ne ($response.content | ConvertFrom-Json).result) {
            Write-Host -f Green "Data collection done."
        
            $reports = ($response.content | ConvertFrom-Json).result
        
            Write-Host  "Adding fields..."
    

            $normals = ($reports | Get-Member | Where-object { $_.MemberType -eq "NoteProperty" }).Name
  

            foreach ($normal in $normals) {
                $reports | Add-Member -NotePropertyName "$($normal).value" -NotePropertyValue $null
                $reports | Add-Member -NotePropertyName "$($normal).display_value" -NotePropertyValue $null
            }
            
            foreach ($ParseDateField in $ParseDateFields) {
                $reports | Add-Member -NotePropertyName "$($ParseDateField).date" -NotePropertyValue $null
            }

            foreach ($DateTimeToAEST in $DateTimeToAESTs) {
                $reports | Add-Member -NotePropertyName "$($DateTimeToAEST)_AEST" -NotePropertyValue $null
            }
    
            foreach ($report in $reports) {
                foreach ($ParseDateField in $ParseDateFields) {
                    $report."$($ParseDateField).date" = ([datetime]::ParseExact($report.$ParseDateField.display_value, 'dd-MM-yyyy HH:mm:ss', $null)).date
                }

                foreach ($DateTimeToAEST in $DateTimeToAESTs) {
                    $report."$($DateTimeToAEST)_AEST" = if (![string]::IsNullOrEmpty($report.$DateTimeToAEST.value)) { [System.TimeZoneInfo]::ConvertTime(([datetime]::ParseExact($report.$DateTimeToAEST.value, 'yyyy-MM-dd HH:mm:ss', $null)), $utcTZ, $aestTZ) } else { $null }
                }

                foreach ($normal in $normals) {
                    $report."$($normal).value" = if ($report.$normal.value.length -ge 190 -and ($normal -notin $LongTextFields)) { $report.$normal.value.substring(0, 190) } else { if ($null -ne $report.$normal.value) { $report.$normal.value.substring(0, [System.Math]::Min(3000, $report.$normal.value.length)) } }
                    $report."$($normal).value" = if ($report."$($normal).value" -cmatch '[^\x20-\x7F]') { $report."$($normal).value" -replace '[^\x20-\x7F]' } else { $report."$($normal).value" }
                
                }

                foreach ($normal in $normals) {
                    $report."$($normal).display_value" = if ($report.$normal.display_value.length -ge 190 -and ($normal -notin $LongTextFields)) { $report.$normal.display_value.substring(0, 190) } else { if ($null -ne $report.$normal.display_value) { $report.$normal.display_value.substring(0, [System.Math]::Min(3000, $report.$normal.display_value.length)) } }
                    $report."$($normal).display_value" = if ($report."$($normal).display_value" -cmatch '[^\x20-\x7F]') { $report."$($normal).display_value" -replace '[^\x20-\x7F]' } else { $report."$($normal).display_value" }
                }

                $report.sys_id = $Report.sys_id.value
            }


            $ValidFields = ($Reports | Get-Member `
                | Where-object { $_.MemberType -eq "NoteProperty" `
                        -and ($_.Name -like "*display_value" -or $_.Name -like "*.value" -or $_.Name -like "*_AEST") `
                        -and ($_.Name -ne "sys_id.display_value") `
                        -and ($_.Name -ne "sys_id.value") } `
            ).Name


            $ValidFields += @("sys_id")

            $Reports = $Reports | Select-Object -Property $ValidFields


            if (!$test -eq $true) {
                Try {
                    Write-host -f Cyan "Creating Database..."
                    Set-CreateSQLTableWithIndex -Report $reports -SQLTable $SQLTable -PrimaryKeys $PrimaryKey -Indexes $Indexes -EA Stop

                }
                catch { Write-Host -f Yellow "Database already exist" }
            
                Set-SnowToSQL -Report $reports -SQLTable $SQLTable -SQLDatabase $SQLDatabase -PrimaryKey $PrimaryKey
            }
        }
    }

    if ($null -ne $response) {
        if ($Raw) {
            Return ($response.content | ConvertFrom-Json).result
        }
        if (!$Raw) {
            Return $reports
        }

    }

}


