Start-Transcript "$PSScriptRoot\..\..\Transcripts\$($($MyInvocation.MyCommand.Name).split('.')[0])\$($MyInvocation.MyCommand.Name)_$(get-date -f 'dd-MM-yyyy_hh.mm.ss').txt"
Function Set-CreateSQLTable {
    Param (
        $Report,
        $SQLTable
    )

    $params = @{'server' = 'POWERBI_Server'; 'Database' = $SQLTable.split(".")[0].trim("[]") }
    $timeStamp = "{0:yyyy-MM-dd HH:mm:ss.fffffff}" -f (get-date)
    $Errors = @()

    $propertyNameAndType = @()
    foreach ($property in $Report[0].PSObject.Properties) {
        $dataType = $null
        $dataType = switch -Wildcard ($property.TypeNameOfValue) {
            "*System.String*" { "[nvarchar](MAX)" }
            "*System.Int32*" { "[int]" }
            "*System.DateTime*" { "[datetime2](7)" }
            "*System.Double*" { "[float]" }
            default { "[nvarchar](MAX)" }
        }
        $propertyNameAndType += "[$($property.name)] $($dataType)"
    }

    $InsertResults = "CREATE TABLE $($SQLTable) (
        $($($propertyNameAndType) -join ', ')
        ,[DataTimeStamp] [datetime2](7)
        );
        "
     

    Invoke-sqlcmd @params -Query $InsertResults -ErrorAction Stop
    
}

Stop-Transcript