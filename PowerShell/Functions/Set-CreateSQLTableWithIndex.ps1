Start-Transcript "$PSScriptRoot\..\..\Transcripts\$($($MyInvocation.MyCommand.Name).split('.')[0])\$($MyInvocation.MyCommand.Name)_$(get-date -f 'dd-MM-yyyy_hh.mm.ss').txt"
Function Set-CreateSQLTableWithIndex {
    Param (
        $Report,
        $SQLTable,
        $PrimaryKeys,
        $DateTypeToDate,
        $Indexes
    )

    $params = @{'server' = 'NEXSY3POWERBI'; 'Database' = $SQLTable.split(".")[0].trim("[]") }
    $timeStamp = "{0:yyyy-MM-dd HH:mm:ss.fffffff}" -f (get-date)
    $Errors = @()

    $propertyNameAndType = @()
    foreach ($property in $Report[0].PSObject.Properties) {
        $dataType = $null
        if ($property.Name -notin $PrimaryKeys) {
            if ($property.Name -notin $Indexes) {
                $dataType = switch -Wildcard ($property.TypeNameOfValue) {
                    "*System.String*" { "[nvarchar](MAX)" }
                    "*System.Int32*" { "[int]" }
                    "*System.DateTime*" { "[datetime2](7)" }
                    "*System.Double*" { "[float]" }
                    default { "[nvarchar](MAX)" }
                }
                $propertyNameAndType += "[$($property.name)] $($dataType)"
            }   
            if ($property.Name -in $Indexes) {
                $dataType = switch -Wildcard ($property.TypeNameOfValue) {
                    "*System.String*" { "[nvarchar](190)" }
                    "*System.Int32*" { "[float]" }
                    "*System.DateTime*" { "[datetime2](7)" }
                    "*System.Double*" { "[float]" }
                    default { "[nvarchar](190)" }
                }
                $propertyNameAndType += "[$($property.name)] $($dataType)"
            }
        }
        if ($property.Name -in $PrimaryKeys) {
            if ($property.Name -in $DateTypeToDate) {
                $dataType = "[date]"
            }
            else {
                $dataType = switch -Wildcard ($property.TypeNameOfValue) {
                    "*System.String*" { "[nvarchar](190) NOT NULL" }
                    "*System.Int32*" { "[float]" }
                    "*System.DateTime*" { "[datetime2](7)" }
                    "*System.Double*" { "[float]" }
                    default { "[nvarchar](120)" }
                }
            }
            $propertyNameAndType += "[$($property.name)] $($dataType)"
        }
    }


    $InsertResults = "CREATE TABLE $($SQLTable) (
        $($($propertyNameAndType) -join ', ')
        ,[DataTimeStamp] [datetime2](7)
        ,PRIMARY KEY ($($PrimaryKeys))
        );
        "
    Write-Host -f Blue $InsertResults
     

    Invoke-sqlcmd @params -Query $InsertResults -ErrorAction Stop
   
    foreach ($Index in $Indexes) {
        WRite-Host -f Yellow "Creating $($index) index"
           
        $CreateIndex = "create index $($SQLTable.split(".")[0].trim('[]'))_$($Index.replace('.','_'))_idx
            on $($SQLTable) ([$($index)])"
            
        Invoke-sqlcmd @params -Query $CreateIndex -ErrorAction Stop
    }
}

Stop-Transcript