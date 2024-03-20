Function Get-QueryLocalDataWarehouse ($Query) {

    Write-Host "Querying from Local Database"


    Try {
        $Results = Invoke-Sqlcmd -Query $Query -ServerInstance "." -ConnectionTimeout 65534 -QueryTimeout 65535 -ErrorAction Stop
    }
    catch {
        Write-host "The database doesn't exist yet"
    }

    Return $Results
}

