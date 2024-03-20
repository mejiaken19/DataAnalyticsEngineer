

Function Get-LogicMonitor-Request {
    Param(
        $ResourcePath = $null
        , $QueryParams = $null
    )   

    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

    $config = Get-Content -path "$($PSScriptRoot)\config.json"
    $accessId = $config.access_id
    $accessKey = $config.access_key
    $company = $config.companyName

    $httpVerb = 'GET'
    $url = 'https://' + $company + '.logicmonitor.com/santaba/rest' + $resourcePath + $queryParams

    $epoch = [Math]::Round((New-TimeSpan -start (Get-Date -Date "1/1/1970") -end (Get-Date).ToUniversalTime()).TotalMilliseconds)
    $requestVars = $httpVerb + $epoch + $resourcePath

    $hmac = New-Object System.Security.Cryptography.HMACSHA256
    $hmac.Key = [Text.Encoding]::UTF8.GetBytes($accessKey)
    $signatureBytes = $hmac.ComputeHash([Text.Encoding]::UTF8.GetBytes($requestVars))
    $signatureHex = [System.BitConverter]::ToString($signatureBytes) -replace '-'
    $signature = [System.Convert]::ToBase64String([System.Text.Encoding]::UTF8.GetBytes($signatureHex.ToLower()))

    $auth = 'LMv1 ' + $accessId + ':' + $signature + ':' + $epoch

    $headers = New-Object "System.Collections.Generic.Dictionary[[String],[String]]"
    $headers.Add("Authorization", $auth)
    $headers.Add("Content-Type", 'application/json')

    $Stoploop = $false
 
    do {
        try {
            $response = Invoke-WebRequest -Uri $url -Method $httpVerb -Headers $headers
            $Stoploop = $true
        }
        catch {
            if ($response.status -eq 429) {
                Write-Host "Request exceeded rate limit, retrying in 60 seconds..."
                Start-Sleep -Seconds 60
            }
            else {
                Write-Host "Request failed, not as a result of rate limiting"
                $Stoploop = $true
            }
        }
    }
    While ($Stoploop -eq $false)

    Return $response
}