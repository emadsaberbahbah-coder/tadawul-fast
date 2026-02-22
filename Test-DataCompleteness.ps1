<#
.SYNOPSIS
    Tadawul Fast Bridge - Enterprise Data Completeness Tester
.DESCRIPTION
    Tests the live API endpoints to ensure data is not only returning 200 OK,
    but that the JSON payloads contain complete, enriched financial data.
#>

param (
    [string]$BaseUrl = "https://tadawul-fast-bridge.onrender.com",
    [string]$Token = ""
)

$ErrorActionPreference = "Stop"
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

$Headers = @{
    "Accept" = "application/json"
}
if (![string]::IsNullOrEmpty($Token)) {
    $Headers["X-APP-TOKEN"] = $Token
}

# Counters
$global:TotalTests = 0
$global:PassedTests = 0

function Write-Banner {
    Clear-Host
    Write-Host "===========================================================" -ForegroundColor Cyan
    Write-Host " 🚀 TADAWUL FAST BRIDGE - DATA COMPLETENESS TESTER" -ForegroundColor Cyan
    Write-Host "===========================================================" -ForegroundColor Cyan
    Write-Host "Target URL : $BaseUrl" -ForegroundColor Gray
    Write-Host "Start Time : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')" -ForegroundColor Gray
    Write-Host "===========================================================" -ForegroundColor Cyan
    Write-Host ""
    
    if ([string]::IsNullOrEmpty($Token)) {
        Write-Host "⚠️ WARNING: No -Token provided. Secure endpoints will return 401 Unauthorized." -ForegroundColor Yellow
        Write-Host ""
    }
}

function Invoke-CompletenessTest {
    param (
        [string]$TestName,
        [string]$Endpoint,
        [string]$Method = "GET",
        [string]$Body = $null,
        [string[]]$RequiredFields
    )

    $global:TotalTests++
    Write-Host "Testing [$TestName]..." -NoNewline

    $Uri = "$BaseUrl$Endpoint"
    
    try {
        $Params = @{
            Uri = $Uri
            Method = $Method
            Headers = $Headers
        }
        if ($Method -eq "POST" -and $Body) {
            $Params["Body"] = $Body
            $Params["ContentType"] = "application/json"
        }

        $Response = Invoke-RestMethod @Params
        
        # Determine the root object to check (Smart Array Unwrapping)
        $TargetObj = $Response
        
        if ($null -ne $Response.data) { 
            if ($Response.data -is [array] -and $Response.data.Count -gt 0) { $TargetObj = $Response.data[0] }
            else { $TargetObj = $Response.data }
        }
        elseif ($null -ne $Response.items -and $Response.items.Count -gt 0) { $TargetObj = $Response.items[0] }
        elseif ($null -ne $Response.results -and $Response.results.Count -gt 0) { $TargetObj = $Response.results[0] }
        elseif ($null -ne $Response.recommendations -and $Response.recommendations.Count -gt 0) { $TargetObj = $Response.recommendations[0] }

        $MissingFields = @()

        foreach ($Field in $RequiredFields) {
            # Check if property exists and is not null/empty
            $Val = $TargetObj.$Field
            if ($null -eq $Val -or $Val -eq "") {
                $MissingFields += $Field
            }
        }

        if ($MissingFields.Count -eq 0) {
            Write-Host " [PASS]" -ForegroundColor Green
            $global:PassedTests++
        } else {
            Write-Host " [FAIL]" -ForegroundColor Red
            Write-Host "    -> Missing or empty fields: $($MissingFields -join ', ')" -ForegroundColor Yellow
            if ($Response.error) {
                Write-Host "    -> API Error: $($Response.error)" -ForegroundColor DarkGray
            }
        }

    } catch {
        Write-Host " [ERROR]" -ForegroundColor Red
        
        $exceptionMsg = $_.Exception.Message
        # Attempt to read the actual HTTP response body (JSON error) instead of just the status code
        if ($_.Exception.Response) {
            try {
                $reader = New-Object System.IO.StreamReader($_.Exception.Response.GetResponseStream())
                $responseBody = $reader.ReadToEnd()
                $exceptionMsg += "`n       Body: $responseBody"
            } catch {}
        }
        
        Write-Host "    -> Exception: $exceptionMsg" -ForegroundColor DarkGray
    }
}

Write-Banner

# -----------------------------------------------------------------------------
# Test Cases
# -----------------------------------------------------------------------------

# 1. System Health
Invoke-CompletenessTest `
    -TestName "System Health" `
    -Endpoint "/readyz" `
    -RequiredFields @("status", "ready")

# 2. Global Enriched Quote (Apple)
Invoke-CompletenessTest `
    -TestName "Global Enriched Quote (AAPL)" `
    -Endpoint "/v1/enriched/quote?symbol=AAPL" `
    -RequiredFields @("symbol", "current_price", "previous_close", "market_cap", "data_quality")

# 3. KSA Advanced Analysis (Aramco) -> UPDATED TO /v1/analysis
Invoke-CompletenessTest `
    -TestName "KSA AI Analysis (2222.SR)" `
    -Endpoint "/v1/analysis/quote?symbol=2222.SR&include_ml=true" `
    -RequiredFields @("symbol", "price", "volume", "overall_score", "risk_score", "recommendation")

# 4. KSA Argaam Direct Quote (Al Rajhi)
Invoke-CompletenessTest `
    -TestName "KSA Argaam Direct (1120.SR)" `
    -Endpoint "/v1/argaam/quote?symbol=1120.SR" `
    -RequiredFields @("symbol", "current_price", "pe_ratio", "market_cap", "data_quality")

# 5. Batch Quotes Processing -> UPDATED TO /v1/analysis
Invoke-CompletenessTest `
    -TestName "Batch AI Quotes (AAPL, MSFT, 2222.SR)" `
    -Endpoint "/v1/analysis/quotes?tickers=AAPL,MSFT,2222.SR" `
    -RequiredFields @("symbol", "price", "overall_score")

# 6. AI Investment Advisor Portfolio Generation
$AdvisorBody = @"
{
  "tickers": ["AAPL", "MSFT", "GOOGL", "2222.SR", "1120.SR"],
  "invest_amount": 100000,
  "risk_profile": "moderate",
  "allocation_strategy": "maximum_sharpe",
  "enable_ml_predictions": true
}
"@

Invoke-CompletenessTest `
    -TestName "AI Advisor Portfolio Generation" `
    -Endpoint "/v1/advisor/run" `
    -Method "POST" `
    -Body $AdvisorBody `
    -RequiredFields @("symbol", "weight", "allocated_amount", "expected_return_12m")


# -----------------------------------------------------------------------------
# Final Summary
# -----------------------------------------------------------------------------
Write-Host ""

Write-Host "===========================================================" -ForegroundColor Cyan
Write-Host " SUMMARY: $global:PassedTests out of $global:TotalTests tests passed." -ForegroundColor White

if ($global:PassedTests -eq $global:TotalTests) {
    Write-Host " STATUS : PERFECT (Data Pipeline is Fully Operational) 🌟" -ForegroundColor Green
} else {
    Write-Host " STATUS : DEGRADED (Some fields are missing/null or threw errors) ⚠️" -ForegroundColor Yellow
}
Write-Host "===========================================================" -ForegroundColor Cyan
