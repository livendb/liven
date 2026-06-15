<#
Install Liven as a Windows Service

This script provides instructions for installing Liven as a Windows Service.
You can use either the built-in sc command or NSSM (Non-Sucking Service Manager).

USAGE:
  .\install-windows.ps1
#>

Write-Host "Liven Windows Service Installation"
Write-Host "====================================="
Write-Host ""

Write-Host "Option 1: Using NSSM (Recommended)"
Write-Host "-----------------------------------"
Write-Host "1. Download NSSM from https://nssm.cc/download"
Write-Host "2. Extract nssm.exe to C:\\liven\\"
Write-Host "3. Run these commands as Administrator:"
Write-Host ""
Write-Host "   nssm install Liven `"C:\\liven\\liven.exe`" start"
Write-Host "   nssm set Liven AppDirectory `"C:\\liven\\`""
Write-Host "   nssm set Liven AppStdout `"C:\\liven\\logs\\stdout.log`""
Write-Host "   nssm set Liven AppStderr `"C:\\liven\\logs\\stderr.log`""
Write-Host "   nssm start Liven"
Write-Host ""

Write-Host "Option 2: Using built-in sc command"
Write-Host "------------------------------------"
Write-Host "1. Create a batch file C:\\liven\\start-liven.bat with:"
Write-Host "   @echo off"
Write-Host "   cd C:\\liven"
Write-Host "   liven.exe start > logs\\liven.log 2>&1"
Write-Host ""
Write-Host "2. Run these commands as Administrator:"
Write-Host "   sc create Liven binPath= `"C:\\liven\\start-liven.bat`" start= auto"
Write-Host "   sc start Liven"
Write-Host ""

Write-Host "Option 3: Manual startup (for testing)"
Write-Host "---------------------------------------"
Write-Host "   liven.exe start > liven.log 2>&1"
Write-Host ""

Write-Host "Commands:"
Write-Host "  Start:     sc start Liven"
Write-Host "  Stop:      sc stop Liven"
Write-Host "  Status:    sc query Liven"
Write-Host "  Logs:      type C:\\liven\\logs\\liven.log"
Write-Host ""

Write-Host "Logs are written to the specified log files."
Write-Host "You can also view logs in Event Viewer under Windows Logs > Application."
