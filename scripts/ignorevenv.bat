@echo off
PowerShell.exe -ExecutionPolicy Unrestricted -File .\ignorevenv.ps1 venv
PowerShell.exe -ExecutionPolicy Unrestricted -File .\ignorevenv.ps1 .git
pause
