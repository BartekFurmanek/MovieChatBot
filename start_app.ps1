$envName = "PythonEnv"

if (-not (Test-Path $envName)) {
    python -m venv $envName
    & ".\install.ps1"
}

Start-Process powershell -ArgumentList "-NoExit", "-Command", ".\$envName\Scripts\Activate.ps1; python .\src\app.py"