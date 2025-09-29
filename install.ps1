$envName = "PythonEnv"

if (-not (Test-Path $envName)) {
    python -m venv $envName
    
}

$activate = ".\$envName\Scripts\Activate.ps1"
& $activate

python -m pip install --upgrade pip
pip install -r requirements.txt
python src/db.py