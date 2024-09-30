brew install python@3.11

python3.11 -m ven .ven

source .venv/bin/activate

pip install -r requirements.txt

where python (to see the path for python 3.11 in .venv and assign to env variables)

export PYSPARK_PYTHON=/Users/phuc.nguyen/Downloads/release/.venv/bin/python3.11
export PYSPARK_DRIVER_PYTHON=/Users/phuc.nguyen/Downloads/release/.venv/bin/python3.11

python index.py 

deactivate (to out of .venv)