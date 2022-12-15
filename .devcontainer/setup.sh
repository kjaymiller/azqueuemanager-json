python -m pip install --upgrade pip
python -m pip install pip-tools
python -m piptools compile -o requirementst.txt pyproject.toml
python -m pip install -r requirements.txt