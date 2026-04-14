### DEV

requirements
- python
- node

getting started
```
python -m venv .venv    # create a python virtual environment
pip install -r requirements.txt

python -m api.main  # start the api
cd frontend & npm run dev
```

### DuckDB GUI

requirements
- docker

a visual helper to access a duckdb
```
duckDB gui: docker run -p 5522:5522 ghcr.io/caioricciuti/duck-ui
```