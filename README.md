### DEV

requirements
- python: (fastapi, asyncio, duckdb, pydantic, uvicorn, ...)
- node: (react, tailwindcss, eslint, ...)

getting started
[this is a react app made with typescript and tailwind css.]
```
python -m venv .venv        # create a python virtual environment
pip install -r requirements.txt

python -m api.main                      # start the api
cd frontend/ & npm i & npm run dev      # install node modules and start frontend
```

### DATABASE URL

NOTICE!
For obvious reasons the database file is not included in the online repository.
Here is an available source:

https://gibonet.ch/duckdb/db_cubi_ustat.ddb


### DuckDB GUI

requirements
- duckdb cli

a visual helper to access a duckdb
```
duckdb -ui
```

### DESCRIPTION

The main purpose for this app is to provide a user the capability perform DB queries using an api to a duckDB database.
The app is built around the web gui, developed in the /frontend folder. Hence the api endpoints where made for the gui itself and not as an extra access point.
Nevertheless, the api endpoints are as follow:
```
GET /api/tables     # lists the tables in the DB
GET /api/tables/{table_name}    # gets the whole table data
POST /api/tables/{table_name}   # gets the whole data filtered by selected columns
POST /api/tables/{table_name}/pivot     # allows for pivot operation given selected columns and selected unique values *
GET /api/tables/{table_name}/columns    # gets all the non numerical or varchar columns of a table
GET /api/tables/{table_name}/columns-values     # gets all the unique values from non numerical or varchar columns

```
\* (if not all columns of the table are selected, then it pivots the unique values of the unselected columns)


In the frontend we mostly use the pivot, columns, tables and column-values api endpoints. At each user change a new query is sent to the api which results a json table.

The backend is build in python using a FastAPI app. 
Other than the api endpoints the backend features:
  - asyncronous readonly db connection pool
  - a table-column cache system
  - ability to insert new csv files to the duckDB database  


developer: William Ambrosetti

references: 
  - [ckan docker project](https://gitlab.com/WiiLife/ckan-docker-data-portal-ustat)
  - [original cubi app](https://gitlab.com/gibonet/cubustat)
