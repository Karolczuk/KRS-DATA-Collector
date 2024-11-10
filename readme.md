# KRS Database Generator

## Description

The `KRS Database Generator` program automatically downloads data about companies in Poland from KRS services using an API and creates a database on a MySQL engine. This enables both analysis and management of company data in Poland. After the initial download, the program will run daily at the selected time to update the data, ensuring you always have the most current information. You can get more information about API: https://prs.ms.gov.pl/krs/openApi.

## Requirements

- Python 3.x
- Libraries:
  - `requests`
  - `mysql-connector-python`
  - `json`
  - `numpy`

To install the required libraries, use:
```bash
pip install requests 
pin mysql-connector-python
pip install numpy
```

## Configuration

before start program configure cofing file.py

database_table = 'table_name'      # Set common name of table
database_name = 'database_name'    # Set database name
database_host = 'host'             # Set host addres
database_user = 'user'             # Set user
database_password = 'password'     # Set password to database
database_port = 3306               # Set port of database (default 3306)

## Start program

To start program
```bash
python main.py
```

You can connect to your database and use SQL queries to retrieve the data you're interested in. In my case, I run this program on my Synology NAS server using Container Manager (Docker). The program runs continuously, updating daily with data from KRS services. I use DBeaver to manipulate and work with the data. If u want u can use my sql query from sample_query.sql! If u dont wanna download KRS from the start u can use my dump-firmy-krs.sql to load data to your server.

ENJOY!!!

