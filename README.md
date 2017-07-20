# Udacity Log Analysis Project
A script to print out various statistics from a database of news articles.

## Running the script
* Clone this repository.
* Download data from <https://d17h27t6h515a5.cloudfront.net/topher/2016/August/57b5f748_newsdata/newsdata.zip>.
* Load data into Postgres: `psql -d news -f newsdata.sql`.
* Install [psycopg2](http://initd.org/psycopg/): `pip install psycopg2`.
* Run the script: `python3 run_log_analysis.py`.