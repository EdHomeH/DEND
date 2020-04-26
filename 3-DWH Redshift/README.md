# Sparkify Data 

This project is intended to get the data collected on songs and user activity of Sparkify and make it queriable.
it first stages all the files into two table in the Redshift cluster and these are then transformed into the five 
tables described down below.

### Fact table:
- <b> songplays: </b> records of log data of songs played by users

### Dimension tables:
- <b> users: </b> the customers infomation
- <b> songs: </b> the songs available in sparkify
- <b> artists: </b> the artists of the songs
- <b> time: </b> the broken down timestamps of the logs

## Usage:

##### Clear database and create the database tables
```shell 
python create_tables.py
```
##### Extract and Load the data into the staging area and then Transforme it into the final database
```shell 
python etl.py
```

### Available files
- <b> create_tables.py </b> python script to clear the database and create the tables from scratch
- <b> etl.py </b> python script to extract data from the json files transform the read data and load it into the database
- <b> README.md </b> readme file for this project
- <b> sql_queries.py </b> python file with the queries necesary for the etl process