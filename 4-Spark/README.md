# Sparkify Data 

This project is intended to get the data collected on songs and user activity of Sparkify and make it queriable.
It reads the data located in S3 and then transforms it into parquet format table using spark.

### Fact table:
- <b> songplays: </b> records of log data of songs played by users

### Dimension tables:
- <b> users: </b> the customers infomation
- <b> songs: </b> the songs available in sparkify
- <b> artists: </b> the artists of the songs
- <b> time: </b> the broken down timestamps of the logs

![Sparkify ERD](images/SparkifyERD.png)

## Usage:

##### Extract Transform and Load the data into the final database
```shell 
python etl.py
```

### Available files
- <b> etl.py </b> python script to extract data from the json files transform the read data and save it as paquet formated tables
- <b> README.md </b> readme file for this project
- <b> dl.cfg </b> config file for the s3 access credentials
- <b> data </b> folder containing subset data to test etl
- <b> images </b> folder containing images for README.md file