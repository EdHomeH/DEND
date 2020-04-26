# Sparkify Data 

This project is intended to get the data collected on user activity of Sparkify and load it into an Apache Cassandra 
database to be able to quickly extract insights given specific queries.
 
## Usage:

Open the Project_1B.ipynb jypter and execute all cells to:

- Load all csv files in event data folder into one csv file in the current directory (Part I)
- Create the cluster, keyspace and tables modelled in such a way to efficiently query these three queries needed by the 
analytics team (Part II):
    
    1. Give me the artist, song title and song's length in the music app history that was heard during  
    sessionId = 338, and itemInSession  = 4

    2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) 
    for userid = 10, sessionid = 182
    
    3. Give me every user name (first and last) in my music app history who listened to the song 
    'All Hands Against His Own'

### Available files
- <b> Project_1B.ipynb </b> jupyter notebook that processes the data and loads it into the database
- <b> README.md </b> readme file for this project
- <b> event_data </b> folder with the data
- <b> images </b> folder with images for the Project_1B.ipyb notebook