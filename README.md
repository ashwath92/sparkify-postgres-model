## Sparkify Postgres Data Model

The purpose of this project is to create a suitable PostgreSQL data model which can be used to analyse user behaviour based on song play logs and additional song data. 

### Description of Data Model:
We create various Postgres tables and insert data into them from the log and song files. A short description of the tables follows.

#### Dimension tables
1. songs: contains information about songs. This is fetched from the song data. Columns: song_id, title, artist_id, year, duration.
2. artists: contains information about the songs' artists. This is fetched from the song data.
3. time: contains information about when songs were played. This information is present in the log files. Columns: start_time, hour, day, week, month, year, weekday.
4. users: a table containing user information. The user information is obtained from the log files. Columns: user_id, first_name, last_name, gender, level.

#### Fact table
1. songplays: contains information pertaining to the event that a song is played. Contains user, time, artist and song info. Columns: songplay_id, start_time, user_id, level, song_id, artist_id, location, session_id, user_agent.

### Description of files.
The log and song data is present in the data/ folder. An overview of the different files in the repository is given below. 

#### Python programs (.py files)
1. sql_queries.py: contains PostgreSQL DDL queries to create and drop tables, as well as DML queries to insert data into the said tables. It also contains queries to create, insert into and drop temporary tables, which are used for bulk insertion of log data using Postgres's COPY_FROM statement.
2. create_tables.py: creates all the dimension and fact tables using the queries from sql_queries.py
3. etl.py: The ETL program which extracts data from the log files as well as the song files, stages them into temporary tables where required, and loads them into the Postgres tables. Temporary tables are used while inserting into the users, time and songplays tables.

#### Ipython Notebooks (.ipynb files)
1. etl.ipynb: used for experimentation before writing etl.py.
2. test.ipynb: contains simple queries to test that the required data has been loaded into the Postgres tables by etl.py
3. analytics.ipynb: contains analytics queries. These queries focus on obtaining information which might be useful to grow the revenue of the music streaming company. 

### Execution
1. Run create_tables.py to create all the Postgres tables.
2. Run etl.py to load data from the log and song files into the Postgres tables.
3. The data can be queried using the queries in the test.ipynb notebook.
4. The results of more complex analytics queries can be seen in the analytics.ipynb noetbook. 