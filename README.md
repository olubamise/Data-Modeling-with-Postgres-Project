# Document Process

**Version: 1.0.**<br>
**Approved By: Joseph Aiyeoribe.**<br>
**Date: June 4, 2021.**<br>

#### Project description
This project involves creating database tables designed to optimize queries on song played analysis. It involves scripting ETL pipelines that analyses and transfer data from both JSON logs generated from user activities and JSON metadata on the songs directories. These data are filtered and stored on the database tables.

The project was designed using PostGreSQL, Jupyter Notebook and Python programming language.

#### Database design
1. Star schema was used as the database organizational structure for the data warehouse project. 
2. The data warehouse has a single large fact table called the songplays.
3. The data warehouse is refrenced by other smaller multiple dimensional tables such as the users, songs, artists and time tables.

#### Table Description
##### Fact Table
songplays - records log data associated with song plays. That is, songs with valid plays where length of play is not None. Below are songplays table column and its types.

*songplay_id int*<br>
*start_time timestamp*<br> 
*user_id varchar*<br> 
*level varchar*<br>
*song_id varchar*<br>
*artist_id varchar*<br>
session_id int*<br> 
*location varchar*<br>
*user_agent varchar*<br>

##### Dimension Tables
Users - records users information. Below are users table column and its types.<br>
*user_id int*<br>
*first_name varchar*<br>
*last_name varchar*<br>
*gender varchar*<br>
*level varchar*<br>

Songs - records songs information in the database. Below are songs table column and its types.<br>
*song_id varchar*<br> 
*title varchar*<br> 
*artist_id varchar*<br> 
*year int*<br> 
*duration numeric*<br>

Artists - records artists information in the database. Below are artists table column and its types.<br>
*artist_id varchar*<br> 
*name varchar*<br> 
*location varchar*<br> 
*latitude varchar*<br> 
*longitude varchar*<br>

Time - timestamps of records in the songplays table broken down into specific units to form the columns. Below are time table column and its types.<br>
*start_time timestamp*<br> 
*hour int*<br> 
*day int*<br> 
*week int*<br> 
*month int*<br>
*year int*<br> 
*weekday int*<br>

#### Entity Relationship Diagrams (ERD)
![Tux, the Linux mascot](/home/workspace/images/Data Modelling with Postgresql Project.png)<br>

#### ETL Process
All the ETL that is extraction, transformation and loading of data is done in the etl.py. Data is read and processed from a single JSON file song_data and log_data. It is then loaded inito loaded into the database tables. The ETL step by step activities includes:<br>
1. Read the JSON song file.
2. Extract and insert songs data into the songs table.
3. Extract and insert artists data into the artists table.
4. Bulk inserts dataframe to destination table.
5. Creates temporary csv.
6. Opens csv and copys into the database table.
7. Deletes temporary csv.
8. Read the JSON log fil.e
9. Filter page column of log data by NextSong to get lists of valid played songs.
10. Convert timestamp column of log data to datetime.
11. Extract the timestamp, hour, day, week of year, month, year, and weekday from the timestamp column of log data and insert time data records to time table.
12. Extract and insert users data into the users table.
13. Extract and populate the songplays table from the log data including song_id and artist_id columns generated from the songs and artists tables. This is the table that the 
    Anaytics team is mostly intrested in as that is where song information to be analysed is stored.<br>
    
#### Project Files Repository
Below are the project files and information about what they are used for:

1. test.ipynb - is the jupyter notebook file that contains SQL scripts to verify or display the first few rows of each table. This helps to confirm if data is stored in the database.
2. create_tables.py - is a python file that contains SQL scripts that drops and creates daabase tables. The file is executed to reset the database tables before the ETL file scripts is executed.
3. etl.ipynb - is a jupyter notebook file that reads and processes a single file from song_data and log_data and loads the data into the  tables. This notebook contains detailed instructions on the ETL process for each of the tables.
4. etl.py is python file that reads and processes files from song_data and log_data and loads them into the database tables. 
5. sql_queries.py - isa a python file that contains SQL scripts and queries, It is imported into the create_tables.py, etl.ipynb and etl.py files. It contains sql queries that drop tables, create tables and insert data into tables songplays, users, songs, artists and time tables as scripted in etl.py file.
6. README.md - is a plain text file that provides discussion on how the system.<br>

#### Steps to Run The Project
1. Click on File menu on Jupyter Notebook. 
2. From the File menu drop down, click on New Launcher.
3. Double click on Python 3 on the console section of the screen.
4. Enter "!python create_tables.py" command on the cell. This will execute the scripts in the create_tables.py file and create database and tables.  
5. Run the test.ipynb file to confirm the creation of the tables with the correct columns. 
6. Enter "!python etl.py" command on the cell. This will execute the script in the etl.py file to process ETL the data.
6. Click on Kernel menu >> "Restart kernel" to close the connection to the database after the test.ipynb.
7. Note that the create_tables.py command should be executed before running etl.py command.
8. Run test.ipynb to confirm the records were successfully inserted into each table.
