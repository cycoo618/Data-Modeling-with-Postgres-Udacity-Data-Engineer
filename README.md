# Project: Data Modeling with Postgres
##### _README file_
&nbsp;
### What it does?

This package uses PostgreSQL and Python to create a relational database called Sparkify and build ETL pipelines for loading song and log data to the database automatically.


### How to use?

- Run _`create_tables.py`_ first to reset/create a database called Sparkify, and create tables inside the database.
- Run _`etl.py`_ after, which performs the ETL process. This will load data from the song and log file paths to the database.

### File in the repository:
- _`create_tables.py`_: create/reset the database and tables. Always run this first when beginning the project.
- _`sql_queries.py`_: stores all the SQL queries document for CREATE, INSERT, DROP and other queries needed for building the database.
- _`etl.py`_: builds ETL pipeline and performs the ETL process by calling _`sql_queries.py`_.
- _`etl_copy.py`_: a different version of _`etl.py`_, which does not output validation file.
- _`etl.ipynb`_: ETL pipeline in Jupyter Notebook. Reads and processes a single file from song data and log data and loads the data into your tables. This is used for the construction and testing phase when building the ETL pipeline.
- _`test.ipynb`_: displays the first few rows of each table to let you check your database and tables' integrity.
- _`Analysis.ipynb`_: connects to the built database and tables and performs the exploratory analysis.
- _`check_list.csv`_: stores all the records that the songs and artists that are not in the Sparkify database. There are three columns in this file: songs, artists, duration.
- _`README.md`_: a readme file provides discussions for this repository.

### Sparkify Databse Schema:

- Fact table:
  - songplays: stores users' music listening records which is extracted from log data
- Dimension tables:
  - songs: stores song information which is extracted from song data
  - artists: stores artist information which is extracted from song data
  - users: stores user information which is extracted from log data
  - time: parsed time information which is extracted from timestamp from log data

![Song_ERD](https://udacity-reviews-uploads.s3.us-west-2.amazonaws.com/_attachments/339318/1586016120/Song_ERD.png)