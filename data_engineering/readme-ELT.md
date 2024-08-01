# Intro
This project uses an ELT (Extract, Load, Transform) process.

- We extract the info from the data sources, at this point csv files.
- We load the data tables into a postgres database using spark
- We engineer any new features from stored variables and store them in the original tables.

# Usage

## TLDR
1. Create the database & Create the User
2. Create the database tables
3. 

## Creating the database
To create the database:
- Install Postgres and create a Postgres db server.
- Use [the python db creation script](create_db.py) or manually create a database and user for loading and engineering the data.
    - To execute the python script for creating the database, call the name of the script on a windows command line or if using vs-code simply use the run button in the top right corner.

## Creating the database tables
Once the database is created you'll want to create the tables where the raw features(variables) will be stored.
To create the tables:
- Execute the [tables creation script](create_tables.py) or copy the queries into your db management tool and execute them on the database.
    - If you decide to manually execute the queries, I suggest you keep the creation_script.py up to dateso you can easily reference the creation script.

## Extracting and loading the data
After creating the database tables, we need to obtain the data from the sources, load it into the database, and use any methods to create the features from the database.