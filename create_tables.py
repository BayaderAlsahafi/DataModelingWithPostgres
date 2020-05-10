"""Create Tables for the Sparkify Database

This script creates all the required tables in the Sparkify database.

This script requires you to have the sql_queries.py file in your project folder.

This script requires 'psycopg2' library to be installed within the Python
environment you are running this script in.
"""

import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def create_database():
    """ A function to create the Sparkify database in your local computer."""
        
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0") 

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """ 
    A function to drop all the existing tables in the database.
    
    Parameters: 
    cur (cursor): Psycopg2 cursor to execute PostgreSQL command in a database session.
    conn (connection): Handles the connection to a PostgreSQL database instance.
    """
        
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """ 
    A function to create all the tables.
    
    Parameters: 
    cur (cursor): Psycopg2 cursor to execute PostgreSQL command in a database session.
    conn (connection): Handles the connection to a PostgreSQL database instance.
    """
        
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    

    conn.close()


if __name__ == "__main__":
    main()