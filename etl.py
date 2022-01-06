import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
      Description: Insert data into the staging tables by executing the queries in 
      'copy_table_queries' array.

      Arguments:
          cur: the cursor object. 
          conn: the connection object. 

      Returns:
          None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
      Description: Insert data into the tables by executing the queries in 'insert_table_queries' array.

      Arguments:
          cur: the cursor object. 
          conn: the connection object. 

      Returns:
          None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        - Read configuration settings defined in the 'dwh.cfg' file.
        - Establishes a connection with the database and gets
          a cursor.  
        - Load data into the staging tables.
        - Gets data from the staging tables and insert into the final tables.
        - Finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()