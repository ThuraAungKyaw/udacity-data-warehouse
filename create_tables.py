import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Description: Drop tables using queries defined in the 'drop_table_queries' array if they already exist.
     
    Arguments:
          cur: the cursor object. 
          conn: the connection object. 

      Returns:
          None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description: Create tables using queries defined in the 'create_table_queries' array.
    
    Arguments:
          cur: the cursor object. 
          conn: the connection object. 

      Returns:
          None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        - Read configuration settings defined in the 'dwh.cfg' file.
        - Establishes a connection with the database and gets
          a cursor.  
        - Drop existing tables and create all tables as needed. 
        - Finally, closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()