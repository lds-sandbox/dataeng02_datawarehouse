import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Executes the queries that load all of the staging tables.
    Show each query being executed to monitor progress
    
    Parameters:
    cur - cursor associated with the database connection
    conn - connection to the database
    """
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Executes the queries that insert data into all of the tables.
    Show each query being executed to monitor progress
    
    Parameters:
    cur - cursor associated with the database connection
    conn - connection to the database
    """
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    main function that reads the configuration file
    and establishes the connection to the database
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