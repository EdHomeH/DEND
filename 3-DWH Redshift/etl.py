import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load all necessary tables with queries from sql_queries to the staging area using the cursor cur and connection conn

    Parameters:
        cur: cursor to execute the queries
        conn: connection to the database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Load all necessary tables from staging to with queries from sql_queries to the staging area using the cursor cur and connection conn

    Parameters:
        cur: cursor to execute the queries
        conn: connection to the database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
