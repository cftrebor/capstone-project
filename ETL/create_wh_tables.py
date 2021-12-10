import configparser
import logging
import psycopg2
import time
from sql_queries import create_wh_table_queries, drop_wh_table_queries


def create_database():
    """
    - Creates and connects to the covid19
    - Returns the connection and cursor to covid19
    """
    # connect to default database using config file
    config = configparser.ConfigParser()
    config.read('config')

    # connect to covid19 database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DEV'].values()))
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in drop_wh_table_queries list.
    """
    logging.info('Dropping existing warehouse tables.')
    for query in drop_wh_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in create_wh_table_queries list. 
    """
    logging.info('Creating new warehouse tables.')
    for query in create_wh_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Establishes connection with the covid19 database and gets cursor to it.  
    
    - Drops all existing warehouse tables.  
    
    - (Re)creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    # configure log file
    timestr = time.strftime("%Y%m%d-%H%M%S")
    logging.basicConfig(filename=f"./logs/create-wh-tables-{timestr}.log", encoding='utf-8', level=logging.INFO)

    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
