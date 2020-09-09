import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


"""Extracts the data from S3

Keyword arguments:
cur -- the db cursor
conn -- the db connection

"""
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


"""Stages the data in Redshift

Keyword arguments:
cur -- the db cursor
conn -- the db connection

"""
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


"""Connect to the database and transforms data into a set of dimensional tables

Keyword arguments:
NONE

"""
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
