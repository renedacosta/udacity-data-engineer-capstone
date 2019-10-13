import os
import pandas as pd
from pathlib import Path
import psycopg2 as ps
from io import StringIO

conn = ps.connect("postgresql://{}:{}@{}/{}".format(os.getenv('DBUSER'), os.getenv('DBPW'), os.getenv('DBHOST'), os.getenv('DB')))
cur = conn.cursor()

def load_staging_csv(file:str, table:str, delimiter:str=',') -> None:
    """Load data into staging tables
    
    :param file: file to be loaded into staging table
    :type file: str
    :param table: table name
    :type table: str
    :return: None
    :rtype: None
    """
    csv = open(file, 'r', encoding="ISO-8859-1")
    sql = f"""
    COPY {table}
    FROM STDIN DELIMITER '{delimiter}' CSV HEADER
    """
    cur.copy_expert(sql, csv)
    conn.commit()
    csv.close()

def load_staging_sas(file:str) -> None:
    df = pd.read_sas(file, 'sas7bdat', encoding="ISO-8859-1")
    # df = pd.read_csv(file, encoding="ISO-8859-1")
    df = df[[
        'i94yr','i94mon','i94cit','i94res','i94port','arrdate',
        'i94mode','depdate','i94bir','i94visa','gender', 'biryear'
    ]]

    csv = StringIO(newline='')
    df.to_csv(csv, sep=',', index=False)
    csv.seek(0)
    sql = f"""
    COPY staging.immigration
    FROM STDIN DELIMITER ',' CSV HEADER
    """
    cur.copy_expert(sql, csv)
    conn.commit()
    # postgres = PostgresHook(postgres_conn_id=hook)
    # postgres.copy_expert(sql, csv)
    csv.close()

def run_sql(sqlfile:str) -> None:
    """Run sql scripts
    
    :param sqlfile: sql script to run
    :type sqlfile: str
    :return: None
    :rtype: None
    """

    try:
        with open (sqlfile, 'r') as f:
            cur.execute(f.read())
        conn.commit()
        print(f'{sqlfile} succesfull')
    except Exception as e:
        conn.rollback()
        raise Exception(f'Failed to run {sqlfile} {str(e)}')

def cleanup():
    try:
        cur.execute("DROP SCHEMA IF EXISTS staging CASCADE")
        conn.commit()
        print('Cleanup done')
    except Exception as e:
        conn.rollback()
        print('Cleanup unsuccessfull: ' + str(e))

#  debugging
# if __name__ == '__main__':
#     load_staging_csv('./data/GlobalLandTemperaturesByCity.csv', 'staging.city_temp')
#     load_staging_csv('./data/i94port.csv', 'staging.i94port')
#     load_staging_csv('./data/i94res.csv', 'staging.i94res', ';')
#     load_staging_sas('./data/i94_sep16_sub.sas7bdat')
