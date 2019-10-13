import psycopg2 as ps
import pandas as pd
from pathlib import Path
from io import StringIO

conn = ps.connect("postgresql://{}:{}@{}/{}".format(os.getenv(DBUSER), os.getenv(DBPW), os.getenv(DBHOST), os.getenv(DB)))
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
    # df = pd.read_sas(file, 'sas7bdat', encoding="ISO-8859-1")
    df = pd.read_csv(file, encoding="ISO-8859-1")
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
    csv.close()

#  debugging
# if __name__ == '__main__':
#     load_staging_csv('./data/GlobalLandTemperaturesByCity.csv', 'staging.city_temp')
#     load_staging_csv('./data/i94port.csv', 'staging.i94port')
#     load_staging_csv('./data/i94res.csv', 'staging.i94res', ';')
#     load_staging_sas('./data/i94_sep16_sub.sas7bdat')
