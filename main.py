import os
import sys
import glob
import psycopg2 as ps
from helpers.stage_file import load_staging_sas, load_staging_csv, run_sql, cleanup

if __name__ == "__main__":
    """Main execution of ETL pipline to stage tranform and load immigration and weather data into analytics stables
    """
    #################
    # create tables
    #################
    try:
        run_sql('./sql/create_staging.sql') 
        run_sql('./sql/create_analytics.sql')
        print('Tables created')
    except Exception as e:
        print('Error creating tables: ' + str(e))
        sys.exit()

    #################
    # stage files
    #################
    try:
        load_staging_csv('../../data2/GlobalLandTemperaturesByCity.csv', 'staging.city_temp')
        load_staging_csv('./data/i94port.csv', 'staging.i94port')
        load_staging_csv('./data/i94res.csv', 'staging.i94res', ';')
        for f in glob.glob('../../*/*.sas7bdat'):
            load_staging_sas(f)
        print('Data staged')
    except Exception as e:
        print('failed to stage files')
        sys.exit()

    ###################
    # Transform data
    ###################
    try:
        run_sql('./sql/load_countries.sql')
        run_sql('./sql/load_cities.sql')
        run_sql('./sql/load_immigration.sql')
        run_sql('./sql/load_countries.sql')
        print ('Data transformation complete')
    except Exception as e:
        print('Failed to transform data: ' + str(e))
        sys.exit()

    ###################
    # Data quality checks
    ###################
    try:
        run_sql('./sql/load_summary.sql')
        run_sql('./sql/data_quality.sql')
        print('Data quality ok')
    except Exception as e:
        print('Data quality checks failed: ' + str(e))
        sys.exit()

    ###############
    # Cleanup
    ###############
    cleanup()
    print('pipeline complete')
