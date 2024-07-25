import os
import pandas as pd
import time
import snowflake.connector as snow
from snowflake.connector.pandas_tools import write_pandas

# URL to the Zillow ZHVI data.
# Should be checked periodically to ensure the URL points to the correct table.
URL = "https://files.zillowstatic.com/research/public_csvs/zhvi/County_zhvi_uc_sfrcondo_tier_0.33_0.67_sm_sa_month.csv?t=1721931323"

# Snowflake data warehouse you wish to use.
# Currently written with example values that you can replace
# with information from your actual account.
# This data warehouse needs to exist prior to running this pipeline.
WAREHOUSE = "EXAMPLE_WH"

# Snowflake database you wish to use.
# This database needs to exist prior to running this pipeline.
DATABASE = "EXAMPLE_DB"

# Snowflake schema you with to use.
# This schema should exist prior to running this pipeline.
SCHEMA = "EXAMPLE_SCHEMA"

# Snowflake table you wish to create.
# This table will e created if it doesnt already exist when you run this pipeline.
TABLE = "ZILLOW_COUNTY_ZHVI_TIMESERIES"

INITIAL_LAYOUT_COLUMN_NAMES = [" REGIONID", "SIZERANK", "REGIONTYPE", "STATENAME", "STATE", "METRO", "STATECODEFIPS", "MUNICIPALCODEFIPS", "REGIONNAME"]

INITIAL_TABLE_LAYOUT = (
    "ZILLOW_COUNTY_ZHVI_TIMESERIES(REGIONID NUMBER(38,0), "
    "SIZERANK NUMBER(38,0), "
    "REGIONNAME VARCHAR(16777216), "
    "REGIONTYPE VARCHAR(16777216), "
    "STATENAME VARCHAR(2), "
    "STATE VARCHAR(2), "
    "METRO VARCHAR(16777216), "
    "STATECODEFIPS NUMBER(38,0), "
    "MUNICIPALCODEFIPS NUMBER(38,0));" 
    )

def add_date(date_ymd: str) -> str:
    return "DATE" + date_ymd

def main():

    df = pd.read_csv(URL)

    df.columns = [col.upper().replace("-", "_") for col in df.columns]

    df.columns = [add_date(col if col in INITIAL_LAYOUT_COLUMN_NAMES else col for col in df.columns)]

    cnct = snow.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        authenticator="externalbrowser"
        warehouse=WAREHOUSE,
        database=DATABASE,
    )

    time.sleep(5)

    cursor = cnct.cursor()

    cursor.execute("USE WAREHOUSE " + WAREHOUSE)

    cursor.execute("USE DATABASE " + DATABASE)

    cursor.execute("USE SCHEMA " + SCHEMA)

    cursor.execute("CREATE OR REPLACE TABLE " + INITIAL_TABLE_LAYOUT)

    cursor.execute("SELECT * FROM " + TABLE + " Limit 1")

    snowf_columns = [desc[0] for desc in cursor.description]

    for col_name in snowf_columns:
        if col_name not in snowf_columns:
            snowf_command = ("ALTER TABLE " + TABLE + " \nADD COLUMN " + col_name + " NUMBER(38,5);")
            cursor.execute(snowf_command)

    write_pandas(cnct, df, TABLE, database=DATABASE, schema=SCHEMA, auto_create_table=True)

    cursor.close()

    cnct.close()

if __name__=="__main__":
    main()