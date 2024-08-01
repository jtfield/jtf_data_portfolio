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
TABLE = "ZHVI_COUNTY_TIMESERIES"

# List of non-data column names included in the Zillow ZHVI county table.
# This is established for easy identification of date and non-date column nnames when processing the table.
# Snowflake doesnt allow for numbers as the first character in the column name.
INITIAL_LAYOUT_COLUMN_NAMES = [
    " REGIONID",
    "SIZERANK",
    "REGIONTYPE",
    "STATENAME",
    "STATE",
    "METRO",
    "STATECODEFIPS",
    "MUNICIPALCODEFIPS",
    "REGIONNAME",
]

# Initial table structure when creating the table in Snowflake.
# This includes the non-date column names found in the Zillow zhvi county data.
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
    """Return the input string with DATE added to the start of the string."""
    return "DATE" + date_ymd


# Main function.
# Consider abstracting some processes to functions.
def main():

    # Read the URL and import the data table to a pandas dataframe
    df = pd.read_csv(URL)

    # Convert dataframe column names to upper case.
    # Sometimes a requirement depending on the organization.
    # Also replaces dashes (-) with underscores (_).
    # Snowflake seems to not allow dashes in column names.
    df.columns = [col.upper().replace("-", "_") for col in df.columns]

    # Apply the add_date function to each column in the dataframe if the column isnt in the list INITIAL_LAYOUT_COLUMN_NAMES
    # We do this because Snowflake doesnt allow column names to start with a number character.
    # The add_date function adds the string `DATE` to the start of the selected column names.
    df.columns = [
        add_date(
            col if col in INITIAL_LAYOUT_COLUMN_NAMES else col for col in df.columns
        )
    ]

    # Connection to users Snowflake account.
    # Uses credentials stored in your environments variables.
    # TODO: upgrade to using tokens if possible.
    cnct = snow.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
    )

    # Sleep command to ensure a connection has been made before proceeding.
    time.sleep(5)

    # Initialize the Snowflake connection cursor.
    # This is used to run SQL commands.
    cursor = cnct.cursor()

    # Executes a SQL command that specifies the warehouse we want to use, established in the global variable.
    cursor.execute("USE WAREHOUSE " + WAREHOUSE)

    # Executes a SQL command that specifies the database we want to use, established in the global variable.
    cursor.execute("USE DATABASE " + DATABASE)

    # Executes a SQL command that specifies the scema we want to use, established in the global variable.
    cursor.execute("USE SCHEMA " + SCHEMA)

    # SQL command to create the table if it doesnt exist OR replace the table if it does already exist.
    # Potentially heavy handed but currently standard practice.
    cursor.execute("CREATE OR REPLACE TABLE " + INITIAL_TABLE_LAYOUT)

    # Collect column name information for the existing table.
    # How this works with the CREATE OR REPLACE command is currently ambiguous.
    # Ultimately, we only want to send column name information if it doesnt already exist in the table.
    # TODO: try replaceing CREATE OR REPLACE TABLE command with CREATE IF NOT EXIST to test preserving columns.
    cursor.execute("SELECT * FROM " + TABLE + " Limit 1")

    # List comprehension collecting just the names of the columns in the existing table.
    # Necessary step for collecting the data from the Snowflake cursor command.
    snowf_columns = [desc[0] for desc in cursor.description]

    # Loop through the column names in the dataframe we collected from the Zillow portal.
    for col_name in snowf_columns:

        # If the column name isnt in the list of column names that we just collected
        # from the Snowflake table, write a command that will add the column to the Snowflake table.
        if col_name not in snowf_columns:

            # This command adds the new column to the Snowflake table.
            # We expect all of the columns being added after we establish the initial layout above
            # to use the same data type for price, so we hard code the type as NUMBER(28,5)
            snowf_command = (
                "ALTER TABLE " + TABLE + " \nADD COLUMN " + col_name + " NUMBER(38,5);"
            )

            # Execute the command we constructed above.
            # This adds the columns to the existing Snowflake table.
            cursor.execute(snowf_command)

    # Upload the data from the pandas dataframe to the Snowflake table.
    # The schema should match as we previously added columns to the Snowflake table
    # that match the column names in the pandas dataframe.
    # TODO: Identify if the auto_create_table option is necessary here.
    write_pandas(
        cnct, df, TABLE, database=DATABASE, schema=SCHEMA, auto_create_table=True
    )

    # Close the curpor object that we used to execute SQL commands.
    cursor.close()

    # Close the connection to the Snowflake account/workspace.
    cnct.close()


if __name__ == "__main__":
    main()
