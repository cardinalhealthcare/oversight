"""
This script fetches data from Google Sheets and loads it into a database.

Add the following to your .env file:

SERVICE_ACCOUNT_FILE: path to service account file; env/service_account.json
DATABASE_URL: database connection string; postgresql://user:password@host:port/database
SHEETS_TO_LOAD_FILE: path to sheets_to_load.csv; env/sheets_to_load.csv

SHEETS_TO_LOAD_FILE is a CSV file with the following columns:
- sheet_id: Google Sheet ID
- worksheet_name: Worksheet name
- table_name: Table name

Example file: https://docs.google.com/spreadsheets/d/1rFo6M58rCcuDa7bOKyGE4NyCp4q_hDUlFXorrvGqwKs/edit?usp=sharing

Usage:
    python scripts/prep_airup.py

"""
import gspread
import pandas as pd
from environs import Env
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Float
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
import re
# LOAD CONFIG FROM ENVIRONMENT VARIABLES
env = Env()
env.read_env()

SERVICE_ACCOUNT_FILE = env.str("SERVICE_ACCOUNT_FILE")
DATABASE_URL = env.str("DATABASE_URL")
SHEETS_TO_LOAD_FILE = env.str("SHEETS_TO_LOAD_FILE")

# Parse sheets to load from CSV file
SHEETS_TO_LOAD = []
try:
    sheets_df = pd.read_csv(SHEETS_TO_LOAD_FILE)
    for _, row in sheets_df.iterrows():
        SHEETS_TO_LOAD.append({
            "sheet_id": row["sheet_id"],
            "worksheet_name": row["worksheet_name"],
            "table_name": row["table_name"]
        })
except Exception as e:
    print(f"Error reading sheets_to_load file: {e}")
    raise

# SQLAlchemy Setup
Base = declarative_base()
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

def clean_column_name(name, position):
    """Clean column name for SQLAlchemy compatibility."""
    if not name or str(name).strip() == '':
        return f'column_{position}'
    # Replace spaces and special characters with underscore
    clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', str(name).strip())
    # Ensure name starts with letter
    if not clean_name[0].isalpha():
        clean_name = 'col_' + clean_name
    return clean_name
def create_or_update_table_from_sheet(table_name, df):
    """Create or update a table schema based on the DataFrame."""
    metadata = MetaData()
    metadata.bind = engine
    # Validate column names
    columns = []
    for i, col in enumerate(df.columns):
        if not col or str(col).strip() == '':
            col = clean_column_name(col, i)
        columns.append(Column(col, String))
    table = Table(table_name, metadata, *columns, extend_existing=True)
    metadata.create_all(engine)  # Creates table if it does not exist
    return table

def fetch_google_sheet(sheet_id, worksheet_name, service_account_file):
    """Fetch data from a Google Sheet."""
    try:
        client = gspread.service_account(filename=service_account_file)
        sheet = client.open_by_key(sheet_id)
        worksheet = sheet.worksheet(worksheet_name)
        data = worksheet.get_all_values()
        headers = data[0]
        rows = data[1:]
        print(f"Fetched {len(rows)} rows from {worksheet_name} in sheet {sheet_id}")
        # Create DataFrame
        df = pd.DataFrame(rows, columns=headers)
        # Clean column names
        df.columns = [clean_column_name(col, i) for i, col in enumerate(df.columns)]
        return df
    except Exception as e:
        print(f"Error fetching data from Google Sheets: {e}")
        raise

def load_data_to_db(table, df):
    """Load a pandas DataFrame into a database table."""
    try:
        # Handle duplicate column names by appending numbers
        col_counts = {}
        new_columns = []
        for col in df.columns:
            if col in col_counts:
                col_counts[col] += 1
                new_columns.append(f"{col}_{col_counts[col]}")
            else:
                col_counts[col] = 0
                new_columns.append(col)
        df.columns = new_columns
        
        conn = engine.connect()
        df.to_sql(table.name, conn, if_exists="replace", index=False)
        print(f"Data loaded into {table.name}")
    except Exception as e:
        print(f"Error loading data into database: {e}")
        raise
    finally:
        conn.close()

def main():
    try:
        for sheet in SHEETS_TO_LOAD:
            df = fetch_google_sheet(sheet["sheet_id"], sheet["worksheet_name"], SERVICE_ACCOUNT_FILE)
            table = create_or_update_table_from_sheet(sheet["table_name"], df)
            load_data_to_db(table, df)
    finally:
        session.close()

if __name__ == "__main__":
    main()
