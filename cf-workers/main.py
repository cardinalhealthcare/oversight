import gspread
import pandas as pd

from sqlalchemy import create_engine, MetaData, Table, Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import json

# LOAD CONFIG FROM ENVIRONMENT VARIABLES
env = {}

SERVICE_ACCOUNT_JSON = env.SERVICE_ACCOUNT_JSON
DATABASE_URL = env.DATABASE_URL
SHEETS_TO_LOAD_CSV = env.SHEETS_TO_LOAD

# Parse sheets to load from CSV format
SHEETS_TO_LOAD = []
for sheet in SHEETS_TO_LOAD_CSV.split(','):
    sheet_id, worksheet_name, table_name = sheet.split(':')
    SHEETS_TO_LOAD.append({"sheet_id": sheet_id, "worksheet_name": worksheet_name, "table_name": table_name})

# SQLAlchemy Setup
Base = declarative_base()
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

def create_or_update_table_from_sheet(table_name, df):
    """Create or update a table schema based on the DataFrame."""
    metadata = MetaData(bind=engine)
    columns = [Column(col, String) for col in df.columns]
    table = Table(table_name, metadata, *columns, extend_existing=True)
    metadata.create_all(engine)  # Creates table if it does not exist
    return table

def fetch_google_sheet(sheet_id, worksheet_name, service_account_json):
    """Fetch data from a Google Sheet."""
    try:
        service_account_info = json.loads(service_account_json)
        client = gspread.service_account_from_dict(service_account_info)
        sheet = client.open_by_key(sheet_id)
        worksheet = sheet.worksheet(worksheet_name)
        data = worksheet.get_all_values()
        headers = data[0]
        rows = data[1:]
        print(f"Fetched {len(rows)} rows from {worksheet_name} in sheet {sheet_id}")
        return pd.DataFrame(rows, columns=headers)
    except Exception as e:
        print(f"Error fetching data from Google Sheets: {e}")
        raise

def load_data_to_db(table, df):
    """Load a pandas DataFrame into a database table."""
    try:
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
            df = fetch_google_sheet(sheet["sheet_id"], sheet["worksheet_name"], SERVICE_ACCOUNT_JSON)
            table = create_or_update_table_from_sheet(sheet["table_name"], df)
            load_data_to_db(table, df)
    finally:
        session.close()

if __name__ == "__main__":
    main()
