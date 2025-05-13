import logging
import csv
import os
import configparser
import time
import datetime
import pyodbc

def load_config(config_file='config.ini'):
    """Load configuration from a file and handle potential errors."""
    config = configparser.ConfigParser()
    if not os.path.exists(config_file):
        logging.error(f"Config file '{config_file}' not found.")
        raise FileNotFoundError(f"Config file '{config_file}' not found.")
    try:
        config.read(config_file)
        return config
    except configparser.Error as e:
        logging.error(f"Error reading config file '{config_file}': {e}")
        raise

config = load_config()
# Pobieranie danych z sekcji Database
db_driver = config['Database']['driver']
db_server = config['Database']['server']
db_name = config['Database']['name']
db_user = config['Database']['user']
db_password = config['Database']['password']

# Pobieranie danych z sekcji Settings
interval_seconds = int(config['Settings']['interval_seconds'])
last_id_file = config['Settings']['last_id_file']
log_file = config['Settings']['log_file']
csv_file_path = config['Settings']['csv_file_path']

logging.basicConfig(filename=log_file, level=logging.DEBUG)


def get_connection():
    """Return a connection to the database."""
    conn = None
    try:
        conn_str = (f"DRIVER={db_driver};SERVER={db_server};DATABASE={db_name};"
                    f"UID={db_user};PWD={db_password};TrustServerCertificate=Yes;")
        conn = pyodbc.connect(conn_str)
        print(f"Successfully connected to database: {db_name}")
        return conn
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        print(f"Database connection error: {e}")
        if conn:
            conn.close()
        return None


def create_csv_if_not_exists(filename, headers):
    """Create a CSV file with headers if it does not exist."""
    if not os.path.exists(filename):
        with open(filename, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()

def read_last_id():
    """Read the last processed ID from a file."""
    if os.path.exists(last_id_file):
        with open(last_id_file, 'r') as f:
            return int(f.read().strip())
    return 0

def save_last_id(last_id):
    """Save the last processed ID to a file."""
    with open(last_id_file, 'w') as f:
        f.write(str(last_id))

def get_actions():
    """Fetch actions from the database."""
    conn = None
    try:
        conn = get_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT id, name, minmax FROM actions ORDER BY action_order")
            actions = cursor.fetchall()
            return actions
    finally:
        if conn:
            conn.close()
    return actions

def fetch_new_records(cursor, last_id, actions):
    """Fetch new records from the database and group related records."""
    cursor.execute("SELECT TOP 100 id, created_at, process_id, number, CASE WHEN SUBSTRING(CAST(status AS VARCHAR), 2, 1) = 3 THEN 'OK' ELSE 'NOK' END status, housing [housing no], pcb [pcb no], arm [arm no] FROM FinalProducts WHERE id > ? ORDER BY id ASC", last_id)
    columns = [column[0] for column in cursor.description]
    new_records = [dict(zip(columns, row)) for row in cursor.fetchall()]
    processed_records = []

    for record in new_records:
        product_id = record.get('process_id')
        record.pop('process_id')
        if product_id is not None:
            cursor.execute("SELECT action, min, max, value FROM FinalWithResults WHERE process_id = ?", product_id)
            related_columns = [column[0] for column in cursor.description]
            related_records = [dict(zip(related_columns, row)) for row in cursor.fetchall()]

            grouped_data = {}
            for action in actions:
                for rel_record in related_records:
                    if action[1] == rel_record['action']:
                        if action[2] == 1:
                            grouped_data[rel_record['action'] + " .min"] = rel_record['min']
                            grouped_data[rel_record['action']] = rel_record['value']
                            grouped_data[rel_record['action'] + " .max"] = rel_record['max']
                        else:
                            grouped_data[rel_record['action']] = rel_record['value']

            # Dodaj zgrupowane dane do głównego rekordu
            record.update(grouped_data)

        processed_records.append(record)

    return processed_records

def append_to_csv_by_month(rows, timestamp_column):
    """Append records to CSV files grouped by month."""
    grouped = {}

    for row in rows:
        record_ts = row[timestamp_column]
        if isinstance(record_ts, str):
            record_ts = datetime.datetime.fromisoformat(record_ts)

        filename = f"{csv_file_path}Wyroby_gotowe_{record_ts.strftime('%m-%Y')}.csv"
        if filename not in grouped:
            grouped[filename] = []

        grouped[filename].append(row)

    for filename, records in grouped.items():
        headers = records[0].keys()
        create_csv_if_not_exists(filename, headers)
        with open(filename, mode='a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writerows(records)

def main_loop():
    conn = None
    cursor = None
    try:
        conn = get_connection()
        if conn:
            cursor = conn.cursor()
            actions = get_actions()

            while True:
                try:
                    last_id = read_last_id()
                    rows = fetch_new_records(cursor, last_id, actions)
                    if rows:
                        append_to_csv_by_month(rows, timestamp_column="created_at")
                        save_last_id(rows[-1]["id"])
                        print(f"[{datetime.datetime.now()}] Added {len(rows)} products.")
                        # logging.info(f"[{datetime.datetime.now()}] Added {len(rows)} products.")
                    else:
                        print(f"[{datetime.datetime.now()}] New products not found.")
                        # logging.info(f"[{datetime.datetime.now()}] New products not found.")

                except Exception as e:
                    print(f"ERROR: Some error occurred. Please check the log file.")
                    logging.error(f"{e}")

                time.sleep(interval_seconds)
    except Exception as e:
        print(f"ERROR: Main loop error: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main_loop()
