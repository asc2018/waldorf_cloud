import psycopg2
import requests
import time
import re

def get_table_facts_values(cursor, conn):
    """
    Connects to the PostgreSQL database and retrieves CIK values from the central_index_key table.
    """
    try:
        
        # Execute the SQL query to fetch CIK values
        cursor.execute("SELECT DISTINCT us_gaap_key FROM edgar_company_facts_us_gaap")
        
        # Fetch all CIK values
        values = cursor.fetchall()
        
        # Return a list of CIK values
        return [(value[0]) for value in values]
    
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return []

def camel_to_snake(name):
    # Convert camelCase to snake_case
    s1 = re.sub('([A-Z])', r'_\1', name)  # Insert underscores before capital letters
    return s1[1:].lower()  # Remove leading underscore and convert to lower case

def insert_values(conn, cursor, obj_key):
    
    # Convert the camel case table name to snake case
    table_name = camel_to_snake(obj_key)
    table_name_unique = table_name + "_unique"

    print(f"Table Name: {table_name}")
    print(f"Obj_key Name: {obj_key}")


    update_facts_us_gaap_json_obj = f'''
DO $$
DECLARE
    original_name TEXT := '{table_name}';
    valid_name TEXT;
    table_name_unique TEXT;
BEGIN
    IF LENGTH(original_name) <= 63 THEN
        valid_name := original_name;
    ELSE
        valid_name := SUBSTRING(original_name FROM 1 FOR 28) || '_' || MD5(original_name);
    END IF;

    -- Create the unique constraint name
    table_name_unique := '"' || SUBSTRING(MD5(original_name), 1, 16) || '"'; -- adjust the length as needed

    -- Insert into the facts_us_gaap_json_obj table
    INSERT INTO facts_us_gaap_json_obj (pretty_name, valid_name, obj_key)
    VALUES (original_name, valid_name, '{obj_key}');

    -- Create the new table
    EXECUTE 'CREATE TABLE ' || valid_name || ' (
        fiscal_year INTEGER,
        end_date DATE,
        value NUMERIC,
        filed DATE,
        start_date DATE,
        cik TEXT,
        time_frame TEXT,
        currency TEXT,
        accession_number TEXT,
        fiscal_period TEXT,
        form TEXT
    );';

    -- Add the unique constraint
    EXECUTE 'ALTER TABLE ' || valid_name || '  
    ADD CONSTRAINT ' || table_name_unique || ' UNIQUE (cik, currency, start_date, end_date, value, accession_number, fiscal_year, fiscal_period, form, filed, time_frame);';

END $$;
'''

    try:
        cursor.execute(update_facts_us_gaap_json_obj)
        print("\n")

    except psycopg2.Error as e:
        print(f"Database error: {e}", "\n")

def main():

    # Define the connection string
    conn_string = "postgresql://neondb_owner:fIwz2bZoFRq6@ep-restless-brook-a2s322z8-pooler.eu-central-1.aws.neon.tech/EDGAR?sslmode=require"

    # Connect to the database
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    table_facts = get_table_facts_values(cursor, conn)

    for obj in table_facts:

        obj_key = obj

        # print(f"Table Name: {obj_key}")        
        insert_values(conn, cursor, obj_key)

        # Commit the transaction
        conn.commit()

    # Close the connection
    cursor.close()
    conn.close()

# Execute the main function
if __name__ == "__main__":
    main()