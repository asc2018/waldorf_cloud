import psycopg2
import requests
import time
import json


def get_cik_values(cursor, conn):
    """
    Connects to the PostgreSQL database and retrieves CIK values from the central_index_key table.
    """
    try:
        
        # Execute the SQL query to fetch CIK values
        cursor.execute("SELECT cik, company_name FROM central_index_keys")
        
        # Fetch all CIK values
        ciks = cursor.fetchall()
        
        # Close the connection
        # cursor.close()
        # conn.close()
        # print("ciks:", ciks)
        
        # Return a list of CIK values
        return [(cik[0], cik[1])  for cik in ciks]
    
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return []

def fetch_company_facts(cik):
    """
    Fetches the company facts JSON for a given CIK from the SEC API.
    """

    url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{str(cik).zfill(10)}.json"

    # Define headers including a User-Agent string
    headers = {
        'User-Agent': 'Your Name asc273@cornell.edu',
        'Accept-Encoding': 'gzip, deflate',
        'Host': 'data.sec.gov',
        'Connection': 'keep-alive',
    }

    # Send a GET request to fetch the raw data with headers
    response = requests.get(url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        # Print the content of the file
        # print(response.json())
        return response.json()
    else:
        # pass
        print(f"Failed to retrieve data. HTTP Status code: {response.status_code}")

def insert_values(cursor, cik, json_obj, table_name):
    """
    Navigates to the target section and inserts the values into the specified table.
    """

    sql = f'''
        INSERT INTO {table_name} (cik, company_facts_json, last_updated)
        VALUES (%s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (cik)
        DO UPDATE SET 
            company_facts_json = EXCLUDED.company_facts_json,
            last_updated = CURRENT_TIMESTAMP;
    '''

    print(f'''{table_name} Values:''')
    print("SQL:", sql)

    
    try:
                    
        values = (cik, json.dumps(json_obj))
        # print("Values", values)

        # Use executemany to insert all rows
        cursor.execute(sql, values)

        print("\n")

    except KeyError: 
        pass 
        print(f'''{table_name} section not found in the JSON response.''')
        print("\n")

def main():

    # Define the connection string
    conn_string = "postgresql://neondb_owner:fIwz2bZoFRq6@ep-restless-brook-a2s322z8-pooler.eu-central-1.aws.neon.tech/EDGAR?sslmode=require"


    # Connect to the database
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()


    # Step 1: Get CIK values from the database
    cik_values = get_cik_values(cursor, conn)
    
    if not cik_values:
        print("No CIK values retrieved.")
        return

    # Step 2: Iterate through each CIK and process the data
    for row in cik_values:
        
        cik = row[0]
        company_name = row[1]
        print(f"\nProcessing CIK: {cik}")
        print(f"Processing Company Name: {company_name}")
        
        # Step 3: Fetch the company facts JSON for the CIK
        company_facts = fetch_company_facts(cik)
        
        if company_facts is None:
            print(f"Failed to retrieve data for CIK: {cik}")
            continue

        # print("company_facts:", company_facts, "\n")
        table_name = 'edgar_company_facts_json'

        insert_values(cursor, cik, company_facts, table_name)

        # Commit the transaction
        conn.commit()

        # Delay to avoid rate limiting
        time.sleep(0.9)  # Sleep for 2 seconds between requests to handle rate limits

    # Close the connection
    cursor.close()
    conn.close()

# Execute the main function
if __name__ == "__main__":
    main()
