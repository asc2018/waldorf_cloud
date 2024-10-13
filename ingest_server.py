import psycopg2
import requests
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed


# Configure logging
logging.basicConfig(level=logging.ERROR, filename='error.log', 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def get_facts_us_gaap_json_obj_values(cursor, cik): #####
    """
    Connects to the PostgreSQL database and retrieves CIK values from the central_index_key table.
    """

    sql = f'''
        select 
            obj.valid_name, obj.obj_key
        from edgar_company_facts_us_gaap gp
        left join facts_us_gaap_json_obj obj on obj.obj_key=gp.us_gaap_key

        where 1=1
        and gp.cik = '{cik}';
    '''

    print("sql:", sql)

    try:
        
        # Execute the SQL query to fetch CIK values
        cursor.execute(sql)
        
        # Fetch all CIK values
        values = cursor.fetchall()
                
        # Return a list of values
        return [(value[0], value[1])  for value in values]
    
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

def insert_values(cursor, cik, json_obj, table_name, target_dictionary):
    """
    Navigates to the 'GrossProfit' section and insert the values to the gross_profits table.
    """
    
    sql = f'''
        INSERT INTO {table_name} (cik, currency, start_date, end_date, value, accession_number, fiscal_year, fiscal_period, form, filed, time_frame)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (cik, currency, start_date, end_date, value, accession_number, fiscal_year, fiscal_period, form, filed, time_frame) -- assuming these columns define uniqueness
            DO NOTHING;
    '''

    # print("SQL:", sql)

    
    try:

        gross_profits = json_obj['facts']['us-gaap'][target_dictionary]['units']
        currencies = list(gross_profits.keys())

        print(f'''{table_name} Values:''')
        
        for currency in currencies:
            
            gross_profit = gross_profits[currency]
            
            values = [
                (
                    cik,
                    currency,
                    entry.get('start', None),  
                    entry.get('end', None),
                    entry.get('val', None),
                    entry.get('accn', None),
                    entry.get('fy', None),
                    entry.get('fp', None),
                    entry.get('form', None),
                    entry.get('filed', None),
                    entry.get('frame', None),
                )
                for entry in gross_profit
            ]
            print("Number Of Entries:", values)

            # Use executemany to insert all rows
            cursor.executemany(sql, values)

            print("\n")
        print("\n")

    except KeyError: 
        pass 
        print(f'''{table_name} section not found in the JSON response.''')
        print("\n")

    except KeyError: 
        logging.warning(f"Table section not found in the JSON response for CIK {cik}.")
    except DatabaseError as e:
        logging.error(f"Database error while inserting data for CIK {cik}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error for CIK {cik}: {e}")

def process_cik(cursor, conn, cik_row):
    print("process_cik:")
    cik, json_obj = cik_row
    table_facts = get_facts_us_gaap_json_obj_values(cursor, cik)

    for table_name, target_dictionary in table_facts:
        insert_values(cursor, cik, json_obj, table_name, target_dictionary)

    # Commit after processing each CIK to avoid large transactions
    conn.commit()  # Add this line to commit more frequently

def main():
    # Define the connection string
    conn_string = "postgresql://neondb_owner:fIwz2bZoFRq6@ep-restless-brook-a2s322z8-pooler.eu-central-1.aws.neon.tech/EDGAR?sslmode=require"

    # Connect to the database
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    batch_size = 1000
    cursor.execute("SELECT COUNT(*) FROM edgar_company_facts_json")
    total_rows = cursor.fetchone()[0]
    offsets = range(0, total_rows, batch_size)

    print("offsets:", offsets)

    print("Making Company Facts:")
    
    index = 0
    ciks = []

    # Prepare a list to store the tasks
    tasks = []

    for offset in offsets:
        print("Offset:", offset)
        cursor.execute(f"SELECT cik, company_facts_json FROM edgar_company_facts_json LIMIT {batch_size} OFFSET {offset}")
        company_facts = cursor.fetchall()
        print("COMPANY FACTS:", company_facts, "\n")
        # Using ThreadPoolExecutor for parallel processing
        
        """
        with ThreadPoolExecutor(max_workers=2) as executor:
            for row in company_facts:
                print("ROW:", row)
                tasks.append(executor.submit(process_cik, cursor, conn, row))

            # Process the results as they complete
            for future in as_completed(tasks):
                try:
                    future.result()
                    index += 1
                except Exception as e:
                    print(f"Error processing CIK: {e}")
        """
        # Commit the transaction after each batch
        # conn.commit()
        print(f"Batch starting at offset {offset} committed.")

    # Close the connection
    cursor.close()
    conn.close()

    #print("INDEX:", index)
    #print("CIKs:", len(ciks))
    #print("UNIQUE CIKs:", len(set(ciks)))

# Execute the main function
if __name__ == "__main__":
    main()
