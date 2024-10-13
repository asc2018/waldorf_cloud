import psycopg2
import requests
import time



def get_cik_values(cursor, conn):
    """
    Connects to the PostgreSQL database and retrieves CIK values from the central_index_key table.
    """
    try:
        
        # Execute the SQL query to fetch CIK values
        cursor.execute("SELECT cik FROM central_index_keys")
        
        # Fetch all CIK values
        ciks = cursor.fetchall()
        
        # Close the connection
        # cursor.close()
        # conn.close()
        
        # Return a list of CIK values
        return [cik[0] for cik in ciks]
    
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
        pass
        # print(f"Failed to retrieve data. HTTP Status code: {response.status_code}")


def insert_gross_profit_values(cursor, cik, json_obj):
    """
    Navigates to the 'GrossProfit' section and insert the values to the gross_profits table.
    """
    
    #sql = '''
    #    INSERT INTO gross_profits (cik, currency, start_date, end_date, value, accession_number, fiscal_year, fiscal_period, form, filed, time_frame)
    #    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    #'''

    try:
        # gross_profit = json_obj['facts']['us-gaap']['GrossProfit']['units']['USD']
        gross_profits = json_obj['facts']['us-gaap']['GrossProfit']['units']
        currencies = list(gross_profits.keys())

        print(f"Gross Profit Values:")
        
        for currency in currencies:
            
            gross_profit = gross_profits[currency]
            print("Currency:", currency)
            
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
            print(values)

            # Use executemany to insert all rows
            cursor.executemany(sql, values)
            
            print("\n")
        print("\n")

    except KeyError: 
        pass # print("GrossProfit section not found in the JSON response.")

def main():
    # Define the connection string
    conn_string = "postgresql://neondb_owner:fIwz2bZoFRq6@ep-restless-brook-a2s322z8.eu-central-1.aws.neon.tech/EDGAR?sslmode=require"


    # Connect to the database
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    # Step 1: Get CIK values from the database
    cik_values = get_cik_values(cursor, conn)
    
    if not cik_values:
        print("No CIK values retrieved.")
        return

    # Step 2: Iterate through each CIK and process the data
    for cik in cik_values:
        # print(f"\nProcessing CIK: {cik}")
        
        # Step 3: Fetch the company facts JSON for the CIK
        company_facts = fetch_company_facts(cik)
        
        if company_facts is None:
            # print(f"Failed to retrieve data for CIK: {cik}")
            continue
        
        # Step 4: Print the Gross Profit values
        # insert_gross_profit_values(cursor, cik, company_facts)
        print("company_facts:")
        print(company_facts)
        print("\n")

        # Commit the transaction
        conn.commit()

        # Delay to avoid rate limiting
        time.sleep(0.5)  # Sleep for 2 seconds between requests to handle rate limits

    # Close the connection
    # cursor.close()
    # conn.close()

# Execute the main function
if __name__ == "__main__":
    main()
