from concurrent.futures import ThreadPoolExecutor
import psycopg2
import json


def process_batch(offset, batch_size):

    # Define the connection string
    conn_string = "postgresql://neondb_owner:fIwz2bZoFRq6@ep-restless-brook-a2s322z8-pooler.eu-central-1.aws.neon.tech/EDGAR?sslmode=require"

    # Connect to the database
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    
    cursor.execute(f"SELECT cik, company_facts_json FROM edgar_company_facts_json LIMIT {batch_size} OFFSET {offset}")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()

    fact_keys_set = {}

    for row in rows:
        cik = row[0]
        company_facts_json = row[1]
        facts = company_facts_json.get('facts', {})
        
        us_gaap = facts.get('us-gaap', {})
        # dei = facts.get('dei', {})
        # ifrs_full = facts.get('ifrs-full', {})
        # invest = facts.get('invest', {})
        # srt = facts.get('srt', {})

        fact_keys_set[cik] = list(us_gaap.keys())

    return fact_keys_set

def insert_values(cursor, cik, table_name, values):
    """
    Navigates to the 'GrossProfit' section and insert the values to the gross_profits table.
    """
    
    sql = f'''
        INSERT INTO {table_name} (cik, us_gaap_key)
        VALUES (%s, %s)
        ON CONFLICT (cik, us_gaap_key) -- assuming these columns define uniqueness
            DO NOTHING;
    '''
    
    try:
            
        values = [
            (
                cik,
                row,
            )
                for row in values
        ]
        # print("Number Of Entries:", values)

        # Use executemany to insert all rows
        cursor.executemany(sql, values)

        print("\n")

    except KeyError: 
        pass 
        print(f'''{table_name} section not found in the JSON response.''')
        print("\n")

def main():

    batch_size = 1000
    total_rows = 18248  # Adjust this based on your table size
    offsets = range(0, total_rows, batch_size)

    all_fact_keys = [] # set()
    
    with ThreadPoolExecutor(max_workers=10) as executor:  # Adjust max_workers based on your system's CPU cores
        results = executor.map(lambda offset: process_batch(offset, batch_size), offsets)

        print("Print Executing Process Batch")
        for result in results:
            # all_fact_keys.update(result)
            all_fact_keys.append(result)

    # Define the connection string
    conn_string = "postgresql://neondb_owner:fIwz2bZoFRq6@ep-restless-brook-a2s322z8-pooler.eu-central-1.aws.neon.tech/EDGAR?sslmode=require"

    # Connect to the database
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()

    table_name = 'edgar_company_facts_us_gaap'

    for batch in all_fact_keys:
        for key, value in batch.items():
            cik = key
            values  = value

            insert_values(cursor, cik, table_name, values)

            print(key, len(value))

            # Commit the transaction
            conn.commit()

    # Close the connection
    cursor.close()
    conn.close()

# Execute the main function
if __name__ == "__main__":
    main()
