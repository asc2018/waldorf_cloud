#!/usr/bin/python


import requests
import psycopg2


"""
hard code as data sources table in DB
"""

"""
iteratate through sources and set a cron job for each new source if a new source exists
"""

# Define the connection string
conn_string = "postgresql://neondb_owner:fIwz2bZoFRq6@ep-restless-brook-a2s322z8.eu-central-1.aws.neon.tech/EDGAR?sslmode=require"


# Connect to the database
conn = psycopg2.connect(conn_string)
cursor = conn.cursor()

# Prepare the insert statement
insert_query = "INSERT INTO central_index_keys (cik, company_name) VALUES (%s, %s) ON CONFLICT (cik) DO NOTHING"



# Define the URL of the file
url = "https://www.sec.gov/Archives/edgar/cik-lookup-data.txt"


def getData(url):

    # Define headers including a User-Agent string
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    # Send a GET request to fetch the raw data with headers
    response = requests.get(url, headers=headers)

    print("response:", response)

    # Check if the request was successful
    if response.status_code == 200:
        # Print the content of the file
        # print(response.text)
        return response
    else:
        print(f"Failed to retrieve data. HTTP Status code: {response.status_code}")


# Send a GET request to fetch the raw data with headers
response = getData(url=url) # requests.get(url, headers=headers)

# Check if the request was successful
if response.status_code == 200:
    # Split the content by new lines
    lines = response.text.splitlines()

    # Iterate through each line and print it
    # Iterate through each line and process it
    for line in lines:

        print("line:", line)

        # Split the line by comma
        data = line.strip(':')

        # Define the target substring
        target = data.split(":")[-1]
        # print("target:", target)

        # Find the start position of the target substring
        start_index = line.find(target)


        # Extract the cik starting from the target string
        cik = line[start_index:start_index + len(target)]
        # print(f"Extracted substring: '{cik}'")

        # Remove the extracted substring from data
        company_name = data.replace(target, "", 1).strip(":")
        # print(f"Data after removal: '{company_name}'")

        cursor.execute(insert_query, (cik, company_name))

        # Commit the transaction and close the connection
        conn.commit()
    
    cursor.close()
    conn.close()
    print("Data insertion completed successfully.")

else:
    print(f"Failed to retrieve data. HTTP Status code: {response.status_code}")