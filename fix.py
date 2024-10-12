import psycopg2
import requests
import time



def insert_values(cursor, table_name):
    """
    Navigates to the 'GrossProfit' section and insert the values to the gross_profits table.
    """
    
    sql = f'''
    ALTER TABLE {table_name}
    ALTER COLUMN value TYPE NUMERIC;
    '''

    print("SQL:", sql)

    
    try:

        # Use executemany to insert all rows
        cursor.execute(sql)

        print("\n")

    except psycopg2.Error as e:
        print(f"Error altering table {table_name}: {e}\n")


def main():
    # Define the connection string
    conn_string = "postgresql://neondb_owner:fIwz2bZoFRq6@ep-restless-brook-a2s322z8-pooler.eu-central-1.aws.neon.tech/EDGAR?sslmode=require"

    # Connect to the database
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    
    cursor.execute(f"SELECT valid_name from facts_us_gaap_json_obj")
    valid_names = cursor.fetchall()

    for valid_name in valid_names:

        table_name = valid_name[0]

        print("TABLE NAME:", table_name)
        insert_values(cursor, table_name)
        
        # Commit the changes
        conn.commit()
        
    # Close the connection
    cursor.close()
    conn.close()

    print("DONE:")


# Execute the main function
if __name__ == "__main__":
    main()
