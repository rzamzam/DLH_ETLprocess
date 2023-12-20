import psycopg2

def insert_new_territory(conn, territory_id, territory_name):
    try:
        # Create a cursor
        cursor = conn.cursor()

        # Insert new values into dimterritory
        cursor.execute("INSERT INTO dimterritory (territoryid, name) VALUES (%s, %s)", (territory_id, territory_name))

        # Commit the changes
        conn.commit()

        print("New territory added successfully!")

    except Exception as e:
        print(f"Error: {str(e)}")

    finally:
        # Close the cursor
        cursor.close()

# Example usage
postgres_conn_string = "dbname=staging2 user=postgres password= host=localhost"

# Connect to PostgreSQL
conn = psycopg2.connect(postgres_conn_string)

# Insert new territory
insert_new_territory(conn, 12, "Surabaya")

# Close the connection
conn.close()
