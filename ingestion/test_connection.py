from database import get_connection


try:
    conn = get_connection()

    print("PostgreSQL connection successful!")

    conn.close()

except Exception as e:
    print("PostgreSQL connection failed.")
    print(e)
