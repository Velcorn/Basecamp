from psycopg2 import connect, Error
from config import dbconfig


def fetch_comments():
    try:
        params = dbconfig()
        connection = connect(**params)
        cursor = connection.cursor()
        print("DB connected.", "\n")

        print("Fetching comments...")
        cursor.execute("SELECT text FROM comments ORDER BY id ASC;")
        comments = cursor.fetchall()

        # Close everything
        cursor.close()
        connection.close()
        print("\n" + "DB disconnected." + "\n")

        return comments
    except (Exception, Error) as error:
        print("Error while connecting to DB:", error)
