import psycopg2
from sshtunnel import SSHTunnelForwarder
from config import config

try:
    with SSHTunnelForwarder(
            ("rzssh1.informatik.uni-hamburg.de", 22),
            ssh_username="6willrut",
            ssh_password="7TuB8binGL",
            remote_bind_address=("basecamp-bigdata.informatik.uni-hamburg.de", 5432)) as tunnel:

        tunnel.start()
        print("SSH tunnel connected.")

        params = config()
        connection = psycopg2.connect(**params)
        cursor = connection.cursor()
        print("Connected to DB.")
        # Print PostgreSQL Connection properties
        print(connection.get_dsn_parameters(), "\n")

        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")

except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)
'''finally:
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")'''
