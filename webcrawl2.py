from psycopg2 import connect
from sshtunnel import SSHTunnelForwarder
from config import config

with open("ssh_config.txt", "r") as f:
    lines = f.readlines()
    hostname = lines[0].strip()
    username = lines[1].strip()
    password = lines[2].strip()


try:
    with SSHTunnelForwarder(
            (hostname, 22),
            ssh_username=username,
            ssh_password=password,
            remote_bind_address=("localhost", 5432)) as tunnel:

        tunnel.start()
        print("SSH connected.")

        params = config()
        conn = connect(**params)
        cursor = conn.cursor()
        print("DB connected.")
        # Print PostgreSQL connection properties.
        print(conn.get_dsn_parameters(), "\n")

        # Print PostgreSQL version.
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")
except:
    print("Connection failed.")
