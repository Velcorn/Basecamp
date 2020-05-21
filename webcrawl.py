from psycopg2 import connect, Error
from sshtunnel import SSHTunnelForwarder
from config import config

with open("ssh_config.txt", "r") as f:
    lines = f.readlines()
    hostname = lines[0].strip()
    username = lines[1].strip()
    password = lines[2].strip()
    remote_bind_address = lines[3].strip()

try:
    with SSHTunnelForwarder(
        (hostname, 22),
        ssh_username=username,
        ssh_password=password,
        remote_bind_address=(remote_bind_address, 5432),
        local_bind_address=("localhost", 8080)) \
            as tunnel:

        tunnel.start()
        print("SSH connected.")

        params = config()
        conn = connect(**params)
        cursor = conn.cursor()
        print("DB connected.")

        print(conn.get_dsn_parameters(), "\n")
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")

        cursor.close()
        conn.close()
        tunnel.close()
        print("DB disconnected.")
except (Exception, Error) as error:
    print("Error while connecting to DB", error)
