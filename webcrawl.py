import os
from psycopg2 import connect, Error
from paramiko import SSHClient
from config import config

with open("ssh_config.txt", "r") as f:
    lines = f.readlines()
    username = lines[0].strip()
    password = lines[1].strip()

ssh = SSHClient()
ssh.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
ssh.connect("rzssh1.informatik.uni-hamburg.de", username=username, password=password)
print("SSH connected.")

try:
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

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
'''finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed.")'''
