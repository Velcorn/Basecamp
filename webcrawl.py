import psycopg2
import paramiko
import os
from config import config

ssh = paramiko.SSHClient()
ssh.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
ssh.connect("rzssh1.informatik.uni-hamburg.de", username="6willrut", password="7TuB8binGL")
print("SSH connected.")

try:
    params = config()
    conn = psycopg2.connect(**params)
    cursor = conn.cursor()
    print("Connected to DB.")
    # Print PostgreSQL connection properties.
    print(conn.get_dsn_parameters(), "\n")

    # Print PostgreSQL version.
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record, "\n")

except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)
'''finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection is closed.")'''
