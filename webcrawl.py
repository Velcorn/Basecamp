from psycopg2 import connect, Error
from sshtunnel import SSHTunnelForwarder
from config import config


def fetch_comments():
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
            print("SSH connected.", "\n")

            params = config()
            connection = connect(**params)
            cursor = connection.cursor()
            print("DB connected.", "\n")

            print("Fetching comments...")
            cursor.execute("SELECT text "
                           "FROM public.comments "
                           "WHERE doc_id=1 and parent_comment_id IS NULL "
                           "ORDER BY id ASC;")
            comments = cursor.fetchmany(10)

            # Close everything
            cursor.close()
            connection.close()
            print("\n" + "DB disconnected." + "\n")

            return comments
    except (Exception, Error) as error:
        print("Error while connecting to DB", error)
