from psycopg2 import connect, Error
from sshtunnel import SSHTunnelForwarder
from config import dbconfig, sshconfig


def fetch_comments():
    config = sshconfig()
    try:
        with SSHTunnelForwarder(
                (config["host"], 22),
                ssh_username=config["user"],
                ssh_password=config["password"],
                remote_bind_address=(config["rba"], 5432),
                local_bind_address=("localhost", 8080)) as tunnel:

            tunnel.start()
            print("SSH connected.", "\n")

            params = dbconfig()
            connection = connect(**params)
            cursor = connection.cursor()
            print("DB connected.", "\n")

            print("Fetching comments...")
            cursor.execute("SELECT text "
                           "FROM public.comments "
                           "WHERE doc_id=1 and parent_comment_id IS NULL"
                           "ORDER BY id ASC;")
            comments = cursor.fetchmany(10)

            # Close everything
            cursor.close()
            connection.close()
            print("\n" + "DB disconnected." + "\n")

            return comments
    except (Exception, Error) as error:
        print("Error while connecting to DB:", error)
