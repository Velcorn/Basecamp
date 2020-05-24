from psycopg2 import connect, Error
from sshtunnel import SSHTunnelForwarder
from config import sshconfig, db_origin, db_target

categories = ["Netzwelt", "Wissenschaft"]


def fetch_entries(category):
    config = sshconfig()
    try:
        with SSHTunnelForwarder(
                (config["host"], 22),
                ssh_username=config["user"],
                ssh_password=config["password"],
                remote_bind_address=(config["rba"], 5432),
                local_bind_address=("localhost", 8080)) as tunnel:

            tunnel.start()
            print("SSH connected.")

            params = db_origin()
            connection = connect(**params)
            cursor = connection.cursor()
            print("Origin DB connected.")

            # Search term and pattern for LIKE condition.
            term = "\"channel\": " + "\"" + category + "\""
            pattern = '%{}%'.format(term)

            print("Fetching documents...")
            cursor.execute("SELECT id, url, title "
                           "FROM documents d "
                           "WHERE metadata LIKE %s",
                           (pattern, ))
            documents = cursor.fetchmany(100)

            print("Fetching comments...")
            cursor.execute("SELECT c.id, c.doc_id, parent_comment_id, c.text, year, month, day "
                           "FROM comments c "
                           "JOIN documents "
                           "ON c.doc_id = documents.id "
                           "WHERE metadata LIKE %s",
                           (pattern, ))
            comments = cursor.fetchmany(100)

            # Close everything
            cursor.close()
            connection.close()
            print("Origin DB disconnected.")

            return documents, comments
    except (Exception, Error) as error:
        return error


def write_entries(category):
    print("Transferring documents and comments from " + category + ":")
    documents, comments = fetch_entries(category)

    try:
        params = db_target()
        connection = connect(**params)
        cursor = connection.cursor()
        print("Target DB connected.")

        print("Writing documents...")
        for d in documents:
            cursor.execute("INSERT INTO documents(id, url, title, category) "
                           "VALUES(%s, %s, %s, %s) "
                           "ON CONFLICT DO NOTHING",
                           (d[0], d[1], d[2], category))
        connection.commit()

        print("Writing comments...")
        for c in comments:
            cursor.execute("INSERT INTO comments(id, doc_id, parent_comment_id, text, year, month, day) "
                           "VALUES(%s, %s, %s, %s, %s, %s, %s) "
                           "ON CONFLICT DO NOTHING",
                           (c[0], c[1], c[2], c[3], c[4], c[5], c[6]))
        connection.commit()

        # Close everything
        cursor.close()
        connection.close()
        print("Target DB disconnected.")
        return "Entries committed to target DB."
    except (Exception, Error) as error:
        return error


'''for category in categories:
    print(fetch_entries(category))'''

for category in categories:
    print(write_entries(category))
