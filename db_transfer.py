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
            print("DB connected.")

            print("Fetching documents...")
            # print("'%\"channel\": " + "\"" + category + "\"%'")
            cursor.execute("SELECT id, url, title "
                           "FROM documents d "
                           "WHERE metadata LIKE :category",
                           {"category": "'%\"channel\": " + "\"" + category + "\"%'"})
            '''cursor.execute("SELECT id, url, title "
                           "FROM documents d "
                           "WHERE metadata LIKE '%\"channel\": \"Netzwelt\"%'")'''
            documents = cursor.fetchmany(10)

            '''print("Fetching comments...")
            cursor.execute("SELECT c.id, c.doc_id, parent_comment_id, c.\"text\", \"year\", \"month\", \"day\" "
                           "FROM \"comments\" c "
                           "JOIN documents ON documents.id = c.doc_id"
                           "WHERE metadata LIKE '%\"channel\": \"Netzwelt\"%'")
            comments = cursor.fetchmany(10)
            print(comments)'''

            # Close everything
            cursor.close()
            connection.close()
            print("DB disconnected.")

            return documents  # , comments
    except (Exception, Error) as error:
        return error


def write_entries(category):
    documents = fetch_entries(category)

    try:
        params = db_target()
        connection = connect(**params)
        cursor = connection.cursor()
        print("DB connected.")

        print("Writing documents...")
        for doc in documents:
            cursor.execute("INSERT INTO documents(id, url, title, category) "
                           "VALUES(%s, %s, %s, %s) "
                           "ON CONFLICT DO NOTHING",
                           (doc[0], doc[1], doc[2], category))
        connection.commit()

        # Close everything
        cursor.close()
        connection.close()
        print("DB disconnected.")

        return "Entries committed to DB."
    except (Exception, Error) as error:
        return error


for category in categories:
    print(fetch_entries(category))

'''for category in categories:
    print(write_entries(category))'''
