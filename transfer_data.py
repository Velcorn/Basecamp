from sshtunnel import SSHTunnelForwarder
from psycopg2 import connect, Error
from config import ssh_config, db_config

categories = ["Gesundheit", "Job & Karriere", "Netzwelt", "Politik", "Sport", "Wirtschaft", "Wissenschaft"]


# Transfer data to new tables.
def transfer_data(category):
    print("Transferring data from " + category + "...")
    config = ssh_config()
    try:
        with SSHTunnelForwarder(
                (config["host"], 22),
                ssh_username=config["user"],
                ssh_password=config["password"],
                remote_bind_address=(config["rba"], 5432),
                local_bind_address=("localhost", 8080)) as tunnel:
            tunnel.start()
            print("SSH connected.")

            params = db_config()
            connection = connect(**params)
            cursor = connection.cursor()
            print("DB connected.")

            # Fetch data.
            # Category search term and pattern for LIKE condition.
            term = "\"channel\": " + "\"" + category + "\""
            pattern = '%{}%'.format(term)

            print("Fetching data...")
            cursor.execute("select distinct d.id, d.url, d.title "
                           "from documents d "
                           "join comments "
                           "on d.id = comments.doc_id "
                           "where metadata like %s "
                           "and comments.user_id is not null "
                           "order by id asc ",
                           (pattern, ))
            documents = cursor.fetchall()

            cursor.execute("select c.id, doc_id, user_id, parent_comment_id, c.text, year, month, day "
                           "from comments c "
                           "join documents "
                           "on c.doc_id = documents.id "
                           "where metadata like %s "
                           "and user_id is not null "
                           "order by c.id asc",
                           (pattern, ))
            comments = cursor.fetchall()

            # Write data.
            print("Writing data...")
            for doc in documents:
                cursor.execute("insert into a_documents(id, url, title, category) "
                               "values(%s, %s, %s, %s) "
                               "on conflict do nothing",
                               (doc[0], doc[1], doc[2], category))
            connection.commit()

            for com in comments:
                cursor.execute("insert into a_comments(id, doc_id, user_id, parent_comment_id, text, year, month, day) "
                               "values(%s, %s, %s, %s, %s, %s, %s, %s) "
                               "on conflict do nothing",
                               (com[0], com[1], com[2], com[3], com[4], com[5], com[6], com[7]))
            connection.commit()

            # Close everything.
            cursor.close()
            connection.close()
            print("DB disconnected.")
            return "Transferred data." + "\n"
    except (Exception, Error) as error:
        return error


for cat in categories:
    print(transfer_data(cat))
