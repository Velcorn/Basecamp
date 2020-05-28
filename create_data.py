from sshtunnel import SSHTunnelForwarder
from psycopg2 import connect, Error
from config import ssh_config, db_config

categories = ["Gesundheit", "Mobilität", "Netzwelt", "Panorama", "Politik", "Sport", "Wirtschaft", "Wissenschaft"]


# Transfer relevant data to new tables and fill remaining holes.
def create_data(category):
    print("Writing data for " + category + "...")
    config = ssh_config()
    try:
        with SSHTunnelForwarder(
                (config["host"], 22),
                ssh_username=config["user"],
                ssh_password=config["password"],
                remote_bind_address=(config["rba"], 5432),
                local_bind_address=("localhost", 8080)) as tunnel:
            tunnel.start()

            params = db_config()
            connection = connect(**params)
            cursor = connection.cursor()

            # Category search patterns for queries.
            like_pattern = '%{}%'.format("\"channel\": " + "\"" + category + "\"")
            equals_pattern = '{}'.format(category)

            print("Writing documents...")
            cursor.execute("select distinct d.id, d.url, d.title, count(comments) "
                           "from documents d "
                           "join comments "
                           "on d.id = comments.doc_id "
                           "where metadata like %s "
                           "and comments.user_id is not null "
                           "group by d.id "
                           "order by count(comments) desc",
                           (like_pattern, ))
            documents = cursor.fetchmany(10)

            for doc in documents:
                cursor.execute("insert into a_documents(id, url, title, category, comment_count) "
                               "values(%s, %s, %s, %s, %s) "
                               "on conflict (id) do update "
                               "set comment_count = EXCLUDED.comment_count",
                               (doc[0], doc[1], doc[2], category, doc[3]))
            connection.commit()

            print("Writing comments...")
            cursor.execute("select c.id, doc_id, user_id, parent_comment_id, c.text, year, month, day "
                           "from comments c "
                           "join a_documents "
                           "on doc_id = a_documents.id "
                           "order by c.id asc",
                           (like_pattern, ))
            comments = cursor.fetchmany(100)

            for com in comments:
                cursor.execute("insert into a_comments(id, doc_id, user_id, parent_comment_id, text, year, month, day) "
                               "values(%s, %s, %s, %s, %s, %s, %s, %s) "
                               "on conflict do nothing",
                               (com[0], com[1], com[2], com[3], com[4], com[5], com[6], com[7]))
            connection.commit()

            print("Writing categories...")
            cursor.execute("select count(id), sum(comment_count) "
                           "from a_documents d "
                           "where category = %s",
                           (equals_pattern, ))
            counts = cursor.fetchall()

            cursor.execute("insert into a_categories(name, doc_count, comment_count) " 
                           "values(%s, %s, %s) "
                           "on conflict (name) do update "
                           "set (doc_count, comment_count) = (EXCLUDED.doc_count, EXCLUDED.comment_count)",
                           (category, counts[0], counts[1]))
            connection.commit()

            print("Writing users...")

            # Close everything.
            cursor.close()
            connection.close()
            return "Finished writing data for " + category + ".\n"
    except (Exception, Error) as error:
        return error


for cat in categories:
    print(create_data(cat))
