from sshtunnel import SSHTunnelForwarder
from psycopg2 import connect, Error
from config import ssh_config, db_config

categories = ["Gesundheit", "Kultur", "Netzwelt", "Panorama", "Politik", "Sport", "Wirtschaft", "Wissenschaft"]


# Transfer relevant document and comment data to new table and generate remaining data.
def create_data(category):
    print("Creating data from " + category + "...")
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

            # Search pattern for like query.
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
            for doc in documents:
                cursor.execute("select distinct c.id, doc_id, user_id, parent_comment_id, c.text, year, month, day "
                               "from comments c "
                               "join a_documents "
                               "on doc_id = %s "
                               "where user_id is not null "
                               "order by c.id asc",
                               (doc[0], ))
                comments = cursor.fetchmany(100)

                for com in comments:
                    cursor.execute("insert into a_comments "
                                   "(id, doc_id, user_id, parent_comment_id, text, year, month, day) "
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
                           (category, counts[0][0], counts[0][1]))
            connection.commit()

            # Close everything.
            cursor.close()
            connection.close()
            return "Finished creating data from " + category + ".\n"
    except (Exception, Error) as error:
        return error


def update_users():
    print("Updating users...")
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

            print("Writing users...")
            cursor.execute("select user_id, count(user_id) "
                           "from a_comments "
                           "group by user_id "
                           "order by count(user_id) desc")

            users = cursor.fetchmany(10)

            for user in users:
                cursor.execute("insert into a_users(id, comment_count) "
                               "values(%s, %s) "
                               "on conflict (id) do update "
                               "set comment_count = EXCLUDED.comment_count",
                               (user[0], user[1]))
            connection.commit()
            cursor.close()
            connection.close()
            return "Updated users." + ".\n"
    except (Exception, Error) as error:
        return error


for cat in categories:
    print(create_data(cat))
print(update_users())
