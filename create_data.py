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

            # Category search patterns for queries.
            like_pattern = '%{}%'.format("\"channel\": " + "\"" + category + "\"")
            equals_pattern = '{}'.format(category)

            cursor.execute("select distinct c.year, c.month, c.day "
                           "from comments c "
                           "join documents "
                           "on doc_id = documents.id "
                           "where user_id is not null "
                           "group by c.year, c.month, c.day")
            days = cursor.fetchall()

            print("Writing documents and comments...")
            for day in days:
                # Create a day pattern for query.
                year = str(day[0])
                month = str(day[1]) if int(day[1]) > 10 else "0" + str(day[1])
                day = str(day[2]) if int(day[2]) > 10 else "0" + str(day[2])
                day_pattern = '%{}%'.format(year + "-" + month + "-" + day)

                cursor.execute("select distinct d.id, d.url, d.title, count(comments) "
                               "from documents d "
                               "join comments "
                               "on d.id = comments.doc_id "
                               "where metadata like %s "
                               "and to_char(d.timestamp, 'YYYY-MM-DD') like %s "
                               "and comments.user_id is not null "
                               "group by d.id "
                               "order by count(comments) desc "
                               "limit 1",
                               (like_pattern, day_pattern, ))
                doc = cursor.fetchall()

                if doc:
                    cursor.execute("insert into a_documents(id, url, title, category, comment_count) "
                                   "values(%s, %s, %s, %s, %s) "
                                   "on conflict (id) do update "
                                   "set comment_count = EXCLUDED.comment_count",
                                   (doc[0][0], doc[0][1], doc[0][2], category, doc[0][3]))
                    connection.commit()

                    cursor.execute("select distinct c.id, doc_id, user_id, parent_comment_id, c.text "
                                   "from comments c "
                                   "join a_documents "
                                   "on doc_id = %s "
                                   "where user_id is not null "
                                   "and parent_comment_id is not null "
                                   "order by parent_comment_id asc "
                                   "limit 10",
                                   (doc[0][0], ))
                    answers = cursor.fetchall()

                    for ans in answers:
                        cursor.execute("insert into a_comments "
                                       "(id, doc_id, user_id, parent_comment_id, text) "
                                       "values(%s, %s, %s, %s, %s) "
                                       "on conflict do nothing",
                                       (ans[0], ans[1], ans[2], ans[3], ans[4]))
                    connection.commit()

                    cursor.execute("select distinct on "
                                   "(parent_comment_id) c.id, doc_id, user_id, parent_comment_id, c.text "
                                   "from comments c "
                                   "join a_comments "
                                   "on c.id = a_comments.parent_comment_id "
                                   "order by parent_comment_id, c.id asc "
                                   "limit 10")
                    comments = cursor.fetchall()

                    for com in comments:
                        cursor.execute("insert into a_comments "
                                       "(id, doc_id, user_id, parent_comment_id, text) "
                                       "values(%s, %s, %s, %s, %s) "
                                       "on conflict do nothing",
                                       (com[0], com[1], com[2], com[3], com[4]))
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
                           "order by count(user_id) desc "
                           "limit 10")

            users = cursor.fetchall()

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
# print(update_users())
