from sshtunnel import SSHTunnelForwarder
from psycopg2 import connect, Error
from config import ssh_config, db_config

# Categories to filter for.
categories = ["Gesundheit", "Kultur", "Netzwelt", "Panorama", "Politik", "Sport", "Wirtschaft", "Wissenschaft"]


# Transfer relevant document and comment data to new tables and generate category and user tables.
def create_data():
    # Connect to the server using SSH tunnel forwarding.
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

            # Get all days with document and/or comment data from the DB.
            cursor.execute("select distinct c.year, c.month, c.day "
                           "from comments c "
                           "join documents "
                           "on doc_id = documents.id "
                           "where user_id is not null "
                           "group by c.year, c.month, c.day")
            days = cursor.fetchall()

            for category in categories:
                print(f"Creating data from {category}...")

                # Category search patterns for queries.
                like_pattern = f"%\"channel\": \"{category}\"%"
                equals_pattern = f"{category}"

                print("Writing documents and comments...")
                for day in days:
                    # Create a day pattern for query.
                    year = str(day[0])
                    month = str(day[1]) if int(day[1]) > 9 else "0" + str(day[1])
                    day = str(day[2]) if int(day[2]) > 9 else "0" + str(day[2])
                    day_pattern = f"%{year + '-' + month + '-' + day}%"

                    # Get one document and its comment count for the day.
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

                    # If a document exists on the day, write it to the DB,
                    # grab the 10 longest answer comments and their parent comments from it and write them to the DB.
                    # Finally, add the comment count to the document.
                    if doc:
                        cursor.execute("insert into a_documents(id, url, title, category) "
                                       "values(%s, %s, %s, %s) "
                                       "on conflict do nothing",
                                       (doc[0][0], doc[0][1], doc[0][2], category))
                        connection.commit()

                        cursor.execute("select distinct on "
                                       "(parent_comment_id) c.id, doc_id, user_id, parent_comment_id, c.text "
                                       "from comments c "
                                       "join a_documents "
                                       "on doc_id = %s "
                                       "where user_id is not null "
                                       "and c.text is not null "
                                       "and parent_comment_id is not null "
                                       "and (select parent_comment_id "
                                       "from comments pc where id = c.parent_comment_id) is null "
                                       "and (select pc.text from comments pc "
                                       "where id = c.parent_comment_id) is not null "
                                       "and length(c.text) >= 100 "
                                       "order by parent_comment_id, length(c.text), c.id asc "
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

                        cursor.execute("select c.id, c.doc_id, c.user_id, c.parent_comment_id, c.text "
                                       "from comments c "
                                       "join a_comments "
                                       "on c.id = a_comments.parent_comment_id "
                                       "where c.doc_id = %s "
                                       "order by length(c.text) "
                                       "limit 10",
                                       (doc[0][0], ))
                        comments = cursor.fetchall()

                        for com in comments:
                            cursor.execute("insert into a_comments "
                                           "(id, doc_id, user_id, parent_comment_id, text) "
                                           "values(%s, %s, %s, %s, %s) "
                                           "on conflict do nothing",
                                           (com[0], com[1], com[2], com[3], com[4]))
                        connection.commit()

                        cursor.execute("select count(c) "
                                       "from a_comments c "
                                       "where c.doc_id = %s",
                                       (doc[0][0], ))
                        comment_count = cursor.fetchall()

                        cursor.execute("insert into a_documents(id, comment_count) "
                                       "values(%s, %s) "
                                       "on conflict (id) do update "
                                       "set comment_count = EXCLUDED.comment_count",
                                       (doc[0][0], comment_count[0][0]))

                print("Writing category data...")
                # Write categories with the amount of documents and comments to the DB.
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
                print(f"Finished creating data from {category}.\n")

            print("Writing users...")
            # Get the top 20 commenting users, their 10 longest comments and answers each and write them to the DB.
            cursor.execute("select user_id "
                           "from comments "
                           "group by user_id "
                           "order by count(user_id) desc "
                           "limit 20")
            users = cursor.fetchall()

            count = 1
            for user in users:
                print(f"{count}/{len(users)}...")
                cursor.execute("insert into a_users(id) "
                               "values(%s) "
                               "on conflict (id) do nothing",
                               (user[0],))
                connection.commit()

                cursor.execute("select id, doc_id, user_id, parent_comment_id, text "
                               "from comments "
                               "where user_id = %s "
                               "and parent_comment_id is null "
                               "order by length(text) desc "
                               "limit 10",
                               (user[0],))
                comments = cursor.fetchall()

                for com in comments:
                    cursor.execute("insert into a_comments(id, doc_id, user_id, parent_comment_id, text) "
                                   "values(%s, %s, %s, %s, %s) "
                                   "on conflict (id) do nothing",
                                   (com[0], com[1], com[2], com[3], com[4]))
                    connection.commit()

                cursor.execute("select id, doc_id, user_id, parent_comment_id, text from comments "
                               "where user_id = %s "
                               "and parent_comment_id is not null "
                               "and (select parent_comment_id "
                               "from comments pc where id = parent_comment_id) is null "
                               "order by length(text) desc "
                               "limit 10",
                               (user[0],))
                answers = cursor.fetchall()

                for ans in answers:
                    cursor.execute("insert into a_comments(id, doc_id, user_id, parent_comment_id, text) "
                                   "values(%s, %s, %s, %s, %s) "
                                   "on conflict (id) do nothing",
                                   (ans[0], ans[1], ans[2], ans[3], ans[4]))
                    connection.commit()

                    cursor.execute("select id, doc_id, user_id, parent_comment_id, text "
                                   "from comments "
                                   "where id = %s",
                                   (ans[3],))
                    pc = cursor.fetchall()

                    cursor.execute("insert into a_comments(id, doc_id, user_id, parent_comment_id, text) "
                                   "values(%s, %s, %s, %s, %s) "
                                   "on conflict (id) do nothing",
                                   (pc[0][0], pc[0][1], pc[0][2], pc[0][3], pc[0][4]))
                    connection.commit()
                count += 1
            print("Finished writing users.\n")

            # Close everything.
            cursor.close()
            connection.close()
            return "Finished creating data.\n"
    except (Exception, Error) as error:
        return error
    finally:
        if connection:
            cursor.close()
            connection.close()
