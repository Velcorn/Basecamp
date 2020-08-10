from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_watson import LanguageTranslatorV3, ToneAnalyzerV3, PersonalityInsightsV3
from sshtunnel import SSHTunnelForwarder
from psycopg2 import connect, Error
from config import api_config, ssh_config, db_config
from re import compile, UNICODE
from json import dumps


# Set up Translator, Tone Analyzer and Personality Insights from Watson.
api = api_config()
API_KEY_TL = api["key_tl"]
API_KEY_TA = api["key_ta"]
API_KEY_PI = api["key_pi"]
URL_TL = api["url_tl"]
URL_TA = api["url_ta"]
URL_PI = api["url_pi"]
VERSION = api["version"]

authenticator_tl = IAMAuthenticator(API_KEY_TL)
translator = LanguageTranslatorV3(
    version=VERSION,
    authenticator=authenticator_tl
)

authenticator_ta = IAMAuthenticator(API_KEY_TA)
tone_analyzer = ToneAnalyzerV3(
    version=VERSION,
    authenticator=authenticator_ta
)

authenticator_pi = IAMAuthenticator(API_KEY_PI)
pers_analyzer = PersonalityInsightsV3(
    version=VERSION,
    authenticator=authenticator_pi
)

translator.set_service_url(URL_TL)
tone_analyzer.set_service_url(URL_TA)
pers_analyzer.set_service_url(URL_PI)

# Emoji regex.
EMOJI = compile(u"[^\U00000000-\U0000d7ff\U0000e000-\U0000ffff]", flags=UNICODE)


# Analyze the tone of comments from the DB and write results to it.
def analyze_tone():
    ssh = ssh_config()
    try:
        with SSHTunnelForwarder(
                (ssh["host"], 22),
                ssh_username=ssh["user"],
                ssh_password=ssh["password"],
                remote_bind_address=(ssh["rba"], 5432),
                local_bind_address=("localhost", 8080)) as tunnel:
            tunnel.start()

            params = db_config()
            connection = connect(**params)
            cursor = connection.cursor()

            cursor.execute("select id, text "
                           "from a_comments "
                           "where translation is null or tone is null")
            comments = cursor.fetchall()

            print("Generating tone analyses and writing results to DB...")
            # Translate each comment, generate tone analysis and write both to the DB.
            count = 1
            for com in comments:
                if count % 10 == 0:
                    print(str(count) + "/" + str(len(comments)) + "...")

                # Remove emojis.
                text = EMOJI.sub(u"", com[1])

                # Translate comment and analyze the tone.
                translation = translator.translate(text,
                                                   model_id="de-en").get_result()["translations"][0]["translation"]
                tone_analysis = tone_analyzer.tone({"text": translation},
                                                   content_type="text/plain").get_result()["document_tone"]["tones"]

                # Convert tone analysis to sorted dict.
                tones = {}
                for tone in tone_analysis:
                    tones[tone["tone_name"]] = tone["score"]
                tones = dict(sorted(tones.items()))

                cursor.execute("update a_comments "
                               "set translation = %s, tone = %s "
                               "where id = %s",
                               (translation, dumps(tones), com[0]))
                connection.commit()
                count += 1

            cursor.close()
            connection.close()
            return "Finished writing tone analyses to DB.\n"
    except (Exception, Error) as error:
        return error
    finally:
        if connection:
            cursor.close()
            connection.close()


# Analyze the personality of users and write results to the DB.
def analyze_pers():
    ssh = ssh_config()
    try:
        with SSHTunnelForwarder(
                (ssh["host"], 22),
                ssh_username=ssh["user"],
                ssh_password=ssh["password"],
                remote_bind_address=(ssh["rba"], 5432),
                local_bind_address=("localhost", 8080)) as tunnel:
            tunnel.start()

            params = db_config()
            connection = connect(**params)
            cursor = connection.cursor()

            print("Generating personality insights and writing results to DB...")
            # Get users without personality insights.
            cursor.execute("select id from a_users "
                           "where personality is null")
            users = cursor.fetchall()

            # Get translations of all comments for each user, combine them and analyze the resulting text.
            for user in users:
                cursor.execute("select distinct translation "
                               "from a_comments "
                               "join a_users on user_id = %s",
                               (user[0], ))
                translations = cursor.fetchall()
                text = []
                for trans in translations:
                    text.append(trans[0].replace("\n", " "))

                pers_insight = pers_analyzer.profile({"text": text}, content_type="text/plain",
                                                     accept="application/json").get_result()

                personality = {}
                for trait in pers_insight["personality"]:
                    if trait["name"] == "Emotional range":
                        personality["Neuroticism"] = round(trait["percentile"], 6)
                    else:
                        personality[trait["name"]] = round(trait["percentile"], 6)

                cursor.execute("update a_users "
                               "set personality = %s "
                               "where id = %s",
                               (dumps(personality), user[0]))
                connection.commit()

            cursor.close()
            connection.close()
            return "Finished writing personality insights to DB.\n"
    except (Exception, Error) as error:
        return error
    finally:
        if connection:
            cursor.close()
            connection.close()


# Converts a dict to a list.
def dict_to_list(dct):
    lst = []
    for item in dct:
        if item[0] != {}:
            for key, value in item[0].items():
                lst.append([key, float(value)])
    return sorted(lst, key=lambda x: x[0])


# Calculates the average of elements in a list.
def list_average(lst, length):
    average = {}
    for key, value in lst:
        average.setdefault(key, []).append(value)
    for key, value in average.items():
        average[key] = round(sum(value) / length, 6)
    return average


# Calculate the average tone for documents, categories and users and write it to the DB.
def calc_averages():
    ssh = ssh_config()
    try:
        with SSHTunnelForwarder(
                (ssh["host"], 22),
                ssh_username=ssh["user"],
                ssh_password=ssh["password"],
                remote_bind_address=(ssh["rba"], 5432),
                local_bind_address=("localhost", 8080)) as tunnel:
            tunnel.start()

            params = db_config()
            connection = connect(**params)
            cursor = connection.cursor()

            # Create a list of all tones from all comments/answers from a document,
            # calculate their average and write it to the DB.
            print("Calculating average and writing results to DB...")
            print("Tone for documents...")
            cursor.execute("select id "
                           "from a_documents")
            documents = cursor.fetchall()

            count = 1
            for doc in documents:
                if count % 10 == 0:
                    print(f"{count}/{len(documents)}")

                cursor.execute("select tone "
                               "from a_comments c "
                               "where c.doc_id = %s "
                               "and c.parent_comment_id is null ",
                               (doc[0], ))
                comment_tones = cursor.fetchall()
                comment_tone_list = dict_to_list(comment_tones)
                length = len(comment_tones)
                average_comment_tone = list_average(comment_tone_list, length)

                cursor.execute("update a_documents "
                               "set comment_tone = %s "
                               "where id = %s",
                               (dumps(average_comment_tone), doc[0]))
                connection.commit()

                cursor.execute("select tone "
                               "from a_comments c "
                               "where c.doc_id = %s "
                               "and c.parent_comment_id is not null",
                               (doc[0],))
                answer_tones = cursor.fetchall()
                answer_tone_list = dict_to_list(answer_tones)
                length = len(answer_tones)
                average_answer_tone = list_average(answer_tone_list, length)

                cursor.execute("update a_documents "
                               "set answer_tone = %s "
                               "where id = %s",
                               (dumps(average_answer_tone), doc[0]))
                connection.commit()
                count += 1

            # Create a list of all tones from all documents from a category,
            # calculate their average and write it to the DB.
            print("\nTone for categories...")
            cursor.execute("select name "
                           "from a_categories")
            categories = cursor.fetchall()

            count = 1
            for cat in categories:
                print(f"{count}/{len(categories)}")

                cursor.execute("select comment_tone "
                               "from a_documents "
                               "where category = %s",
                               (cat[0], ))
                comment_tones = cursor.fetchall()
                comment_tone_list = dict_to_list(comment_tones)
                length = len(comment_tones)
                average_comment_tone = list_average(comment_tone_list, length)

                cursor.execute("update a_categories "
                               "set comment_tone = %s "
                               "where name = %s",
                               (dumps(average_comment_tone), cat[0]))
                connection.commit()

                cursor.execute("select answer_tone "
                               "from a_documents d "
                               "where d.category = %s",
                               (cat[0],))
                answer_tones = cursor.fetchall()
                answer_tone_list = dict_to_list(answer_tones)
                length = len(answer_tones)
                average_answer_tone = list_average(answer_tone_list, length)

                cursor.execute("update a_categories "
                               "set answer_tone = %s "
                               "where name = %s",
                               (dumps(average_answer_tone), cat[0]))
                connection.commit()
                count += 1

            # Create a list of all tones from all comments from a user,
            # calculate their average and write it to the DB.
            print("\nTone for users...")
            cursor.execute("select id "
                           "from a_users")
            users = cursor.fetchall()

            count = 1
            for user in users:
                print(f"{count}/{len(users)}")
                cursor.execute("select tone "
                               "from a_comments "
                               "where user_id = %s "
                               "and parent_comment_id is null "
                               "order by length(text) desc "
                               "limit 10",
                               (user[0],))
                comment_tones = cursor.fetchall()
                comment_tone_list = dict_to_list(comment_tones)
                length = len(comment_tones)
                average_comment_tone = list_average(comment_tone_list, length)

                cursor.execute("update a_users "
                               "set comment_tone = %s "
                               "where id = %s",
                               (dumps(average_comment_tone), user[0]))
                connection.commit()

                cursor.execute("select tone "
                               "from a_comments "
                               "where user_id = %s "
                               "and parent_comment_id is not null "
                               "order by length(text) desc "
                               "limit 10",
                               (user[0],))
                answer_tones = cursor.fetchall()
                answer_tone_list = dict_to_list(answer_tones)
                length = len(answer_tones)
                average_answer_tone = list_average(answer_tone_list, length)

                cursor.execute("update a_users "
                               "set answer_tone = %s "
                               "where id = %s",
                               (dumps(average_answer_tone), user[0]))
                connection.commit()
                count += 1

            # Calculate averages for all categories, comments and user comments/personality insights
            # and write them to the DB.
            print("\nTone for all comments...")
            cursor.execute("select comment_tone "
                           "from a_categories")
            comment_tones = cursor.fetchall()
            comment_tone_list = dict_to_list(comment_tones)
            length = len(comment_tones)
            average_comment_tone = list_average(comment_tone_list, length)

            cursor.execute("select answer_tone "
                           "from a_categories")
            answer_tones = cursor.fetchall()
            answer_tone_list = dict_to_list(answer_tones)
            length = len(answer_tones)
            average_answer_tone = list_average(answer_tone_list, length)

            cursor.execute("insert into a_averages(name, comment_tone, answer_tone) "
                           "values(%s, %s, %s) "
                           "on conflict (name) do update "
                           "set (comment_tone, answer_tone) = (EXCLUDED.comment_tone, EXCLUDED.answer_tone)",
                           ("Average Tone Overall", dumps(average_comment_tone), dumps(average_answer_tone)))
            connection.commit()

            print("Tone and personality for all users...")
            cursor.execute("select comment_tone "
                           "from a_users")
            comment_tones = cursor.fetchall()
            comment_tone_list = dict_to_list(comment_tones)
            length = len(comment_tones)
            average_comment_tone = list_average(comment_tone_list, length)

            cursor.execute("select answer_tone "
                           "from a_users")
            answer_tones = cursor.fetchall()
            answer_tone_list = dict_to_list(answer_tones)
            length = len(answer_tones)
            average_answer_tone = list_average(answer_tone_list, length)

            cursor.execute("select personality "
                           "from a_users")
            insights = cursor.fetchall()
            insights_list = []
            for i in insights:
                for key, value in i[0].items():
                    insights_list.append([key, float(value)])
            length = len(insights)
            average_insights = list_average(insights_list, length)

            cursor.execute("insert into a_averages(name, comment_tone, answer_tone, personality) "
                           "values(%s, %s, %s, %s) "
                           "on conflict (name) do update "
                           "set (comment_tone, answer_tone, personality) = "
                           "(EXCLUDED.comment_tone, EXCLUDED.answer_tone, EXCLUDED.personality)",
                           ("Average TP Users", dumps(average_comment_tone),
                            dumps(average_answer_tone), dumps(average_insights)))
            connection.commit()
            cursor.close()
            connection.close()
            return "\nFinished writing averages to DB."
    except (Exception, Error) as error:
        return error
    finally:
        if connection:
            cursor.close()
            connection.close()
