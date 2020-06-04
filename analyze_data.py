from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_watson import LanguageTranslatorV3, ToneAnalyzerV3, PersonalityInsightsV3
from sshtunnel import SSHTunnelForwarder
from psycopg2 import connect, Error
from config import api_config, ssh_config, db_config
from re import compile, UNICODE
from json import dumps


# Set up Tone Analyzer and Translator from Watson
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
                           "where translation is null or tone is null "
                           "order by id asc")
            comments = cursor.fetchall()

            print("Generating analysis and writing results to DB...")
            # Translate each comment, generate tone analysis and write it to the DB.
            for com in comments:
                # Remove emojis.
                text = EMOJI.sub(u'', com[1])

                # Translate comment and analyze the tone.
                translation = translator.translate(text, model_id='de-en').get_result()['translations'][0]['translation']
                tone_analysis = tone_analyzer.tone({'text': translation}, content_type='text/plain').get_result()

                # Write tone analysis to sorted dict.
                tones = {}
                for tone in tone_analysis['document_tone']['tones']:
                    tones[tone['tone_name']] = tone['score']
                tones = dict(sorted(tones.items()))

                cursor.execute("update a_comments "
                               "set translation = %s, tone = %s "
                               "where id = %s",
                               (translation, dumps(tones), com[0]))
                connection.commit()

            cursor.close()
            connection.close()
            return "Finished writing results to DB.\n"
    except (Exception, Error) as error:
        return error
    finally:
        if connection:
            cursor.close()
            connection.close()


# Calculates the average tone for documents, categories and users.
def calc_average_tone():
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
            print("Calculating document tones...")
            cursor.execute("select d.id "
                           "from a_documents d "
                           "order by d.id")
            documents = cursor.fetchall()

            for doc in documents:
                cursor.execute("select tone "
                               "from a_comments c "
                               "where c.doc_id = %s "
                               "and c.parent_comment_id is not null ",
                               (doc[0], ))
                comment_tones = cursor.fetchall()
                comment_tone_list = dict_to_list(comment_tones)
                average_comment_tone = list_average(comment_tone_list)

                cursor.execute("update a_documents "
                               "set comment_tone = %s "
                               "where id = %s",
                               (dumps(average_comment_tone), doc[0]))
                connection.commit()

                cursor.execute("select tone "
                               "from a_comments c "
                               "where c.doc_id = %s "
                               "and c.parent_comment_id is null",
                               (doc[0],))
                answer_tones = cursor.fetchall()
                answer_tone_list = dict_to_list(answer_tones)
                average_answer_tone = list_average(answer_tone_list)

                cursor.execute("update a_documents "
                               "set answer_tone = %s "
                               "where id = %s",
                               (dumps(average_answer_tone), doc[0]))
                connection.commit()

            # Create a list of all tones from all documents from a category,
            # calculate their average and write it to the DB.
            print("Calculating category tones...")
            cursor.execute("select c.name "
                           "from a_categories c "
                           "order by c.name")
            categories = cursor.fetchall()

            for cat in categories:
                cursor.execute("select comment_tone "
                               "from a_documents d "
                               "where d.category = %s",
                               (cat[0], ))
                comment_tones = cursor.fetchall()
                comment_tone_list = dict_to_list(comment_tones)
                average_comment_tone = list_average(comment_tone_list)

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
                average_answer_tone = list_average(answer_tone_list)

                cursor.execute("update a_categories "
                               "set answer_tone = %s "
                               "where name = %s",
                               (dumps(average_answer_tone), cat[0]))
                connection.commit()

            # Create a list of all tones from all comments from a user,
            # calculate their average and write it to the DB.
            print("Calculating user tones...")
            cursor.execute("select id "
                           "from a_users "
                           "order by id")
            users = cursor.fetchall()

            for user in users:
                cursor.execute("select tone "
                               "from a_comments c "
                               "where c.user_id = %s",
                               (user[0], ))
                comment_tones = cursor.fetchall()
                comment_tone_list = dict_to_list(comment_tones)
                average_comment_tone = list_average(comment_tone_list)

                cursor.execute("update a_users "
                               "set comment_tone = %s "
                               "where id = %s",
                               (dumps(average_comment_tone), user[0]))
                connection.commit()

            cursor.close()
            connection.close()
            return "Calculated tones.\n"
    except (Exception, Error) as error:
        return error
    finally:
        if connection:
            cursor.close()
            connection.close()


# Analyze the personality of users and write results to DB.
def analyze_personality():
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
            cursor.execute("select id from a_users "
                           "where personality is null "
                           "order by id")
            users = cursor.fetchall()

            # Get translations of all comments for each user and combine them.
            for user in users:
                cursor.execute("select distinct translation from a_comments "
                               "join a_users on user_id = %s",
                               (user[0], ))
                translations = cursor.fetchall()
                text = []
                for trans in translations:
                    text.append(trans[0].replace("\n", " "))

                pers_insight = pers_analyzer.profile({'text': text}, content_type='text/plain',
                                                     accept='application/json').get_result()

                personality = {}
                for trait in pers_insight['personality']:
                    if trait['name'] == "Emotional range":
                        personality['Neuroticism'] = round(trait['percentile'], 6)
                    personality[trait['name']] = round(trait['percentile'], 6)

                cursor.execute("update a_users "
                               "set personality = %s "
                               "where id = %s",
                               (dumps(personality), user[0]))
                connection.commit()

            cursor.close()
            connection.close()
            return "Finished writing results to DB.\n"
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
def list_average(lst):
    average = {}
    for key, value in lst:
        average.setdefault(key, []).append(value)
    for key, value in average.items():
        average[key] = round(sum(value) / len(value), 6)
    return average


# print(analyze_tone())
# print(calc_average_tone())
print(analyze_personality())
