from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_watson import LanguageTranslatorV3
from ibm_watson import ToneAnalyzerV3
from googletrans import Translator
from sshtunnel import SSHTunnelForwarder
from psycopg2 import connect, Error
from config import ssh_config, db_config
from re import compile, UNICODE


# Set up Tone Analyzer and Translator from Watson
API_KEY_TL = '2Q5-kJJN8MqVEgGxf5VM2Dv063cL7r5VTp44IcreG3EN'
API_KEY_TA = '1zk1LfROS3ccoX0iEOBomvK6euIpbJSkp9K-wu2IzS_A'
URL_TL = 'https://api.eu-de.language-translator.watson.cloud.ibm.com/instances/371a365a-dafb-4d40-8f7a-0da85693ee4a'
URL_TA = 'https://api.eu-de.tone-analyzer.watson.cloud.ibm.com/instances/d0f39694-bd5f-4d32-900c-e689a9109a31'
VERSION = '2020-05-08'

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

translator.set_service_url(URL_TL)
tone_analyzer.set_service_url(URL_TA)

# Temporarily using googletrans to avoid using up quota from Watson Translator.
googletrans = Translator()

# Remove emojis.
EMOJI = compile(u"[^\U00000000-\U0000d7ff\U0000e000-\U0000ffff]", flags=UNICODE)


# Fetch comments from DB.
def fetch_comments():
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

            print("Fetching comments...")
            cursor.execute("select text "
                           "from comments "
                           "where translation is null "
                           "order by id asc;")
            comments = cursor.fetchmany(1)

            cursor.close()
            connection.close()
            print("DB disconnected.")
            return comments
    except (Exception, Error) as error:
        return error


# Analyze the tone of comments from the DB and write results to it.
def analyze():
    # Get comments from the DB.
    comments = fetch_comments()

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

        print("Generating analysis and writing results to DB...")
        # Translate each comment and generate tone analysis.
        for comment in comments:
            comment = EMOJI.sub(u'', comment[0])
            # Translate comment and analyze the tone.
            # translation = translator.translate(comment, model_id='de-en').get_result()['translations'][0]['translation']
            translation = googletrans.translate(comment)
            '''tone_analysis = tone_analyzer.tone({'text': translation}, content_type='text/plain').get_result()
            analysis = []
            for tone in tone_analysis['document_tone']['tones']:
                analysis.append(tone['tone_name'] + ": " + str(tone['score']))'''

            # Write translation and analysis to DB.
            cursor.execute("update comments "
                           "set translation=(%s) "
                           "where id=(select id from comments where translation is null order by id asc limit 1);",
                           (translation, ))

            '''cursor.execute("update comments "
                           "set tone=(%s) "
                           "where id=(select id from comments where tone is null order by id asc limit 1);",
                           (analysis, ))'''
        connection.commit()

        cursor.close()
        connection.close()
        print("DB disconnected.")
        return "Committed entries to DB."
    except (Exception, Error) as error:
        return error


print(analyze())
