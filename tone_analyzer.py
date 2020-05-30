from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_watson import LanguageTranslatorV3
from ibm_watson import ToneAnalyzerV3
from googletrans import Translator
from sshtunnel import SSHTunnelForwarder
from psycopg2 import connect, Error
from config import api_config, ssh_config, db_config
from re import compile, UNICODE


# Set up Tone Analyzer and Translator from Watson
api = api_config()
API_KEY_TL = api["key_tl"]
API_KEY_TA = api["key_ta"]
URL_TL = api["url_tl"]
URL_TA = api["url_ta"]
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

translator.set_service_url(URL_TL)
tone_analyzer.set_service_url(URL_TA)

# Googletrans to save quota.
googletrans = Translator()

# Emoji regex.
EMOJI = compile(u"[^\U00000000-\U0000d7ff\U0000e000-\U0000ffff]", flags=UNICODE)


# Analyze the tone of comments from the DB and write results to it.
def analyze():
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

            print("Fetching comments...")
            cursor.execute("select id, text "
                           "from a_comments "
                           "where translation is null "
                           "order by id asc")
            comments = cursor.fetchall()
            print("Fetched comments.")

            print("Generating analysis and writing results to DB...")
            # Translate each comment and generate tone analysis.
            for com in comments:
                # Remove emojis.
                text = EMOJI.sub(u'', com[1])
                # Translate comment and analyze the tone.
                translation = translator.translate(text, model_id='de-en').get_result()['translations'][0]['translation']
                # translation = googletrans.translate(text).text
                tone_analysis = tone_analyzer.tone({'text': translation}, content_type='text/plain').get_result()

                tones = []
                for tone in tone_analysis['document_tone']['tones']:
                    tones.append(tone['tone_name'] + ": " + str(tone['score']))

                # Write translation and tone to DB.
                cursor.execute("update a_comments "
                               "set translation = %s, tone = %s "
                               "where id = %s",
                               (translation, tones, com[0]))
                connection.commit()

            cursor.close()
            connection.close()
            return "Finished writing results to DB."
    except (Exception, Error) as error:
        return error
    finally:
        if connection:
            cursor.close()
            connection.close()


print(analyze())
